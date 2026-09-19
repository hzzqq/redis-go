// Package server implements a Redis-compatible TCP server.
package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// Server holds the in-memory store and dispatches commands.
type Server struct {
	store *store.Store
	aof   *persist.AOF // nil = persistence disabled
	addr  string       // listen address ("" until Listen is called)

	// rdbPath enables RDB snapshots (SAVE/BGSAVE + startup load); empty = off.
	rdbPath string
	// rdbMu serializes SAVE/BGSAVE writers; lastSave is unix seconds of the
	// last successful snapshot (INFO reporting).
	rdbMu    sync.Mutex
	lastSave atomic.Int64

	// applyMu serializes the (dispatch → AOF-log) section of write commands.
	// The invariant this protects: a write command's store commit and its AOF
	// entry become visible atomically, so a concurrent BGREWRITEAOF snapshot
	// can never see "committed but not yet logged" state (which would make
	// the command appear both in the snapshot and in the post-rewrite log,
	// e.g. duplicating an RPUSH after replay). Read commands bypass it.
	// MULTI/EXEC also runs its whole block under this lock for atomicity.
	applyMu sync.Mutex

	// subMu guards the pub/sub hub below.
	subMu    sync.Mutex
	channels map[string]map[*client]struct{} // channel → subscribed conns

	// replication (Phase 7/8): replID is this server's 40-char run id; master
	// is non-nil while this instance is a replica; replicas holds the
	// downstream replica links this server streams commands to. replMu guards
	// master/replicas; lock order is applyMu → replMu → link.mu. backlog is
	// the replication ring buffer backing partial resync (PSYNC offset): fed
	// under applyMu (same critical section as the command commit), read under
	// applyMu in handlePSYNC. masterOff is the global replication offset.
	replID    string
	replMu    sync.Mutex
	master    *masterLink
	replicas  map[*client]*replicaLink
	backlog   *replBacklog
	masterOff atomic.Int64

	// upstream 保存最近一次主库同步的凭据（replMu 保护）：REPLICAOF NO ONE
	// 晋升后保留，之后再挂回同一主库时据此发 PSYNC <id> <offset> 请求部分
	// 重同步（对齐真实 Redis 的 replid 保留语义；指向其他主库时 run id 不
	// 匹配自动退化全量）。实现见 replication.go。
	upstreamID  string
	upstreamOff int64

	// WATCH optimistic-lock hub (Phase 8): key → clients watching it.
	// watchMu guards the hub; lock order is applyMu → watchMu.
	watchMu  sync.Mutex
	watchers map[string]map[*client]struct{}

	// 阻塞弹出等待者队列（Phase 10）：key → FIFO 等待者。bwMu 保护；锁序
	// applyMu → bwMu → watchMu。等待者不持 applyMu 睡眠，投喂由推送命令的
	// applyMu 临界区经 logAndPropagate → serveWaitersFrames 完成。
	bwMu   sync.Mutex
	blockQ map[string][]*blockWaiter

	// 流阻塞读等待者队列（Phase 11）：key → 等待者集合。xwMu 保护；锁序
	// applyMu → xwMu。XREAD BLOCK 的唤醒发生在 XADD 提交的 applyMu 临界区
	// （logAndPropagate → serveStreamWaiters），投喂为纯读快照、不产生帧。
	xwMu    sync.Mutex
	xblockQ map[string][]*streamWaiter

	// Lua script cache (Phase 8): sha1 hex → source. scriptMu guards it; the
	// cache is per-process memory and lost on restart (Redis same).
	scriptMu sync.Mutex
	scripts  map[string]string

	// aofFsync is the AOF fsync policy for reporting (INFO/CONFIG): "always",
	// "everysec" or "no". The actual flushing runs inside persist.AOF; set via
	// ConfigureAOF (default "no" for library users, main.go passes -appendfsync).
	aofFsync string
}

// New returns a ready-to-serve in-memory Server.
func New() *Server {
	return &Server{store: store.New(), channels: make(map[string]map[*client]struct{}),
		replicas: make(map[*client]*replicaLink), replID: newReplID(),
		watchers: make(map[string]map[*client]struct{}), backlog: newReplBacklog(),
		scripts: make(map[string]string), blockQ: make(map[string][]*blockWaiter),
		xblockQ: make(map[string][]*streamWaiter)}
}

// NewWithAOF returns a Server backed by an append-only file at path.
// Any existing file is replayed first (a truncated tail is tolerated:
// commands parsed before the truncation point are applied), then the file is
// reopened for appending.
func NewWithAOF(path string) (*Server, error) {
	return NewWithPersist("", path)
}

// NewWithPersist restores server state from persistence. The AOF is a full,
// deterministic history: replaying it yields the complete state, and the RDB
// snapshot point is always inside that history. Loading RDB first and then
// replaying the whole AOF would apply every pre-snapshot non-idempotent
// command (RPUSH/LPUSH/INCR/APPEND/...) a second time — observed as duplicated
// list elements in verification — so when the AOF is enabled it is the sole
// data source and the RDB is skipped; the RDB loads only without an AOF.
// Corrupt RDB stays fatal (like Redis); AOF tolerates a truncated tail.
func NewWithPersist(rdbPath, aofPath string) (*Server, error) {
	s := &Server{store: store.New(), channels: make(map[string]map[*client]struct{}),
		rdbPath: rdbPath, replicas: make(map[*client]*replicaLink), replID: newReplID(),
		watchers: make(map[string]map[*client]struct{}), backlog: newReplBacklog(),
		scripts: make(map[string]string), blockQ: make(map[string][]*blockWaiter),
		xblockQ: make(map[string][]*streamWaiter)}
	if rdbPath != "" && aofPath == "" {
		entries, err := persist.LoadRDB(rdbPath)
		if err != nil {
			return nil, err
		}
		for _, en := range entries {
			s.applyExported(en)
		}
		log.Printf("rdb: loaded %d keys from %s", len(entries), rdbPath)
	}
	if aofPath != "" {
		cmds, err := persist.Load(aofPath)
		if err != nil {
			log.Printf("warning: %v", err)
		}
		s.replay(cmds)
		a, err := persist.Open(aofPath)
		if err != nil {
			return nil, err
		}
		s.aof = a
	}
	return s, nil
}

// Close releases the AOF file handle, if any.
func (s *Server) Close() error {
	if s.aof == nil {
		return nil
	}
	return s.aof.Close()
}

// ConfigureAOF sets the AOF fsync policy (Redis appendfsync): "always" = sync
// after every write command, "everysec" = a background goroutine flushes once
// per second (a crash loses at most ~1s of acknowledged writes), "no" = let
// the OS decide. No-op when the AOF is disabled.
func (s *Server) ConfigureAOF(mode string) {
	if s.aof == nil {
		return
	}
	if mode != "always" && mode != "everysec" && mode != "no" {
		mode = "no"
	}
	s.aof.SetFsync(mode)
	s.aofFsync = mode
}

// Listen accepts connections on addr (e.g. ":6379") until an error occurs.
func (s *Server) Listen(addr string) error {
	go s.ackLoop() // REPLCONF ACK 心跳：由 Listen 启动，随进程存活
	s.addr = addr
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return err
		}
		go s.handle(conn)
	}
}

// client is the per-connection state: one bufio writer guarded by writeMu
// (command replies and pub/sub pushes interleave on the same socket, so every
// write goes through here as one atomic WriteValue+Flush unit), plus the set
// of channels this connection subscribes to and the MULTI/EXEC transaction
// state (touched only by this connection's handle goroutine).
type client struct {
	conn    net.Conn // raw socket (replica-link writer writes frames directly)
	writeMu sync.Mutex
	w       *bufio.Writer
	chans   map[string]struct{}

	inMulti  bool         // MULTI received, commands are being queued
	queue    []resp.Value // commands queued since MULTI
	queueErr bool         // a queue-time error poisoned the transaction

	// replication-link state (set once by PSYNC, read by handle/dropClient)
	replPort    string       // announced by REPLCONF listening-port
	replicaMode bool         // upgraded to replica: handle() suppresses replies
	replicaLink *replicaLink // this connection's master-side link (nil otherwise)

	// WATCH state (touched by this connection's handle goroutine + writers)
	watchKeys map[string]struct{} // keys this connection watches
	dirtyCAS  bool                // a watched key changed → EXEC must abort
}

// write serializes one reply/push onto the connection.
func (c *client) write(v resp.Value) bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := resp.WriteValue(c.w, v); err != nil {
		return false
	}
	return c.w.Flush() == nil
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	r := resp.NewReader(conn)
	cl := &client{conn: conn, w: bufio.NewWriter(conn), chans: make(map[string]struct{})}
	defer s.dropClient(cl) // 断连自动退订所有频道 + 摘除副本链路
	for {
		v, err := r.Read()
		if err != nil {
			return
		}
		reply := s.applyConn(cl, v)
		if cl.replicaMode {
			// 已升级为副本连接：命令流 + REPLCONF ACK 单向，回复一律静默
			continue
		}
		if !cl.write(reply) {
			return
		}
		if reply.Type == resp.SimpleString && reply.Str == "OK" && isQuit(v) {
			return
		}
	}
}

// applyConn layers connection-scoped semantics on top of apply: the pub/sub
// subscribe-mode restriction, MULTI/EXEC transaction state, and per-client
// subscription bookkeeping.
func (s *Server) applyConn(cl *client, v resp.Value) resp.Value {
	cmd, ok := firstCmd(v)
	if !ok {
		return s.apply(v) // 非法协议形态保持原错误
	}
	subModeErr := func() resp.Value {
		return resp.Value{Type: resp.Error, Str: fmt.Sprintf(
			"ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
			strings.ToLower(cmd))}
	}
	// 订阅模式：仅放行订阅族 / PING / QUIT（Redis 同限制，事务命令同样拒绝）
	if len(cl.chans) > 0 {
		switch cmd {
		case "SUBSCRIBE":
			return cl.subscribe(s, v.Arr[1:])
		case "UNSUBSCRIBE":
			return cl.unsubscribe(s, v.Arr[1:])
		case "PING":
			// 订阅模式下 PING 回复 [pong, <msg|"">] 数组（Redis 行为）
			msg := ""
			if len(v.Arr) > 1 {
				msg = v.Arr[1].Str
			}
			return resp.Value{Type: resp.Array, Arr: []resp.Value{
				{Type: resp.SimpleString, Str: "PONG"},
				{Type: resp.BulkString, Str: msg},
			}}
		case "QUIT":
			// 交给 apply 返回 OK，handle 负责断开
		default:
			return subModeErr()
		}
		return s.apply(v)
	}
	// 事务状态机（订阅模式之外）
	switch cmd {
	case "MULTI":
		if cl.inMulti {
			return resp.Value{Type: resp.Error, Str: "ERR MULTI calls can not be nested"}
		}
		cl.inMulti = true
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "EXEC":
		if !cl.inMulti {
			return resp.Value{Type: resp.Error, Str: "ERR EXEC without MULTI"}
		}
		return s.execTransaction(cl)
	case "DISCARD":
		if !cl.inMulti {
			return resp.Value{Type: resp.Error, Str: "ERR DISCARD without MULTI"}
		}
		cl.resetTxn()
		s.clearWatch(cl) // Redis 同语义：DISCARD 一并取消 WATCH
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "QUIT":
		// QUIT 不入队：清掉未执行的事务后立即生效（Redis 同语义）
		cl.resetTxn()
		s.clearWatch(cl)
		return s.apply(v)
	case "WATCH":
		if cl.inMulti {
			return watchCmdError()
		}
		return s.cmdWatch(cl, v.Arr[1:])
	case "UNWATCH":
		return s.cmdUnwatch(cl)
	case "BLPOP", "BRPOP", "BRPOPLPUSH":
		// MULTI 内排队、EXEC 时非阻塞执行（Redis 同语义）；连接级走阻塞路径
		if cl.inMulti {
			return s.queueForTxn(cl, cmd, v)
		}
		return s.blockingPop(cl, cmd, v.Arr[1:])
	case "XREAD":
		// MULTI 内排队、EXEC 时非阻塞执行（Redis 同语义）；连接级带 BLOCK
		// 走阻塞路径（cmdXReadConn 内分流）
		if cl.inMulti {
			return s.queueForTxn(cl, cmd, v)
		}
		return s.cmdXReadConn(cl, v.Arr[1:])
	case "XREADGROUP":
		// MULTI 内排队、EXEC 时非阻塞执行（Redis 同语义）；连接级带 BLOCK
		// 走阻塞路径（cmdXReadGroupConn 内分流，仅 '>' 可阻塞）
		if cl.inMulti {
			return s.queueForTxn(cl, cmd, v)
		}
		return s.cmdXReadGroupConn(cl, v)
	}
	if cl.inMulti {
		return s.queueForTxn(cl, cmd, v)
	}
	// 订阅命令需要 per-connection 状态（cl.chans），走连接层而非 dispatch；
	// PUBLISH 无连接状态，经 apply→dispatch 执行（事务内同样可执行，Redis 同语义）。
	// REPLCONF/PSYNC 是复制协议命令，连接级处理（PSYNC 把连接升级为副本）。
	switch cmd {
	case "SUBSCRIBE":
		return cl.subscribe(s, v.Arr[1:])
	case "UNSUBSCRIBE":
		return cl.unsubscribe(s, v.Arr[1:])
	case "REPLCONF":
		return s.handleReplConf(cl, v.Arr[1:])
	case "PSYNC", "SYNC":
		return s.handlePSYNC(cl, v.Arr[1:])
	case "EVAL", "EVALSHA", "SCRIPT":
		return s.cmdEval(cmd, v.Arr[1:])
	}
	return s.apply(v)
}

// firstCmd returns the upper-cased command name of v (false for non-arrays).
func firstCmd(v resp.Value) (string, bool) {
	if v.Type != resp.Array || len(v.Arr) == 0 || v.Arr[0].Type != resp.BulkString {
		return "", false
	}
	return strings.ToUpper(v.Arr[0].Str), true
}

// subscribe handles SUBSCRIBE channel [channel ...]: registers the client on
// each channel in the hub and emits one [subscribe, channel, count] row per
// channel (count = channels this connection now subscribes to). Redis sends
// each row as its own RESP frame, so all rows but the last are written
// directly on the connection (same goroutine → ordering holds) and the last
// becomes the command reply. Resubscribing to a joined channel is a no-op
// that still replies.
func (c *client) subscribe(s *Server, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return wrongArgs("subscribe")
	}
	s.subMu.Lock()
	rows := make([]resp.Value, 0, len(args))
	for _, a := range args {
		ch := a.Str
		if _, exists := c.chans[ch]; !exists {
			c.chans[ch] = struct{}{}
		}
		set, ok := s.channels[ch]
		if !ok {
			set = make(map[*client]struct{})
			s.channels[ch] = set
		}
		set[c] = struct{}{}
		// Redis 语义：每行的 count 是“到目前为止”的订阅总数
		rows = append(rows, subRow("subscribe", ch, int64(len(c.chans))))
	}
	s.subMu.Unlock()
	for _, row := range rows[:len(rows)-1] {
		c.write(row)
	}
	return rows[len(rows)-1]
}

// subRow builds one [kind, channel, count] confirmation row.
func subRow(kind, ch string, count int64) resp.Value {
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: kind},
		{Type: resp.BulkString, Str: ch},
		{Type: resp.Integer, Num: count},
	}}
}

// unsubscribe handles UNSUBSCRIBE [channel ...]: with no arguments it
// unsubscribes from all channels of this connection. Rows follow the
// [unsubscribe, channel, count] shape, one frame per row like Redis; with
// nothing subscribed the single row carries a null channel name. Unknown
// channel names still get a row (with the current count), matching Redis.
func (c *client) unsubscribe(s *Server, args []resp.Value) resp.Value {
	var targets []string
	if len(args) == 0 {
		for ch := range c.chans {
			targets = append(targets, ch)
		}
		sort.Strings(targets) // map 遍历序随机，排序保证回复确定
	} else {
		targets = make([]string, 0, len(args))
		for _, a := range args {
			targets = append(targets, a.Str)
		}
	}
	s.subMu.Lock()
	rows := make([]resp.Value, 0, len(targets))
	for _, ch := range targets {
		if _, was := c.chans[ch]; !was {
			// 未订阅的频道也要回复一行（count 为当前值），与 Redis 一致
			rows = append(rows, subRow("unsubscribe", ch, int64(len(c.chans))))
			continue
		}
		delete(c.chans, ch)
		if set, ok := s.channels[ch]; ok {
			delete(set, c)
			if len(set) == 0 {
				delete(s.channels, ch)
			}
		}
		rows = append(rows, subRow("unsubscribe", ch, int64(len(c.chans))))
	}
	s.subMu.Unlock()
	if len(rows) == 0 {
		return resp.Value{Type: resp.Array, Arr: []resp.Value{
			{Type: resp.BulkString, Str: "unsubscribe"},
			{Type: resp.BulkString, Null: true},
			{Type: resp.Integer, Num: 0},
		}}
	}
	for _, row := range rows[:len(rows)-1] {
		c.write(row)
	}
	return rows[len(rows)-1]
}

// cmdPublish handles PUBLISH channel message: delivers [message, channel,
// payload] to every subscriber of the channel and replies with the receiver
// count. Delivery writes each subscriber's socket under its writeMu, so a
// slow subscriber applies TCP backpressure to the publisher (prototype
// simplification; Redis uses output-buffer limits + disconnect instead).
func (s *Server) cmdPublish(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("publish")
	}
	ch, payload := args[0].Str, args[1].Str
	s.subMu.Lock()
	var targets []*client
	if set, ok := s.channels[ch]; ok {
		targets = make([]*client, 0, len(set))
		for c := range set {
			targets = append(targets, c)
		}
	}
	s.subMu.Unlock()
	msg := resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: "message"},
		{Type: resp.BulkString, Str: ch},
		{Type: resp.BulkString, Str: payload},
	}}
	for _, c := range targets {
		c.write(msg) // 写失败 = 对端已断开，其 handle 循环负责清理
	}
	return resp.Value{Type: resp.Integer, Num: int64(len(targets))}
}

// dropClient removes a closing connection from every channel it subscribed
// to, and detaches its replica link if it had been upgraded to one (both run
// via defer in handle).
func (s *Server) dropClient(cl *client) {
	if cl.replicaLink != nil {
		s.removeReplica(cl.replicaLink)
	}
	s.clearWatch(cl)
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for ch := range cl.chans {
		if set, ok := s.channels[ch]; ok {
			delete(set, cl)
			if len(set) == 0 {
				delete(s.channels, ch)
			}
		}
	}
	cl.chans = make(map[string]struct{})
}

// apply executes v and, when the command succeeded, propagates the canonical
// form of the write command to the AOF (if enabled) and to every attached
// replica before the reply is returned (Redis executes, then propagates, then
// replies). The canonical form is shared by both sinks: random commands are
// already determinized (SPOP → SREM) and relative TTLs already absolutized,
// so replica state and AOF replay agree. Write commands run dispatch and
// log/propagate atomically under applyMu (see the Server field comment);
// reads stay lock-free and concurrent.
func (s *Server) apply(v resp.Value) resp.Value {
	// SORT 带 STORE 是写（覆盖目标 key）；其余 SORT 是读。阻塞弹出由
	// applyConn 在连接层拦截（阻塞路径不能持 applyMu 睡眠），apply 只会经
	// MULTI/兜底路径见到它们的 immediate 形态。
	if !isWriteCmd(v) && !isSortStore(v) {
		return s.dispatch(v)
	}
	// 只读副本拒绝写（Redis 同文 READONLY 错误）；主库命令流（applyFromMaster）
	// 与启动回放不走 apply，因此不受影响。副本仍可 REPLICAOF NO ONE 晋升。
	// XREADGROUP 豁免：投喂改本地 PEL 但语义是读，副本按读放行（Phase 10
	// 阻塞命令同款先例）。
	if s.isReplica() && !isXReadGroup(v) {
		return readonlyErr()
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	setPre := s.setPreState(v)
	reply := s.dispatch(v)
	var frames []resp.Value
	if reply.Type != resp.Error {
		if canon, ok := s.canonicalFor(v, reply, setPre); ok {
			frames = append(frames, canon...)
		}
		// 写命令提交后触碰 WATCH 了这些 key 的连接（乐观锁 CAS 标记）。
		// 缺 key 的 DEL 也会触碰（偏保守：只多 abort 不漏 abort）。
		s.touchWatched(v, nil)
	}
	if err := s.logAndPropagate(frames); err != nil {
		return resp.Value{Type: resp.Error, Str: "ERR AOF write error: " + err.Error()}
	}
	return reply
}

// isSortStore reports whether v is SORT ... STORE dst — a state-mutating
// command that must go through the write path (applyMu + AOF + propagation).
func isSortStore(v resp.Value) bool {
	if v.Type != resp.Array || len(v.Arr) == 0 || !strings.EqualFold(v.Arr[0].Str, "SORT") {
		return false
	}
	for _, a := range v.Arr[1:] {
		if strings.EqualFold(a.Str, "STORE") {
			return true
		}
	}
	return false
}

// canonicalFor wraps canonicalWrite with two special cases. SORT STORE: the
// stored list is persisted as one RPUSH (or DEL for an empty result) built by
// reading the just-written key back under the caller's applyMu — the SORT
// command itself is never replayed (it would re-run its random-free but
// context-dependent computation; the deterministic frame is the stored list).
// XREADGROUP (Phase 12): the delivery's PEL effect is persisted as per-entry
// XCLAIM FORCE JUSTID frames read back from the PEL (the command itself is a
// read and is never replayed). Returns a frame slice (XREADGROUP serving
// multiple streams yields one XCLAIM per entry) plus whether anything should
// be logged.
func (s *Server) canonicalFor(v, reply resp.Value, setPre bool) ([]resp.Value, bool) {
	if isSortStore(v) {
		var dst string
		for i, a := range v.Arr[1:] {
			if strings.EqualFold(a.Str, "STORE") && i+2 < len(v.Arr) {
				dst = v.Arr[i+2].Str
			}
		}
		items, err := s.store.ListRange(dst, 0, -1)
		if err != nil || len(items) == 0 {
			return []resp.Value{respCmd("DEL", dst)}, true
		}
		parts := make([]string, 0, 2+len(items))
		parts = append(parts, "RPUSH", dst)
		parts = append(parts, items...)
		return []resp.Value{respCmd(parts...)}, true
	}
	if isXReadGroup(v) {
		return s.xreadGroupCanonical(v, reply)
	}
	f, ok := canonicalWrite(v, reply, setPre)
	if !ok {
		return nil, false
	}
	return []resp.Value{f}, true
}

// isWriteCmd reports whether v dispatches a state-mutating command (the
// cheap pre-dispatch check that gates the applyMu critical section).
func isWriteCmd(v resp.Value) bool {
	return v.Type == resp.Array && len(v.Arr) > 0 &&
		writeCmds[strings.ToUpper(v.Arr[0].Str)]
}

func isQuit(v resp.Value) bool {
	return v.Type == resp.Array && len(v.Arr) > 0 &&
		strings.EqualFold(v.Arr[0].Str, "QUIT")
}

// writeCmds is the set of commands that mutate state and therefore get
// logged to the AOF.
var writeCmds = map[string]bool{
	"SET": true, "SETEX": true, "DEL": true, "EXPIRE": true, "PEXPIREAT": true,
	"APPEND": true, "INCR": true, "DECR": true, "INCRBY": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true, "LSET": true, "LTRIM": true,
	"LMOVE": true, "LINSERT": true,
	// 阻塞弹出：immediate 路径（MULTI/EXEC、apply 兜底）经 canonicalWrite
	// 确定化为 LPOP/RPOP/LMOVE；连接级阻塞路径在 applyConn 拦截、不经此处。
	"BLPOP": true, "BRPOP": true, "BRPOPLPUSH": true,
	"HSET": true, "HDEL": true, "HINCRBY": true,
	"SADD": true, "SREM": true, "SPOP": true,
	"ZADD": true, "ZINCRBY": true, "ZREM": true,
	"MSET":     true,
	"FLUSHALL": true,
	// Phase 11（stream）：XADD 自动 ID 经 canonicalWrite 定化为显式 ID；
	// XDEL/XTRIM 本身确定，原样落盘。
	"XADD": true, "XDEL": true, "XTRIM": true,
	// Phase 12（消费者组）：XGROUP/XACK/XCLAIM 确定性写命令原样落盘；
	// XREADGROUP 的 PEL 效果经 canonicalFor 特判帧化为 XCLAIM FORCE JUSTID
	// （命令本身永不落盘——与 BLPOP 同款「immediate 形态经 canonical 确定化」
	// 模式，连接级阻塞路径在 applyConn 拦截）。
	"XGROUP": true, "XACK": true, "XCLAIM": true, "XREADGROUP": true,
}

// canonicalWrite maps a successful write command to its persisted form.
// Relative TTLs are rewritten to absolute-millisecond forms so that state is
// correct after a restart regardless of elapsed time (same approach as Redis
// AOF propagation): SETEX → SET key val PXAT ms, EXPIRE → PEXPIREAT key ms,
// SET key val EX/PX n → SET key val PXAT ms. SPOP is rewritten to SREM with
// the members it actually popped — random commands must not be replayed
// verbatim or state drifts after a restart. A popped-nothing SPOP (null or
// empty reply) is not logged at all. Other write commands are stored
// verbatim. The second return value is false for non-write commands.
//
// setPre 是 SET（仅 NX+GET 组合）执行前的 key 存在性快照，用于消歧 null 回复：
// NX+GET 在新 key 上成功时旧值为 null（必须落盘），与 NX 失败的 null 无法从
// reply 区分。其余命令忽略该参数。
func canonicalWrite(v, reply resp.Value, setPre bool) (resp.Value, bool) {
	if v.Type != resp.Array || len(v.Arr) == 0 {
		return resp.Value{}, false
	}
	cmd := strings.ToUpper(v.Arr[0].Str)
	if !writeCmds[cmd] {
		return resp.Value{}, false
	}
	args := v.Arr[1:]
	switch cmd {
	case "BLPOP":
		// immediate 成功（[key, value] 数组）才落盘；null array = 没弹出
		if reply.Type == resp.Array && !reply.Null && len(reply.Arr) == 2 {
			return respCmd("LPOP", args[0].Str), true
		}
		return resp.Value{}, false
	case "BRPOP":
		if reply.Type == resp.Array && !reply.Null && len(reply.Arr) == 2 {
			return respCmd("RPOP", args[0].Str), true
		}
		return resp.Value{}, false
	case "BRPOPLPUSH":
		// 尾弹 src 头推 dst 的确定性形态（阻塞唤醒路径同款）
		if reply.Type == resp.BulkString && !reply.Null {
			return respCmd("LMOVE", args[0].Str, args[1].Str, "RIGHT", "LEFT"), true
		}
		return resp.Value{}, false
	case "SPOP":
		if len(args) < 1 {
			return v, true
		}
		switch {
		case reply.Type == resp.BulkString && reply.Null:
			return resp.Value{}, false // nothing popped
		case reply.Type == resp.BulkString:
			return respCmd("SREM", args[0].Str, reply.Str), true
		case reply.Type == resp.Array && len(reply.Arr) > 0:
			parts := make([]string, 0, 2+len(reply.Arr))
			parts = append(parts, "SREM", args[0].Str)
			for _, m := range reply.Arr {
				parts = append(parts, m.Str)
			}
			return respCmd(parts...), true
		default:
			return resp.Value{}, false // empty array = nothing popped
		}
	case "SETEX":
		if len(args) != 3 {
			return v, true
		}
		sec, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil || sec <= 0 {
			return v, true
		}
		return respCmd("SET", args[0].Str, args[2].Str,
			"PXAT", msAt(time.Now().Add(time.Duration(sec)*time.Second))), true
	case "EXPIRE":
		// 条件未命中（Integer 0）没有状态变化，不落盘
		if reply.Type == resp.Integer && reply.Num == 0 {
			return resp.Value{}, false
		}
		if len(args) < 2 {
			return v, true
		}
		sec, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return v, true
		}
		out := []resp.Value{
			resp.Value{Type: resp.BulkString, Str: "PEXPIREAT"},
			args[0],
			resp.Value{Type: resp.BulkString, Str: msAt(time.Now().Add(time.Duration(sec) * time.Second))},
		}
		// 选项原样保留：NX/XX/GT/LT 比较的是绝对时间，回放时 key 状态与
		// 执行时刻一致（AOF 全序回放），条件重判结果相同。
		out = append(out, args[2:]...)
		return resp.Value{Type: resp.Array, Arr: out}, true
	case "SET":
		if reply.Type == resp.BulkString && reply.Null {
			// null 回复的成因判定：
			//   NX 无 GET / XX（含 XX+GET，key 缺失时旧值必为 null）→ 条件
			//   失败，无状态变化，不落盘；
			//   GET 无 NX/XX → SET 无条件生效（新 key 旧值为 null），必须落盘；
			//   NX+GET → 执行前快照消歧：key 原不存在 = NX 通过，落盘。
			nx, xx, get := setFlags(args)
			if !(get && (!nx && !xx || nx && !setPre)) {
				return resp.Value{}, false
			}
		}
		// 重组选项：剥掉 NX/XX/GET（条件与副作用已在执行时刻定格），保留
		// KEEPTTL，EX/PX 绝对化为 PXAT（原逻辑）。剥 NX/XX 后回放为无条件
		// 覆盖——执行时刻条件已通过，回放到此处时 key 状态一致，覆盖等价。
		out := make([]resp.Value, 0, len(v.Arr))
		out = append(out,
			resp.Value{Type: resp.BulkString, Str: "SET"},
			args[0], args[1])
		for i := 2; i < len(args); i++ {
			opt := strings.ToUpper(args[i].Str)
			switch opt {
			case "NX", "XX", "GET":
				continue
			case "KEEPTTL":
				out = append(out, args[i])
			case "EX", "PX", "PXAT":
				if i+1 >= len(args) {
					return v, true
				}
				if opt == "PXAT" {
					out = append(out, args[i], args[i+1])
					i++
					continue
				}
				n, err := strconv.ParseInt(args[i+1].Str, 10, 64)
				if err != nil || n <= 0 {
					return v, true // replay would fail identically; keep verbatim
				}
				unit := time.Second
				if opt == "PX" {
					unit = time.Millisecond
				}
				out = append(out,
					resp.Value{Type: resp.BulkString, Str: "PXAT"},
					resp.Value{Type: resp.BulkString, Str: msAt(time.Now().Add(time.Duration(n) * unit))})
				i++
			default:
				return v, true
			}
		}
		return resp.Value{Type: resp.Array, Arr: out}, true
	case "XADD":
		// null 回复 = NOMKSTREAM 且 key 不存在：无状态变化，不落盘。
		// 其余成功回复：把 id 参数（"*" 或显式）统一替换为回复中的解析后
		// ID——自动 ID 以显式形态落盘/传播，重启/副本不漂移；修剪选项保留
		// 原样（回放状态与执行时刻一致，修剪效果相同）。
		if reply.Type == resp.BulkString && reply.Null {
			return resp.Value{}, false
		}
		o, errv := xaddParse(args)
		if errv.Type == resp.Error {
			return v, true // 防御：成功回复时参数必合法，理论不达
		}
		optLen := len(args) - len(o.rest)
		out := make([]resp.Value, 0, len(v.Arr))
		out = append(out, v.Arr[:1+optLen]...) // 命令名 + 选项区
		out = append(out, resp.Value{Type: resp.BulkString, Str: reply.Str})
		out = append(out, o.rest[1:]...)
		return resp.Value{Type: resp.Array, Arr: out}, true
	default:
		return v, true
	}
}

func msAt(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

// setFlags 扫描 SET 命令选项，报告 NX/XX/GET 是否出现。EX/PX/PXAT 的取值是
// 数字字面量，不会误判为选项词；非法取值会使命令以 Error 结束，从而根本
// 不会进入 canonicalWrite。
func setFlags(args []resp.Value) (nx, xx, get bool) {
	for _, a := range args[2:] {
		switch strings.ToUpper(a.Str) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		}
	}
	return nx, xx, get
}

// setPreState 捕获 SET 执行前的 key 存在性，供 canonicalWrite 消歧 NX+GET 的
// null 回复（key 原不存在 = NX 通过 = 必须落盘）。仅 NX+GET 组合才访问
// store，其余命令/组合零开销。返回值与「无需快照」共用 false：canonicalWrite
// 只在 NX+GET 分支读取它，语义不冲突。调用方与 dispatch 同锁（applyMu 或
// 事务/脚本临界区），快照与执行之间无并发写。
func (s *Server) setPreState(v resp.Value) bool {
	if v.Type != resp.Array || len(v.Arr) < 3 || !strings.EqualFold(v.Arr[0].Str, "SET") {
		return false
	}
	nx, _, get := setFlags(v.Arr[1:])
	if !nx || !get {
		return false
	}
	_, ok, _ := s.store.Get(v.Arr[1].Str)
	return ok
}

// respCmd builds an array-of-bulk-strings RESP value from plain strings.
func respCmd(parts ...string) resp.Value {
	arr := make([]resp.Value, len(parts))
	for i, p := range parts {
		arr[i] = resp.Value{Type: resp.BulkString, Str: p}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

func (s *Server) dispatch(v resp.Value) resp.Value {
	if v.Type != resp.Array || len(v.Arr) == 0 {
		return resp.Value{Type: resp.Error, Str: "ERR Protocol error: expected array command"}
	}
	cmd := strings.ToUpper(v.Arr[0].Str)
	args := v.Arr[1:]
	switch cmd {
	case "PING":
		if len(args) == 0 {
			return resp.Value{Type: resp.SimpleString, Str: "PONG"}
		}
		return resp.Value{Type: resp.BulkString, Str: args[0].Str}
	case "ECHO":
		if len(args) != 1 {
			return wrongArgs("echo")
		}
		return resp.Value{Type: resp.BulkString, Str: args[0].Str}
	case "GET":
		if len(args) != 1 {
			return wrongArgs("get")
		}
		val, ok, err := s.store.Get(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		if !ok {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: val}
	case "SET":
		return s.cmdSet(args)
	case "SETEX":
		return s.cmdSetEX(args)
	case "DEL":
		var n int64
		for _, a := range args {
			if s.store.Del(a.Str) {
				n++
			}
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "EXISTS":
		var n int64
		for _, a := range args {
			if s.store.Exists(a.Str) {
				n++
			}
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "EXPIRE":
		return s.cmdExpire(args)
	case "PEXPIREAT":
		return s.cmdPExpireAt(args)
	case "TTL":
		if len(args) != 1 {
			return wrongArgs("ttl")
		}
		rem, ok := s.store.TTL(args[0].Str)
		if !ok {
			return resp.Value{Type: resp.Integer, Num: -2}
		}
		if rem < 0 {
			return resp.Value{Type: resp.Integer, Num: -1}
		}
		return resp.Value{Type: resp.Integer, Num: rem}
	case "APPEND":
		return s.cmdAppend(args)
	case "INCR":
		return s.cmdIncrBy(args, 1, "incr")
	case "DECR":
		return s.cmdIncrBy(args, -1, "decr")
	case "INCRBY":
		return s.cmdIncrByWithAmount(args, "incrby")
	case "LPUSH":
		return s.cmdListPush(args, true)
	case "RPUSH":
		return s.cmdListPush(args, false)
	case "LPOP":
		return s.cmdListPop(args, true)
	case "RPOP":
		return s.cmdListPop(args, false)
	case "BLPOP":
		// dispatch 只做非阻塞形态（MULTI/EXEC 执行、回放兜底）；连接级阻塞
		// 路径在 applyConn 拦截
		return s.cmdBlockingImmediate(args, true, "")
	case "BRPOP":
		return s.cmdBlockingImmediate(args, false, "")
	case "BRPOPLPUSH":
		return s.cmdBlockingImmediate(args, false, "dst")
	case "SORT":
		return s.cmdSort(args)
	case "LLEN":
		if len(args) != 1 {
			return wrongArgs("llen")
		}
		n, err := s.store.ListLen(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "LRANGE":
		if len(args) != 3 {
			return wrongArgs("lrange")
		}
		start, ok := parseIntArg(args[1].Str, "lrange")
		if !ok {
			return notIntegerErr()
		}
		stop, ok := parseIntArg(args[2].Str, "lrange")
		if !ok {
			return notIntegerErr()
		}
		items, err := s.store.ListRange(args[0].Str, start, stop)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(items)
	case "LINDEX":
		if len(args) != 2 {
			return wrongArgs("lindex")
		}
		idx, ok := parseIntArg(args[1].Str, "lindex")
		if !ok {
			return notIntegerErr()
		}
		val, found, err := s.store.ListIndex(args[0].Str, idx)
		if err != nil {
			return errReply(err)
		}
		if !found {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: val}
	case "LSET":
		if len(args) != 3 {
			return wrongArgs("lset")
		}
		idx, ok := parseIntArg(args[1].Str, "lset")
		if !ok {
			return notIntegerErr()
		}
		if err := s.store.ListSet(args[0].Str, idx, args[2].Str); err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "LTRIM":
		if len(args) != 3 {
			return wrongArgs("ltrim")
		}
		start, ok := parseIntArg(args[1].Str, "ltrim")
		if !ok {
			return notIntegerErr()
		}
		stop, ok := parseIntArg(args[2].Str, "ltrim")
		if !ok {
			return notIntegerErr()
		}
		if err := s.store.ListTrim(args[0].Str, start, stop); err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "HSET":
		return s.cmdHSet(args)
	case "HGET":
		if len(args) != 2 {
			return wrongArgs("hget")
		}
		val, ok, err := s.store.HashGet(args[0].Str, args[1].Str)
		if err != nil {
			return errReply(err)
		}
		if !ok {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: val}
	case "HGETALL":
		if len(args) != 1 {
			return wrongArgs("hgetall")
		}
		pairs, err := s.store.HashGetAll(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		items := make([]string, 0, len(pairs)*2)
		for _, p := range pairs {
			items = append(items, p[0], p[1])
		}
		return bulkArray(items)
	case "HDEL":
		if len(args) < 2 {
			return wrongArgs("hdel")
		}
		n, err := s.store.HashDel(args[0].Str, fieldsOf(args[1:]))
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "HLEN":
		if len(args) != 1 {
			return wrongArgs("hlen")
		}
		n, err := s.store.HashLen(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "HEXISTS":
		if len(args) != 2 {
			return wrongArgs("hexists")
		}
		exists, err := s.store.HashExists(args[0].Str, args[1].Str)
		if err != nil {
			return errReply(err)
		}
		if exists {
			return resp.Value{Type: resp.Integer, Num: 1}
		}
		return resp.Value{Type: resp.Integer, Num: 0}
	case "HKEYS":
		if len(args) != 1 {
			return wrongArgs("hkeys")
		}
		keys, err := s.store.HashKeys(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(keys)
	case "HVALS":
		if len(args) != 1 {
			return wrongArgs("hvals")
		}
		vals, err := s.store.HashVals(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(vals)
	case "HINCRBY":
		if len(args) != 3 {
			return wrongArgs("hincrby")
		}
		delta, ok := parseIntArg(args[2].Str, "hincrby")
		if !ok {
			return notIntegerErr()
		}
		n, err := s.store.HashIncrBy(args[0].Str, args[1].Str, delta)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "SADD":
		return s.cmdSetMembers(args, true)
	case "SREM":
		return s.cmdSetMembers(args, false)
	case "SISMEMBER":
		if len(args) != 2 {
			return wrongArgs("sismember")
		}
		exists, err := s.store.SetIsMember(args[0].Str, args[1].Str)
		if err != nil {
			return errReply(err)
		}
		if exists {
			return resp.Value{Type: resp.Integer, Num: 1}
		}
		return resp.Value{Type: resp.Integer, Num: 0}
	case "SMEMBERS":
		if len(args) != 1 {
			return wrongArgs("smembers")
		}
		members, err := s.store.SetMembers(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(members)
	case "SCARD":
		if len(args) != 1 {
			return wrongArgs("scard")
		}
		n, err := s.store.SetCard(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "SPOP":
		return s.cmdSetPop(args)
	case "SRANDMEMBER":
		return s.cmdSetRandMember(args)
	case "SINTER":
		return s.cmdSetAlgebra(args, "sinter")
	case "SUNION":
		return s.cmdSetAlgebra(args, "sunion")
	case "SDIFF":
		return s.cmdSetAlgebra(args, "sdiff")
	case "ZADD":
		return s.cmdZAdd(args)
	case "ZSCORE":
		if len(args) != 2 {
			return wrongArgs("zscore")
		}
		score, found, err := s.store.ZScore(args[0].Str, args[1].Str)
		if err != nil {
			return errReply(err)
		}
		if !found {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: formatScore(score)}
	case "ZINCRBY":
		return s.cmdZIncrBy(args)
	case "ZCARD":
		if len(args) != 1 {
			return wrongArgs("zcard")
		}
		n, err := s.store.ZCard(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "ZRANK":
		return s.cmdZRank(args, false)
	case "ZREVRANK":
		return s.cmdZRank(args, true)
	case "ZCOUNT":
		return s.cmdZCount(args)
	case "ZRANGE":
		return s.cmdZRange(args, false)
	case "ZREVRANGE":
		return s.cmdZRange(args, true)
	case "ZREM":
		return s.cmdZRem(args)
	case "TYPE":
		if len(args) != 1 {
			return wrongArgs("type")
		}
		return resp.Value{Type: resp.SimpleString, Str: s.store.Type(args[0].Str)}
	case "DBSIZE":
		if len(args) != 0 {
			return wrongArgs("dbsize")
		}
		return resp.Value{Type: resp.Integer, Num: s.store.DBSize()}
	case "INFO":
		return s.cmdInfo(args)
	case "CONFIG":
		return s.cmdConfig(args)
	case "BGREWRITEAOF":
		return s.cmdBGRewriteAOF()
	case "SAVE":
		return s.cmdSave()
	case "BGSAVE":
		return s.cmdBGSave()
	case "MGET":
		return s.cmdMGet(args)
	case "MSET":
		return s.cmdMSet(args)
	case "ZRANGEBYSCORE":
		return s.cmdZRangeByScore(args, false)
	case "ZREVRANGEBYSCORE":
		return s.cmdZRangeByScore(args, true)
	case "ZRANGEBYLEX":
		return s.cmdZRangeByLex(args, false)
	case "ZREVRANGEBYLEX":
		return s.cmdZRangeByLex(args, true)
	case "ZRANDMEMBER":
		return s.cmdZRandMember(args)
	case "SCAN":
		return s.cmdScan(args)
	case "SSCAN":
		return s.cmdSScan(args)
	case "HSCAN":
		return s.cmdHScan(args)
	case "ZSCAN":
		return s.cmdZScan(args)
	case "OBJECT":
		return s.cmdObject(args)
	case "LMOVE":
		return s.cmdLMove(args)
	case "LINSERT":
		return s.cmdLInsert(args)
	case "LPOS":
		return s.cmdLPos(args)
	case "PUBLISH":
		return s.cmdPublish(args)
	case "XADD":
		return s.cmdXAdd(args)
	case "XLEN":
		return s.cmdXLen(args)
	case "XRANGE":
		return s.cmdXRange(args, false)
	case "XREVRANGE":
		return s.cmdXRange(args, true)
	case "XDEL":
		return s.cmdXDel(args)
	case "XTRIM":
		return s.cmdXTrim(args)
	case "XREAD":
		// dispatch 只做非阻塞形态（MULTI/EXEC 执行、回放兜底）；连接级
		// BLOCK 路径在 applyConn 拦截
		return s.cmdXRead(args)
	case "XGROUP":
		return s.cmdXGroup(args)
	case "XREADGROUP":
		// dispatch 只做非阻塞形态；连接级 BLOCK 路径在 applyConn 拦截
		return s.cmdXReadGroupImmediate(args)
	case "XACK":
		return s.cmdXack(args)
	case "XPENDING":
		return s.cmdXPending(args)
	case "XCLAIM":
		return s.cmdXClaim(args)
	case "REPLICAOF", "SLAVEOF":
		return s.cmdReplicaOf(args)
	case "FLUSHALL":
		s.store.Flush()
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "COMMAND":
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "QUIT":
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	default:
		return resp.Value{Type: resp.Error, Str: fmt.Sprintf("ERR unknown command '%s'", cmd)}
	}
}

// cmdSet handles the full SET form: SET key val [NX|XX] [KEEPTTL]
// [EX s|PX ms|PXAT ms] [GET]. NX/XX gate the write on (non-)existence, GET
// turns the reply into the old value (null when the key was absent),
// KEEPTTL preserves the existing TTL (mutually exclusive with EX/PX/PXAT,
// as Redis rejects the combination with a syntax error).
func (s *Server) cmdSet(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	key, val := args[0].Str, args[1].Str
	var exp time.Time // zero = no expiry
	keepTTL, nx, xx, wantOld := false, false, false, false
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "EX", "PX", "PXAT":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			n, err := strconv.ParseInt(args[i+1].Str, 10, 64)
			if err != nil {
				return resp.Value{Type: resp.Error, Str: "ERR invalid expire time in 'set' command"}
			}
			opt := strings.ToUpper(args[i].Str)
			if opt == "PXAT" {
				exp = time.UnixMilli(n) // past timestamps delete the key
			} else {
				if n <= 0 {
					return resp.Value{Type: resp.Error, Str: "ERR invalid expire time in 'set' command"}
				}
				unit := time.Second
				if opt == "PX" {
					unit = time.Millisecond
				}
				exp = time.Now().Add(time.Duration(n) * unit)
			}
			i++
		case "KEEPTTL":
			keepTTL = true
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			wantOld = true
		default:
			return syntaxErr()
		}
	}
	if nx && xx {
		return syntaxErr()
	}
	if keepTTL && !exp.IsZero() {
		return syntaxErr()
	}
	old, didSet, err := s.store.SetFull(key, val, exp, keepTTL, nx, xx, wantOld)
	if err != nil {
		return errReply(err)
	}
	if wantOld {
		if old == nil {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: *old}
	}
	if !didSet {
		return resp.Value{Type: resp.BulkString, Null: true}
	}
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

func (s *Server) cmdSetEX(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return wrongArgs("setex")
	}
	sec, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil || sec <= 0 {
		return resp.Value{Type: resp.Error, Str: "ERR invalid expire time in 'setex' command"}
	}
	s.store.Set(args[0].Str, args[2].Str, time.Duration(sec)*time.Second)
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

// cmdExpire handles EXPIRE key seconds [NX|XX|GT|LT ...] (options may
// combine, e.g. XX+GT; NX+XX / GT+LT conflict). Returns 1 when the expiry
// was set, 0 when the key is missing or a condition failed.
func (s *Server) cmdExpire(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("expire")
	}
	sec, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	cond, errv := parseExpireConds(args[2:], "expire")
	if errv.Type == resp.Error {
		return errv
	}
	if s.store.ExpireAtOpts(args[0].Str, time.Now().Add(time.Duration(sec)*time.Second),
		cond["NX"], cond["XX"], cond["GT"], cond["LT"]) {
		return resp.Value{Type: resp.Integer, Num: 1}
	}
	return resp.Value{Type: resp.Integer, Num: 0}
}

// cmdPExpireAt handles PEXPIREAT key ms [NX|XX|GT|LT ...] (absolute expiry;
// past deletes key). Shares the option grammar with EXPIRE.
func (s *Server) cmdPExpireAt(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("pexpireat")
	}
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	cond, errv := parseExpireConds(args[2:], "pexpireat")
	if errv.Type == resp.Error {
		return errv
	}
	if s.store.ExpireAtOpts(args[0].Str, time.UnixMilli(ms),
		cond["NX"], cond["XX"], cond["GT"], cond["LT"]) {
		return resp.Value{Type: resp.Integer, Num: 1}
	}
	return resp.Value{Type: resp.Integer, Num: 0}
}

// parseExpireConds parses the NX/XX/GT/LT tail shared by EXPIRE/PEXPIREAT,
// rejecting the incompatible pairs with the Redis wording.
func parseExpireConds(args []resp.Value, _ string) (map[string]bool, resp.Value) {
	cond := map[string]bool{"NX": false, "XX": false, "GT": false, "LT": false}
	for _, a := range args {
		opt := strings.ToUpper(a.Str)
		switch opt {
		case "NX", "XX", "GT", "LT":
			cond[opt] = true
		default:
			return nil, syntaxErr()
		}
	}
	if cond["NX"] && cond["XX"] || cond["GT"] && cond["LT"] {
		return nil, resp.Value{Type: resp.Error,
			Str: "ERR NX and XX, GT or LT options at the same time are not compatible"}
	}
	return cond, resp.Value{}
}

// cmdAppend handles APPEND key value: returns the new length (Integer).
func (s *Server) cmdAppend(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("append")
	}
	n, err := s.store.Append(args[0].Str, args[1].Str)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdIncrBy handles INCR/DECR (fixed delta, no extra args). cmdName is used
// for error messages.
func (s *Server) cmdIncrBy(args []resp.Value, delta int64, cmdName string) resp.Value {
	if len(args) != 1 {
		return wrongArgs(cmdName)
	}
	v, err := s.store.IncrBy(args[0].Str, delta)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: v}
}

// cmdIncrByWithAmount handles INCRBY key increment.
func (s *Server) cmdIncrByWithAmount(args []resp.Value, cmdName string) resp.Value {
	if len(args) != 2 {
		return wrongArgs(cmdName)
	}
	delta, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	v, err := s.store.IncrBy(args[0].Str, delta)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: v}
}

// cmdListPush handles LPUSH/RPUSH key val [val...]: returns the new length.
func (s *Server) cmdListPush(args []resp.Value, front bool) resp.Value {
	if len(args) < 2 {
		return wrongArgs("lpush/rpush")
	}
	vals := make([]string, len(args)-1)
	for i, a := range args[1:] {
		vals[i] = a.Str
	}
	n, err := s.store.ListPush(args[0].Str, front, vals...)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdListPop handles LPOP/RPOP key [count]. Without count the reply is a
// single bulk (null when missing); with count it is an array (possibly empty).
func (s *Server) cmdListPop(args []resp.Value, front bool) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgs("lpop/rpop")
	}
	var count int64 = 1
	withCount := len(args) == 2
	if withCount {
		var err error
		count, err = strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return notIntegerErr()
		}
	}
	popped, err := s.store.ListPop(args[0].Str, front, count)
	if err != nil {
		return errReply(err)
	}
	if !withCount {
		if len(popped) == 0 {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: popped[0]}
	}
	return bulkArray(popped)
}

// cmdHSet handles HSET key field val [field val ...]: returns the number of
// newly added fields.
func (s *Server) cmdHSet(args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgs("hset")
	}
	pairs := make([][2]string, 0, len(args)/2)
	for i := 1; i < len(args); i += 2 {
		pairs = append(pairs, [2]string{args[i].Str, args[i+1].Str})
	}
	n, err := s.store.HashSet(args[0].Str, pairs)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

func fieldsOf(args []resp.Value) []string {
	fields := make([]string, len(args))
	for i, a := range args {
		fields[i] = a.Str
	}
	return fields
}

func parseIntArg(s, _ string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func bulkArray(items []string) resp.Value {
	arr := make([]resp.Value, len(items))
	for i, s := range items {
		arr[i] = resp.Value{Type: resp.BulkString, Str: s}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

func errReply(err error) resp.Value {
	return resp.Value{Type: resp.Error, Str: err.Error()}
}

func notIntegerErr() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR value is not an integer or out of range"}
}

func wrongArgs(cmd string) resp.Value {
	return resp.Value{Type: resp.Error, Str: fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd)}
}

func syntaxErr() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR syntax error"}
}

// cmdSetMembers handles SADD/SREM key member [member ...]: both return the
// number of members actually added/removed.
func (s *Server) cmdSetMembers(args []resp.Value, add bool) resp.Value {
	name := "sadd"
	if !add {
		name = "srem"
	}
	if len(args) < 2 {
		return wrongArgs(name)
	}
	members := fieldsOf(args[1:])
	var n int64
	var err error
	if add {
		n, err = s.store.SetAdd(args[0].Str, members)
	} else {
		n, err = s.store.SetRem(args[0].Str, members)
	}
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdSetPop handles SPOP key [count]: without count the reply is a single
// bulk (null when nothing was popped); with count an array (possibly empty).
func (s *Server) cmdSetPop(args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgs("spop")
	}
	withCount := len(args) == 2
	count := int64(1)
	if withCount {
		var err error
		count, err = strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return notIntegerErr()
		}
	}
	popped, err := s.store.SetPop(args[0].Str, count)
	if err != nil {
		return errReply(err)
	}
	if !withCount {
		if len(popped) == 0 {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: popped[0]}
	}
	return bulkArray(popped)
}

// cmdSetRandMember handles SRANDMEMBER key [count]: like SPOP but nothing is
// removed. Single form replies null for a missing key.
func (s *Server) cmdSetRandMember(args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgs("srandmember")
	}
	withCount := len(args) == 2
	var count int64
	if withCount {
		var err error
		count, err = strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return notIntegerErr()
		}
	}
	got, err := s.store.SetRandMember(args[0].Str, count, withCount)
	if err != nil {
		return errReply(err)
	}
	if !withCount {
		if len(got) == 0 {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: got[0]}
	}
	return bulkArray(got)
}

// cmdSetAlgebra handles SINTER/SUNION/SDIFF key [key ...]: sorted arrays.
func (s *Server) cmdSetAlgebra(args []resp.Value, name string) resp.Value {
	if len(args) < 1 {
		return wrongArgs(name)
	}
	keys := fieldsOf(args)
	var members []string
	var err error
	switch name {
	case "sinter":
		members, err = s.store.SetInter(keys)
	case "sunion":
		members, err = s.store.SetUnion(keys)
	default:
		members, err = s.store.SetDiff(keys)
	}
	if err != nil {
		return errReply(err)
	}
	return bulkArray(members)
}

// formatScore renders a score the way Redis replies do: the shortest decimal
// that round-trips ("1", "2.5", "-0.75").
func formatScore(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// cmdZAdd handles ZADD key score member [score member ...] (plain ZADD, no
// NX/GT/CH flags): replies with the number of newly added members.
func (s *Server) cmdZAdd(args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgs("zadd")
	}
	pairs := make([]store.ZItem, 0, len(args)/2)
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i].Str, 64)
		if err != nil {
			return resp.Value{Type: resp.Error, Str: "ERR value is not a valid float"}
		}
		pairs = append(pairs, store.ZItem{Member: args[i+1].Str, Score: score})
	}
	n, err := s.store.ZAdd(args[0].Str, pairs)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdZIncrBy handles ZINCRBY key increment member: replies with the new score.
func (s *Server) cmdZIncrBy(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return wrongArgs("zincrby")
	}
	delta, err := strconv.ParseFloat(args[1].Str, 64)
	if err != nil {
		return resp.Value{Type: resp.Error, Str: "ERR value is not a valid float"}
	}
	v, err := s.store.ZIncrBy(args[0].Str, args[2].Str, delta)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.BulkString, Str: formatScore(v)}
}

// cmdZRank handles ZRANK/ZREVRANK key member: 0-based rank as an Integer, or
// null bulk when the member is absent.
func (s *Server) cmdZRank(args []resp.Value, rev bool) resp.Value {
	name := "zrank"
	if rev {
		name = "zrevrank"
	}
	if len(args) != 2 {
		return wrongArgs(name)
	}
	var rank int64
	var found bool
	var err error
	if rev {
		rank, found, err = s.store.ZRevRank(args[0].Str, args[1].Str)
	} else {
		rank, found, err = s.store.ZRank(args[0].Str, args[1].Str)
	}
	if err != nil {
		return errReply(err)
	}
	if !found {
		return resp.Value{Type: resp.BulkString, Null: true}
	}
	return resp.Value{Type: resp.Integer, Num: rank}
}

// cmdZCount handles ZCOUNT key min max with Redis range syntax
// ("-inf", "+inf", floats, "(" exclusive prefix).
func (s *Server) cmdZCount(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return wrongArgs("zcount")
	}
	minB, err := store.ParseZBound(args[1].Str)
	if err != nil {
		return resp.Value{Type: resp.Error, Str: "ERR min or max is not a float"}
	}
	maxB, err := store.ParseZBound(args[2].Str)
	if err != nil {
		return resp.Value{Type: resp.Error, Str: "ERR min or max is not a float"}
	}
	n, err := s.store.ZCount(args[0].Str, minB, maxB)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdZRange handles ZRANGE/ZREVRANGE key start stop [WITHSCORES]: a flat
// member array, interleaved with score strings when WITHSCORES is given.
func (s *Server) cmdZRange(args []resp.Value, rev bool) resp.Value {
	name := "zrange"
	if rev {
		name = "zrevrange"
	}
	if len(args) < 3 || len(args) > 4 {
		return wrongArgs(name)
	}
	start, ok := parseIntArg(args[1].Str, name)
	if !ok {
		return notIntegerErr()
	}
	stop, ok := parseIntArg(args[2].Str, name)
	if !ok {
		return notIntegerErr()
	}
	withScores := false
	if len(args) == 4 {
		if !strings.EqualFold(args[3].Str, "WITHSCORES") {
			return syntaxErr()
		}
		withScores = true
	}
	items, err := s.store.ZRange(args[0].Str, start, stop, rev)
	if err != nil {
		return errReply(err)
	}
	out := make([]string, 0, len(items)*2)
	for _, it := range items {
		out = append(out, it.Member)
		if withScores {
			out = append(out, formatScore(it.Score))
		}
	}
	return bulkArray(out)
}

// cmdZRem handles ZREM key member [member ...].
func (s *Server) cmdZRem(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("zrem")
	}
	n, err := s.store.ZRem(args[0].Str, fieldsOf(args[1:]))
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdBGRewriteAOF handles BGREWRITEAOF: replaces the AOF with the minimal
// canonical command set of the current store snapshot. Unlike Redis (fork +
// background child), this runs synchronously under applyMu — when the reply
// arrives the rewrite is already complete, which is a strictly stronger
// guarantee. Pub/sub traffic never touches the AOF, so it needs no rewrite.
func (s *Server) cmdBGRewriteAOF() resp.Value {
	if s.aof == nil {
		return errReply(errors.New(
			"ERR Append only file is disabled: please check \"appendonly\" configuration"))
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	// applyMu 之下做快照：此刻没有任何写命令处于「已入库、未落盘」状态，
	// 快照与重写后追加的日志合起来与真实状态一致（见 applyMu 字段注释）。
	snap := s.store.Snapshot()
	vals := make([]resp.Value, 0, len(snap))
	for _, cmd := range snap {
		if len(cmd) > 0 {
			vals = append(vals, respCmd(cmd...))
		}
	}
	if err := s.aof.Rewrite(vals); err != nil {
		return resp.Value{Type: resp.Error, Str: "ERR " + err.Error()}
	}
	return resp.Value{Type: resp.SimpleString, Str: "Background append only file rewriting started"}
}

// infoSection is one block of the INFO reply (header line + fields + blank).
type infoSection struct {
	name string
	body string
}

// cmdInfo handles INFO [section]. Without an argument all sections are
// concatenated; with a section name only that block is returned (empty bulk
// for an unknown section, like Redis).
func (s *Server) cmdInfo(args []resp.Value) resp.Value {
	sections := s.infoSections()
	if len(args) > 0 {
		for _, sec := range sections {
			if strings.EqualFold(sec.name, args[0].Str) {
				return resp.Value{Type: resp.BulkString, Str: sec.body}
			}
		}
		return resp.Value{Type: resp.BulkString, Str: ""}
	}
	var b strings.Builder
	for _, sec := range sections {
		b.WriteString(sec.body)
	}
	return resp.Value{Type: resp.BulkString, Str: b.String()}
}

// infoSections builds the INFO blocks from live runtime state.
func (s *Server) infoSections() []infoSection {
	keys, expires := s.store.Stats()
	aofEnabled := "0"
	if s.aof != nil {
		aofEnabled = "1"
	}
	server := "# Server\r\n" +
		"redis_version:redis-go-0.6.0\r\n" +
		"redis_mode:standalone\r\n" +
		"os:" + runtime.GOOS + "\r\n" +
		"go_version:" + runtime.Version() + "\r\n" +
		"tcp_port:" + s.port() + "\r\n" +
		fmt.Sprintf("process_id:%d\r\n", os.Getpid()) +
		"\r\n"
	rdbEnabled := "0"
	if s.rdbPath != "" {
		rdbEnabled = "1"
	}
	persistence := "# Persistence\r\n" +
		"rdb_enabled:" + rdbEnabled + "\r\n" +
		fmt.Sprintf("rdb_last_save_time:%d\r\n", s.lastSave.Load()) +
		"aof_enabled:" + aofEnabled + "\r\n" +
		"aof_fsync:" + s.aofFsync + "\r\n"
	if s.aof != nil {
		// everysec 停滞看门狗的主路径兜底同步计数（Phase 10）
		persistence += fmt.Sprintf("aof_fsync_stalls:%d\r\n", s.aof.Stalls())
	}
	persistence += "\r\n"
	keyspace := "# Keyspace\r\n" +
		fmt.Sprintf("db0:keys=%d,expires=%d\r\n", keys, expires)
	return []infoSection{
		{"Server", server},
		{"Persistence", persistence},
		{"Replication", s.replicationSection()},
		{"Keyspace", keyspace},
	}
}

// port extracts the TCP port from the listen address (default 6379).
func (s *Server) port() string {
	if s.addr == "" {
		return "6379"
	}
	if _, p, err := net.SplitHostPort(s.addr); err == nil && p != "" {
		return p
	}
	return s.addr
}

// cmdConfig handles CONFIG GET <param> with a small static registry
// (appendonly reflects the actual AOF state). CONFIG SET is rejected honestly
// because no option is runtime-mutable in redis-go yet. Unknown GET params
// yield an empty array, like Redis.
func (s *Server) cmdConfig(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return wrongArgs("config")
	}
	switch strings.ToUpper(args[0].Str) {
	case "GET":
		if len(args) != 2 {
			return wrongArgs("config")
		}
		name := strings.ToLower(args[1].Str)
		switch name {
		case "appendonly", "appendfsync", "databases", "maxmemory", "save":
			return bulkArray([]string{name, s.configValue(name)})
		}
		return bulkArray(nil)
	case "SET":
		if len(args) != 3 {
			return wrongArgs("config")
		}
		// appendfsync 是目前唯一运行期可变参数：复用 ConfigureAOF（内部对
		// 非法值回退 no，并同步 persist 层 fsync 模式与 INFO/CONFIG 展示字段）。
		if strings.ToLower(args[1].Str) == "appendfsync" {
			s.ConfigureAOF(args[2].Str)
			return resp.Value{Type: resp.SimpleString, Str: "OK"}
		}
		return resp.Value{Type: resp.Error,
			Str: "ERR Unsupported CONFIG parameter: " + args[1].Str}
	default:
		return resp.Value{Type: resp.Error, Str: fmt.Sprintf(
			"ERR Unknown subcommand or wrong number of arguments for '%s'. Try CONFIG HELP.", args[0].Str)}
	}
}

// configValue resolves the CONFIG GET registry (all static except appendonly).
func (s *Server) configValue(name string) string {
	switch name {
	case "appendonly":
		if s.aof != nil {
			return "yes"
		}
		return "no"
	case "appendfsync":
		if s.aof == nil {
			return "no"
		}
		if s.aofFsync == "" {
			return "no"
		}
		return s.aofFsync
	case "databases":
		return "16"
	case "maxmemory":
		return "0"
	case "save":
		return ""
	}
	return ""
}

// ensure io is used (kept for symmetry with future reader refactors)
var _ = io.EOF
