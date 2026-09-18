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

	// applyMu serializes the (dispatch → AOF-log) section of write commands.
	// The invariant this protects: a write command's store commit and its AOF
	// entry become visible atomically, so a concurrent BGREWRITEAOF snapshot
	// can never see "committed but not yet logged" state (which would make
	// the command appear both in the snapshot and in the post-rewrite log,
	// e.g. duplicating an RPUSH after replay). Read commands bypass it.
	applyMu sync.Mutex

	// subMu guards the pub/sub hub below.
	subMu     sync.Mutex
	channels  map[string]map[*client]struct{} // channel → subscribed conns
}

// New returns a ready-to-serve in-memory Server.
func New() *Server {
	return &Server{store: store.New(), channels: make(map[string]map[*client]struct{})}
}

// NewWithAOF returns a Server backed by an append-only file at path.
// Any existing file is replayed first (a truncated tail is tolerated:
// commands parsed before the truncation point are applied), then the file is
// reopened for appending.
func NewWithAOF(path string) (*Server, error) {
	cmds, err := persist.Load(path)
	if err != nil {
		log.Printf("warning: %v", err)
	}
	s := &Server{store: store.New(), channels: make(map[string]map[*client]struct{})}
	for _, v := range cmds {
		s.dispatch(v)
	}
	a, err := persist.Open(path)
	if err != nil {
		return nil, err
	}
	s.aof = a
	return s, nil
}

// Close releases the AOF file handle, if any.
func (s *Server) Close() error {
	if s.aof == nil {
		return nil
	}
	return s.aof.Close()
}

// Listen accepts connections on addr (e.g. ":6379") until an error occurs.
func (s *Server) Listen(addr string) error {
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
// of channels this connection subscribes to.
type client struct {
	writeMu sync.Mutex
	w       *bufio.Writer
	chans   map[string]struct{}
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
	cl := &client{w: bufio.NewWriter(conn), chans: make(map[string]struct{})}
	defer s.dropClient(cl) // 断连自动退订所有频道
	for {
		v, err := r.Read()
		if err != nil {
			return
		}
		reply := s.applyConn(cl, v)
		if !cl.write(reply) {
			return
		}
		if reply.Type == resp.SimpleString && reply.Str == "OK" && isQuit(v) {
			return
		}
	}
}

// applyConn layers connection-scoped semantics on top of apply: pub/sub
// commands are handled per client, and while subscribed only subscription
// commands, PING and QUIT are accepted (Redis subscribe-mode restriction).
func (s *Server) applyConn(cl *client, v resp.Value) resp.Value {
	cmd, ok := firstCmd(v)
	if !ok {
		return s.apply(v) // 非法协议形态保持原错误
	}
	args := v.Arr[1:]
	subModeErr := func() resp.Value {
		return resp.Value{Type: resp.Error, Str: fmt.Sprintf(
			"ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
			strings.ToLower(cmd))}
	}
	switch cmd {
	case "SUBSCRIBE":
		return cl.subscribe(s, args)
	case "UNSUBSCRIBE":
		return cl.unsubscribe(s, args)
	case "PUBLISH":
		if len(cl.chans) > 0 {
			return subModeErr() // Redis 订阅模式下拒绝 PUBLISH
		}
		return s.cmdPublish(args)
	case "PING":
		if len(cl.chans) > 0 {
			// 订阅模式下 PING 回复 [pong, <msg|"">] 数组（Redis 行为）
			msg := ""
			if len(args) > 0 {
				msg = args[0].Str
			}
			return resp.Value{Type: resp.Array, Arr: []resp.Value{
				{Type: resp.SimpleString, Str: "PONG"},
				{Type: resp.BulkString, Str: msg},
			}}
		}
	case "QUIT":
		// 交给 apply 返回 OK，handle 负责断开
	default:
		if len(cl.chans) > 0 {
			return subModeErr()
		}
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
// to (runs via defer in handle).
func (s *Server) dropClient(cl *client) {
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

// apply executes v and, when AOF is enabled and the command succeeded,
// appends the canonical form of the write command to the file before the
// reply is returned (Redis executes, then propagates, then replies). The
// reply is passed along because some commands (SPOP) canonicalize to a form
// derived from what actually happened. Write commands run dispatch and log
// atomically under applyMu (see the Server field comment); reads stay
// lock-free and concurrent.
func (s *Server) apply(v resp.Value) resp.Value {
	if !isWriteCmd(v) {
		return s.dispatch(v)
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	reply := s.dispatch(v)
	if s.aof == nil || reply.Type == resp.Error {
		return reply
	}
	if canon, ok := canonicalWrite(v, reply); ok {
		if err := s.aof.Log(canon); err != nil {
			return resp.Value{Type: resp.Error, Str: "ERR AOF write error: " + err.Error()}
		}
	}
	return reply
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
	"HSET": true, "HDEL": true, "HINCRBY": true,
	"SADD": true, "SREM": true, "SPOP": true,
	"ZADD": true, "ZINCRBY": true, "ZREM": true,
	"FLUSHALL": true,
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
func canonicalWrite(v, reply resp.Value) (resp.Value, bool) {
	if v.Type != resp.Array || len(v.Arr) == 0 {
		return resp.Value{}, false
	}
	cmd := strings.ToUpper(v.Arr[0].Str)
	if !writeCmds[cmd] {
		return resp.Value{}, false
	}
	args := v.Arr[1:]
	switch cmd {
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
		if len(args) != 2 {
			return v, true
		}
		sec, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return v, true
		}
		return respCmd("PEXPIREAT", args[0].Str,
			msAt(time.Now().Add(time.Duration(sec)*time.Second))), true
	case "SET":
		out := make([]resp.Value, len(v.Arr))
		out[0] = resp.Value{Type: resp.BulkString, Str: "SET"}
		copy(out[1:], args)
		for i := 1; i < len(out)-1; i++ {
			opt := strings.ToUpper(out[i].Str)
			if opt != "EX" && opt != "PX" {
				continue
			}
			n, err := strconv.ParseInt(out[i+1].Str, 10, 64)
			if err != nil || n <= 0 {
				return v, true // replay would fail identically; keep verbatim
			}
			unit := time.Second
			if opt == "PX" {
				unit = time.Millisecond
			}
			out[i] = resp.Value{Type: resp.BulkString, Str: "PXAT"}
			out[i+1] = resp.Value{Type: resp.BulkString, Str: msAt(time.Now().Add(time.Duration(n) * unit))}
			break
		}
		return resp.Value{Type: resp.Array, Arr: out}, true
	default:
		return v, true
	}
}

func msAt(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

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

func (s *Server) cmdSet(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	key, val := args[0].Str, args[1].Str
	var exp time.Time // zero = no expiry
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
		default:
			return syntaxErr()
		}
	}
	s.store.SetAt(key, val, exp)
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

func (s *Server) cmdExpire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("expire")
	}
	sec, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	if s.store.Expire(args[0].Str, time.Duration(sec)*time.Second) {
		return resp.Value{Type: resp.Integer, Num: 1}
	}
	return resp.Value{Type: resp.Integer, Num: 0}
}

// cmdPExpireAt handles PEXPIREAT key ms (absolute expiry; past deletes key).
func (s *Server) cmdPExpireAt(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("pexpireat")
	}
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	if s.store.ExpireAt(args[0].Str, time.UnixMilli(ms)) {
		return resp.Value{Type: resp.Integer, Num: 1}
	}
	return resp.Value{Type: resp.Integer, Num: 0}
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
		"redis_version:redis-go-0.5.0\r\n" +
		"redis_mode:standalone\r\n" +
		"os:" + runtime.GOOS + "\r\n" +
		"go_version:" + runtime.Version() + "\r\n" +
		"tcp_port:" + s.port() + "\r\n" +
		fmt.Sprintf("process_id:%d\r\n", os.Getpid()) +
		"\r\n"
	persistence := "# Persistence\r\n" +
		"aof_enabled:" + aofEnabled + "\r\n" +
		"\r\n"
	keyspace := "# Keyspace\r\n" +
		fmt.Sprintf("db0:keys=%d,expires=%d\r\n", keys, expires)
	return []infoSection{
		{"Server", server},
		{"Persistence", persistence},
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
		case "appendonly", "databases", "maxmemory", "save":
			return bulkArray([]string{name, s.configValue(name)})
		}
		return bulkArray(nil)
	case "SET":
		if len(args) < 2 {
			return wrongArgs("config")
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
