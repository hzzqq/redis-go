// 主从复制（Phase 7/8）。
//
// 语义对齐 Redis 的复制协议：默认全量 RDB 同步，辅以 repl-backlog 支持的
// 部分重同步（PSYNC <replid> <offset> → +CONTINUE 增量续传）与
// REPLCONF GETACK/ACK 心跳（副本确认偏移，INFO slaveN 展示）。
//
//	副本端 REPLICAOF host port
//	  → replicationLoop：TCP 连主库 → REPLCONF listening-port/capa（主库一律 +OK）
//	  → PSYNC <replid> <offset>（从未同步过则 ? -1）：
//	    - run id 不匹配 / offset 超出 backlog → +FULLRESYNC <replid> <off>，
//	      随后把此刻的全量 RDB 作为一个 RESP bulk 值发送 → 副本 Flush 后载入
//	      RDB（若开启 AOF，则在同一临界区把基线重写进 AOF，保证副本重启后
//	      「基线 + 命令流」完整）；
//	    - run id 匹配且 backlog 仍覆盖 offset → +CONTINUE <replid>，直接续传
//	      backlog 中的增量字节，RDB 与 AOF 均不动。
//	  → 之后串行回放命令流，连接断开则退避重连（优先尝试部分重同步）。
//
//	主库端：每条写命令执行并在 AOF 落盘点（与持久化同一份 canonical 形式：
//	随机命令已确定化、相对 TTL 已绝对化）实时推给所有副本，同时 feed 进
//	repl-backlog 环形缓冲（feed 与命令提交同在 applyMu 临界区）；事务以
//	MULTI ... EXEC 帧序列传播，副本端按块感知整体执行，保持原子性。
//	每秒向副本发 GETACK 心跳（控制帧不进 backlog、不计 offset）。
//
//	并发模型：副本出站走独立 writer goroutine（applyMu 临界区内只做加锁入队，
//	慢副本不会拖住主库写路径）；积压超过 replBufLimit 断开该副本（对齐 Redis
//	client-output-buffer-limit replica）。副本身份的连接升级后回复一律静默
//	（复制链路单向，REPLCONF/PSYNC 之后的命令不再回写）。
//
//	简化点（有意为之）：无主从故障自动切换（REPLICAOF NO ONE 手动晋升）；
//	backlog 固定 1 MiB（断线期间主库写入量超过即退化为全量）。
package server

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

const (
	replConnectTimeout = 3 * time.Second
	replRetryDelay     = 2 * time.Second
	// 副本出站缓冲上限（字节）：超过即断开，对齐 Redis
	// client-output-buffer-limit "replica 256mb"。
	replBufLimit = 256 << 20

	// 部分重同步（Phase 8）：repl-backlog 环形缓冲大小与 ACK 心跳间隔。
	// 断线期间主库写入量超过 backlog 即退化为全量重同步（对齐 Redis 行为）。
	replBacklogSize = 1 << 20 // 1 MiB
	replAckInterval = 1 * time.Second
)

// replBacklog 是主库侧的复制流环形缓冲：保存最近 replBacklogSize 字节的
// 传播帧，支撑副本断线后的 PSYNC <offset> 增量续传。feed/since 都在 applyMu
// 临界区内调用（feed 与命令提交同锁，since 与副本注册同锁），无需自带锁。
type replBacklog struct {
	buf    []byte // 环形缓冲（容量 replBacklogSize）
	oldest int64  // buf 中最旧字节的-global-偏移
	off    int64  // 写入游标 = 已 feed 的总字节数（全局 master offset）
}

func newReplBacklog() *replBacklog {
	return &replBacklog{buf: make([]byte, replBacklogSize)}
}

// feed appends one frame to the ring, advancing the global offset. A frame
// larger than the whole buffer invalidates it entirely (replicas must fall
// back to a full sync).
func (b *replBacklog) feed(frame []byte) {
	if len(frame) >= len(b.buf) {
		// 超过整个缓冲：不保留该帧，backlog 作废（副本退化为全量同步）
		b.off += int64(len(frame))
		b.oldest = b.off
		return
	}
	pos := int(b.off % int64(len(b.buf)))
	n := copy(b.buf[pos:], frame)
	if n < len(frame) { // 环回：绕到头部续写
		copy(b.buf, frame[n:])
	}
	b.off += int64(len(frame))
	if b.off-int64(len(b.buf)) > b.oldest {
		b.oldest = b.off - int64(len(b.buf)) // 最旧数据被覆盖
	}
}

// since returns the backlog bytes in [offset, off). ok=false when offset is
// no longer covered (overwritten or ahead) — caller falls back to full sync.
func (b *replBacklog) since(offset int64) (data []byte, ok bool) {
	if offset < b.oldest || offset > b.off {
		return nil, false
	}
	total := int(b.off - offset)
	start := int(offset % int64(len(b.buf)))
	end := int(b.off % int64(len(b.buf)))
	out := make([]byte, 0, total)
	if start < end {
		out = append(out, b.buf[start:end]...)
	} else if start > end { // 数据环绕：两段拼接
		out = append(out, b.buf[start:]...)
		out = append(out, b.buf[:end]...)
	} else if total > 0 { // start == end 且有数据 = 恰好绕整圈
		out = append(out, b.buf[:]...)
	}
	return out, true
}

// readonlyErr 是只读副本拒绝写命令的 Redis 同文错误。
func readonlyErr() resp.Value {
	return resp.Value{Type: resp.Error,
		Str: "READONLY You can't write against a read only replica."}
}

// ---------------------------------------------------------------------------
// 副本端：masterLink + 复制循环
// ---------------------------------------------------------------------------

// masterLink 是副本已挂在某主库时的状态。stop 由 REPLICAOF NO ONE / 切换
// 主库时关闭；conn 在 link.mu 下保存，便于 detach 时关闭打断阻塞中的 read。
// replid/offset 是部分重同步凭据（跨重连保存）：offset 由 syncWithMaster
// goroutine 独占写、INFO 并发读，因此用 atomic；replid 同 goroutine 独占。
type masterLink struct {
	host, port string
	stop       chan struct{}

	mu     sync.Mutex
	status string       // connect | sync | online（INFO 展示）
	conn   net.Conn     // 当前复制连接（握手前为 nil）
	replid string       // 主库 run id（"" = 从未同步，PSYNC ? -1）
	offset atomic.Int64 // 已从主库收到的流偏移（字节）
}

func (l *masterLink) setStatus(st string) {
	l.mu.Lock()
	l.status = st
	l.mu.Unlock()
}

func (l *masterLink) setConn(c net.Conn) {
	l.mu.Lock()
	l.conn = c
	l.mu.Unlock()
}

func (l *masterLink) currentStatus() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.status
}

func (l *masterLink) closeConn() {
	l.mu.Lock()
	if l.conn != nil {
		l.conn.Close()
	}
	l.mu.Unlock()
}

// BecomeReplica 把当前实例挂到 host:port 作为副本（-replicaof 启动参数路径；
// 运行时路径是 REPLICAOF 命令）。重复指向同一主库为幂等 no-op。
func (s *Server) BecomeReplica(host, port string) { s.cmdReplicaOfImpl(host, port) }

// isReplica 报告当前是否处于副本角色（READONLY 拒写门）。
func (s *Server) isReplica() bool {
	s.replMu.Lock()
	defer s.replMu.Unlock()
	return s.master != nil
}

// cmdReplicaOf handles REPLICAOF/SLAVEOF host port | NO ONE.
func (s *Server) cmdReplicaOf(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("replicaof")
	}
	if strings.EqualFold(args[0].Str, "NO") && strings.EqualFold(args[1].Str, "ONE") {
		s.detachMaster()
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	}
	host := strings.TrimSpace(args[0].Str)
	port := args[1].Str
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
		return resp.Value{Type: resp.Error, Str: "ERR Invalid port"}
	}
	if port == s.port() && isLocalHost(host) {
		return errReply(errors.New("ERR can't replicate to self"))
	}
	s.cmdReplicaOfImpl(host, port)
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

func isLocalHost(host string) bool {
	switch strings.ToLower(host) {
	case "", "127.0.0.1", "localhost", "::1", "0.0.0.0":
		return true
	}
	return false
}

// cmdReplicaOfImpl 切换主库（REPLICAOF 命令运行时路径，含事务 EXEC 内执行——
// 因此只取 replMu，不碰 applyMu：execTransaction 全程持有 applyMu，会重入
// 死锁；s.master 的切换不参与命令流传播路径，无需 applyMu 保护）。
func (s *Server) cmdReplicaOfImpl(host, port string) {
	s.replMu.Lock()
	if s.master != nil && s.master.host == host && s.master.port == port {
		s.replMu.Unlock()
		return // 幂等
	}
	old := s.master
	link := &masterLink{host: host, port: port, stop: make(chan struct{}), status: "connect"}
	if s.upstreamID != "" {
		// 带上一次主库凭据发起 PSYNC；id 不匹配时对方回 FULLRESYNC，无害
		link.replid = s.upstreamID
		link.offset.Store(s.upstreamOff)
	}
	s.master = link
	s.replMu.Unlock()
	if old != nil {
		close(old.stop)
		old.closeConn() // 打断阻塞中的 read/write
	}
	go s.replicationLoop(link)
}

// detachMaster 处理 REPLICAOF NO ONE：停止复制循环并晋升为主库（数据保留）。
// 同步凭据存入 upstream：重挂同一主库时可请求部分重同步。
func (s *Server) detachMaster() {
	s.replMu.Lock()
	link := s.master
	s.master = nil
	if link != nil && link.replid != "" {
		s.upstreamID = link.replid
		s.upstreamOff = link.offset.Load()
	}
	s.replMu.Unlock()
	if link != nil {
		close(link.stop)
		link.closeConn()
	}
}

// isDetached reports whether link was already replaced/cleared.
func (s *Server) isDetached(link *masterLink) bool {
	s.replMu.Lock()
	defer s.replMu.Unlock()
	return s.master != link
}

// replicationLoop 以固定退避重连，直到 REPLICAOF NO ONE / 切换主库。
func (s *Server) replicationLoop(link *masterLink) {
	for {
		err := s.syncWithMaster(link)
		if s.isDetached(link) {
			return
		}
		log.Printf("replication: master %s:%s link down: %v (retry in %s)",
			link.host, link.port, err, replRetryDelay)
		link.setStatus("connect")
		select {
		case <-link.stop:
			return
		case <-time.After(replRetryDelay):
		}
	}
}

// syncWithMaster 完成一次握手 + 全量同步 + 命令流回放，直到链路断开。
// 返回非 nil 表示链路中断（外层会重试）。
func (s *Server) syncWithMaster(link *masterLink) error {
	dialAddr := net.JoinHostPort(link.host, link.port)
	conn, err := net.DialTimeout("tcp", dialAddr, replConnectTimeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	link.setConn(conn)
	defer func() {
		link.setConn(nil)
		conn.Close()
	}()
	link.setStatus("sync")

	// -replicaof 启动路径在 Listen goroutine 之后立即挂载：等端口就绪，
	// 保证 REPLCONF listening-port 报告真实端口。
	deadline := time.Now().Add(2 * time.Second)
	for s.addr == "" {
		if time.Now().After(deadline) || s.isDetached(link) {
			return errors.New("listen addr not ready")
		}
		time.Sleep(5 * time.Millisecond)
	}

	r := resp.NewReader(conn)
	// 握手：REPLCONF listening-port → REPLCONF capa → PSYNC <replid> <offset>。
	handshake := func(v resp.Value, what string) error {
		if err := resp.WriteValue(conn, v); err != nil {
			return fmt.Errorf("%s write: %w", what, err)
		}
		rep, err := r.Read()
		if err != nil {
			return fmt.Errorf("%s reply: %w", what, err)
		}
		if rep.Type == resp.Error {
			return fmt.Errorf("%s: %s", what, rep.Str)
		}
		return nil
	}
	if err := handshake(respCmd("REPLCONF", "listening-port", s.port()), "replconf"); err != nil {
		return err
	}
	if err := handshake(respCmd("REPLCONF", "capa", "psync2"), "replconf capa"); err != nil {
		return err
	}
	// PSYNC 凭据：曾同步过则带 id+offset 尝试部分重同步，否则 ? -1（全量）。
	psID, psOff := "?", "-1"
	if link.replid != "" {
		psID = link.replid
		psOff = strconv.FormatInt(link.offset.Load(), 10)
	}
	// PSYNC 没有 +OK 回复：主库的应答就是下一行 +FULLRESYNC / +CONTINUE，
	// 不能走 handshake（否则会把应答当回复吞掉，误把 RDB bulk 当应答）。
	if err := resp.WriteValue(conn, respCmd("PSYNC", psID, psOff)); err != nil {
		return fmt.Errorf("psync write: %w", err)
	}
	rep, err := r.Read()
	if err != nil {
		return fmt.Errorf("psync ack: %w", err)
	}
	if rep.Type != resp.SimpleString {
		if rep.Type == resp.Error {
			return errors.New(rep.Str)
		}
		return fmt.Errorf("unexpected PSYNC reply type %c", rep.Type)
	}
	fields := strings.Fields(rep.Str)
	if len(fields) < 2 {
		return fmt.Errorf("bad PSYNC reply %q", rep.Str)
	}
	partial := false
	switch {
	case fields[0] == "FULLRESYNC" && len(fields) == 3:
		start, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("bad FULLRESYNC offset %q", fields[2])
		}
		link.replid = fields[1]
		link.offset.Store(start) // RDB 快照点即流起点
	case fields[0] == "CONTINUE":
		link.replid = fields[1]
		partial = true // offset 保持请求值，增量流直接续上
	default:
		return fmt.Errorf("unexpected PSYNC reply %q", rep.Str)
	}

	if !partial {
		// 全量 RDB：主库把它作为一个 RESP bulk 值发送（长度前缀 + 二进制安全）。
		rdbVal, err := r.Read()
		if err != nil {
			return fmt.Errorf("rdb read: %w", err)
		}
		if rdbVal.Type != resp.BulkString || rdbVal.Null {
			return fmt.Errorf("expected RDB bulk value, got type %c", rdbVal.Type)
		}
		entries, err := persist.DecodeRDB([]byte(rdbVal.Str))
		if err != nil {
			return fmt.Errorf("rdb decode: %w", err)
		}

		// 原子换库：Flush + 逐条载入；AOF 基线重写也在同一 applyMu 临界区完成
		// （必须先于任何命令流回放，否则重启时「基线之后收到的命令」会被
		// 旧 AOF 内容与新基线的覆写冲掉）。此刻客户端写已被 READONLY 门拦住，
		// 临界区内没有竞争者。
		s.applyMu.Lock()
		s.store.Flush()
		for _, en := range entries {
			s.applyExported(en)
		}
		if s.aof != nil {
			snap := s.store.Snapshot()
			vals := make([]resp.Value, 0, len(snap))
			for _, cmd := range snap {
				if len(cmd) > 0 {
					vals = append(vals, respCmd(cmd...))
				}
			}
			if err := s.aof.Rewrite(vals); err != nil {
				s.applyMu.Unlock()
				return fmt.Errorf("aof baseline rewrite: %w", err)
			}
		}
		s.applyMu.Unlock()
		log.Printf("replication: full sync done from %s:%s (%d keys)",
			link.host, link.port, len(entries))
	} else {
		log.Printf("replication: partial resync with %s:%s at offset %d",
			link.host, link.port, link.offset.Load())
	}
	link.setStatus("online")

	// 命令流回放：串行；回复一律丢弃（复制链路单向）。MULTI...EXEC 按
	// replay() 同款块感知处理，副本端整块原子执行。offset 按帧重编码长度
	// 推进：与主库 propagate 的编码逐字节一致，且不受 bufio 预读影响
	// （socket 级字节数跨帧虚高，重连 PSYNC 会跳帧丢数据）；主库 GETACK
	// 心跳回 REPLCONF ACK（不计 offset：控制帧不进 backlog）。
	// 注意必须复用握手的同一个 resp.Reader：新建 Reader 会丢掉旧 bufio
	// 已预读的字节（部分重同步的增量帧在 +CONTINUE 应答后紧跟着到达，
	// 恰好落在旧 bufio 缓冲里——曾因此丢失整段增量）。
	var block []resp.Value
	inBlock := false
	for {
		v, err := r.Read()
		if err != nil {
			return fmt.Errorf("stream: %w", err)
		}
		name, ok := firstCmd(v)
		if !ok {
			continue
		}
		if name == "REPLCONF" {
			// GETACK 心跳：回 ACK <当前 offset>，不执行不计偏移
			if len(v.Arr) >= 2 && strings.EqualFold(v.Arr[1].Str, "GETACK") {
				ack := respCmd("REPLCONF", "ACK", strconv.FormatInt(link.offset.Load(), 10))
				if err := resp.WriteValue(conn, ack); err != nil {
					return fmt.Errorf("ack write: %w", err)
				}
			}
			continue
		}
		var fb bytes.Buffer
		if err := resp.WriteValue(&fb, v); err != nil {
			return fmt.Errorf("frame size: %w", err)
		}
		link.offset.Add(int64(fb.Len()))
		switch {
		case name == "MULTI":
			inBlock, block = true, block[:0]
		case inBlock && name == "EXEC":
			s.applyMasterBlock(block)
			inBlock = false
		case inBlock:
			block = append(block, v)
		case name == "EXEC" || name == "DISCARD":
			// 无块的散落包装命令：忽略
		default:
			s.applyFromMaster(v)
		}
	}
}

// ackLoop 每秒向所有在线副本发送 REPLCONF GETACK <offset> 心跳，副本回
// ACK 后 INFO slaveN 展示其已确认偏移。由 Listen 启动，随进程存活。
func (s *Server) ackLoop() {
	t := time.NewTicker(replAckInterval)
	defer t.Stop()
	for range t.C {
		off := s.masterOff.Load()
		var b bytes.Buffer
		fmt.Fprintf(&b, "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$%d\r\n%d\r\n",
			len(strconv.FormatInt(off, 10)), off)
		frame := b.Bytes()
		s.replMu.Lock()
		for _, l := range s.replicas {
			if !l.enqueue(frame) {
				l.close()
				delete(s.replicas, l.cl)
			}
		}
		s.replMu.Unlock()
	}
}

// applyFromMaster applies one non-block command received from the master
// stream: executes, logs to AOF (if enabled) and propagates downstream to any
// sub-replicas. Replies are discarded. Not gated by READONLY — this is how
// writes arrive on a replica.
func (s *Server) applyFromMaster(v resp.Value) {
	if !isWriteCmd(v) {
		s.dispatch(v) // 读命令（PING 等）直接执行，不传播
		return
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	setPre := s.setPreState(v)
	reply := s.dispatch(v)
	var frames []resp.Value
	if reply.Type != resp.Error {
		if canon, ok := s.canonicalFor(v, reply, setPre); ok {
			frames = append(frames, canon)
		}
	}
	s.logAndPropagate(frames)
}

// applyMasterBlock replays one MULTI...EXEC block from the master stream as
// one atomic unit (same shape as execTransaction's AOF section).
func (s *Server) applyMasterBlock(block []resp.Value) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	frames := []resp.Value{respCmd("MULTI")}
	for _, q := range block {
		setPre := s.setPreState(q)
		reply := s.dispatch(q)
		if isWriteCmd(q) && reply.Type != resp.Error {
			if canon, ok := s.canonicalFor(q, reply, setPre); ok {
				frames = append(frames, canon)
			}
		}
	}
	frames = append(frames, respCmd("EXEC"))
	s.logAndPropagate(frames)
}

// ---------------------------------------------------------------------------
// 主库端：副本连接 + 命令流 fanout
// ---------------------------------------------------------------------------

// replicaLink 是主库侧每条副本连接的出站队列。apply 临界区只加锁入队，
// 独立 writer goroutine 依次写到 socket；FULLRESYNC + RDB 帧在注册时先行
// 入队，与后续命令流的顺序因此天然正确（同一 writer、同一队列）。
type replicaLink struct {
	cl     *client // 副本连接（replPort 已在 REPLCONF 时记录）
	conn   net.Conn
	remote string // 对端 IP（INFO slaveN 展示）
	port   string // 副本自报的监听端口

	mu      sync.Mutex
	cond    *sync.Cond
	pending [][]byte
	bytes   int64
	closed  bool
	acked   atomic.Int64 // 副本 REPLCONF ACK 上报的偏移（INFO slaveN 展示）
}

func newReplicaLink(cl *client, conn net.Conn) *replicaLink {
	l := &replicaLink{cl: cl, conn: conn, remote: hostOf(conn.RemoteAddr().String()), port: cl.replPort}
	l.cond = sync.NewCond(&l.mu)
	return l
}

func hostOf(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return addr
}

// enqueue appends one frame. false = link closed or over the output-buffer
// limit (caller drops the replica).
func (l *replicaLink) enqueue(frame []byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false
	}
	l.pending = append(l.pending, frame)
	l.bytes += int64(len(frame))
	if l.bytes > replBufLimit {
		return false
	}
	l.cond.Signal()
	return true
}

// next blocks for the next frame; ok=false once closed.
func (l *replicaLink) next() ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for !l.closed && len(l.pending) == 0 {
		l.cond.Wait()
	}
	if l.closed {
		return nil, false
	}
	frame := l.pending[0]
	l.pending = l.pending[1:]
	l.bytes -= int64(len(frame))
	return frame, true
}

func (l *replicaLink) close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.mu.Unlock()
	l.cond.Broadcast()
	l.conn.Close()
}

// writer 序列化地把队列帧写到 socket，直到链路关闭。
func (l *replicaLink) writer() {
	for {
		frame, ok := l.next()
		if !ok {
			return
		}
		if _, err := l.conn.Write(frame); err != nil {
			l.close()
			return
		}
	}
}

// handleReplConf handles REPLCONF <k> <v> [...]: records listening-port for
// INFO, consumes ACK offsets reported by replicas (the reply to our GETACK
// heartbeat), replies +OK to everything else (unknown capa etc., like Redis).
// REPLCONF ACK itself gets no reply (it IS a reply; Redis same).
func (s *Server) handleReplConf(cl *client, args []resp.Value) resp.Value {
	if len(args) >= 2 && strings.EqualFold(args[0].Str, "listening-port") {
		cl.replPort = args[1].Str
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	}
	if len(args) >= 2 && strings.EqualFold(args[0].Str, "ACK") {
		if n, err := strconv.ParseInt(args[1].Str, 10, 64); err == nil && cl.replicaLink != nil {
			cl.replicaLink.acked.Store(n)
		}
		return resp.Value{}
	}
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

// handlePSYNC upgrades a regular connection into a replica connection. Under
// applyMu it either (a) resumes the command stream from the requested offset
// out of the repl-backlog (+CONTINUE, partial resync), or (b) exports + encodes
// the full RDB and registers the link (+FULLRESYNC, full sync). Either way the
// snapshot/resume point and the registration happen in the same critical
// section, so no write command can slip between them. The reply is empty
// because handle() suppresses writes on replica-mode connections.
func (s *Server) handlePSYNC(cl *client, args []resp.Value) resp.Value {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	// 解析 PSYNC <replid> <offset>（SYNC 等价于 PSYNC ? -1）
	wantID, wantOff := "", int64(-1)
	if len(args) == 2 {
		wantID = args[0].Str
		if n, err := strconv.ParseInt(args[1].Str, 10, 64); err == nil {
			wantOff = n
		}
	}
	link := newReplicaLink(cl, cl.conn)
	var hb bytes.Buffer

	// 部分重同步：run id 匹配且 backlog 仍覆盖该 offset → 续传增量字节
	if wantID == s.replID && wantOff >= 0 {
		if delta, ok := s.backlog.since(wantOff); ok {
			fmt.Fprintf(&hb, "+CONTINUE %s\r\n", s.replID)
			hb.Write(delta)
			s.registerReplica(cl, link, &hb)
			log.Printf("replication: partial resync, resumed at offset %d (%d bytes)",
				wantOff, len(delta))
			return resp.Value{}
		}
		log.Printf("replication: offset %d not in backlog, falling back to full sync", wantOff)
	}

	// 全量同步：快照点 = 当前全局偏移；RDB 之后的一切由命令流覆盖
	keys := s.store.Export()
	rdbBytes, err := persist.EncodeRDB(keys)
	if err != nil {
		return errReply(fmt.Errorf("ERR rdb encode: %v", err))
	}
	fmt.Fprintf(&hb, "+FULLRESYNC %s %d\r\n", s.replID, s.backlog.off)
	if err := resp.WriteValue(&hb, resp.Value{Type: resp.BulkString, Str: string(rdbBytes)}); err != nil {
		return errReply(fmt.Errorf("ERR rdb frame: %v", err))
	}
	s.registerReplica(cl, link, &hb)
	return resp.Value{}
}

// registerReplica queues the handshake bytes (+FULLRESYNC/+CONTINUE + optional
// RDB frame), registers the link and starts its writer.
func (s *Server) registerReplica(cl *client, link *replicaLink, hb *bytes.Buffer) {
	link.mu.Lock()
	link.pending = append(link.pending, hb.Bytes())
	link.bytes += int64(hb.Len())
	link.mu.Unlock()

	s.replMu.Lock()
	s.replicas[cl] = link
	s.replMu.Unlock()
	cl.replicaLink = link // dropClient 依赖此字段在断连时摘除链路
	cl.replicaMode = true // handle() 从此静默；REPLCONF 仍被消费但不回写
	go link.writer()
}

// propagate enqueues one canonical command frame on every attached replica,
// and feeds the repl-backlog (the bytes back partial resync). Called under
// applyMu (after the command executed); enqueuing is a cheap mutex-protected
// append, so a slow replica never stalls the write path.
func (s *Server) propagate(v resp.Value) {
	var buf bytes.Buffer
	if err := resp.WriteValue(&buf, v); err != nil {
		return // frame 编码不可能失败（RESP 序列化无 IO）
	}
	frame := buf.Bytes()
	s.backlog.feed(frame) // backlog.off 即全局复制偏移
	s.masterOff.Store(s.backlog.off)
	s.replMu.Lock()
	defer s.replMu.Unlock()
	for _, l := range s.replicas {
		if !l.enqueue(frame) {
			l.close()
			delete(s.replicas, l.cl)
		}
	}
}

// removeReplica detaches one replica link (connection closed / REPLICAOF NO
// ONE on the replica side drops the TCP link and lands here via dropClient).
func (s *Server) removeReplica(l *replicaLink) {
	l.close()
	s.replMu.Lock()
	delete(s.replicas, l.cl)
	s.replMu.Unlock()
}

// logAndPropagate writes each canonical frame to the AOF (when enabled) and
// enqueues it on every replica. Called under applyMu. The first AOF write
// error is returned to the caller for the reply path (same semantics as
// before), but propagation continues so replicas stay consistent with the
// in-memory state (the command did execute locally).
//
// 阻塞弹出投喂收口（Phase 10）：本批帧含 list 推送（LPUSH/RPUSH/LMOVE）时，
// 在同一临界区内按 FIFO 投喂阻塞等待者，其确定性弹出帧（LPOP/RPOP/LMOVE）
// 追加在本批帧之后一并落盘/传播——AOF 帧序 = 推送帧 + 弹出帧，重放与副本
// 状态严格一致。所有调用方都持有 applyMu。
func (s *Server) logAndPropagate(frames []resp.Value) error {
	if extra := s.serveWaitersFrames(frames); len(extra) > 0 {
		frames = append(append([]resp.Value{}, frames...), extra...)
	}
	var firstErr error
	for _, f := range frames {
		if s.aof != nil && firstErr == nil {
			if err := s.aof.Log(f); err != nil {
				firstErr = err // AOF 已坏：停止写盘，继续传播保持副本=内存
			}
		}
		s.propagate(f)
	}
	return firstErr
}

// replicationSection builds the INFO replication block from live state.
func (s *Server) replicationSection() string {
	s.replMu.Lock()
	defer s.replMu.Unlock()
	var b strings.Builder
	b.WriteString("# Replication\r\n")
	// 上游视角：级联中间节点既是副本又是主库，两段信息都输出（对齐 Redis）。
	if s.master == nil {
		b.WriteString("role:master\r\n")
	} else {
		b.WriteString("role:slave\r\n")
		fmt.Fprintf(&b, "master_host:%s\r\n", s.master.host)
		fmt.Fprintf(&b, "master_port:%s\r\n", s.master.port)
		// 对齐真实 Redis 字段值：仅命令流完全在线为 up，握手/全量同步期间为 down
		// （内部 connect/sync/online 三态保留，只在 INFO 展示处映射）。
		st := s.master.currentStatus()
		if st == "online" {
			st = "up"
		} else {
			st = "down"
		}
		fmt.Fprintf(&b, "master_link_status:%s\r\n", st)
	}
	// 下游视角：已挂载的副本（0 个也如实上报）
	fmt.Fprintf(&b, "connected_slaves:%d\r\n", len(s.replicas))
	links := make([]*replicaLink, 0, len(s.replicas))
	for _, l := range s.replicas {
		links = append(links, l)
	}
	sort.Slice(links, func(i, j int) bool { return links[i].remote < links[j].remote })
	for i, l := range links {
		fmt.Fprintf(&b, "slave%d:ip=%s,port=%s,state=online,offset=%d\r\n",
			i, l.remote, l.port, l.acked.Load())
	}
	fmt.Fprintf(&b, "master_replid:%s\r\n", s.replID)
	fmt.Fprintf(&b, "master_repl_offset:%d\r\n", s.masterOff.Load())
	b.WriteString("\r\n")
	return b.String()
}

// newReplID draws a 40-hex-char run id (Redis runid shape); a crypto/rand
// failure falls back to a time-based id (never expected in practice).
func newReplID() string {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%040x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
