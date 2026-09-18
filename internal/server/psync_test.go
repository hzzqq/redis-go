package server

// 部分重同步（Phase 8）单测：repl-backlog 环形缓冲的单元语义 + raw RESP
// 客户端模拟副本走 PSYNC 全量/部分/退化三条路径 + GETACK 心跳闭环。
// 协议语义都在连接与 goroutine 层，沿用 replication_test 的真实 TCP 验证方式。

import (
	"bytes"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
)

// TestReplBacklogRing repl-backlog 的 feed/since 环形语义：整体可取、尾段
// 可取、覆盖后失效、超大帧作废整个缓冲。
func TestReplBacklogRing(t *testing.T) {
	b := newReplBacklog()
	if data, ok := b.since(0); !ok || len(data) != 0 {
		t.Fatalf("empty since(0): ok=%v len=%d", ok, len(data))
	}
	b.feed([]byte("hello"))
	b.feed([]byte("world!"))
	if data, ok := b.since(0); !ok || string(data) != "helloworld!" {
		t.Fatalf("since(0) = %q ok=%v", data, ok)
	}
	if data, ok := b.since(5); !ok || string(data) != "world!" {
		t.Fatalf("since(5) = %q ok=%v", data, ok)
	}
	if _, ok := b.since(b.off + 1); ok {
		t.Fatal("since(ahead of off) should fail")
	}
	// 环回覆盖：1.25 MiB 写入 > 1 MiB 缓冲，最旧字节被覆盖
	big := make([]byte, 64<<10)
	for i := range big {
		big[i] = byte(i)
	}
	for i := 0; i < 20; i++ {
		b.feed(big)
	}
	if _, ok := b.since(0); ok {
		t.Fatal("since(0) after overwrite should fail")
	}
	if data, ok := b.since(b.off - int64(len(big))); !ok || !bytes.Equal(data, big) {
		t.Fatalf("since(last frame): ok=%v len=%d", ok, len(data))
	}
	// 超大帧（≥ 缓冲）：不作保留，backlog 作废（副本退化为全量）
	b.feed(make([]byte, replBacklogSize))
	if _, ok := b.since(b.off - int64(len(big))); ok {
		t.Fatal("big frame should invalidate backlog")
	}
	if data, ok := b.since(b.off); !ok || len(data) != 0 {
		t.Fatalf("since(off) after invalidate: ok=%v len=%d", ok, len(data))
	}
}

// rawReplica 是模拟副本的 raw 连接：按「帧重编码长度」推进 offset（与
// syncWithMaster 的计量方式一致，不受 bufio 预读影响），GETACK 心跳回 ACK。
type rawReplica struct {
	t   *testing.T
	c   net.Conn
	r   *resp.Reader
	off int64
}

// readFrame 从复制流读一帧命令；GETACK 控制帧不计数（与副本实现一致）。
func (rr *rawReplica) readFrame() resp.Value {
	rr.t.Helper()
	rr.c.SetDeadline(time.Now().Add(3 * time.Second))
	for {
		v, err := rr.r.Read()
		if err != nil {
			rr.t.Fatalf("stream read: %v", err)
		}
		if name, ok := firstCmd(v); ok && name == "REPLCONF" {
			if err := resp.WriteValue(rr.c, respCmd("REPLCONF", "ACK",
				strconv.FormatInt(rr.off, 10))); err != nil {
				rr.t.Fatalf("ack write: %v", err)
			}
			continue
		}
		var fb bytes.Buffer
		if err := resp.WriteValue(&fb, v); err != nil {
			rr.t.Fatalf("re-encode frame: %v", err)
		}
		rr.off += int64(fb.Len())
		return v
	}
}

// psyncDial 完成副本握手（REPLCONF listening-port/capa + PSYNC）并解析应答：
// 返回 raw 连接、run id、是否部分重同步、RDB 内容（仅全量路径非空）。
func psyncDial(t *testing.T, addr, psID, psOff string) (*rawReplica, string, bool, string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	rr := &rawReplica{t: t, c: c, r: resp.NewReader(c)}
	// 升级为副本连接前 REPLCONF 有 +OK 回复
	for _, kv := range [][2]string{{"listening-port", "7777"}, {"capa", "psync2"}} {
		if err := resp.WriteValue(c, respCmd("REPLCONF", kv[0], kv[1])); err != nil {
			t.Fatalf("replconf write: %v", err)
		}
		rep, err := rr.r.Read()
		if err != nil || rep.Type != resp.SimpleString || rep.Str != "OK" {
			t.Fatalf("replconf %s: rep=%#v err=%v", kv[0], rep, err)
		}
	}
	// PSYNC 的应答就是下一行 +FULLRESYNC / +CONTINUE（没有 +OK）
	if err := resp.WriteValue(c, respCmd("PSYNC", psID, psOff)); err != nil {
		t.Fatalf("psync write: %v", err)
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	rep, err := rr.r.Read()
	if err != nil {
		t.Fatalf("psync reply: %v", err)
	}
	c.SetDeadline(time.Time{})
	if rep.Type != resp.SimpleString {
		t.Fatalf("psync reply: expected simple string, got %#v", rep)
	}
	f := strings.Fields(rep.Str)
	switch f[0] {
	case "FULLRESYNC":
		if len(f) != 3 {
			t.Fatalf("FULLRESYNC fields: %q", rep.Str)
		}
		off, err := strconv.ParseInt(f[2], 10, 64)
		if err != nil {
			t.Fatalf("FULLRESYNC offset %q: %v", f[2], err)
		}
		rr.off = off // 快照点即流起点
		bulk, err := rr.r.Read()
		if err != nil || bulk.Type != resp.BulkString || bulk.Null {
			t.Fatalf("rdb bulk: %#v err=%v", bulk, err)
		}
		return rr, f[1], false, bulk.Str
	case "CONTINUE":
		if len(f) != 2 {
			t.Fatalf("CONTINUE fields: %q", rep.Str)
		}
		// 副本语义：部分重同步后 offset 保持请求值，增量帧从续上
		if n, err := strconv.ParseInt(psOff, 10, 64); err == nil && n >= 0 {
			rr.off = n
		}
		return rr, f[1], true, ""
	}
	t.Fatalf("unexpected PSYNC reply %q", rep.Str)
	return nil, "", false, ""
}

// masterOffset 读主库 INFO replication 的 master_repl_offset。
func masterOffset(t *testing.T, masterAddr string) int64 {
	t.Helper()
	m := fetchInfo(t, masterAddr, "replication")
	n, err := strconv.ParseInt(m["master_repl_offset"], 10, 64)
	if err != nil {
		t.Fatalf("master_repl_offset: %v (%v)", m["master_repl_offset"], m)
	}
	return n
}

// TestPSYNCFullThenPartialResync 全量同步 → 记录 offset → 断开 → 期间主库
// 继续写 → 重连 PSYNC <id> <offset> → +CONTINUE 增量续传恰为断线期间的帧，
// 且副本累计 offset 与主库 master_repl_offset 收敛（双端计量一致）。
func TestPSYNCFullThenPartialResync(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	mConn, mR := dialRaw(t, masterAddr)
	write := func(k, v string) {
		t.Helper()
		send(t, mConn, "SET", k, v)
		wantSimple(t, "SET "+k, readReply(t, "SET "+k, mR), "OK")
	}

	// 第一阶段：PSYNC ? -1 → 全量（+FULLRESYNC + RDB bulk）
	rr, replid, partial, rdb := psyncDial(t, masterAddr, "?", "-1")
	if partial {
		t.Fatal("first sync should be FULLRESYNC")
	}
	if len(rdb) == 0 {
		t.Fatal("first sync RDB bulk empty")
	}
	write("a", "1")
	write("b", "2")
	for i, want := range []string{"a", "b"} {
		f := rr.readFrame()
		if name, _ := firstCmd(f); name != "SET" || f.Arr[1].Str != want {
			t.Fatalf("stream frame %d = %#v (want SET %s)", i, f, want)
		}
	}
	discoOff := rr.off
	waitUntil(t, "master offset == replica offset", func() bool {
		return masterOffset(t, masterAddr) == discoOff
	})

	// 断开 → 期间写入只进 backlog → 重连请求部分重同步
	rr.c.Close()
	write("c", "3")
	write("d", "4")
	rr2, replid2, partial2, rdb2 := psyncDial(t, masterAddr, replid,
		strconv.FormatInt(discoOff, 10))
	if !partial2 {
		t.Fatal("resync should be CONTINUE (partial)")
	}
	if rdb2 != "" {
		t.Fatalf("partial resync must not carry RDB, got %d bytes", len(rdb2))
	}
	if replid2 != replid {
		t.Fatalf("replid changed: %s != %s", replid2, replid)
	}
	for i, want := range []string{"c", "d"} {
		f := rr2.readFrame()
		if name, _ := firstCmd(f); name != "SET" || f.Arr[1].Str != want {
			t.Fatalf("delta frame %d = %#v (want SET %s)", i, f, want)
		}
	}
	waitUntil(t, "resync offset converges", func() bool {
		return masterOffset(t, masterAddr) == rr2.off
	})
}

// TestPSYNCFallbacks 部分重同步退化路径：run id 不匹配 / offset 超前 /
// offset 为负 → 一律回 +FULLRESYNC + RDB bulk。
func TestPSYNCFallbacks(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	mConn, mR := dialRaw(t, masterAddr)
	send(t, mConn, "SET", "k", "v")
	readReply(t, "SET", mR)

	// 先做一次全量拿到真实 replid
	rr0, replid, _, _ := psyncDial(t, masterAddr, "?", "-1")
	if got := masterOffset(t, masterAddr); got == 0 {
		t.Fatal("master_repl_offset should advance after a write")
	}

	// 错误 run id → 全量
	rr1, _, partial1, rdb1 := psyncDial(t, masterAddr,
		"0123456789012345678901234567890123456789", "0")
	if partial1 || len(rdb1) == 0 {
		t.Fatalf("wrong replid: want full sync, partial=%v rdb=%d", partial1, len(rdb1))
	}
	// 正确 run id 但 offset 超前 → 全量
	rr2, _, partial2, rdb2 := psyncDial(t, masterAddr, replid, "999999999")
	if partial2 || len(rdb2) == 0 {
		t.Fatalf("ahead offset: want full sync, partial=%v rdb=%d", partial2, len(rdb2))
	}
	// offset 为负（非法）→ 全量
	rr3, _, partial3, rdb3 := psyncDial(t, masterAddr, replid, "-5")
	if partial3 || len(rdb3) == 0 {
		t.Fatalf("negative offset: want full sync, partial=%v rdb=%d", partial3, len(rdb3))
	}
	rr0.c.Close()
	rr1.c.Close()
	rr2.c.Close()
	rr3.c.Close()
}

// TestReplicationAckHeartbeat GETACK/ACK 心跳闭环：主库每秒发 GETACK，副本
// 回 ACK 上报偏移，INFO slaveN 的 offset 与 master_repl_offset 收敛。
func TestReplicationAckHeartbeat(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	replica := New()
	replicaAddr := startPubSubServer(t, replica)
	rConn, rR := dialRaw(t, replicaAddr)
	attachReplica(t, rConn, rR, masterAddr)
	waitOnline(t, replicaAddr)

	mConn, mR := dialRaw(t, masterAddr)
	send(t, mConn, "SET", "k", "v")
	wantSimple(t, "SET", readReply(t, "SET", mR), "OK")

	waitUntil(t, "slave ack offset converges", func() bool {
		m := fetchInfo(t, masterAddr, "replication")
		slave, ok := m["slave0"]
		return ok && strings.Contains(slave, "offset="+m["master_repl_offset"])
	})
}
