package server

// WATCH/UNWATCH 乐观锁：连接级语义（CAS dirty 标记），必须走真实 TCP 验证。

import (
	"testing"

	"github.com/hzzqq/redis-go/internal/resp"
)

// TestWatchCASAborts 监视的 key 被其他连接修改 → EXEC 返回 null array
// （*-1，Redis 同款），队列整体放弃（一条不执行）。
func TestWatchCASAborts(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	other, ro := dialRaw(t, addr)

	send(t, c, "SET", "k", "1")
	readReply(t, "SET k", r)
	send(t, c, "WATCH", "k")
	wantSimple(t, "WATCH", readReply(t, "WATCH", r), "OK")

	send(t, other, "SET", "k", "2")
	readReply(t, "other SET", ro)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "k", "9")
	wantSimple(t, "queued", readReply(t, "SET", r), "QUEUED")
	send(t, c, "INCR", "ctr")
	wantSimple(t, "queued", readReply(t, "INCR", r), "QUEUED")
	send(t, c, "EXEC")
	wantNullArray(t, "EXEC after touched", readReply(t, "EXEC", r))

	// 队列一条都没执行
	send(t, c, "GET", "k")
	wantBulk(t, "k untouched by txn", readReply(t, "GET k", r), "2")
	send(t, c, "GET", "ctr")
	wantNull(t, "ctr absent", readReply(t, "GET ctr", r))
}

// TestWatchUnmodifiedCommits 其他 key 被改不影响本事务，EXEC 正常提交；
// 提交后 WATCH 自动取消。
func TestWatchUnmodifiedCommits(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	other, ro := dialRaw(t, addr)

	send(t, c, "WATCH", "k", "k2")
	readReply(t, "WATCH", r)
	send(t, other, "SET", "unrelated", "x")
	readReply(t, "other SET", ro)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "k", "v")
	readReply(t, "q", r)
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 1 {
		t.Fatalf("EXEC should commit, got %#v", v)
	}
	// EXEC 后 WATCH 已取消：再改 k 不影响下一个事务
	send(t, other, "SET", "k", "changed")
	readReply(t, "other SET2", ro)
	send(t, c, "MULTI")
	readReply(t, "MULTI2", r)
	send(t, c, "SET", "n", "1")
	readReply(t, "q2", r)
	send(t, c, "EXEC")
	v = readReply(t, "EXEC2", r)
	if v.Type != resp.Array || len(v.Arr) != 1 {
		t.Fatalf("second EXEC should commit, got %#v", v)
	}
}

// TestWatchSelfOutsideWriteAborts 事务外自己的写同样触发 abort（Redis 语义：
// WATCH 之后任何对该 key 的修改都算，包括自己非事务的写）。
func TestWatchSelfOutsideWriteAborts(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	send(t, c, "WATCH", "k")
	readReply(t, "WATCH", r)
	send(t, c, "SET", "k", "self") // 事务外自己的写
	readReply(t, "SET", r)
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "k", "txn")
	readReply(t, "q", r)
	send(t, c, "EXEC")
	wantNullArray(t, "self write aborts", readReply(t, "EXEC", r))
}

// TestWatchInsideMultiRejected MULTI 内 WATCH 报错（不入队、不污染事务）。
func TestWatchInsideMultiRejected(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "WATCH", "k")
	wantErr(t, "WATCH in MULTI", readReply(t, "WATCH", r),
		"ERR WATCH inside MULTI is not allowed")
	send(t, c, "SET", "k", "v")
	wantSimple(t, "still queues", readReply(t, "SET", r), "QUEUED")
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 1 {
		t.Fatalf("rejected WATCH must not poison the txn, got %#v", v)
	}
}

// TestUnwatchClears UNWATCH 取消全部 watch：之后被改也不 abort。
func TestUnwatchClears(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	other, ro := dialRaw(t, addr)

	send(t, c, "WATCH", "k")
	readReply(t, "WATCH", r)
	send(t, c, "UNWATCH")
	wantSimple(t, "UNWATCH", readReply(t, "UNWATCH", r), "OK")
	send(t, other, "SET", "k", "2")
	readReply(t, "other SET", ro)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "n", "1")
	readReply(t, "q", r)
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 1 {
		t.Fatalf("UNWATCH must let the txn commit, got %#v", v)
	}
}

// TestDiscardClearsWatch DISCARD 一并取消 WATCH（Redis 同语义）：
// discard 后 key 再被改，新事务照常提交。
func TestDiscardClearsWatch(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	other, ro := dialRaw(t, addr)

	send(t, c, "WATCH", "k")
	readReply(t, "WATCH", r)
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "a", "1")
	readReply(t, "q", r)
	send(t, c, "DISCARD")
	wantSimple(t, "DISCARD", readReply(t, "DISCARD", r), "OK")

	send(t, other, "SET", "k", "2")
	readReply(t, "other SET", ro)
	send(t, c, "MULTI")
	readReply(t, "MULTI2", r)
	send(t, c, "SET", "b", "1")
	readReply(t, "q2", r)
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 1 {
		t.Fatalf("DISCARD must clear watches, got %#v", v)
	}
}

// TestWatchFlushAllAborts FLUSHALL 触碰所有被监视 key。
func TestWatchFlushAllAborts(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	other, ro := dialRaw(t, addr)

	send(t, c, "WATCH", "k")
	readReply(t, "WATCH", r)
	send(t, other, "FLUSHALL")
	readReply(t, "other FLUSHALL", ro)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "n", "1")
	readReply(t, "q", r)
	send(t, c, "EXEC")
	wantNullArray(t, "FLUSHALL aborts", readReply(t, "EXEC", r))
}

// TestWatchMissingKeyAborts 监视不存在的 key：被创建后 EXEC 也 abort
// （DEL 触碰偏保守：缺 key 的写也 abort，绝不漏 abort）。
func TestWatchMissingKeyAborts(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	other, ro := dialRaw(t, addr)

	send(t, c, "WATCH", "ghost")
	readReply(t, "WATCH", r)
	send(t, other, "SET", "ghost", "born")
	readReply(t, "other SET", ro)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "n", "1")
	readReply(t, "q", r)
	send(t, c, "EXEC")
	wantNullArray(t, "created watched key aborts", readReply(t, "EXEC", r))
}
