// Phase 10 阻塞弹出命令测试：immediate/多 key 顺序/WRONGTYPE、timeout 解析
// 与超时回复、推送唤醒（FIFO、AOF 帧序 = 推送帧+弹出帧）、死客户端不投喂、
// MULTI 内非阻塞执行、canonical 形态、LMOVE WATCH 触碰回归修复。
package server

import (
	"bufio"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// newTestClient builds a client on a net.Pipe (probe deadlines work on pipe
// conns); the other end is simply parked — no handle loop runs.
func newTestClient(t *testing.T) *client {
	t.Helper()
	c1, _ := net.Pipe()
	t.Cleanup(func() { c1.Close() })
	return &client{conn: c1, w: bufio.NewWriter(c1), chans: map[string]struct{}{}}
}

func TestBlockingImmediateMultiKeyOrder(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "k1", "a"))
	s.apply(mkCmd("RPUSH", "k2", "b"))
	cl := newTestClient(t)
	reply := s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "miss"},
		{Type: resp.BulkString, Str: "k2"},
		{Type: resp.BulkString, Str: "k1"},
		{Type: resp.BulkString, Str: "0"},
	})
	if reply.Type != resp.Array || len(reply.Arr) != 2 ||
		reply.Arr[0].Str != "k2" || reply.Arr[1].Str != "b" {
		t.Fatalf("expected [k2 b], got %+v", reply)
	}
	// k2 被弹空删 key
	if n, _ := s.store.ListLen("k2"); n != 0 {
		t.Fatalf("k2 should be empty, len=%d", n)
	}
	// BRPOP 尾弹
	s.apply(mkCmd("RPUSH", "k1", "x", "y"))
	reply = s.blockingPop(cl, "BRPOP", []resp.Value{
		{Type: resp.BulkString, Str: "k1"}, {Type: resp.BulkString, Str: "0"},
	})
	if reply.Type != resp.Array || len(reply.Arr) != 2 || reply.Arr[1].Str != "y" {
		t.Fatalf("BRPOP expected tail y, got %+v", reply)
	}
}

func TestBlockingImmediateBRPOPLPUSH(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "src", "v1"))
	cl := newTestClient(t)
	reply := s.blockingPop(cl, "BRPOPLPUSH", []resp.Value{
		{Type: resp.BulkString, Str: "src"},
		{Type: resp.BulkString, Str: "dst"},
		{Type: resp.BulkString, Str: "0"},
	})
	wantBulk(t, "brpoplpush", reply, "v1")
	got, _ := s.store.ListRange("dst", 0, -1)
	if len(got) != 1 || got[0] != "v1" {
		t.Fatalf("dst = %v, want [v1]", got)
	}
}

func TestBlockingErrorsImmediate(t *testing.T) {
	s := New()
	cl := newTestClient(t)
	s.apply(mkCmd("SET", "k", "v")) // WRONGTYPE
	wantErr(t, "wrongtype", s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "1"},
	}), "WRONGTYPE")
	wantErr(t, "negative timeout", s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "-1"},
	}), "timeout is negative")
	wantErr(t, "bad timeout", s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "k"}, {Type: resp.BulkString, Str: "abc"},
	}), "timeout is not a float")
	wantErr(t, "arity", s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "k"},
	}), "wrong number of arguments")
	// 全部 key 为空 + timeout 0.05 → null array
	reply := s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "nope"}, {Type: resp.BulkString, Str: "0.05"},
	})
	if reply.Type != resp.Array || !reply.Null {
		t.Fatalf("timeout must be null array, got %+v", reply)
	}
	// BRPOPLPUSH 超时 → null bulk
	reply = s.blockingPop(cl, "BRPOPLPUSH", []resp.Value{
		{Type: resp.BulkString, Str: "nope"}, {Type: resp.BulkString, Str: "dst"},
		{Type: resp.BulkString, Str: "0.05"},
	})
	if reply.Type != resp.BulkString || !reply.Null {
		t.Fatalf("brpoplpush timeout must be null bulk, got %+v", reply)
	}
}

// TestBlockingWakeByPush 核心闭环：等待者被另一连接的 LPUSH 投喂，AOF 帧序
// = LPUSH + LPOP（重放状态一致），元素恰好被消费。
func TestBlockingWakeByPush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cl := newTestClient(t)

	done := make(chan resp.Value, 1)
	go func() {
		done <- s.blockingPop(cl, "BLPOP", []resp.Value{
			{Type: resp.BulkString, Str: "q"}, {Type: resp.BulkString, Str: "0"},
		})
	}()
	time.Sleep(50 * time.Millisecond) // 让等待者先入队
	s.apply(mkCmd("LPUSH", "q", "hello"))

	select {
	case reply := <-done:
		if reply.Type != resp.Array || len(reply.Arr) != 2 ||
			reply.Arr[0].Str != "q" || reply.Arr[1].Str != "hello" {
			t.Fatalf("expected [q hello], got %+v", reply)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken by push")
	}
	if n, _ := s.store.ListLen("q"); n != 0 {
		t.Fatalf("element should be consumed, len=%d", n)
	}
	// AOF：推送帧 + 确定性弹出帧
	s.Close()
	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 2 {
		t.Fatalf("AOF must hold LPUSH+LPOP, got %d frames", len(cmds))
	}
	if mustFirstCmd(cmds[0]) != "LPUSH" || mustFirstCmd(cmds[1]) != "LPOP" {
		t.Fatalf("AOF frames = %s, %s", mustFirstCmd(cmds[0]), mustFirstCmd(cmds[1]))
	}
}

// waitForQueueLen polls until a key's waiter queue reaches n (makes FIFO
// registration order deterministic across separately-started goroutines).
func waitForQueueLen(t *testing.T, s *Server, key string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.bwMu.Lock()
		got := len(s.blockQ[key])
		s.bwMu.Unlock()
		if got == n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("waiter queue %q never reached %d", key, n)
}

func TestBlockingFIFOAndBRPOPLPUSHWake(t *testing.T) {
	s := New()
	cl1, cl2 := newTestClient(t), newTestClient(t)
	get := func(cl *client) <-chan resp.Value {
		ch := make(chan resp.Value, 1)
		go func() {
			ch <- s.blockingPop(cl, "BRPOPLPUSH", []resp.Value{
				{Type: resp.BulkString, Str: "q"},
				{Type: resp.BulkString, Str: "out"},
				{Type: resp.BulkString, Str: "0"},
			})
		}()
		return ch
	}
	r1 := get(cl1)
	waitForQueueLen(t, s, "q", 1) // 确定性 FIFO：cl1 先入队
	r2 := get(cl2)
	waitForQueueLen(t, s, "q", 2)
	s.apply(mkCmd("RPUSH", "q", "a", "b")) // 一次推送两个元素 → 两个等待者都投喂

	// BRPOPLPUSH 尾弹：每个等待者恰好被投喂一次，FIFO 靠前的拿尾部元素
	select {
	case reply := <-r1:
		if reply.Str != "b" {
			t.Fatalf("waiter1: got %q, want %q", reply.Str, "b")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter1 not served")
	}
	select {
	case reply := <-r2:
		if reply.Str != "a" {
			t.Fatalf("waiter2: got %q, want %q", reply.Str, "a")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter2 not served")
	}
	got, _ := s.store.ListRange("out", 0, -1)
	if strings.Join(got, ",") != "a,b" {
		t.Fatalf("out = %v, want [a b]", got)
	}
}

// TestBlockingAbortedNotFed 等待者已放弃（超时/断连）后推送，元素不得被
// 无主消费。
func TestBlockingAbortedNotFed(t *testing.T) {
	s := New()
	w := &blockWaiter{ch: make(chan struct{}), keys: []string{"q"},
		key: "q", front: true, aborted: true}
	close(w.ch)
	s.bwMu.Lock()
	s.blockQ["q"] = append(s.blockQ["q"], w)
	s.bwMu.Unlock()

	s.apply(mkCmd("RPUSH", "q", "x")) // 投喂路径应跳过 aborted
	if n, _ := s.store.ListLen("q"); n != 1 {
		t.Fatalf("aborted waiter must not consume, len=%d", n)
	}
	// 超时路径注册→注销后同理（空 key 上超时回 null，元素留在 q）
	cl := newTestClient(t)
	if reply := s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "never"}, {Type: resp.BulkString, Str: "0.05"},
	}); !reply.Null {
		t.Fatalf("expected timeout null, got %+v", reply)
	}
	if n, _ := s.store.ListLen("q"); n != 1 {
		t.Fatalf("element must survive the timeout, len=%d", n)
	}
	// 注销后队列已空：推送不再触发任何投喂扫描残留
	s.apply(mkCmd("RPUSH", "q", "y"))
	if n, _ := s.store.ListLen("q"); n != 2 {
		t.Fatalf("len=%d, want 2", n)
	}
}

// TestTxnBlockingImmediate MULTI 内 BLPOP 非阻塞执行（Redis 同语义），块内
// canonical 落 LPOP。
func TestTxnBlockingImmediate(t *testing.T) {
	s := New()
	cl := newTestClient(t)
	r := s.applyConn(cl, mkCmd("MULTI"))
	if r.Str != "OK" {
		t.Fatalf("MULTI: %+v", r)
	}
	s.applyConn(cl, mkCmd("LPUSH", "l", "a", "b"))
	s.applyConn(cl, mkCmd("BLPOP", "l", "empty", "0")) // 有元素 → 立即弹出
	s.applyConn(cl, mkCmd("BLPOP", "missing", "0"))    // 无元素 → null 槽位
	exec := s.applyConn(cl, mkCmd("EXEC"))
	if exec.Type != resp.Array || len(exec.Arr) != 3 {
		t.Fatalf("EXEC shape: %+v", exec)
	}
	slot := exec.Arr[1]
	if slot.Type != resp.Array || len(slot.Arr) != 2 || slot.Arr[1].Str != "b" {
		t.Fatalf("BLPOP slot: %+v", slot)
	}
	if exec.Arr[2].Type != resp.Array || !exec.Arr[2].Null {
		t.Fatalf("empty slot must be null array: %+v", exec.Arr[2])
	}
	// 复制/AOF 形态：canonicalWrite 的确定化（null 不落盘、成功帧确定性）
	{
		// 无 AOF/副本：直接验证 canonicalWrite 的确定化
		canon, ok := canonicalWrite(mkCmd("BLPOP", "l", "0"),
			resp.Value{Type: resp.Array, Arr: []resp.Value{
				{Type: resp.BulkString, Str: "l"}, {Type: resp.BulkString, Str: "b"},
			}}, false)
		if !ok || mustFirstCmd(canon) != "LPOP" || canon.Arr[1].Str != "l" {
			t.Fatalf("BLPOP canon: %+v ok=%v", canon, ok)
		}
		canon, ok = canonicalWrite(mkCmd("BRPOPLPUSH", "s", "d", "0"),
			resp.Value{Type: resp.BulkString, Str: "v"}, false)
		if !ok || mustFirstCmd(canon) != "LMOVE" || canon.Arr[3].Str != "RIGHT" {
			t.Fatalf("BRPOPLPUSH canon: %+v", canon)
		}
		if _, ok = canonicalWrite(mkCmd("BLPOP", "l", "0"),
			resp.Value{Type: resp.Array, Null: true}, false); ok {
			t.Fatal("null BLPOP must not be logged")
		}
	}
}

// TestLMoveWatchKeys regression：LMOVE 必须同时触碰 src 与 dst 的 WATCH
// （此前误取方向词当 key，dst 的观察者永不 abort）。
func TestLMoveWatchKeys(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "src", "v"))
	watcher := newTestClient(t)
	if r := s.cmdWatch(watcher, []resp.Value{
		{Type: resp.BulkString, Str: "dst"},
	}); r.Str != "OK" {
		t.Fatalf("WATCH: %+v", r)
	}
	s.apply(mkCmd("LMOVE", "src", "dst", "LEFT", "RIGHT"))
	if !watcher.dirtyCAS {
		t.Fatal("LMOVE dst change must dirty the WATCHer")
	}
}

// TestBlockingScriptDenied 脚本内禁用阻塞命令与 SORT STORE（Redis 同）。
func TestBlockingScriptDenied(t *testing.T) {
	s := New()
	wantErr(t, "blpop in script",
		s.cmdEval("EVAL", []resp.Value{
			{Type: resp.BulkString, Str: `return redis.call('BLPOP','k',0)`},
			{Type: resp.BulkString, Str: "0"},
		}), "not allowed from script")
	wantErr(t, "sort store in script",
		s.cmdEval("EVAL", []resp.Value{
			{Type: resp.BulkString, Str: `return redis.call('SORT','k','STORE','d')`},
			{Type: resp.BulkString, Str: "0"},
		}), "not allowed from script")
	// 纯读 SORT 放行
	s.apply(mkCmd("RPUSH", "k", "2", "1"))
	if r := s.cmdEval("EVAL", []resp.Value{
		{Type: resp.BulkString, Str: `return redis.call('SORT','k')`},
		{Type: resp.BulkString, Str: "0"},
	}); r.Type != resp.Array {
		t.Fatalf("read SORT from script: %+v", r)
	}
}

// TestBlockingReplicaImmediate 副本上阻塞弹出按读执行（不回 READONLY）。
func TestBlockingReplicaImmediate(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "q", "v")) // 先写后置副本态：apply 的写门会拒绝副本写
	s.replMu.Lock()
	s.master = &masterLink{}
	s.replMu.Unlock()
	defer func() { s.replMu.Lock(); s.master = nil; s.replMu.Unlock() }()

	cl := newTestClient(t)
	reply := s.blockingPop(cl, "BLPOP", []resp.Value{
		{Type: resp.BulkString, Str: "q"}, {Type: resp.BulkString, Str: "1"},
	})
	if reply.Type != resp.Array || len(reply.Arr) != 2 || reply.Arr[1].Str != "v" {
		t.Fatalf("replica BLPOP must serve reads, got %+v", reply)
	}
}
