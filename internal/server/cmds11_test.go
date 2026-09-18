// Phase 11 stream 命令测试：XADD 回复/错误文/NOMKSTREAM、XRANGE 族边界
// 与 0-0 错误、XLEN/XDEL/XTRIM（空流删 key）、XREAD 非阻塞（$、COUNT、
// 多流、unbalanced）、MULTI 内 XREAD 非阻塞、XREAD BLOCK 唤醒/超时/多等
// 待者、canonical XADD 自动 ID 定化、TYPE/OBJECT ENCODING、脚本内 BLOCK
// 禁用。
package server

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

func TestXAddRepliesAndErrors(t *testing.T) {
	s := New()
	// 显式 ID
	r := s.dispatch(mkCmd("XADD", "st", "1-0", "f", "v1"))
	wantBulk(t, "explicit", r, "1-0")
	// 自动 ID：回复为 <ms>-<seq> 且递增
	r = s.dispatch(mkCmd("XADD", "st", "*", "f", "v2"))
	if r.Type != resp.BulkString || !strings.Contains(r.Str, "-") {
		t.Fatalf("auto id reply: %+v", r)
	}
	// 同毫秒连发 seq 递增
	r2 := s.dispatch(mkCmd("XADD", "st", "*", "f", "v3"))
	if r2.Str == r.Str {
		t.Fatalf("auto ids must differ: %s", r.Str)
	}
	// XLEN
	wantInt(t, "xlen", s.dispatch(mkCmd("XLEN", "st")), 3)
	// 错误文
	wantErr(t, "arity", s.dispatch(mkCmd("XADD", "st", "5-0", "f")),
		"wrong number of arguments for 'xadd'")
	wantErr(t, "odd fields", s.dispatch(mkCmd("XADD", "st", "5-0", "f")),
		"wrong number of arguments")
	wantErr(t, "bad id", s.dispatch(mkCmd("XADD", "st", "abc", "f", "v")),
		"Invalid stream ID specified as stream command argument")
	wantErr(t, "zero id", s.dispatch(mkCmd("XADD", "st", "0-0", "f", "v")),
		"greater than 0-0")
	wantErr(t, "non-increasing", s.dispatch(mkCmd("XADD", "st", "1-0", "f", "v")),
		"must be greater than")
	// WRONGTYPE
	s.apply(mkCmd("SET", "str", "x"))
	wantErr(t, "wrongtype", s.dispatch(mkCmd("XADD", "str", "*", "f", "v")), "WRONGTYPE")
	wantInt(t, "xlen missing", s.dispatch(mkCmd("XLEN", "str2")), 0)
	wantErr(t, "wrongtype len", s.dispatch(mkCmd("XLEN", "str")), "WRONGTYPE")
}

// wantIDs 断言 XRANGE/XREAD 行数组的 ID 顺序（测试内公用）。
func wantIDs(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func TestXAddNOMKStream(t *testing.T) {
	s := New()
	r := s.dispatch(mkCmd("XADD", "nope", "NOMKSTREAM", "1-0", "f", "v"))
	if r.Type != resp.BulkString || !r.Null {
		t.Fatalf("NOMKSTREAM missing: %+v", r)
	}
	if s.store.Type("nope") != "none" {
		t.Fatal("key must not be created")
	}
	// 已有流时正常添加
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "v"))
	wantBulk(t, "nomkstream existing", s.dispatch(mkCmd("XADD", "st", "NOMKSTREAM", "2-0", "f", "v")), "2-0")
}

func TestXRangeFamily(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "a"))
	s.dispatch(mkCmd("XADD", "st", "1-5", "f", "b"))
	s.dispatch(mkCmd("XADD", "st", "2-0", "f", "c"))
	s.dispatch(mkCmd("XADD", "st", "3-7", "f", "d"))

	ids := func(v resp.Value) []string {
		if v.Type != resp.Array {
			t.Fatalf("not array: %+v", v)
		}
		out := make([]string, len(v.Arr))
		for i, row := range v.Arr {
			out[i] = row.Arr[0].Str
		}
		return out
	}
	wantIDs(t, "- +", ids(s.dispatch(mkCmd("XRANGE", "st", "-", "+"))), "1-0", "1-5", "2-0", "3-7")
	wantIDs(t, "closed", ids(s.dispatch(mkCmd("XRANGE", "st", "1-5", "2-0"))), "1-5", "2-0")
	wantIDs(t, "excl", ids(s.dispatch(mkCmd("XRANGE", "st", "(1-5", "3-7"))), "2-0", "3-7")
	wantIDs(t, "bare end", ids(s.dispatch(mkCmd("XRANGE", "st", "-", "2"))), "1-0", "1-5", "2-0")
	wantIDs(t, "rev", ids(s.dispatch(mkCmd("XREVRANGE", "st", "+", "-"))), "3-7", "2-0", "1-5", "1-0")
	wantIDs(t, "rev count shape", ids(s.dispatch(mkCmd("XREVRANGE", "st", "2", "1-5"))), "2-0", "1-5")
	wantIDs(t, "count", ids(s.dispatch(mkCmd("XRANGE", "st", "-", "+", "COUNT", "2"))), "1-0", "1-5")
	wantIDs(t, "rev count", ids(s.dispatch(mkCmd("XREVRANGE", "st", "+", "-", "COUNT", "2"))), "3-7", "2-0")
	wantErr(t, "count zero", s.dispatch(mkCmd("XRANGE", "st", "-", "+", "COUNT", "0")),
		"value is out of range, must be positive")
	wantErr(t, "count bad", s.dispatch(mkCmd("XRANGE", "st", "-", "+", "BOGUS", "2")),
		"syntax error")
	wantIDs(t, "empty", ids(s.dispatch(mkCmd("XRANGE", "st", "5-0", "-"))))
	// 字段偶数展开
	row := s.dispatch(mkCmd("XRANGE", "st", "1-0", "1-0"))
	if row.Arr[0].Arr[1].Arr[0].Str != "f" || row.Arr[0].Arr[1].Arr[1].Str != "a" {
		t.Fatalf("field pair: %+v", row)
	}
	// 0-0 端点错误
	wantErr(t, "start 0-0", s.dispatch(mkCmd("XRANGE", "st", "0-0", "+")),
		"start ID must be greater than 0-0")
	wantErr(t, "end 0-0", s.dispatch(mkCmd("XRANGE", "st", "-", "0-0")),
		"end ID must be greater than 0-0")
	// 缺失 key → 空数组
	if v := s.dispatch(mkCmd("XRANGE", "nope", "-", "+")); v.Type != resp.Array || len(v.Arr) != 0 {
		t.Fatalf("missing key: %+v", v)
	}
	// WRONGTYPE
	s.apply(mkCmd("SET", "str", "x"))
	wantErr(t, "wrongtype", s.dispatch(mkCmd("XRANGE", "str", "-", "+")), "WRONGTYPE")
}

func TestXDelXTrim(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "a"))
	s.dispatch(mkCmd("XADD", "st", "2-0", "f", "b"))
	s.dispatch(mkCmd("XADD", "st", "3-0", "f", "c"))
	wantErr(t, "bad id", s.dispatch(mkCmd("XDEL", "st", "zz")),
		"Invalid stream ID")
	wantInt(t, "partial", s.dispatch(mkCmd("XDEL", "st", "2-0", "9-9")), 1)
	wantInt(t, "missing key", s.dispatch(mkCmd("XDEL", "nope", "1-0")), 0)
	// XTRIM MAXLEN
	wantInt(t, "maxlen", s.dispatch(mkCmd("XTRIM", "st", "MAXLEN", "1")), 1)
	entries := s.dispatch(mkCmd("XRANGE", "st", "-", "+"))
	if len(entries.Arr) != 1 || entries.Arr[0].Arr[0].Str != "3-0" {
		t.Fatalf("after maxlen trim: %+v", entries)
	}
	// XTRIM MINID 清空 → key 删除（Redis 7）
	wantInt(t, "minid", s.dispatch(mkCmd("XTRIM", "st", "MINID", "5-0")), 1)
	if s.store.Type("st") != "none" {
		t.Fatal("trimmed-to-empty stream must be deleted")
	}
	// XDEL 清空 → key 删除
	s.dispatch(mkCmd("XADD", "st2", "1-0", "f", "a"))
	wantInt(t, "final del", s.dispatch(mkCmd("XDEL", "st2", "1-0")), 1)
	if s.store.Type("st2") != "none" {
		t.Fatal("XDEL-emptied stream must be deleted")
	}
	// XTRIM 语法错误
	wantErr(t, "xtrim syntax", s.dispatch(mkCmd("XTRIM", "st", "BOGUS", "1")), "syntax error")
	wantErr(t, "xtrim negative", s.dispatch(mkCmd("XTRIM", "st", "MAXLEN", "-1")),
		"The MAXLEN argument must be >= 0")
}

func TestXReadNonBlocking(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("XADD", "a", "1-0", "f", "v1"))
	s.dispatch(mkCmd("XADD", "a", "2-0", "f", "v2"))
	s.dispatch(mkCmd("XADD", "b", "5-0", "f", "w1"))

	// $ → 无新数据 → null array
	r := s.dispatch(mkCmd("XREAD", "STREAMS", "a", "$"))
	if r.Type != resp.Array || !r.Null {
		t.Fatalf("$ must be null: %+v", r)
	}
	// 显式 ID 排他
	r = s.dispatch(mkCmd("XREAD", "STREAMS", "a", "1-0"))
	if r.Type != resp.Array || len(r.Arr) != 1 || len(r.Arr[0].Arr) != 2 {
		t.Fatalf("read shape: %+v", r)
	}
	if r.Arr[0].Arr[0].Str != "a" || r.Arr[0].Arr[1].Arr[0].Arr[0].Str != "2-0" {
		t.Fatalf("exclusive read: %+v", r)
	}
	// 多流 + COUNT
	r = s.dispatch(mkCmd("XREAD", "COUNT", "1", "STREAMS", "a", "b", "0-0", "0-0"))
	if len(r.Arr) != 2 {
		t.Fatalf("multi-stream: %+v", r)
	}
	if r.Arr[0].Arr[0].Str != "a" || len(r.Arr[0].Arr[1].Arr) != 1 {
		t.Fatalf("COUNT cap: %+v", r)
	}
	if r.Arr[1].Arr[0].Str != "b" {
		t.Fatalf("second stream: %+v", r)
	}
	// 混合：有数据的流才出现在回复里（b 无大于 6-0 的条目）
	r = s.dispatch(mkCmd("XREAD", "STREAMS", "a", "b", "1-0", "6-0"))
	if len(r.Arr) != 1 || r.Arr[0].Arr[0].Str != "a" {
		t.Fatalf("only-new stream: %+v", r)
	}
	// 缺失 key
	if r = s.dispatch(mkCmd("XREAD", "STREAMS", "nope", "0-0")); r.Type != resp.Array || !r.Null {
		t.Fatalf("missing key: %+v", r)
	}
	// 错误
	wantErr(t, "unbalanced", s.dispatch(mkCmd("XREAD", "STREAMS", "a")),
		"Unbalanced 'xread' list of streams")
	wantErr(t, "unbalanced odd", s.dispatch(mkCmd("XREAD", "STREAMS", "a", "1-0", "b")),
		"Unbalanced 'xread' list of streams")
	wantErr(t, "bad id", s.dispatch(mkCmd("XREAD", "STREAMS", "a", "zz")),
		"Invalid stream ID")
	wantErr(t, "bad count", s.dispatch(mkCmd("XREAD", "COUNT", "0", "STREAMS", "a", "$")),
		"COUNT must be > 0")
	wantErr(t, "neg block", s.dispatch(mkCmd("XREAD", "BLOCK", "-1", "STREAMS", "a", "$")),
		"timeout is negative")
	// WRONGTYPE 流
	s.apply(mkCmd("SET", "str", "x"))
	wantErr(t, "wrongtype", s.dispatch(mkCmd("XREAD", "STREAMS", "str", "$")), "WRONGTYPE")
}

func TestXReadInMulti(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "v"))
	cl := newTestClient(t)
	s.applyConn(cl, mkCmd("MULTI"))
	s.applyConn(cl, mkCmd("XADD", "st", "2-0", "f", "w"))
	s.applyConn(cl, mkCmd("XREAD", "BLOCK", "0", "STREAMS", "st", "0-0"))
	exec := s.applyConn(cl, mkCmd("EXEC"))
	if exec.Type != resp.Array || len(exec.Arr) != 2 {
		t.Fatalf("EXEC: %+v", exec)
	}
	// XADD 槽位为 id bulk
	if exec.Arr[0].Type != resp.BulkString || exec.Arr[0].Str != "2-0" {
		t.Fatalf("XADD slot: %+v", exec.Arr[0])
	}
	// 事务内 XREAD BLOCK 非阻塞执行：有数据回数据
	slot := exec.Arr[1]
	if slot.Type != resp.Array || len(slot.Arr) != 1 ||
		slot.Arr[0].Arr[1].Arr[0].Arr[0].Str != "1-0" {
		t.Fatalf("XREAD slot: %+v", slot)
	}
}

func waitForXQueueLen(t *testing.T, s *Server, key string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.xwMu.Lock()
		got := len(s.xblockQ[key])
		s.xwMu.Unlock()
		if got == n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("stream waiter queue %q never reached %d", key, n)
}

// TestXReadBlockingWake 核心闭环：XREAD BLOCK 等待者被 XADD 唤醒，回复
// 含新条目；AOF 只落 XADD 帧（流读非消费，无弹出帧）。
func TestXReadBlockingWake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.apply(mkCmd("XADD", "st", "1-0", "f", "old")) // 经 apply 落 AOF
	cl := newTestClient(t)

	done := make(chan resp.Value, 1)
	go func() {
		done <- s.cmdXReadConn(cl, []resp.Value{
			{Type: resp.BulkString, Str: "BLOCK"},
			{Type: resp.BulkString, Str: "0"},
			{Type: resp.BulkString, Str: "STREAMS"},
			{Type: resp.BulkString, Str: "st"},
			{Type: resp.BulkString, Str: "$"},
		})
	}()
	waitForXQueueLen(t, s, "st", 1)
	// 同毫秒内补一条（auto ID 必然 > 快照的 last）
	s.apply(mkCmd("XADD", "st", "*", "f", "new"))

	select {
	case r := <-done:
		if r.Type != resp.Array || len(r.Arr) != 1 || r.Arr[0].Arr[0].Str != "st" {
			t.Fatalf("wake reply: %+v", r)
		}
		if n := len(r.Arr[0].Arr[1].Arr); n != 1 {
			t.Fatalf("expected 1 new entry, got %d", n)
		}
		if r.Arr[0].Arr[1].Arr[0].Arr[1].Arr[1].Str != "new" {
			t.Fatalf("new entry value: %+v", r.Arr[0].Arr[1].Arr[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("XREAD BLOCK not woken by XADD")
	}
	// AOF：两条 XADD（自动 ID 已定化为显式 ID），无任何弹出帧
	s.Close()
	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 2 || mustFirstCmd(cmds[0]) != "XADD" || mustFirstCmd(cmds[1]) != "XADD" {
		t.Fatalf("AOF = %d frames: %s / %s", len(cmds),
			mustFirstCmd(cmds[0]), mustFirstCmd(cmds[len(cmds)-1]))
	}
	if cmds[1].Arr[2].Str == "*" {
		t.Fatalf("auto id must be resolved in AOF frame: %+v", cmds[1])
	}
}

// TestXReadBlockingAllWaiters 流读非消费：同一批新条目唤醒全部等待者。
func TestXReadBlockingAllWaiters(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "old"))
	cl1, cl2 := newTestClient(t), newTestClient(t)
	get := func(cl *client) <-chan resp.Value {
		ch := make(chan resp.Value, 1)
		go func() {
			ch <- s.cmdXReadConn(cl, []resp.Value{
				{Type: resp.BulkString, Str: "BLOCK"},
				{Type: resp.BulkString, Str: "0"},
				{Type: resp.BulkString, Str: "STREAMS"},
				{Type: resp.BulkString, Str: "st"},
				{Type: resp.BulkString, Str: "$"},
			})
		}()
		return ch
	}
	r1, r2 := get(cl1), get(cl2)
	waitForXQueueLen(t, s, "st", 2)
	s.apply(mkCmd("XADD", "st", "*", "f", "new"))
	for i, ch := range []<-chan resp.Value{r1, r2} {
		select {
		case r := <-ch:
			if r.Type != resp.Array || len(r.Arr) != 1 ||
				r.Arr[0].Arr[1].Arr[0].Arr[1].Arr[1].Str != "new" {
				t.Fatalf("waiter%d: %+v", i, r)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter%d not woken", i)
		}
	}
}

func TestXReadBlockingTimeout(t *testing.T) {
	s := New()
	cl := newTestClient(t)
	reply := s.cmdXReadConn(cl, []resp.Value{
		{Type: resp.BulkString, Str: "BLOCK"},
		{Type: resp.BulkString, Str: "50"},
		{Type: resp.BulkString, Str: "STREAMS"},
		{Type: resp.BulkString, Str: "nope"},
		{Type: resp.BulkString, Str: "$"},
	})
	if reply.Type != resp.Array || !reply.Null {
		t.Fatalf("timeout must be null array: %+v", reply)
	}
	// 快路径：已有数据立即返回，不注册等待者
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "v"))
	reply = s.cmdXReadConn(cl, []resp.Value{
		{Type: resp.BulkString, Str: "BLOCK"},
		{Type: resp.BulkString, Str: "1000"},
		{Type: resp.BulkString, Str: "STREAMS"},
		{Type: resp.BulkString, Str: "st"},
		{Type: resp.BulkString, Str: "0-0"},
	})
	if reply.Type != resp.Array || reply.Null || len(reply.Arr) != 1 {
		t.Fatalf("fast path: %+v", reply)
	}
}

// TestXAddCanonicalFrame 自动 ID 的 canonical 形态：显式 ID 落盘、保留
// 修剪选项；NOMKSTREAM null 不落盘；XDEL/XTRIM 原样。
func TestXAddCanonicalFrame(t *testing.T) {
	reply := resp.Value{Type: resp.BulkString, Str: "7-1"}
	canon, ok := canonicalWrite(mkCmd("XADD", "st", "*", "f", "v"), reply, false)
	if !ok || mustFirstCmd(canon) != "XADD" || canon.Arr[2].Str != "7-1" ||
		canon.Arr[3].Str != "f" || canon.Arr[4].Str != "v" {
		t.Fatalf("auto canon: %+v ok=%v", canon, ok)
	}
	// 显式 ID 原样
	canon, _ = canonicalWrite(mkCmd("XADD", "st", "5-0", "f", "v"),
		resp.Value{Type: resp.BulkString, Str: "5-0"}, false)
	if canon.Arr[2].Str != "5-0" {
		t.Fatalf("explicit canon: %+v", canon)
	}
	// 修剪选项保留（回放同状态同效果）
	canon, _ = canonicalWrite(mkCmd("XADD", "st", "MAXLEN", "~", "2", "*", "f", "v"), reply, false)
	if canon.Arr[2].Str != "MAXLEN" || canon.Arr[4].Str != "2" || canon.Arr[5].Str != "7-1" {
		t.Fatalf("trim canon: %+v", canon)
	}
	// NOMKSTREAM null：不落盘
	if _, ok = canonicalWrite(mkCmd("XADD", "st", "NOMKSTREAM", "1-0", "f", "v"),
		resp.Value{Type: resp.BulkString, Null: true}, false); ok {
		t.Fatal("null XADD must not be logged")
	}
	// XDEL/XTRIM 原样
	canon, _ = canonicalWrite(mkCmd("XDEL", "st", "1-0"),
		resp.Value{Type: resp.Integer, Num: 1}, false)
	if mustFirstCmd(canon) != "XDEL" {
		t.Fatalf("XDEL canon: %+v", canon)
	}
	canon, _ = canonicalWrite(mkCmd("XTRIM", "st", "MAXLEN", "10"),
		resp.Value{Type: resp.Integer, Num: 3}, false)
	if mustFirstCmd(canon) != "XTRIM" {
		t.Fatalf("XTRIM canon: %+v", canon)
	}
}

// TestStreamTypeAndEncoding TYPE=stream、OBJECT ENCODING=stream。
func TestStreamTypeAndEncoding(t *testing.T) {
	s := New()
	if r := s.dispatch(mkCmd("TYPE", "st")); r.Type != resp.SimpleString || r.Str != "none" {
		t.Fatalf("type missing: %+v", r)
	}
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "v"))
	if r := s.dispatch(mkCmd("TYPE", "st")); r.Type != resp.SimpleString || r.Str != "stream" {
		t.Fatalf("type stream: %+v", r)
	}
	wantBulk(t, "encoding", s.dispatch(mkCmd("OBJECT", "ENCODING", "st")), "stream")
	// 脚本内 XADD 效果传播（canonical 帧路径）+ 纯读 XREAD 放行
	r := s.cmdEval("EVAL", []resp.Value{
		{Type: resp.BulkString, Str: `return redis.call('XADD','st','2-0','g','w')`},
		{Type: resp.BulkString, Str: "0"},
	})
	wantBulk(t, "eval xadd", r, "2-0")
	r = s.cmdEval("EVAL", []resp.Value{
		{Type: resp.BulkString, Str: `return redis.call('XREAD','STREAMS','st','1-0')`},
		{Type: resp.BulkString, Str: "0"},
	})
	if r.Type != resp.Array || len(r.Arr) != 1 {
		t.Fatalf("read XREAD from script: %+v", r)
	}
	wantErr(t, "xread block in script", s.cmdEval("EVAL", []resp.Value{
		{Type: resp.BulkString, Str: `return redis.call('XREAD','BLOCK','0','STREAMS','st','$')`},
		{Type: resp.BulkString, Str: "0"},
	}), "not allowed from script")
}
