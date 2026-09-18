package server

// MULTI/EXEC/DISCARD 事务的连接级语义必须走真实 TCP 验证：
// 事务状态机位于 applyConn（per-connection state），dispatch 单测无法触达。

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// readReply 从连接读一条回复，失败即 Fatal。
func readReply(t *testing.T, name string, r *resp.Reader) resp.Value {
	t.Helper()
	v, err := r.Read()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}

// wantNull 断言 reply 是 null bulk string（事务测试里的缺 key 检查）。
func wantNull(t *testing.T, name string, v resp.Value) {
	t.Helper()
	if v.Type != resp.BulkString || !v.Null {
		t.Fatalf("%s: expected null bulk, got %#v", name, v)
	}
}

// TestTransactionHappyPath MULTI→QUEUED→EXEC：结果合成数组，状态生效，
// EXEC 后事务状态复位。
func TestTransactionHappyPath(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	send(t, c, "MULTI")
	wantSimple(t, "MULTI", readReply(t, "MULTI", r), "OK")
	send(t, c, "SET", "k", "v")
	wantSimple(t, "SET queued", readReply(t, "SET", r), "QUEUED")
	send(t, c, "INCR", "ctr")
	wantSimple(t, "INCR queued", readReply(t, "INCR", r), "QUEUED")
	send(t, c, "RPUSH", "l", "a", "b")
	wantSimple(t, "RPUSH queued", readReply(t, "RPUSH", r), "QUEUED")
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 3 {
		t.Fatalf("EXEC: expected 3-element array, got %#v", v)
	}
	wantSimple(t, "EXEC[0] SET", v.Arr[0], "OK")
	wantInt(t, "EXEC[1] INCR", v.Arr[1], 1)
	wantInt(t, "EXEC[2] RPUSH", v.Arr[2], 2)

	// EXEC 之后事务已复位：普通命令照常，再 EXEC 报 without MULTI
	send(t, c, "GET", "k")
	wantBulk(t, "GET after EXEC", readReply(t, "GET", r), "v")
	send(t, c, "EXEC")
	wantErr(t, "EXEC after EXEC", readReply(t, "EXEC2", r), "EXEC without MULTI")
}

// TestTransactionQueueErrorAborts 队列期错误（未知命令/参数个数）立即报错并
// 污染事务，EXEC 变为 EXECABORT，已排队的命令一条都不执行。
func TestTransactionQueueErrorAborts(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "k", "v")
	wantSimple(t, "SET queued", readReply(t, "SET", r), "QUEUED")
	send(t, c, "NOSUCHCMD", "x")
	wantErr(t, "unknown cmd queued", readReply(t, "NOSUCHCMD", r),
		"ERR unknown command 'NOSUCHCMD'")
	send(t, c, "GET") // 参数个数错误，同样污染
	wantErr(t, "arity error queued", readReply(t, "GET", r),
		"wrong number of arguments for 'get' command")
	send(t, c, "EXEC")
	wantErr(t, "EXECABORT", readReply(t, "EXEC", r),
		"EXECABORT Transaction discarded because of previous errors.")

	// 已排队的 SET 不得生效
	send(t, c, "GET", "k")
	wantNull(t, "queued SET must not run", readReply(t, "GET k", r))
}

// TestTransactionExecutionErrorInSlot 执行期错误只占据 EXEC 结果的对应槽位，
// 其余命令照常生效；AOF 块只包含成功的写命令。
func TestTransactionExecutionErrorInSlot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	send(t, c, "SET", "str", "abc")
	readReply(t, "SET str", r)
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "ok1", "x")
	readReply(t, "q1", r)
	send(t, c, "INCR", "str") // 非整数串 → 执行期错误（进槽位，不污染其余）
	readReply(t, "q2", r)
	send(t, c, "SET", "ok2", "y")
	readReply(t, "q3", r)
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 3 {
		t.Fatalf("EXEC: expected 3 slots, got %#v", v)
	}
	wantSimple(t, "slot0", v.Arr[0], "OK")
	wantErr(t, "slot1", v.Arr[1], "value is not an integer or out of range")
	wantSimple(t, "slot2", v.Arr[2], "OK")
	send(t, c, "GET", "ok2")
	wantBulk(t, "ok2 applied", readReply(t, "GET ok2", r), "y")
	send(t, c, "GET", "str")
	wantBulk(t, "str untouched", readReply(t, "GET str", r), "abc")

	s.Close()
	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	// [SET str abc, MULTI, SET ok1 x, SET ok2 y, EXEC] —— 失败的 INCR 不落盘
	if len(cmds) != 5 {
		t.Fatalf("expected 5 AOF entries, got %v", cmds)
	}
	if cmds[1].Arr[0].Str != "MULTI" || cmds[4].Arr[0].Str != "EXEC" {
		t.Fatalf("expected MULTI..EXEC block, got %v", cmds)
	}
	if cmds[2].Arr[1].Str != "ok1" || cmds[3].Arr[1].Str != "ok2" {
		t.Fatalf("unexpected block content: %v", cmds)
	}
}

// TestDiscard DISCARD 清空队列放弃事务，事务状态复位。
func TestDiscard(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "k", "v")
	readReply(t, "queued", r)
	send(t, c, "DISCARD")
	wantSimple(t, "DISCARD", readReply(t, "DISCARD", r), "OK")
	send(t, c, "GET", "k")
	wantNull(t, "queued command dropped", readReply(t, "GET k", r))
	send(t, c, "EXEC")
	wantErr(t, "EXEC after DISCARD", readReply(t, "EXEC", r), "EXEC without MULTI")
}

// TestExecDiscardWithoutMulti 未开事务直接 EXEC/DISCARD 报错。
func TestExecDiscardWithoutMulti(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	send(t, c, "EXEC")
	wantErr(t, "EXEC without MULTI", readReply(t, "EXEC", r), "ERR EXEC without MULTI")
	send(t, c, "DISCARD")
	wantErr(t, "DISCARD without MULTI", readReply(t, "DISCARD", r), "ERR DISCARD without MULTI")
}

// TestNestedMulti 嵌套 MULTI 报错但不污染事务（Redis 同语义）。
func TestNestedMulti(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "MULTI")
	wantErr(t, "nested MULTI", readReply(t, "MULTI2", r), "ERR MULTI calls can not be nested")
	// 事务未被污染，照常排队执行
	send(t, c, "SET", "k", "v")
	wantSimple(t, "still queues", readReply(t, "SET", r), "QUEUED")
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 1 || v.Arr[0].Str != "OK" {
		t.Fatalf("nested MULTI must not poison the txn, got %#v", v)
	}
}

// TestSubscribeNotAllowedInTxn 事务内 SUBSCRIBE 被拒且不污染事务。
func TestSubscribeNotAllowedInTxn(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SUBSCRIBE", "ch")
	wantErr(t, "SUBSCRIBE in txn", readReply(t, "SUBSCRIBE", r),
		"ERR subscribe is not allowed in transactions")
	send(t, c, "SET", "k", "v")
	wantSimple(t, "still queues", readReply(t, "SET", r), "QUEUED")
	send(t, c, "EXEC")
	v := readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 1 || v.Arr[0].Str != "OK" {
		t.Fatalf("rejected SUBSCRIBE must not poison the txn, got %#v", v)
	}
}

// TestQuitDiscardsTxn QUIT 不入队：清掉未执行的事务并断连。
func TestQuitDiscardsTxn(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "k", "v")
	readReply(t, "queued", r)
	send(t, c, "QUIT")
	wantSimple(t, "QUIT", readReply(t, "QUIT", r), "OK")
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := r.Read(); err == nil {
		t.Fatal("connection should be closed after QUIT")
	}
	// 事务没有执行
	c2, r2 := dialRaw(t, addr)
	send(t, c2, "GET", "k")
	wantNull(t, "QUIT must discard the txn", readReply(t, "GET k", r2))
}

// TestTransactionAOFRoundTrip 事务以 MULTI..EXEC 块落盘、重启整块回放；
// 空事务不落盘；崩溃留下的未闭合块整体丢弃（等价于事务未发生）。
func TestTransactionAOFRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s1, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	addr := startPubSubServer(t, s1)
	c, r := dialRaw(t, addr)

	// 空事务：回复空数组，不产生任何 AOF 记录
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "EXEC")
	v := readReply(t, "empty EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 0 {
		t.Fatalf("empty transaction: expected empty array, got %#v", v)
	}
	send(t, c, "SET", "pre", "1")
	readReply(t, "SET pre", r)

	// 正常事务块
	send(t, c, "MULTI")
	readReply(t, "MULTI", r)
	send(t, c, "SET", "a", "1")
	readReply(t, "q1", r)
	send(t, c, "INCR", "n")
	readReply(t, "q2", r)
	send(t, c, "RPUSH", "l", "x", "y")
	readReply(t, "q3", r)
	send(t, c, "EXEC")
	v = readReply(t, "EXEC", r)
	if v.Type != resp.Array || len(v.Arr) != 3 {
		t.Fatalf("EXEC: expected 3 slots, got %#v", v)
	}
	s1.Close()

	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	// [SET pre 1, MULTI, SET a 1, INCR n, RPUSH l x y, EXEC]
	if len(cmds) != 6 || cmds[1].Arr[0].Str != "MULTI" || cmds[5].Arr[0].Str != "EXEC" {
		t.Fatalf("unexpected AOF content (%d entries): %v", len(cmds), cmds)
	}

	// 追加一条完整事务 + 一条截断的未闭合事务（模拟崩溃于写入中途）
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	for _, cmd := range [][]string{
		{"MULTI"}, {"SET", "x", "9"}, {"EXEC"},
	} {
		if err := resp.WriteValue(f, mkCmd(cmd[0], cmd[1:]...)); err != nil {
			t.Fatal(err)
		}
	}
	f.WriteString("*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nk") // 半条命令
	f.Close()

	s2, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulk(t, "pre", s2.dispatch(mkCmd("GET", "pre")), "1")
	wantBulk(t, "a", s2.dispatch(mkCmd("GET", "a")), "1")
	wantBulk(t, "n", s2.dispatch(mkCmd("GET", "n")), "1")
	wantBulkArray(t, "l", s2.dispatch(mkCmd("LRANGE", "l", "0", "-1")), []string{"x", "y"})
	wantBulk(t, "x", s2.dispatch(mkCmd("GET", "x")), "9")
	wantNull(t, "truncated block discarded", s2.dispatch(mkCmd("GET", "k")))
}
