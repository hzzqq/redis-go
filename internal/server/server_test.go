package server

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// mkCmd 构造一个 RESP 命令数组（cmd + args），便于 dispatch 单测。
func mkCmd(cmd string, args ...string) resp.Value {
	arr := make([]resp.Value, 0, 1+len(args))
	arr = append(arr, resp.Value{Type: resp.BulkString, Str: cmd})
	for _, a := range args {
		arr = append(arr, resp.Value{Type: resp.BulkString, Str: a})
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

// wantInt 断言 reply 是 Integer 且 Num == expected。
func wantInt(t *testing.T, name string, reply resp.Value, expected int64) {
	t.Helper()
	if reply.Type != resp.Integer {
		t.Fatalf("%s: expected Integer, got type=%c str=%q", name, reply.Type, reply.Str)
	}
	if reply.Num != expected {
		t.Fatalf("%s: expected %d, got %d", name, expected, reply.Num)
	}
}

// wantErr 断言 reply 是 Error 且 Str 包含 substr。
func wantErr(t *testing.T, name string, reply resp.Value, substr string) {
	t.Helper()
	if reply.Type != resp.Error {
		t.Fatalf("%s: expected Error, got type=%c str=%q", name, reply.Type, reply.Str)
	}
	if !strings.Contains(reply.Str, substr) {
		t.Fatalf("%s: expected error containing %q, got %q", name, substr, reply.Str)
	}
}

// wantBulk 断言 reply 是 BulkString 且 Str == expected。
func wantBulk(t *testing.T, name string, reply resp.Value, expected string) {
	t.Helper()
	if reply.Type != resp.BulkString {
		t.Fatalf("%s: expected BulkString, got type=%c", name, reply.Type)
	}
	if reply.Str != expected {
		t.Fatalf("%s: expected %q, got %q", name, expected, reply.Str)
	}
}

// ---------------- APPEND ----------------

func TestAppendNewKey(t *testing.T) {
	s := New()
	// 不存在的 key 视作空串拼接，新长度 = 输入长度
	reply := s.dispatch(mkCmd("APPEND", "k", "hello"))
	wantInt(t, "APPEND new", reply, 5)
	// 值应为 "hello"
	got := s.dispatch(mkCmd("GET", "k"))
	wantBulk(t, "GET after APPEND", got, "hello")
}

func TestAppendExistingKey(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "k", "hello"))
	reply := s.dispatch(mkCmd("APPEND", "k", " world"))
	wantInt(t, "APPEND existing", reply, 11)
	got := s.dispatch(mkCmd("GET", "k"))
	wantBulk(t, "GET after APPEND", got, "hello world")
}

func TestAppendWrongArgs(t *testing.T) {
	s := New()
	reply := s.dispatch(mkCmd("APPEND", "only-one-arg"))
	wantErr(t, "APPEND wrong args", reply, "wrong number of arguments")
}

// ---------------- INCR / DECR / INCRBY ----------------

func TestIncrNewKey(t *testing.T) {
	s := New()
	// 不存在的 key 视作 0，INCR 后为 1
	reply := s.dispatch(mkCmd("INCR", "counter"))
	wantInt(t, "INCR new", reply, 1)
	got := s.dispatch(mkCmd("GET", "counter"))
	wantBulk(t, "GET after INCR", got, "1")
}

func TestIncrExistingNumber(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "c", "10"))
	reply := s.dispatch(mkCmd("INCR", "c"))
	wantInt(t, "INCR 10", reply, 11)
}

func TestDecr(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "c", "5"))
	reply := s.dispatch(mkCmd("DECR", "c"))
	wantInt(t, "DECR 5", reply, 4)
}

func TestIncrByPositive(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "c", "100"))
	reply := s.dispatch(mkCmd("INCRBY", "c", "23"))
	wantInt(t, "INCRBY 100+23", reply, 123)
}

func TestIncrByNegative(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "c", "100"))
	reply := s.dispatch(mkCmd("INCRBY", "c", "-50"))
	wantInt(t, "INCRBY 100-50", reply, 50)
}

func TestIncrNonInteger(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "s", "abc"))
	reply := s.dispatch(mkCmd("INCR", "s"))
	wantErr(t, "INCR non-integer", reply, "value is not an integer or out of range")
	// 失败时原值保持不变
	got := s.dispatch(mkCmd("GET", "s"))
	wantBulk(t, "GET after failed INCR", got, "abc")
}

func TestIncrByBadAmount(t *testing.T) {
	s := New()
	reply := s.dispatch(mkCmd("INCRBY", "c", "not-a-number"))
	wantErr(t, "INCRBY bad amount", reply, "value is not an integer or out of range")
}

func TestIncrWrongArgs(t *testing.T) {
	s := New()
	reply := s.dispatch(mkCmd("INCR"))
	wantErr(t, "INCR no args", reply, "wrong number of arguments")
	reply = s.dispatch(mkCmd("INCR", "a", "b"))
	wantErr(t, "INCR too many args", reply, "wrong number of arguments")
}

func TestIncrByWrongArgs(t *testing.T) {
	s := New()
	reply := s.dispatch(mkCmd("INCRBY", "only-key"))
	wantErr(t, "INCRBY missing amount", reply, "wrong number of arguments")
	reply = s.dispatch(mkCmd("INCRBY", "a", "1", "b"))
	wantErr(t, "INCRBY too many args", reply, "wrong number of arguments")
}

// TestIncrOverflow 触发 int64 上溢。
func TestIncrOverflow(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "max", "9223372036854775807")) // math.MaxInt64
	reply := s.dispatch(mkCmd("INCR", "max"))
	wantErr(t, "INCR overflow", reply, "increment or decrement would overflow")
	// 失败时原值不变
	got := s.dispatch(mkCmd("GET", "max"))
	wantBulk(t, "GET after overflow", got, "9223372036854775807")
}

// TestDecrUnderflow 触发 int64 下溢。
func TestDecrUnderflow(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "min", "-9223372036854775808")) // math.MinInt64
	reply := s.dispatch(mkCmd("DECR", "min"))
	wantErr(t, "DECR underflow", reply, "increment or decrement would overflow")
}

// ---------------- APPEND/INCR 与 TTL 交互 ----------------

// TestAppendPreservesTTL APPEND 不应重置已设置的 TTL（与 Redis 一致）。
func TestAppendPreservesTTL(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "k", "ab", "EX", "1000"))
	s.dispatch(mkCmd("APPEND", "k", "cd"))
	rem, ok := s.store.TTL("k")
	if !ok || rem <= 0 {
		t.Fatalf("APPEND should preserve TTL, got rem=%d ok=%v", rem, ok)
	}
	// TTL 应仍接近 1000（允许调度器已扣 1-2 秒，但不应变成 -1）
	if rem < 990 {
		t.Fatalf("TTL dropped too much after APPEND: %d", rem)
	}
	got := s.dispatch(mkCmd("GET", "k"))
	wantBulk(t, "GET after APPEND with TTL", got, "abcd")
}

// TestIncrPreservesTTL INCR 不应重置已设置的 TTL。
func TestIncrPreservesTTL(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "c", "10", "EX", "1000"))
	s.dispatch(mkCmd("INCR", "c"))
	rem, ok := s.store.TTL("c")
	if !ok || rem <= 0 {
		t.Fatalf("INCR should preserve TTL, got rem=%d ok=%v", rem, ok)
	}
	if rem < 990 {
		t.Fatalf("TTL dropped too much after INCR: %d", rem)
	}
}

// TestIncrOnExpiredKey 已过期的 key 应视作不存在（INCR 视作 0 → 1）。
func TestIncrOnExpiredKey(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "c", "99", "EX", "1"))
	time.Sleep(1100 * time.Millisecond)
	// 等 sweeper 或惰性过期：先 GET 触发惰性删除
	if _, ok, _ := s.store.Get("c"); ok {
		t.Fatal("expected key to have expired before INCR")
	}
	reply := s.dispatch(mkCmd("INCR", "c"))
	wantInt(t, "INCR on expired key", reply, 1)
	// 应建立新条目（无 TTL）
	rem, ok := s.store.TTL("c")
	if !ok || rem >= 0 {
		t.Fatalf("expected no TTL after INCR on expired, got rem=%d ok=%v", rem, ok)
	}
}

// ---------------- Phase 3: List ----------------

// wantBulkArray 断言 reply 是 BulkString 数组且元素依序等于 expected。
func wantBulkArray(t *testing.T, name string, reply resp.Value, expected []string) {
	t.Helper()
	if reply.Type != resp.Array {
		t.Fatalf("%s: expected Array, got type=%c", name, reply.Type)
	}
	if len(reply.Arr) != len(expected) {
		t.Fatalf("%s: expected %d elements, got %d (%v)", name, len(expected), len(reply.Arr), reply.Arr)
	}
	for i, want := range expected {
		if reply.Arr[i].Str != want {
			t.Fatalf("%s: element %d expected %q, got %q", name, i, want, reply.Arr[i].Str)
		}
	}
}

func TestListCommands(t *testing.T) {
	s := New()
	wantInt(t, "RPUSH", s.dispatch(mkCmd("RPUSH", "l", "a", "b", "c")), 3)
	wantInt(t, "LPUSH", s.dispatch(mkCmd("LPUSH", "l", "z")), 4)
	wantBulkArray(t, "LRANGE 0 -1", s.dispatch(mkCmd("LRANGE", "l", "0", "-1")),
		[]string{"z", "a", "b", "c"})
	wantInt(t, "LLEN", s.dispatch(mkCmd("LLEN", "l")), 4)
	wantBulk(t, "LINDEX 0", s.dispatch(mkCmd("LINDEX", "l", "0")), "z")
	wantBulk(t, "LINDEX -1", s.dispatch(mkCmd("LINDEX", "l", "-1")), "c")
	// LPOP 单元素 → bulk；带 count → array
	wantBulk(t, "LPOP", s.dispatch(mkCmd("LPOP", "l")), "z")
	wantBulkArray(t, "LPOP 2", s.dispatch(mkCmd("LPOP", "l", "2")), []string{"a", "b"})
	// RPOP count 按弹出序（尾在前）
	wantBulkArray(t, "RPOP 1", s.dispatch(mkCmd("RPOP", "l", "1")), []string{"c"})
	// 弹空后 key 消失
	if got := s.dispatch(mkCmd("LLEN", "l")); got.Num != 0 {
		t.Fatalf("expected LLEN 0 after popping all, got %v", got.Num)
	}
	// 缺失 key：LPOP → null bulk；LPOP k 2 → 空数组
	if got := s.dispatch(mkCmd("LPOP", "nope")); !(got.Type == resp.BulkString && got.Null) {
		t.Fatalf("expected null bulk for LPOP on missing key, got type=%c", got.Type)
	}
	wantBulkArray(t, "LPOP missing 2", s.dispatch(mkCmd("LPOP", "nope", "2")), []string{})
	// LSET / LTRIM
	s.dispatch(mkCmd("RPUSH", "l2", "a", "b", "c"))
	if got := s.dispatch(mkCmd("LSET", "l2", "1", "B")); got.Str != "OK" {
		t.Fatalf("expected OK from LSET, got %q", got.Str)
	}
	wantBulk(t, "LINDEX after LSET", s.dispatch(mkCmd("LINDEX", "l2", "1")), "B")
	wantErr(t, "LSET out of range", s.dispatch(mkCmd("LSET", "l2", "9", "x")), "index out of range")
	wantErr(t, "LSET missing key", s.dispatch(mkCmd("LSET", "nokey", "0", "x")), "no such key")
	if got := s.dispatch(mkCmd("LTRIM", "l2", "0", "1")); got.Str != "OK" {
		t.Fatalf("expected OK from LTRIM, got %q", got.Str)
	}
	wantBulkArray(t, "LRANGE after LTRIM", s.dispatch(mkCmd("LRANGE", "l2", "0", "-1")), []string{"a", "B"})
	// 参数错误
	wantErr(t, "LPUSH too few", s.dispatch(mkCmd("LPUSH", "k")), "wrong number of arguments")
	wantErr(t, "LRANGE arity", s.dispatch(mkCmd("LRANGE", "k", "0")), "wrong number of arguments")
}

func TestGetOnListWrongType(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("RPUSH", "l", "a"))
	wantErr(t, "GET on list", s.dispatch(mkCmd("GET", "l")), "WRONGTYPE")
	wantErr(t, "APPEND on list", s.dispatch(mkCmd("APPEND", "l", "x")), "WRONGTYPE")
	wantErr(t, "INCR on list", s.dispatch(mkCmd("INCR", "l")), "WRONGTYPE")
	s.dispatch(mkCmd("SET", "k", "v"))
	wantErr(t, "LPUSH on string", s.dispatch(mkCmd("LPUSH", "k", "x")), "WRONGTYPE")
	wantErr(t, "HSET on string", s.dispatch(mkCmd("HSET", "k", "f", "v")), "WRONGTYPE")
}

// ---------------- Phase 3: Hash ----------------

func TestHashCommands(t *testing.T) {
	s := New()
	wantInt(t, "HSET new field", s.dispatch(mkCmd("HSET", "h", "f", "v")), 1)
	wantInt(t, "HSET existing field", s.dispatch(mkCmd("HSET", "h", "f", "v2")), 0)
	wantInt(t, "HSET two fields", s.dispatch(mkCmd("HSET", "h", "g", "w", "x", "y")), 2)
	wantBulk(t, "HGET", s.dispatch(mkCmd("HGET", "h", "f")), "v2")
	if got := s.dispatch(mkCmd("HGET", "h", "nope")); !(got.Type == resp.BulkString && got.Null) {
		t.Fatalf("expected null bulk for missing field, got type=%c", got.Type)
	}
	wantInt(t, "HLEN", s.dispatch(mkCmd("HLEN", "h")), 3)
	wantInt(t, "HEXISTS yes", s.dispatch(mkCmd("HEXISTS", "h", "f")), 1)
	wantInt(t, "HEXISTS no", s.dispatch(mkCmd("HEXISTS", "h", "zz")), 0)
	wantBulkArray(t, "HKEYS", s.dispatch(mkCmd("HKEYS", "h")), []string{"f", "g", "x"})
	wantBulkArray(t, "HVALS", s.dispatch(mkCmd("HVALS", "h")), []string{"v2", "w", "y"})
	// HGETALL 平铺 [f v2 g w x y]
	wantBulkArray(t, "HGETALL", s.dispatch(mkCmd("HGETALL", "h")),
		[]string{"f", "v2", "g", "w", "x", "y"})
	// 缺失 key → 空数组
	wantBulkArray(t, "HGETALL missing", s.dispatch(mkCmd("HGETALL", "nope")), []string{})
	// HINCRBY
	wantInt(t, "HINCRBY new", s.dispatch(mkCmd("HINCRBY", "h2", "n", "5")), 5)
	wantInt(t, "HINCRBY existing", s.dispatch(mkCmd("HINCRBY", "h2", "n", "-2")), 3)
	s.dispatch(mkCmd("HSET", "h2", "s", "abc"))
	wantErr(t, "HINCRBY non-integer", s.dispatch(mkCmd("HINCRBY", "h2", "s", "1")), "not an integer")
	// HDEL
	wantInt(t, "HDEL", s.dispatch(mkCmd("HDEL", "h", "f", "g", "zz")), 2)
	wantInt(t, "HDEL last", s.dispatch(mkCmd("HDEL", "h", "x")), 1)
	// 删空后 key 消失
	wantInt(t, "HLEN after purge", s.dispatch(mkCmd("HLEN", "h")), 0)
	// HSET 参数错误（偶数个参数）
	wantErr(t, "HSET odd pairs", s.dispatch(mkCmd("HSET", "h", "f")), "wrong number of arguments")
}

// ---------------- Phase 3: SET PXAT / PEXPIREAT ----------------

func TestSetPxatPastDeletesKey(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "k", "v", "PXAT", "1000")) // 1970 → 立即删除
	if got := s.dispatch(mkCmd("GET", "k")); !(got.Type == resp.BulkString && got.Null) {
		t.Fatalf("expected null bulk after SET PXAT past, got type=%c", got.Type)
	}
}

func TestPExpireAt(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "k", "v"))
	// 过去时间戳 → 删除 key，返回 1
	wantInt(t, "PEXPIREAT past", s.dispatch(mkCmd("PEXPIREAT", "k", "1000")), 1)
	if got := s.dispatch(mkCmd("GET", "k")); !(got.Type == resp.BulkString && got.Null) {
		t.Fatalf("expected key deleted after PEXPIREAT past, got type=%c", got.Type)
	}
	// 未来时间戳 → TTL 为正
	s.dispatch(mkCmd("SET", "k2", "v"))
	future := time.Now().Add(60 * time.Second).UnixMilli()
	wantInt(t, "PEXPIREAT future", s.dispatch(mkCmd("PEXPIREAT", "k2", strconv.FormatInt(future, 10))), 1)
	rem, ok := s.store.TTL("k2")
	if !ok || rem <= 0 || rem > 61 {
		t.Fatalf("expected TTL ~60s, got rem=%d ok=%v", rem, ok)
	}
	// 缺失 key → 0
	wantInt(t, "PEXPIREAT missing", s.dispatch(mkCmd("PEXPIREAT", "nope", "99999999999999")), 0)
}

// ---------------- Phase 3: AOF 持久化 ----------------

// TestCanonicalRewrite 验证 SETEX/EXPIRE/SET EX 落盘为绝对时间戳形式。
func TestCanonicalRewrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s.apply(mkCmd("SET", "a", "1", "EX", "100"))
	s.apply(mkCmd("SETEX", "b", "200", "x"))
	s.apply(mkCmd("EXPIRE", "c", "300")) // c 不存在 → 失败不入 AOF（reply Error）
	s.apply(mkCmd("SET", "c", "3"))
	s.apply(mkCmd("EXPIRE", "c", "300"))
	s.Close()

	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	strs := make([]string, len(cmds))
	for i, v := range cmds {
		parts := make([]string, len(v.Arr))
		for j, item := range v.Arr {
			parts[j] = item.Str
		}
		strs[i] = strings.Join(parts, " ")
	}
	joined := strings.Join(strs, "\n")
	if !strings.Contains(joined, "SET a 1 PXAT ") {
		t.Fatalf("SET EX should be rewritten to SET..PXAT, got:\n%s", joined)
	}
	if !strings.Contains(joined, "SET b x PXAT ") {
		t.Fatalf("SETEX should be rewritten to SET..PXAT, got:\n%s", joined)
	}
	if !strings.Contains(joined, "PEXPIREAT c ") {
		t.Fatalf("EXPIRE should be rewritten to PEXPIREAT, got:\n%s", joined)
	}
	if strings.Contains(joined, "SETEX") || strings.Contains(joined, "EXPIRE c 300") {
		t.Fatalf("relative TTL forms must not be logged, got:\n%s", joined)
	}
	// 读命令不应落盘
	if strings.Contains(joined, "GET") {
		t.Fatalf("read commands must not be logged, got:\n%s", joined)
	}
}

// TestAOFRoundTrip 写入 → 重启回放 → 状态一致（含 TTL 与 list/hash）。
func TestAOFRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")

	s1, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s1.apply(mkCmd("SET", "str", "hello"))
	s1.apply(mkCmd("SET", "exp", "1", "EX", "1000"))
	s1.apply(mkCmd("INCR", "counter"))
	s1.apply(mkCmd("INCR", "counter"))
	s1.apply(mkCmd("RPUSH", "list", "a", "b", "c"))
	s1.apply(mkCmd("LPOP", "list"))
	s1.apply(mkCmd("HSET", "hash", "f1", "v1", "f2", "v2"))
	s1.apply(mkCmd("DEL", "missing")) // 无效变化也落盘，回放后无副作用
	s1.Close()

	s2, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulk(t, "str", s2.dispatch(mkCmd("GET", "str")), "hello")
	wantBulk(t, "counter", s2.dispatch(mkCmd("GET", "counter")), "2")
	wantBulkArray(t, "list", s2.dispatch(mkCmd("LRANGE", "list", "0", "-1")), []string{"b", "c"})
	wantBulkArray(t, "hash", s2.dispatch(mkCmd("HGETALL", "hash")), []string{"f1", "v1", "f2", "v2"})
	// TTL 以绝对时间戳回放：重启后仍为正且接近 1000s
	rem, ok := s2.store.TTL("exp")
	if !ok || rem <= 900 || rem > 1000 {
		t.Fatalf("expected TTL ~1000s after replay, got rem=%d ok=%v", rem, ok)
	}
}

// TestAOFTruncatedTail 容忍崩溃导致的截断尾部：截断前命令全部回放。
func TestAOFTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")

	s1, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s1.apply(mkCmd("SET", "a", "1"))
	s1.apply(mkCmd("SET", "b", "2"))
	s1.Close()

	// 追加半条命令模拟崩溃
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("$5\r\nhel") // 残缺 bulk
	f.Close()

	s2, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulk(t, "a survives truncation", s2.dispatch(mkCmd("GET", "a")), "1")
	wantBulk(t, "b survives truncation", s2.dispatch(mkCmd("GET", "b")), "2")
}
