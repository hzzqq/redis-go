package server

import (
	"os"
	"path/filepath"
	"sort"
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

// ---------------- Phase 3: Set ----------------

// wantSimple 断言 reply 是 SimpleString 且 Str == expected。
func wantSimple(t *testing.T, name string, reply resp.Value, expected string) {
	t.Helper()
	if reply.Type != resp.SimpleString || reply.Str != expected {
		t.Fatalf("%s: expected +%s, got type=%c str=%q", name, expected, reply.Type, reply.Str)
	}
}

func TestSetCommands(t *testing.T) {
	s := New()
	wantInt(t, "SADD 2", s.dispatch(mkCmd("SADD", "s", "a", "b")), 2)
	wantInt(t, "SADD dup", s.dispatch(mkCmd("SADD", "s", "b", "c")), 1)
	wantInt(t, "SISMEMBER yes", s.dispatch(mkCmd("SISMEMBER", "s", "a")), 1)
	wantInt(t, "SISMEMBER no", s.dispatch(mkCmd("SISMEMBER", "s", "zz")), 0)
	wantInt(t, "SCARD", s.dispatch(mkCmd("SCARD", "s")), 3)
	// SMEMBERS 无序 → 排序后比较
	reply := s.dispatch(mkCmd("SMEMBERS", "s"))
	if reply.Type != resp.Array {
		t.Fatalf("SMEMBERS: expected Array, got type=%c", reply.Type)
	}
	got := make([]string, 0, len(reply.Arr))
	for _, it := range reply.Arr {
		got = append(got, it.Str)
	}
	sort.Strings(got)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("SMEMBERS: expected [a b c], got %v", got)
	}
	wantInt(t, "SREM", s.dispatch(mkCmd("SREM", "s", "a", "zz")), 1)
	wantInt(t, "SREM empties set", s.dispatch(mkCmd("SREM", "s", "b", "c")), 2)
	wantInt(t, "SCARD after purge", s.dispatch(mkCmd("SCARD", "s")), 0)
	// 缺失 key
	wantInt(t, "SCARD missing", s.dispatch(mkCmd("SCARD", "nope")), 0)
	wantBulkArray(t, "SMEMBERS missing", s.dispatch(mkCmd("SMEMBERS", "nope")), []string{})
	wantInt(t, "SISMEMBER missing", s.dispatch(mkCmd("SISMEMBER", "nope", "a")), 0)
	// 参数错误
	wantErr(t, "SADD too few", s.dispatch(mkCmd("SADD", "k")), "wrong number of arguments")
	wantErr(t, "SREM too few", s.dispatch(mkCmd("SREM", "k")), "wrong number of arguments")
}

// wantNullBulk 断言 reply 是 null bulk string。
func wantNullBulk(t *testing.T, name string, reply resp.Value) {
	t.Helper()
	if reply.Type != resp.BulkString || !reply.Null {
		t.Fatalf("%s: expected null bulk, got type=%c null=%v", name, reply.Type, reply.Null)
	}
}

// ---------------- Phase 4: SPOP / SRANDMEMBER / SINTER / SUNION / SDIFF ----------------

func TestSetExtraCommands(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SADD", "a", "x", "y", "z"))
	s.dispatch(mkCmd("SADD", "b", "y", "z", "w"))
	// SPOP 单元素形态：bulk、且真的弹出
	s.dispatch(mkCmd("SADD", "p", "m1", "m2"))
	reply := s.dispatch(mkCmd("SPOP", "p"))
	if reply.Type != resp.BulkString || reply.Null {
		t.Fatalf("SPOP single: expected bulk, got type=%c", reply.Type)
	}
	wantInt(t, "SCARD after SPOP", s.dispatch(mkCmd("SCARD", "p")), 1)
	// SPOP count 形态：数组；count 超过基数 → 全弹并删 key
	reply = s.dispatch(mkCmd("SPOP", "p", "5"))
	if reply.Type != resp.Array || len(reply.Arr) != 1 {
		t.Fatalf("SPOP 5: expected 1-element array, got type=%c len=%d", reply.Type, len(reply.Arr))
	}
	wantInt(t, "emptied key deleted", s.dispatch(mkCmd("EXISTS", "p")), 0)
	// 缺失 key：单形态 null bulk、count 形态空数组
	wantNullBulk(t, "SPOP missing", s.dispatch(mkCmd("SPOP", "nope")))
	wantBulkArray(t, "SPOP missing count", s.dispatch(mkCmd("SPOP", "nope", "2")), []string{})
	// 负数 count
	wantErr(t, "SPOP negative", s.dispatch(mkCmd("SPOP", "a", "-1")), "must be positive")
	// SRANDMEMBER：不删除
	reply = s.dispatch(mkCmd("SRANDMEMBER", "a"))
	if reply.Type != resp.BulkString || reply.Null {
		t.Fatalf("SRANDMEMBER single: expected bulk, got type=%c", reply.Type)
	}
	wantInt(t, "SRANDMEMBER must not remove", s.dispatch(mkCmd("SCARD", "a")), 3)
	reply = s.dispatch(mkCmd("SRANDMEMBER", "a", "2"))
	if reply.Type != resp.Array || len(reply.Arr) != 2 {
		t.Fatalf("SRANDMEMBER 2: expected 2-element array, got type=%c", reply.Type)
	}
	reply = s.dispatch(mkCmd("SRANDMEMBER", "a", "-5"))
	if reply.Type != resp.Array || len(reply.Arr) != 5 {
		t.Fatalf("SRANDMEMBER -5: expected 5-element array, got len=%d", len(reply.Arr))
	}
	wantNullBulk(t, "SRANDMEMBER missing", s.dispatch(mkCmd("SRANDMEMBER", "nope")))
	wantBulkArray(t, "SRANDMEMBER missing count", s.dispatch(mkCmd("SRANDMEMBER", "nope", "3")), []string{})
	// 集合代数：排序输出
	wantBulkArray(t, "SINTER", s.dispatch(mkCmd("SINTER", "a", "b")), []string{"y", "z"})
	wantBulkArray(t, "SUNION", s.dispatch(mkCmd("SUNION", "a", "b")), []string{"w", "x", "y", "z"})
	wantBulkArray(t, "SDIFF", s.dispatch(mkCmd("SDIFF", "a", "b")), []string{"x"})
	wantBulkArray(t, "SINTER missing", s.dispatch(mkCmd("SINTER", "a", "nope")), []string{})
	wantBulkArray(t, "SUNION missing", s.dispatch(mkCmd("SUNION", "a", "nope")), []string{"x", "y", "z"})
	wantBulkArray(t, "SDIFF missing", s.dispatch(mkCmd("SDIFF", "a", "nope")), []string{"x", "y", "z"})
	s.dispatch(mkCmd("SET", "str2", "v"))
	wantErr(t, "SINTER wrongtype", s.dispatch(mkCmd("SINTER", "a", "str2")), "WRONGTYPE")
}

// ---------------- Phase 4: ZSet ----------------

func TestZSetCommands(t *testing.T) {
	s := New()
	wantInt(t, "ZADD 3", s.dispatch(mkCmd("ZADD", "z", "1", "a", "2", "b", "3", "c")), 3)
	wantInt(t, "ZADD update not counted", s.dispatch(mkCmd("ZADD", "z", "9", "a")), 0)
	// 现在 a=9 b=2 c=3
	wantBulk(t, "ZSCORE a", s.dispatch(mkCmd("ZSCORE", "z", "a")), "9")
	wantNullBulk(t, "ZSCORE missing member", s.dispatch(mkCmd("ZSCORE", "z", "zz")))
	wantNullBulk(t, "ZSCORE missing key", s.dispatch(mkCmd("ZSCORE", "nope", "a")))
	wantBulk(t, "ZINCRBY", s.dispatch(mkCmd("ZINCRBY", "z", "1.5", "b")), "3.5")
	wantInt(t, "ZCARD", s.dispatch(mkCmd("ZCARD", "z")), 3)
	// 排名（升序：b=3.5 在 c=3 之后…… 实际 asc: c(0) b(1) a(2)）
	wantInt(t, "ZRANK c", s.dispatch(mkCmd("ZRANK", "z", "c")), 0)
	wantInt(t, "ZRANK a", s.dispatch(mkCmd("ZRANK", "z", "a")), 2)
	wantInt(t, "ZREVRANK a", s.dispatch(mkCmd("ZREVRANK", "z", "a")), 0)
	wantInt(t, "ZREVRANK c", s.dispatch(mkCmd("ZREVRANK", "z", "c")), 2)
	wantNullBulk(t, "ZRANK missing member", s.dispatch(mkCmd("ZRANK", "z", "zz")))
	// ZCOUNT 边界语法
	wantInt(t, "ZCOUNT full", s.dispatch(mkCmd("ZCOUNT", "z", "-inf", "+inf")), 3)
	wantInt(t, "ZCOUNT inclusive", s.dispatch(mkCmd("ZCOUNT", "z", "3", "9")), 3)
	wantInt(t, "ZCOUNT exclusive", s.dispatch(mkCmd("ZCOUNT", "z", "(3", "9")), 2)
	// ZRANGE / ZREVRANGE（asc: c b a）
	wantBulkArray(t, "ZRANGE 0 -1", s.dispatch(mkCmd("ZRANGE", "z", "0", "-1")), []string{"c", "b", "a"})
	wantBulkArray(t, "ZRANGE 1 2", s.dispatch(mkCmd("ZRANGE", "z", "1", "2")), []string{"b", "a"})
	wantBulkArray(t, "ZRANGE WITHSCORES",
		s.dispatch(mkCmd("ZRANGE", "z", "0", "-1", "WITHSCORES")),
		[]string{"c", "3", "b", "3.5", "a", "9"})
	wantBulkArray(t, "ZREVRANGE 0 1", s.dispatch(mkCmd("ZREVRANGE", "z", "0", "1")), []string{"a", "b"})
	wantBulkArray(t, "ZREVRANGE -2 -1 WITHSCORES",
		s.dispatch(mkCmd("ZREVRANGE", "z", "-2", "-1", "WITHSCORES")),
		[]string{"b", "3.5", "c", "3"})
	wantBulkArray(t, "ZRANGE out of range", s.dispatch(mkCmd("ZRANGE", "z", "10", "20")), []string{})
	// ZREM
	wantInt(t, "ZREM", s.dispatch(mkCmd("ZREM", "z", "b", "zz")), 1)
	wantInt(t, "ZCARD after ZREM", s.dispatch(mkCmd("ZCARD", "z")), 2)
	wantSimple(t, "TYPE zset", s.dispatch(mkCmd("TYPE", "z")), "zset")
	// 弹空删 key
	wantInt(t, "ZREM empties", s.dispatch(mkCmd("ZREM", "z", "a", "c")), 2)
	wantSimple(t, "TYPE after purge", s.dispatch(mkCmd("TYPE", "z")), "none")
	// WRONGTYPE
	s.dispatch(mkCmd("SET", "str", "x"))
	wantErr(t, "ZADD wrongtype", s.dispatch(mkCmd("ZADD", "str", "1", "m")), "WRONGTYPE")
	wantErr(t, "ZRANGE wrongtype", s.dispatch(mkCmd("ZRANGE", "str", "0", "-1")), "WRONGTYPE")
	// 参数与解析错误
	wantErr(t, "ZADD odd args", s.dispatch(mkCmd("ZADD", "z2", "1")), "wrong number of arguments")
	wantErr(t, "ZADD bad float", s.dispatch(mkCmd("ZADD", "z2", "abc", "m")), "not a valid float")
	wantErr(t, "ZINCRBY bad float", s.dispatch(mkCmd("ZINCRBY", "z2", "abc", "m")), "not a valid float")
	wantErr(t, "ZCOUNT bad bound", s.dispatch(mkCmd("ZCOUNT", "z2", "abc", "5")), "min or max is not a float")
	wantErr(t, "ZRANGE bad option", s.dispatch(mkCmd("ZRANGE", "z2", "0", "1", "BOGUS")), "syntax error")
	wantErr(t, "ZREM too few", s.dispatch(mkCmd("ZREM", "z2")), "wrong number of arguments")
}

// TestSPOPAOFRewrite SPOP 是随机命令，落盘必须重写为按实际弹出成员的 SREM；
// 什么都没弹出的 SPOP 不落盘。回放后状态与原库一致。
func TestSPOPAOFRewrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s1, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s1.apply(mkCmd("SADD", "s", "a", "b", "c"))
	// count 形态弹出 2 个
	reply := s1.apply(mkCmd("SPOP", "s", "2"))
	if reply.Type != resp.Array || len(reply.Arr) != 2 {
		t.Fatalf("SPOP 2: expected 2-element array, got type=%c", reply.Type)
	}
	popped := map[string]bool{}
	for _, m := range reply.Arr {
		popped[m.Str] = true
	}
	// 单元素形态弹出剩下的 1 个
	reply = s1.apply(mkCmd("SPOP", "s"))
	if reply.Type != resp.BulkString || reply.Null {
		t.Fatalf("SPOP single: expected bulk, got type=%c", reply.Type)
	}
	popped[reply.Str] = true
	if len(popped) != 3 {
		t.Fatalf("expected to pop all of {a,b,c}, got %v", popped)
	}
	// 未弹出任何东西的 SPOP 不应落盘
	s1.apply(mkCmd("SPOP", "gone"))
	s1.apply(mkCmd("SPOP", "gone", "3"))
	// 弹空后再加一个成员（验证回放顺序）
	s1.apply(mkCmd("SADD", "s", "keep"))
	s1.Close()

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
	if strings.Contains(joined, "SPOP") {
		t.Fatalf("SPOP must never be logged verbatim, got:\n%s", joined)
	}
	if !strings.Contains(joined, "SREM s ") {
		t.Fatalf("SPOP should be rewritten to SREM with popped members, got:\n%s", joined)
	}
	// 回放：SADD a b c → SREM 弹出的 → SADD keep → 只剩 keep
	s2, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantInt(t, "EXISTS s after replay", s2.dispatch(mkCmd("EXISTS", "s")), 1)
	wantBulkArray(t, "SMEMBERS after replay", s2.dispatch(mkCmd("SMEMBERS", "s")), []string{"keep"})
}

func TestSetWrongTypeDispatch(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "k", "v"))
	wantErr(t, "SADD on string", s.dispatch(mkCmd("SADD", "k", "x")), "WRONGTYPE")
	s.dispatch(mkCmd("SADD", "s", "a"))
	wantErr(t, "GET on set", s.dispatch(mkCmd("GET", "s")), "WRONGTYPE")
	wantErr(t, "LPUSH on set", s.dispatch(mkCmd("LPUSH", "s", "x")), "WRONGTYPE")
	wantErr(t, "HSET on set", s.dispatch(mkCmd("HSET", "s", "f", "v")), "WRONGTYPE")
}

// ---------------- Phase 3: TYPE / DBSIZE / INFO / CONFIG ----------------

func TestTypeDispatch(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "k", "v"))
	s.dispatch(mkCmd("RPUSH", "l", "a"))
	s.dispatch(mkCmd("HSET", "h", "f", "v"))
	s.dispatch(mkCmd("SADD", "s", "a"))
	wantSimple(t, "TYPE string", s.dispatch(mkCmd("TYPE", "k")), "string")
	wantSimple(t, "TYPE list", s.dispatch(mkCmd("TYPE", "l")), "list")
	wantSimple(t, "TYPE hash", s.dispatch(mkCmd("TYPE", "h")), "hash")
	wantSimple(t, "TYPE set", s.dispatch(mkCmd("TYPE", "s")), "set")
	wantSimple(t, "TYPE none", s.dispatch(mkCmd("TYPE", "nope")), "none")
	wantErr(t, "TYPE arity", s.dispatch(mkCmd("TYPE")), "wrong number of arguments")
}

func TestDBSizeAndInfo(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "a", "1"))
	s.dispatch(mkCmd("SET", "b", "2", "EX", "1000"))
	s.dispatch(mkCmd("RPUSH", "l", "x"))
	wantInt(t, "DBSIZE", s.dispatch(mkCmd("DBSIZE")), 3)
	wantErr(t, "DBSIZE arity", s.dispatch(mkCmd("DBSIZE", "x")), "wrong number of arguments")
	// INFO 全量
	info := s.dispatch(mkCmd("INFO"))
	if info.Type != resp.BulkString {
		t.Fatalf("INFO: expected BulkString, got type=%c", info.Type)
	}
	for _, want := range []string{
		"# Server", "redis_version:redis-go-", "redis_mode:standalone",
		"tcp_port:6379", "aof_enabled:0", "# Keyspace", "db0:keys=3,expires=1",
	} {
		if !strings.Contains(info.Str, want) {
			t.Fatalf("INFO missing %q in:\n%s", want, info.Str)
		}
	}
	// section 过滤
	sec := s.dispatch(mkCmd("INFO", "keyspace"))
	if !strings.Contains(sec.Str, "# Keyspace") || strings.Contains(sec.Str, "# Server") {
		t.Fatalf("INFO keyspace section filter failed:\n%s", sec.Str)
	}
	if empty := s.dispatch(mkCmd("INFO", "bogus")); empty.Str != "" {
		t.Fatalf("INFO bogus section: expected empty, got %q", empty.Str)
	}
}

func TestConfigCommands(t *testing.T) {
	s := New()
	wantBulkArray(t, "CONFIG GET appendonly", s.dispatch(mkCmd("CONFIG", "GET", "appendonly")),
		[]string{"appendonly", "no"})
	wantBulkArray(t, "CONFIG GET maxmemory", s.dispatch(mkCmd("CONFIG", "GET", "maxmemory")),
		[]string{"maxmemory", "0"})
	wantBulkArray(t, "CONFIG GET unknown", s.dispatch(mkCmd("CONFIG", "GET", "bogus")), []string{})
	wantErr(t, "CONFIG SET", s.dispatch(mkCmd("CONFIG", "SET", "maxmemory", "100")),
		"Unsupported CONFIG parameter")
	wantErr(t, "CONFIG bogus subcommand", s.dispatch(mkCmd("CONFIG", "BOGUS", "x")),
		"Unknown subcommand")
	// AOF 实例：appendonly 如实反映为 yes
	s2, err := NewWithAOF(filepath.Join(t.TempDir(), "x.aof"))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulkArray(t, "CONFIG GET appendonly (aof)", s2.dispatch(mkCmd("CONFIG", "GET", "appendonly")),
		[]string{"appendonly", "yes"})
}
