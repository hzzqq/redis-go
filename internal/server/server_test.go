package server

import (
	"strings"
	"testing"
	"time"

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
	if _, ok := s.store.Get("c"); ok {
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
