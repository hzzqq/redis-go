package store

import (
	"sort"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("k", "v", 0)
	got, ok, err := s.Get("k")
	if err != nil || !ok || got != "v" {
		t.Fatalf("expected (v,true,nil), got (%q,%v,%v)", got, ok, err)
	}
}

func TestMissingKey(t *testing.T) {
	s := New()
	if _, ok, _ := s.Get("nope"); ok {
		t.Fatal("expected missing key")
	}
	if s.Exists("nope") {
		t.Fatal("expected Exists=false for missing key")
	}
}

func TestExpire(t *testing.T) {
	s := New()
	s.Set("k", "v", 50*time.Millisecond)
	if !s.Exists("k") {
		t.Fatal("should exist right after set")
	}
	time.Sleep(90 * time.Millisecond)
	if s.Exists("k") {
		t.Fatal("should have expired")
	}
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("get should miss after expiry")
	}
}

func TestTTL(t *testing.T) {
	s := New()
	s.Set("a", "1", 0)
	if rem, ok := s.TTL("a"); !ok || rem >= 0 {
		t.Fatalf("expected no-expiry (-1), got rem=%d ok=%v", rem, ok)
	}
	s.Set("b", "2", 100*time.Millisecond)
	rem, ok := s.TTL("b")
	if !ok || rem <= 0 {
		t.Fatalf("expected positive ttl, got rem=%d ok=%v", rem, ok)
	}
	if _, ok := s.TTL("missing"); ok {
		t.Fatal("expected missing key ttl exists=false")
	}
}

func TestDelExists(t *testing.T) {
	s := New()
	s.Set("a", "1", 0)
	s.Set("b", "2", 0)
	if !s.Exists("a") {
		t.Fatal("a should exist")
	}
	if !s.Del("a") {
		t.Fatal("del a should return true")
	}
	if s.Exists("a") {
		t.Fatal("a should be gone")
	}
	if s.Del("a") {
		t.Fatal("del missing should return false")
	}
}

func TestExpireCommandSemantics(t *testing.T) {
	s := New()
	s.Set("k", "v", 0)
	if !s.Expire("k", 100*time.Millisecond) {
		t.Fatal("expire on existing key should return true")
	}
	if s.Expire("missing", 100*time.Millisecond) {
		t.Fatal("expire on missing key should return false")
	}
}

// ---------------- Phase 2: Append / IncrBy ----------------

func TestAppendNew(t *testing.T) {
	s := New()
	n, err := s.Append("k", "abc")
	if err != nil || n != 3 {
		t.Fatalf("expected (3,nil), got (%d,%v)", n, err)
	}
	if v, ok, _ := s.Get("k"); !ok || v != "abc" {
		t.Fatalf("expected abc, got %q ok=%v", v, ok)
	}
}

func TestAppendExisting(t *testing.T) {
	s := New()
	s.Set("k", "foo", 0)
	n, err := s.Append("k", "bar")
	if err != nil || n != 6 {
		t.Fatalf("expected (6,nil), got (%d,%v)", n, err)
	}
	if v, ok, _ := s.Get("k"); !ok || v != "foobar" {
		t.Fatalf("expected foobar, got %q ok=%v", v, ok)
	}
}

func TestAppendPreservesTTL(t *testing.T) {
	s := New()
	s.Set("k", "ab", 1000*time.Millisecond)
	_, _ = s.Append("k", "cd")
	rem, ok := s.TTL("k")
	if !ok || rem <= 0 {
		t.Fatalf("TTL should be preserved after APPEND, got rem=%d ok=%v", rem, ok)
	}
}

func TestAppendOnExpired(t *testing.T) {
	s := New()
	s.Set("k", "old", 1*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	// APPEND 应视作新 key（值 = 拼接值，无 TTL）
	if n, err := s.Append("k", "new"); err != nil || n != 3 {
		t.Fatalf("expected (3,nil), got (%d,%v)", n, err)
	}
	if v, ok, _ := s.Get("k"); !ok || v != "new" {
		t.Fatalf("expected 'new', got %q ok=%v", v, ok)
	}
	// 新条目应无 TTL
	if rem, ok := s.TTL("k"); !ok || rem >= 0 {
		t.Fatalf("expected no TTL after APPEND on expired, got rem=%d ok=%v", rem, ok)
	}
}

func TestIncrByNew(t *testing.T) {
	s := New()
	v, err := s.IncrBy("c", 1)
	if err != nil || v != 1 {
		t.Fatalf("expected (1,nil), got (%d,%v)", v, err)
	}
}

func TestIncrByExisting(t *testing.T) {
	s := New()
	s.Set("c", "10", 0)
	v, err := s.IncrBy("c", 5)
	if err != nil || v != 15 {
		t.Fatalf("expected (15,nil), got (%d,%v)", v, err)
	}
}

func TestIncrByNegative(t *testing.T) {
	s := New()
	s.Set("c", "100", 0)
	v, err := s.IncrBy("c", -50)
	if err != nil || v != 50 {
		t.Fatalf("expected (50,nil), got (%d,%v)", v, err)
	}
}

func TestIncrByNonInteger(t *testing.T) {
	s := New()
	s.Set("s", "abc", 0)
	if _, err := s.IncrBy("s", 1); err == nil {
		t.Fatal("expected error on non-integer value")
	}
	// 原值不变
	if v, ok, _ := s.Get("s"); !ok || v != "abc" {
		t.Fatalf("value should be unchanged after error, got %q", v)
	}
}

func TestIncrByOverflow(t *testing.T) {
	s := New()
	s.Set("max", "9223372036854775807", 0) // math.MaxInt64
	if _, err := s.IncrBy("max", 1); err == nil {
		t.Fatal("expected overflow error")
	}
	// 失败时原值不变
	if v, ok, _ := s.Get("max"); !ok || v != "9223372036854775807" {
		t.Fatalf("value should be unchanged after overflow, got %q", v)
	}
}

func TestIncrByUnderflow(t *testing.T) {
	s := New()
	s.Set("min", "-9223372036854775808", 0) // math.MinInt64
	if _, err := s.IncrBy("min", -1); err == nil {
		t.Fatal("expected underflow error")
	}
}

func TestIncrByPreservesTTL(t *testing.T) {
	s := New()
	s.Set("c", "10", 1000*time.Millisecond)
	if _, err := s.IncrBy("c", 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rem, ok := s.TTL("c")
	if !ok || rem <= 0 {
		t.Fatalf("TTL should be preserved after IncrBy, got rem=%d ok=%v", rem, ok)
	}
}

// ---------------- Phase 3: List ----------------

func TestListPushAndLen(t *testing.T) {
	s := New()
	n, err := s.ListPush("l", false, "a", "b", "c") // RPUSH → [a b c]
	if err != nil || n != 3 {
		t.Fatalf("RPUSH: expected (3,nil), got (%d,%v)", n, err)
	}
	n, err = s.ListPush("l", true, "z") // LPUSH → [z a b c]
	if err != nil || n != 4 {
		t.Fatalf("LPUSH: expected (4,nil), got (%d,%v)", n, err)
	}
	// Redis 语义回归：多参数 LPUSH 依次头插，LPUSH l p q → [q p z a b c]
	n, err = s.ListPush("l", true, "p", "q")
	if err != nil || n != 6 {
		t.Fatalf("multi-arg LPUSH: expected (6,nil), got (%d,%v)", n, err)
	}
	got, err := s.ListRange("l", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"q", "p", "z", "a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
	if n, _ := s.ListLen("l"); n != 6 {
		t.Fatalf("expected len 6, got %d", n)
	}
}

func TestListRangeNegativeAndClamp(t *testing.T) {
	s := New()
	s.ListPush("l", false, "a", "b", "c", "d", "e")
	cases := []struct {
		start, stop int64
		want        []string
	}{
		{0, -1, []string{"a", "b", "c", "d", "e"}},
		{-2, -1, []string{"d", "e"}},
		{1, 3, []string{"b", "c", "d"}},
		{0, -100, []string{}}, // stop=-100+5=-5 < start=0 → 空
	}
	for _, c := range cases {
		got, err := s.ListRange("l", c.start, c.stop)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(c.want) {
			t.Fatalf("LRANGE %d %d: expected %v, got %v", c.start, c.stop, c.want, got)
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Fatalf("LRANGE %d %d: expected %v, got %v", c.start, c.stop, c.want, got)
			}
		}
	}
	// 越界：start >= n → 空
	if got, _ := s.ListRange("l", 10, 20); len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
	// 缺失 key → 空
	if got, _ := s.ListRange("nope", 0, -1); len(got) != 0 {
		t.Fatalf("expected empty for missing key, got %v", got)
	}
}

func TestListPop(t *testing.T) {
	s := New()
	s.ListPush("l", false, "a", "b", "c")
	// 单元素 LPOP
	popped, err := s.ListPop("l", true, 1)
	if err != nil || len(popped) != 1 || popped[0] != "a" {
		t.Fatalf("LPOP: expected [a], got %v err=%v", popped, err)
	}
	// 带数量 RPOP
	popped, err = s.ListPop("l", false, 2)
	if err != nil || len(popped) != 2 || popped[0] != "c" || popped[1] != "b" {
		t.Fatalf("RPOP 2: expected [c b], got %v err=%v", popped, err)
	}
	// 弹空后 key 应被删除
	if s.Exists("l") {
		t.Fatal("expected key deleted after popping all elements")
	}
	// 缺失 key 弹出 → 空结果
	if popped, _ := s.ListPop("l", true, 1); len(popped) != 0 {
		t.Fatalf("expected empty pop on missing key, got %v", popped)
	}
	// count=0 → 空结果且不动 key
	s.ListPush("l2", false, "x")
	if popped, _ := s.ListPop("l2", true, 0); len(popped) != 0 || !s.Exists("l2") {
		t.Fatalf("expected count=0 no-op, got %v exists=%v", popped, s.Exists("l2"))
	}
	// count<0 → ErrPopRange
	if _, err := s.ListPop("l2", true, -1); err != ErrPopRange {
		t.Fatalf("expected ErrPopRange, got %v", err)
	}
}

func TestListIndexAndSet(t *testing.T) {
	s := New()
	s.ListPush("l", false, "a", "b", "c")
	if v, ok, _ := s.ListIndex("l", 0); !ok || v != "a" {
		t.Fatalf("LINDEX 0: expected a, got %q ok=%v", v, ok)
	}
	if v, ok, _ := s.ListIndex("l", -1); !ok || v != "c" {
		t.Fatalf("LINDEX -1: expected c, got %q ok=%v", v, ok)
	}
	if _, ok, _ := s.ListIndex("l", 99); ok {
		t.Fatal("LINDEX 99: expected not found")
	}
	if err := s.ListSet("l", 1, "B"); err != nil {
		t.Fatal(err)
	}
	if v, _, _ := s.ListIndex("l", 1); v != "B" {
		t.Fatalf("expected B after LSET, got %q", v)
	}
	if err := s.ListSet("l", 99, "x"); err != ErrIndexOutOfRange {
		t.Fatalf("expected ErrIndexOutOfRange, got %v", err)
	}
	if err := s.ListSet("missing", 0, "x"); err != ErrNoSuchKey {
		t.Fatalf("expected ErrNoSuchKey, got %v", err)
	}
}

func TestListTrim(t *testing.T) {
	s := New()
	s.ListPush("l", false, "a", "b", "c", "d", "e")
	if err := s.ListTrim("l", 1, 3); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ListRange("l", 0, -1)
	if len(got) != 3 || got[0] != "b" || got[2] != "d" {
		t.Fatalf("expected [b c d], got %v", got)
	}
	// 修剪为空 → key 删除
	if err := s.ListTrim("l", 5, 10); err != nil {
		t.Fatal(err)
	}
	if s.Exists("l") {
		t.Fatal("expected key deleted after trim to empty")
	}
	// 缺失 key → 无操作
	if err := s.ListTrim("nope", 0, -1); err != nil {
		t.Fatal(err)
	}
}

func TestListWrongType(t *testing.T) {
	s := New()
	s.Set("k", "str", 0)
	if _, err := s.ListPush("k", true, "v"); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
	if _, err := s.ListLen("k"); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
	if _, err := s.ListRange("k", 0, -1); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
	// list 上做 string 操作
	s.ListPush("l", false, "a")
	if _, _, err := s.Get("l"); err != ErrWrongType {
		t.Fatalf("GET on list: expected ErrWrongType, got %v", err)
	}
	if _, err := s.Append("l", "x"); err != ErrWrongType {
		t.Fatalf("APPEND on list: expected ErrWrongType, got %v", err)
	}
	if _, err := s.IncrBy("l", 1); err != ErrWrongType {
		t.Fatalf("INCRBY on list: expected ErrWrongType, got %v", err)
	}
}

func TestListPreservesTTL(t *testing.T) {
	s := New()
	s.ListPush("l", false, "a")
	if !s.Expire("l", 1000*time.Millisecond) {
		t.Fatal("expire should succeed")
	}
	if _, err := s.ListPush("l", false, "b"); err != nil {
		t.Fatal(err)
	}
	if rem, ok := s.TTL("l"); !ok || rem <= 0 {
		t.Fatalf("TTL should be preserved after ListPush, got rem=%d ok=%v", rem, ok)
	}
}

// ---------------- Phase 3: Hash ----------------

func TestHashSetGetAll(t *testing.T) {
	s := New()
	if n, _ := s.HashSet("h", [][2]string{{"f", "v"}}); n != 1 {
		t.Fatalf("expected 1 added, got %d", n)
	}
	// 已存在字段 → 不计新增
	if n, _ := s.HashSet("h", [][2]string{{"f", "v2"}}); n != 0 {
		t.Fatalf("expected 0 added, got %d", n)
	}
	if n, _ := s.HashSet("h", [][2]string{{"g", "w"}}); n != 1 {
		t.Fatalf("expected 1 added, got %d", n)
	}
	if v, ok, _ := s.HashGet("h", "f"); !ok || v != "v2" {
		t.Fatalf("expected v2, got %q ok=%v", v, ok)
	}
	if _, ok, _ := s.HashGet("h", "nope"); ok {
		t.Fatal("expected missing field")
	}
	// HGETALL 按插入序
	pairs, _ := s.HashGetAll("h")
	if len(pairs) != 2 || pairs[0][0] != "f" || pairs[0][1] != "v2" || pairs[1][0] != "g" || pairs[1][1] != "w" {
		t.Fatalf("unexpected HGETALL order: %v", pairs)
	}
}

func TestHashDelLenExistsKeysVals(t *testing.T) {
	s := New()
	s.HashSet("h", [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	if n, _ := s.HashLen("h"); n != 3 {
		t.Fatalf("expected len 3, got %d", n)
	}
	// 删 2 个（1 个不存在）
	if n, _ := s.HashDel("h", []string{"a", "b", "zz"}); n != 2 {
		t.Fatalf("expected 2 deleted, got %d", n)
	}
	if ok, _ := s.HashExists("h", "a"); ok {
		t.Fatal("a should be gone")
	}
	if ok, _ := s.HashExists("h", "c"); !ok {
		t.Fatal("c should remain")
	}
	keys, _ := s.HashKeys("h")
	if len(keys) != 1 || keys[0] != "c" {
		t.Fatalf("expected [c], got %v", keys)
	}
	vals, _ := s.HashVals("h")
	if len(vals) != 1 || vals[0] != "3" {
		t.Fatalf("expected [3], got %v", vals)
	}
	// 删空 → key 删除
	s.HashDel("h", []string{"c"})
	if s.Exists("h") {
		t.Fatal("expected key deleted after removing last field")
	}
	if n, _ := s.HashLen("h"); n != 0 {
		t.Fatalf("expected len 0 on missing key, got %d", n)
	}
}

func TestHashIncrBy(t *testing.T) {
	s := New()
	// 新 hash + 新 field → delta
	if v, err := s.HashIncrBy("h", "n", 5); err != nil || v != 5 {
		t.Fatalf("expected (5,nil), got (%d,%v)", v, err)
	}
	if v, err := s.HashIncrBy("h", "n", -2); err != nil || v != 3 {
		t.Fatalf("expected (3,nil), got (%d,%v)", v, err)
	}
	// 非整数字段
	s.HashSet("h", [][2]string{{"s", "abc"}})
	if _, err := s.HashIncrBy("h", "s", 1); err == nil {
		t.Fatal("expected error on non-integer field")
	}
	// 溢出
	s.HashSet("h", [][2]string{{"max", "9223372036854775807"}})
	if _, err := s.HashIncrBy("h", "max", 1); err == nil {
		t.Fatal("expected overflow error")
	}
	// hash 上的 TTL 保留
	if !s.Expire("h", 1000*time.Millisecond) {
		t.Fatal("expire should succeed")
	}
	if _, err := s.HashIncrBy("h", "n", 1); err != nil {
		t.Fatal(err)
	}
	if rem, ok := s.TTL("h"); !ok || rem <= 0 {
		t.Fatalf("TTL should be preserved after HashIncrBy, got rem=%d ok=%v", rem, ok)
	}
}

func TestHashWrongType(t *testing.T) {
	s := New()
	s.Set("k", "str", 0)
	if _, err := s.HashSet("k", [][2]string{{"f", "v"}}); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
	if _, _, err := s.HashGet("k", "f"); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
	// hash 上做 string 操作
	s.HashSet("h", [][2]string{{"f", "v"}})
	if _, _, err := s.Get("h"); err != ErrWrongType {
		t.Fatalf("GET on hash: expected ErrWrongType, got %v", err)
	}
}

func TestFlushKeepsSweeper(t *testing.T) {
	s := New()
	s.Set("a", "1", 0)
	s.ListPush("l", false, "x")
	s.HashSet("h", [][2]string{{"f", "v"}})
	s.Flush()
	if s.Len() != 0 {
		t.Fatalf("expected empty store after Flush, got %d", s.Len())
	}
	// Flush 后写入仍可用（sweeper 协程未泄漏中断）
	s.Set("b", "2", 0)
	if v, ok, _ := s.Get("b"); !ok || v != "2" {
		t.Fatalf("expected (2,true), got (%q,%v)", v, ok)
	}
}

// ---------------- Phase 3: Set ----------------

func TestSetAddRemMembersCard(t *testing.T) {
	s := New()
	// 新集合添加 2 个成员
	if n, err := s.SetAdd("s", []string{"a", "b"}); err != nil || n != 2 {
		t.Fatalf("SADD new: expected (2,nil), got (%d,%v)", n, err)
	}
	// 重复成员只计一次
	if n, _ := s.SetAdd("s", []string{"b", "c"}); n != 1 {
		t.Fatalf("SADD dup: expected 1 added, got %d", n)
	}
	if ok, _ := s.SetIsMember("s", "a"); !ok {
		t.Fatal("SISMEMBER a: expected true")
	}
	if ok, _ := s.SetIsMember("s", "zz"); ok {
		t.Fatal("SISMEMBER zz: expected false")
	}
	// SMEMBERS 无序 → 排序后比较
	members, err := s.SetMembers("s")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(members)
	if len(members) != 3 || members[0] != "a" || members[1] != "b" || members[2] != "c" {
		t.Fatalf("SMEMBERS: expected [a b c], got %v", members)
	}
	if n, _ := s.SetCard("s"); n != 3 {
		t.Fatalf("SCARD: expected 3, got %d", n)
	}
	// SREM：删 1 存在 + 1 不存在 → 1
	if n, _ := s.SetRem("s", []string{"a", "zz"}); n != 1 {
		t.Fatalf("SREM: expected 1 removed, got %d", n)
	}
	// 删空 → key 删除
	s.SetRem("s", []string{"b", "c"})
	if s.Exists("s") {
		t.Fatal("expected key deleted after removing all members")
	}
	// 缺失 key：card=0 / members 空 / sismember=false
	if n, _ := s.SetCard("s"); n != 0 {
		t.Fatalf("SCARD missing: expected 0, got %d", n)
	}
	if members, _ := s.SetMembers("s"); len(members) != 0 {
		t.Fatalf("SMEMBERS missing: expected empty, got %v", members)
	}
	if ok, _ := s.SetIsMember("s", "a"); ok {
		t.Fatal("SISMEMBER missing: expected false")
	}
}

func TestSetWrongType(t *testing.T) {
	s := New()
	s.Set("k", "str", 0)
	if _, err := s.SetAdd("k", []string{"v"}); err != ErrWrongType {
		t.Fatalf("SADD on string: expected ErrWrongType, got %v", err)
	}
	s.SetAdd("s", []string{"a"})
	if _, _, err := s.Get("s"); err != ErrWrongType {
		t.Fatalf("GET on set: expected ErrWrongType, got %v", err)
	}
	if _, err := s.Append("s", "x"); err != ErrWrongType {
		t.Fatalf("APPEND on set: expected ErrWrongType, got %v", err)
	}
	if _, err := s.IncrBy("s", 1); err != ErrWrongType {
		t.Fatalf("INCRBY on set: expected ErrWrongType, got %v", err)
	}
	if _, err := s.ListPush("s", false, "x"); err != ErrWrongType {
		t.Fatalf("RPUSH on set: expected ErrWrongType, got %v", err)
	}
	if _, err := s.HashSet("s", [][2]string{{"f", "v"}}); err != ErrWrongType {
		t.Fatalf("HSET on set: expected ErrWrongType, got %v", err)
	}
}

func TestSetTTLPreserved(t *testing.T) {
	s := New()
	s.SetAdd("s", []string{"a"})
	if !s.Expire("s", 1000*time.Millisecond) {
		t.Fatal("expire should succeed")
	}
	if _, err := s.SetAdd("s", []string{"b"}); err != nil {
		t.Fatal(err)
	}
	if rem, ok := s.TTL("s"); !ok || rem <= 0 {
		t.Fatalf("TTL should be preserved after SetAdd, got rem=%d ok=%v", rem, ok)
	}
}

// ---------------- Phase 3: Type / Stats / DBSize ----------------

func TestTypeAndStats(t *testing.T) {
	s := New()
	if got := s.Type("nope"); got != "none" {
		t.Fatalf("TYPE missing: expected none, got %q", got)
	}
	s.Set("k", "v", 0)
	s.ListPush("l", false, "a")
	s.HashSet("h", [][2]string{{"f", "v"}})
	s.SetAdd("s", []string{"a"})
	for key, want := range map[string]string{"k": "string", "l": "list", "h": "hash", "s": "set"} {
		if got := s.Type(key); got != want {
			t.Fatalf("TYPE %s: expected %s, got %s", key, want, got)
		}
	}
	// Stats：4 个无 TTL + 1 个带 TTL
	s.Set("t", "v", time.Hour)
	keys, expires := s.Stats()
	if keys != 5 || expires != 1 {
		t.Fatalf("Stats: expected (5,1), got (%d,%d)", keys, expires)
	}
	if s.DBSize() != 5 {
		t.Fatalf("DBSize: expected 5, got %d", s.DBSize())
	}
	// 过期键不计入 Stats/DBSize
	s.Set("gone", "v", 1*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	keys, _ = s.Stats()
	if keys != 5 {
		t.Fatalf("Stats after expiry: expected 5, got %d", keys)
	}
}

// ---------------- Phase 4: Set 补差（SPOP/SRANDMEMBER/SINTER/SUNION/SDIFF） ----------------

func TestSetPop(t *testing.T) {
	s := New()
	s.SetAdd("s", []string{"a", "b", "c", "d"})
	popped, err := s.SetPop("s", 2)
	if err != nil || len(popped) != 2 {
		t.Fatalf("SetPop 2: got (%v,%v)", popped, err)
	}
	// 弹出的应从集合中消失
	for _, m := range popped {
		if ok, _ := s.SetIsMember("s", m); ok {
			t.Fatalf("member %q should have been popped", m)
		}
	}
	if card, _ := s.SetCard("s"); card != 2 {
		t.Fatalf("card after pop: got %d", card)
	}
	// count 超过基数 → 全弹并删 key
	popped, err = s.SetPop("s", 99)
	if err != nil || len(popped) != 2 {
		t.Fatalf("SetPop overflow: got (%v,%v)", popped, err)
	}
	if s.Exists("s") {
		t.Fatal("emptied set should delete the key")
	}
	// count=0 → 空切片；缺失 key → 空切片；负数 → ErrPopRange
	if popped, _ := s.SetPop("gone", 1); len(popped) != 0 {
		t.Fatalf("SetPop missing: got %v", popped)
	}
	s.SetAdd("s2", []string{"x"})
	if popped, _ := s.SetPop("s2", 0); len(popped) != 0 {
		t.Fatalf("SetPop 0: got %v", popped)
	}
	if _, err := s.SetPop("s2", -1); err != ErrPopRange {
		t.Fatalf("SetPop negative: got %v", err)
	}
	// wrongtype
	s.Set("str", "v", 0)
	if _, err := s.SetPop("str", 1); err != ErrWrongType {
		t.Fatalf("SetPop wrongtype: got %v", err)
	}
}

func TestSetRandMember(t *testing.T) {
	s := New()
	s.SetAdd("s", []string{"a", "b", "c"})
	// 单元素形态：不删除，只随机取一个
	m, err := s.SetRandMember("s", 0, false)
	if err != nil || len(m) != 1 {
		t.Fatalf("SRANDMEMBER single: got (%v,%v)", m, err)
	}
	if ok, _ := s.SetIsMember("s", m[0]); !ok {
		t.Fatalf("SRANDMEMBER must not remove %q", m[0])
	}
	if card, _ := s.SetCard("s"); card != 3 {
		t.Fatalf("SRANDMEMBER must not modify set, card=%d", card)
	}
	// 正数 count：去重、≤count
	for i := 0; i < 20; i++ {
		got, _ := s.SetRandMember("s", 5, true)
		if len(got) != 3 { // count > 基数 → 全集
			t.Fatalf("count>card: expected all 3, got %v", got)
		}
		seen := map[string]bool{}
		for _, g := range got {
			if seen[g] {
				t.Fatalf("positive count must be distinct: %v", got)
			}
			seen[g] = true
		}
	}
	got, _ := s.SetRandMember("s", 2, true)
	if len(got) != 2 {
		t.Fatalf("count=2: got %v", got)
	}
	// 负数 count：恰好 |count| 个、可重复
	got, _ = s.SetRandMember("s", -7, true)
	if len(got) != 7 {
		t.Fatalf("count=-7: expected 7, got %v", got)
	}
	for _, g := range got {
		if ok, _ := s.SetIsMember("s", g); !ok {
			t.Fatalf("drawn member %q not in set", g)
		}
	}
	// 缺失 key → 空数组（单元素与 count 形态）
	if got, _ := s.SetRandMember("nope", 0, false); len(got) != 0 {
		t.Fatalf("single on missing: got %v", got)
	}
	if got, _ := s.SetRandMember("nope", 3, true); len(got) != 0 {
		t.Fatalf("count on missing: got %v", got)
	}
}

func TestSetInterUnionDiff(t *testing.T) {
	s := New()
	s.SetAdd("a", []string{"x", "y", "z"})
	s.SetAdd("b", []string{"y", "z", "w"})
	s.SetAdd("c", []string{"z"})
	eq := func(name string, got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: got %v want %v", name, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s: got %v want %v", name, got, want)
			}
		}
	}
	got, _ := s.SetInter([]string{"a", "b", "c"})
	eq("SINTER", got, []string{"z"})
	got, _ = s.SetUnion([]string{"a", "b"})
	eq("SUNION", got, []string{"w", "x", "y", "z"})
	got, _ = s.SetDiff([]string{"a", "b", "c"})
	eq("SDIFF", got, []string{"x"})
	// 任一 key 缺失：SINTER 为空、SUNION/SDIFF 正常
	got, _ = s.SetInter([]string{"a", "missing"})
	eq("SINTER missing", got, []string{})
	got, _ = s.SetUnion([]string{"a", "missing"})
	eq("SUNION missing", got, []string{"x", "y", "z"})
	got, _ = s.SetDiff([]string{"a", "missing"})
	eq("SDIFF missing", got, []string{"x", "y", "z"})
	// wrongtype 中断
	s.Set("str", "v", 0)
	if _, err := s.SetInter([]string{"a", "str"}); err != ErrWrongType {
		t.Fatalf("SINTER wrongtype: got %v", err)
	}
	if _, err := s.SetUnion([]string{"str"}); err != ErrWrongType {
		t.Fatalf("SUNION wrongtype: got %v", err)
	}
	if _, err := s.SetDiff([]string{"a", "str"}); err != ErrWrongType {
		t.Fatalf("SDIFF wrongtype: got %v", err)
	}
}
