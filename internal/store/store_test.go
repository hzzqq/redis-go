package store

import (
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	s := New()
	s.Set("k", "v", 0)
	got, ok := s.Get("k")
	if !ok || got != "v" {
		t.Fatalf("expected (v,true), got (%q,%v)", got, ok)
	}
}

func TestMissingKey(t *testing.T) {
	s := New()
	if _, ok := s.Get("nope"); ok {
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
	if _, ok := s.Get("k"); ok {
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
	if n := s.Append("k", "abc"); n != 3 {
		t.Fatalf("expected len=3, got %d", n)
	}
	if v, ok := s.Get("k"); !ok || v != "abc" {
		t.Fatalf("expected abc, got %q ok=%v", v, ok)
	}
}

func TestAppendExisting(t *testing.T) {
	s := New()
	s.Set("k", "foo", 0)
	if n := s.Append("k", "bar"); n != 6 {
		t.Fatalf("expected len=6, got %d", n)
	}
	if v, ok := s.Get("k"); !ok || v != "foobar" {
		t.Fatalf("expected foobar, got %q ok=%v", v, ok)
	}
}

func TestAppendPreservesTTL(t *testing.T) {
	s := New()
	s.Set("k", "ab", 1000*time.Millisecond)
	_ = s.Append("k", "cd")
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
	if n := s.Append("k", "new"); n != 3 {
		t.Fatalf("expected len=3, got %d", n)
	}
	if v, ok := s.Get("k"); !ok || v != "new" {
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
	if v, ok := s.Get("s"); !ok || v != "abc" {
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
	if v, ok := s.Get("max"); !ok || v != "9223372036854775807" {
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
