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
	if !s.Expire("missing", 100*time.Millisecond) {
		t.Fatal("expire on missing key should return false")
	}
}
