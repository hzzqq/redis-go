// Package store is an in-memory key/value store with TTL support.
package store

import (
	"sync"
	"time"
)

type entry struct {
	value  string
	expiry time.Time // zero value means "no expiry"
}

// Store is a concurrent in-memory KV store with lazy + periodic expiry.
type Store struct {
	mu sync.RWMutex
	m  map[string]entry
}

// New creates a Store and starts a background sweeper.
func New() *Store {
	s := &Store{m: make(map[string]entry)}
	go s.sweeper()
	return s
}

func (s *Store) sweeper() {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for now := range t.C {
		s.mu.Lock()
		for k, e := range s.m {
			if !e.expiry.IsZero() && now.After(e.expiry) {
				delete(s.m, k)
			}
		}
		s.mu.Unlock()
	}
}

// Get returns the value and whether it exists (expired keys are treated as missing).
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	if !e.expiry.IsZero() && time.Now().After(e.expiry) {
		s.Del(key)
		return "", false
	}
	return e.value, true
}

// Set stores val with an optional TTL (ttl<=0 means no expiry).
func (s *Store) Set(key, val string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	s.m[key] = entry{value: val, expiry: exp}
}

// Del removes key; reports whether it existed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		return true
	}
	return false
}

// Exists reports whether key is present and not expired.
func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	if !e.expiry.IsZero() && time.Now().After(e.expiry) {
		s.Del(key)
		return false
	}
	return true
}

// Expire sets a TTL on key; reports whether key existed.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok {
		return false
	}
	if ttl <= 0 {
		delete(s.m, key)
		return true
	}
	e.expiry = time.Now().Add(ttl)
	s.m[key] = e
	return true
}

// TTL returns the remaining TTL.
//   - exists == false : key missing
//   - rem  < 0        : key exists but has no expiry
//   - rem  >= 1       : remaining time in seconds（向上取整，对齐 Redis TTL 语义）
func (s *Store) TTL(key string) (rem int64, exists bool) {
	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()
	if !ok {
		return 0, false
	}
	if e.expiry.IsZero() {
		return -1, true
	}
	left := time.Until(e.expiry)
	if left <= 0 {
		s.Del(key)
		return 0, false
	}
	// 剩余秒数向上取整（Redis 语义：TTL 对不足 1 秒的剩余时间返回 1）
	return int64((left + time.Second - 1) / time.Second), true
}

// Len returns the current number of keys (approximate under concurrency).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}
