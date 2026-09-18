// Package store is an in-memory key/value store with TTL support.
package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

// 哨兵错误：INCR/DECR/INCRBY 遇到非整数值或溢出时返回。
// 文本与 redis 的 "ERR value is not an integer or out of range" / "ERR increment or decrement would overflow" 对齐。
var (
	errIncrementNotInteger = errors.New("ERR value is not an integer or out of range")
	errIncrementOverflow    = errors.New("ERR increment or decrement would overflow")
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

// Append 原子地把 val 拼到 key 末尾，返回拼接后的新长度。
// 不存在的 key 视作空串。保留原 expiry（已过期/不存在则建立无 TTL 的新条目）。
// 与 Redis 一致：APPEND 不会重置 TTL，但对已过期 key 会先惰性删除再建立新条目。
func (s *Store) Append(key, val string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok || (!e.expiry.IsZero() && time.Now().After(e.expiry)) {
		// 不存在或已过期：建立新条目（无 TTL）
		e = entry{value: val}
	} else {
		e.value = e.value + val
		// 保留原 expiry（零值表示无 TTL）
	}
	s.m[key] = e
	return int64(len(e.value))
}

// IncrBy 原子地把 key 的整数值加 delta，返回新值。
//   - 不存在的 key 视作 0
//   - 当前值非 base-10 整数（含空串）→ 返回错误，store 不变
//   - 已过期的 key 视作不存在（惰性删除后建立新条目，无 TTL）
//   - 整数溢出 int64 范围 → 返回 overflow 错误，store 不变
//
// DECR 等价于 IncrBy(key, -1)；INCR 等价于 IncrBy(key, 1)。
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	missing := !ok || (!e.expiry.IsZero() && time.Now().After(e.expiry))
	if missing {
		// 不存在或已过期：视作 0，建立新条目（无 TTL）
		e = entry{}
	}
	// 不存在的 key 视作 0；已存在的空串视为非法值（Redis 不允许 INCR 空 key）
	if missing {
		e.value = "0"
	}
	cur, err := strconv.ParseInt(e.value, 10, 64)
	if err != nil {
		return 0, errIncrementNotInteger
	}
	// 溢出检测：同号相加可能溢出
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, errIncrementOverflow
	}
	newVal := cur + delta
	// 保留原 expiry（零值表示无 TTL）
	e.value = strconv.FormatInt(newVal, 10)
	s.m[key] = e
	return newVal, nil
}

// Len returns the current number of keys (approximate under concurrency).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}
