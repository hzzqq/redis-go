// Package store is an in-memory key/value store with TTL support.
//
// Concurrency model:
//   - All state is guarded by mu. Writers mutate in place under the write
//     lock; readers copy anything that escapes the lock under the read lock
//     (strings are immutable copies, slices/maps are explicitly copied).
//   - Expired keys are treated as missing. Write paths lazily delete them;
//     read paths only report them as missing (the background sweeper
//     physically removes them within 1s).
//
// Value types: a key holds either a string, a list, a hash, a set, or a
// zset. Operating on a key with the wrong command family returns
// ErrWrongType, mirroring Redis. Empty collections (list/hash/set with no
// elements left) delete the key, as in Redis. SET always overwrites to a
// string regardless of the previous type.
package store

import (
	"errors"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Sentinel errors. Text is Redis-formatted so the server can pass it through.
var (
	// ErrWrongType is returned when a command hits a key of another type.
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	// ErrNoSuchKey is returned by LSET when the key does not exist.
	ErrNoSuchKey = errors.New("ERR no such key")
	// ErrIndexOutOfRange is returned by LSET for an out-of-range index.
	ErrIndexOutOfRange = errors.New("ERR index out of range")
	// ErrPopRange is returned by LPOP/RPOP with a negative count.
	ErrPopRange = errors.New("ERR value is out of range, must be positive")

	errIncrementNotInteger = errors.New("ERR value is not an integer or out of range")
	errIncrementOverflow   = errors.New("ERR increment or decrement would overflow")
)

// list is a growable sequence of strings.
type list struct {
	items []string
}

// hash keeps fields in insertion order (like Redis listpack encoding).
type hash struct {
	order []string
	m     map[string]string
}

// set is an unordered collection of unique strings.
type set struct {
	m map[string]struct{}
}

type entry struct {
	val    any // string | *list | *hash | *set
	expiry time.Time
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

// validLocked looks key up assuming the write lock is held, lazily deleting
// expired entries.
func validLocked(m map[string]entry, key string) (entry, bool) {
	e, ok := m[key]
	if !ok {
		return entry{}, false
	}
	if !e.expiry.IsZero() && time.Now().After(e.expiry) {
		delete(m, key)
		return entry{}, false
	}
	return e, true
}

// validRO looks key up assuming a read lock is held; expired entries are
// reported as missing but not deleted.
func validRO(m map[string]entry, key string) (entry, bool) {
	e, ok := m[key]
	if !ok {
		return entry{}, false
	}
	if !e.expiry.IsZero() && time.Now().After(e.expiry) {
		return entry{}, false
	}
	return e, true
}

// -------- string commands --------

// Get returns the value and whether it exists. A non-string key yields
// (…"", false, ErrWrongType).
func (s *Store) Get(key string) (string, bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return "", false, nil
	}
	v, isStr := e.val.(string)
	if !isStr {
		return "", false, ErrWrongType
	}
	return v, true, nil
}

// SetAt stores val with an absolute expiry (zero = no expiry) and overwrites
// any previous type, like Redis SET.
func (s *Store) SetAt(key, val string, exp time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = entry{val: val, expiry: exp}
}

// Set stores val with a relative TTL (ttl<=0 means no expiry).
func (s *Store) Set(key, val string, ttl time.Duration) {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	s.SetAt(key, val, exp)
}

// SetFull is the full SET form behind SET key val [NX|XX] [KEEPTTL]
// [EX s|PX ms|PXAT ms] [GET]:
//   - nx: set only when the key does NOT exist; xx: only when it does.
//     When the condition fails the store is untouched (didSet=false).
//   - keepTTL: keep the existing TTL instead of exp (caller guarantees the
//     two are not combined, as Redis rejects that at parse time).
//   - wantOld (GET): returns the old string value; a non-string old value
//     fails with ErrWrongType and nothing is written (Redis semantics).
//     Expired/missing keys count as absent (old=nil).
//
// Expired keys are lazily deleted, so SET on an expired key writes fresh.
func (s *Store) SetFull(key, val string, exp time.Time, keepTTL, nx, xx, wantOld bool) (*string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var old *string
	if ok && wantOld {
		sv, isStr := e.val.(string)
		if !isStr {
			return nil, false, ErrWrongType
		}
		cp := sv
		old = &cp
	}
	if ok && nx {
		return old, false, nil
	}
	if !ok && xx {
		return nil, false, nil
	}
	var newExp time.Time
	if keepTTL && ok {
		newExp = e.expiry
	} else {
		newExp = exp
	}
	s.m[key] = entry{val: val, expiry: newExp}
	return old, true, nil
}

// Append atomically appends val to the string at key and returns the new
// length. A missing/expired key is treated as an empty string (new entry has
// no TTL). ErrWrongType if the key holds a list/hash.
func (s *Store) Append(key, val string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		e = entry{val: val}
		s.m[key] = e
		return int64(len(val)), nil
	}
	cur, isStr := e.val.(string)
	if !isStr {
		return 0, ErrWrongType
	}
	e.val = cur + val
	s.m[key] = e
	return int64(len(cur) + len(val)), nil
}

// IncrBy atomically adds delta to the integer value of key and returns the
// new value.
//   - missing/expired key counts as 0 (new entry has no TTL)
//   - non-integer or wrong-type value → error, store unchanged
//   - int64 overflow/underflow → error, store unchanged
//
// DECR == IncrBy(key,-1); INCR == IncrBy(key,1).
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var curStr string
	if ok {
		sv, isStr := e.val.(string)
		if !isStr {
			return 0, ErrWrongType
		}
		curStr = sv
	} else {
		curStr = "0"
	}
	cur, err := strconv.ParseInt(curStr, 10, 64)
	if err != nil {
		return 0, errIncrementNotInteger
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, errIncrementOverflow
	}
	e.val = strconv.FormatInt(cur+delta, 10)
	s.m[key] = e
	return cur + delta, nil
}

// -------- generic key commands --------

// Del removes key; reports whether it existed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := validLocked(s.m, key); ok {
		delete(s.m, key)
		return true
	}
	return false
}

// Exists reports whether key is present and not expired.
func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	_, ok := validRO(s.m, key)
	s.mu.RUnlock()
	return ok
}

// ExpireAt sets an absolute expiry on key; reports whether key existed.
// An expiry in the past deletes the key (Redis PEXPIREAT semantics).
func (s *Store) ExpireAt(key string, exp time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return false
	}
	if !exp.IsZero() && !exp.After(time.Now()) {
		delete(s.m, key)
		return true
	}
	e.expiry = exp
	s.m[key] = e
	return true
}

// Expire sets a relative TTL on key; reports whether key existed.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	return s.ExpireAt(key, time.Now().Add(ttl))
}

// ExpireAtOpts is the conditional EXPIRE/PEXPIREAT core (Redis 7 options):
//   - nx: set only when the key has NO expiry
//   - xx: set only when the key HAS an expiry
//   - gt: set only when the new expiry is strictly greater than the current
//     one (fails when the key has no expiry)
//   - lt: set only when the new expiry is strictly less than the current one,
//     or when the key has no expiry
//
// Unmet conditions leave the key untouched and return false. A past expiry
// that passes the conditions deletes the key (returns true), matching Redis.
func (s *Store) ExpireAtOpts(key string, exp time.Time, nx, xx, gt, lt bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return false
	}
	oldHas := !e.expiry.IsZero()
	if nx && oldHas {
		return false
	}
	if xx && !oldHas {
		return false
	}
	if gt && (!oldHas || !exp.After(e.expiry)) {
		return false
	}
	if lt && (oldHas && !exp.Before(e.expiry)) {
		return false
	}
	if !exp.IsZero() && !exp.After(time.Now()) {
		delete(s.m, key)
		return true
	}
	e.expiry = exp
	s.m[key] = e
	return true
}

// TTL returns the remaining TTL.
//   - exists == false : key missing
//   - rem  < 0        : key exists but has no expiry
//   - rem  >= 1       : remaining seconds, rounded up (Redis TTL semantics)
func (s *Store) TTL(key string) (rem int64, exists bool) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, false
	}
	if e.expiry.IsZero() {
		return -1, true
	}
	left := time.Until(e.expiry)
	if left <= 0 {
		return 0, false
	}
	return int64((left + time.Second - 1) / time.Second), true
}

// Flush removes every key (FLUSHALL), keeping the sweeper alive.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = make(map[string]entry)
}

// Len returns the current number of keys (approximate under concurrency).
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

// Type returns the Redis type name of key: "none", "string", "list", "hash"
// or "set" / "zset" (TYPE command).
func (s *Store) Type(key string) string {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return "none"
	}
	return typeName(e.val)
}

// typeName maps a stored value to its Redis type name.
func typeName(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case *list:
		return "list"
	case *hash:
		return "hash"
	case *set:
		return "set"
	case *zsetVal:
		return "zset"
	case *streamVal:
		return "stream"
	default:
		return "none"
	}
}

// ScanAll returns every live key (expired-but-unswept entries excluded) in
// sorted order, filtered to typeFilter when non-empty ("string"/"list"/
// "hash"/"set"/"zset"). SCAN turns this snapshot into cursor pages on the
// server side; a stable sort keeps one logical traversal consistent across
// calls (within the usual SCAN no-guarantees for concurrent mutations).
func (s *Store) ScanAll(typeFilter string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	out := make([]string, 0, len(s.m))
	for k, e := range s.m {
		if !e.expiry.IsZero() && now.After(e.expiry) {
			continue
		}
		if typeFilter != "" && typeName(e.val) != typeFilter {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Encoding returns the internal encoding name of key for OBJECT ENCODING:
//   - string: "int" (fits int64) / "embstr" (≤44 bytes) / "raw"
//   - list:   "listpack" (≤128 elems) / "quicklist"
//   - hash:   "listpack" (≤128 fields) / "hashtable"
//   - set:    "intset" (all-integer, ≤512) / "listpack" (≤128) / "hashtable"
//   - zset:   "listpack" (≤128 members) / "skiplist"
//
// The thresholds mirror Redis 7 defaults (hash/set/zset-max-listpack-entries
// 128, set-max-intset-entries 512); as redis-go stores everything as native
// Go structures these names are a faithful *mapping*, not real encodings.
// missing → ("", false); wrong-type never happens (the value type IS the
// Redis type), so no error path.
func (s *Store) Encoding(key string) (string, bool) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	switch v := e.val.(type) {
	case string:
		if _, err := strconv.ParseInt(v, 10, 64); err == nil {
			return "int", true
		}
		if len(v) <= 44 {
			return "embstr", true
		}
		return "raw", true
	case *list:
		if len(v.items) <= 128 {
			return "listpack", true
		}
		return "quicklist", true
	case *hash:
		if len(v.m) <= 128 {
			return "listpack", true
		}
		return "hashtable", true
	case *set:
		allInt := len(v.m) > 0
		for m := range v.m {
			if _, err := strconv.ParseInt(m, 10, 64); err != nil {
				allInt = false
				break
			}
		}
		if allInt && len(v.m) <= 512 {
			return "intset", true
		}
		if len(v.m) <= 128 {
			return "listpack", true
		}
		return "hashtable", true
	case *zsetVal:
		if v.sl.length <= 128 {
			return "listpack", true
		}
		return "skiplist", true
	case *streamVal:
		// Redis 对 stream 报告的编码名就是 "stream"（radix tree + listpack
		// 的组合在 OBJECT ENCODING 里统一呈现为该名）；我们同样是映射而非
		// 真实编码。
		return "stream", true
	default:
		return "", false
	}
}

// Stats returns the number of live keys and how many of them carry a TTL
// (used by DBSIZE and INFO). Expired-but-unswept entries are reported as
// gone without being deleted; the background sweeper removes them.
func (s *Store) Stats() (keys, expires int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	for _, e := range s.m {
		if !e.expiry.IsZero() && now.After(e.expiry) {
			continue
		}
		keys++
		if !e.expiry.IsZero() {
			expires++
		}
	}
	return keys, expires
}

// DBSize returns the number of live keys (DBSIZE command).
func (s *Store) DBSize() int64 {
	keys, _ := s.Stats()
	return keys
}

// Snapshot exports every live key as a minimal canonical command set for AOF
// rewrite: exactly one command per key — SET (with absolute PXAT when the key
// carries a TTL), RPUSH (full list), HSET (insertion order), SADD (members
// sorted) or ZADD (skip-list order, score asc) — skipping expired entries.
// A single read-lock pass yields a consistent point-in-time view; map
// iteration order is random but each key's command is self-contained, so the
// set replays to the same state in any order.
func (s *Store) Snapshot() [][]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	out := make([][]string, 0, len(s.m))
	for k, e := range s.m {
		if !e.expiry.IsZero() && now.After(e.expiry) {
			continue // 已过期（尚未被清扫）的 key 不进快照
		}
		switch v := e.val.(type) {
		case string:
			cmd := []string{"SET", k, v}
			if !e.expiry.IsZero() {
				cmd = append(cmd, "PXAT", strconv.FormatInt(e.expiry.UnixMilli(), 10))
			}
			out = append(out, cmd)
		case *list:
			cmd := make([]string, 0, 2+len(v.items))
			cmd = append(cmd, "RPUSH", k)
			cmd = append(cmd, v.items...)
			out = append(out, cmd)
		case *hash:
			cmd := make([]string, 0, 2+2*len(v.order))
			cmd = append(cmd, "HSET", k)
			for _, f := range v.order {
				cmd = append(cmd, f, v.m[f])
			}
			out = append(out, cmd)
		case *set:
			cmd := make([]string, 0, 2+len(v.m))
			cmd = append(cmd, "SADD", k)
			members := make([]string, 0, len(v.m))
			for m := range v.m {
				members = append(members, m)
			}
			sort.Strings(members) // 排序保证重写输出确定性
			cmd = append(cmd, members...)
			out = append(out, cmd)
		case *zsetVal:
			items := v.sl.items() // 跳表序 = score asc, member asc
			cmd := make([]string, 0, 2+2*len(items))
			cmd = append(cmd, "ZADD", k)
			for _, it := range items {
				cmd = append(cmd, formatZScore(it.Score), it.Member)
			}
			out = append(out, cmd)
		case *streamVal:
			// 流：每条条目一条显式 ID 的 XADD（XADD 一次只加一条；显式 ID
			// 重放严格递增，与原写入序一致）。last 随条目重放自然恢复。
			for _, en := range v.entries {
				cmd := make([]string, 0, 3+len(en.Fields))
				cmd = append(cmd, "XADD", k, en.ID.String())
				cmd = append(cmd, en.Fields...)
				out = append(out, cmd)
			}
		}
	}
	return out
}

// -------- list commands --------

// ListPush prepends (front=true) or appends vals to the list at key, creating
// it if needed, and returns the new length. ErrWrongType on a non-list key.
func (s *Store) ListPush(key string, front bool, vals ...string) (int64, error) {
	if len(vals) == 0 {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var items []string
	if ok {
		lst, isList := e.val.(*list)
		if !isList {
			return 0, ErrWrongType
		}
		items = lst.items
	}
	if front {
		grown := make([]string, 0, len(vals)+len(items))
		// Redis 语义：多参数 LPUSH 依次头插，LPUSH l a b → [b a ...old]
		for i := len(vals) - 1; i >= 0; i-- {
			grown = append(grown, vals[i])
		}
		grown = append(grown, items...)
		items = grown
	} else {
		items = append(items, vals...)
	}
	e.val = &list{items: items}
	s.m[key] = e
	return int64(len(items)), nil
}

// ListPop removes and returns up to count elements from the head (front=true)
// or tail of the list. count==0 pops nothing (empty result, key untouched);
// count<0 → ErrPopRange. An emptied list deletes the key.
func (s *Store) ListPop(key string, front bool, count int64) ([]string, error) {
	if count < 0 {
		return nil, ErrPopRange
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return []string{}, nil
	}
	lst, isList := e.val.(*list)
	if !isList {
		return nil, ErrWrongType
	}
	n := int64(len(lst.items))
	if count == 0 || n == 0 {
		return []string{}, nil
	}
	if count > n {
		count = n
	}
	var popped []string
	if front {
		popped = lst.items[:count]
		lst.items = lst.items[count:]
	} else {
		// Redis RPOP count 按弹出序返回（尾元素在前）
		popped = make([]string, count)
		for i := int64(0); i < count; i++ {
			popped[i] = lst.items[n-1-i]
		}
		lst.items = lst.items[:n-count]
	}
	if len(lst.items) == 0 {
		delete(s.m, key)
	}
	return popped, nil
}

// ListLen returns the list length (0 if the key is missing).
func (s *Store) ListLen(key string) (int64, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	lst, isList := e.val.(*list)
	if !isList {
		return 0, ErrWrongType
	}
	return int64(len(lst.items)), nil
}

// ListRange returns elements in [start, stop] (inclusive), with negative
// indices counted from the tail and out-of-range ends clamped, like Redis.
// A missing key yields an empty result.
func (s *Store) ListRange(key string, start, stop int64) ([]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []string{}, nil
	}
	lst, isList := e.val.(*list)
	if !isList {
		return nil, ErrWrongType
	}
	return rangeSlice(lst.items, start, stop), nil
}

// rangeSlice applies Redis LRANGE index semantics to items.
func rangeSlice(items []string, start, stop int64) []string {
	n := int64(len(items))
	if start < 0 {
		start = n + start
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = n + stop
	}
	if n == 0 || start >= n || start > stop {
		return []string{}
	}
	if stop >= n {
		stop = n - 1
	}
	out := make([]string, stop-start+1)
	copy(out, items[start:stop+1])
	return out
}

// ListIndex returns the element at idx (negative = from the tail).
func (s *Store) ListIndex(key string, idx int64) (string, bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return "", false, nil
	}
	lst, isList := e.val.(*list)
	if !isList {
		return "", false, ErrWrongType
	}
	n := int64(len(lst.items))
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		return "", false, nil
	}
	return lst.items[idx], true, nil
}

// ListSet replaces the element at idx. Errors: ErrNoSuchKey,
// ErrWrongType, ErrIndexOutOfRange.
func (s *Store) ListSet(key string, idx int64, val string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return ErrNoSuchKey
	}
	lst, isList := e.val.(*list)
	if !isList {
		return ErrWrongType
	}
	n := int64(len(lst.items))
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		return ErrIndexOutOfRange
	}
	lst.items[idx] = val
	return nil
}

// ListTrim keeps only elements in [start, stop] (Redis LRANGE semantics).
// A missing key is a no-op; an emptied list deletes the key.
func (s *Store) ListTrim(key string, start, stop int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return nil
	}
	lst, isList := e.val.(*list)
	if !isList {
		return ErrWrongType
	}
	kept := rangeSlice(lst.items, start, stop)
	if len(kept) == 0 {
		delete(s.m, key)
		return nil
	}
	lst.items = kept
	return nil
}

// ListMove atomically pops one element from the head (srcLeft) or tail of
// src and pushes it onto the head (dstLeft) or tail of dst — the Redis LMOVE
// core, done under a single write lock so no other command can observe the
// intermediate state. src == dst rotates the list (LMOVE l l LEFT RIGHT ==
// RPOP+LPUSH). A missing src returns ("", false, nil); a popped-empty src is
// deleted (unless it is also dst); a newly created dst carries no TTL, an
// existing dst keeps its TTL (Redis semantics).
func (s *Store) ListMove(src, dst string, srcLeft, dstLeft bool) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	se, ok := validLocked(s.m, src)
	if !ok {
		return "", false, nil
	}
	sl, isList := se.val.(*list)
	if !isList {
		return "", false, ErrWrongType
	}
	var dl *list
	var dstExp time.Time
	if src == dst {
		dl = sl // 同 key 轮转：弹出后立即推回，key 不会变空
	} else {
		de, dok := validLocked(s.m, dst)
		if dok {
			var isList bool
			dl, isList = de.val.(*list)
			if !isList {
				return "", false, ErrWrongType
			}
			dstExp = de.expiry
		} else {
			dl = &list{}
		}
	}
	var val string
	n := len(sl.items)
	if srcLeft {
		val = sl.items[0]
		sl.items = sl.items[1:]
	} else {
		val = sl.items[n-1]
		sl.items = sl.items[:n-1]
	}
	if dstLeft {
		dl.items = append([]string{val}, dl.items...)
	} else {
		dl.items = append(dl.items, val)
	}
	if src != dst {
		if len(sl.items) == 0 {
			delete(s.m, src) // 弹空的源 key 删除（Redis 语义）
		}
		s.m[dst] = entry{val: dl, expiry: dstExp}
	}
	return val, true, nil
}

// ListInsert inserts val before (before=true) or after the first occurrence
// of pivot in the list at key. Returns the new length; a missing key → 0, a
// missing pivot → -1 (key untouched), ErrWrongType on a non-list key.
func (s *Store) ListInsert(key string, before bool, pivot, val string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, nil
	}
	lst, isList := e.val.(*list)
	if !isList {
		return 0, ErrWrongType
	}
	for i, it := range lst.items {
		if it == pivot {
			pos := i
			if !before {
				pos = i + 1
			}
			// 先把新元素包装成独立切片再 splice，避免 append 别名写穿
			grown := append(lst.items[:pos:pos], append([]string{val}, lst.items[pos:]...)...)
			lst.items = grown
			return int64(len(grown)), nil
		}
	}
	return -1, nil
}

// ListPos finds occurrences of elem in the list at key (Redis LPOS core):
//   - rank>0: scan from the head, start collecting at the rank-th match;
//     rank<0: scan from the tail, start at the |rank|-th match and walk
//     toward the head (results in discovery order, i.e. descending index);
//   - count>0 collects up to count matches (count<0 → just the first one);
//   - maxLen>0 caps the TOTAL number of elements compared across the whole
//     search, including the collection phase (Redis t_list.c behavior: the
//     comparison counter keeps running while collecting).
//
// Returns the matched 0-based indices (nil when nothing matched),
// ErrWrongType on a non-list key. rank==0 is rejected by the caller.
func (s *Store) ListPos(key, elem string, rank, count, maxLen int64) ([]int64, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	lst, isList := e.val.(*list)
	if !isList {
		return nil, ErrWrongType
	}
	items := lst.items
	n := int64(len(items))
	want := count
	if want < 0 {
		want = 1
	}
	var matched []int64
	var compared int64
	var rev int64
	if rank < 0 {
		rev = -rank
	}
	for i := int64(0); i < n; i++ {
		idx := i
		if rank < 0 {
			idx = n - 1 - i // 反向：下标从尾往前
		}
		if maxLen > 0 && compared >= maxLen {
			break
		}
		compared++
		if items[idx] != elem {
			continue
		}
		if rev > 0 {
			rev-- // 跳过前 |rank|-1 个匹配
			if rev > 0 {
				continue
			}
		} else if rank > 1 {
			rank--
			continue
		}
		matched = append(matched, idx)
		if int64(len(matched)) >= want {
			break
		}
	}
	return matched, nil
}

// -------- hash commands --------

// HashSet stores field/value pairs on the hash at key (creating it if needed)
// and returns the number of NEW fields added. ErrWrongType on a non-hash key.
func (s *Store) HashSet(key string, pairs [][2]string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var h *hash
	if ok {
		h, ok = e.val.(*hash)
		if !ok {
			return 0, ErrWrongType
		}
	} else {
		h = &hash{m: make(map[string]string)}
	}
	var added int64
	for _, p := range pairs {
		if _, exists := h.m[p[0]]; !exists {
			h.order = append(h.order, p[0])
			added++
		}
		h.m[p[0]] = p[1]
	}
	e.val = h
	s.m[key] = e
	return added, nil
}

// HashGet returns the value of field and whether it exists.
func (s *Store) HashGet(key, field string) (string, bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return "", false, nil
	}
	h, isHash := e.val.(*hash)
	if !isHash {
		return "", false, ErrWrongType
	}
	v, exists := h.m[field]
	return v, exists, nil
}

// HashGetAll returns all field/value pairs in insertion order (empty for a
// missing key).
func (s *Store) HashGetAll(key string) ([][2]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return [][2]string{}, nil
	}
	h, isHash := e.val.(*hash)
	if !isHash {
		return nil, ErrWrongType
	}
	out := make([][2]string, 0, len(h.order))
	for _, f := range h.order {
		out = append(out, [2]string{f, h.m[f]})
	}
	return out, nil
}

// HashDel removes fields and returns how many were deleted. An emptied hash
// deletes the key.
func (s *Store) HashDel(key string, fields []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, nil
	}
	h, isHash := e.val.(*hash)
	if !isHash {
		return 0, ErrWrongType
	}
	var del int64
	for _, f := range fields {
		if _, exists := h.m[f]; !exists {
			continue
		}
		delete(h.m, f)
		del++
		for i, of := range h.order {
			if of == f {
				h.order = append(h.order[:i], h.order[i+1:]...)
				break
			}
		}
	}
	if len(h.m) == 0 {
		delete(s.m, key)
	}
	return del, nil
}

// HashLen returns the number of fields (0 if the key is missing).
func (s *Store) HashLen(key string) (int64, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	h, isHash := e.val.(*hash)
	if !isHash {
		return 0, ErrWrongType
	}
	return int64(len(h.m)), nil
}

// HashExists reports whether field exists in the hash at key.
func (s *Store) HashExists(key, field string) (bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return false, nil
	}
	h, isHash := e.val.(*hash)
	if !isHash {
		return false, ErrWrongType
	}
	_, exists := h.m[field]
	return exists, nil
}

// HashKeys returns field names in insertion order.
func (s *Store) HashKeys(key string) ([]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []string{}, nil
	}
	h, isHash := e.val.(*hash)
	if !isHash {
		return nil, ErrWrongType
	}
	out := make([]string, len(h.order))
	copy(out, h.order)
	return out, nil
}

// HashVals returns values in field insertion order.
func (s *Store) HashVals(key string) ([]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []string{}, nil
	}
	h, isHash := e.val.(*hash)
	if !isHash {
		return nil, ErrWrongType
	}
	out := make([]string, 0, len(h.order))
	for _, f := range h.order {
		out = append(out, h.m[f])
	}
	return out, nil
}

// HashIncrBy adds delta to the integer value of field on the hash at key,
// creating the hash/field as needed. A missing field counts as 0.
// Errors: ErrWrongType, errIncrementNotInteger, errIncrementOverflow.
func (s *Store) HashIncrBy(key, field string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var h *hash
	if ok {
		h, ok = e.val.(*hash)
		if !ok {
			return 0, ErrWrongType
		}
	} else {
		h = &hash{m: make(map[string]string)}
	}
	var cur int64
	curStr, exists := h.m[field]
	if exists {
		var err error
		cur, err = strconv.ParseInt(curStr, 10, 64)
		if err != nil {
			return 0, errIncrementNotInteger
		}
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, errIncrementOverflow
	}
	if !exists {
		h.order = append(h.order, field)
	}
	h.m[field] = strconv.FormatInt(cur+delta, 10)
	e.val = h
	s.m[key] = e
	return cur + delta, nil
}

// -------- set commands --------

// SetAdd adds members to the set at key (creating it if needed) and returns
// the number of members actually added (duplicates count once).
// ErrWrongType on a non-set key.
func (s *Store) SetAdd(key string, members []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var st *set
	if ok {
		st, ok = e.val.(*set)
		if !ok {
			return 0, ErrWrongType
		}
	} else {
		st = &set{m: make(map[string]struct{})}
	}
	var added int64
	for _, m := range members {
		if _, exists := st.m[m]; !exists {
			st.m[m] = struct{}{}
			added++
		}
	}
	e.val = st
	s.m[key] = e
	return added, nil
}

// SetRem removes members from the set at key and returns how many were
// removed. An emptied set deletes the key, as in Redis.
func (s *Store) SetRem(key string, members []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, nil
	}
	st, isSet := e.val.(*set)
	if !isSet {
		return 0, ErrWrongType
	}
	var removed int64
	for _, m := range members {
		if _, exists := st.m[m]; !exists {
			continue
		}
		delete(st.m, m)
		removed++
	}
	if len(st.m) == 0 {
		delete(s.m, key)
	}
	return removed, nil
}

// SetIsMember reports whether member is in the set at key (false for a
// missing key).
func (s *Store) SetIsMember(key, member string) (bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return false, nil
	}
	st, isSet := e.val.(*set)
	if !isSet {
		return false, ErrWrongType
	}
	_, exists := st.m[member]
	return exists, nil
}

// SetMembers returns all members of the set at key in unspecified order
// (empty for a missing key), like Redis SMEMBERS.
func (s *Store) SetMembers(key string) ([]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []string{}, nil
	}
	st, isSet := e.val.(*set)
	if !isSet {
		return nil, ErrWrongType
	}
	out := make([]string, 0, len(st.m))
	for m := range st.m {
		out = append(out, m)
	}
	return out, nil
}

// SetCard returns the cardinality of the set at key (0 if the key is missing).
func (s *Store) SetCard(key string) (int64, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	st, isSet := e.val.(*set)
	if !isSet {
		return 0, ErrWrongType
	}
	return int64(len(st.m)), nil
}

// SetPop removes and returns up to count uniformly random members. count==0
// pops nothing; count<0 → ErrPopRange. An emptied set deletes the key.
func (s *Store) SetPop(key string, count int64) ([]string, error) {
	if count < 0 {
		return nil, ErrPopRange
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return []string{}, nil
	}
	st, isSet := e.val.(*set)
	if !isSet {
		return nil, ErrWrongType
	}
	if count == 0 || len(st.m) == 0 {
		return []string{}, nil
	}
	members := make([]string, 0, len(st.m))
	for m := range st.m {
		members = append(members, m)
	}
	rand.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
	if count > int64(len(members)) {
		count = int64(len(members))
	}
	popped := members[:count]
	for _, m := range popped {
		delete(st.m, m)
	}
	if len(st.m) == 0 {
		delete(s.m, key)
	}
	return popped, nil
}

// SetRandMember returns random members WITHOUT removing them. withCount
// selects the count form: count>0 → up to count DISTINCT members; count<0 →
// exactly |count| members with repetition allowed (Redis semantics).
func (s *Store) SetRandMember(key string, count int64, withCount bool) ([]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []string{}, nil
	}
	st, isSet := e.val.(*set)
	if !isSet {
		return nil, ErrWrongType
	}
	members := make([]string, 0, len(st.m))
	for m := range st.m {
		members = append(members, m)
	}
	if !withCount {
		if len(members) == 0 {
			return []string{}, nil
		}
		return []string{members[rand.Intn(len(members))]}, nil
	}
	if count == 0 {
		return []string{}, nil
	}
	if count > 0 {
		rand.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
		if count > int64(len(members)) {
			count = int64(len(members))
		}
		return members[:count], nil
	}
	// negative: |count| draws with repetition
	out := make([]string, -count)
	for i := range out {
		out[i] = members[rand.Intn(len(members))]
	}
	return out, nil
}

// multiSetView materializes the given keys as sets (missing → nil) under one
// read-lock pass. Any wrong-type key aborts with ErrWrongType.
func (s *Store) multiSetView(keys []string) ([]map[string]struct{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	views := make([]map[string]struct{}, len(keys))
	for i, k := range keys {
		e, ok := validRO(s.m, k)
		if !ok {
			continue
		}
		st, isSet := e.val.(*set)
		if !isSet {
			return nil, ErrWrongType
		}
		views[i] = st.m
	}
	return views, nil
}

// SetInter returns the members present in EVERY key, sorted. Redis makes no
// ordering guarantee for set algebra results; sorting is our documented
// choice for deterministic replies.
func (s *Store) SetInter(keys []string) ([]string, error) {
	views, err := s.multiSetView(keys)
	if err != nil {
		return nil, err
	}
	var inter map[string]struct{}
	for _, v := range views {
		if v == nil {
			return []string{}, nil // any missing key → empty intersection
		}
		if inter == nil {
			inter = make(map[string]struct{}, len(v))
			for m := range v {
				inter[m] = struct{}{}
			}
			continue
		}
		for m := range inter {
			if _, exists := v[m]; !exists {
				delete(inter, m)
			}
		}
	}
	out := make([]string, 0, len(inter))
	for m := range inter {
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}

// SetUnion returns the members present in ANY key, sorted.
func (s *Store) SetUnion(keys []string) ([]string, error) {
	views, err := s.multiSetView(keys)
	if err != nil {
		return nil, err
	}
	union := make(map[string]struct{})
	for _, v := range views {
		for m := range v {
			union[m] = struct{}{}
		}
	}
	out := make([]string, 0, len(union))
	for m := range union {
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}

// SetDiff returns the members in the FIRST key that appear in no other key,
// sorted.
func (s *Store) SetDiff(keys []string) ([]string, error) {
	views, err := s.multiSetView(keys)
	if err != nil {
		return nil, err
	}
	diff := make(map[string]struct{})
	if views[0] != nil {
		for m := range views[0] {
			diff[m] = struct{}{}
		}
	}
	for _, v := range views[1:] {
		for m := range v {
			delete(diff, m)
		}
	}
	out := make([]string, 0, len(diff))
	for m := range diff {
		out = append(out, m)
	}
	sort.Strings(out)
	return out, nil
}

// -------- Phase 6: 批量命令 + RDB 导出 --------

// MGet fetches several keys under one read-lock pass. Missing keys AND
// wrong-type keys yield nil (Redis MGET replies nil instead of WRONGTYPE).
func (s *Store) MGet(keys []string) []*string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*string, len(keys))
	for i, k := range keys {
		e, ok := validRO(s.m, k)
		if !ok {
			continue
		}
		if v, isStr := e.val.(string); isStr {
			out[i] = &v
		}
	}
	return out
}

// MSet stores every key/value pair, overwriting any previous type (Redis
// MSET semantics). All pairs land under a single write lock.
func (s *Store) MSet(pairs [][2]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range pairs {
		s.m[p[0]] = entry{val: p[1]}
	}
}

// Exported is one key's state in a neutral shape for RDB export.
type Exported struct {
	Key    string
	Expiry time.Time // zero = no TTL
	Kind   string    // "string" | "list" | "hash" | "set" | "zset" | "stream"
	Str    string
	List   []string
	Hash   [][2]string
	Set    []string // sorted for deterministic output
	ZItems []ZItem  // skip-list order (score asc, member asc)
	Stream []StreamEntry
}

// Export returns every live key as a typed snapshot for RDB saving (expired
// entries are skipped). A single read-lock pass yields a consistent
// point-in-time view, which is self-contained for an RDB file — unlike the
// AOF rewrite snapshot, no coordination with an append log is needed.
func (s *Store) Export() []Exported {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	out := make([]Exported, 0, len(s.m))
	for k, e := range s.m {
		if !e.expiry.IsZero() && now.After(e.expiry) {
			continue
		}
		en := Exported{Key: k, Expiry: e.expiry}
		switch v := e.val.(type) {
		case string:
			en.Kind, en.Str = "string", v
		case *list:
			en.Kind = "list"
			en.List = append([]string(nil), v.items...)
		case *hash:
			en.Kind = "hash"
			en.Hash = make([][2]string, 0, len(v.order))
			for _, f := range v.order {
				en.Hash = append(en.Hash, [2]string{f, v.m[f]})
			}
		case *set:
			en.Kind = "set"
			en.Set = make([]string, 0, len(v.m))
			for m := range v.m {
				en.Set = append(en.Set, m)
			}
			sort.Strings(en.Set)
		case *zsetVal:
			en.Kind = "zset"
			en.ZItems = v.sl.items()
		case *streamVal:
			en.Kind = "stream"
			en.Stream = append([]StreamEntry(nil), v.entries...)
		}
		out = append(out, en)
	}
	return out
}
