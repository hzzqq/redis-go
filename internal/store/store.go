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
// Value types: a key holds either a string, a list, or a hash. Operating on a
// key with the wrong command family returns ErrWrongType, mirroring Redis.
// Empty collections (list/hash with no elements left) delete the key, as in
// Redis. SET always overwrites to a string regardless of the previous type.
package store

import (
	"errors"
	"math"
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

type entry struct {
	val    any // string | *list | *hash
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
	e.val = strconv.FormatInt(cur + delta, 10)
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
		grown = append(grown, vals...)
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
