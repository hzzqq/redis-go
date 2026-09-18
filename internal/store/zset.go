// ZSet (sorted set) for the store package.
//
// Layout mirrors Redis t_zset.c: a dict (member → score) for O(1) lookups
// plus a hand-written skip list ordered by (score asc, member lex asc) for
// rank/ordering queries. Skip-list nodes carry per-level spans so that
// ZRANK/ZRANGE are O(log n) without counting.
package store

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
)

const (
	zskiplistMaxLevel = 32
	zskiplistP        = 0.25
)

// ZItem is one (member, score) pair returned by range queries.
type ZItem struct {
	Member string
	Score  float64
}

// zbound is one endpoint of a score range. inf: -1 = -inf, +1 = +inf,
// 0 = finite value; ex marks an exclusive bound (the "(" prefix).
type zbound struct {
	value float64
	ex    bool
	inf   int
}

// ParseZBound parses a Redis ZCOUNT/ZRANGEBYSCORE-style endpoint:
// "-inf", "+inf", "inf", a float, each optionally prefixed with "(" for an
// exclusive bound.
func ParseZBound(s string) (zbound, error) {
	var b zbound
	if strings.HasPrefix(s, "(") {
		b.ex = true
		s = s[1:]
	}
	switch strings.ToLower(s) {
	case "-inf":
		b.inf = -1
		return b, nil
	case "+inf", "inf":
		b.inf = 1
		return b, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return zbound{}, err
	}
	b.value = v
	return b, nil
}

func valueGteMin(score float64, b zbound) bool {
	switch b.inf {
	case -1:
		return true
	case 1:
		return false
	}
	if b.ex {
		return score > b.value
	}
	return score >= b.value
}

func valueLteMax(score float64, b zbound) bool {
	switch b.inf {
	case 1:
		return true
	case -1:
		return false
	}
	if b.ex {
		return score < b.value
	}
	return score <= b.value
}

// zslNode is a skip-list node. level[i].span is the number of nodes skipped
// by level[i].forward (ZSKIPLIST-style rank arithmetic).
type zslNode struct {
	member   string
	score    float64
	backward *zslNode
	level    []zslLevel
}

type zslLevel struct {
	forward *zslNode
	span    int64
}

type zskiplist struct {
	head   *zslNode
	tail   *zslNode
	level  int
	length int64
}

func zslCreate() *zskiplist {
	return &zskiplist{head: &zslNode{level: make([]zslLevel, zskiplistMaxLevel)}, level: 1}
}

// zslRandomLevel draws a tower height with geometric distribution P(p=0.25),
// capped at zskiplistMaxLevel (same shape as Redis ZSKIPLIST).
func zslRandomLevel() int {
	lvl := 1
	for rand.Float64() < zskiplistP && lvl < zskiplistMaxLevel {
		lvl++
	}
	return lvl
}

// less reports whether (score, member) sorts strictly before the target
// position (score asc, member lex asc tiebreak).
func zslLess(scoreA float64, memberA string, scoreB float64, memberB string) bool {
	if scoreA != scoreB {
		return scoreA < scoreB
	}
	return memberA < memberB
}

// insert adds (score, member) assuming it is NOT already present.
func (z *zskiplist) insert(score float64, member string) {
	update := make([]*zslNode, zskiplistMaxLevel)
	rank := make([]int64, zskiplistMaxLevel)
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		if i == z.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			zslLess(x.level[i].forward.score, x.level[i].forward.member, score, member) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}
	lvl := zslRandomLevel()
	if lvl > z.level {
		for i := z.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = z.head
			update[i].level[i].span = z.length
		}
		z.level = lvl
	}
	x = &zslNode{member: member, score: score, level: make([]zslLevel, lvl)}
	for i := 0; i < lvl; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	for i := lvl; i < z.level; i++ {
		update[i].level[i].span++
	}
	if update[0] != z.head {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		z.tail = x
	}
	z.length++
}

// deleteNode unlinks x using the recorded update path.
func (z *zskiplist) deleteNode(x *zslNode, update []*zslNode) {
	for i := 0; i < z.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		z.tail = x.backward
	}
	for z.level > 1 && z.head.level[z.level-1].forward == nil {
		z.level--
	}
	z.length--
}

// delete removes (score, member) if present; reports whether it was found.
func (z *zskiplist) delete(score float64, member string) bool {
	update := make([]*zslNode, zskiplistMaxLevel)
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			zslLess(x.level[i].forward.score, x.level[i].forward.member, score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && x.score == score && x.member == member {
		z.deleteNode(x, update)
		return true
	}
	return false
}

// getByRank returns the node at the 1-based rank (same convention as Redis
// zslGetElementByRank), or nil when rank is outside [1, length].
func (z *zskiplist) getByRank(rank int64) *zslNode {
	if rank < 1 || rank > z.length {
		return nil
	}
	var traversed int64
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if x != z.head && traversed == rank {
			return x
		}
	}
	return nil
}

// getRank returns the 0-based rank of (score, member), or -1 if absent.
func (z *zskiplist) getRank(score float64, member string) int64 {
	var rank int64
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(zslLess(x.level[i].forward.score, x.level[i].forward.member, score, member) ||
				(x.level[i].forward.score == score && x.level[i].forward.member == member)) {
			if x.level[i].forward.score == score && x.level[i].forward.member == member {
				return rank + x.level[i].span - 1
			}
			rank += x.level[i].span
			x = x.level[i].forward
		}
	}
	return -1
}

// firstInRange returns the first node with min <= score <= max, or nil.
func (z *zskiplist) firstInRange(min, max zbound) *zslNode {
	if !z.isInRange(min, max) {
		return nil
	}
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && !valueGteMin(x.level[i].forward.score, min) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x == nil || !valueLteMax(x.score, max) {
		return nil
	}
	return x
}

// isInRange reports whether [min, max] intersects the list's score range
// (same logic as Redis zslIsInRange: empty list → false; the tail must
// satisfy min and the first node must satisfy max).
func (z *zskiplist) isInRange(min, max zbound) bool {
	if z.length == 0 {
		return false
	}
	if !valueGteMin(z.tail.score, min) {
		return false
	}
	x := z.head.level[0].forward
	if x == nil || !valueLteMax(x.score, max) {
		return false
	}
	return true
}

// items walks the level-0 chain in ascending order (test/diagnostic helper).
func (z *zskiplist) items() []ZItem {
	out := make([]ZItem, 0, z.length)
	for x := z.head.level[0].forward; x != nil; x = x.level[0].forward {
		out = append(out, ZItem{Member: x.member, Score: x.score})
	}
	return out
}

// zsetVal is the store-level sorted-set value: dict for O(1) member lookups
// plus the skip list for ordering.
type zsetVal struct {
	m  map[string]float64
	sl *zskiplist
}

func newZsetVal() *zsetVal {
	return &zsetVal{m: make(map[string]float64), sl: zslCreate()}
}

// -------- store API (callers must hold no lock; store locks internally) --------

// ZAdd adds or updates (score, member) pairs on the sorted set at key,
// creating it if needed, and returns the number of NEW members added
// (score updates are not counted — plain ZADD, no CH flag).
func (s *Store) ZAdd(key string, pairs []ZItem) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var z *zsetVal
	if ok {
		z, ok = e.val.(*zsetVal)
		if !ok {
			return 0, ErrWrongType
		}
	} else {
		z = newZsetVal()
	}
	var added int64
	for _, p := range pairs {
		if cur, exists := z.m[p.Member]; exists {
			if cur == p.Score {
				continue
			}
			z.sl.delete(cur, p.Member)
		} else {
			added++
		}
		z.m[p.Member] = p.Score
		z.sl.insert(p.Score, p.Member)
	}
	e.val = z
	s.m[key] = e
	return added, nil
}

// ZScore returns the score of member (found=false for missing key/member).
func (s *Store) ZScore(key, member string) (float64, bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, false, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return 0, false, ErrWrongType
	}
	score, exists := z.m[member]
	return score, exists, nil
}

// ZIncrBy adds delta to the score of member (creating it at delta if
// missing) and returns the new score.
func (s *Store) ZIncrBy(key, member string, delta float64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var z *zsetVal
	if ok {
		z, ok = e.val.(*zsetVal)
		if !ok {
			return 0, ErrWrongType
		}
	} else {
		z = newZsetVal()
	}
	var newScore float64
	if cur, exists := z.m[member]; exists {
		newScore = cur + delta
		z.sl.delete(cur, member)
	} else {
		newScore = delta
	}
	z.m[member] = newScore
	z.sl.insert(newScore, member)
	e.val = z
	s.m[key] = e
	return newScore, nil
}

// ZCard returns the number of members (0 if the key is missing).
func (s *Store) ZCard(key string) (int64, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return 0, ErrWrongType
	}
	return int64(len(z.m)), nil
}

// ZRank returns the 0-based ascending rank of member (score asc, member lex
// asc tiebreak). found=false when the member is absent.
func (s *Store) ZRank(key, member string) (int64, bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, false, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return 0, false, ErrWrongType
	}
	score, exists := z.m[member]
	if !exists {
		return 0, false, nil
	}
	return z.sl.getRank(score, member), true, nil
}

// ZRevRank returns the 0-based rank in descending order.
func (s *Store) ZRevRank(key, member string) (int64, bool, error) {
	rank, found, err := s.ZRank(key, member)
	if err != nil || !found {
		return 0, found, err
	}
	n, _ := s.ZCard(key)
	return n - 1 - rank, true, nil
}

// ZCount returns the number of members with min <= score <= max.
func (s *Store) ZCount(key string, min, max zbound) (int64, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return 0, ErrWrongType
	}
	x := z.sl.firstInRange(min, max)
	var n int64
	for ; x != nil && valueLteMax(x.score, max); x = x.level[0].forward {
		n++
	}
	return n, nil
}

// ZRange returns the elements at ranks [start, stop] (0-based, negative
// counts from the end, out-of-range ends clamped — same semantics as
// LRANGE). With rev the ordering is score-descending.
func (s *Store) ZRange(key string, start, stop int64, rev bool) ([]ZItem, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []ZItem{}, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return nil, ErrWrongType
	}
	n := z.sl.length
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
		return []ZItem{}, nil
	}
	if stop >= n {
		stop = n - 1
	}
	out := make([]ZItem, 0, stop-start+1)
	if rev {
		// descending rank r maps to 1-based ascending rank n-r; output from
		// descending rank start up to stop (ZREVRANGE order: highest first)
		for r := start; r <= stop; r++ {
			node := z.sl.getByRank(n - r)
			out = append(out, ZItem{Member: node.member, Score: node.score})
		}
	} else {
		for r := start; r <= stop; r++ {
			node := z.sl.getByRank(r + 1)
			out = append(out, ZItem{Member: node.member, Score: node.score})
		}
	}
	return out, nil
}

// ZRem removes members and returns how many were removed. An emptied sorted
// set deletes the key.
func (s *Store) ZRem(key string, members []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return 0, ErrWrongType
	}
	var removed int64
	for _, m := range members {
		if score, exists := z.m[m]; exists {
			z.sl.delete(score, m)
			delete(z.m, m)
			removed++
		}
	}
	if len(z.m) == 0 {
		delete(s.m, key)
	}
	return removed, nil
}

// zsetItemsSorted is a convenience used by tests: full ascending snapshot.
func (s *Store) zsetItems(key string) []ZItem {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return nil
	}
	return z.sl.items()
}

// formatZScore renders a score for AOF rewrite: the shortest decimal that
// round-trips through ParseFloat (so replayed floats are bit-identical).
func formatZScore(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// -------- Phase 6: score/lex 范围遍历 + 随机成员 --------

// lastInRange returns the last node with min <= score <= max, or nil. Mirror
// of firstInRange: level-wise descent advancing while the forward node
// satisfies max, which lands on the last node <= max.
func (z *zskiplist) lastInRange(min, max zbound) *zslNode {
	if !z.isInRange(min, max) {
		return nil
	}
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && valueLteMax(x.level[i].forward.score, max) {
			x = x.level[i].forward
		}
	}
	if x == z.head || !valueGteMin(x.score, min) {
		return nil
	}
	return x
}

// zlexbound is one endpoint of a member (lex) range. inf: -1 = "-"（最小）、
// +1 = "+"（最大）；ex 表示 "(" 排他边界（"[" 为包含）。
type zlexbound struct {
	value string
	ex    bool
	inf   int
}

// ParseZLexBound parses a ZRANGEBYLEX-style endpoint: "-", "+", "[member"
// (inclusive), "(member" (exclusive).
func ParseZLexBound(s string) (zlexbound, error) {
	switch s {
	case "-":
		return zlexbound{inf: -1}, nil
	case "+":
		return zlexbound{inf: 1}, nil
	}
	if s == "" {
		return zlexbound{}, errors.New("min or max not valid string range item")
	}
	switch s[0] {
	case '[':
		return zlexbound{value: s[1:]}, nil
	case '(':
		return zlexbound{value: s[1:], ex: true}, nil
	}
	return zlexbound{}, errors.New("min or max not valid string range item")
}

func lexGteMin(m string, b zlexbound) bool {
	switch b.inf {
	case -1:
		return true
	case 1:
		return false
	}
	if b.ex {
		return m > b.value
	}
	return m >= b.value
}

func lexLteMax(m string, b zlexbound) bool {
	switch b.inf {
	case 1:
		return true
	case -1:
		return false
	}
	if b.ex {
		return m < b.value
	}
	return m <= b.value
}

// firstInLexRange returns the first node whose member is in [min, max] (lex),
// or nil. Lex ranges are only meaningful when all members share one score —
// the same caveat as Redis ZRANGEBYLEX.
func (z *zskiplist) firstInLexRange(min, max zlexbound) *zslNode {
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && !lexGteMin(x.level[i].forward.member, min) {
			x = x.level[i].forward
		}
	}
	x = x.level[0].forward
	if x == nil || !lexLteMax(x.member, max) {
		return nil
	}
	return x
}

// lastInLexRange returns the last node whose member is in [min, max] (lex),
// or nil.
func (z *zskiplist) lastInLexRange(min, max zlexbound) *zslNode {
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && lexLteMax(x.level[i].forward.member, max) {
			x = x.level[i].forward
		}
	}
	if x == z.head || !lexGteMin(x.member, min) {
		return nil
	}
	return x
}

// ZRangeByScore returns members with min <= score <= max in ascending score
// order, skipping the first `offset` matches and returning up to count
// (count < 0 = unlimited, count == 0 = empty, like Redis LIMIT).
func (s *Store) ZRangeByScore(key string, min, max zbound, offset, count int64) ([]ZItem, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []ZItem{}, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return nil, ErrWrongType
	}
	x := z.sl.firstInRange(min, max)
	for ; x != nil && valueLteMax(x.score, max) && offset > 0; x = x.level[0].forward {
		offset--
	}
	out := []ZItem{}
	for ; x != nil && valueLteMax(x.score, max); x = x.level[0].forward {
		if count == 0 {
			break
		}
		out = append(out, ZItem{Member: x.member, Score: x.score})
		if count > 0 {
			count--
		}
	}
	return out, nil
}

// ZRevRangeByScore is ZRangeByScore in descending score order.
func (s *Store) ZRevRangeByScore(key string, min, max zbound, offset, count int64) ([]ZItem, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []ZItem{}, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return nil, ErrWrongType
	}
	x := z.sl.lastInRange(min, max)
	for ; x != nil && valueGteMin(x.score, min) && offset > 0; x = x.backward {
		offset--
	}
	out := []ZItem{}
	for ; x != nil && valueGteMin(x.score, min); x = x.backward {
		if count == 0 {
			break
		}
		out = append(out, ZItem{Member: x.member, Score: x.score})
		if count > 0 {
			count--
		}
	}
	return out, nil
}

// ZRangeByLex returns members whose member sorts inside [min, max] (lex), in
// ascending member order, with the same offset/count semantics as
// ZRangeByScore. Only meaningful when all members share one score.
func (s *Store) ZRangeByLex(key string, min, max zlexbound, offset, count int64) ([]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []string{}, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return nil, ErrWrongType
	}
	x := z.sl.firstInLexRange(min, max)
	out := []string{}
	for ; x != nil && lexLteMax(x.member, max); x = x.level[0].forward {
		if offset > 0 {
			offset--
			continue
		}
		if count == 0 {
			break
		}
		out = append(out, x.member)
		if count > 0 {
			count--
		}
	}
	return out, nil
}

// ZRevRangeByLex is ZRangeByLex in descending member order.
func (s *Store) ZRevRangeByLex(key string, min, max zlexbound, offset, count int64) ([]string, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []string{}, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return nil, ErrWrongType
	}
	x := z.sl.lastInLexRange(min, max)
	out := []string{}
	for ; x != nil && lexGteMin(x.member, min); x = x.backward {
		if offset > 0 {
			offset--
			continue
		}
		if count == 0 {
			break
		}
		out = append(out, x.member)
		if count > 0 {
			count--
		}
	}
	return out, nil
}

// ZRandMember returns random members without removing them. withCount
// selects the count form: count > 0 → up to count DISTINCT members;
// count < 0 → exactly |count| members with repetition allowed (Redis
// semantics). Each item carries its score so the server can serve
// WITHSCORES replies.
func (s *Store) ZRandMember(key string, count int64, withCount bool) ([]ZItem, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	s.mu.RUnlock()
	if !ok {
		return []ZItem{}, nil
	}
	z, isZ := e.val.(*zsetVal)
	if !isZ {
		return nil, ErrWrongType
	}
	members := make([]string, 0, len(z.m))
	for m := range z.m {
		members = append(members, m)
	}
	if !withCount {
		if len(members) == 0 {
			return []ZItem{}, nil
		}
		m := members[rand.Intn(len(members))]
		return []ZItem{{Member: m, Score: z.m[m]}}, nil
	}
	if count == 0 {
		return []ZItem{}, nil
	}
	if count > 0 {
		rand.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
		if count > int64(len(members)) {
			count = int64(len(members))
		}
		out := make([]ZItem, 0, count)
		for _, m := range members[:count] {
			out = append(out, ZItem{Member: m, Score: z.m[m]})
		}
		return out, nil
	}
	out := make([]ZItem, -count)
	for i := range out {
		m := members[rand.Intn(len(members))]
		out[i] = ZItem{Member: m, Score: z.m[m]}
	}
	return out, nil
}

