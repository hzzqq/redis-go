package store

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func TestZAddScoreCard(t *testing.T) {
	s := New()
	n, err := s.ZAdd("z", []ZItem{{"a", 1}, {"b", 2}, {"c", 3}})
	if err != nil || n != 3 {
		t.Fatalf("ZAdd new: got (%d,%v)", n, err)
	}
	// 纯 score 更新不计入 added
	n, _ = s.ZAdd("z", []ZItem{{"a", 10}})
	if n != 0 {
		t.Fatalf("ZAdd update should not count, got %d", n)
	}
	// 新增与更新混合
	n, _ = s.ZAdd("z", []ZItem{{"a", 10}, {"d", 4}})
	if n != 1 {
		t.Fatalf("ZAdd mixed: expected 1, got %d", n)
	}
	if score, found, _ := s.ZScore("z", "a"); !found || score != 10 {
		t.Fatalf("ZScore a: got (%v,%v)", score, found)
	}
	if card, _ := s.ZCard("z"); card != 4 {
		t.Fatalf("ZCard: got %d", card)
	}
	if _, found, _ := s.ZScore("z", "nope"); found {
		t.Fatal("ZScore missing member should be found=false")
	}
}

func TestZIncrBy(t *testing.T) {
	s := New()
	v, err := s.ZIncrBy("z", "m", 2.5) // missing member → delta
	if err != nil || v != 2.5 {
		t.Fatalf("ZIncrBy new: got (%v,%v)", v, err)
	}
	v, _ = s.ZIncrBy("z", "m", 1.5)
	if v != 4 {
		t.Fatalf("ZIncrBy existing: got %v", v)
	}
	// wrongtype
	s.Set("str", "x", 0)
	if _, err := s.ZIncrBy("str", "m", 1); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
}

func TestZRankTiebreakAndRev(t *testing.T) {
	s := New()
	// 同分成员按 member 字典序升序排名
	s.ZAdd("z", []ZItem{{"b", 5}, {"a", 5}, {"c", 5}, {"d", 7}})
	if r, found, _ := s.ZRank("z", "a"); !found || r != 0 {
		t.Fatalf("ZRank a: got (%d,%v)", r, found)
	}
	if r, found, _ := s.ZRank("z", "c"); !found || r != 2 {
		t.Fatalf("ZRank c: got (%d,%v)", r, found)
	}
	if r, found, _ := s.ZRank("z", "d"); !found || r != 3 {
		t.Fatalf("ZRank d: got (%d,%v)", r, found)
	}
	if _, found, _ := s.ZRank("z", "x"); found {
		t.Fatal("ZRank missing member should be found=false")
	}
	// ZREVRANK：降序 = n-1-升序
	if r, found, _ := s.ZRevRank("z", "d"); !found || r != 0 {
		t.Fatalf("ZRevRank d: got (%d,%v)", r, found)
	}
	if r, found, _ := s.ZRevRank("z", "a"); !found || r != 3 {
		t.Fatalf("ZRevRank a: got (%d,%v)", r, found)
	}
}

func TestZCountBounds(t *testing.T) {
	s := New()
	s.ZAdd("z", []ZItem{{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}, {"e", 5}})
	must := func(minStr, maxStr string, want int64) {
		t.Helper()
		minB, err1 := ParseZBound(minStr)
		maxB, err2 := ParseZBound(maxStr)
		if err1 != nil || err2 != nil {
			t.Fatalf("parse bounds %s %s: %v %v", minStr, maxStr, err1, err2)
		}
		if n, _ := s.ZCount("z", minB, maxB); n != want {
			t.Fatalf("ZCount %s %s: expected %d, got %d", minStr, maxStr, want, n)
		}
	}
	must("-inf", "+inf", 5)
	must("2", "4", 3)
	must("(2", "4", 2)
	must("2", "(4", 2)
	must("(2", "(4", 1)
	must("5", "5", 1)
	must("6", "+inf", 0)
	// 解析失败
	if _, err := ParseZBound("abc"); err == nil {
		t.Fatal("ParseZBound should reject non-numeric")
	}
}

func TestZRangeAscDesc(t *testing.T) {
	s := New()
	s.ZAdd("z", []ZItem{{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}, {"e", 5}})
	eq := func(got, want []ZItem) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("range mismatch: got %v want %v", got, want)
		}
	}
	got, _ := s.ZRange("z", 0, -1, false)
	eq(got, []ZItem{{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}, {"e", 5}})
	got, _ = s.ZRange("z", 1, 3, false)
	eq(got, []ZItem{{"b", 2}, {"c", 3}, {"d", 4}})
	got, _ = s.ZRange("z", -2, -1, false)
	eq(got, []ZItem{{"d", 4}, {"e", 5}})
	// ZREVRANGE：降序、首元素是最高分
	got, _ = s.ZRange("z", 0, -1, true)
	eq(got, []ZItem{{"e", 5}, {"d", 4}, {"c", 3}, {"b", 2}, {"a", 1}})
	got, _ = s.ZRange("z", 0, 1, true)
	eq(got, []ZItem{{"e", 5}, {"d", 4}})
	got, _ = s.ZRange("z", -2, -1, true)
	eq(got, []ZItem{{"b", 2}, {"a", 1}})
	// 越界与空结果
	got, _ = s.ZRange("z", 10, 20, false)
	eq(got, []ZItem{})
	got, _ = s.ZRange("z", 3, 1, false)
	eq(got, []ZItem{})
	got, _ = s.ZRange("missing", 0, -1, false)
	eq(got, []ZItem{})
	// wrongtype
	s.Set("str", "x", 0)
	if _, err := s.ZRange("str", 0, -1, false); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
}

func TestZRemDeletesEmptyKey(t *testing.T) {
	s := New()
	s.ZAdd("z", []ZItem{{"a", 1}, {"b", 2}})
	n, _ := s.ZRem("z", []string{"a", "b", "gone"})
	if n != 2 {
		t.Fatalf("ZRem: expected 2, got %d", n)
	}
	if s.Exists("z") {
		t.Fatal("emptied zset should delete the key")
	}
	if n, _ := s.ZRem("z", []string{"a"}); n != 0 {
		t.Fatalf("ZRem on missing key: got %d", n)
	}
	if s.Type("z") != "none" {
		t.Fatalf("TYPE after ZRem: got %s", s.Type("z"))
	}
	// 部分删除后 key 保留
	s.ZAdd("z2", []ZItem{{"a", 1}, {"b", 2}})
	s.ZRem("z2", []string{"a"})
	if s.Type("z2") != "zset" {
		t.Fatalf("TYPE z2: got %s", s.Type("z2"))
	}
}

func TestZSetWrongType(t *testing.T) {
	s := New()
	s.Set("str", "x", 0)
	if _, err := s.ZAdd("str", []ZItem{{"a", 1}}); err != ErrWrongType {
		t.Fatalf("ZAdd wrongtype: got %v", err)
	}
	if _, _, err := s.ZScore("str", "a"); err != ErrWrongType {
		t.Fatalf("ZScore wrongtype: got %v", err)
	}
	if _, err := s.ZCard("str"); err != ErrWrongType {
		t.Fatalf("ZCard wrongtype: got %v", err)
	}
	if _, _, err := s.ZRank("str", "a"); err != ErrWrongType {
		t.Fatalf("ZRank wrongtype: got %v", err)
	}
	if _, _, err := s.ZRank("str", "a"); err != ErrWrongType {
		t.Fatalf("ZRevRank wrongtype: got %v", err)
	}
	if _, err := s.ZRem("str", []string{"a"}); err != ErrWrongType {
		t.Fatalf("ZRem wrongtype: got %v", err)
	}
	// 反向：对 zset 用 string 命令
	s.ZAdd("z", []ZItem{{"a", 1}})
	if _, _, err := s.Get("z"); err != ErrWrongType {
		t.Fatalf("Get on zset: got %v", err)
	}
}

// TestZSetStressAgainstReference 插入大量随机 (score, member)（含大量同分与
// 覆盖更新），以排序后的参照切片逐项核对跳表的顺序遍历、排名与按排名取元素，
// 然后随机删除一半再核对 —— 验证 span 算术在增删后依然自洽。
func TestZSetStressAgainstReference(t *testing.T) {
	s := New()
	rng := rand.New(rand.NewSource(20260918))
	const (
		rounds   = 2000
		scores   = 300 // 分数空间刻意小 → 大量同分 tiebreak
		memberSp = 800
	)
	ref := map[string]float64{}
	for i := 0; i < rounds; i++ {
		score := float64(rng.Intn(scores))
		member := fmt.Sprintf("m%03d", rng.Intn(memberSp))
		ref[member] = score
		if _, err := s.ZAdd("z", []ZItem{{Member: member, Score: score}}); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
	}
	verify := func(phase string) {
		t.Helper()
		// 参照：按 (score asc, member lex asc) 排序
		want := make([]ZItem, 0, len(ref))
		for m, sc := range ref {
			want = append(want, ZItem{Member: m, Score: sc})
		}
		for i := 1; i < len(want); i++ {
			for j := i; j > 0 && zslLess(want[j].Score, want[j].Member, want[j-1].Score, want[j-1].Member); j-- {
				want[j], want[j-1] = want[j-1], want[j]
			}
		}
		// 1) 顺序遍历一致
		got := s.zsetItems("z")
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: skip-list walk differs from reference (n=%d)", phase, len(want))
		}
		// 2) 每个成员的 getRank 与参照位置一致（校验 span 算术）
		for i, it := range want {
			rank, found, _ := s.ZRank("z", it.Member)
			if !found || rank != int64(i) {
				t.Fatalf("%s: rank of %q: expected %d, got (%d,%v)", phase, it.Member, i, rank, found)
			}
			revRank, _, _ := s.ZRevRank("z", it.Member)
			if revRank != int64(len(want)-1-i) {
				t.Fatalf("%s: revrank of %q: expected %d, got %d", phase, it.Member, len(want)-1-i, revRank)
			}
		}
		// 3) getByRank 抽查 + ZRange 全量对照
		for probe := 0; probe < 50; probe++ {
			idx := int64(rng.Intn(len(want)))
			seg, _ := s.ZRange("z", idx, idx, false)
			if len(seg) != 1 || seg[0] != want[idx] {
				t.Fatalf("%s: ZRange rank %d: got %v want %v", phase, idx, seg, want[idx])
			}
		}
		full, _ := s.ZRange("z", 0, -1, false)
		if !reflect.DeepEqual(full, want) {
			t.Fatalf("%s: ZRange 0 -1 differs from reference", phase)
		}
		rev, _ := s.ZRange("z", 0, -1, true)
		for i := range rev {
			if rev[i] != want[len(want)-1-i] {
				t.Fatalf("%s: ZREVRANGE element %d wrong", phase, i)
			}
		}
	}
	verify("after inserts")

	// 随机删除一半（含不存在的成员），再全量核对
	members := make([]string, 0, len(ref))
	for m := range ref {
		members = append(members, m)
	}
	rng.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
	half := members[:len(members)/2]
	if n, err := s.ZRem("z", half); err != nil || n != int64(len(half)) {
		t.Fatalf("ZRem half: got (%d,%v), want (%d,nil)", n, err, len(half))
	}
	for _, m := range half {
		delete(ref, m)
	}
	verify("after deletes")
}
