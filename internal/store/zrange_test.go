package store

import (
	"testing"
)

// zsetup 建一个带同分 tiebreak 的 zset：a=1, b=2, c=2, d=3, e=3
//（同分内按成员字典序：b<c, d<e）。
func zsetup(t *testing.T) *Store {
	t.Helper()
	s := New()
	if _, err := s.ZAdd("z", []ZItem{
		{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 2},
		{Member: "d", Score: 3}, {Member: "e", Score: 3},
	}); err != nil {
		t.Fatal(err)
	}
	return s
}

func members(items []ZItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.Member
	}
	return out
}

func zassert(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", name, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", name, got, want)
		}
	}
}

func TestZRangeByScore(t *testing.T) {
	s := zsetup(t)
	minAll, _ := ParseZBound("-inf")
	maxAll, _ := ParseZBound("+inf")

	got, err := s.ZRangeByScore("z", minAll, maxAll, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	zassert(t, "all", members(got), []string{"a", "b", "c", "d", "e"})

	lo, _ := ParseZBound("(2") // 排他下界
	hi, _ := ParseZBound("3")
	got, _ = s.ZRangeByScore("z", lo, hi, 0, -1)
	zassert(t, "(2..3", members(got), []string{"d", "e"})

	hi2, _ := ParseZBound("(3") // 排他上界
	got, _ = s.ZRangeByScore("z", minAll, hi2, 0, -1)
	zassert(t, "-inf..(3", members(got), []string{"a", "b", "c"})

	// LIMIT offset count：跳 1 取 2
	got, _ = s.ZRangeByScore("z", minAll, maxAll, 1, 2)
	zassert(t, "limit 1 2", members(got), []string{"b", "c"})
	// offset 越界 → 空
	got, _ = s.ZRangeByScore("z", minAll, maxAll, 99, 2)
	zassert(t, "offset overflow", members(got), []string{})
	// count 0 → 空
	got, _ = s.ZRangeByScore("z", minAll, maxAll, 0, 0)
	zassert(t, "count 0", members(got), []string{})
}

func TestZRevRangeByScore(t *testing.T) {
	s := zsetup(t)
	minAll, _ := ParseZBound("-inf")
	maxAll, _ := ParseZBound("+inf")

	// store 契约：ZRevRangeByScore(key, min, max)，server 层负责命令序换位
	got, err := s.ZRevRangeByScore("z", minAll, maxAll, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	zassert(t, "rev all", members(got), []string{"e", "d", "c", "b", "a"})

	// rev 区间 [2,3]：从最后一个 ≤3 的（e）向回走到 ≥2
	min2, _ := ParseZBound("2")
	max3, _ := ParseZBound("3")
	got, _ = s.ZRevRangeByScore("z", min2, max3, 0, -1)
	zassert(t, "rev 2..3", members(got), []string{"e", "d", "c", "b"})

	got, _ = s.ZRevRangeByScore("z", minAll, maxAll, 0, 2)
	zassert(t, "rev limit 0 2", members(got), []string{"e", "d"})

	got, _ = s.ZRevRangeByScore("z", minAll, maxAll, 1, 2)
	zassert(t, "rev limit 1 2", members(got), []string{"d", "c"})
}

// TestZRangeByLex 全同分场景：a aa b c。
func TestZRangeByLex(t *testing.T) {
	s := New()
	s.ZAdd("lx", []ZItem{
		{Member: "c", Score: 0}, {Member: "a", Score: 0},
		{Member: "b", Score: 0}, {Member: "aa", Score: 0},
	})
	lo, _ := ParseZLexBound("-")
	hi, _ := ParseZLexBound("+")
	got, err := s.ZRangeByLex("lx", lo, hi, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	zassert(t, "lex all", got, []string{"a", "aa", "b", "c"})

	inclA, _ := ParseZLexBound("[a")
	exclC, _ := ParseZLexBound("(c")
	got, _ = s.ZRangeByLex("lx", inclA, exclC, 0, -1)
	zassert(t, "[a..(c", got, []string{"a", "aa", "b"})

	exclA, _ := ParseZLexBound("(a")
	inclC, _ := ParseZLexBound("[c")
	got, _ = s.ZRangeByLex("lx", exclA, inclC, 0, -1)
	zassert(t, "(a..[c", got, []string{"aa", "b", "c"})

	got, _ = s.ZRangeByLex("lx", lo, hi, 1, 2)
	zassert(t, "lex limit 1 2", got, []string{"aa", "b"})

	// 反向：+ → -，LIMIT 0 2 → [c b]（store 契约仍为 min-first）
	got, err = s.ZRevRangeByLex("lx", lo, hi, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	zassert(t, "lex rev", got, []string{"c", "b", "aa", "a"})
	got, _ = s.ZRevRangeByLex("lx", lo, hi, 0, 2)
	zassert(t, "lex rev limit", got, []string{"c", "b"})

	if _, err := ParseZLexBound("nope"); err == nil {
		t.Fatal("expected invalid lex bound error")
	}
}

// TestZRangeBoundsErrors min>max / 空集 → 空结果；WRONGTYPE 照常。
func TestZRangeBoundsErrors(t *testing.T) {
	s := zsetup(t)
	three, _ := ParseZBound("3")
	one, _ := ParseZBound("1")
	got, err := s.ZRangeByScore("z", three, one, 0, -1)
	if err != nil || len(got) != 0 {
		t.Fatalf("inverted range: got %v err=%v", got, err)
	}
	got, err = s.ZRangeByScore("missing", three, one, 0, -1)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing key: got %v err=%v", got, err)
	}
	s.Set("str", "x", 0)
	if _, err := s.ZRangeByScore("str", three, one, 0, -1); err != ErrWrongType {
		t.Fatalf("wrongtype: err=%v", err)
	}
}

// TestZRandMember 正数去重 / 负数可重复 / 缺 key / WRONGTYPE。
func TestZRandMember(t *testing.T) {
	s := zsetup(t)
	got, err := s.ZRandMember("z", 2, true)
	if err != nil || len(got) != 2 {
		t.Fatalf("count 2: got %v err=%v", got, err)
	}
	seen := map[string]bool{}
	for _, it := range got {
		if it.Score == 0 {
			t.Fatalf("withScores form must carry score: %+v", it)
		}
		seen[it.Member] = true
	}
	if len(seen) != 2 {
		t.Fatalf("positive count must be distinct: %v", got)
	}

	got, err = s.ZRandMember("z", -5, true)
	if err != nil || len(got) != 5 {
		t.Fatalf("count -5: got %v err=%v", got, err)
	}
	for _, it := range got {
		if it.Member != "a" && it.Member != "b" && it.Member != "c" &&
			it.Member != "d" && it.Member != "e" {
			t.Fatalf("repeated draw escaped the set: %v", got)
		}
	}

	got, err = s.ZRandMember("missing", 3, true)
	if err != nil || len(got) != 0 {
		t.Fatalf("missing key: got %v err=%v", got, err)
	}
	if _, err = s.ZRandMember("z", 0, true); err != nil || len(got) != 0 {
		// count 0 → 空（上面的 got 是 missing 的；单独判长度）
	}
	got0, err := s.ZRandMember("z", 0, true)
	if err != nil || len(got0) != 0 {
		t.Fatalf("count 0: got %v err=%v", got0, err)
	}
	s.Set("str", "x", 0)
	if _, err := s.ZRandMember("str", 1, false); err != ErrWrongType {
		t.Fatalf("wrongtype: err=%v", err)
	}
}
