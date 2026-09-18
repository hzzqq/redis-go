// Phase 9 store 层测试：SetFull（SET NX/XX/KEEPTTL/GET 核心）、ExpireAtOpts
// （EXPIRE NX/XX/GT/LT 核心）、ListMove/ListInsert/ListPos、Encoding、ScanAll。
package store

import (
	"strconv"
	"testing"
	"time"
)

// ---------------- SetFull ----------------

func TestSetFullNxXx(t *testing.T) {
	s := New()
	// NX：key 不存在 → 设置成功
	if _, didSet, err := s.SetFull("k", "v1", time.Time{}, false, true, false, false); err != nil || !didSet {
		t.Fatalf("NX on missing key: didSet=%v err=%v", didSet, err)
	}
	// NX：key 已存在 → 不设置，状态不变
	if _, didSet, _ := s.SetFull("k", "v2", time.Time{}, false, true, false, false); didSet {
		t.Fatal("NX on existing key must not set")
	}
	if v, _, _ := s.Get("k"); v != "v1" {
		t.Fatalf("value changed despite NX: %q", v)
	}
	// XX：key 存在 → 覆盖
	if _, didSet, _ := s.SetFull("k", "v3", time.Time{}, false, false, true, false); !didSet {
		t.Fatal("XX on existing key must set")
	}
	// XX：key 不存在 → 不设置
	if _, didSet, _ := s.SetFull("other", "v", time.Time{}, false, false, true, false); didSet {
		t.Fatal("XX on missing key must not set")
	}
}

func TestSetFullKeepTTL(t *testing.T) {
	s := New()
	s.Set("k", "v1", 30*time.Second)
	// KEEPTTL：覆盖值但保留 TTL
	if _, didSet, _ := s.SetFull("k", "v2", time.Time{}, true, false, false, false); !didSet {
		t.Fatal("KEEPTTL set failed")
	}
	rem, exists := s.TTL("k")
	if !exists || rem <= 0 || rem > 30 {
		t.Fatalf("TTL not preserved: rem=%d exists=%v", rem, exists)
	}
	// 无 KEEPTTL 的普通 SET 覆盖后 TTL 消失
	s.SetFull("k", "v3", time.Time{}, false, false, false, false)
	if rem, exists := s.TTL("k"); !exists || rem != -1 {
		t.Fatalf("TTL should be cleared: rem=%d exists=%v", rem, exists)
	}
}

func TestSetFullGet(t *testing.T) {
	s := New()
	// GET：key 不存在 → old=nil，仍设置成功
	old, didSet, err := s.SetFull("k", "v1", time.Time{}, false, false, false, true)
	if err != nil || !didSet || old != nil {
		t.Fatalf("GET on missing key: old=%v didSet=%v err=%v", old, didSet, err)
	}
	// GET：key 存在 → 返回旧值
	old, _, _ = s.SetFull("k", "v2", time.Time{}, false, false, false, true)
	if old == nil || *old != "v1" {
		t.Fatalf("GET old value: got %v", old)
	}
	// GET + NX：key 存在 → 返回旧值且不设置
	old, didSet, _ = s.SetFull("k", "v3", time.Time{}, false, true, false, true)
	if didSet || old == nil || *old != "v2" {
		t.Fatalf("GET+NX on existing: old=%v didSet=%v", old, didSet)
	}
	if v, _, _ := s.Get("k"); v != "v2" {
		t.Fatalf("value should stay v2: %q", v)
	}
	// GET 对非 string key → ErrWrongType 且不写入
	s.ListPush("l", false, "a")
	if _, didSet, err := s.SetFull("l", "x", time.Time{}, false, false, false, true); err != ErrWrongType || didSet {
		t.Fatalf("GET on list: err=%v didSet=%v", err, didSet)
	}
	if items, _ := s.ListRange("l", 0, -1); len(items) != 1 || items[0] != "a" {
		t.Fatalf("list must be untouched: %v", items)
	}
}

// ---------------- ExpireAtOpts ----------------

func TestExpireAtOptsConditions(t *testing.T) {
	s := New()
	future := time.Now().Add(time.Hour)
	// key 不存在 → false
	if s.ExpireAtOpts("nope", future, false, false, false, false) {
		t.Fatal("missing key must fail")
	}
	s.Set("k", "v", time.Hour)
	old := time.Now().Add(30 * time.Minute)

	// NX：key 已有 TTL → 失败
	if s.ExpireAtOpts("k", old, true, false, false, false) {
		t.Fatal("NX with existing TTL must fail")
	}
	// XX：key 已有 TTL → 成功
	if !s.ExpireAtOpts("k", old, false, true, false, false) {
		t.Fatal("XX with existing TTL must succeed")
	}
	// GT：新 < 旧 → 失败；新 > 旧 → 成功
	if s.ExpireAtOpts("k", old.Add(-time.Minute), false, false, true, false) {
		t.Fatal("GT with smaller expiry must fail")
	}
	if !s.ExpireAtOpts("k", old.Add(time.Minute), false, false, true, false) {
		t.Fatal("GT with bigger expiry must succeed")
	}
	// LT：新 < 旧 → 成功；新 > 旧 → 失败
	if !s.ExpireAtOpts("k", old, false, false, false, true) {
		t.Fatal("LT with smaller expiry must succeed")
	}
	if s.ExpireAtOpts("k", old.Add(time.Hour), false, false, false, true) {
		t.Fatal("LT with bigger expiry must fail")
	}
	// GT：key 无 TTL → 失败
	s.SetAt("plain", "v", time.Time{})
	if s.ExpireAtOpts("plain", future, false, false, true, false) {
		t.Fatal("GT without existing TTL must fail")
	}
	// LT：key 无 TTL → 成功
	if !s.ExpireAtOpts("plain", future, false, false, false, true) {
		t.Fatal("LT without existing TTL must succeed")
	}
	// NX：key 无 TTL → 成功（key 必须存在；不存在时 EXPIRE 一律返回 0）
	s.SetAt("k2", "v", time.Time{})
	if !s.ExpireAtOpts("k2", future, true, false, false, false) {
		t.Fatal("NX without existing TTL must succeed")
	}
}

// ---------------- ListMove ----------------

func TestListMoveAcrossKeys(t *testing.T) {
	s := New()
	s.ListPush("a", false, "1", "2", "3") // [1 2 3]
	// LMOVE a b LEFT RIGHT：弹头 1，推尾 → b=[1]
	val, moved, err := s.ListMove("a", "b", true, false)
	if err != nil || !moved || val != "1" {
		t.Fatalf("move: val=%q moved=%v err=%v", val, moved, err)
	}
	la, _ := s.ListRange("a", 0, -1) // [2 3]
	lb, _ := s.ListRange("b", 0, -1) // [1]
	if len(la) != 2 || la[0] != "2" || len(lb) != 1 || lb[0] != "1" {
		t.Fatalf("a=%v b=%v", la, lb)
	}
	// 弹空源 → key 删除
	s.ListMove("a", "b", true, false)
	s.ListMove("a", "b", true, false)
	if s.Exists("a") {
		t.Fatal("emptied source key must be deleted")
	}
	if n, _ := s.ListLen("b"); n != 3 {
		t.Fatalf("b should have 3 elems: %d", n)
	}
	// 源不存在 → moved=false
	if _, moved, _ := s.ListMove("a", "b", true, false); moved {
		t.Fatal("missing source must report moved=false")
	}
}

func TestListMoveRotation(t *testing.T) {
	s := New()
	s.ListPush("l", false, "1", "2", "3") // [1 2 3]
	// LMOVE l l LEFT RIGHT == ROL：头元素转到尾
	for _, want := range []string{"1", "2"} {
		val, moved, err := s.ListMove("l", "l", true, false)
		if err != nil || !moved || val != want {
			t.Fatalf("rotate: val=%q want %q moved=%v err=%v", val, want, moved, err)
		}
	}
	items, _ := s.ListRange("l", 0, -1) // [3 1 2]
	if len(items) != 3 || items[0] != "3" || items[2] != "2" {
		t.Fatalf("rotation result: %v", items)
	}
	// TTL 保留（同 key 轮转）
	s.Del("r")
	s.ListPush("r", false, "x")
	s.Expire("r", time.Hour)
	if _, moved, err := s.ListMove("r", "r", true, false); err != nil || !moved {
		t.Fatalf("rotate with ttl: err=%v", err)
	}
	if rem, exists := s.TTL("r"); !exists || rem <= 0 {
		t.Fatal("rotation must preserve TTL")
	}
	// WRONGTYPE
	s.SetAt("str", "v", time.Time{})
	if _, _, err := s.ListMove("l", "str", true, false); err != ErrWrongType {
		t.Fatalf("dst wrong type: %v", err)
	}
}

// ---------------- ListInsert ----------------

func TestListInsert(t *testing.T) {
	s := New()
	s.ListPush("l", false, "a", "c")
	// BEFORE pivot=b 在 c 前 → [a b c]
	if n, err := s.ListInsert("l", true, "c", "b"); err != nil || n != 3 {
		t.Fatalf("insert before: n=%d err=%v", n, err)
	}
	items, _ := s.ListRange("l", 0, -1)
	if items[1] != "b" {
		t.Fatalf("insert position: %v", items)
	}
	// AFTER pivot=a → [a x b c]
	if n, _ := s.ListInsert("l", false, "a", "x"); n != 4 {
		t.Fatalf("insert after: n=%d", n)
	}
	// pivot 不存在 → -1 且不动
	if n, _ := s.ListInsert("l", true, "zzz", "q"); n != -1 {
		t.Fatalf("missing pivot: n=%d", n)
	}
	if n, _ := s.ListLen("l"); n != 4 {
		t.Fatalf("untouched after failed insert: %d", n)
	}
	// key 不存在 → 0
	if n, _ := s.ListInsert("nope", true, "a", "b"); n != 0 {
		t.Fatalf("missing key: n=%d", n)
	}
	// WRONGTYPE
	s.SetAt("str", "v", time.Time{})
	if _, err := s.ListInsert("str", true, "a", "b"); err != ErrWrongType {
		t.Fatalf("wrong type: %v", err)
	}
}

// ---------------- ListPos ----------------

func TestListPos(t *testing.T) {
	s := New()
	s.ListPush("l", false, "a", "b", "a", "c", "a", "b") // 索引 0..5

	cases := []struct {
		name            string
		rank, cnt, maxL int64
		want            []int64
	}{
		{"first", 1, -1, 0, []int64{0}},
		{"second", 2, -1, 0, []int64{2}},
		{"from tail", -1, -1, 0, []int64{4}},
		{"count all", 1, 10, 0, []int64{0, 2, 4}},
		{"count from rank 2", 2, 2, 0, []int64{2, 4}},
		{"negative count walk", -1, 2, 0, []int64{4, 2}},
		{"maxlen limits", 1, 10, 3, []int64{0, 2}},  // 只比较前 3 个元素
		{"maxlen from tail", -1, 10, 2, []int64{4}}, // 从尾比较 2 个：b,a → a@4
	}
	for _, c := range cases {
		got, err := s.ListPos("l", "a", c.rank, c.cnt, c.maxL)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(got) != len(c.want) {
			t.Fatalf("%s: got %v want %v", c.name, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("%s: got %v want %v", c.name, got, c.want)
			}
		}
	}
	// 无匹配 → nil
	if got, _ := s.ListPos("l", "zzz", 1, -1, 0); got != nil {
		t.Fatalf("no match: %v", got)
	}
	// key 不存在 → nil
	if got, _ := s.ListPos("nope", "a", 1, -1, 0); got != nil {
		t.Fatalf("missing key: %v", got)
	}
	// WRONGTYPE
	s.SetAt("str", "v", time.Time{})
	if _, err := s.ListPos("str", "a", 1, -1, 0); err != ErrWrongType {
		t.Fatalf("wrong type: %v", err)
	}
}

// ---------------- Encoding ----------------

func TestEncoding(t *testing.T) {
	s := New()
	// string：int / embstr / raw
	s.SetAt("i", "12345", time.Time{})
	if enc, _ := s.Encoding("i"); enc != "int" {
		t.Fatalf("int encoding: %q", enc)
	}
	s.SetAt("short", "hello world", time.Time{})
	if enc, _ := s.Encoding("short"); enc != "embstr" {
		t.Fatalf("embstr encoding: %q", enc)
	}
	s.SetAt("long", string(make([]byte, 100)), time.Time{})
	if enc, _ := s.Encoding("long"); enc != "raw" {
		t.Fatalf("raw encoding: %q", enc)
	}
	// list：小 listpack
	s.ListPush("l", false, "a", "b")
	if enc, _ := s.Encoding("l"); enc != "listpack" {
		t.Fatalf("list encoding: %q", enc)
	}
	// hash / set / zset
	s.HashSet("h", [][2]string{{"f", "v"}})
	if enc, _ := s.Encoding("h"); enc != "listpack" {
		t.Fatalf("hash encoding: %q", enc)
	}
	s.SetAdd("s", []string{"x"})
	if enc, _ := s.Encoding("s"); enc != "listpack" {
		t.Fatalf("set encoding: %q", enc)
	}
	s.SetAdd("ints", []string{"1", "2", "3"})
	if enc, _ := s.Encoding("ints"); enc != "intset" {
		t.Fatalf("intset encoding: %q", enc)
	}
	s.ZAdd("z", []ZItem{{Member: "m", Score: 1}})
	if enc, _ := s.Encoding("z"); enc != "listpack" {
		t.Fatalf("zset encoding: %q", enc)
	}
	// 大 set → hashtable（>512）
	big := make([]string, 0, 600)
	for i := 0; i < 600; i++ {
		big = append(big, "m"+strconv.Itoa(i))
	}
	s.SetAdd("bigset", big)
	if enc, _ := s.Encoding("bigset"); enc != "hashtable" {
		t.Fatalf("big set encoding: %q", enc)
	}
	// 缺失 key
	if enc, ok := s.Encoding("nope"); ok || enc != "" {
		t.Fatalf("missing key: %q %v", enc, ok)
	}
}

// ---------------- ScanAll ----------------

func TestScanAll(t *testing.T) {
	s := New()
	s.SetAt("s1", "v", time.Time{})
	s.ListPush("l1", false, "x")
	s.HashSet("h1", [][2]string{{"f", "v"}})
	s.SetAdd("se1", []string{"m"})
	s.ZAdd("z1", []ZItem{{Member: "m", Score: 1}})
	all := s.ScanAll("")
	if len(all) != 5 {
		t.Fatalf("scan all: %v", all)
	}
	for i := 1; i < len(all); i++ { // 排序断言
		if all[i-1] >= all[i] {
			t.Fatalf("not sorted: %v", all)
		}
	}
	strs := s.ScanAll("string")
	if len(strs) != 1 || strs[0] != "s1" {
		t.Fatalf("scan string: %v", strs)
	}
	zs := s.ScanAll("zset")
	if len(zs) != 1 || zs[0] != "z1" {
		t.Fatalf("scan zset: %v", zs)
	}
	// 过期 key 不出现（SetAt 允许写入过去时间戳，模拟未清扫的过期 entry）
	s.SetAt("gone", "v", time.Now().Add(-time.Second))
	if got := s.ScanAll(""); len(got) != 5 {
		t.Fatalf("expired key must be excluded: %v", got)
	}
}
