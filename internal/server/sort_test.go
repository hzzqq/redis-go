// Phase 10 SORT 测试：数值/字典序、DESC 含 tiebreak 反转、LIMIT 截取、
// GET 展开（# / 模式 / 缺失 null / 多 GET 扁平化）、BY hash 字段与缺失权重
// 按 0、nosort 保原序、set/zset 源、STORE 覆写与空结果删 key、副本 READONLY、
// WRONGTYPE / 语法 / 非数值错误、missing key 空数组。
package server

import "testing"

func TestSortNumericAndAlpha(t *testing.T) {
	s := New()
	// 数值序（默认 ASC）：故意乱序插入
	s.apply(mkCmd("RPUSH", "nums", "3", "1", "2"))
	wantBulkArray(t, "sort numeric", s.dispatch(mkCmd("SORT", "nums")),
		[]string{"1", "2", "3"})
	wantBulkArray(t, "sort desc", s.dispatch(mkCmd("SORT", "nums", "DESC")),
		[]string{"3", "2", "1"})
	// ALPHA：字典序下 "10" < "9"
	s.apply(mkCmd("RPUSH", "strs", "10", "9", "a"))
	wantBulkArray(t, "sort alpha", s.dispatch(mkCmd("SORT", "strs", "ALPHA")),
		[]string{"10", "9", "a"})
	// 数值序遇非数值元素 → Redis 原文错误
	wantErr(t, "sort non-numeric", s.dispatch(mkCmd("SORT", "strs")),
		"can't be converted into double")
	// DESC tiebreak：等权元素整体反转（含打破平局的字典序）
	s.apply(mkCmd("HSET", "w_1", "w", "5"))
	s.apply(mkCmd("HSET", "w_2", "w", "5"))
	s.apply(mkCmd("RPUSH", "tie", "1", "2"))
	wantBulkArray(t, "asc tie", s.dispatch(mkCmd("SORT", "tie", "BY", "w_*->w")),
		[]string{"1", "2"})
	wantBulkArray(t, "desc tie", s.dispatch(mkCmd("SORT", "tie", "BY", "w_*->w", "DESC")),
		[]string{"2", "1"})
}

func TestSortLimit(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "nums", "3", "1", "2")) // 排序后 [1 2 3]
	wantBulkArray(t, "limit mid", s.dispatch(mkCmd("SORT", "nums", "LIMIT", "1", "1")),
		[]string{"2"})
	wantBulkArray(t, "limit to end", s.dispatch(mkCmd("SORT", "nums", "LIMIT", "1", "-1")),
		[]string{"2", "3"})
	wantBulkArray(t, "limit all", s.dispatch(mkCmd("SORT", "nums", "LIMIT", "0", "-1")),
		[]string{"1", "2", "3"})
	// offset 截断：超出长度 → 空
	wantBulkArray(t, "limit past end", s.dispatch(mkCmd("SORT", "nums", "LIMIT", "5", "10")),
		[]string{})
	// LIMIT 先于 GET 展开（数值源，无需 ALPHA）
	s.apply(mkCmd("RPUSH", "nl", "3", "1", "2"))
	wantBulkArray(t, "limit before get",
		s.dispatch(mkCmd("SORT", "nl", "LIMIT", "1", "1", "GET", "#")), []string{"2"})
	// 语法/整型错误
	wantErr(t, "limit syntax", s.dispatch(mkCmd("SORT", "nums", "LIMIT", "-1", "1")), "syntax error")
	wantErr(t, "limit arity", s.dispatch(mkCmd("SORT", "nums", "LIMIT", "1")), "syntax error")
	wantErr(t, "limit non-int", s.dispatch(mkCmd("SORT", "nums", "LIMIT", "a", "b")),
		"not an integer")
	wantErr(t, "bad option", s.dispatch(mkCmd("SORT", "nums", "BOGUS")), "syntax error")
}

func TestSortGet(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "l", "a", "b"))
	s.apply(mkCmd("HSET", "obj_a", "name", "A"))
	// GET # 取元素本身；GET 模式取引用；缺失 → null bulk（Str 空）
	wantBulkArray(t, "get hash and missing",
		s.dispatch(mkCmd("SORT", "l", "ALPHA", "GET", "#", "GET", "obj_*->name")),
		[]string{"a", "A", "b", ""})
	// 多 GET 扁平化 + ALPHA
	s.apply(mkCmd("SET", "w_b", "x"))
	wantBulkArray(t, "get string ref",
		s.dispatch(mkCmd("SORT", "l", "ALPHA", "GET", "w_*")),
		[]string{"", "x"})
}

func TestSortByWeights(t *testing.T) {
	s := New()
	// BY hash 字段作权重
	s.apply(mkCmd("RPUSH", "l", "a", "b"))
	s.apply(mkCmd("HSET", "w_a", "w", "9"))
	s.apply(mkCmd("HSET", "w_b", "w", "1"))
	wantBulkArray(t, "by hash field",
		s.dispatch(mkCmd("SORT", "l", "BY", "w_*->w")), []string{"b", "a"})
	// BY 引用缺失：数值序按 0（Redis 同）→ 缺失者排最前
	s.apply(mkCmd("SET", "w_y", "3"))
	s.apply(mkCmd("RPUSH", "m", "y", "x")) // w_y=3，w_x 缺失→0
	wantBulkArray(t, "by missing weight zero",
		s.dispatch(mkCmd("SORT", "m", "BY", "w_*")), []string{"x", "y"})
	// BY nosort：保留 list 原序（配合 GET 做批量取值）
	s.apply(mkCmd("RPUSH", "orig", "c", "a", "b"))
	wantBulkArray(t, "by nosort",
		s.dispatch(mkCmd("SORT", "orig", "BY", "nosort")), []string{"c", "a", "b"})
	wantBulkArray(t, "by nosort limit",
		s.dispatch(mkCmd("SORT", "orig", "BY", "nosort", "LIMIT", "1", "2")), []string{"a", "b"})
	// BY 非数值且无 ALPHA → 错误
	s.apply(mkCmd("RPUSH", "l", "c"))
	s.apply(mkCmd("SET", "w_c", "abc"))
	wantErr(t, "by non-numeric",
		s.dispatch(mkCmd("SORT", "l", "BY", "w_*")), "can't be converted into double")
}

func TestSortSources(t *testing.T) {
	s := New()
	// set 源：确定输入序（成员字典序快照）后排序
	s.apply(mkCmd("SADD", "st", "3", "1", "2"))
	wantBulkArray(t, "set source", s.dispatch(mkCmd("SORT", "st")), []string{"1", "2", "3"})
	// zset 源：按 rank 序输入（score asc），再按元素数值排序
	s.apply(mkCmd("ZADD", "z", "5", "20", "1", "10"))
	wantBulkArray(t, "zset source", s.dispatch(mkCmd("SORT", "z")), []string{"10", "20"})
	// missing key → 空数组
	wantBulkArray(t, "missing source", s.dispatch(mkCmd("SORT", "nope")), []string{})
	// string 源 → WRONGTYPE
	s.apply(mkCmd("SET", "k", "v"))
	wantErr(t, "wrongtype", s.dispatch(mkCmd("SORT", "k")), "WRONGTYPE")
}

func TestSortStore(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "nums", "3", "1", "2"))
	// 非空结果：覆写 dst 为 list，回复元素数
	s.apply(mkCmd("RPUSH", "dst", "junk"))
	wantInt(t, "store len", s.dispatch(mkCmd("SORT", "nums", "STORE", "dst")), 3)
	wantBulkArray(t, "store content",
		s.dispatch(mkCmd("LRANGE", "dst", "0", "-1")), []string{"1", "2", "3"})
	// 空结果：删除 dst，回复 0
	wantInt(t, "store empty",
		s.dispatch(mkCmd("SORT", "nope", "STORE", "dst")), 0)
	if s.store.Type("dst") != "none" {
		t.Fatalf("empty store result must delete dst, type=%s", s.store.Type("dst"))
	}
	// STORE 与 GET 展开组合（缺失项跳过）
	s.apply(mkCmd("HSET", "obj_a", "n", "A"))
	s.apply(mkCmd("RPUSH", "l", "a", "b"))
	wantInt(t, "store get",
		s.dispatch(mkCmd("SORT", "l", "ALPHA", "GET", "obj_*->n", "STORE", "out")), 1)
	wantBulkArray(t, "store get content",
		s.dispatch(mkCmd("LRANGE", "out", "0", "-1")), []string{"A"})
}

func TestSortStoreReplicaReadonly(t *testing.T) {
	s := New()
	s.apply(mkCmd("RPUSH", "nums", "2", "1"))
	s.replMu.Lock()
	s.master = &masterLink{}
	s.replMu.Unlock()
	defer func() { s.replMu.Lock(); s.master = nil; s.replMu.Unlock() }()
	// 副本上纯读 SORT 放行，STORE 报 READONLY
	wantBulkArray(t, "replica read sort", s.dispatch(mkCmd("SORT", "nums")), []string{"1", "2"})
	wantErr(t, "replica store", s.dispatch(mkCmd("SORT", "nums", "STORE", "dst")), "READONLY")
}
