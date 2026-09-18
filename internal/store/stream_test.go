// Phase 11 stream 存储层测试：ID 生成与递增校验、NOMKSTREAM、范围边界
// （-/+/( 排他/裸数字 end 全毫秒）、XREV/截断、XDEL/XTRIM 空流删 key、
// StreamRead 严格大于、Snapshot/Export 重建。
package store

import (
	"strings"
	"testing"
	"time"
)

func mustStreamAdd(t *testing.T, s *Store, key string, auto bool, id StreamID, fields ...string) StreamID {
	t.Helper()
	got, added, err := s.StreamAdd(key, auto, id, fields, true)
	if err != nil || !added {
		t.Fatalf("StreamAdd: got (%v,%v,%v)", got, added, err)
	}
	return got
}

func streamIDs(entries []StreamEntry) []string {
	out := make([]string, len(entries))
	for i, en := range entries {
		out[i] = en.ID.String()
	}
	return out
}

func wantIDs(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func TestStreamAddExplicitIDs(t *testing.T) {
	s := New()
	mustStreamAdd(t, s, "st", false, StreamID{1, 0}, "f", "v1")
	mustStreamAdd(t, s, "st", false, StreamID{1, 5}, "f", "v2")
	mustStreamAdd(t, s, "st", false, StreamID{2, 0}, "f", "v3")
	if n, _ := s.StreamLen("st"); n != 3 {
		t.Fatalf("len = %d", n)
	}
	// 0-0 固定错误
	if _, _, err := s.StreamAdd("st", false, StreamID{}, []string{"f", "v"}, true); err == nil ||
		!strings.Contains(err.Error(), "greater than 0-0") {
		t.Fatalf("0-0: %v", err)
	}
	// 等于 last / 小于 last 报错（错误文含 last）
	if _, _, err := s.StreamAdd("st", false, StreamID{2, 0}, nil, true); err == nil ||
		!strings.Contains(err.Error(), "greater than 2-0") {
		t.Fatalf("equal last: %v", err)
	}
	if _, _, err := s.StreamAdd("st", false, StreamID{1, 9}, nil, true); err == nil {
		t.Fatal("smaller id must fail")
	}
	// last 不因失败回退
	if id, _ := s.StreamLastID("st"); id != (StreamID{2, 0}) {
		t.Fatalf("last = %v", id)
	}
}

func TestStreamAddAutoIDs(t *testing.T) {
	s := New()
	a := mustStreamAdd(t, s, "st", true, StreamID{}, "f", "a")
	if a.Seq != 0 {
		t.Fatalf("first auto seq = %d", a.Seq)
	}
	// 显式 ID 定格在当前毫秒：随后自动 ID 应为同毫秒 seq+1 或跨毫秒归 0
	last := StreamID{uint64(time.Now().UnixMilli()), 5}
	mustStreamAdd(t, s, "st", false, last, "f", "b")
	b := mustStreamAdd(t, s, "st", true, StreamID{}, "f", "c")
	ok := (b.MS > last.MS && b.Seq == 0) || (b.MS == last.MS && b.Seq == last.Seq+1)
	if !ok {
		t.Fatalf("auto after %v = %v", last, b)
	}
	// 显式未来 ID 后的自动 ID 追赶该毫秒
	mustStreamAdd(t, s, "st", false, StreamID{a.MS + 100000, 3}, "f", "d")
	c := mustStreamAdd(t, s, "st", true, StreamID{}, "f", "e")
	if !(c.MS == a.MS+100000 && c.Seq == 4) && c.MS <= a.MS+100000 {
		t.Fatalf("auto after future id = %v", c)
	}
}

func TestStreamAddNOMKAndWrongType(t *testing.T) {
	s := New()
	if _, added, err := s.StreamAdd("nope", true, StreamID{1, 0}, []string{"f", "v"}, false); err != nil || added {
		t.Fatalf("NOMKSTREAM missing: (%v,%v)", added, err)
	}
	if s.Type("nope") != "none" {
		t.Fatal("NOMKSTREAM must not create the key")
	}
	// 新流允许（mk=true）
	mustStreamAdd(t, s, "nope", true, StreamID{}, "f", "v")
	// WRONGTYPE
	s.Set("str", "x", 0)
	if _, _, err := s.StreamAdd("str", true, StreamID{}, nil, true); err != ErrWrongType {
		t.Fatalf("wrongtype add: %v", err)
	}
	if _, err := s.StreamLen("str"); err != ErrWrongType {
		t.Fatalf("wrongtype len: %v", err)
	}
	if _, err := s.StreamRange("str", StreamBound{Inf: -1}, StreamBound{Inf: 1}, false, -1); err != ErrWrongType {
		t.Fatalf("wrongtype range: %v", err)
	}
	if _, _, err := s.StreamRead("str", StreamID{}, 0); err != ErrWrongType {
		t.Fatalf("wrongtype read: %v", err)
	}
}

func TestStreamRangeBounds(t *testing.T) {
	s := New()
	for _, id := range []StreamID{{1, 0}, {1, 5}, {2, 0}, {3, 7}} {
		mustStreamAdd(t, s, "st", false, id, "f", "v")
	}
	rangeIDs := func(min, max StreamBound, rev bool, count int64) []string {
		entries, err := s.StreamRange("st", min, max, rev, count)
		if err != nil {
			t.Fatalf("StreamRange: %v", err)
		}
		return streamIDs(entries)
	}
	// - 到 +
	wantIDs(t, "all", rangeIDs(StreamBound{Inf: -1}, StreamBound{Inf: 1}, false, -1),
		"1-0", "1-5", "2-0", "3-7")
	// 闭区间
	wantIDs(t, "closed", rangeIDs(StreamBound{ID: StreamID{1, 5}}, StreamBound{ID: StreamID{2, 0}}, false, -1),
		"1-5", "2-0")
	// ( 排他
	wantIDs(t, "excl", rangeIDs(StreamBound{ID: StreamID{1, 5}, Ex: true}, StreamBound{ID: StreamID{3, 7}, Ex: true}, false, -1),
		"2-0")
	// 裸数字 end 含该毫秒全部序号（2 → 2-maxseq）
	wantIDs(t, "bareEnd", rangeIDs(StreamBound{ID: StreamID{1, 0}}, StreamBound{ID: StreamID{2, ^uint64(0)}}, false, -1),
		"1-0", "1-5", "2-0")
	// 反转 + count
	wantIDs(t, "rev", rangeIDs(StreamBound{Inf: -1}, StreamBound{Inf: 1}, true, -1),
		"3-7", "2-0", "1-5", "1-0")
	wantIDs(t, "rev2", rangeIDs(StreamBound{Inf: -1}, StreamBound{Inf: 1}, true, 2),
		"3-7", "2-0")
	// start > end → 空数组
	wantIDs(t, "empty", rangeIDs(StreamBound{ID: StreamID{5, 0}}, StreamBound{ID: StreamID{1, 0}}, false, -1))
	// 缺失 key → 空数组
	entries, err := s.StreamRange("nope", StreamBound{Inf: -1}, StreamBound{Inf: 1}, false, -1)
	if err != nil || len(entries) != 0 {
		t.Fatalf("missing key range: (%v,%v)", entries, err)
	}
}

func TestStreamDel(t *testing.T) {
	s := New()
	for _, id := range []StreamID{{1, 0}, {2, 0}, {3, 0}} {
		mustStreamAdd(t, s, "st", false, id, "f", "v")
	}
	n, err := s.StreamDel("st", StreamID{2, 0}, StreamID{9, 9})
	if err != nil || n != 1 {
		t.Fatalf("del: (%d,%v)", n, err)
	}
	entries, _ := s.StreamRange("st", StreamBound{Inf: -1}, StreamBound{Inf: 1}, false, -1)
	wantIDs(t, "after del", streamIDs(entries), "1-0", "3-0")
	// last 不回退（自动 ID 仍须越过已删除的 3-0）
	if id, _ := s.StreamLastID("st"); id != (StreamID{3, 0}) {
		t.Fatalf("last after del = %v", id)
	}
	got := mustStreamAdd(t, s, "st", true, StreamID{}, "f", "v")
	if got.MS < 3 {
		t.Fatalf("auto id %v must exceed deleted last 3-0", got)
	}
	// 清空 → key 删除（Redis 7）
	if n, _ := s.StreamDel("st", StreamID{1, 0}, StreamID{3, 0}, got); n != 3 {
		t.Fatalf("final del: %d", n)
	}
	if s.Type("st") != "none" {
		t.Fatal("emptied stream must be deleted")
	}
	if n, _ := s.StreamDel("st", StreamID{1, 0}); n != 0 {
		t.Fatalf("del missing: %d", n)
	}
}

func TestStreamTrim(t *testing.T) {
	s := New()
	for _, id := range []StreamID{{1, 0}, {2, 0}, {3, 0}, {4, 0}} {
		mustStreamAdd(t, s, "st", false, id, "f", "v")
	}
	// MAXLEN 保留最新 2 条
	n, err := s.StreamTrim("st", 2, nil)
	if err != nil || n != 2 {
		t.Fatalf("maxlen trim: (%d,%v)", n, err)
	}
	entries, _ := s.StreamRange("st", StreamBound{Inf: -1}, StreamBound{Inf: 1}, false, -1)
	wantIDs(t, "maxlen", streamIDs(entries), "3-0", "4-0")
	// MAXLEN 不足时不删
	if n, _ := s.StreamTrim("st", 10, nil); n != 0 {
		t.Fatalf("noop trim: %d", n)
	}
	// MINID 5-0 删除 < 5-0 → 清空 → key 删除（Redis 7）
	n, _ = s.StreamTrim("st", -1, &StreamID{5, 0})
	if n != 2 {
		t.Fatalf("minid trim: %d", n)
	}
	if s.Type("st") != "none" {
		t.Fatal("trimmed-to-empty stream must be deleted")
	}
	// 缺失 key
	if n, _ := s.StreamTrim("nope", 2, nil); n != 0 {
		t.Fatalf("missing trim: %d", n)
	}
}

func TestStreamReadStrictlyGreater(t *testing.T) {
	s := New()
	for _, id := range []StreamID{{1, 0}, {2, 0}, {3, 0}} {
		mustStreamAdd(t, s, "st", false, id, "k", "v")
	}
	entries, exists, err := s.StreamRead("st", StreamID{1, 0}, -1)
	if err != nil || !exists {
		t.Fatalf("read: (%v,%v)", exists, err)
	}
	wantIDs(t, "exclusive", streamIDs(entries), "2-0", "3-0")
	// count 截断
	entries, _, _ = s.StreamRead("st", StreamID{}, 1)
	wantIDs(t, "count", streamIDs(entries), "1-0")
	// 缺失 key：exists=false
	if _, exists, _ := s.StreamRead("nope", StreamID{}, -1); exists {
		t.Fatal("missing key must report exists=false")
	}
}

func TestStreamSnapshotAndExport(t *testing.T) {
	s := New()
	mustStreamAdd(t, s, "st", false, StreamID{1, 0}, "f", "v1")
	mustStreamAdd(t, s, "st", false, StreamID{2, 3}, "g", "v2", "h", "v3")
	// Snapshot：每条条目一条显式 ID 的 XADD
	cmds := s.Snapshot()
	if len(cmds) != 2 {
		t.Fatalf("snapshot = %v", cmds)
	}
	if strings.Join(cmds[0], " ") != "XADD st 1-0 f v1" ||
		strings.Join(cmds[1], " ") != "XADD st 2-3 g v2 h v3" {
		t.Fatalf("snapshot cmds = %v", cmds)
	}
	// Export → 新 store 重建 → 状态一致
	exports := s.Export()
	if len(exports) != 1 || exports[0].Kind != "stream" || len(exports[0].Stream) != 2 {
		t.Fatalf("export = %+v", exports)
	}
	s2 := New()
	s2.StreamRestore("st", exports[0].Stream)
	entries, _ := s2.StreamRange("st", StreamBound{Inf: -1}, StreamBound{Inf: 1}, false, -1)
	wantIDs(t, "restored", streamIDs(entries), "1-0", "2-3")
	if id, _ := s2.StreamLastID("st"); id != (StreamID{2, 3}) {
		t.Fatalf("restored last = %v", id)
	}
	// 重建后自动 ID 递增校验依旧生效
	if _, _, err := s2.StreamAdd("st", false, StreamID{2, 3}, nil, true); err == nil {
		t.Fatal("restore must preserve increment validation")
	}
}
