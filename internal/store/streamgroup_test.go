// Phase 12 消费者组存储层测试：XGROUP 语义、XREADGROUP '>'/history 投喂、
// PEL 状态、XACK、XPENDING、XCLAIM 选项矩阵、空流+组生命周期、幽灵清理。
package store

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func mustCreateGroup(t *testing.T, s *Store, key, group, id string, mk bool) {
	t.Helper()
	var sid StreamID
	if id == "$" {
		sid, _ = s.StreamLastID(key) // 空流 → 0-0
	} else {
		var ok bool
		sid, ok = ParseStreamID(id)
		if !ok {
			t.Fatalf("bad id %q", id)
		}
	}
	if err := s.StreamGroupCreate(key, group, sid, mk); err != nil {
		t.Fatalf("StreamGroupCreate: %v", err)
	}
}

func TestGroupCreateAndErrors(t *testing.T) {
	s := New()
	s.StreamAdd("st", false, StreamID{MS: 1}, []string{"f", "a"}, true)
	mustCreateGroup(t, s, "st", "g1", "$", false)
	// BUSYGROUP：重复创建
	if err := s.StreamGroupCreate("st", "g1", StreamID{}, false); err == nil ||
		err.Error() != "BUSYGROUP Consumer Group name already exists" {
		t.Fatalf("expected BUSYGROUP, got %v", err)
	}
	// key 不存在且无 MKSTREAM
	err := s.StreamGroupCreate("nope", "g", StreamID{}, false)
	if err == nil || err.Error() != errXGroupKeyRequired.Error() {
		t.Fatalf("expected key-required error, got %v", err)
	}
	// MKSTREAM：建空流建组
	mustCreateGroup(t, s, "empty", "g", "0-0", true)
	n, err := s.StreamLen("empty")
	if err != nil || n != 0 {
		t.Fatalf("MKSTREAM stream len: (%d,%v)", n, err)
	}
	if s.Type("empty") != "stream" {
		t.Fatalf("MKSTREAM type: %s", s.Type("empty"))
	}
	// WRONGTYPE
	s.Set("str", "x", 0)
	if err := s.StreamGroupCreate("str", "g", StreamID{}, true); err != ErrWrongType {
		t.Fatalf("expected ErrWrongType, got %v", err)
	}
	// SETID / DESTROY / CREATECONSUMER / DELCONSUMER
	if err := s.StreamGroupSetID("st", "g1", StreamID{MS: 1}); err != nil {
		t.Fatalf("SetID: %v", err)
	}
	if err := s.StreamGroupSetID("st", "nope", StreamID{}); err == nil ||
		err.Error() != "BUSYGROUP Consumer Group does not exist" {
		t.Fatalf("SetID missing group: %v", err)
	}
	if n, _ := s.StreamGroupCreateConsumer("st", "g1", "alice"); n != 1 {
		t.Fatalf("CreateConsumer first: %d", n)
	}
	if n, _ := s.StreamGroupCreateConsumer("st", "g1", "alice"); n != 0 {
		t.Fatalf("CreateConsumer again: %d", n)
	}
	if n, _ := s.StreamGroupDelConsumer("st", "g1", "ghost"); n != 0 {
		t.Fatalf("DelConsumer missing: %d", n)
	}
	if n, err := s.StreamGroupDestroy("st", "g1"); err != nil || n != 1 {
		t.Fatalf("Destroy: (%d,%v)", n, err)
	}
	if n, _ := s.StreamGroupDestroy("st", "g1"); n != 0 {
		t.Fatalf("Destroy again: %d", n)
	}
}

func TestReadGroupNewDeliversAndTracksPEL(t *testing.T) {
	s := New()
	s.StreamAdd("st", true, StreamID{}, []string{"f", "v1"}, true)
	s.StreamAdd("st", true, StreamID{}, []string{"f", "v2"}, true)
	s.StreamAdd("st", true, StreamID{}, []string{"f", "v3"}, true)
	mustCreateGroup(t, s, "st", "g", "0-0", false)

	// '>' 首次投喂：count=1 截断，PEL 入账
	entries, exists, err := s.StreamReadGroupNew("st", "g", "alice", 2, false)
	if err != nil || !exists || len(entries) != 2 {
		t.Fatalf("first read: (%v,%v,%v)", entries, exists, err)
	}
	if entries[0].ID.MS == 0 || entries[1].ID.less(entries[0].ID) {
		t.Fatalf("delivery order: %v", entries)
	}
	// lastDelivered 已推进：再次 '>' 拿到剩余 1 条
	entries2, _, _ := s.StreamReadGroupNew("st", "g", "bob", 0, false)
	if len(entries2) != 1 {
		t.Fatalf("second read should get the tail, got %d", len(entries2))
	}
	// PEL 状态：两个消费者各持所投条目
	sum, err := s.StreamXPendingSummary("st", "g")
	if err != nil || sum.Count != 3 || !sum.Has {
		t.Fatalf("summary: %+v %v", sum, err)
	}
	if sum.Consumers[0].Name != "alice" || sum.Consumers[0].Count != 2 ||
		sum.Consumers[1].Name != "bob" || sum.Consumers[1].Count != 1 {
		t.Fatalf("per-consumer: %+v", sum.Consumers)
	}
	items, _ := s.StreamXPendingDetail("st", "g",
		StreamBound{Inf: -1}, StreamBound{Inf: 1}, 10, "alice")
	if len(items) != 2 || items[0].Consumer != "alice" || items[0].Count != 1 {
		t.Fatalf("detail: %+v", items)
	}
	// 全部投喂完：null
	entries3, exists3, _ := s.StreamReadGroupNew("st", "g", "alice", 0, false)
	if exists3 == false || len(entries3) != 0 {
		t.Fatalf("exhausted stream: (%v,%v)", entries3, exists3)
	}
	// NOGROUP
	if _, _, err := s.StreamReadGroupNew("st", "nope", "a", 0, false); err == nil ||
		err.Error() != "NOGROUP No such consumer group 'nope' for key name 'st'" {
		t.Fatalf("NOGROUP: %v", err)
	}
	// key 缺失
	if _, exists, _ := s.StreamReadGroupNew("missing", "g", "a", 0, false); exists {
		t.Fatal("missing key must report exists=false")
	}
	// NOACK：不进 PEL
	mustCreateGroup(t, s, "st2", "g", "0-0", true)
	s.StreamAdd("st2", true, StreamID{}, []string{"f", "x"}, true)
	if _, _, err := s.StreamReadGroupNew("st2", "g", "c1", 0, true); err != nil {
		t.Fatalf("noack read: %v", err)
	}
	sum2, _ := s.StreamXPendingSummary("st2", "g")
	if sum2.Count != 0 {
		t.Fatalf("NOACK must skip PEL, got %d", sum2.Count)
	}
	// 但 lastDelivered 已推进：再次 '>' 为空
	again, _, _ := s.StreamReadGroupNew("st2", "g", "c1", 0, false)
	if len(again) != 0 {
		t.Fatalf("NOACK must still advance lastDelivered, got %v", again)
	}
}

func TestReadGroupHistoryAndGhostCleanup(t *testing.T) {
	s := New()
	s.StreamAdd("st", false, StreamID{MS: 1}, []string{"f", "a"}, true)
	s.StreamAdd("st", false, StreamID{MS: 2}, []string{"f", "b"}, true)
	s.StreamAdd("st", false, StreamID{MS: 3}, []string{"f", "c"}, true)
	mustCreateGroup(t, s, "st", "g", "0-0", false)
	s.StreamReadGroupNew("st", "g", "alice", 0, false) // 3 条全进 alice 的 PEL

	// history：ID > start 的待确认条目，count++
	entries, err := s.StreamReadGroupHistory("st", "g", "alice", StreamID{MS: 1}, 10)
	if err != nil || len(entries) != 2 || entries[0].ID.MS != 2 {
		t.Fatalf("history: (%v,%v)", entries, err)
	}
	items, _ := s.StreamXPendingDetail("st", "g",
		StreamBound{Inf: -1}, StreamBound{Inf: 1}, 10, "")
	for _, it := range items {
		want := int64(1)
		if it.ID.MS > 1 { // 2-0/3-0 被本次 history 再投递
			want = 2
		}
		if it.Count != want {
			t.Fatalf("delivery count: %+v (want %d)", it, want)
		}
	}
	// 只服务本人 PEL（bob 的 PEL 为空）
	entries, _ = s.StreamReadGroupHistory("st", "g", "bob", StreamID{}, 10)
	if len(entries) != 0 {
		t.Fatalf("bob history must be empty, got %v", entries)
	}
	// 幽灵清理：XDEL 2-0 后 history 不再返回它，PEL 同步移除
	s.StreamDel("st", StreamID{MS: 2})
	entries, _ = s.StreamReadGroupHistory("st", "g", "alice", StreamID{MS: 1}, 10)
	if len(entries) != 1 || entries[0].ID.MS != 3 {
		t.Fatalf("ghost must be skipped: %v", entries)
	}
	sum, _ := s.StreamXPendingSummary("st", "g")
	if sum.Count != 2 {
		t.Fatalf("ghost must leave PEL, count=%d", sum.Count)
	}
}

func TestXack(t *testing.T) {
	s := New()
	s.StreamAdd("st", false, StreamID{MS: 1}, []string{"f", "a"}, true)
	s.StreamAdd("st", false, StreamID{MS: 2}, []string{"f", "b"}, true)
	mustCreateGroup(t, s, "st", "g", "0-0", false)
	s.StreamReadGroupNew("st", "g", "alice", 0, false)
	// XACK 部分确认
	n, err := s.StreamXack("st", "g", StreamID{MS: 1}, StreamID{MS: 9})
	if err != nil || n != 1 {
		t.Fatalf("Xack: (%d,%v)", n, err)
	}
	sum, _ := s.StreamXPendingSummary("st", "g")
	if sum.Count != 1 || !sum.Min.IsZero() == false || sum.Min.MS != 2 {
		t.Fatalf("after ack: %+v", sum)
	}
	// 缺失 key/组 → 0 不报错
	if n, err := s.StreamXack("missing", "g", StreamID{MS: 1}); err != nil || n != 0 {
		t.Fatalf("Xack missing key: (%d,%v)", n, err)
	}
	if n, err := s.StreamXack("st", "nope", StreamID{MS: 1}); err != nil || n != 0 {
		t.Fatalf("Xack missing group: (%d,%v)", n, err)
	}
	// WRONGTYPE
	s.Set("str", "x", 0)
	if _, err := s.StreamXack("str", "g", StreamID{MS: 1}); err != ErrWrongType {
		t.Fatalf("Xack wrongtype: %v", err)
	}
}

func TestXPendingErrorsAndBounds(t *testing.T) {
	s := New()
	mustCreateGroup(t, s, "st", "g", "0-0", true)
	// key/组缺失 → NOGROUP
	if _, err := s.StreamXPendingSummary("missing", "g"); err == nil ||
		err.Error() != "NOGROUP No such consumer group 'g' for key name 'missing'" {
		t.Fatalf("summary missing: %v", err)
	}
	if _, err := s.StreamXPendingDetail("st", "nope",
		StreamBound{Inf: -1}, StreamBound{Inf: 1}, 10, ""); err == nil {
		t.Fatal("detail missing group must error")
	}
	// 空组：summary 全零、无消费者
	sum, err := s.StreamXPendingSummary("st", "g")
	if err != nil || sum.Count != 0 || sum.Has || len(sum.Consumers) != 0 {
		t.Fatalf("empty summary: %+v %v", sum, err)
	}
	// WRONGTYPE
	s.Set("str", "x", 0)
	if _, err := s.StreamXPendingSummary("str", "g"); err != ErrWrongType {
		t.Fatalf("summary wrongtype: %v", err)
	}
}

func TestXClaimSemantics(t *testing.T) {
	s := New()
	s.StreamAdd("st", false, StreamID{MS: 1}, []string{"f", "a"}, true)
	s.StreamAdd("st", false, StreamID{MS: 2}, []string{"f", "b"}, true)
	mustCreateGroup(t, s, "st", "g", "0-0", false)
	s.StreamReadGroupNew("st", "g", "alice", 0, false)

	// 把 1-0 的投递时间回拨 60s（模拟久未确认），2-0 保持新鲜
	now := time.Now().UnixMilli()
	old, err := s.StreamXClaim("st", "g", "alice", 0, []StreamID{{MS: 1}},
		ClaimOpts{TimeMS: now - 60_000, SetTime: true})
	if err != nil || len(old) != 1 {
		t.Fatalf("backdate claim: (%v,%v)", old, err)
	}

	// min-idle 门：2-0（新鲜）被跳过，1-0（空闲 60s）被认领到 bob
	res, err := s.StreamXClaim("st", "g", "bob", 30_000,
		[]StreamID{{MS: 2}, {MS: 1}}, ClaimOpts{})
	if err != nil || len(res) != 1 || res[0].ID.MS != 1 {
		t.Fatalf("min-idle claim: (%v,%v)", res, err)
	}
	items, _ := s.StreamXPendingDetail("st", "g",
		StreamBound{Inf: -1}, StreamBound{Inf: 1}, 10, "")
	if items[0].Consumer != "bob" || items[0].ID.MS != 1 {
		t.Fatalf("owner change: %+v", items)
	}
	// 认领是再投递：初始 1 → 回拨 claim ++ → 2 → 认领 ++ → 3
	if items[0].Count != 3 {
		t.Fatalf("count after claim: %+v", items)
	}
	// FORCE：不在 PEL 的条目强行置入（3-0 不存在；2-0 已在 alice 的 PEL——
	// 先 XACK 移除它再 FORCE）
	s.StreamXack("st", "g", StreamID{MS: 2})
	res, err = s.StreamXClaim("st", "g", "carol", 0, []StreamID{{MS: 2}},
		ClaimOpts{Force: true})
	if err != nil || len(res) != 1 || res[0].Entry.Fields[1] != "b" {
		t.Fatalf("force claim: (%v,%v)", res, err)
	}
	sum, _ := s.StreamXPendingSummary("st", "g")
	if sum.Count != 2 {
		t.Fatalf("after force: %d", sum.Count)
	}
	// JUSTID：只改归属，不动计数
	n0 := items[0].Count
	res, _ = s.StreamXClaim("st", "g", "dave", 0, []StreamID{{MS: 1}},
		ClaimOpts{JustID: true})
	if len(res) != 1 {
		t.Fatalf("justid claim: %v", res)
	}
	items, _ = s.StreamXPendingDetail("st", "g",
		StreamBound{Inf: -1}, StreamBound{Inf: 1}, 10, "dave")
	if items[0].Count != n0 {
		t.Fatalf("JUSTID must not change count: %d -> %d", n0, items[0].Count)
	}
	// RETRYCOUNT 直接设定
	s.StreamXClaim("st", "g", "dave", 0, []StreamID{{MS: 1}},
		ClaimOpts{Retry: 7, SetRetry: true})
	items, _ = s.StreamXPendingDetail("st", "g",
		StreamBound{Inf: -1}, StreamBound{Inf: 1}, 10, "dave")
	if items[0].Count != 7 {
		t.Fatalf("RETRYCOUNT: %+v", items)
	}
	// 幽灵：XDEL 1-0 后 XCLAIM 清除 PEL 且不返回
	s.StreamDel("st", StreamID{MS: 1})
	res, err = s.StreamXClaim("st", "g", "eve", 0, []StreamID{{MS: 1}}, ClaimOpts{Force: true})
	if err != nil || len(res) != 0 {
		t.Fatalf("ghost claim: (%v,%v)", res, err)
	}
	sum, _ = s.StreamXPendingSummary("st", "g")
	if sum.Count != 1 {
		t.Fatalf("ghost must leave PEL: %d", sum.Count)
	}
	// NOGROUP
	if _, err := s.StreamXClaim("st", "nope", "x", 0, []StreamID{{MS: 1}}, ClaimOpts{}); err == nil {
		t.Fatal("claim missing group must error")
	}
}

func TestEmptyStreamWithGroupsSurvives(t *testing.T) {
	s := New()
	s.StreamAdd("st", false, StreamID{MS: 1}, []string{"f", "a"}, true)
	mustCreateGroup(t, s, "st", "g", "0-0", false)
	s.StreamReadGroupNew("st", "g", "alice", 0, false)
	// XDEL 清空：有组 → key 保留（空流）
	s.StreamDel("st", StreamID{MS: 1})
	if s.Type("st") != "stream" {
		t.Fatalf("stream with group must survive XDEL, type=%s", s.Type("st"))
	}
	if n, _ := s.StreamLen("st"); n != 0 {
		t.Fatalf("stream must be empty, len=%d", n)
	}
	sum, _ := s.StreamXPendingSummary("st", "g") // 幽灵 PEL 仍在（XDEL 不清 PEL）
	if sum.Count != 1 {
		t.Fatalf("XDEL must not touch PEL: %d", sum.Count)
	}
	// DESTROY 后组没了、流也空 → key 删除
	if n, _ := s.StreamGroupDestroy("st", "g"); n != 1 {
		t.Fatal("destroy must succeed")
	}
	if s.Type("st") != "none" {
		t.Fatalf("emptied stream without groups must be deleted, type=%s", s.Type("st"))
	}
	// XTRIM 同语义
	mustCreateGroup(t, s, "st2", "g", "0-0", true)
	s.StreamAdd("st2", false, StreamID{MS: 5}, []string{"f", "x"}, true)
	s.StreamTrim("st2", 0, nil)
	if s.Type("st2") != "stream" {
		t.Fatalf("stream with group must survive XTRIM, type=%s", s.Type("st2"))
	}
}

func TestGroupSnapshotCanonicalCommands(t *testing.T) {
	s := New()
	s.StreamAdd("st", false, StreamID{MS: 1}, []string{"f", "a"}, true)
	s.StreamAdd("st", false, StreamID{MS: 2}, []string{"f", "b"}, true)
	mustCreateGroup(t, s, "st", "g", "0-0", false)
	s.StreamReadGroupNew("st", "g", "alice", 1, false)
	mustCreateGroup(t, s, "st", "g2", "2-0", false)
	s.StreamGroupCreateConsumer("st", "g2", "idle-guy")

	// BGREWRITEAOF 用的 Snapshot：组重建命令（CREATE + XCLAIM FORCE JUSTID
	// + CREATECONSUMER）必须出现且形态确定
	cmds := s.Snapshot()
	joined := ""
	for _, c := range cmds {
		joined += strings.Join(c, " ") + "\n"
	}
	for _, want := range []string{
		"XGROUP CREATE st g 1-0 MKSTREAM", // lastDelivered 已推进到 1-0
		"XCLAIM st g alice 0 1-0 TIME ",   // PEL 逐条恢复（TIME 前缀）
		" RETRYCOUNT 1 FORCE JUSTID",
		"XGROUP CREATE st g2 2-0 MKSTREAM",
		"XGROUP CREATECONSUMER st g2 idle-guy",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("snapshot missing %q:\n%s", want, joined)
		}
	}
	// 重放 Snapshot 命令 → 组状态一致（lastDelivered/PEL）
	s2 := New()
	replaySnapshot(t, s2, cmds)
	got := s2.StreamGroups("st")
	want := s.StreamGroups("st")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed groups differ:\n got %+v\nwant %+v", got, want)
	}
	// 重放侧 '>' 从 lastDelivered 之后继续：只应得到 2-0
	next, _, _ := s2.StreamReadGroupNew("st", "g", "x", 0, false)
	if len(next) != 1 || next[0].ID.MS != 2 {
		t.Fatalf("replayed lastDelivered wrong: %v", next)
	}
}

func replaySnapshot(t *testing.T, s *Store, cmds [][]string) {
	t.Helper()
	for _, c := range cmds {
		switch c[0] {
		case "XADD":
			id, ok := ParseStreamID(c[2])
			if !ok {
				t.Fatalf("bad id %q", c[2])
			}
			if _, _, err := s.StreamAdd(c[1], false, id, c[3:], true); err != nil {
				t.Fatalf("replay XADD: %v", err)
			}
		case "XGROUP":
			switch c[1] {
			case "CREATE":
				id, _ := ParseStreamID(c[4])
				mk := len(c) > 5 && c[5] == "MKSTREAM"
				if err := s.StreamGroupCreate(c[2], c[3], id, mk); err != nil {
					t.Fatalf("replay XGROUP CREATE: %v", err)
				}
			case "CREATECONSUMER":
				if _, err := s.StreamGroupCreateConsumer(c[2], c[3], c[4]); err != nil {
					t.Fatalf("replay CREATECONSUMER: %v", err)
				}
			default:
				t.Fatalf("unexpected XGROUP %v", c)
			}
		case "XCLAIM":
			id, _ := ParseStreamID(c[5])
			tm, _ := strconv.ParseInt(c[7], 10, 64)
			rc, _ := strconv.ParseInt(c[9], 10, 64)
			if _, err := s.StreamXClaim(c[1], c[2], c[3], 0, []StreamID{id},
				ClaimOpts{TimeMS: tm, SetTime: true, Retry: rc, SetRetry: true,
					Force: true, JustID: true}); err != nil {
				t.Fatalf("replay XCLAIM: %v", err)
			}
		default:
			t.Fatalf("unexpected command %v", c)
		}
	}
}

// TestGroupRestoreGroups 直接验证 StreamRestoreGroups（RDB kind6 回放入口）。
func TestGroupRestoreGroups(t *testing.T) {
	s := New()
	s.StreamRestore("st", []StreamEntry{
		{ID: StreamID{MS: 1}, Fields: []string{"f", "a"}},
		{ID: StreamID{MS: 2}, Fields: []string{"f", "b"}},
	})
	want := []GroupState{{
		Name:          "g",
		LastDelivered: StreamID{MS: 1},
		Consumers:     []string{"alice", "bob"},
		Pel: []PelState{
			{ID: StreamID{MS: 1}, Consumer: "alice", DeliveryMS: 1000, Count: 2},
		},
	}}
	s.StreamRestoreGroups("st", want)
	got := s.StreamGroups("st")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restore mismatch:\n got %+v\nwant %+v", got, want)
	}
	// PEL 状态可查询（canonicalFor 读回路径）
	pel := s.StreamGroupPel("st", "g", []StreamID{{MS: 1}, {MS: 2}})
	if len(pel) != 1 || pel[0].Consumer != "alice" || pel[0].Count != 2 ||
		pel[0].DeliveryMS != 1000 {
		t.Fatalf("StreamGroupPel: %+v", pel)
	}
	// 非法目标：非流 key 静默忽略
	s.Set("str", "x", 0)
	s.StreamRestoreGroups("str", want) // 不 panic 即可
	if pel := s.StreamGroupPel("missing", "g", nil); pel != nil {
		t.Fatalf("pel on missing key must be nil: %v", pel)
	}
}
