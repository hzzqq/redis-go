package store

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSnapshotCanonicalForms 验证快照对每种类型输出恰好一条 canonical 命令：
// string→SET(带 PXAT)、list→RPUSH 全量、hash→HSET 插入序、set→SADD 排序、
// zset→ZADD 跳表序；过期 key 跳过。
func TestSnapshotCanonicalForms(t *testing.T) {
	s := New()
	s.Set("str", "hello", 0)
	s.SetAt("exp", "v", time.Now().Add(100*time.Second))
	s.ListPush("lst", false, "a", "b")
	s.HashSet("h", [][2]string{{"f1", "v1"}, {"f2", "v2"}})
	s.SetAdd("st", []string{"b", "a"})
	s.ZAdd("z", []ZItem{{Member: "m2", Score: 2}, {Member: "m1", Score: 1.5}})
	s.SetAt("gone", "x", time.Now().Add(-time.Second)) // 已过期，不进快照

	got := make(map[string]bool)
	for _, c := range s.Snapshot() {
		got[strings.Join(c, " ")] = true
	}
	want := map[string]bool{
		"SET str hello":      true,
		"RPUSH lst a b":      true,
		"HSET h f1 v1 f2 v2": true,
		"SADD st a b":        true, // 成员排序输出
		"ZADD z 1.5 m1 2 m2": true, // score 升序（跳表序）
	}
	for cmd := range want {
		if !got[cmd] {
			t.Errorf("snapshot missing %q, got %v", cmd, got)
		}
	}
	// TTL key：前缀 SET exp v PXAT <毫秒时间戳>
	found := false
	for cmd := range got {
		if strings.HasPrefix(cmd, "SET exp v PXAT ") {
			found = true
			delete(got, cmd)
			break
		}
	}
	if !found {
		t.Errorf("snapshot missing SET exp v PXAT, got %v", got)
	}
	if len(got) != len(want) {
		t.Fatalf("unexpected extra commands: %v", got)
	}
	if got["SET gone x"] {
		t.Error("expired key must not appear in snapshot")
	}
}

// TestSnapshotRoundTripsThroughCommands 把快照命令逐条回放进新 store，
// 验证与原 store 状态一致（score/TTL 精度、集合成员、list 序）。
func TestSnapshotRoundTripsThroughCommands(t *testing.T) {
	s := New()
	s.Set("str", "hello", 0)
	s.ListPush("lst", false, "a", "b")
	s.HashSet("h", [][2]string{{"f1", "v1"}})
	s.SetAdd("st", []string{"x"})
	s.ZAdd("z", []ZItem{{Member: "m1", Score: 0.1}, {Member: "m2", Score: 1e21}})

	s2 := New()
	for _, c := range s.Snapshot() {
		switch c[0] {
		case "SET":
			var exp time.Time
			for i := 1; i < len(c); i++ {
				if c[i] == "PXAT" {
					ms, err := strconv.ParseInt(c[i+1], 10, 64)
					if err != nil {
						t.Fatal(err)
					}
					exp = time.UnixMilli(ms)
				}
			}
			s2.SetAt(c[1], c[2], exp)
		case "RPUSH":
			s2.ListPush(c[1], false, c[2:]...)
		case "HSET":
			pairs := make([][2]string, 0, (len(c)-2)/2)
			for i := 2; i < len(c); i += 2 {
				pairs = append(pairs, [2]string{c[i], c[i+1]})
			}
			if _, err := s2.HashSet(c[1], pairs); err != nil {
				t.Fatal(err)
			}
		case "SADD":
			if _, err := s2.SetAdd(c[1], c[2:]); err != nil {
				t.Fatal(err)
			}
		case "ZADD":
			pairs := make([]ZItem, 0, (len(c)-2)/2)
			for i := 2; i < len(c); i += 2 {
				score, err := strconv.ParseFloat(c[i], 64)
				if err != nil {
					t.Fatal(err)
				}
				pairs = append(pairs, ZItem{Member: c[i+1], Score: score})
			}
			if _, err := s2.ZAdd(c[1], pairs); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unexpected command %v", c)
		}
	}
	if v, _, _ := s2.Get("str"); v != "hello" {
		t.Errorf("str = %q", v)
	}
	if items, _ := s2.ListRange("lst", 0, -1); len(items) != 2 || items[0] != "a" {
		t.Errorf("lst = %v", items)
	}
	if ok, _ := s2.SetIsMember("st", "x"); !ok {
		t.Error("st missing x")
	}
	if score, found, _ := s2.ZScore("z", "m2"); !found || score != 1e21 {
		t.Errorf("z m2 score = %v (round-trip must be exact)", score)
	}
	if score, found, _ := s2.ZScore("z", "m1"); !found || score != 0.1 {
		t.Errorf("z m1 score = %v", score)
	}
}
