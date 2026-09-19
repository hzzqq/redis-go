package persist

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/store"
)

// TestRDBRoundtrip 五种类型 + TTL + 二进制内容 + 大集合，保存→加载逐字段一致。
func TestRDBRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")
	keys := []store.Exported{
		{Key: "s", Kind: "string", Str: "hello"},
		{Key: "exp", Kind: "string", Str: "x",
			Expiry: time.UnixMilli(time.Now().Add(90 * time.Second).UnixMilli())},
		{Key: "l", Kind: "list", List: []string{"a", "", "c"}}, // 含空串元素
		{Key: "h", Kind: "hash", Hash: [][2]string{{"f2", "v2"}, {"f1", ""}}},
		{Key: "st", Kind: "set", Set: []string{"m1", "m2"}},
		{Key: "z", Kind: "zset", ZItems: []store.ZItem{{Member: "bob", Score: 8}, {Member: "alice", Score: 10.5}}},
		{Key: "bin", Kind: "string", Str: "\x00\xff中文\r\n"},
		{Key: "big", Kind: "set", Set: bigMembers(17000)}, // 跨过小长度编码边界
		// kind 6：stream + 消费者组（PEL 含多消费者、多投递计数）
		{Key: "grp", Kind: "stream",
			Stream: []store.StreamEntry{
				{ID: store.StreamID{MS: 1, Seq: 0}, Fields: []string{"f", "a"}},
				{ID: store.StreamID{MS: 2, Seq: 0}, Fields: []string{"f", "b"}},
			},
			Groups: []store.GroupState{{
				Name:          "g1",
				LastDelivered: store.StreamID{MS: 2, Seq: 0},
				Consumers:     []string{"alice", "bob"},
				Pel: []store.PelState{
					{ID: store.StreamID{MS: 1, Seq: 0}, Consumer: "alice",
						DeliveryMS: 1111, Count: 2},
					{ID: store.StreamID{MS: 2, Seq: 0}, Consumer: "bob",
						DeliveryMS: 2222, Count: 1},
				},
			}, {
				Name:          "g2",
				LastDelivered: store.StreamID{MS: 1, Seq: 0},
				Consumers:     []string{"solo"},
			}},
		},
	}
	if err := SaveRDB(path, keys); err != nil {
		t.Fatal(err)
	}
	got, err := LoadRDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), len(got))
	}
	for i, want := range keys {
		if !reflect.DeepEqual(got[i], want) {
			t.Fatalf("record %d (%s) mismatch:\n got %+v\nwant %+v", i, want.Key, got[i], want)
		}
	}
}

func bigMembers(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "member-" + string(rune('a'+i%26)) + "-" + strconv.Itoa(i)
	}
	return out
}

// TestRDBCorruption CRC 翻转 / 截断都必须报错（RDB 不容忍损坏，对齐 Redis）。
func TestRDBCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")
	if err := SaveRDB(path, []store.Exported{{Key: "s", Kind: "string", Str: "hello"}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 40 {
		t.Fatalf("unexpectedly small rdb: %d bytes", len(data))
	}

	flip := append([]byte(nil), data...)
	flip[30] ^= 0xFF // payload 中部一个字节
	bad := filepath.Join(t.TempDir(), "bad.rdb")
	if err := os.WriteFile(bad, flip, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRDB(bad); err == nil {
		t.Fatal("expected CRC mismatch error")
	}

	trunc := filepath.Join(t.TempDir(), "trunc.rdb")
	if err := os.WriteFile(trunc, data[:len(data)-10], 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRDB(trunc); err == nil {
		t.Fatal("expected truncation error")
	}

	garbage := filepath.Join(t.TempDir(), "garbage.rdb")
	if err := os.WriteFile(garbage, []byte("not an rdb at all........"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRDB(garbage); err == nil {
		t.Fatal("expected bad-magic error")
	}
}

// TestRDBMissingFile 缺文件 = 干净起步（nil, nil）。
func TestRDBMissingFile(t *testing.T) {
	got, err := LoadRDB(filepath.Join(t.TempDir(), "nope.rdb"))
	if err != nil || got != nil {
		t.Fatalf("missing file: got %v err=%v", got, err)
	}
}
