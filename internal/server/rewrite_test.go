package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// TestBGRewriteAOFCompactsAndReplays 制造大量可压缩历史（RPUSH/LTRIM、
// HSET/HDEL、SADD/SREM/SPOP、ZINCRBY、SETEX），重写后文件变为每 key 一条
// 的 canonical 命令集，重启回放状态一致，且重写后的新写入落在替换后的文件里。
func TestBGRewriteAOFCompactsAndReplays(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s1, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	ap := func(v resp.Value) { s1.apply(v) }
	ap(mkCmd("SET", "str", "hello"))
	ap(mkCmd("SETEX", "exp", "100", "x"))
	ap(mkCmd("RPUSH", "big", "a", "b", "c", "d", "e"))
	ap(mkCmd("LTRIM", "big", "2", "-1")) // 前 2 条 RPUSH 历史作废
	ap(mkCmd("HSET", "h", "f1", "v1", "f2", "v2"))
	ap(mkCmd("HDEL", "h", "f1"))
	ap(mkCmd("SADD", "st", "m1", "m2", "m3"))
	ap(mkCmd("SREM", "st", "m2"))
	ap(mkCmd("SPOP", "st")) // 随机弹一个（m1/m3 之一）→ 剩 1 个
	ap(mkCmd("ZADD", "z", "1", "m1", "2", "m2"))
	ap(mkCmd("ZINCRBY", "z", "3", "m1"))
	ap(mkCmd("GET", "str")) // 读命令不落盘

	before, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) < 10 {
		t.Fatalf("expected a bloated AOF before rewrite, got %d commands", len(before))
	}

	wantSimple(t, "BGREWRITEAOF", s1.dispatch(mkCmd("BGREWRITEAOF")),
		"Background append only file rewriting started")

	after, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) >= len(before) {
		t.Fatalf("rewrite must compact: before=%d after=%d", len(before), len(after))
	}
	// 每条命令的首 token 只能是 canonical 形式
	for _, v := range after {
		head := strings.ToUpper(v.Arr[0].Str)
		switch head {
		case "SET", "RPUSH", "HSET", "SADD", "ZADD":
		default:
			t.Fatalf("non-canonical command %q survived rewrite", head)
		}
	}

	// 重写后的新写入必须落到替换后的文件（句柄已换新）
	ap(mkCmd("SET", "afterrewrite", "ok"))
	s1.Close()

	s2, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulk(t, "str", s2.dispatch(mkCmd("GET", "str")), "hello")
	wantBulkArray(t, "big", s2.dispatch(mkCmd("LRANGE", "big", "0", "-1")),
		[]string{"c", "d", "e"})
	wantBulkArray(t, "h", s2.dispatch(mkCmd("HGETALL", "h")), []string{"f2", "v2"})
	wantInt(t, "scard st", s2.dispatch(mkCmd("SCARD", "st")), 1)
	wantInt(t, "m2 gone", s2.dispatch(mkCmd("SISMEMBER", "st", "m2")), 0)
	// SPOP 的随机性被 SREM 重写固定：m1/m3 恰好存活一个，重放后一致
	m1, m3 := s2.dispatch(mkCmd("SISMEMBER", "st", "m1")), s2.dispatch(mkCmd("SISMEMBER", "st", "m3"))
	if (m1.Num == 1) == (m3.Num == 1) {
		t.Fatalf("exactly one of m1/m3 must survive SPOP: m1=%d m3=%d", m1.Num, m3.Num)
	}
	wantBulk(t, "zscore m1", s2.dispatch(mkCmd("ZSCORE", "z", "m1")), "4")
	wantBulk(t, "afterrewrite", s2.dispatch(mkCmd("GET", "afterrewrite")), "ok")
	rem, ok := s2.store.TTL("exp")
	if !ok || rem <= 0 || rem > 100 {
		t.Fatalf("exp TTL after rewrite+replay: rem=%d ok=%v", rem, ok)
	}
}

// TestBGRewriteAOFDisabled 未开启 AOF 时 BGREWRITEAOF 报错（对齐 Redis 文案）。
func TestBGRewriteAOFDisabled(t *testing.T) {
	s := New()
	wantErr(t, "BGREWRITEAOF no aof", s.dispatch(mkCmd("BGREWRITEAOF")),
		"Append only file is disabled")
}

// TestRewriteEmptySnapshot 空库重写后 AOF 变为空文件，重启为空库。
func TestRewriteEmptySnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s1, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s1.apply(mkCmd("SET", "a", "1"))
	s1.apply(mkCmd("DEL", "a"))
	s1.dispatch(mkCmd("BGREWRITEAOF"))
	s1.Close()

	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 0 {
		t.Fatalf("expected empty AOF after rewriting an empty store, got %d", len(cmds))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("rewritten file must exist: %v", err)
	}
}
