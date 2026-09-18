// Phase 9 server 层测试：glob 匹配、SCAN/SSCAN/HSCAN/ZSCAN 协议与完整遍历、
// OBJECT ENCODING、LMOVE/LINSERT/LPOS、SET NX/XX/GET/KEEPTTL、EXPIRE 条件选项、
// AOF canonical 行为（条件未命中不落盘、选项剥离）。
package server

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// replyStrings 把 bulkArray 展开成 []string。
func replyStrings(t *testing.T, name string, reply resp.Value) []string {
	t.Helper()
	if reply.Type != resp.Array {
		t.Fatalf("%s: expected Array, got type=%c", name, reply.Type)
	}
	out := make([]string, 0, len(reply.Arr))
	for _, v := range reply.Arr {
		out = append(out, v.Str)
	}
	return out
}

// replyInts 把 Integer 数组展开成 []int64（LPOS COUNT 回复）。
func replyInts(t *testing.T, name string, reply resp.Value) []int64 {
	t.Helper()
	if reply.Type != resp.Array {
		t.Fatalf("%s: expected Array, got type=%c", name, reply.Type)
	}
	out := make([]int64, 0, len(reply.Arr))
	for _, v := range reply.Arr {
		if v.Type != resp.Integer {
			t.Fatalf("%s: expected Integer element, got type=%c", name, v.Type)
		}
		out = append(out, v.Num)
	}
	return out
}

// ---------------- glob ----------------

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"h?llo", "hello", true},
		{"h?llo", "heello", false},
		{"h*llo", "heeeello", true},
		{"h*llo", "hllo", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-b]llo", "hbllo", true},
		{"h[a-b]llo", "hcllo", false},
		{"h\\?llo", "h?llo", true},
		{"h\\?llo", "hallo", false},
		{"a\\*b", "a*b", true},
		{"a\\*b", "axb", false},
		{"[]]", "]", true},     // [ 后第一个 ] 是字面量
		{"[a\\]c]", "a", true}, // 区间内转义 ]（按字面量 ']' 参与）
		{"user:*:name", "user:42:name", true},
		{"user:*:name", "user:42:age", false},
		{"", "", true},
		{"", "x", false},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.s); got != c.want {
			t.Fatalf("globMatch(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

// ---------------- SCAN ----------------

func TestScanPaginationCoversAll(t *testing.T) {
	s := New()
	const total = 25
	for i := 0; i < total; i++ {
		wantSimple(t, "SET", s.dispatch(mkCmd("SET", fmt.Sprintf("key%02d", i), "v")), "OK")
	}
	seen := map[string]bool{}
	cursor := "0"
	pages := 0
	for {
		reply := s.dispatch(mkCmd("SCAN", cursor, "COUNT", "10"))
		if reply.Type != resp.Array || len(reply.Arr) != 2 {
			t.Fatalf("SCAN reply shape: type=%c", reply.Type)
		}
		next := reply.Arr[0].Str
		for _, k := range replyStrings(t, "SCAN elems", reply.Arr[1]) {
			seen[k] = true
		}
		pages++
		if next == "0" {
			break
		}
		if next == cursor {
			t.Fatalf("cursor stuck: %s", next)
		}
		cursor = next
		if pages > 100 {
			t.Fatal("SCAN did not terminate")
		}
	}
	if len(seen) != total {
		t.Fatalf("SCAN must cover all keys exactly once: %d/%d", len(seen), total)
	}
}

func TestScanOptions(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "user:1", "a"))
	s.dispatch(mkCmd("SET", "user:2", "b"))
	s.dispatch(mkCmd("RPUSH", "list:1", "x"))
	s.dispatch(mkCmd("HSET", "hash:1", "f", "v"))

	// MATCH
	reply := s.dispatch(mkCmd("SCAN", "0", "MATCH", "user:*"))
	elems := replyStrings(t, "MATCH", reply.Arr[1])
	if len(elems) != 2 {
		t.Fatalf("MATCH user:*: %v", elems)
	}
	// TYPE
	reply = s.dispatch(mkCmd("SCAN", "0", "TYPE", "list"))
	if elems := replyStrings(t, "TYPE", reply.Arr[1]); len(elems) != 1 || elems[0] != "list:1" {
		t.Fatalf("TYPE list: %v", elems)
	}
	// 无效 cursor
	wantErr(t, "bad cursor", s.dispatch(mkCmd("SCAN", "abc")), "invalid cursor")
	// COUNT 0
	wantErr(t, "COUNT 0", s.dispatch(mkCmd("SCAN", "0", "COUNT", "0")), "COUNT")
	// 未知选项
	wantErr(t, "bad option", s.dispatch(mkCmd("SCAN", "0", "WHAT", "x")), "syntax error")
}

func TestSScanHScanZScan(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SADD", "st", "a", "b", "c", "d"))
	s.dispatch(mkCmd("HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3"))
	s.dispatch(mkCmd("ZADD", "z", "1", "m1", "2", "m2", "3", "m3"))

	// SSCAN：COUNT 2 分页拿全 4 个成员（排序序）
	cursor := "0"
	seen := map[string]bool{}
	for i := 0; ; i++ {
		reply := s.dispatch(mkCmd("SSCAN", "st", cursor, "COUNT", "2"))
		cursor = reply.Arr[0].Str
		for _, m := range replyStrings(t, "SSCAN", reply.Arr[1]) {
			seen[m] = true
		}
		if cursor == "0" || i > 10 {
			break
		}
	}
	if len(seen) != 4 {
		t.Fatalf("SSCAN coverage: %v", seen)
	}
	// SSCAN MATCH
	reply := s.dispatch(mkCmd("SSCAN", "st", "0", "MATCH", "a*"))
	if got := replyStrings(t, "SSCAN MATCH", reply.Arr[1]); len(got) != 1 || got[0] != "a" {
		t.Fatalf("SSCAN MATCH: %v", got)
	}
	// HSCAN：扁平 [field, value, ...]
	reply = s.dispatch(mkCmd("HSCAN", "h", "0", "COUNT", "100"))
	flat := replyStrings(t, "HSCAN", reply.Arr[1])
	if len(flat) != 6 || flat[0] != "f1" || flat[1] != "v1" || flat[4] != "f3" || flat[5] != "v3" {
		t.Fatalf("HSCAN flat pairs: %v", flat)
	}
	// HSCAN MATCH 按 field 过滤
	reply = s.dispatch(mkCmd("HSCAN", "h", "0", "MATCH", "f2"))
	if flat := replyStrings(t, "HSCAN MATCH", reply.Arr[1]); len(flat) != 2 || flat[0] != "f2" {
		t.Fatalf("HSCAN MATCH: %v", flat)
	}
	// ZSCAN：扁平 [member, score, ...]，score 为跳表序
	reply = s.dispatch(mkCmd("ZSCAN", "z", "0", "COUNT", "100"))
	flat = replyStrings(t, "ZSCAN", reply.Arr[1])
	if len(flat) != 6 || flat[0] != "m1" || flat[1] != "1" || flat[5] != "3" {
		t.Fatalf("ZSCAN flat pairs: %v", flat)
	}
	// key 不存在 → [0, []]
	reply = s.dispatch(mkCmd("SSCAN", "nope", "0"))
	if reply.Arr[0].Str != "0" || len(reply.Arr[1].Arr) != 0 {
		t.Fatalf("SSCAN missing key: %v", reply)
	}
	// WRONGTYPE
	wantErr(t, "SSCAN wrong type", s.dispatch(mkCmd("SSCAN", "h", "0")), "WRONGTYPE")
}

// ---------------- OBJECT ENCODING ----------------

func TestObjectEncoding(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "i", "42"))
	wantBulk(t, "int", s.dispatch(mkCmd("OBJECT", "ENCODING", "i")), "int")
	s.dispatch(mkCmd("SET", "e", "hello"))
	wantBulk(t, "embstr", s.dispatch(mkCmd("OBJECT", "ENCODING", "e")), "embstr")
	s.dispatch(mkCmd("SET", "r", string(make([]byte, 100))))
	wantBulk(t, "raw", s.dispatch(mkCmd("OBJECT", "ENCODING", "r")), "raw")
	s.dispatch(mkCmd("RPUSH", "l", "a"))
	wantBulk(t, "list", s.dispatch(mkCmd("OBJECT", "ENCODING", "l")), "listpack")
	s.dispatch(mkCmd("SADD", "ints", "1", "2"))
	wantBulk(t, "intset", s.dispatch(mkCmd("OBJECT", "ENCODING", "ints")), "intset")
	s.dispatch(mkCmd("ZADD", "z", "1", "m"))
	wantBulk(t, "zset", s.dispatch(mkCmd("OBJECT", "ENCODING", "z")), "listpack")
	wantNull(t, "missing", s.dispatch(mkCmd("OBJECT", "ENCODING", "nope")))
	wantErr(t, "bad subcommand", s.dispatch(mkCmd("OBJECT", "REFCOUNT", "i")), "Unknown subcommand")
}

// ---------------- LMOVE / LINSERT / LPOS ----------------

func TestLMove(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("RPUSH", "src", "1", "2", "3"))
	// LMOVE src dst LEFT RIGHT：头弹尾推
	wantBulk(t, "moved", s.dispatch(mkCmd("LMOVE", "src", "dst", "LEFT", "RIGHT")), "1")
	if got := replyStrings(t, "src", s.dispatch(mkCmd("LRANGE", "src", "0", "-1"))); len(got) != 2 || got[0] != "2" {
		t.Fatalf("src: %v", got)
	}
	if got := replyStrings(t, "dst", s.dispatch(mkCmd("LRANGE", "dst", "0", "-1"))); len(got) != 1 || got[0] != "1" {
		t.Fatalf("dst: %v", got)
	}
	// 同 key 轮转
	s.dispatch(mkCmd("DEL", "dst"))
	s.dispatch(mkCmd("RPUSH", "rot", "a", "b", "c"))
	wantBulk(t, "rotate", s.dispatch(mkCmd("LMOVE", "rot", "rot", "LEFT", "RIGHT")), "a")
	if got := replyStrings(t, "rot", s.dispatch(mkCmd("LRANGE", "rot", "0", "-1"))); got[0] != "b" || got[2] != "a" {
		t.Fatalf("rotation: %v", got)
	}
	// 源不存在 → null
	wantNull(t, "missing src", s.dispatch(mkCmd("LMOVE", "nope", "dst", "LEFT", "RIGHT")))
	// 方向词错误
	wantErr(t, "bad side", s.dispatch(mkCmd("LMOVE", "rot", "rot", "UP", "RIGHT")), "syntax error")
	// 弹空源删除
	s.dispatch(mkCmd("DEL", "rot"))
	s.dispatch(mkCmd("RPUSH", "one", "x"))
	s.dispatch(mkCmd("LMOVE", "one", "dst", "LEFT", "LEFT"))
	if s.store.Exists("one") {
		t.Fatal("emptied src must be deleted")
	}
}

func TestLInsert(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("RPUSH", "l", "a", "c"))
	wantInt(t, "before", s.dispatch(mkCmd("LINSERT", "l", "BEFORE", "c", "b")), 3)
	if got := replyStrings(t, "after", s.dispatch(mkCmd("LRANGE", "l", "0", "-1"))); got[1] != "b" {
		t.Fatalf("inserted: %v", got)
	}
	wantInt(t, "after", s.dispatch(mkCmd("LINSERT", "l", "AFTER", "a", "x")), 4)
	// pivot 不存在 → -1
	wantInt(t, "missing pivot", s.dispatch(mkCmd("LINSERT", "l", "BEFORE", "zzz", "q")), -1)
	// key 不存在 → 0
	wantInt(t, "missing key", s.dispatch(mkCmd("LINSERT", "nope", "BEFORE", "a", "b")), 0)
	// 词错误
	wantErr(t, "bad side", s.dispatch(mkCmd("LINSERT", "l", "NEAR", "a", "b")), "syntax error")
}

func TestLPos(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("RPUSH", "l", "a", "b", "a", "c", "a", "b"))
	wantInt(t, "first", s.dispatch(mkCmd("LPOS", "l", "a")), 0)
	wantInt(t, "rank 2", s.dispatch(mkCmd("LPOS", "l", "a", "RANK", "2")), 2)
	wantInt(t, "rank -1", s.dispatch(mkCmd("LPOS", "l", "a", "RANK", "-1")), 4)
	// COUNT
	reply := s.dispatch(mkCmd("LPOS", "l", "a", "COUNT", "10"))
	if got := replyInts(t, "COUNT", reply); len(got) != 3 {
		t.Fatalf("COUNT 10: %v", got)
	}
	reply = s.dispatch(mkCmd("LPOS", "l", "a", "RANK", "-1", "COUNT", "2"))
	if got := replyInts(t, "COUNT rev", reply); len(got) != 2 || got[0] != 4 || got[1] != 2 {
		t.Fatalf("COUNT rev: %v", got)
	}
	// MAXLEN 限制总比较量
	reply = s.dispatch(mkCmd("LPOS", "l", "a", "COUNT", "10", "MAXLEN", "3"))
	if got := replyInts(t, "MAXLEN", reply); len(got) != 2 || got[1] != 2 {
		t.Fatalf("MAXLEN 3: %v", got)
	}
	// 无匹配
	wantNull(t, "no match", s.dispatch(mkCmd("LPOS", "l", "zzz")))
	// RANK 0 / COUNT 0
	wantErr(t, "RANK 0", s.dispatch(mkCmd("LPOS", "l", "a", "RANK", "0")), "RANK can't be zero")
	wantErr(t, "COUNT 0", s.dispatch(mkCmd("LPOS", "l", "a", "COUNT", "0")), "COUNT can't be negative")
}

// ---------------- SET NX/XX/GET/KEEPTTL ----------------

func TestSetNxXxGetKeepTTL(t *testing.T) {
	s := New()
	// NX 成功
	wantSimple(t, "NX new", s.dispatch(mkCmd("SET", "k", "v1", "NX")), "OK")
	// NX 失败 → null
	wantNull(t, "NX exists", s.dispatch(mkCmd("SET", "k", "v2", "NX")))
	if v := s.dispatch(mkCmd("GET", "k")); v.Str != "v1" {
		t.Fatalf("NX must not overwrite: %q", v.Str)
	}
	// XX 失败 → null
	wantNull(t, "XX missing", s.dispatch(mkCmd("SET", "other", "v", "XX")))
	// XX 成功
	wantSimple(t, "XX exists", s.dispatch(mkCmd("SET", "k", "v3", "XX")), "OK")
	// GET 返回旧值
	if got := s.dispatch(mkCmd("SET", "k", "v4", "GET")); got.Type != resp.BulkString || got.Str != "v3" {
		t.Fatalf("SET GET old: %q", got.Str)
	}
	// GET + key 不存在 → null 且设置
	if got := s.dispatch(mkCmd("SET", "fresh", "v", "GET")); got.Type != resp.BulkString || !got.Null {
		t.Fatalf("SET GET missing: %v", got)
	}
	if v := s.dispatch(mkCmd("GET", "fresh")); v.Str != "v" {
		t.Fatalf("fresh must be set: %v", v)
	}
	// GET 对 list key → WRONGTYPE
	s.dispatch(mkCmd("RPUSH", "l", "a"))
	wantErr(t, "GET on list", s.dispatch(mkCmd("SET", "l", "x", "GET")), "WRONGTYPE")
	if got, _ := s.store.ListLen("l"); got != 1 {
		t.Fatal("list must be untouched")
	}
	// KEEPTTL
	s.dispatch(mkCmd("SET", "ttl", "v1", "EX", "100"))
	wantSimple(t, "KEEPTTL", s.dispatch(mkCmd("SET", "ttl", "v2", "KEEPTTL")), "OK")
	if rem, ok := s.store.TTL("ttl"); !ok || rem <= 0 || rem > 100 {
		t.Fatalf("KEEPTTL lost the TTL: rem=%d", rem)
	}
	// KEEPTTL + EX 互斥
	wantErr(t, "KEEPTTL+EX", s.dispatch(mkCmd("SET", "ttl", "v3", "KEEPTTL", "EX", "5")), "syntax error")
	// NX+XX 互斥
	wantErr(t, "NX+XX", s.dispatch(mkCmd("SET", "k", "v", "NX", "XX")), "syntax error")
}

// ---------------- EXPIRE NX/XX/GT/LT ----------------

func TestExpireOptions(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "k", "v", "EX", "100"))
	// NX：已有 TTL → 0
	wantInt(t, "NX has ttl", s.dispatch(mkCmd("EXPIRE", "k", "50", "NX")), 0)
	// XX：已有 TTL → 1
	wantInt(t, "XX has ttl", s.dispatch(mkCmd("EXPIRE", "k", "50", "XX")), 1)
	// GT：更小 → 0；更大 → 1
	wantInt(t, "GT smaller", s.dispatch(mkCmd("EXPIRE", "k", "10", "GT")), 0)
	wantInt(t, "GT bigger", s.dispatch(mkCmd("EXPIRE", "k", "200", "GT")), 1)
	// LT：更小 → 1；更大 → 0
	wantInt(t, "LT smaller", s.dispatch(mkCmd("EXPIRE", "k", "150", "LT")), 1)
	wantInt(t, "LT bigger", s.dispatch(mkCmd("EXPIRE", "k", "500", "LT")), 0)
	// 无 TTL key：GT 失败，LT 成功
	s.dispatch(mkCmd("SET", "plain", "v"))
	wantInt(t, "GT no ttl", s.dispatch(mkCmd("EXPIRE", "plain", "10", "GT")), 0)
	wantInt(t, "LT no ttl", s.dispatch(mkCmd("EXPIRE", "plain", "10", "LT")), 1)
	// key 不存在 → 0
	wantInt(t, "missing key", s.dispatch(mkCmd("EXPIRE", "nope", "10", "NX")), 0)
	// 选项冲突
	wantErr(t, "NX+XX", s.dispatch(mkCmd("EXPIRE", "k", "10", "NX", "XX")), "not compatible")
	wantErr(t, "GT+LT", s.dispatch(mkCmd("EXPIRE", "k", "10", "GT", "LT")), "not compatible")
	// PEXPIREAT 同语法
	wantInt(t, "pexpireat GT smaller", s.dispatch(mkCmd("PEXPIREAT", "k", "1", "GT")), 0)
}

// ---------------- AOF canonical 行为 ----------------

// aofHeads 读取 AOF 文件中每条命令的首 token。
func aofHeads(t *testing.T, path string) [][]string {
	t.Helper()
	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	out := make([][]string, 0, len(cmds))
	for _, v := range cmds {
		parts := make([]string, 0, len(v.Arr))
		for _, a := range v.Arr {
			parts = append(parts, a.Str)
		}
		out = append(out, parts)
	}
	return out
}

func TestCanonicalWritePhase9(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close() // 释放 AOF 句柄，否则 t.TempDir 清理在 Windows 上失败
	// 写命令必须走 apply（dispatch 纯执行不落盘）
	s.apply(mkCmd("SET", "k", "v1"))
	// SET NX 失败：状态无变化 → 不落盘
	if got := s.apply(mkCmd("SET", "k", "v2", "NX")); got.Type != resp.BulkString || !got.Null {
		t.Fatal("SET NX must fail on existing key")
	}
	// SET NX 成功（新 key）→ 落盘剥掉 NX
	s.apply(mkCmd("SET", "nk", "v", "NX", "GET"))
	// EXPIRE 条件失败（GT 更小）→ 不落盘
	s.apply(mkCmd("EXPIRE", "k", "100"))
	s.apply(mkCmd("EXPIRE", "k", "10", "GT"))
	// EXPIRE 带选项成功 → PEXPIREAT + 选项保留
	s.apply(mkCmd("EXPIRE", "k", "200", "XX"))
	// LMOVE / LINSERT 原样落盘
	s.apply(mkCmd("RPUSH", "a", "1"))
	s.apply(mkCmd("LMOVE", "a", "b", "LEFT", "RIGHT"))
	s.apply(mkCmd("LINSERT", "b", "BEFORE", "1", "0"))

	heads := aofHeads(t, path)
	var setNx, expireOpt, lmoveSeen, linsertSeen bool
	for _, h := range heads {
		switch {
		case h[0] == "SET" && h[1] == "k" && len(h) > 2 && h[2] == "v2":
			t.Fatalf("failed SET NX must not be logged: %v", h)
		case h[0] == "SET" && h[1] == "nk":
			for _, w := range h {
				if w == "NX" || w == "GET" {
					t.Fatalf("NX/GET must be stripped from AOF: %v", h)
				}
			}
			setNx = true
		case h[0] == "PEXPIREAT" && h[1] == "k" && len(h) == 4 && h[3] == "XX":
			expireOpt = true
		case h[0] == "EXPIRE":
			t.Fatalf("EXPIRE must be absolutized to PEXPIREAT: %v", h)
		case h[0] == "LMOVE" && len(h) == 5:
			lmoveSeen = true
		case h[0] == "LINSERT" && len(h) == 5:
			linsertSeen = true
		}
	}
	if !setNx || !expireOpt || !lmoveSeen || !linsertSeen {
		t.Fatalf("missing canonical forms: setNx=%v expireOpt=%v lmove=%v linsert=%v\n%v",
			setNx, expireOpt, lmoveSeen, linsertSeen, heads)
	}
	// 条件失败的 EXPIRE（GT smaller）不得落盘：数一下 PEXPIREAT k 的条数
	n := 0
	for _, h := range heads {
		if h[0] == "PEXPIREAT" && h[1] == "k" {
			n++
		}
	}
	if n != 2 { // EXPIRE k 100 + EXPIRE k 200 XX
		t.Fatalf("conditional-failed EXPIRE must not be logged: %d PEXPIREAT entries", n)
	}
}

// ---------------- WATCH 覆盖（LMOVE 触碰两个 key） ----------------

func TestWatchLMoveTouchesBothKeys(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("RPUSH", "src", "1"))
	s.dispatch(mkCmd("RPUSH", "dst", "2"))
	cl := &client{chans: make(map[string]struct{})}
	s.cmdWatch(cl, []resp.Value{
		{Type: resp.BulkString, Str: "src"},
		{Type: resp.BulkString, Str: "dst"},
	})
	s.apply(mkCmd("LMOVE", "src", "dst", "LEFT", "RIGHT"))
	if !cl.dirtyCAS {
		t.Fatal("LMOVE must dirty watchers of both src and dst")
	}
}

// ---------------- CONFIG/INFO 汇报 ----------------

func TestAOFsyncReporting(t *testing.T) {
	dir := t.TempDir()
	s, err := NewWithAOF(filepath.Join(dir, "appendonly.aof"))
	if err != nil {
		t.Fatal(err)
	}
	s.ConfigureAOF("everysec")
	reply := s.dispatch(mkCmd("CONFIG", "GET", "appendfsync"))
	if got := replyStrings(t, "CONFIG appendfsync", reply); len(got) != 2 || got[1] != "everysec" {
		t.Fatalf("CONFIG appendfsync: %v", got)
	}
	info := s.dispatch(mkCmd("INFO", "persistence"))
	if info.Type != resp.BulkString {
		t.Fatal("INFO must be bulk")
	}
	if !strings.Contains(info.Str, "aof_fsync:everysec") {
		t.Fatalf("INFO missing aof_fsync: %q", info.Str)
	}
	// 关闭前停掉后台刷盘（Close 由 t.TempDir 清理，不显式调用也安全，
	// 但 AOF goroutine 会持有句柄直到进程结束——测试里显式 Close）
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestConfigSetAppendFsync 回归：CONFIG SET appendfsync 运行期切换（README Phase 9
// 宣称能力，验收冒烟发现 CONFIG SET 仍回 Unsupported CONFIG parameter——server 层
// 漏接线，persist 层 SetFsync 与 ConfigureAOF 能力齐备）。非法值由 ConfigureAOF
// 回退 no；其他参数保持诚实拒绝（对齐 server_test.go 既有 CONFIG SET maxmemory 断言）。
func TestConfigSetAppendFsync(t *testing.T) {
	dir := t.TempDir()
	s, err := NewWithAOF(filepath.Join(dir, "appendonly.aof"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.dispatch(mkCmd("CONFIG", "SET", "appendfsync", "always")); got.Type != resp.SimpleString || got.Str != "OK" {
		t.Fatalf("CONFIG SET appendfsync always: %v", got.Str)
	}
	if got := replyStrings(t, "CONFIG GET after set", s.dispatch(mkCmd("CONFIG", "GET", "appendfsync"))); len(got) != 2 || got[1] != "always" {
		t.Fatalf("CONFIG GET appendfsync after SET: %v", got)
	}
	info := s.dispatch(mkCmd("INFO", "persistence"))
	if !strings.Contains(info.Str, "aof_fsync:always") {
		t.Fatalf("INFO missing aof_fsync:always: %q", info.Str)
	}
	// 非法值回退 no（ConfigureAOF 内置规则）
	if got := s.dispatch(mkCmd("CONFIG", "SET", "appendfsync", "bogus")); got.Type != resp.SimpleString || got.Str != "OK" {
		t.Fatalf("CONFIG SET appendfsync bogus: %v", got.Str)
	}
	if got := replyStrings(t, "CONFIG GET after bogus", s.dispatch(mkCmd("CONFIG", "GET", "appendfsync"))); len(got) != 2 || got[1] != "no" {
		t.Fatalf("bogus must fall back to no: %v", got)
	}
	// 其他参数仍拒绝
	wantErr(t, "CONFIG SET maxmemory", s.dispatch(mkCmd("CONFIG", "SET", "maxmemory", "100")),
		"Unsupported CONFIG parameter")
}
