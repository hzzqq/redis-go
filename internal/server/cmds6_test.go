package server

// Phase 6 新命令的 dispatch 层测试：MGET/MSET、ZRANGEBYSCORE 族、
// ZRANDMEMBER、SAVE/BGSAVE。事务语义在 txn_test.go（走 TCP）。

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// ---------------- MGET / MSET ----------------

func TestMGetDispatch(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("SET", "a", "1"))
	s.dispatch(mkCmd("RPUSH", "l", "x"))
	reply := s.dispatch(mkCmd("MGET", "a", "missing", "l"))
	if reply.Type != resp.Array || len(reply.Arr) != 3 {
		t.Fatalf("MGET: expected 3-element array, got %#v", reply)
	}
	wantBulk(t, "MGET[0]", reply.Arr[0], "1")
	// missing 与 wrong-type 都回 null（Redis MGET 不报 WRONGTYPE）
	wantNullBulk(t, "MGET[1] missing", reply.Arr[1])
	wantNullBulk(t, "MGET[2] wrongtype", reply.Arr[2])
	wantErr(t, "MGET no args", s.dispatch(mkCmd("MGET")), "wrong number of arguments")
}

func TestMSetDispatch(t *testing.T) {
	s := New()
	wantSimple(t, "MSET", s.dispatch(mkCmd("MSET", "a", "1", "b", "2")), "OK")
	wantBulk(t, "GET a", s.dispatch(mkCmd("GET", "a")), "1")
	// 覆写已有 string
	wantSimple(t, "MSET overwrite", s.dispatch(mkCmd("MSET", "a", "9")), "OK")
	wantBulk(t, "GET a", s.dispatch(mkCmd("GET", "a")), "9")
	// 覆写任意类型（list → string，Redis MSET 语义）
	s.dispatch(mkCmd("RPUSH", "l", "x"))
	wantSimple(t, "MSET over list", s.dispatch(mkCmd("MSET", "l", "now-str")), "OK")
	wantSimple(t, "TYPE l", s.dispatch(mkCmd("TYPE", "l")), "string")
	// 参数错误：缺参 / 奇数个
	wantErr(t, "MSET one arg", s.dispatch(mkCmd("MSET", "a")), "wrong number of arguments")
	wantErr(t, "MSET odd", s.dispatch(mkCmd("MSET", "a", "1", "b")), "wrong number of arguments")
}

// TestMSetAOFVerbatim MSET 原样落 AOF（canonicalWrite 默认分支），重启后一致。
func TestMSetAOFVerbatim(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s1, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	s1.apply(mkCmd("MSET", "a", "1", "b", "2"))
	s1.Close()

	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 1 || cmds[0].Arr[0].Str != "MSET" || len(cmds[0].Arr) != 5 {
		t.Fatalf("expected verbatim MSET a 1 b 2 in AOF, got %v", cmds)
	}
	s2, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulk(t, "a", s2.dispatch(mkCmd("GET", "a")), "1")
	wantBulk(t, "b", s2.dispatch(mkCmd("GET", "b")), "2")
}

// ---------------- ZRANGEBYSCORE / ZREVRANGEBYSCORE ----------------

func TestZRangeByScoreCmd(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("ZADD", "z", "1", "a", "2", "b", "2", "c", "3", "d"))
	// 全量（同分按成员字典序）
	wantBulkArray(t, "all", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "-inf", "+inf")),
		[]string{"a", "b", "c", "d"})
	wantBulkArray(t, "exclusive", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "(1", "3")),
		[]string{"b", "c", "d"})
	wantBulkArray(t, "limit", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "-inf", "+inf",
		"LIMIT", "1", "2")), []string{"b", "c"})
	wantBulkArray(t, "withscores", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "2", "3", "WITHSCORES")),
		[]string{"b", "2", "c", "2", "d", "3"})
	// rev 形态：第一个参数是 max，结果降序
	wantBulkArray(t, "rev all", s.dispatch(mkCmd("ZREVRANGEBYSCORE", "z", "+inf", "-inf")),
		[]string{"d", "c", "b", "a"})
	wantBulkArray(t, "rev bounds", s.dispatch(mkCmd("ZREVRANGEBYSCORE", "z", "3", "(2")),
		[]string{"d"})
	wantBulkArray(t, "rev limit", s.dispatch(mkCmd("ZREVRANGEBYSCORE", "z", "+inf", "(2",
		"LIMIT", "0", "1")), []string{"d"})
	// min > max → 空数组
	wantBulkArray(t, "inverted", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "3", "1")), []string{})
	// 错误分支
	wantErr(t, "bad bound", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "abc", "3")),
		"min or max is not a float")
	wantErr(t, "bad limit offset", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "1", "3",
		"LIMIT", "-1", "2")), "not an integer")
	wantErr(t, "bad limit count", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "1", "3",
		"LIMIT", "0", "-1")), "must be positive")
	wantErr(t, "dup withscores", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "1", "3",
		"WITHSCORES", "WITHSCORES")), "syntax error")
	wantErr(t, "bogus option", s.dispatch(mkCmd("ZRANGEBYSCORE", "z", "1", "3", "BOGUS")),
		"syntax error")
	// 缺 key → 空数组；WRONGTYPE
	wantBulkArray(t, "missing key", s.dispatch(mkCmd("ZRANGEBYSCORE", "nope", "-inf", "+inf")),
		[]string{})
	s.dispatch(mkCmd("SET", "str", "x"))
	wantErr(t, "wrongtype", s.dispatch(mkCmd("ZRANGEBYSCORE", "str", "-inf", "+inf")),
		"WRONGTYPE")
}

// ---------------- ZRANGEBYLEX / ZREVRANGEBYLEX ----------------

func TestZRangeByLexCmd(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("ZADD", "lx", "0", "a", "0", "aa", "0", "b", "0", "c"))
	wantBulkArray(t, "all", s.dispatch(mkCmd("ZRANGEBYLEX", "lx", "-", "+")),
		[]string{"a", "aa", "b", "c"})
	wantBulkArray(t, "inclusive-exclusive", s.dispatch(mkCmd("ZRANGEBYLEX", "lx", "[a", "(c")),
		[]string{"a", "aa", "b"})
	wantBulkArray(t, "limit", s.dispatch(mkCmd("ZRANGEBYLEX", "lx", "-", "+", "LIMIT", "1", "2")),
		[]string{"aa", "b"})
	// rev 形态：第一个参数是字典序较大的端点，结果降序
	wantBulkArray(t, "rev all", s.dispatch(mkCmd("ZREVRANGEBYLEX", "lx", "+", "-")),
		[]string{"c", "b", "aa", "a"})
	wantBulkArray(t, "rev limit", s.dispatch(mkCmd("ZREVRANGEBYLEX", "lx", "(c", "[a",
		"LIMIT", "0", "1")), []string{"b"})
	wantErr(t, "bad bound", s.dispatch(mkCmd("ZRANGEBYLEX", "lx", "nope", "+")),
		"min or max not valid string range item")
	// lex 族不接受 WITHSCORES
	wantErr(t, "withscores rejected", s.dispatch(mkCmd("ZRANGEBYLEX", "lx", "-", "+", "WITHSCORES")),
		"syntax error")
	wantBulkArray(t, "missing key", s.dispatch(mkCmd("ZRANGEBYLEX", "nope", "-", "+")), []string{})
}

// ---------------- ZRANDMEMBER ----------------

func TestZRandMemberCmd(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("ZADD", "z", "1", "a", "2", "b", "3", "c"))
	// 单形态：bulk，必是成员之一，不删除
	reply := s.dispatch(mkCmd("ZRANDMEMBER", "z"))
	if reply.Type != resp.BulkString || reply.Null {
		t.Fatalf("single: expected bulk, got type=%c", reply.Type)
	}
	if reply.Str != "a" && reply.Str != "b" && reply.Str != "c" {
		t.Fatalf("single: unexpected member %q", reply.Str)
	}
	wantInt(t, "single must not remove", s.dispatch(mkCmd("ZCARD", "z")), 3)
	// 正数 count：distinct
	reply = s.dispatch(mkCmd("ZRANDMEMBER", "z", "3"))
	if reply.Type != resp.Array || len(reply.Arr) != 3 {
		t.Fatalf("count: expected 3 elements, got %#v", reply)
	}
	seen := map[string]bool{}
	for _, m := range reply.Arr {
		seen[m.Str] = true
	}
	if len(seen) != 3 {
		t.Fatalf("positive count must be distinct, got %v", seen)
	}
	// 负数 count：可重复，绝对值个
	reply = s.dispatch(mkCmd("ZRANDMEMBER", "z", "-5"))
	if reply.Type != resp.Array || len(reply.Arr) != 5 {
		t.Fatalf("negative count: expected 5 elements, got %#v", reply)
	}
	// WITHSCORES 交错
	reply = s.dispatch(mkCmd("ZRANDMEMBER", "z", "2", "WITHSCORES"))
	if reply.Type != resp.Array || len(reply.Arr) != 4 {
		t.Fatalf("withscores: expected 4 elements, got %#v", reply)
	}
	for _, i := range []int{1, 3} {
		if _, err := strconv.ParseFloat(reply.Arr[i].Str, 64); err != nil {
			t.Fatalf("withscores: element %d must be a score, got %q", i, reply.Arr[i].Str)
		}
	}
	// 缺 key：单形态 null / count 形态空数组
	wantNullBulk(t, "missing single", s.dispatch(mkCmd("ZRANDMEMBER", "nope")))
	wantBulkArray(t, "missing count", s.dispatch(mkCmd("ZRANDMEMBER", "nope", "2")), []string{})
	wantBulkArray(t, "count 0", s.dispatch(mkCmd("ZRANDMEMBER", "z", "0")), []string{})
	// 参数与类型错误
	wantErr(t, "bad count", s.dispatch(mkCmd("ZRANDMEMBER", "z", "x")), "not an integer")
	wantErr(t, "bad option", s.dispatch(mkCmd("ZRANDMEMBER", "z", "2", "BOGUS")), "syntax error")
	s.dispatch(mkCmd("SET", "str", "v"))
	wantErr(t, "wrongtype", s.dispatch(mkCmd("ZRANDMEMBER", "str")), "WRONGTYPE")
}

// ---------------- SAVE / BGSAVE / RDB 服务器级往返 ----------------

func TestRDBDisabled(t *testing.T) {
	s := New()
	wantErr(t, "SAVE disabled", s.dispatch(mkCmd("SAVE")), "RDB snapshot is disabled")
	wantErr(t, "BGSAVE disabled", s.dispatch(mkCmd("BGSAVE")), "RDB snapshot is disabled")
}

// TestRDBServerRoundTrip SAVE 落盘 → 重启（NewWithPersist）→ 五类型 + TTL 完整回放。
func TestRDBServerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.rdb")
	s1, err := NewWithPersist(path, "")
	if err != nil {
		t.Fatal(err)
	}
	s1.dispatch(mkCmd("SET", "str", "hello"))
	s1.dispatch(mkCmd("SETEX", "exp", "500", "x"))
	s1.dispatch(mkCmd("RPUSH", "l", "a", "b"))
	s1.dispatch(mkCmd("HSET", "h", "f", "v"))
	s1.dispatch(mkCmd("SADD", "st", "m"))
	s1.dispatch(mkCmd("ZADD", "z", "1", "a", "2", "b"))
	s1.dispatch(mkCmd("MSET", "m1", "1", "m2", "2"))
	wantSimple(t, "SAVE", s1.dispatch(mkCmd("SAVE")), "OK")
	info := s1.dispatch(mkCmd("INFO", "persistence"))
	if !strings.Contains(info.Str, "rdb_enabled:1") ||
		!strings.Contains(info.Str, "rdb_last_save_time:") {
		t.Fatalf("INFO persistence: %s", info.Str)
	}
	s1.Close()

	s2, err := NewWithPersist(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulk(t, "str", s2.dispatch(mkCmd("GET", "str")), "hello")
	wantBulkArray(t, "l", s2.dispatch(mkCmd("LRANGE", "l", "0", "-1")), []string{"a", "b"})
	wantBulkArray(t, "h", s2.dispatch(mkCmd("HGETALL", "h")), []string{"f", "v"})
	wantBulkArray(t, "st", s2.dispatch(mkCmd("SMEMBERS", "st")), []string{"m"})
	wantBulkArray(t, "z", s2.dispatch(mkCmd("ZRANGE", "z", "0", "-1", "WITHSCORES")),
		[]string{"a", "1", "b", "2"})
	wantBulk(t, "m2", s2.dispatch(mkCmd("GET", "m2")), "2")
	// TTL 以绝对时间戳保存：重载后仍为正且接近 500s
	rem, ok := s2.store.TTL("exp")
	if !ok || rem <= 400 || rem > 500 {
		t.Fatalf("exp TTL after reload: rem=%d ok=%v", rem, ok)
	}
}

// TestBGSavesEventually BGSAVE 立即回复，后台 goroutine 完成落盘。
func TestBGSavesEventually(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.rdb")
	s, err := NewWithPersist(path, "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.dispatch(mkCmd("SET", "k", "v"))
	wantSimple(t, "BGSAVE", s.dispatch(mkCmd("BGSAVE")), "Background saving started")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("BGSAVE never wrote the file")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestRDBAOFBothNoDoubleApply 回归：RDB 与 AOF 双开时数据源唯一取 AOF。
// RDB 快照点必在 AOF 全量历史之内，"先载 RDB 再重放 AOF"会把快照前的
// 非幂等命令（RPUSH/LPUSH/INCR/APPEND...）执行两遍（验收实测 list 元素
// 重复 [x,x]）；AOF 开启时必须跳过 RDB 加载。
func TestRDBAOFBothNoDoubleApply(t *testing.T) {
	dir := t.TempDir()
	rdbPath := filepath.Join(dir, "dump.rdb")
	aofPath := filepath.Join(dir, "appendonly.aof")

	s1, err := NewWithPersist(rdbPath, aofPath)
	if err != nil {
		t.Fatal(err)
	}
	// 写命令走 apply（真实写路径：dispatch + AOF 落盘）；dispatch 只改内存不落盘
	s1.apply(mkCmd("RPUSH", "l", "a", "b"))
	s1.apply(mkCmd("SET", "s", "x"))
	s1.apply(mkCmd("SET", "n", "10"))
	s1.apply(mkCmd("INCR", "n"))
	if got := s1.dispatch(mkCmd("SAVE")); got.Type == resp.Error {
		t.Fatalf("SAVE: %s", got.Str)
	}
	// SAVE 之后的写只进 AOF（RDB 快照不含）
	s1.apply(mkCmd("SET", "after", "yes"))
	s1.Close()

	s2, err := NewWithPersist(rdbPath, aofPath)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	// 非幂等命令不得因 RDB+AOF 叠加而重复执行
	wantBulkArray(t, "l", s2.dispatch(mkCmd("LRANGE", "l", "0", "-1")), []string{"a", "b"})
	wantBulk(t, "s", s2.dispatch(mkCmd("GET", "s")), "x")
	wantBulk(t, "n", s2.dispatch(mkCmd("GET", "n")), "11")
	wantBulk(t, "after", s2.dispatch(mkCmd("GET", "after")), "yes")
}

// TestRDBLoadsWithoutAOF 无 AOF 时 RDB 仍是有效启动数据源（修复不回归）。
func TestRDBLoadsWithoutAOF(t *testing.T) {
	dir := t.TempDir()
	rdbPath := filepath.Join(dir, "dump.rdb")
	s1, err := NewWithPersist(rdbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	s1.dispatch(mkCmd("RPUSH", "l", "only"))
	wantSimple(t, "SAVE", s1.dispatch(mkCmd("SAVE")), "OK")
	s1.Close()

	s2, err := NewWithPersist(rdbPath, "")
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	wantBulkArray(t, "l", s2.dispatch(mkCmd("LRANGE", "l", "0", "-1")), []string{"only"})
}
