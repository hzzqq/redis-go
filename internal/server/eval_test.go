package server

// EVAL/SCRIPT 族（Phase 8）测试：走真实 TCP（路由在 applyConn），覆盖返回
// 值双向转换、KEYS/ARGV、效果复制（AOF 重启回放）、事务内 EVAL、副本只读。

import (
	"crypto/sha1"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hzzqq/redis-go/internal/resp"
)

// sha1Hex 计算脚本的 sha1（与 SCRIPT LOAD 返回值同形）。
func sha1Hex(t *testing.T, script string) string {
	t.Helper()
	sum := sha1.Sum([]byte(script))
	return hex.EncodeToString(sum[:])
}

// evalRun1 在独立连接上跑一条 EVAL 并返回回复；args 不带 numkeys 时默认补
// "0"（复用 replication_test 的 roundTrip，包装短名免得与 evalRun 混淆）。
func evalRun1(t *testing.T, addr, script string, args ...string) resp.Value {
	t.Helper()
	if len(args) == 0 || !isNumKeysArg(args) {
		args = append([]string{"0"}, args...)
	}
	full := append([]string{script}, args...)
	return mustCmd(t, addr, "EVAL", full...)
}

// isNumKeysArg 粗判首个参数是否 numkeys（纯数字）。
func isNumKeysArg(args []string) bool {
	if len(args) == 0 {
		return false
	}
	for _, c := range args[0] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return args[0] != ""
}

// mustCmd 单连接单命令，出错即 Fatal。
func mustCmd(t *testing.T, addr, cmd string, args ...string) resp.Value {
	t.Helper()
	v, err := roundTrip(t, addr, cmd, args...)
	if err != nil {
		t.Fatalf("%s: %v", cmd, err)
	}
	return v
}

// TestEvalReturnTypes 脚本返回值 → RESP 的转换矩阵。
func TestEvalReturnTypes(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	cases := []struct {
		script string
		check  func(resp.Value)
	}{
		{"return 42", func(v resp.Value) {
			if v.Type != resp.Integer || v.Num != 42 {
				t.Fatalf("return 42: got %#v", v)
			}
		}},
		{"return 'hello'", func(v resp.Value) {
			if v.Type != resp.BulkString || v.Null || v.Str != "hello" {
				t.Fatalf("return 'hello': got %#v", v)
			}
		}},
		{"return {1, 'a', 'b'}", func(v resp.Value) {
			if v.Type != resp.Array || len(v.Arr) != 3 {
				t.Fatalf("return table: got %#v", v)
			}
		}},
		{"return", func(v resp.Value) {
			if v.Type != resp.BulkString || !v.Null {
				t.Fatalf("return (no value): got %#v", v)
			}
		}},
		{"return nil", func(v resp.Value) {
			if v.Type != resp.BulkString || !v.Null {
				t.Fatalf("return nil: got %#v", v)
			}
		}},
		{"return false", func(v resp.Value) {
			if v.Type != resp.BulkString || !v.Null {
				t.Fatalf("return false: got %#v", v)
			}
		}},
		{"return true", func(v resp.Value) {
			if v.Type != resp.Integer || v.Num != 1 {
				t.Fatalf("return true: got %#v", v)
			}
		}},
		{"return redis.status_reply('OK')", func(v resp.Value) {
			if v.Type != resp.SimpleString || v.Str != "OK" {
				t.Fatalf("status_reply: got %#v", v)
			}
		}},
		{"return redis.error_reply('boom')", func(v resp.Value) {
			if v.Type != resp.Error || !strings.Contains(v.Str, "boom") {
				t.Fatalf("error_reply: got %#v", v)
			}
		}},
	}
	for _, c := range cases {
		c.check(evalRun1(t, addr, c.script))
	}
}

// TestEvalKeysArgv KEYS/ARGV 注入与 redis.call 写读闭环。
func TestEvalKeysArgv(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	wantSimple(t, "EVAL SET", evalRun1(t, addr,
		"return redis.call('SET', KEYS[1], ARGV[1])", "1", "greet", "hi"), "OK")
	wantInt(t, "EVAL APPEND", evalRun1(t, addr,
		"return redis.call('APPEND', KEYS[1], ARGV[1])", "1", "greet", " there"), 8)
	v := mustCmd(t, addr, "GET", "greet")
	if v.Str != "hi there" {
		t.Fatalf("GET after script: got %q", v.Str)
	}
	// KEYS[2] 越界为 nil；redis.call EXISTS 缺失 key → 整数 0（Redis 同）
	v = evalRun1(t, addr, "if KEYS[2] == nil then return redis.call('EXISTS', 'nokey') end return -1")
	if v.Type != resp.Integer || v.Num != 0 {
		t.Fatalf("EXISTS missing via script: got %#v", v)
	}
	// 多 key + ARGV 组合
	wantSimple(t, "EVAL MSET-ish", evalRun1(t, addr,
		"return redis.call('MSET', KEYS[1], ARGV[1], KEYS[2], ARGV[2])",
		"2", "k1", "k2", "v1", "v2"), "OK")
}

// TestEvalCallErrorAborts redis.call 遇错中止脚本（含已执行效果保留）。
func TestEvalCallErrorAborts(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	wantSimple(t, "SET str", mustCmd(t, addr, "SET", "str", "x"), "OK")
	// 脚本先成功 SET，再对 string 做 INCR → 错误中止；第一个效果保留
	v := evalRun1(t, addr,
		"redis.call('SET', KEYS[1], 'pre') return redis.call('INCR', KEYS[2])",
		"2", "okk", "str")
	if v.Type != resp.Error {
		t.Fatalf("call error should abort script, got %#v", v)
	}
	if got := mustCmd(t, addr, "GET", "okk"); got.Str != "pre" {
		t.Fatalf("effect before error should persist: got %#v", got)
	}
	// pcall：错误转 {err=} 表，脚本可继续
	v = evalRun1(t, addr, "local r = redis.pcall('INCR', KEYS[1]) return type(r) == 'table' and 1 or 0",
		"1", "str")
	if v.Type != resp.Integer || v.Num != 1 {
		t.Fatalf("pcall error table: got %#v", v)
	}
}

// TestEvalShaAndScriptCache SCRIPT LOAD/EXISTS/FLUSH + EVALSHA/NOSCRIPT。
func TestEvalShaAndScriptCache(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	script := "return ARGV[1] .. '!'"
	v := mustCmd(t, addr, "SCRIPT", "LOAD", script)
	if v.Type != resp.BulkString || len(v.Str) != 40 {
		t.Fatalf("SCRIPT LOAD: got %#v", v)
	}
	sha := v.Str
	v = mustCmd(t, addr, "SCRIPT", "EXISTS", sha, "deadbeef")
	if v.Type != resp.Array || len(v.Arr) != 2 ||
		v.Arr[0].Num != 1 || v.Arr[1].Num != 0 {
		t.Fatalf("SCRIPT EXISTS: got %#v", v)
	}
	v = mustCmd(t, addr, "EVALSHA", sha, "0", "hey")
	if v.Str != "hey!" {
		t.Fatalf("EVALSHA: got %#v", v)
	}
	// EVAL 也登记缓存
	v = evalRun1(t, addr, "return 'ev'", "0")
	if v.Str != "ev" {
		t.Fatalf("EVAL: got %#v", v)
	}
	sum := sha1Hex(t, "return 'ev'")
	if v = mustCmd(t, addr, "EVALSHA", sum, "0"); v.Str != "ev" {
		t.Fatalf("EVALSHA after EVAL: got %#v", v)
	}
	wantSimple(t, "SCRIPT FLUSH", mustCmd(t, addr, "SCRIPT", "FLUSH"), "OK")
	v = mustCmd(t, addr, "EVALSHA", sha, "0")
	if v.Type != resp.Error || !strings.HasPrefix(v.Str, "NOSCRIPT") {
		t.Fatalf("EVALSHA after FLUSH: got %#v", v)
	}
}

// TestEvalEffectsAOFRestart 效果复制：脚本写命令确定化落 AOF，重启回放
// 状态一致（含随机命令 SPOP→SREM 与相对 TTL→绝对毫秒的 canonical 化）。
func TestEvalEffectsAOFRestart(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, "eval.aof")
	s, err := NewWithAOF(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	addr := startPubSubServer(t, s)
	mustCmd(t, addr, "SADD", "sp", "a", "b", "c")
	// 脚本弹出一个成员（随机）+ 写一个带 TTL 的 key
	popped := evalRun1(t, addr, "return redis.call('SPOP', KEYS[1])", "1", "sp")
	if popped.Type != resp.BulkString || popped.Null {
		t.Fatalf("SPOP via script: got %#v", popped)
	}
	wantSimple(t, "script SETEX", evalRun1(t, addr,
		"return redis.call('SETEX', KEYS[1], '100', ARGV[1])", "1", "ttl", "v"), "OK")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewWithAOF(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })
	addr2 := startPubSubServer(t, s2)
	// 弹出的成员不再出现（AOF 里是 SREM <member>，非 SPOP 重放）
	v := mustCmd(t, addr2, "SISMEMBER", "sp", popped.Str)
	if v.Num != 0 {
		t.Fatalf("popped member should be removed after replay: got %#v", v)
	}
	v = mustCmd(t, addr2, "SCARD", "sp")
	if v.Num != 2 {
		t.Fatalf("SCARD after replay: got %#v", v)
	}
	v = mustCmd(t, addr2, "TTL", "ttl")
	if v.Type != resp.Integer || v.Num <= 0 || v.Num > 100 {
		t.Fatalf("TTL after replay: got %#v", v)
	}
	v = mustCmd(t, addr2, "GET", "ttl")
	if v.Str != "v" {
		t.Fatalf("GET after replay: got %#v", v)
	}
}

// TestEvalInTransaction 事务内 EVAL：回复进 EXEC 数组，效果并入
// MULTI...EXEC 块传播（副本回放后状态一致）。
func TestEvalInTransaction(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	replica := New()
	replicaAddr := startPubSubServer(t, replica)
	rConn, rR := dialRaw(t, replicaAddr)
	attachReplica(t, rConn, rR, masterAddr)
	waitOnline(t, replicaAddr)

	mConn, mR := dialRaw(t, masterAddr)
	send(t, mConn, "MULTI")
	wantSimple(t, "MULTI", readReply(t, "MULTI", mR), "OK")
	send(t, mConn, "EVAL", "redis.call('SET', KEYS[1], ARGV[1]) return 7",
		"1", "tk", "tv")
	wantSimple(t, "queued EVAL", readReply(t, "EVAL", mR), "QUEUED")
	send(t, mConn, "SET", "plain", "yes")
	wantSimple(t, "queued SET", readReply(t, "SET", mR), "QUEUED")
	send(t, mConn, "EXEC")
	v := readReply(t, "EXEC", mR)
	if v.Type != resp.Array || len(v.Arr) != 2 {
		t.Fatalf("EXEC: expected 2-slot array, got %#v", v)
	}
	if v.Arr[0].Num != 7 {
		t.Fatalf("EVAL slot: got %#v", v.Arr[0])
	}

	pollGet(t, "txn eval effect", replicaAddr, "tk", "tv")
	pollGet(t, "txn plain", replicaAddr, "plain", "yes")
}

// TestEvalReplicaReadOnly 副本上脚本：读命令可跑，写调用中止脚本并报
// READONLY，无效果落地。
func TestEvalReplicaReadOnly(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	mConn, mR := dialRaw(t, masterAddr)
	send(t, mConn, "SET", "r", "v")
	readReply(t, "SET", mR)

	replica := New()
	replicaAddr := startPubSubServer(t, replica)
	rConn, rR := dialRaw(t, replicaAddr)
	attachReplica(t, rConn, rR, masterAddr)
	waitOnline(t, replicaAddr)

	// 读脚本正常
	v := evalRun1(t, replicaAddr, "return redis.call('GET', KEYS[1])", "1", "r")
	if v.Str != "v" {
		t.Fatalf("read script on replica: got %#v", v)
	}
	// 写脚本：call 报错中止（READONLY）
	v = evalRun1(t, replicaAddr,
		"return redis.call('SET', KEYS[1], 'nope')", "1", "w")
	if v.Type != resp.Error || !strings.Contains(v.Str, "READONLY") {
		t.Fatalf("write script on replica: got %#v", v)
	}
	v = mustCmd(t, replicaAddr, "EXISTS", "w")
	if v.Num != 0 {
		t.Fatalf("no effect should land on replica: got %#v", v)
	}
}

// TestEvalScriptWriteTouchesWatch 脚本写效果触碰其他连接的 WATCH：
// EVAL 改了被 WATCH 的 key 后，watcher 的 EXEC 应回 null。
func TestEvalScriptWriteTouchesWatch(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	wConn, wR := dialRaw(t, addr)
	send(t, wConn, "SET", "wk", "1")
	readReply(t, "SET", wR)
	send(t, wConn, "WATCH", "wk")
	readReply(t, "WATCH", wR)
	send(t, wConn, "MULTI")
	readReply(t, "MULTI", wR)
	send(t, wConn, "SET", "other", "x")
	readReply(t, "queued", wR)

	// 另一连接用脚本改 wk
	evalRun1(t, addr, "return redis.call('SET', KEYS[1], '2')", "1", "wk")

	send(t, wConn, "EXEC")
	v := readReply(t, "EXEC", wR)
	if v.Type != resp.Array || !v.Null {
		t.Fatalf("EXEC after script touched watched key: got %#v", v)
	}
}
