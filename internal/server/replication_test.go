package server

// 主从复制的握手/命令流/角色切换都在连接与 goroutine 层，必须走真实 TCP
// 验证（同 txn_test 的做法）。

import (
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
)

// roundTrip 打一条新连接执行单条命令并读回复（带超时，防止服务端卡死拖垮测试）。
func roundTrip(t *testing.T, addr string, cmd string, args ...string) (resp.Value, error) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return resp.Value{}, err
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	if err := resp.WriteValue(c, mkCmd(cmd, args...)); err != nil {
		return resp.Value{}, err
	}
	return resp.NewReader(c).Read()
}

// waitUntil 轮询条件直到成立（5s 超时即 Fatal）。
func waitUntil(t *testing.T, name string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s: condition not met within 5s", name)
}

// pollGet 轮询 addr 上的 GET key 直到等于 want（全量同步/命令流就绪）。
func pollGet(t *testing.T, name, addr, key, want string) {
	t.Helper()
	waitUntil(t, name, func() bool {
		v, err := roundTrip(t, addr, "GET", key)
		return err == nil && v.Type == resp.BulkString && !v.Null && v.Str == want
	})
}

// fetchInfo 取 INFO <section> 并解析成 k:v 映射（避开 server.go 的
// infoSection 类型名）。
func fetchInfo(t *testing.T, addr, section string) map[string]string {
	t.Helper()
	v, err := roundTrip(t, addr, "INFO", section)
	if err != nil {
		t.Fatalf("INFO %s: %v", section, err)
	}
	if v.Type != resp.BulkString {
		t.Fatalf("INFO %s: expected bulk, got type=%c", section, v.Type)
	}
	m := map[string]string{}
	for _, line := range strings.Split(v.Str, "\r\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		if k, val, ok := strings.Cut(line, ":"); ok {
			m[k] = val
		}
	}
	return m
}

// attachReplica 通过 REPLICAOF 把 replica 挂到 master，并在测试结束时自动
// REPLICAOF NO ONE（避免复制循环泄漏到其他测试）。
func attachReplica(t *testing.T, c net.Conn, r *resp.Reader, masterAddr string) {
	t.Helper()
	host, port, err := net.SplitHostPort(masterAddr)
	if err != nil {
		t.Fatal(err)
	}
	send(t, c, "REPLICAOF", host, port)
	wantSimple(t, "REPLICAOF", readReply(t, "REPLICAOF", r), "OK")
	t.Cleanup(func() {
		send(t, c, "REPLICAOF", "NO", "ONE")
		readReply(t, "REPLICAOF NO ONE", r)
	})
}

// waitOnline 等待副本 INFO replication 显示 master_link_status:up（INFO 输出对齐真实 Redis：up/down）。
func waitOnline(t *testing.T, replicaAddr string) {
	t.Helper()
	waitUntil(t, "replica online", func() bool {
		m := fetchInfo(t, replicaAddr, "replication")
		return m["master_link_status"] == "up"
	})
}

// TestReplicationFullSyncAndStream 全量同步（五类型 + TTL）→ 命令流实时传播
// （含 DEL/EXPIRE 的 canonical 化与 SPOP→SREM 随机命令确定化）→ 双端 INFO 角色。
func TestReplicationFullSyncAndStream(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	mConn, mR := dialRaw(t, masterAddr)

	// 同步前写入：五类型 + TTL + 计数器
	send(t, mConn, "SET", "str", "hello")
	wantSimple(t, "SET", readReply(t, "SET", mR), "OK")
	send(t, mConn, "RPUSH", "list", "a", "b", "c")
	wantInt(t, "RPUSH", readReply(t, "RPUSH", mR), 3)
	send(t, mConn, "HSET", "h", "f1", "v1", "f2", "v2")
	wantInt(t, "HSET", readReply(t, "HSET", mR), 2)
	send(t, mConn, "SADD", "s", "x", "y")
	wantInt(t, "SADD", readReply(t, "SADD", mR), 2)
	send(t, mConn, "ZADD", "z", "1", "m1", "2", "m2")
	wantInt(t, "ZADD", readReply(t, "ZADD", mR), 2)
	send(t, mConn, "SET", "ttl", "v", "EX", "300")
	wantSimple(t, "SET EX", readReply(t, "SET EX", mR), "OK")
	send(t, mConn, "INCR", "ctr")
	wantInt(t, "INCR", readReply(t, "INCR", mR), 1)

	replica := New()
	replicaAddr := startPubSubServer(t, replica)
	rConn, rR := dialRaw(t, replicaAddr)
	attachReplica(t, rConn, rR, masterAddr)

	pollGet(t, "sync str", replicaAddr, "str", "hello")

	// 全量同步：其余类型逐项断言
	v, err := roundTrip(t, replicaAddr, "LRANGE", "list", "0", "-1")
	if err != nil {
		t.Fatal(err)
	}
	wantBulkArray(t, "replay list", v, []string{"a", "b", "c"})
	v, err = roundTrip(t, replicaAddr, "HGETALL", "h")
	if err != nil {
		t.Fatal(err)
	}
	wantBulkArray(t, "replay hash", v, []string{"f1", "v1", "f2", "v2"})
	v, err = roundTrip(t, replicaAddr, "ZRANGE", "z", "0", "-1", "WITHSCORES")
	if err != nil {
		t.Fatal(err)
	}
	wantBulkArray(t, "replay zset", v, []string{"m1", "1", "m2", "2"})
	v, err = roundTrip(t, replicaAddr, "SCARD", "s")
	if err != nil {
		t.Fatal(err)
	}
	wantInt(t, "replay set", v, 2)
	v, err = roundTrip(t, replicaAddr, "TTL", "ttl")
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != resp.Integer || v.Num <= 0 || v.Num > 300 {
		t.Fatalf("replay TTL: want (0,300], got %#v", v)
	}

	// 双端 INFO 角色与链路
	m := fetchInfo(t, replicaAddr, "replication")
	if m["role"] != "slave" || m["master_link_status"] != "up" {
		t.Fatalf("replica INFO: got %v", m)
	}
	if _, ok := m["master_port"]; !ok {
		t.Fatalf("replica INFO missing master_port: %v", m)
	}
	m = fetchInfo(t, masterAddr, "replication")
	if m["role"] != "master" || m["connected_slaves"] != "1" {
		t.Fatalf("master INFO: got %v", m)
	}
	if slave, ok := m["slave0"]; !ok || !strings.Contains(slave, "ip=127.0.0.1") {
		t.Fatalf("master INFO slave0: got %q (all: %v)", slave, m)
	}

	// 命令流：同步后的写入实时到达副本
	send(t, mConn, "SET", "post", "sync1")
	wantSimple(t, "SET post", readReply(t, "SET post", mR), "OK")
	pollGet(t, "stream SET", replicaAddr, "post", "sync1")
	send(t, mConn, "INCR", "ctr")
	readReply(t, "INCR2", mR)
	pollGet(t, "stream INCR", replicaAddr, "ctr", "2")
	send(t, mConn, "DEL", "list")
	readReply(t, "DEL", mR)
	waitUntil(t, "stream DEL", func() bool {
		v, err := roundTrip(t, replicaAddr, "GET", "list")
		return err == nil && v.Type == resp.BulkString && v.Null
	})
	send(t, mConn, "EXPIRE", "post", "100")
	readReply(t, "EXPIRE", mR)
	waitUntil(t, "stream EXPIRE→PEXPIREAT", func() bool {
		v, err := roundTrip(t, replicaAddr, "TTL", "post")
		return err == nil && v.Type == resp.Integer && v.Num > 0 && v.Num <= 100
	})

	// 随机命令确定化传播：SPOP 在主库按实际弹出成员改写为 SREM，
	// 副本状态与主库严格一致（弹出的成员不再出现）。
	send(t, mConn, "SADD", "sp", "a", "b", "c")
	readReply(t, "SADD sp", mR)
	send(t, mConn, "SPOP", "sp", "1")
	popped := readReply(t, "SPOP", mR)
	if popped.Type != resp.Array || len(popped.Arr) != 1 {
		t.Fatalf("SPOP: expected 1-element array, got %#v", popped)
	}
	member := popped.Arr[0].Str
	waitUntil(t, "SPOP propagation", func() bool {
		vc, err := roundTrip(t, replicaAddr, "SCARD", "sp")
		vm, err2 := roundTrip(t, replicaAddr, "SISMEMBER", "sp", member)
		return err == nil && err2 == nil &&
			vc.Type == resp.Integer && vc.Num == 2 &&
			vm.Type == resp.Integer && vm.Num == 0
	})
}

// TestReplicationTransactionPropagation 事务以 MULTI...EXEC 帧序列传播，
// 副本端块感知整体回放。
func TestReplicationTransactionPropagation(t *testing.T) {
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
	for _, c := range [][]string{
		{"SET", "a", "1"}, {"INCR", "n"}, {"RPUSH", "l", "x"},
	} {
		send(t, mConn, c[0], c[1:]...)
		wantSimple(t, "queued "+c[0], readReply(t, c[0], mR), "QUEUED")
	}
	send(t, mConn, "EXEC")
	v := readReply(t, "EXEC", mR)
	if v.Type != resp.Array || len(v.Arr) != 3 {
		t.Fatalf("EXEC: expected 3-slot array, got %#v", v)
	}

	pollGet(t, "txn SET on replica", replicaAddr, "a", "1")
	pollGet(t, "txn INCR on replica", replicaAddr, "n", "1")
	v, err := roundTrip(t, replicaAddr, "LRANGE", "l", "0", "-1")
	if err != nil {
		t.Fatal(err)
	}
	wantBulkArray(t, "txn RPUSH on replica", v, []string{"x"})
}

// TestReplicaReadOnlyAndPromote 副本拒绝写（含事务槽位），REPLICAOF NO ONE
// 晋升后恢复可写、角色回到 master。
func TestReplicaReadOnlyAndPromote(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	replica := New()
	replicaAddr := startPubSubServer(t, replica)
	rConn, rR := dialRaw(t, replicaAddr)
	attachReplica(t, rConn, rR, masterAddr)
	waitOnline(t, replicaAddr)

	send(t, rConn, "SET", "k", "v")
	wantErr(t, "write against replica", readReply(t, "SET", rR), "READONLY")

	// 事务内写命令同样在 EXEC 槽位返回 READONLY（执行期错误）
	send(t, rConn, "MULTI")
	readReply(t, "MULTI", rR)
	send(t, rConn, "SET", "x", "1")
	wantSimple(t, "queued", readReply(t, "SET", rR), "QUEUED")
	send(t, rConn, "EXEC")
	v := readReply(t, "EXEC", rR)
	if v.Type != resp.Array || len(v.Arr) != 1 {
		t.Fatalf("EXEC on replica: expected 1-slot array, got %#v", v)
	}
	wantErr(t, "EXEC slot", v.Arr[0], "READONLY")

	send(t, rConn, "REPLICAOF", "NO", "ONE")
	wantSimple(t, "REPLICAOF NO ONE", readReply(t, "NO ONE", rR), "OK")
	send(t, rConn, "SET", "k", "v")
	wantSimple(t, "write after promote", readReply(t, "SET2", rR), "OK")
	m := fetchInfo(t, replicaAddr, "replication")
	if m["role"] != "master" {
		t.Fatalf("promoted INFO: got %v", m)
	}
	// 回归（Phase 7 冒烟发现）：副本断开后主库必须摘除链路，
	// connected_slaves 归零（dropClient 依赖 cl.replicaLink）。
	waitUntil(t, "master drops detached replica", func() bool {
		return fetchInfo(t, masterAddr, "replication")["connected_slaves"] == "0"
	})
}

// TestReplicaRestartAOFBaseline 副本开启 AOF：全量同步后立即把基线重写进
// AOF，命令流照常追加——重启后重放「基线 + 命令流」状态完整。
func TestReplicaRestartAOFBaseline(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, "replica.aof")
	master := New()
	masterAddr := startPubSubServer(t, master)
	mConn, mR := dialRaw(t, masterAddr)
	send(t, mConn, "SET", "pre", "v")
	readReply(t, "SET pre", mR)
	send(t, mConn, "RPUSH", "rl", "a", "b")
	readReply(t, "RPUSH", mR)

	replica, err := NewWithAOF(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	replicaAddr := startPubSubServer(t, replica)
	rConn, rR := dialRaw(t, replicaAddr)
	attachReplica(t, rConn, rR, masterAddr)
	pollGet(t, "sync pre", replicaAddr, "pre", "v")

	// 同步后的命令流也进副本 AOF
	send(t, mConn, "SET", "post", "sync1")
	readReply(t, "SET post", mR)
	pollGet(t, "stream post", replicaAddr, "post", "sync1")

	// 重启副本：重放自己的 AOF（基线重写 + 命令流），不再依赖主库
	send(t, rConn, "REPLICAOF", "NO", "ONE")
	readReply(t, "detach", rR)
	if err := replica.Close(); err != nil {
		t.Fatal(err)
	}
	replica2, err := NewWithAOF(aofPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { replica2.Close() }) // 释放 AOF 句柄，TempDir 才能清理
	addr2 := startPubSubServer(t, replica2)
	pollGet(t, "restart pre", addr2, "pre", "v")
	pollGet(t, "restart post", addr2, "post", "sync1")
	v, err := roundTrip(t, addr2, "LRANGE", "rl", "0", "-1")
	if err != nil {
		t.Fatal(err)
	}
	wantBulkArray(t, "restart list", v, []string{"a", "b"})
}

// TestReplicationResyncAfterDetach 断开后再挂载触发重新全量同步：断开期间
// 主库的写入通过重同步补齐，旧数据也在（Flush+重载语义）。
func TestReplicationResyncAfterDetach(t *testing.T) {
	master := New()
	masterAddr := startPubSubServer(t, master)
	mConn, mR := dialRaw(t, masterAddr)
	send(t, mConn, "SET", "k1", "v1")
	readReply(t, "SET k1", mR)

	replica := New()
	replicaAddr := startPubSubServer(t, replica)
	rConn, rR := dialRaw(t, replicaAddr)
	attachReplica(t, rConn, rR, masterAddr)
	pollGet(t, "first sync k1", replicaAddr, "k1", "v1")

	send(t, rConn, "REPLICAOF", "NO", "ONE")
	readReply(t, "detach", rR)
	// 断开期间的写入副本收不到
	send(t, mConn, "SET", "k2", "v2")
	readReply(t, "SET k2", mR)
	waitUntil(t, "k2 absent while detached", func() bool {
		v, err := roundTrip(t, replicaAddr, "GET", "k2")
		return err == nil && v.Type == resp.BulkString && v.Null
	})

	host, port, _ := net.SplitHostPort(masterAddr)
	send(t, rConn, "REPLICAOF", host, port)
	wantSimple(t, "re-attach", readReply(t, "REPLICAOF", rR), "OK")
	pollGet(t, "resync k2", replicaAddr, "k2", "v2")
	pollGet(t, "resync keeps k1", replicaAddr, "k1", "v1")
}

// TestReplicationCascade 级联复制：C 挂 B、B 挂 A，A 的写入沿命令流到达 C。
func TestReplicationCascade(t *testing.T) {
	a := New()
	aAddr := startPubSubServer(t, a)
	b := New()
	bAddr := startPubSubServer(t, b)
	c := New()
	cAddr := startPubSubServer(t, c)

	bConn, bR := dialRaw(t, bAddr)
	attachReplica(t, bConn, bR, aAddr)
	cConn, cR := dialRaw(t, cAddr)
	attachReplica(t, cConn, cR, bAddr)

	aConn, aR := dialRaw(t, aAddr)
	send(t, aConn, "SET", "deep", "v")
	readReply(t, "SET deep", aR)
	pollGet(t, "cascade B", bAddr, "deep", "v")
	pollGet(t, "cascade C", cAddr, "deep", "v")

	m := fetchInfo(t, bAddr, "replication")
	if m["role"] != "slave" || m["connected_slaves"] != "1" {
		t.Fatalf("middle INFO: got %v", m)
	}
}

// TestReplicaofSelfRejected 拒绝复制到自己（否则命令流会在同一连接上自我
// 放大成死循环）。
func TestReplicaofSelfRejected(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)
	_, port, _ := net.SplitHostPort(addr)
	send(t, c, "REPLICAOF", "127.0.0.1", port)
	wantErr(t, "self replication", readReply(t, "REPLICAOF", r), "can't replicate to self")
	m := fetchInfo(t, addr, "replication")
	if m["role"] != "master" {
		t.Fatalf("role after reject: got %v", m)
	}
}
