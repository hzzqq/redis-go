package server

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// startPubSubServer 起一个真实 TCP 服务并等待可拨号，返回拨号地址。
func startPubSubServer(t *testing.T, s *Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close() // 让 s.Listen 复用该端口（本机回环，窗口极小）
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	go s.Listen(addr)
	for i := 0; i < 100; i++ {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("pub/sub test server did not start")
	return ""
}

// dialRaw 拨一条测试连接，注册关闭。
func dialRaw(t *testing.T, addr string) (net.Conn, *resp.Reader) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, resp.NewReader(c)
}

// send 发送一条命令。
func send(t *testing.T, c net.Conn, cmd string, args ...string) {
	t.Helper()
	if err := resp.WriteValue(c, mkCmd(cmd, args...)); err != nil {
		t.Fatal(err)
	}
}

// wantSubRow 断言读到一条 [kind, channel, count] 确认行。
func wantSubRow(t *testing.T, name string, r *resp.Reader, kind, ch string, count int64) {
	t.Helper()
	v, err := r.Read()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if v.Type != resp.Array || len(v.Arr) != 3 {
		t.Fatalf("%s: expected 3-element array, got %#v", name, v)
	}
	if v.Arr[0].Str != kind || v.Arr[1].Str != ch || v.Arr[2].Type != resp.Integer || v.Arr[2].Num != count {
		t.Fatalf("%s: got %#v, want [%s %s %d]", name, v, kind, ch, count)
	}
}

// wantPushRow 断言读到一条 [message, channel, payload] 推送。
func wantPushRow(t *testing.T, name string, r *resp.Reader, ch, payload string) {
	t.Helper()
	v, err := r.Read()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if v.Type != resp.Array || len(v.Arr) != 3 {
		t.Fatalf("%s: expected 3-element array, got %#v", name, v)
	}
	if v.Arr[0].Str != "message" || v.Arr[1].Str != ch || v.Arr[2].Str != payload {
		t.Fatalf("%s: got %#v, want [message %s %s]", name, v, ch, payload)
	}
}

// TestPubSubCrossConn 跨连接订阅/发布：确认行、推送帧、发布者计数互不串扰。
func TestPubSubCrossConn(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)

	subConn, subR := dialRaw(t, addr)
	pubConn, pubR := dialRaw(t, addr)

	// 订阅两个频道（一条命令）：两行独立帧，计数 1 → 2
	send(t, subConn, "SUBSCRIBE", "news", "chat")
	wantSubRow(t, "subscribe news", subR, "subscribe", "news", 1)
	wantSubRow(t, "subscribe chat", subR, "subscribe", "chat", 2)

	// 发布到无人订阅的频道：计数 0
	send(t, pubConn, "PUBLISH", "empty", "before-anyone")
	v, err := pubR.Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != resp.Integer || v.Num != 0 {
		t.Fatalf("publish with no subscribers: got %#v, want 0", v)
	}

	// 未订阅的发布者向 chat 发布：chat 此时有 1 个订阅者
	send(t, pubConn, "PUBLISH", "chat", "hello")
	v, err = pubR.Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != resp.Integer || v.Num != 1 {
		t.Fatalf("publish count: got %#v, want 1", v)
	}
	wantPushRow(t, "sub push", subR, "chat", "hello")

	// 第三个连接加入订阅后，一条消息 fanout 给全部订阅者
	sub2Conn, sub2R := dialRaw(t, addr)
	send(t, sub2Conn, "SUBSCRIBE", "chat")
	wantSubRow(t, "sub2 subscribe", sub2R, "subscribe", "chat", 1)
	send(t, pubConn, "PUBLISH", "chat", "again")
	if v, err = pubR.Read(); err != nil || v.Num != 2 {
		t.Fatalf("publish count 2: got %#v err=%v", v, err)
	}
	wantPushRow(t, "sub push 2", subR, "chat", "again")
	wantPushRow(t, "sub2 push 2", sub2R, "chat", "again")
}

// TestUnsubscribeAll 无参数 UNSUBSCRIBE 退订全部；空退订回 null 频道行。
func TestUnsubscribeAll(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	send(t, c, "UNSUBSCRIBE") // 从未订阅
	v, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != resp.Array || len(v.Arr) != 3 || v.Arr[1].Type != resp.BulkString || !v.Arr[1].Null {
		t.Fatalf("unsubscribe with none: got %#v, want null channel row", v)
	}

	send(t, c, "SUBSCRIBE", "b", "a")
	wantSubRow(t, "row b", r, "subscribe", "b", 1)
	wantSubRow(t, "row a", r, "subscribe", "a", 2)

	send(t, c, "UNSUBSCRIBE") // 全部退订，行按频道名排序
	wantSubRow(t, "unsub a", r, "unsubscribe", "a", 1)
	wantSubRow(t, "unsub b", r, "unsubscribe", "b", 0)

	// 退订后不再收到推送
	pubConn, pubR := dialRaw(t, addr)
	send(t, pubConn, "PUBLISH", "a", "x")
	if v, err = pubR.Read(); err != nil || v.Num != 0 {
		t.Fatalf("publish after unsubscribe: got %#v err=%v", v, err)
	}
}

// TestSubscribeModeRestriction 订阅模式下拒绝普通命令，PING 回数组，QUIT 正常。
func TestSubscribeModeRestriction(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	c, r := dialRaw(t, addr)

	// 未订阅时 PING 是普通 PONG
	send(t, c, "PING")
	v, err := r.Read()
	if err != nil || v.Str != "PONG" {
		t.Fatalf("plain ping: got %#v err=%v", v, err)
	}

	send(t, c, "SUBSCRIBE", "ch")
	wantSubRow(t, "subscribe", r, "subscribe", "ch", 1)

	send(t, c, "GET", "k")
	v, err = r.Read()
	if err != nil || v.Type != resp.Error || !strings.Contains(v.Str, "Can't execute 'get'") {
		t.Fatalf("GET in subscribe mode: got %#v err=%v", v, err)
	}
	send(t, c, "PUBLISH", "ch", "x")
	v, err = r.Read()
	if err != nil || v.Type != resp.Error || !strings.Contains(v.Str, "Can't execute 'publish'") {
		t.Fatalf("PUBLISH in subscribe mode: got %#v err=%v", v, err)
	}

	// 订阅模式 PING → [pong, ""]
	send(t, c, "PING")
	v, err = r.Read()
	if err != nil || v.Type != resp.Array || len(v.Arr) != 2 ||
		v.Arr[0].Str != "PONG" || v.Arr[1].Str != "" {
		t.Fatalf("subscribe-mode ping: got %#v err=%v", v, err)
	}

	// QUIT 正常返回 OK 并断开
	send(t, c, "QUIT")
	v, err = r.Read()
	if err != nil || v.Str != "OK" {
		t.Fatalf("quit: got %#v err=%v", v, err)
	}
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err = r.Read(); err == nil {
		t.Fatal("connection should be closed after QUIT")
	}
}

// TestSubscriberDisconnectCleanup 订阅者断连后自动退订，计数归零。
func TestSubscriberDisconnectCleanup(t *testing.T) {
	s := New()
	addr := startPubSubServer(t, s)
	subConn, subR := dialRaw(t, addr)
	pubConn, pubR := dialRaw(t, addr)

	send(t, subConn, "SUBSCRIBE", "ch")
	wantSubRow(t, "subscribe", subR, "subscribe", "ch", 1)
	subConn.Close() // 断连 → dropClient

	// 轮询直到发布计数归零（dropClient 在 handle 退出时执行）
	deadline := time.Now().Add(2 * time.Second)
	for {
		send(t, pubConn, "PUBLISH", "ch", "x")
		v, err := pubR.Read()
		if err != nil {
			t.Fatal(err)
		}
		if v.Num == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscriber was not cleaned up after disconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 新订阅者加入后恢复正常收发
	sub2, r2 := dialRaw(t, addr)
	send(t, sub2, "SUBSCRIBE", "ch")
	wantSubRow(t, "resubscribe", r2, "subscribe", "ch", 1)
	send(t, pubConn, "PUBLISH", "ch", "y")
	if v, err := pubR.Read(); err != nil || v.Num != 1 {
		t.Fatalf("publish after resubscribe: got %#v err=%v", v, err)
	}
	wantPushRow(t, "push to new sub", r2, "ch", "y")
}

// TestPubSubNotLoggedToAOF 订阅/发布/推送不落 AOF，数据命令照常落盘。
func TestPubSubNotLoggedToAOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	addr := startPubSubServer(t, s)

	subConn, subR := dialRaw(t, addr)
	send(t, subConn, "SUBSCRIBE", "ch")
	wantSubRow(t, "subscribe", subR, "subscribe", "ch", 1)
	pubConn, pubR := dialRaw(t, addr)
	send(t, pubConn, "PUBLISH", "ch", "secret-payload")
	if v, err := pubR.Read(); err != nil || v.Num != 1 {
		t.Fatalf("publish: got %#v err=%v", v, err)
	}
	wantPushRow(t, "push", subR, "ch", "secret-payload")
	send(t, pubConn, "SET", "k", "v")
	if v, err := pubR.Read(); err != nil || v.Str != "OK" {
		t.Fatalf("set: got %#v err=%v", v, err)
	}

	s.Close()
	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 1 {
		t.Fatalf("expected exactly the SET in AOF, got %v", cmds)
	}
	if cmds[0].Arr[0].Str != "SET" || cmds[0].Arr[1].Str != "k" {
		t.Fatalf("unexpected AOF content: %#v", cmds[0])
	}
}
