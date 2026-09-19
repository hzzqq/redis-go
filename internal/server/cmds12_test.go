// Phase 12 消费者组命令层测试：XGROUP 子命令、XREADGROUP '>'/history/NOACK、
// canonical XCLAIM FORCE JUSTID 帧化（AOF/重放收敛）、MULTI 内非阻塞执行、
// BLOCK 唤醒（同组 FIFO 单消费）、WATCH 触碰、脚本禁用、副本拒写豁免形态。
package server

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
)

// xrow 提取 XREAD 族回复的 [stream, id...] 行。
func xrow(t *testing.T, r resp.Value) map[string][]string {
	t.Helper()
	if r.Type != resp.Array {
		t.Fatalf("expected array, got type=%c str=%q", r.Type, r.Str)
	}
	out := map[string][]string{}
	for _, row := range r.Arr {
		name := row.Arr[0].Str
		ids := []string{}
		for _, en := range row.Arr[1].Arr {
			ids = append(ids, en.Arr[0].Str)
		}
		out[name] = ids
	}
	return out
}

func TestXGroupSubcommands(t *testing.T) {
	s := New()
	if r := s.dispatch(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM")); r.Str != "OK" {
		t.Fatalf("CREATE MKSTREAM: %+v", r)
	}
	// BUSYGROUP
	wantErr(t, "busygroup", s.dispatch(mkCmd("XGROUP", "CREATE", "st", "g", "0-0")),
		"BUSYGROUP")
	// key 不存在且无 MKSTREAM
	wantErr(t, "key required", s.dispatch(mkCmd("XGROUP", "CREATE", "nope", "g", "0-0")),
		"requires the key to exist")
	// $ 定位（空流 → 0-0）
	if r := s.dispatch(mkCmd("XGROUP", "CREATE", "st", "g2", "$")); r.Str != "OK" {
		t.Fatalf("CREATE $: %+v", r)
	}
	// 显式 ID 重放后 '>' 从该 ID 之后开始
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "a"))
	s.dispatch(mkCmd("XADD", "st", "2-0", "f", "b"))
	s.dispatch(mkCmd("XGROUP", "SETID", "st", "g2", "1-0"))
	r := s.dispatch(mkCmd("XREADGROUP", "GROUP", "g2", "c", "STREAMS", "st", ">"))
	if ids := xrow(t, r)["st"]; len(ids) != 1 || ids[0] != "2-0" {
		t.Fatalf("SETID effect: %v", ids)
	}
	// CREATECONSUMER / DELCONSUMER
	wantInt(t, "createconsumer", s.dispatch(
		mkCmd("XGROUP", "CREATECONSUMER", "st", "g2", "alice")), 1)
	wantInt(t, "createconsumer again", s.dispatch(
		mkCmd("XGROUP", "CREATECONSUMER", "st", "g2", "alice")), 0)
	wantInt(t, "delconsumer missing", s.dispatch(
		mkCmd("XGROUP", "DELCONSUMER", "st", "g2", "ghost")), 0)
	// DESTROY：先挂一个组再删
	s.dispatch(mkCmd("XGROUP", "CREATE", "st", "g3", "0-0"))
	wantInt(t, "destroy", s.dispatch(mkCmd("XGROUP", "DESTROY", "st", "g3")), 1)
	wantInt(t, "destroy again", s.dispatch(mkCmd("XGROUP", "DESTROY", "st", "g3")), 0)
	// 错误形态
	wantErr(t, "setid busygroup", s.dispatch(
		mkCmd("XGROUP", "SETID", "st", "nope", "0-0")), "BUSYGROUP")
	wantErr(t, "bad id", s.dispatch(
		mkCmd("XGROUP", "CREATE", "st", "g9", "zz")), "Invalid stream ID")
}

func TestXReadGroupNewAndHistory(t *testing.T) {
	s := New()
	s.dispatch(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM"))
	s.dispatch(mkCmd("XADD", "st", "1-0", "f", "a"))
	s.dispatch(mkCmd("XADD", "st", "2-0", "f", "b"))
	s.dispatch(mkCmd("XADD", "st", "3-0", "f", "c"))

	// '>' + COUNT 1：逐条投递
	r := s.dispatch(mkCmd("XREADGROUP", "GROUP", "g", "alice", "COUNT", "1", "STREAMS", "st", ">"))
	if ids := xrow(t, r)["st"]; len(ids) != 1 || ids[0] != "1-0" {
		t.Fatalf("count 1: %v", ids)
	}
	// history：显式 ID 只回该消费者 PEL 中 > start 的条目（无论 lastDelivered）
	r = s.dispatch(mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", "0-0"))
	if ids := xrow(t, r)["st"]; len(ids) != 1 || ids[0] != "1-0" {
		t.Fatalf("history re-read: %v", ids)
	}
	// NOACK '>' + COUNT 1：bob 拿 2-0 但不进 PEL（lastDelivered 仍推进）
	r = s.dispatch(mkCmd("XREADGROUP", "GROUP", "g", "bob", "NOACK", "COUNT", "1", "STREAMS", "st", ">"))
	if ids := xrow(t, r)["st"]; len(ids) != 1 || ids[0] != "2-0" {
		t.Fatalf("noack delivery: %v", ids)
	}
	// PEL：只有 alice 的 1-0（NOACK 无 PEL）
	r = s.dispatch(mkCmd("XPENDING", "st", "g"))
	if r.Arr[0].Num != 1 {
		t.Fatalf("pending count: %+v", r.Arr[0])
	}
	// alice 继续 '>'：3-0（bob 的 NOACK 推进了位点）
	r = s.dispatch(mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", ">"))
	if ids := xrow(t, r)["st"]; len(ids) != 1 || ids[0] != "3-0" {
		t.Fatalf("next delivery: %v", ids)
	}
	// 全部投喂完：null array
	r = s.dispatch(mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", ">"))
	if r.Type != resp.Array || !r.Null {
		t.Fatalf("exhausted: %+v", r)
	}
	// 缺 GROUP / 不平衡 / BAD ID
	wantErr(t, "no group", s.dispatch(
		mkCmd("XREADGROUP", "STREAMS", "st", ">")), "GROUP subcommand is required")
	wantErr(t, "unbalanced", s.dispatch(
		mkCmd("XREADGROUP", "GROUP", "g", "c", "STREAMS", "st")), "Unbalanced")
	wantErr(t, "bad id", s.dispatch(
		mkCmd("XREADGROUP", "GROUP", "g", "c", "STREAMS", "st", "zz")), "Invalid stream ID")
	// NOGROUP（组缺失）
	wantErr(t, "nogroup", s.dispatch(
		mkCmd("XREADGROUP", "GROUP", "nope", "c", "STREAMS", "st", ">")), "NOGROUP")
	// history 且组缺失 → NOGROUP（key 存在）
	wantErr(t, "nogroup history", s.dispatch(
		mkCmd("XREADGROUP", "GROUP", "nope", "c", "STREAMS", "st", "0-0")), "NOGROUP")
	// history：count 刷新（第 1 次 history 读在前面、此处第 2 次 → 1-0 计数 3）
	s.dispatch(mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", "0-0"))
	r = s.dispatch(mkCmd("XPENDING", "st", "g", "-", "+", "10"))
	if r.Arr[0].Arr[3].Num != 3 {
		t.Fatalf("history must bump delivery count: %+v", r.Arr[0])
	}
}

func TestXReadGroupCanonicalFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// 组与数据经 apply（写路径）落盘
	s.apply(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM"))
	s.apply(mkCmd("XADD", "st", "1-0", "f", "a"))
	s.apply(mkCmd("XADD", "st", "2-0", "f", "b"))
	// XREADGROUP 经 apply：本身不落盘，PEL 效果帧化为 XCLAIM FORCE JUSTID
	s.apply(mkCmd("XREADGROUP", "GROUP", "g", "alice", "COUNT", "1", "STREAMS", "st", ">"))
	s.apply(mkCmd("XREADGROUP", "GROUP", "g", "bob", "NOACK", "STREAMS", "st", ">"))
	s.Close()

	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range cmds {
		names = append(names, mustFirstCmd(c))
	}
	got := strings.Join(names, ",")
	wantPre := "XGROUP,XADD,XADD,XCLAIM,XGROUP,XGROUP"
	if !strings.HasPrefix(got, wantPre) {
		t.Fatalf("AOF frames = %s, want prefix %s (XREADGROUP itself must not persist)", got, wantPre)
	}
	// 恰 6 帧：alice 的 XCLAIM(1-0) + SETID(1-0)；bob 的 NOACK 只落 SETID(2-0)
	if len(cmds) != 6 {
		t.Fatalf("AOF must hold exactly 6 frames, got %d: %s", len(cmds), got)
	}
	cl := cmds[3]
	if mustFirstCmd(cl) != "XCLAIM" || cl.Arr[1].Str != "st" || cl.Arr[2].Str != "g" ||
		cl.Arr[3].Str != "alice" || cl.Arr[4].Str != "0" || cl.Arr[5].Str != "1-0" ||
		mustFirstCmd(cl) == "" || cl.Arr[10].Str != "FORCE" || cl.Arr[11].Str != "JUSTID" {
		t.Fatalf("XCLAIM frame shape: %+v", cl)
	}
	// 重放收敛：新 store 逐帧 dispatch → PEL 计数与位点一致
	s2 := New()
	for _, c := range cmds {
		s2.dispatch(c)
	}
	r := s2.dispatch(mkCmd("XPENDING", "st", "g"))
	if r.Arr[0].Num != 1 {
		t.Fatalf("replayed PEL count: %+v", r.Arr[0])
	}
	// 重放侧 '>' 位点应为 2-0（NOACK 读的推进经 SETID 帧恢复）：1-0 不重复
	// 投递，且其上没有新条目 → null
	r = s2.dispatch(mkCmd("XREADGROUP", "GROUP", "g", "carol", "STREAMS", "st", ">"))
	if r.Type != resp.Array || !r.Null {
		t.Fatalf("replayed '>' position: %+v", r)
	}
}

func TestXReadGroupInMulti(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.apply(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM"))
	s.apply(mkCmd("XADD", "st", "1-0", "f", "a"))
	cl := newTestClient(t)
	if r := s.applyConn(cl, mkCmd("MULTI")); r.Str != "OK" {
		t.Fatalf("MULTI: %+v", r)
	}
	s.applyConn(cl, mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", ">"))
	s.applyConn(cl, mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", ">"))
	exec := s.applyConn(cl, mkCmd("EXEC"))
	if exec.Type != resp.Array || len(exec.Arr) != 2 {
		t.Fatalf("EXEC shape: %+v", exec)
	}
	// 槽位 1：投到 1-0；槽位 2：已投完 → null
	if ids := xrow(t, exec.Arr[0])["st"]; len(ids) != 1 || ids[0] != "1-0" {
		t.Fatalf("txn slot1: %v", ids)
	}
	if !exec.Arr[1].Null {
		t.Fatalf("txn slot2 must be null: %+v", exec.Arr[1])
	}
	// AOF：MULTI 块内 XCLAIM + XGROUP SETID（XREADGROUP 的 canonical 形态），
	// 无 XREADGROUP
	s.Close()
	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 6 { // XGROUP, XADD, MULTI, XCLAIM, SETID, EXEC
		var names []string
		for _, c := range cmds {
			names = append(names, mustFirstCmd(c))
		}
		t.Fatalf("AOF frames: %v", names)
	}
	sawClaim := false
	inBlock := false
	for _, c := range cmds {
		name := mustFirstCmd(c)
		switch {
		case name == "MULTI":
			inBlock = true
		case name == "EXEC":
			inBlock = false
		case inBlock && name == "XCLAIM":
			if c.Arr[3].Str != "alice" || c.Arr[5].Str != "1-0" {
				t.Fatalf("block frame: %+v", c)
			}
			sawClaim = true
		case inBlock && name == "XGROUP":
			if c.Arr[1].Str != "SETID" || c.Arr[4].Str != "1-0" {
				t.Fatalf("block SETID frame: %+v", c)
			}
		}
	}
	if !sawClaim {
		t.Fatal("MULTI block must contain the XCLAIM frame")
	}
}

func TestXReadGroupBlockingWake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	s, err := NewWithAOF(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.apply(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM"))
	cl := newTestClient(t)
	done := make(chan resp.Value, 1)
	go func() {
		done <- s.cmdXReadGroupConn(cl, mkCmd("XREADGROUP", "GROUP", "g", "alice",
			"BLOCK", "0", "STREAMS", "st", ">"))
	}()
	time.Sleep(50 * time.Millisecond) // 让等待者先入队
	s.apply(mkCmd("XADD", "st", "5-0", "f", "wake"))

	select {
	case r := <-done:
		if ids := xrow(t, r)["st"]; len(ids) != 1 || ids[0] != "5-0" {
			t.Fatalf("wake reply: %v", ids)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken by XADD")
	}
	// PEL：alice 持有 5-0
	r := s.dispatch(mkCmd("XPENDING", "st", "g"))
	if r.Arr[0].Num != 1 {
		t.Fatalf("PEL after wake: %+v", r.Arr[0])
	}
	// 同组 FIFO 单消费：第二个等待者不被同一批条目重复投喂（5-0 已推进）
	// AOF：XGROUP + XADD + XCLAIM FORCE JUSTID + XGROUP SETID（位点推进）
	s.Close()
	cmds, err := persist.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 4 || mustFirstCmd(cmds[2]) != "XCLAIM" ||
		cmds[2].Arr[3].Str != "alice" || cmds[2].Arr[5].Str != "5-0" ||
		cmds[2].Arr[10].Str != "FORCE" ||
		mustFirstCmd(cmds[3]) != "XGROUP" || cmds[3].Arr[1].Str != "SETID" ||
		cmds[3].Arr[4].Str != "5-0" {
		var names []string
		for _, c := range cmds {
			names = append(names, mustFirstCmd(c))
		}
		t.Fatalf("AOF frames: %v", names)
	}
	// BLOCK 超时 → null array
	r = s.cmdXReadGroupConn(newTestClient(t), mkCmd("XREADGROUP", "GROUP", "g",
		"bob", "BLOCK", "50", "STREAMS", "st", ">"))
	if r.Type != resp.Array || !r.Null {
		t.Fatalf("timeout: %+v", r)
	}
	// BLOCK + history ID → 错误
	wantErr(t, "block history", s.cmdXReadGroupConn(newTestClient(t),
		mkCmd("XREADGROUP", "GROUP", "g", "bob", "BLOCK", "50", "STREAMS", "st", "0-0")),
		"BLOCK option is only allowed for >")
}

func TestXReadGroupWatchTouch(t *testing.T) {
	s := New()
	s.apply(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM"))
	cl := newTestClient(t)
	if r := s.cmdWatch(cl, []resp.Value{
		{Type: resp.BulkString, Str: "st"}}); r.Str != "OK" {
		t.Fatalf("WATCH: %+v", r)
	}
	// 其他连接的 XREADGROUP 投喂触碰 st（writeCmdKeys 提取 STREAMS keys）
	s.apply(mkCmd("XADD", "st", "1-0", "f", "a"))
	s.apply(mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", ">"))
	s.applyConn(cl, mkCmd("MULTI"))
	s.applyConn(cl, mkCmd("SET", "k", "v"))
	if exec := s.applyConn(cl, mkCmd("EXEC")); !exec.Null {
		t.Fatalf("WATCH on stream must be touched by XREADGROUP: %+v", exec)
	}
}

func TestXReadGroupScriptDenied(t *testing.T) {
	s := New()
	s.apply(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM"))
	r := s.cmdEval("EVAL", []resp.Value{
		{Type: resp.BulkString, Str: "return redis.call('XREADGROUP','GROUP','g','c','STREAMS','st','>')"},
		{Type: resp.BulkString, Str: "0"}})
	wantErr(t, "script", r, "not allowed from script")
	// XGROUP/XACK/XCLAIM 脚本内放行（普通写命令）
	r = s.cmdEval("EVAL", []resp.Value{
		{Type: resp.BulkString, Str: "return redis.call('XGROUP','CREATE','st','g2','0-0')"},
		{Type: resp.BulkString, Str: "0"}})
	if r.Str != "OK" {
		t.Fatalf("XGROUP in script: %+v", r)
	}
}

func TestXClaimCommand(t *testing.T) {
	s := New()
	s.apply(mkCmd("XGROUP", "CREATE", "st", "g", "0-0", "MKSTREAM"))
	s.apply(mkCmd("XADD", "st", "1-0", "f", "a"))
	s.apply(mkCmd("XADD", "st", "2-0", "f", "b"))
	s.apply(mkCmd("XREADGROUP", "GROUP", "g", "alice", "STREAMS", "st", ">"))

	// FORCE + min-idle 0：把 1-0 从 alice 认领到 bob（附 TIME 回拨模拟陈旧）
	// min-idle 0：1-0、2-0 全部认领到 bob
	r := s.dispatch(mkCmd("XCLAIM", "st", "g", "bob", "0", "1-0", "2-0"))
	if len(r.Arr) != 2 || r.Arr[0].Arr[0].Str != "1-0" || r.Arr[1].Arr[1].Arr[1].Str != "b" {
		t.Fatalf("claim reply: %+v", r)
	}
	// 认领后 PEL 归属：bob 2 条（summary 消费者按名字字典序，仅剩 bob）
	r = s.dispatch(mkCmd("XPENDING", "st", "g"))
	if r.Arr[3].Arr[0].Arr[0].Str != "bob" || r.Arr[3].Arr[0].Arr[1].Num != 2 {
		t.Fatalf("per-consumer after claim: %+v", r.Arr[3])
	}
	// min-idle 门：回拨 bob 的 1-0 到 60s 前，min-idle 30s 只认领它
	s.dispatch(mkCmd("XCLAIM", "st", "g", "bob", "0", "1-0", "TIME", "1", "RETRYCOUNT", "5"))
	r = s.dispatch(mkCmd("XCLAIM", "st", "g", "carol", "30000", "1-0", "2-0", "JUSTID"))
	if len(r.Arr) != 1 || r.Arr[0].Str != "1-0" {
		t.Fatalf("min-idle justid claim: %+v", r)
	}
	// RETRYCOUNT 已在 TIME 步设定为 5，carol JUSTID 不改计数
	r = s.dispatch(mkCmd("XPENDING", "st", "g", "-", "+", "10", "carol"))
	if r.Arr[0].Arr[3].Num != 5 {
		t.Fatalf("JUSTID must keep count: %+v", r.Arr[0])
	}
	// FORCE 认领不在 PEL 的条目：3-0 不存在 → 先加；4-0 存在但未投递
	s.apply(mkCmd("XADD", "st", "4-0", "f", "d"))
	r = s.dispatch(mkCmd("XCLAIM", "st", "g", "dave", "0", "4-0", "FORCE"))
	if len(r.Arr) != 1 || r.Arr[0].Arr[0].Str != "4-0" {
		t.Fatalf("force claim: %+v", r)
	}
	// 已删除条目：XCLAIM 清 PEL 且不返回
	s.apply(mkCmd("XDEL", "st", "4-0"))
	r = s.dispatch(mkCmd("XCLAIM", "st", "g", "dave", "0", "4-0", "FORCE"))
	if len(r.Arr) != 0 {
		t.Fatalf("ghost claim must be empty: %+v", r)
	}
}
