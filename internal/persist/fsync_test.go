// appendfsync 三档策略测试：模式回退、everysec 后台刷盘 goroutine 的启停
// 生命周期（含 Close 等待退出、重复切换不泄漏）、跨模式切换的数据完整性。
package persist

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
)

// TestSetFsyncFallback 非法值回退为 "no"；Open 默认无模式（等同 no）。
func TestSetFsyncFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.fsyncMode != "" {
		t.Fatalf("default mode must be empty (no), got %q", a.fsyncMode)
	}
	a.SetFsync("bogus")
	if a.fsyncMode != "no" {
		t.Fatalf("bogus must fall back to no, got %q", a.fsyncMode)
	}
	a.SetFsync("ALWAYS") // 大写同样非法（Redis 配置值小写）
	if a.fsyncMode != "no" {
		t.Fatalf("case-sensitive values expected, got %q", a.fsyncMode)
	}
}

// TestSetFsyncEverysecLifecycle everysec 启动后台刷盘；切走停止；重复切换
// 不重复启动；Close 等待 goroutine 退出（测试不超时即通过）。
func TestSetFsyncEverysecLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a.SetFsync("everysec")
	if a.syncStop == nil {
		t.Fatal("everysec must start the background flusher")
	}
	stop1 := a.syncStop
	a.SetFsync("everysec") // 重复开启：复用同一 goroutine
	if a.syncStop != stop1 {
		t.Fatal("repeated everysec must reuse the running flusher")
	}
	a.SetFsync("no")
	if a.syncStop != nil {
		t.Fatal("switching away must stop the flusher")
	}
	a.SetFsync("everysec")
	if a.syncStop == nil || a.syncStop == stop1 {
		t.Fatal("re-enabling everysec must start a fresh flusher")
	}
	// Close 必须停掉刷盘并等待退出
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if a.syncStop != nil {
		t.Fatal("Close must clear the flusher")
	}
	// 已停止的刷盘不会在 Close 后再触碰文件：再 Log 应返回错误而非 panic
	if err := a.Log(mkArr("SET", "x", "1")); err == nil {
		t.Fatal("Log after Close must fail")
	}
}

// TestFsyncEverysecFlushesInBackground everysec 模式下写入后不显式 Sync，
// 等待刷盘周期（>1s）后 Close，Load 仍能读到全部数据。
func TestFsyncEverysecFlushesInBackground(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a.SetFsync("everysec")
	cmds := []resp.Value{
		mkArr("SET", "a", "1"),
		mkArr("RPUSH", "l", "x"),
	}
	for _, c := range cmds {
		if err := a.Log(c); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1300 * time.Millisecond) // 越过一个 1s ticker 周期
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(cmds) {
		t.Fatalf("expected %d commands after background flush, got %d", len(cmds), len(got))
	}
}

// TestFsyncModeSwitchDataIntegrity 同一文件跨 no → everysec → always → no
// 切换写入，Close 后 Load 逐条一致（fsync 策略不影响可见的数据序列）。
func TestFsyncModeSwitchDataIntegrity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cmds := []resp.Value{
		mkArr("SET", "a", "1"), // no
		mkArr("SET", "b", "2"), // everysec
		mkArr("SET", "c", "3"), // always（同步刷盘）
		mkArr("SET", "d", "4"), // 回到 no
	}
	modes := []string{"no", "everysec", "always", "no"}
	for i, c := range cmds {
		a.SetFsync(modes[i])
		if err := a.Log(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(cmds) {
		t.Fatalf("expected %d commands, got %d", len(cmds), len(got))
	}
	for i, c := range cmds {
		if got[i].Arr[1].Str != c.Arr[1].Str {
			t.Fatalf("cmd %d: expected %s, got %s", i, c.Arr[1].Str, got[i].Arr[1].Str)
		}
	}
}

// TestFsyncStallWatchdog everysec 停滞看门狗：刚切到 everysec（lastSync 已随
// 启动刷新）不误报；模拟后台刷盘停滞（lastSync 拨回 31s 前）时主路径 Log 兜底
// 同步并计数一次（INFO aof_fsync_stalls）；兜底后 lastSync 已刷新，紧接着的
// Log 不再计数；显式 Sync 同样刷新时间戳。
func TestFsyncStallWatchdog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	a.SetFsync("everysec")
	// 启动即写：时间戳已随 SetFsync 刷新，不得误触发兜底
	if err := a.Log(mkArr("SET", "boot", "1")); err != nil {
		t.Fatal(err)
	}
	if n := a.Stalls(); n != 0 {
		t.Fatalf("fresh everysec must not trip the watchdog, stalls=%d", n)
	}
	// 模拟停滞：最近成功 fsync 拨回 31s 前（首秒 ticker 尚未轮到，无竞态）
	a.lastSync.Store(time.Now().Add(-31 * time.Second).UnixNano())
	if err := a.Log(mkArr("SET", "stall", "1")); err != nil {
		t.Fatal(err)
	}
	if n := a.Stalls(); n != 1 {
		t.Fatalf("stalled flusher must trip the watchdog once, stalls=%d", n)
	}
	// 兜底同步已刷新 lastSync：紧接着的 Log 不再计数
	if err := a.Log(mkArr("SET", "ok", "1")); err != nil {
		t.Fatal(err)
	}
	if n := a.Stalls(); n != 1 {
		t.Fatalf("refreshed lastSync must suppress the watchdog, stalls=%d", n)
	}
	// 显式 Sync 同样刷新：再拨回过去后 Sync，Log 不触发
	a.lastSync.Store(time.Now().Add(-31 * time.Second).UnixNano())
	if err := a.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := a.Log(mkArr("SET", "ok2", "1")); err != nil {
		t.Fatal(err)
	}
	if n := a.Stalls(); n != 1 {
		t.Fatalf("explicit Sync must refresh lastSync, stalls=%d", n)
	}
	// 数据完整：4 条全在
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 commands, got %d", len(got))
	}
}
