package persist

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hzzqq/redis-go/internal/resp"
)

// mkArr 构造一个由 bulk string 组成的数组 RESP 值。
func mkArr(parts ...string) resp.Value {
	arr := make([]resp.Value, len(parts))
	for i, p := range parts {
		arr[i] = resp.Value{Type: resp.BulkString, Str: p}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

// TestLogLoadRoundTrip 写入的命令经 Load 读回应逐条一致。
func TestLogLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cmds := []resp.Value{
		mkArr("SET", "a", "1"),
		mkArr("LPUSH", "l", "x", "y"),
		mkArr("HSET", "h", "f", "v"),
	}
	for _, c := range cmds {
		if err := a.Log(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Sync(); err != nil {
		t.Fatal(err)
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
		if len(got[i].Arr) != len(c.Arr) {
			t.Fatalf("command %d: expected %d parts, got %d", i, len(c.Arr), len(got[i].Arr))
		}
		for j, part := range c.Arr {
			if got[i].Arr[j].Str != part.Str {
				t.Fatalf("command %d part %d: expected %q, got %q", i, j, part.Str, got[i].Arr[j].Str)
			}
		}
	}
}

// TestLoadMissingFile 缺失文件 → (nil, nil)，新服务器可冷启动。
func TestLoadMissingFile(t *testing.T) {
	got, err := Load(filepath.Join(t.TempDir(), "nope.aof"))
	if err != nil || got != nil {
		t.Fatalf("expected (nil,nil), got (%v,%v)", got, err)
	}
}

// TestLoadTruncated 截断尾部被容忍：完整命令 + error 一并返回。
func TestLoadTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Log(mkArr("SET", "a", "1"))
	_ = a.Close()
	// 追加残缺 bulk
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("$3\r\nab"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	got, err := Load(path)
	if err == nil {
		t.Fatal("expected error for truncated tail")
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 complete command before truncation, got %d", len(got))
	}
	if !strings.Contains(err.Error(), "load stopped at command 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}
