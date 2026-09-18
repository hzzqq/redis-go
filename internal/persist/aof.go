// Package persist provides append-only-file (AOF) persistence: write
// commands are stored RESP-encoded in arrival order, and the store state is
// rebuilt on startup by replaying the file.
package persist

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
)

// AOF is a concurrency-safe append-only file of RESP-encoded commands, with
// a pluggable fsync policy (Redis appendfsync):
//   - "always": every Log call also fsyncs before returning — the caller's
//     reply only goes out once the bytes survive a power loss;
//   - "everysec": a background goroutine fsyncs once per second — a crash
//     loses at most ~1s of acknowledged writes (Redis's default trade-off);
//   - "no" (default): bytes reach the OS page cache only; durability is the
//     kernel's business (original redis-go behavior).
type AOF struct {
	mu        sync.Mutex
	f         *os.File
	fsyncMode string

	// everysec loop lifecycle: syncStop is closed to stop the ticker
	// goroutine, syncDone follows when it has exited (guarded by mu).
	syncStop chan struct{}
	syncDone chan struct{}
}

// Open opens path for appending, creating it if needed.
func Open(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("aof: open %s: %w", path, err)
	}
	return &AOF{f: f}, nil
}

// SetFsync switches the fsync policy ("always" / "everysec" / "no"; unknown
// values fall back to "no"). Switching to everysec starts the background
// flusher, switching away stops it. Safe to call repeatedly.
func (a *AOF) SetFsync(mode string) {
	if mode != "always" && mode != "everysec" && mode != "no" {
		mode = "no"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fsyncMode = mode
	if mode == "everysec" {
		if a.syncStop == nil { // not running yet
			a.syncStop = make(chan struct{})
			a.syncDone = make(chan struct{})
			go a.fsyncLoop(a.syncStop, a.syncDone)
		}
		return
	}
	if a.syncStop != nil { // running → stop it
		close(a.syncStop)
		a.syncStop, a.syncDone = nil, nil
	}
}

// fsyncLoop flushes the OS buffers once per second until stop is closed.
// The fsync itself takes a.mu (shared with Log/Rewrite/Close), so a big
// backlog cannot stall appends beyond one fsync duration.
func (a *AOF) fsyncLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			a.Sync()
		}
	}
}

// fsyncNow syncs the file honoring the current mode; called by Log under mu.
func (a *AOF) fsyncNow() error {
	if a.fsyncMode == "always" {
		return a.f.Sync()
	}
	return nil
}

// Log appends one command value to the file. With the "always" policy the
// data is fsynced before Log returns (synchronous durability).
func (a *AOF) Log(v resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := resp.WriteValue(a.f, v); err != nil {
		return err
	}
	return a.fsyncNow()
}

// Sync flushes OS buffers to disk.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Sync()
}

// Close stops any background flusher and closes the file.
func (a *AOF) Close() error {
	a.mu.Lock()
	stop, done := a.syncStop, a.syncDone
	a.syncStop, a.syncDone = nil, nil
	a.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Close()
}

// Rewrite atomically replaces the AOF contents with cmds (the minimal
// canonical command set produced by a store snapshot): RESP-encode to
// <path>.tmp, fsync, then rename over the original file and reopen it for
// appending. On Windows a file cannot be renamed over an open handle, so the
// old handle is closed just before the rename and a fresh one is opened
// after. The caller must guarantee no Log is in flight (the server
// serializes writes); a.mu still guards Log/Sync/Close from other paths.
func (a *AOF) Rewrite(cmds []resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	tmp := a.f.Name() + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("aof: rewrite: open %s: %w", tmp, err)
	}
	werr := func() error {
		for _, v := range cmds {
			if err := resp.WriteValue(tf, v); err != nil {
				return err
			}
		}
		return tf.Sync()
	}()
	if cerr := tf.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(tmp)
		return fmt.Errorf("aof: rewrite: %w", werr)
	}
	// 先关旧句柄再改名（Windows 限制）；改名失败则重开旧文件兜底，状态不受损。
	if err := a.f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("aof: rewrite: close old: %w", err)
	}
	if err := os.Rename(tmp, a.f.Name()); err != nil {
		os.Remove(tmp)
		if nf, e2 := os.OpenFile(a.f.Name(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); e2 == nil {
			a.f = nf
		}
		return fmt.Errorf("aof: rewrite: rename: %w", err)
	}
	nf, err := os.OpenFile(a.f.Name(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("aof: rewrite: reopen: %w", err)
	}
	a.f = nf
	return nil
}

// Load reads every RESP value in path. A missing file yields (nil, nil) so a
// fresh server starts clean. A truncated tail (e.g. crash mid-write) is
// tolerated: every command parsed before the truncation point is returned
// together with the error, mirroring Redis's aof-load-truncated yes mode.
func Load(path string) ([]resp.Value, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("aof: load %s: %w", path, err)
	}
	defer f.Close()
	r := resp.NewReader(f)
	var cmds []resp.Value
	for {
		v, err := r.Read()
		if err == io.EOF {
			return cmds, nil
		}
		if err != nil {
			return cmds, fmt.Errorf("aof: %s: load stopped at command %d: %w", path, len(cmds), err)
		}
		cmds = append(cmds, v)
	}
}
