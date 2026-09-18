// Package persist provides append-only-file (AOF) persistence: write
// commands are stored RESP-encoded in arrival order, and the store state is
// rebuilt on startup by replaying the file.
package persist

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/hzzqq/redis-go/internal/resp"
)

// AOF is a concurrency-safe append-only file of RESP-encoded commands.
type AOF struct {
	mu sync.Mutex
	f  *os.File
}

// Open opens path for appending, creating it if needed.
func Open(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("aof: open %s: %w", path, err)
	}
	return &AOF{f: f}, nil
}

// Log appends one command value to the file.
func (a *AOF) Log(v resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return resp.WriteValue(a.f, v)
}

// Sync flushes OS buffers to disk.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Sync()
}

// Close closes the file.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.f.Close()
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
