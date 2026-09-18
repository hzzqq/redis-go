// Command bench is a minimal redis-benchmark-style load generator speaking
// raw RESP to a redis-go (or real Redis) server: pipelined SET/GET with
// configurable concurrency, reporting throughput and latency percentiles.
//
// 用法示例:
//
//	go run ./cmd/bench -addr 127.0.0.1:6379 -cmd SET -n 100000 -c 50 -pipeline 1
package main

import (
	"bytes"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "server address")
	cmdName := flag.String("cmd", "SET", "benchmark command: SET or GET")
	n := flag.Int("n", 100000, "total number of requests")
	concurrency := flag.Int("c", 50, "parallel connections")
	pipeline := flag.Int("pipeline", 1, "pipeline depth (commands per round-trip)")
	size := flag.Int("size", 64, "value size in bytes")
	flag.Parse()

	if *cmdName != "SET" && *cmdName != "GET" {
		fmt.Fprintf(os.Stderr, "bench: unsupported -cmd %q (SET|GET)\n", *cmdName)
		os.Exit(2)
	}
	if *n <= 0 || *concurrency <= 0 || *pipeline <= 0 {
		fmt.Fprintf(os.Stderr, "bench: -n/-c/-pipeline must be positive\n")
		os.Exit(2)
	}

	if *cmdName == "GET" {
		// GET 基线需要 key 先存在：先流水线灌入全部 key（不计入测量）
		fill(*addr, *n, *size)
	}

	value := string(bytes.Repeat([]byte("x"), *size))
	start := time.Now()
	lat, errs := run(*addr, *cmdName, *n, *concurrency, *pipeline, value)
	elapsed := time.Since(start)

	sort.Float64s(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		i := int(p * float64(len(lat)))
		if i >= len(lat) {
			i = len(lat) - 1
		}
		return lat[i]
	}
	fmt.Printf("====== %s ======\n", *cmdName)
	fmt.Printf("  %d requests completed in %.2fs\n", *n, elapsed.Seconds())
	fmt.Printf("  %d parallel clients, pipeline %d\n", *concurrency, *pipeline)
	fmt.Printf("  throughput: %.2f ops/s\n", float64(*n)/elapsed.Seconds())
	fmt.Printf("  latency p50: %.3fms  p90: %.3fms  p99: %.3fms  max: %.3fms\n",
		pct(0.50), pct(0.90), pct(0.99), pct(1.00))
	fmt.Printf("  errors: %d\n", errs)
	if errs > 0 {
		os.Exit(1)
	}
}

// fill pipelines SET commands for keys [0, n) on one connection.
func fill(addr string, n, size int) {
	value := string(bytes.Repeat([]byte("x"), size))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	r := resp.NewReader(conn)
	const depth = 256
	buf := &bytes.Buffer{}
	done := 0
	for i := 0; i < n; i++ {
		if err := resp.WriteValue(buf, mkSet(keyName(i), value)); err != nil {
			fatal(err)
		}
		if (i+1)%depth == 0 || i == n-1 {
			if _, err := conn.Write(buf.Bytes()); err != nil {
				fatal(err)
			}
			buf.Reset()
			for j := 0; j <= i-done; j++ {
				v, err := r.Read()
				if err != nil {
					fatal(err)
				}
				if v.Type == resp.Error {
					fatal(fmt.Errorf("fill: %s", v.Str))
				}
			}
			done = i + 1
		}
	}
}

// run drives the measured phase: workers split [0, n) into contiguous ranges,
// each with its own connection; a batch of `pipeline` commands is sent in one
// write and its per-op latency is approximated as batch latency / pipeline.
func run(addr, cmdName string, n, concurrency, pipeline int, value string) ([]float64, int64) {
	lat := make([][]float64, concurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var errs int64

	for w := 0; w < concurrency; w++ {
		lo := n * w / concurrency
		hi := n * (w + 1) / concurrency
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			if lo >= hi {
				return
			}
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				fatal(err)
			}
			defer conn.Close()
			r := resp.NewReader(conn)
			buf := &bytes.Buffer{}
			samples := make([]float64, 0, (hi-lo)/pipeline+1)
			for i := lo; i < hi; i += pipeline {
				batch := pipeline
				if i+batch > hi {
					batch = hi - i
				}
				buf.Reset()
				for j := 0; j < batch; j++ {
					var v resp.Value
					if cmdName == "SET" {
						v = mkSet(keyName(i+j), value)
					} else {
						v = mkGet(keyName(i + j))
					}
					if err := resp.WriteValue(buf, v); err != nil {
						fatal(err)
					}
				}
				t0 := time.Now()
				if _, err := conn.Write(buf.Bytes()); err != nil {
					fatal(err)
				}
				for j := 0; j < batch; j++ {
					reply, err := r.Read()
					if err != nil {
						fatal(err)
					}
					if reply.Type == resp.Error {
						mu.Lock()
						errs++
						mu.Unlock()
					}
				}
				perOp := float64(time.Since(t0).Microseconds()) / 1000 / float64(batch)
				samples = append(samples, perOp)
			}
			mu.Lock()
			lat[w] = samples
			mu.Unlock()
		}(w, lo, hi)
	}
	wg.Wait()

	all := make([]float64, 0, n)
	for _, s := range lat {
		all = append(all, s...)
	}
	return all, errs
}

func keyName(i int) string { return fmt.Sprintf("key:%d", i) }

func mkSet(key, val string) resp.Value {
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: "SET"},
		{Type: resp.BulkString, Str: key},
		{Type: resp.BulkString, Str: val},
	}}
}

func mkGet(key string) resp.Value {
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: "GET"},
		{Type: resp.BulkString, Str: key},
	}}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "bench: %v\n", err)
	os.Exit(1)
}
