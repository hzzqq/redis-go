// Command bench is a minimal redis-benchmark-style load generator speaking
// raw RESP to a redis-go (or real Redis) server: pipelined SET/GET/EVAL with
// configurable concurrency, plus a WATCH+MULTI+EXEC optimistic-lock (CAS)
// mode, reporting throughput and latency percentiles.
//
// 用法示例:
//
//	go run ./cmd/bench -addr 127.0.0.1:6379 -cmd SET -n 100000 -c 50 -pipeline 1
//	go run ./cmd/bench -addr 127.0.0.1:6379 -cmd EVAL -n 20000 -c 20
//	go run ./cmd/bench -addr 127.0.0.1:6379 -cmd CAS -n 5000 -c 10
package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
)

// evalScript 是 EVAL/EVALSHA 基准的脚本：一次带参写（走效果复制路径：
// canonical 化 + AOF + 传播）。EVALSHA 用其 sha1（先 EVAL 预热登记缓存）。
const evalScript = "redis.call('SET', KEYS[1], ARGV[1]) return 1"

func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "server address")
	cmdName := flag.String("cmd", "SET", "benchmark command: SET|GET|EVAL|EVALSHA|CAS")
	n := flag.Int("n", 100000, "total number of requests")
	concurrency := flag.Int("c", 50, "parallel connections")
	pipeline := flag.Int("pipeline", 1, "pipeline depth (commands per round-trip)")
	size := flag.Int("size", 64, "value size in bytes")
	flag.Parse()

	switch *cmdName {
	case "SET", "GET", "EVAL", "EVALSHA", "CAS":
	default:
		fmt.Fprintf(os.Stderr, "bench: unsupported -cmd %q (SET|GET|EVAL|EVALSHA|CAS)\n", *cmdName)
		os.Exit(2)
	}
	if *n <= 0 || *concurrency <= 0 || *pipeline <= 0 {
		fmt.Fprintf(os.Stderr, "bench: -n/-c/-pipeline must be positive\n")
		os.Exit(2)
	}
	if *cmdName == "CAS" && *pipeline != 1 {
		fmt.Fprintf(os.Stderr, "bench: CAS mode forces -pipeline 1 (4 round trips per op)\n")
		*pipeline = 1
	}

	value := string(bytes.Repeat([]byte("x"), *size))
	if *cmdName == "CAS" {
		// CAS 模式不走命令 builder（每 op 是 WATCH→MULTI→SET→EXEC 序列）
		start := time.Now()
		lat, errs := runCAS(*addr, *n, *concurrency, value)
		elapsed := time.Since(start)
		report(*cmdName, *n, *concurrency, 1, lat, errs, elapsed)
		if errs > 0 {
			os.Exit(1)
		}
		return
	}
	mk := mkCmdBuilder(*cmdName, value)

	if *cmdName == "GET" {
		// GET 基线需要 key 先存在：先流水线灌入全部 key（不计入测量）
		fill(*addr, *n, *size)
	}
	if *cmdName == "EVALSHA" {
		// EVALSHA 需要 sha 已登记：先预热一条（不计入测量）
		warmEval(*addr, value)
	}

	start := time.Now()
	lat, errs := run(*addr, mk, *n, *concurrency, *pipeline)
	elapsed := time.Since(start)
	report(*cmdName, *n, *concurrency, *pipeline, lat, errs, elapsed)
	if errs > 0 {
		os.Exit(1)
	}
}

// report prints one benchmark block (throughput + latency percentiles).
func report(cmdName string, n, concurrency, pipeline int, lat []float64, errs int64, elapsed time.Duration) {
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
	fmt.Printf("====== %s ======\n", cmdName)
	fmt.Printf("  %d requests completed in %.2fs\n", n, elapsed.Seconds())
	fmt.Printf("  %d parallel clients, pipeline %d\n", concurrency, pipeline)
	fmt.Printf("  throughput: %.2f ops/s\n", float64(n)/elapsed.Seconds())
	fmt.Printf("  latency p50: %.3fms  p90: %.3fms  p99: %.3fms  max: %.3fms\n",
		pct(0.50), pct(0.90), pct(0.99), pct(1.00))
	fmt.Printf("  errors: %d\n", errs)
}

// mkCmdBuilder 返回按请求序号构造基准命令的函数。
func mkCmdBuilder(cmdName, value string) func(i int) resp.Value {
	sum := sha1.Sum([]byte(evalScript))
	sha := hex.EncodeToString(sum[:])
	switch cmdName {
	case "SET":
		return func(i int) resp.Value { return mkSet(keyName(i), value) }
	case "GET":
		return func(i int) resp.Value { return mkGet(keyName(i)) }
	case "EVAL", "EVALSHA":
		name := cmdName
		return func(i int) resp.Value {
			return mkEval(name, sha, keyName(i), value)
		}
	}
	fatal(fmt.Errorf("no builder for %s", cmdName))
	return nil
}

// warmEval 先跑一条 EVAL 让服务端登记脚本缓存（EVALSHA 基线的前提）。
func warmEval(addr, value string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fatal(err)
	}
	defer conn.Close()
	if err := resp.WriteValue(conn, mkEval("EVAL", "", keyName(-1), value)); err != nil {
		fatal(err)
	}
	v, err := resp.NewReader(conn).Read()
	if err != nil || v.Type == resp.Error {
		fatal(fmt.Errorf("warm eval: %v %v", v, err))
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
func run(addr string, mk func(i int) resp.Value, n, concurrency, pipeline int) ([]float64, int64) {
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
					if err := resp.WriteValue(buf, mk(i+j)); err != nil {
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

// runCAS measures the WATCH+MULTI+EXEC optimistic-lock round trip: each op
// is WATCH key → MULTI → SET key value → EXEC on a fresh key (always wins,
// 4 round trips; the number is the cost of a CAS cycle, not of a write).
func runCAS(addr string, n, concurrency int, value string) ([]float64, int64) {
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
			samples := make([]float64, 0, hi-lo)
			for i := lo; i < hi; i++ {
				k := keyName(i)
				t0 := time.Now()
				for _, cmd := range []resp.Value{
					mkCmdRaw("WATCH", k),
					mkCmdRaw("MULTI"),
					mkCmdRaw("SET", k, value),
					mkCmdRaw("EXEC"),
				} {
					if err := resp.WriteValue(conn, cmd); err != nil {
						fatal(err)
					}
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
				samples = append(samples,
					float64(time.Since(t0).Microseconds())/1000)
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

// mkEval 构造 EVAL/EVALSHA <script|sha> <numkeys> <key> <value>。
func mkEval(name, sha, key, value string) resp.Value {
	arg0 := sha
	if name == "EVAL" {
		arg0 = evalScript
	}
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: name},
		{Type: resp.BulkString, Str: arg0},
		{Type: resp.BulkString, Str: "1"},
		{Type: resp.BulkString, Str: key},
		{Type: resp.BulkString, Str: value},
	}}
}

// mkCmdRaw 构造任意字符串参数命令（CAS 序列用）。
func mkCmdRaw(name string, args ...string) resp.Value {
	arr := []resp.Value{{Type: resp.BulkString, Str: name}}
	for _, a := range args {
		arr = append(arr, resp.Value{Type: resp.BulkString, Str: a})
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "bench: %v\n", err)
	os.Exit(1)
}
