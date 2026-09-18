// 阻塞弹出命令（Phase 10）：BLPOP / BRPOP / BRPOPLPUSH。
//
// 语义对齐 Redis：
//   - 给定 key 顺序扫描，第一个非空 list 立即弹出回复（BLPOP/BRPOP 回
//     [key, value] 数组，BRPOPLPUSH 回弹出值本身）；
//   - 全部为空（或不存在）则阻塞：timeout 为 0 = 无限等待，支持小数秒，
//     负数报 "timeout is negative"；超时回 null（BLPOP/BRPOP 为 null array
//     *-1，BRPOPLPUSH 为 null bulk $-1）；
//   - 等待期间任一 key 被推入元素（LPUSH/RPUSH/LMOVE，含事务/主库流/Lua
//     效果帧），按 FIFO 投喂最早等待者：弹出动作发生在推送命令的 applyMu
//     临界区内，AOF/副本收到的帧序为「推送帧 + 确定性弹出帧」（LPOP/RPOP/
//     LMOVE RIGHT LEFT），重放状态严格一致——阻塞命令本身永不落盘（Redis
//     传播形态同款）；
//   - MULTI 内排队、EXEC 时非阻塞执行（有元素则弹、无元素回 null，Redis
//     同语义）；脚本内禁用（Redis 同：blocking command not allowed）；
//   - 存在 WRONGTYPE key 时立即报错不阻塞（Redis 同）。
//
// 并发模型：每 key 一条 FIFO 等待者队列（bwMu 保护）。等待者绝不持 applyMu
// 睡眠（否则推送命令会死锁）；投喂发生在推送方持有的 applyMu 临界区内
// （logAndPropagate 统一收口），加 bwMu 读取队列。锁序：applyMu → bwMu →
// watchMu。等待结束（投喂/超时/断连）三方竞态由 bwMu 串行化，先到先得。
//
// 断连检测：阻塞期间 handle goroutine 睡在 channel 上，socket 无人读——
// 探针 goroutine 以 200ms 读超时轮询连接，任何读结果（EOF/错误/意外数据，
// 正常客户端阻塞期间不发命令）即放弃等待者并关闭连接，避免「向死客户端
// 投喂元素」造成无主消费与 goroutine 滞留。探针退出前清掉自己设置的读
// deadline，把连接原样还给 handle 循环。
package server

import (
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
)

// blockWaiter 是一个阻塞中的等待者。注册于其全部目标 key 的队列；resolving
// 一方（投喂/超时/探针断连）在 bwMu 下置位标志并 close(ch)，恰好一次。
type blockWaiter struct {
	ch   chan struct{} // resolved 时 close
	keys []string      // 注册过的 key（注销用）

	key, value string // served 时由投喂方填充（key = 实际弹出的 key）
	front      bool   // true = 头部弹出（BLPOP）
	moveDst    string // BRPOPLPUSH 的目标 key（"" = 普通弹出）

	served, aborted bool
}

// isBlockingCmd reports whether cmd is a blocking pop (replica EXEC 语义分流
// 用：副本上这些命令是读，MULTI 内照常非阻塞执行而非报 READONLY）。
func isBlockingCmd(name string) bool {
	return name == "BLPOP" || name == "BRPOP" || name == "BRPOPLPUSH"
}

// mustFirstCmd 是 firstCmd 的无错误形态（非法协议形态返回 ""）。
func mustFirstCmd(v resp.Value) string {
	name, _ := firstCmd(v)
	return name
}

// parseBlockingTimeout parses the trailing timeout argument: seconds as a
// float (decimal supported), 0 = wait forever, negative = error.
func parseBlockingTimeout(arg resp.Value) (time.Duration, resp.Value) {
	tf, err := strconv.ParseFloat(arg.Str, 64)
	if err != nil || math.IsNaN(tf) || math.IsInf(tf, 0) {
		return 0, resp.Value{Type: resp.Error,
			Str: "ERR timeout is not a float or out of range"}
	}
	if tf < 0 {
		return 0, resp.Value{Type: resp.Error, Str: "ERR timeout is negative"}
	}
	if tf == 0 {
		return 0, resp.Value{} // 无限等待
	}
	return time.Duration(tf * float64(time.Second)), resp.Value{}
}

// blockingPop is the connection-level entry (applyConn, outside MULTI).
func (s *Server) blockingPop(cl *client, cmd string, args []resp.Value) resp.Value {
	var keys []string
	var moveDst string
	front := cmd != "BRPOP" && cmd != "BRPOPLPUSH" // BLPOP 头弹，其余尾弹
	if cmd == "BRPOPLPUSH" {
		if len(args) != 3 {
			return wrongArgs("brpoplpush")
		}
		keys = []string{args[0].Str}
		moveDst = args[1].Str
		args = args[2:]
	} else {
		if len(args) < 2 {
			return wrongArgs(strings.ToLower(cmd))
		}
		keys = make([]string, len(args)-1)
		for i, a := range args[:len(args)-1] {
			keys[i] = a.Str
		}
		args = args[len(args)-1:]
	}
	dur, errVal := parseBlockingTimeout(args[0])
	if errVal.Type == resp.Error {
		return errVal
	}

	// 快速路径：任一 key 可弹（或 WRONGTYPE 报错）。弹出与落盘/传播在写命令
	// 同一 applyMu 临界区内完成。
	s.applyMu.Lock()
	key, val, ok, rerr := s.blockingPopOnce(keys, front, moveDst)
	if rerr.Type == resp.Error {
		s.applyMu.Unlock()
		return rerr
	}
	if ok {
		frame := canonicalPopFrame(key, val, front, moveDst)
		s.logAndPropagate([]resp.Value{frame})
		s.touchWatched(frame, nil)
	}
	s.applyMu.Unlock()
	if ok {
		return blockingReply(key, val, moveDst)
	}

	// 阻塞路径：注册等待者（绝不持 applyMu 睡眠）。
	w := &blockWaiter{ch: make(chan struct{}), keys: keys,
		key: keys[0], front: front, moveDst: moveDst}
	s.bwMu.Lock()
	for _, k := range keys {
		s.blockQ[k] = append(s.blockQ[k], w)
	}
	s.bwMu.Unlock()

	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if dur > 0 {
		timer = time.NewTimer(dur)
		timeoutCh = timer.C
		defer timer.Stop()
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go s.probeConn(cl, w, stop, done)

	select {
	case <-w.ch: // 投喂成功 或 探针发现断连
	case <-timeoutCh:
	}
	close(stop)
	// 踢出可能仍睡在 Read 里的探针，等它退出并还清 deadline 后再继续——
	// 否则探针可能吞掉客户端阻塞结束后的下一条命令字节。
	cl.conn.SetReadDeadline(time.Now().Add(time.Millisecond))
	<-done
	cl.conn.SetReadDeadline(time.Time{})

	// 解析结果：投喂与超时竞态时 served 已置位则以投喂为准。
	s.bwMu.Lock()
	served := w.served
	if !served && !w.aborted {
		w.aborted = true
		close(w.ch)
	}
	s.removeWaiterLocked(w)
	s.bwMu.Unlock()
	if served {
		return blockingReply(w.key, w.value, moveDst)
	}
	return blockingNullReply(moveDst) // 超时；探针断连时该回复写入失败，handle 随即退出
}

// blockingPopOnce tries one non-blocking pop across keys in order (caller
// holds applyMu). A WRONGTYPE key aborts with the Redis error.
func (s *Server) blockingPopOnce(keys []string, front bool, moveDst string) (key, val string, ok bool, rerr resp.Value) {
	if moveDst != "" { // BRPOPLPUSH：单 key 尾弹 + 目标头推（store 内原子）
		v, moved, err := s.store.ListMove(keys[0], moveDst, false, true)
		if err != nil {
			return "", "", false, errReply(err)
		}
		if moved {
			return keys[0], v, true, resp.Value{}
		}
		return "", "", false, resp.Value{}
	}
	for _, k := range keys {
		popped, err := s.store.ListPop(k, front, 1)
		if err != nil {
			return "", "", false, errReply(err)
		}
		if len(popped) == 1 {
			return k, popped[0], true, resp.Value{}
		}
	}
	return "", "", false, resp.Value{}
}

// canonicalPopFrame is the deterministic AOF/replica form of a successful
// blocking pop (identical state effect, never blocking on replay).
func canonicalPopFrame(key, _ string, front bool, moveDst string) resp.Value {
	if moveDst != "" {
		return respCmd("LMOVE", key, moveDst, "RIGHT", "LEFT")
	}
	if front {
		return respCmd("LPOP", key)
	}
	return respCmd("RPOP", key)
}

// blockingReply builds the client reply: BLPOP/BRPOP → [key, value];
// BRPOPLPUSH → the popped value.
func blockingReply(key, val, moveDst string) resp.Value {
	if moveDst != "" {
		return resp.Value{Type: resp.BulkString, Str: val}
	}
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: key},
		{Type: resp.BulkString, Str: val},
	}}
}

// blockingNullReply is the timeout reply: null array for BLPOP/BRPOP,
// null bulk for BRPOPLPUSH (Redis same).
func blockingNullReply(moveDst string) resp.Value {
	if moveDst != "" {
		return resp.Value{Type: resp.BulkString, Null: true}
	}
	return resp.Value{Type: resp.Array, Null: true}
}

// cmdBlockingImmediate is the non-blocking form used by dispatch (MULTI/EXEC
// 执行与回放兜底）：有元素则弹，否则回 null——绝不等待。
func (s *Server) cmdBlockingImmediate(args []resp.Value, front bool, moveDst string) resp.Value {
	var keys []resp.Value
	if moveDst != "" {
		if len(args) != 2 {
			return wrongArgs("brpoplpush")
		}
		keys = args[:1]
	} else {
		if len(args) < 2 {
			return wrongArgs("blpop/brpop")
		}
		keys = args[:len(args)-1]
	}
	names := make([]string, len(keys))
	for i, a := range keys {
		names[i] = a.Str
	}
	key, val, ok, rerr := s.blockingPopOnce(names, front, moveDst)
	if rerr.Type == resp.Error {
		return rerr
	}
	if !ok {
		return blockingNullReply(moveDst)
	}
	return blockingReply(key, val, moveDst)
}

// serveWaitersFrames scans the committed canonical frames for list pushes
// (LPUSH/RPUSH/LMOVE) and feeds blocked waiters FIFO, popping under the same
// applyMu critical section (the pushed element cannot be consumed by anyone
// else in between). Returns the deterministic pop frames to append to the
// log/propagation batch — AOF order is push-frame then pop-frame. Called from
// logAndPropagate (all its callers hold applyMu).
func (s *Server) serveWaitersFrames(frames []resp.Value) []resp.Value {
	s.bwMu.Lock()
	defer s.bwMu.Unlock()
	if len(s.blockQ) == 0 {
		return nil
	}
	var pushed []string
	for _, f := range frames {
		name, ok := firstCmd(f)
		if !ok {
			continue
		}
		switch name {
		case "LPUSH", "RPUSH":
			if len(f.Arr) >= 2 {
				pushed = append(pushed, f.Arr[1].Str)
			}
		case "LMOVE":
			if len(f.Arr) >= 3 {
				pushed = append(pushed, f.Arr[2].Str)
			}
		}
	}
	if len(pushed) == 0 {
		return nil
	}
	var out []resp.Value
	for _, key := range pushed {
		for {
			s.compactQueueLocked(key)
			q := s.blockQ[key]
			if len(q) == 0 {
				break
			}
			w := q[0]
			var frame resp.Value
			if w.moveDst != "" {
				val, moved, err := s.store.ListMove(key, w.moveDst, false, true)
				if err != nil || !moved {
					break // 元素已消失/类型异常：保持等待，由超时或下次推送兜底
				}
				frame = respCmd("LMOVE", key, w.moveDst, "RIGHT", "LEFT")
				w.value = val
			} else {
				popped, err := s.store.ListPop(key, w.front, 1)
				if err != nil || len(popped) == 0 {
					break
				}
				if w.front {
					frame = respCmd("LPOP", key)
				} else {
					frame = respCmd("RPOP", key)
				}
				w.value = popped[0]
			}
			w.key, w.served = key, true
			close(w.ch)
			out = append(out, frame)
			// 弹出同样是 key 变更：触碰 WATCH 了该 key 的连接（乐观锁）
			s.touchWatched(frame, nil)
		}
	}
	return out
}

// compactQueueLocked drops resolved waiters from a key's queue front.
func (s *Server) compactQueueLocked(key string) {
	q := s.blockQ[key]
	i := 0
	for i < len(q) && (q[i].served || q[i].aborted) {
		i++
	}
	if i == 0 {
		return
	}
	q = q[i:]
	if len(q) == 0 {
		delete(s.blockQ, key)
	} else {
		s.blockQ[key] = q
	}
}

// removeWaiterLocked unregisters w from every queue it joined (caller holds
// bwMu).
func (s *Server) removeWaiterLocked(w *blockWaiter) {
	for _, k := range w.keys {
		q := s.blockQ[k]
		for i, x := range q {
			if x == w {
				q = append(q[:i], q[i+1:]...)
				break
			}
		}
		if len(q) == 0 {
			delete(s.blockQ, k)
		} else {
			s.blockQ[k] = q
		}
	}
}

// probeConn watches the connection while its client is blocked: any read
// result (EOF / error / unexpected data — a well-behaved client sends nothing
// while blocked) abandons the waiter and closes the connection so the blocked
// element can never be fed to a dead client. Polls with a 200ms read deadline
// so it always notices stop; on exit it clears the deadline it set, handing
// the connection back to the handle loop untouched.
func (s *Server) probeConn(cl *client, w *blockWaiter, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	buf := make([]byte, 1)
	for {
		select {
		case <-stop:
			cl.conn.SetReadDeadline(time.Time{})
			return
		default:
		}
		cl.conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		_, err := cl.conn.Read(buf)
		if err != nil && os.IsTimeout(err) {
			continue
		}
		// 数据到达 / EOF / 其他错误：对端断开或阻塞期间发来命令
		s.bwMu.Lock()
		live := !w.served && !w.aborted
		if live {
			w.aborted = true
			close(w.ch)
		}
		s.bwMu.Unlock()
		if live {
			cl.conn.Close() // handle 下一次读写失败退出，dropClient 收尾
		}
		return
	}
}
