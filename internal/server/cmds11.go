// STREAM 命令（Phase 11）：XADD / XLEN / XRANGE / XREVRANGE / XDEL /
// XTRIM / XREAD（含 BLOCK）。
//
// 语义对齐 Redis 7：
//   - XADD key [NOMKSTREAM] [MAXLEN [=|~] n | MINID [=|~] id] [LIMIT n]
//     <id|*> field value [field value ...]：修剪先于添加（Redis 6.2+）；
//     自动 ID 以显式 ID 形态落盘/传播（canonicalWrite 确定化，重启/副本
//     不漂移）；MAXLEN ~ 按精确修剪实现（诚实取舍），LIMIT 仅允许与 ~
//     同用（Redis 同报 syntax error）且对精确修剪无效果；
//   - XRANGE/XREVRANGE：-、+、( 排他、裸数字端点（end 含该毫秒全部序号）；
//     显式 0-0 端点报 Redis 同文错误；start > end 回空数组；
//   - XDEL/XTRIM 清空流即删除 key（Redis 7 不保留空流）；
//   - XREAD [COUNT n] [BLOCK ms] STREAMS key id ...：给定 ID 为排他下界
//     （返回严格大于该 ID 的条目），"$" = 当前最后 ID；BLOCK 0 = 无限等待。
//
// 阻塞 XREAD 与 BLPOP 族同款并发模型：applyMu 临界区内完成「快路径检查 +
// $ 快照 + 注册」，等待者绝不持 applyMu 睡眠；唤醒发生在 XADD 提交的
// applyMu 临界区内（logAndPropagate → serveStreamWaiters），构建纯读快照
// 回复（流读非消费型，不产生确定性帧、不落盘；同一批新条目可唤醒全部等
// 待者）。断连探针 probeConnStream 镜像 Phase 10 probeConn（xwMu 版本），
// 死客户端绝不被投喂。锁序：applyMu → xwMu（→ store.mu）。MULTI 内
// XREAD 一律非阻塞执行（有数据回数据、无数据回 null，Redis 同语义）；
// 脚本内禁用带 BLOCK 的 XREAD（见 eval.go callFunc）。
package server

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// errXReadID 是 XREAD id 参数解析失败错误（与 errStreamIDArg 同文，error
// 形态供 xreadFetch 返回）。
var errXReadID = errors.New("ERR Invalid stream ID specified as stream command argument")

// errStreamIDArg 是 XADD/XDEL 的非法 ID 参数错误（Redis 同文）。
func errStreamIDArg() resp.Value {
	return resp.Value{Type: resp.Error, Str: errXReadID.Error()}
}

// streamRow 构建一个 [stream-name, [[id, [f,v,...]], ...]] 行（XREAD 回复行）。
func streamRow(name string, entries []store.StreamEntry) resp.Value {
	arr := make([]resp.Value, 0, len(entries))
	for _, en := range entries {
		arr = append(arr, streamEntryReply(en))
	}
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: name},
		{Type: resp.Array, Arr: arr},
	}}
}

// streamEntryReply 构建一个 [id, [f,v,...]] 条目行。
func streamEntryReply(en store.StreamEntry) resp.Value {
	pairs := make([]resp.Value, len(en.Fields))
	for i, f := range en.Fields {
		pairs[i] = resp.Value{Type: resp.BulkString, Str: f}
	}
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: en.ID.String()},
		{Type: resp.Array, Arr: pairs},
	}}
}

// streamEntriesReply 把条目列表变为 [[id, [f,v,...]], ...]（XRANGE 族回复）。
func streamEntriesReply(entries []store.StreamEntry) resp.Value {
	arr := make([]resp.Value, 0, len(entries))
	for _, en := range entries {
		arr = append(arr, streamEntryReply(en))
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

// xaddOpts 是 XADD 解析后的选项（xaddParse 产出；canonicalWrite 复用以
// 定位 ID 参数、把自动 ID 替换为解析后的显式 ID）。
type xaddOpts struct {
	key     string
	mk      bool // false = NOMKSTREAM
	maxLen  int64
	minID   *store.StreamID
	hasTrim bool
	rest    []resp.Value // <id|*> field value ...
}

// xaddParse 解析 XADD 参数（v.Arr[1:]）：选项区在 ID 之前，遇到首个非选项
// 参数即视为 ID 起点。错误返回 Redis 同文 resp 错误。
func xaddParse(args []resp.Value) (xaddOpts, resp.Value) {
	var o xaddOpts
	o.key, o.mk, o.maxLen = args[0].Str, true, -1
	lastApprox := false
	i := 1
	for i < len(args) {
		switch strings.ToUpper(args[i].Str) {
		case "NOMKSTREAM":
			o.mk = false
			i++
		case "MAXLEN", "MINID":
			if o.hasTrim {
				return o, syntaxErr() // MAXLEN/MINID 互斥（Redis 同）
			}
			approx := false
			j := i + 1
			if j < len(args) && (args[j].Str == "=" || args[j].Str == "~") {
				approx = args[j].Str == "~"
				j++
			}
			if j >= len(args) {
				return o, syntaxErr()
			}
			if strings.ToUpper(args[i].Str) == "MAXLEN" {
				n, ok := parseIntArg(args[j].Str, "xadd")
				if !ok {
					return o, notIntegerErr()
				}
				if n < 0 {
					return o, resp.Value{Type: resp.Error,
						Str: "ERR The MAXLEN argument must be >= 0"}
				}
				o.maxLen = n
			} else {
				id, ok := store.ParseStreamID(args[j].Str)
				if !ok {
					return o, errStreamIDArg()
				}
				o.minID = &id
			}
			o.hasTrim, lastApprox = true, approx
			i = j + 1
		case "LIMIT":
			// LIMIT 仅允许配合 ~（Redis 同 syntax error）；精确修剪下无效果
			if !lastApprox || i+1 >= len(args) {
				return o, syntaxErr()
			}
			if _, ok := parseIntArg(args[i+1].Str, "xadd"); !ok {
				return o, notIntegerErr()
			}
			i += 2
		default:
			o.rest = args[i:]
			return o, resp.Value{}
		}
	}
	return o, resp.Value{} // 只剩选项没有 id/字段：由调用方按 arity 报错
}

// cmdXAdd handles XADD. 回复为解析后的 ID bulk；NOMKSTREAM 且 key 不存在
// 时回复 null bulk。
func (s *Server) cmdXAdd(args []resp.Value) resp.Value {
	if len(args) < 4 {
		return wrongArgs("xadd")
	}
	o, errv := xaddParse(args)
	if errv.Type == resp.Error {
		return errv
	}
	if len(o.rest) < 3 || len(o.rest)%2 == 0 {
		return wrongArgs("xadd") // <id> + 偶数对 field/value
	}
	auto := o.rest[0].Str == "*"
	var id store.StreamID
	if !auto {
		var ok bool
		if id, ok = store.ParseStreamID(o.rest[0].Str); !ok {
			return errStreamIDArg()
		}
	}
	fields := make([]string, 0, len(o.rest)-1)
	for _, a := range o.rest[1:] {
		fields = append(fields, a.Str)
	}
	// 修剪先于添加（Redis 6.2+：MAXLEN 1 时流中永远只有最新一条）
	if o.hasTrim {
		if _, err := s.store.StreamTrim(o.key, o.maxLen, o.minID); err != nil {
			return errReply(err)
		}
	}
	got, added, err := s.store.StreamAdd(o.key, auto, id, fields, o.mk)
	if err != nil {
		return errReply(err)
	}
	if !added {
		return resp.Value{Type: resp.BulkString, Null: true}
	}
	return resp.Value{Type: resp.BulkString, Str: got.String()}
}

func (s *Server) cmdXLen(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return wrongArgs("xlen")
	}
	n, err := s.store.StreamLen(args[0].Str)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdXRange handles XRANGE/XREVRANGE key start end。XREVRANGE 的参数序为
// end first（Redis 同）：rev=true 时交换解析目标，无需额外归一。
func (s *Server) cmdXRange(args []resp.Value, rev bool) resp.Value {
	if len(args) != 3 && len(args) != 5 {
		return wrongArgs(strings.ToLower("xrange"))
	}
	loArg, hiArg := args[1].Str, args[2].Str
	if rev {
		loArg, hiArg = args[2].Str, args[1].Str
	}
	count := int64(-1)
	if len(args) == 5 {
		if !strings.EqualFold(args[3].Str, "COUNT") {
			return syntaxErr()
		}
		n, ok := parseIntArg(args[4].Str, "xrange")
		if !ok {
			return notIntegerErr()
		}
		if n <= 0 {
			return resp.Value{Type: resp.Error,
				Str: "ERR value is out of range, must be positive"}
		}
		count = n
	}
	minB, err := store.ParseStreamBound(loArg, true)
	if err != nil {
		return errReply(err)
	}
	maxB, err := store.ParseStreamBound(hiArg, false)
	if err != nil {
		return errReply(err)
	}
	// 显式 0-0 端点（非 -/+、非 "(" 排他）报 Redis 同文错误
	if minB.Inf == 0 && !minB.Ex && minB.ID.IsZero() {
		return resp.Value{Type: resp.Error, Str: "ERR start ID must be greater than 0-0"}
	}
	if maxB.Inf == 0 && !maxB.Ex && maxB.ID.IsZero() {
		return resp.Value{Type: resp.Error, Str: "ERR end ID must be greater than 0-0"}
	}
	entries, err := s.store.StreamRange(args[0].Str, minB, maxB, rev, count)
	if err != nil {
		return errReply(err)
	}
	return streamEntriesReply(entries)
}

func (s *Server) cmdXDel(args []resp.Value) resp.Value {
	ids := make([]store.StreamID, 0, len(args)-1)
	for _, a := range args[1:] {
		id, ok := store.ParseStreamID(a.Str)
		if !ok {
			return errStreamIDArg()
		}
		ids = append(ids, id)
	}
	n, err := s.store.StreamDel(args[0].Str, ids...)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdXTrim handles XTRIM key MAXLEN|MINID [=|~] val [LIMIT n]。LIMIT 同
// XADD：精确修剪下接受但无效果（Redis 中它只对 ~ 有意义）。
func (s *Server) cmdXTrim(args []resp.Value) resp.Value {
	if len(args) < 3 {
		return wrongArgs("xtrim")
	}
	var maxLen int64 = -1
	var minID *store.StreamID
	switch strings.ToUpper(args[1].Str) {
	case "MAXLEN", "MINID":
		isMax := strings.ToUpper(args[1].Str) == "MAXLEN"
		i := 2
		if i < len(args) && (args[i].Str == "=" || args[i].Str == "~") {
			i++
		}
		if i >= len(args) {
			return syntaxErr()
		}
		if isMax {
			n, ok := parseIntArg(args[i].Str, "xtrim")
			if !ok {
				return notIntegerErr()
			}
			if n < 0 {
				return resp.Value{Type: resp.Error,
					Str: "ERR The MAXLEN argument must be >= 0"}
			}
			maxLen = n
		} else {
			id, ok := store.ParseStreamID(args[i].Str)
			if !ok {
				return errStreamIDArg()
			}
			minID = &id
		}
		i++
		for i < len(args) {
			if !strings.EqualFold(args[i].Str, "LIMIT") || i+1 >= len(args) {
				return syntaxErr()
			}
			if _, ok := parseIntArg(args[i+1].Str, "xtrim"); !ok {
				return notIntegerErr()
			}
			i += 2
		}
	default:
		return syntaxErr()
	}
	n, err := s.store.StreamTrim(args[0].Str, maxLen, minID)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// -------- XREAD --------

// xreadOpts 是 XREAD 解析结果。block: -1 = 未指定（纯读），0 = 无限，>0 = 毫秒。
type xreadOpts struct {
	count int64 // <=0 = 不限
	block int64
	keys  []string
	ids   []string // 原始 id 参数（"$" 或显式）
}

func errUnbalancedXRead() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR Unbalanced 'xread' list of streams: " +
		"for each stream key an ID or '$' must be specified."}
}

// parseXRead 解析 XREAD [COUNT n] [BLOCK ms] STREAMS key id [key id ...]。
func parseXRead(args []resp.Value) (xreadOpts, resp.Value) {
	var o xreadOpts
	o.block = -1
	i := 0
	for i < len(args) {
		switch strings.ToUpper(args[i].Str) {
		case "COUNT":
			if i+1 >= len(args) {
				return o, syntaxErr()
			}
			n, ok := parseIntArg(args[i+1].Str, "xread")
			if !ok {
				return o, notIntegerErr()
			}
			if n <= 0 {
				return o, resp.Value{Type: resp.Error, Str: "ERR COUNT must be > 0"}
			}
			o.count = n
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return o, syntaxErr()
			}
			n, ok := parseIntArg(args[i+1].Str, "xread")
			if !ok {
				return o, resp.Value{Type: resp.Error,
					Str: "ERR timeout is not an integer or out of range"}
			}
			if n < 0 {
				return o, resp.Value{Type: resp.Error, Str: "ERR timeout is negative"}
			}
			o.block = n
			i += 2
		case "STREAMS":
			rest := args[i+1:]
			if len(rest) < 2 || len(rest)%2 != 0 {
				return o, errUnbalancedXRead()
			}
			half := len(rest) / 2
			o.keys = make([]string, half)
			o.ids = make([]string, half)
			for j := 0; j < half; j++ {
				o.keys[j] = rest[j].Str
				o.ids[j] = rest[half+j].Str
			}
			return o, resp.Value{}
		default:
			return o, errUnbalancedXRead()
		}
	}
	return o, errUnbalancedXRead() // 缺 STREAMS
}

// resolveXReadID 把一个 id 参数解析为 StreamID："$" 取流的 last（缺失为
// 0-0）；显式 ID 原样。WRONGTYPE 流在此即报错（Redis 同）。
func (s *Server) resolveXReadID(key, idArg string) (store.StreamID, error) {
	if idArg == "$" {
		return s.store.StreamLastID(key)
	}
	id, ok := store.ParseStreamID(idArg)
	if !ok {
		return store.StreamID{}, errXReadID
	}
	return id, nil
}

// xreadFetch 非阻塞执行一次 XREAD 读：返回 (reply, any, err)。any=false 时
// reply 为 null array。key 顺序即请求顺序；只含有数据的流。
func (s *Server) xreadFetch(o xreadOpts) (resp.Value, bool, error) {
	rows := make([]resp.Value, 0, len(o.keys))
	for i, k := range o.keys {
		last, err := s.resolveXReadID(k, o.ids[i])
		if err != nil {
			return resp.Value{}, false, err
		}
		entries, exists, err := s.store.StreamRead(k, last, o.count)
		if err != nil {
			return resp.Value{}, false, err
		}
		if !exists || len(entries) == 0 {
			continue
		}
		rows = append(rows, streamRow(k, entries))
	}
	if len(rows) == 0 {
		return resp.Value{Type: resp.Array, Null: true}, false, nil
	}
	return resp.Value{Type: resp.Array, Arr: rows}, true, nil
}

// cmdXRead 是 dispatch 层的非阻塞形态（MULTI/EXEC 执行、回放兜底）：指定
// BLOCK 也按非阻塞执行（Redis 同语义：事务内不阻塞）。
func (s *Server) cmdXRead(args []resp.Value) resp.Value {
	o, errv := parseXRead(args)
	if errv.Type == resp.Error {
		return errv
	}
	reply, _, err := s.xreadFetch(o)
	if err != nil {
		return errReply(err)
	}
	return reply
}

// cmdXReadConn 是连接层入口（applyConn，MULTI 之外）：未指定 BLOCK 走纯读，
// 指定 BLOCK（含 0=无限）走阻塞路径。
func (s *Server) cmdXReadConn(cl *client, args []resp.Value) resp.Value {
	o, errv := parseXRead(args)
	if errv.Type == resp.Error {
		return errv
	}
	if o.block < 0 {
		reply, _, err := s.xreadFetch(o)
		if err != nil {
			return errReply(err)
		}
		return reply
	}
	return s.cmdXReadBlocking(cl, o)
}

// streamWaiter 是一个阻塞中的流等待者（xwMu 串行化 resolved 竞态）。
// grp 非 nil 时为 Phase 12 的 XREADGROUP '>' 等待者（无需 ids 快照：新条目
// 即 > 组位点，投喂由组状态自持）。
type streamWaiter struct {
	ch   chan struct{} // resolved 时 close
	keys []string      // 注册过的 key（去重，注销用）
	ids  map[string]store.StreamID
	o    xreadOpts
	grp  *xreadGroupOpts // 非 nil = XREADGROUP 等待者

	served, aborted bool
	reply           resp.Value // served 时由投喂方填充
}

// cmdXReadBlocking 是 XREAD ... BLOCK ms 的连接级路径。与 BLPOP 同款：
// applyMu 临界区内「快路径检查 + $ 快照 + 注册」（消除错过投喂的空窗），
// 之后绝不持 applyMu 睡眠。
func (s *Server) cmdXReadBlocking(cl *client, o xreadOpts) resp.Value {
	s.applyMu.Lock()
	reply, any, rerr := s.xreadFetch(o)
	if rerr != nil {
		s.applyMu.Unlock()
		return errReply(rerr)
	}
	if any {
		s.applyMu.Unlock()
		return reply
	}
	// 快照各流 ID（"$" 在此刻定格；注册与唤醒同持 applyMu 序，无空窗）
	ids := make(map[string]store.StreamID, len(o.keys))
	for i, k := range o.keys {
		last, err := s.resolveXReadID(k, o.ids[i])
		if err != nil {
			s.applyMu.Unlock()
			return errReply(err)
		}
		ids[k] = last
	}
	w := &streamWaiter{ch: make(chan struct{}), ids: ids, o: o,
		keys: dedupStrings(o.keys)}
	s.xwMu.Lock()
	for _, k := range w.keys {
		s.xblockQ[k] = append(s.xblockQ[k], w)
	}
	s.xwMu.Unlock()
	s.applyMu.Unlock()

	var timer *time.Timer
	var timeoutCh <-chan time.Time
	if o.block > 0 {
		timer = time.NewTimer(time.Duration(o.block) * time.Millisecond)
		timeoutCh = timer.C
		defer timer.Stop()
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go s.probeConnStream(cl, w, stop, done)

	select {
	case <-w.ch:
	case <-timeoutCh:
	}
	close(stop)
	// 踢出可能仍睡在 Read 里的探针（Phase 10 同款），还清 deadline 再继续
	cl.conn.SetReadDeadline(time.Now().Add(time.Millisecond))
	<-done
	cl.conn.SetReadDeadline(time.Time{})

	s.xwMu.Lock()
	served := w.served
	if !served && !w.aborted {
		w.aborted = true
		close(w.ch)
	}
	s.removeStreamWaiterLocked(w)
	s.xwMu.Unlock()
	if served {
		return w.reply
	}
	return resp.Value{Type: resp.Array, Null: true} // 超时（探针断连时写入失败即退出）
}

// serveStreamWaiters 扫描已提交的 canonical 帧中的 XADD，唤醒等待这些流的
// 等待者。XREAD 等待者（grp==nil）是纯读快照：同一批新条目喂给所有等待者，
// 每人独立构建全量快照回复，不产生帧。XREADGROUP '>' 等待者（grp!=nil）是
// 单消费投喂：同组先到先得（先投喂者拿走新条目并推进 lastDelivered，后者
// 保持阻塞），不同组各有独立位点；投喂产生的 XCLAIM FORCE JUSTID 帧返回给
// 调用方并入本批帧落盘/传播。由 logAndPropagate 调用（applyMu 内）。
func (s *Server) serveStreamWaiters(frames []resp.Value) []resp.Value {
	s.xwMu.Lock()
	defer s.xwMu.Unlock()
	if len(s.xblockQ) == 0 {
		return nil
	}
	var pushed []string
	for _, f := range frames {
		if name, ok := firstCmd(f); !ok || name != "XADD" || len(f.Arr) < 2 {
			continue
		}
		k := f.Arr[1].Str
		if !containsString(pushed, k) {
			pushed = append(pushed, k)
		}
	}
	var out []resp.Value
	for _, key := range pushed {
		for _, w := range s.xblockQ[key] {
			if w.served || w.aborted {
				continue
			}
			if w.grp != nil {
				out = append(out, s.feedStreamGroupWaiterLocked(w)...)
				continue
			}
			fresh, _, err := s.store.StreamRead(key, w.ids[key], 1)
			if err != nil || len(fresh) == 0 {
				continue // 该流对此等待者暂无新数据
			}
			// 构建全量快照回复：请求的每个流各自取新数据（count 生效）
			rows := make([]resp.Value, 0, len(w.o.keys))
			for _, k := range w.o.keys {
				entries, exists, err := s.store.StreamRead(k, w.ids[k], w.o.count)
				if err != nil || !exists || len(entries) == 0 {
					continue
				}
				rows = append(rows, streamRow(k, entries))
			}
			if len(rows) == 0 {
				continue
			}
			w.reply = resp.Value{Type: resp.Array, Arr: rows}
			w.served = true
			close(w.ch)
		}
		s.compactStreamQueueLocked(key)
	}
	return out
}

// feedStreamGroupWaiterLocked 尝试为 XREADGROUP '>' 等待者投喂（caller
// holds xwMu，且处于 XADD 提交的 applyMu 临界区内）：按请求序对各 key 执行
// '>' 取数（首 key 有数据即成行，全部空则保持阻塞）；store 错误（如组被
// DESTROY）以错误回复终结等待。非 NOACK 投喂逐流帧化 XCLAIM（读回精确
// PEL 状态）并触碰 WATCH。
func (s *Server) feedStreamGroupWaiterLocked(w *streamWaiter) []resp.Value {
	o := w.grp
	rows := make([]resp.Value, 0, len(o.keys))
	type fedStream struct {
		key string
		ids []store.StreamID
	}
	var fed []fedStream
	for _, k := range o.keys {
		entries, exists, err := s.store.StreamReadGroupNew(k, o.group, o.consumer, o.count, o.noack)
		if err != nil {
			// 组被销毁/类型变化：错误回复终结等待（Redis 语义近似）
			w.reply = errReply(err)
			w.served = true
			close(w.ch)
			return nil
		}
		if !exists || len(entries) == 0 {
			continue
		}
		rows = append(rows, streamRow(k, entries))
		fs := fedStream{key: k, ids: make([]store.StreamID, 0, len(entries))}
		for _, en := range entries {
			fs.ids = append(fs.ids, en.ID)
		}
		fed = append(fed, fs)
	}
	if len(rows) == 0 {
		return nil // 无新数据：保持阻塞，下次 XADD 再唤醒
	}
	w.reply = resp.Value{Type: resp.Array, Arr: rows}
	w.served = true
	close(w.ch)
	// 非 NOACK：逐流 XCLAIM 帧；NOACK：无 PEL 变化（读回为空，XCLAIM 自然
	// 缺席）——但 '>' 位点推进对两者都必须以 XGROUP SETID 帧落盘/传播。
	var out []resp.Value
	for _, fs := range fed {
		frames := s.xreadGroupFrames(fs.key, o.group, o.consumer, fs.ids, true)
		out = append(out, frames...)
	}
	for _, f := range out {
		s.touchWatched(f, nil)
	}
	return out
}

// compactStreamQueueLocked 清掉已解决的等待者（caller holds xwMu）。
func (s *Server) compactStreamQueueLocked(key string) {
	q := s.xblockQ[key]
	i := 0
	for i < len(q) && (q[i].served || q[i].aborted) {
		i++
	}
	if i == 0 {
		return
	}
	q = q[i:]
	if len(q) == 0 {
		delete(s.xblockQ, key)
	} else {
		s.xblockQ[key] = q
	}
}

// removeStreamWaiterLocked 把 w 从它注册的所有队列注销（caller holds xwMu）。
func (s *Server) removeStreamWaiterLocked(w *streamWaiter) {
	for _, k := range w.keys {
		q := s.xblockQ[k]
		for i, x := range q {
			if x == w {
				q = append(q[:i], q[i+1:]...)
				break
			}
		}
		if len(q) == 0 {
			delete(s.xblockQ, k)
		} else {
			s.xblockQ[k] = q
		}
	}
}

// probeConnStream 是 XREAD BLOCK 的断连探针（Phase 10 probeConn 的 xwMu
// 版本）：阻塞期间 handle goroutine 睡在 channel 上，以 200ms 读超时轮询
// 连接；任何读结果（EOF/错误/意外数据）即放弃等待者并关闭连接，死客户端
// 永不被投喂。退出前还清读 deadline。
func (s *Server) probeConnStream(cl *client, w *streamWaiter, stop <-chan struct{}, done chan<- struct{}) {
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
		s.xwMu.Lock()
		live := !w.served && !w.aborted
		if live {
			w.aborted = true
			close(w.ch)
		}
		s.xwMu.Unlock()
		if live {
			cl.conn.Close() // handle 下一次读写失败退出，dropClient 收尾
		}
		return
	}
}

// -------- 小工具 --------

// xreadHasBlock 报告 XREAD 参数是否带 BLOCK 选项（eval 脚本禁用判定）。
func xreadHasBlock(args []string) bool {
	for _, a := range args {
		if strings.EqualFold(a, "BLOCK") {
			return true
		}
	}
	return false
}

// dedupStrings 保序去重（等待者注册用）。
func dedupStrings(xs []string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		if !containsString(out, x) {
			out = append(out, x)
		}
	}
	return out
}

func containsString(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
