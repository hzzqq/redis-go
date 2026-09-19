// STREAM 消费者组命令（Phase 12）：XGROUP / XREADGROUP / XACK / XPENDING /
// XCLAIM。
//
// 语义对齐 Redis 7（存储层细节见 store/streamgroup.go 文件头）：
//   - XGROUP CREATE key group [id|$] [MKSTREAM] / SETID / DESTROY /
//     CREATECONSUMER / DELCONSUMER：全为写命令，原样落盘/传播（确定性）；
//   - XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS key id ...：
//     '>' 服务未投递新条目（推进 lastDelivered、非 NOACK 时入 PEL），显式
//     ID 服务该消费者 PEL 历史（count++ 刷新投递时间）。XREADGROUP 本身
//     永不落盘/传播（Redis 同：读命令），其 PEL 效果由 canonicalFor 以
//     「XCLAIM key group consumer 0 <id> TIME <ms> RETRYCOUNT <n> FORCE
//     JUSTID」逐条帧化（Redis streamPropagateXCLAIM 同款）——FORCE 保证
//     重放侧从无到有建 PEL，JUSTID+RETRYCOUNT/TIME 让计数与投递时间精确
//     收敛；NOACK 投喂无 PEL 变化，自然无帧（读回 StreamGroupPel 为空）。
//     BLOCK 仅允许配合 '>'（历史读永不阻塞，Redis 同文报错）；
//   - XACK：写命令原样落盘；组/key 不存在回 0 不报错（Redis 同）；
//   - XPENDING：summary（count/min/max/每消费者）与 detail（端点支持
//   - / + / ( 排他）；
//   - XCLAIM：写命令原样落盘；min-idle 门 + FORCE/JUSTID/IDLE/TIME/
//     RETRYCOUNT。
//
// 阻塞 XREADGROUP（'>' + BLOCK）复用 Phase 11 流阻塞基础设施：applyMu 临
// 界区内「快路径投喂 + 注册」，等待者不持锁睡眠；唤醒挂在 XADD 提交路径
// （logAndPropagate → serveStreamWaiters），同组多等待者 FIFO 先到先得
// （'>' 单消费：先投喂者拿走新条目并推进 lastDelivered，后者保持阻塞），
// 不同组各有独立位点互不干扰。投喂产生的 XCLAIM 帧并入本批帧一并落盘/
// 传播。锁序：applyMu → xwMu（→ watchMu）。
//
// MULTI 内 XREADGROUP 非阻塞执行（'>' 无数据回 null 槽位，Redis 同）；
// 副本上按读放行（连接级与 MULTI 两处豁免 READONLY 门）；脚本内禁用
// （eval.go scriptBlocklist——脚本效果复制与组投喂帧的交互未验证，诚实
// 收窄）。XAUTOCLAIM / XINFO 留后续阶段。
package server

import (
	"strconv"
	"strings"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// errGroupRequired 是 XREADGROUP 缺 GROUP 选项的 Redis 同文错误。
func errGroupRequired() resp.Value {
	return resp.Value{Type: resp.Error,
		Str: "ERR The GROUP subcommand is required to define the consumer group and consumer name"}
}

// errUnbalancedXReadGroup 是 XREADGROUP STREAMS 列表不平衡错误（对齐
// Redis 的 xreadgroup 措辞）。
func errUnbalancedXReadGroup() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR Unbalanced 'xreadgroup' list of streams: " +
		"for each stream key an ID or '$' must be specified."}
}

// errBlockOnlyForGreater 是 BLOCK 配合 history ID 的错误（Redis 同文）。
func errBlockOnlyForGreater() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR The BLOCK option is only allowed for >"}
}

// -------- XGROUP --------

// resolveGroupID 解析组定位 ID："$" = 流 last（缺失 0-0；WRONGTYPE 流在此
// 报错），显式 ID 原样解析。
func (s *Server) resolveGroupID(key, idStr string) (store.StreamID, error) {
	if idStr == "$" {
		return s.store.StreamLastID(key)
	}
	id, ok := store.ParseStreamID(idStr)
	if !ok {
		return store.StreamID{}, errXReadID
	}
	return id, nil
}

// cmdXGroup 分发 XGROUP 子命令（CREATE/SETID/DESTROY/CREATECONSUMER/
// DELCONSUMER）。全部为确定性写命令：经 writeCmds 原样落盘/传播。
func (s *Server) cmdXGroup(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return wrongArgs("xgroup")
	}
	switch strings.ToUpper(args[0].Str) {
	case "CREATE": // key group [id|$] [MKSTREAM]
		if len(args) < 3 || len(args) > 5 {
			return wrongArgs("xgroup create")
		}
		key, group := args[1].Str, args[2].Str
		idStr, mk, seen := "$", false, 0
		for _, a := range args[3:] {
			if strings.EqualFold(a.Str, "MKSTREAM") {
				if mk {
					return syntaxErr()
				}
				mk = true
				continue
			}
			if seen > 0 {
				return syntaxErr()
			}
			idStr, seen = a.Str, seen+1
		}
		id, err := s.resolveGroupID(key, idStr)
		if err != nil {
			return errReply(err)
		}
		if err := s.store.StreamGroupCreate(key, group, id, mk); err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "SETID": // key group id|$
		if len(args) != 4 {
			return wrongArgs("xgroup setid")
		}
		id, err := s.resolveGroupID(args[1].Str, args[3].Str)
		if err != nil {
			return errReply(err)
		}
		if err := s.store.StreamGroupSetID(args[1].Str, args[2].Str, id); err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "DESTROY": // key group
		if len(args) != 3 {
			return wrongArgs("xgroup destroy")
		}
		n, err := s.store.StreamGroupDestroy(args[1].Str, args[2].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "CREATECONSUMER", "DELCONSUMER": // key group consumer
		if len(args) != 4 {
			return wrongArgs("xgroup " + strings.ToLower(args[0].Str))
		}
		if strings.ToUpper(args[0].Str) == "CREATECONSUMER" {
			n, err := s.store.StreamGroupCreateConsumer(args[1].Str, args[2].Str, args[3].Str)
			if err != nil {
				return errReply(err)
			}
			return resp.Value{Type: resp.Integer, Num: n}
		}
		n, err := s.store.StreamGroupDelConsumer(args[1].Str, args[2].Str, args[3].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	default:
		return syntaxErr()
	}
}

// -------- XREADGROUP --------

// xreadGroupOpts 是 XREADGROUP 解析结果。block: -1 = 未指定，0 = 无限，>0 = 毫秒。
type xreadGroupOpts struct {
	group, consumer string
	hasGroup        bool
	count           int64 // <=0 = 不限
	block           int64
	noack           bool
	keys            []string
	ids             []string // ">" 或显式 ID（history 模式）
}

// parseXReadGroup 解析 XREADGROUP [GROUP g c] [COUNT n] [BLOCK ms] [NOACK]
// STREAMS key id [key id ...]。
func parseXReadGroup(args []resp.Value) (xreadGroupOpts, resp.Value) {
	var o xreadGroupOpts
	o.block = -1
	i := 0
	for i < len(args) {
		switch strings.ToUpper(args[i].Str) {
		case "GROUP":
			if i+2 >= len(args) {
				return o, syntaxErr()
			}
			o.group, o.consumer, o.hasGroup = args[i+1].Str, args[i+2].Str, true
			i += 3
		case "COUNT":
			if i+1 >= len(args) {
				return o, syntaxErr()
			}
			n, ok := parseIntArg(args[i+1].Str, "xreadgroup")
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
			n, ok := parseIntArg(args[i+1].Str, "xreadgroup")
			if !ok {
				return o, resp.Value{Type: resp.Error,
					Str: "ERR timeout is not an integer or out of range"}
			}
			if n < 0 {
				return o, resp.Value{Type: resp.Error, Str: "ERR timeout is negative"}
			}
			o.block = n
			i += 2
		case "NOACK":
			o.noack = true
			i++
		case "STREAMS":
			rest := args[i+1:]
			if len(rest) < 2 || len(rest)%2 != 0 {
				return o, errUnbalancedXReadGroup()
			}
			half := len(rest) / 2
			o.keys = make([]string, half)
			o.ids = make([]string, half)
			for j := 0; j < half; j++ {
				o.keys[j] = rest[j].Str
				o.ids[j] = rest[half+j].Str
			}
			if !o.hasGroup {
				return o, errGroupRequired()
			}
			return o, resp.Value{}
		default:
			return o, errUnbalancedXReadGroup()
		}
	}
	return o, errUnbalancedXReadGroup() // 缺 STREAMS
}

// xreadGroupFetch 非阻塞执行一次 XREADGROUP 读（caller holds applyMu 或在
// 事务/回放临界区内）：'>' 走 StreamReadGroupNew（投喂+PEL），显式 ID 走
// history。只含有数据的流；无数据回 null array。
func (s *Server) xreadGroupFetch(o xreadGroupOpts) (resp.Value, bool, error) {
	rows := make([]resp.Value, 0, len(o.keys))
	for i, k := range o.keys {
		if o.ids[i] == ">" {
			entries, exists, err := s.store.StreamReadGroupNew(k, o.group, o.consumer, o.count, o.noack)
			if err != nil {
				return resp.Value{}, false, err
			}
			if !exists || len(entries) == 0 {
				continue
			}
			rows = append(rows, streamRow(k, entries))
			continue
		}
		id, ok := store.ParseStreamID(o.ids[i])
		if !ok {
			return resp.Value{}, false, errXReadID
		}
		entries, err := s.store.StreamReadGroupHistory(k, o.group, o.consumer, id, o.count)
		if err != nil {
			return resp.Value{}, false, err
		}
		if len(entries) == 0 {
			continue
		}
		rows = append(rows, streamRow(k, entries))
	}
	if len(rows) == 0 {
		return resp.Value{Type: resp.Array, Null: true}, false, nil
	}
	return resp.Value{Type: resp.Array, Arr: rows}, true, nil
}

// cmdXReadGroupImmediate 是 dispatch 层非阻塞形态（MULTI/EXEC 执行、回放
// 兜底、apply 写路径执行体）：指定 BLOCK 也按非阻塞执行（Redis 事务同语义）。
// 帧化由调用方 canonicalFor 完成，此处只执行并回复。
func (s *Server) cmdXReadGroupImmediate(args []resp.Value) resp.Value {
	o, errv := parseXReadGroup(args)
	if errv.Type == resp.Error {
		return errv
	}
	reply, _, err := s.xreadGroupFetch(o)
	if err != nil {
		return errReply(err)
	}
	return reply
}

// cmdXReadGroupConn 是连接层入口（applyConn，MULTI 之外）：未指定 BLOCK 走
// apply 写路径（applyMu + canonical 帧化 + 传播全自动），指定 BLOCK（含 0=
// 无限）走阻塞路径。BLOCK 仅允许 '>'（history 永不阻塞）。
func (s *Server) cmdXReadGroupConn(cl *client, v resp.Value) resp.Value {
	o, errv := parseXReadGroup(v.Arr[1:])
	if errv.Type == resp.Error {
		return errv
	}
	if o.block < 0 {
		return s.apply(v)
	}
	for _, id := range o.ids {
		if id != ">" {
			return errBlockOnlyForGreater()
		}
	}
	return s.cmdXReadGroupBlocking(cl, v, o)
}

// cmdXReadGroupBlocking 是 XREADGROUP ... BLOCK 的连接级路径。与 BLPOP/
// XREAD BLOCK 同款：applyMu 临界区内「快路径投喂 + 注册」消除空窗，之后
// 绝不持 applyMu 睡眠。
func (s *Server) cmdXReadGroupBlocking(cl *client, v resp.Value, o xreadGroupOpts) resp.Value {
	s.applyMu.Lock()
	reply, any, rerr := s.xreadGroupFetch(o)
	if rerr != nil {
		s.applyMu.Unlock()
		return errReply(rerr)
	}
	if any {
		// 投喂成功：PEL 效果帧化（读回精确状态）+ 落盘/传播 + 触碰 WATCH
		frames, _ := s.canonicalFor(v, reply, false)
		s.logAndPropagate(frames)
		s.touchWatched(v, nil)
		s.applyMu.Unlock()
		return reply
	}
	w := &streamWaiter{ch: make(chan struct{}), keys: dedupStrings(o.keys), grp: &o}
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
	// 踢出可能仍睡在 Read 里的探针（Phase 10/11 同款），还清 deadline
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
	return resp.Value{Type: resp.Array, Null: true} // 超时（断连时该回复写入失败即退出）
}

// -------- XACK / XPENDING / XCLAIM --------

// cmdXack 处理 XACK key group id [id ...]（写命令，原样落盘）。
func (s *Server) cmdXack(args []resp.Value) resp.Value {
	if len(args) < 3 {
		return wrongArgs("xack")
	}
	ids := make([]store.StreamID, 0, len(args)-2)
	for _, a := range args[2:] {
		id, ok := store.ParseStreamID(a.Str)
		if !ok {
			return errStreamIDArg()
		}
		ids = append(ids, id)
	}
	n, err := s.store.StreamXack(args[0].Str, args[1].Str, ids...)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdXPending 处理 XPENDING：2 参数 summary 形态、5-6 参数 detail 形态。
func (s *Server) cmdXPending(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("xpending")
	}
	key, group := args[0].Str, args[1].Str
	if len(args) == 2 {
		sum, err := s.store.StreamXPendingSummary(key, group)
		if err != nil {
			return errReply(err)
		}
		minV := resp.Value{Type: resp.BulkString, Null: true}
		maxV := minV
		if sum.Has {
			minV = resp.Value{Type: resp.BulkString, Str: sum.Min.String()}
			maxV = resp.Value{Type: resp.BulkString, Str: sum.Max.String()}
		}
		cons := make([]resp.Value, 0, len(sum.Consumers))
		for _, c := range sum.Consumers {
			cons = append(cons, resp.Value{Type: resp.Array, Arr: []resp.Value{
				{Type: resp.BulkString, Str: c.Name},
				{Type: resp.Integer, Num: c.Count},
			}})
		}
		return resp.Value{Type: resp.Array, Arr: []resp.Value{
			{Type: resp.Integer, Num: sum.Count}, minV, maxV,
			{Type: resp.Array, Arr: cons},
		}}
	}
	if len(args) != 5 && len(args) != 6 {
		return wrongArgs("xpending")
	}
	startB, err := store.ParseStreamBound(args[2].Str, true)
	if err != nil {
		return errReply(err)
	}
	endB, err := store.ParseStreamBound(args[3].Str, false)
	if err != nil {
		return errReply(err)
	}
	n, ok := parseIntArg(args[4].Str, "xpending")
	if !ok {
		return notIntegerErr()
	}
	if n <= 0 {
		return resp.Value{Type: resp.Error,
			Str: "ERR value is out of range, must be positive"}
	}
	consumer := ""
	if len(args) == 6 {
		consumer = args[5].Str
	}
	items, err := s.store.StreamXPendingDetail(key, group, startB, endB, n, consumer)
	if err != nil {
		return errReply(err)
	}
	rows := make([]resp.Value, 0, len(items))
	for _, it := range items {
		rows = append(rows, resp.Value{Type: resp.Array, Arr: []resp.Value{
			{Type: resp.BulkString, Str: it.ID.String()},
			{Type: resp.BulkString, Str: it.Consumer},
			{Type: resp.Integer, Num: it.IdleMS},
			{Type: resp.Integer, Num: it.Count},
		}})
	}
	return resp.Value{Type: resp.Array, Arr: rows}
}

// isClaimOption 报告 s 是否为 XCLAIM 选项词（选项区与 ID 区的分界）。
func isClaimOption(s string) bool {
	switch strings.ToUpper(s) {
	case "IDLE", "TIME", "RETRYCOUNT", "FORCE", "JUSTID":
		return true
	}
	return false
}

// cmdXClaim 处理 XCLAIM key group consumer min-idle-time id [id ...]
// [IDLE ms] [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID]（写命令，原样落盘）。
func (s *Server) cmdXClaim(args []resp.Value) resp.Value {
	if len(args) < 5 {
		return wrongArgs("xclaim")
	}
	key, group, consumer := args[0].Str, args[1].Str, args[2].Str
	minIdle, ok := parseIntArg(args[3].Str, "xclaim")
	if !ok {
		return notIntegerErr()
	}
	ids := make([]store.StreamID, 0, len(args)-4)
	i := 4
	for i < len(args) && !isClaimOption(args[i].Str) {
		id, valid := store.ParseStreamID(args[i].Str)
		if !valid {
			return errStreamIDArg()
		}
		ids = append(ids, id)
		i++
	}
	var o store.ClaimOpts
	for i < len(args) {
		switch strings.ToUpper(args[i].Str) {
		case "IDLE", "TIME", "RETRYCOUNT":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			v, valid := parseIntArg(args[i+1].Str, "xclaim")
			if !valid {
				return notIntegerErr()
			}
			switch strings.ToUpper(args[i].Str) {
			case "IDLE":
				o.IdleMS, o.SetIdle = v, true
			case "TIME":
				o.TimeMS, o.SetTime = v, true
			case "RETRYCOUNT":
				o.Retry, o.SetRetry = v, true
			}
			i += 2
		case "FORCE":
			o.Force = true
			i++
		case "JUSTID":
			o.JustID = true
			i++
		default:
			return syntaxErr()
		}
	}
	if len(ids) == 0 {
		return wrongArgs("xclaim")
	}
	res, err := s.store.StreamXClaim(key, group, consumer, minIdle, ids, o)
	if err != nil {
		return errReply(err)
	}
	if o.JustID {
		arr := make([]resp.Value, 0, len(res))
		for _, r := range res {
			arr = append(arr, resp.Value{Type: resp.BulkString, Str: r.ID.String()})
		}
		return resp.Value{Type: resp.Array, Arr: arr}
	}
	rows := make([]resp.Value, 0, len(res))
	for _, r := range res {
		rows = append(rows, streamEntryReply(r.Entry))
	}
	return resp.Value{Type: resp.Array, Arr: rows}
}

// -------- canonical 帧化 --------

// xreadGroupFrames 把一次已完成的 XREADGROUP 投喂帧化为 canonical 序列：
// 非空 PEL 变化逐条 XCLAIM（读回精确状态：TIME=投递毫秒、RETRYCOUNT=投递
// 计数、FORCE JUSTID——重放侧从无到有建 PEL 且状态精确收敛；NOACK 无 PEL
// 变化自然无帧）；'>' 模式再追加一条 XGROUP SETID 恢复推进后的组位点——
// XCLAIM 本身不动 lastDelivered，缺 SETID 帧重启后 '>' 会重复投递（Redis
// 对 XREADGROUP 的传播同为 XCLAIM + XGROUP SETID 组合）。caller 需持
// applyMu（store 读回）。
func (s *Server) xreadGroupFrames(key, group, consumer string, ids []store.StreamID, fresh bool) []resp.Value {
	pels := s.store.StreamGroupPel(key, group, ids)
	out := make([]resp.Value, 0, len(pels)+1)
	for _, ps := range pels {
		out = append(out, respCmd("XCLAIM", key, group, consumer, "0",
			ps.ID.String(), "TIME", strconv.FormatInt(ps.DeliveryMS, 10),
			"RETRYCOUNT", strconv.FormatInt(ps.Count, 10), "FORCE", "JUSTID"))
	}
	if fresh && len(ids) > 0 {
		out = append(out, respCmd("XGROUP", "SETID", key, group,
			ids[len(ids)-1].String()))
	}
	return out
}

// xreadGroupCanonical 是 canonicalFor 的 XREADGROUP 特判：从命令参数与成功
// 回复行重建每流投喂的 ID，逐流读回 PEL 生成 XCLAIM 帧（'>' 追加 XGROUP
// SETID 恢复位点）。null 回复（无数据）不产生帧。
func (s *Server) xreadGroupCanonical(v, reply resp.Value) ([]resp.Value, bool) {
	o, errv := parseXReadGroup(v.Arr[1:])
	if errv.Type == resp.Error {
		return nil, false // 防御：成功回复时参数必合法，理论不达
	}
	if reply.Type != resp.Array || reply.Null {
		return nil, false
	}
	idArg := make(map[string]string, len(o.keys))
	for i, k := range o.keys {
		idArg[k] = o.ids[i]
	}
	var out []resp.Value
	for _, row := range reply.Arr {
		if row.Type != resp.Array || len(row.Arr) != 2 {
			return nil, false
		}
		key := row.Arr[0].Str
		ids := make([]store.StreamID, 0, len(row.Arr[1].Arr))
		for _, en := range row.Arr[1].Arr {
			id, ok := store.ParseStreamID(en.Arr[0].Str)
			if !ok {
				return nil, false
			}
			ids = append(ids, id)
		}
		out = append(out, s.xreadGroupFrames(key, o.group, o.consumer,
			ids, idArg[key] == ">")...)
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

// isXReadGroup 报告 v 是否为 XREADGROUP（副本 READONLY 豁免判定：投喂会改
// 本地 PEL，但语义是读——Redis 允许副本执行，Phase 10 阻塞命令同款先例）。
func isXReadGroup(v resp.Value) bool {
	return v.Type == resp.Array && len(v.Arr) > 0 &&
		strings.EqualFold(v.Arr[0].Str, "XREADGROUP")
}
