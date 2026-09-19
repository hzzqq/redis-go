// Stream 消费者组（Phase 12）：XGROUP / XREADGROUP / XACK / XPENDING /
// XCLAIM 的存储层。
//
// 结构：streamVal.groups（组名 → streamGroup）。组内 PEL（Pending Entries
// List）以 map[StreamID]*pelEntry 保存（consumer 归属、投递毫秒、投递次
// 数）；消费者表 map[string]*streamConsumer 仅记录存在性（PEL 归属在
// pelEntry.consumer 字段，遍历过滤即可——PEL 规模有限，换取单一事实源）。
//
// 语义对齐 Redis 7：
//   - XGROUP CREATE：$ = 流 last（空流 0-0）；MKSTREAM 允许在 key 不存在时
//     建空流建组；组已存在 BUSYGROUP；key 不存在且无 MKSTREAM 报 Redis 同文
//     "requires the key to exist" 错误。
//   - XREADGROUP '>'：服务 lastDelivered 之后的条目并推进 lastDelivered
//     （NOACK 同样推进）；非 NOACK 时每条入 PEL（消费者自动创建，count=1）。
//     history（显式 ID）：只服务该消费者 PEL 中 ID > start 的条目，每条
//     count++ 并刷新投递时间；已从流中删除（XDEL）的幽灵条目从 PEL 清除
//     且不返回（Redis 7 清理语义）。
//   - XACK：从 PEL 移除并返回移除数；组不存在回 0 不报错（Redis 同）。
//   - XPENDING：summary（计数/最小最大/每消费者计数）与 detail（ID/消费者/
//     空闲毫秒/投递次数）；key 缺失或组缺失 → NOGROUP，非流 → WRONGTYPE。
//   - XCLAIM：min-idle-time 门（空闲不足跳过且不出现在回复）；FORCE 把不
//     在 PEL 的条目强行置入；JUSTID 只改归属（抑制自动刷新 time=now/count++，
//     显式 TIME/IDLE/RETRYCOUNT 仍生效——传播形态正是 JUSTID+TIME+RETRYCOUNT）；
//     IDLE/TIME 定投递时间（TIME 优先）；RETRYCOUNT 直接设定投递计数；条目
//     已删除的 PEL 幽灵被清除不返回（FORCE 也不为已删条目造幽灵——Redis 7
//     清理语义的从宽实现，诚实取舍）。
//
// 传播形态（server 层职责，此处只供数据）：XREADGROUP 的 PEL 效果由 server
// 以「XCLAIM key group consumer 0 <id> TIME <ms> RETRYCOUNT <n> FORCE
// JUSTID」逐条帧化（Redis streamPropagateXCLAIM 同款），FORCE 保证重放侧
// 从无到有建 PEL、JUSTID+RETRYCOUNT/TIME 使计数与投递时间精确收敛。
//
// 并发：全部方法由 server 在 applyMu 临界区内串行调用，内部 store.mu 照旧。
package store

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// pelEntry 是 PEL 中一条待确认条目的归属与投递状态。
type pelEntry struct {
	consumer   string
	deliveryMS int64 // 最近一次投递的毫秒时间戳
	count      int64 // 累计投递次数
}

// streamConsumer 只记录消费者存在性（名字即身份）。
type streamConsumer struct {
	name string
}

// streamGroup 是一个消费者组的完整状态。
type streamGroup struct {
	name          string
	lastDelivered StreamID // '>' 模式的服务位点
	pel           map[StreamID]*pelEntry
	consumers     map[string]*streamConsumer
}

// PendingItem 是 XPENDING detail 的一行。
type PendingItem struct {
	ID       StreamID
	Consumer string
	IdleMS   int64
	Count    int64
}

// PendingConsumer 是 XPENDING summary 的每消费者计数行。
type PendingConsumer struct {
	Name  string
	Count int64
}

// PendingSummary 是 XPENDING 无参形态（Has=false 时 Min/Max 无意义）。
type PendingSummary struct {
	Count     int64
	Min, Max  StreamID
	Has       bool
	Consumers []PendingConsumer // 名字字典序
}

// PelState 是一条 PEL 条目的可导出形态（RDB/AOF 重写/帧化共用）。
type PelState struct {
	ID         StreamID
	Consumer   string
	DeliveryMS int64
	Count      int64
}

// GroupState 是一个消费者组的可导出形态（RDB kind6 / AOF 重写重建）。
type GroupState struct {
	Name          string
	LastDelivered StreamID
	Consumers     []string   // 名字字典序（含空 PEL 消费者）
	Pel           []PelState // ID 升序
}

// ClaimOpts 是 XCLAIM 的选项（min-idle-time 之外的全部修饰）。
type ClaimOpts struct {
	TimeMS   int64 // SetTime 时为最近投递时间（优先于 IdleMS）
	SetTime  bool
	IdleMS   int64 // SetIdle 时最近投递时间 = now - IdleMS
	SetIdle  bool
	Retry    int64 // SetRetry 时投递计数直接设定
	SetRetry bool
	Force    bool // 不在 PEL 的现存条目强行置入 PEL
	JustID   bool // 只改归属，不动投递时间/计数
}

// ClaimResult 是 XCLAIM 对单个请求 ID 的结果（OK=false = 被跳过，回复不出现）。
type ClaimResult struct {
	ID    StreamID
	Entry StreamEntry // 命中的条目内容（从流中读出）
	OK    bool
}

// -------- 错误（Redis 同文） --------

func errNoGroup(group, key string) error {
	return fmt.Errorf("NOGROUP No such consumer group '%s' for key name '%s'", group, key)
}

var errBusyGroupExists = errors.New("BUSYGROUP Consumer Group name already exists")

var errBusyGroupMissing = errors.New("BUSYGROUP Consumer Group does not exist")

var errXGroupKeyRequired = errors.New(
	"ERR The XGROUP subcommand requires the key to exist. Note that for " +
		"CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")

// lookupStream 取流值（caller holds s.mu）；非流返回 (nil, false)。
func lookupStream(e entry) (*streamVal, bool) {
	sv, isStream := e.val.(*streamVal)
	return sv, isStream
}

// groupOf 从流值取组（缺失返回 false）。
func groupOf(sv *streamVal, group string) (*streamGroup, bool) {
	g, ok := sv.groups[group]
	return g, ok
}

// ensureConsumer 取回（或创建）消费者。
func (g *streamGroup) ensureConsumer(name string) *streamConsumer {
	c, ok := g.consumers[name]
	if !ok {
		c = &streamConsumer{name: name}
		g.consumers[name] = c
	}
	return c
}

// streamEntryAt 在流中二分查找条目（未命中返回 false）。
func (sv *streamVal) streamEntryAt(id StreamID) (StreamEntry, bool) {
	lo := sort.Search(len(sv.entries), func(i int) bool { return sv.entries[i].ID.gte(id) })
	if lo < len(sv.entries) && sv.entries[lo].ID == id {
		return sv.entries[lo], true
	}
	return StreamEntry{}, false
}

// -------- XGROUP --------

// StreamGroupCreate 处理 XGROUP CREATE key group [id|$] [MKSTREAM]。
// mkstream=true 且 key 不存在时创建空流。id 为显式定位（$ 由 server 层先解
// 析为 StreamLastID）。
func (s *Store) StreamGroupCreate(key, group string, id StreamID, mkstream bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var sv *streamVal
	if ok {
		v, isStream := e.val.(*streamVal)
		if !isStream {
			return ErrWrongType
		}
		sv = v
	} else {
		if !mkstream {
			return errXGroupKeyRequired
		}
		sv = &streamVal{}
		s.m[key] = entry{val: sv}
	}
	if sv.groups == nil {
		sv.groups = map[string]*streamGroup{}
	}
	if _, exists := sv.groups[group]; exists {
		return errBusyGroupExists
	}
	sv.groups[group] = &streamGroup{
		name:          group,
		lastDelivered: id,
		pel:           map[StreamID]*pelEntry{},
		consumers:     map[string]*streamConsumer{},
	}
	return nil
}

// StreamGroupSetID 处理 XGROUP SETID key group id|$。
func (s *Store) StreamGroupSetID(key, group string, id StreamID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return errXGroupKeyRequired
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return errBusyGroupMissing
	}
	g.lastDelivered = id
	return nil
}

// StreamGroupDestroy 处理 XGROUP DESTROY key group（1 = 已删除）。
func (s *Store) StreamGroupDestroy(key, group string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, nil // key 不存在：组必然不存在，回 0（Redis 同）
	}
	sv, isStream := e.val.(*streamVal)
	if !isStream {
		return 0, ErrWrongType
	}
	if _, exists := sv.groups[group]; !exists {
		return 0, nil
	}
	delete(sv.groups, group)
	// 组删空且流已空 → key 恢复「清空即删」语义（Redis 7 同）
	if len(sv.entries) == 0 && len(sv.groups) == 0 {
		delete(s.m, key)
	}
	return 1, nil
}

// StreamGroupCreateConsumer 处理 XGROUP CREATECONSUMER（1 = 新建）。
func (s *Store) StreamGroupCreateConsumer(key, group, consumer string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, errXGroupKeyRequired
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return 0, ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return 0, errBusyGroupMissing
	}
	if _, exists := g.consumers[consumer]; exists {
		return 0, nil
	}
	g.consumers[consumer] = &streamConsumer{name: consumer}
	return 1, nil
}

// StreamGroupDelConsumer 处理 XGROUP DELCONSUMER：删除消费者并清空其 PEL，
// 返回清掉的 PEL 条数。
func (s *Store) StreamGroupDelConsumer(key, group, consumer string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, errXGroupKeyRequired
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return 0, ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return 0, errBusyGroupMissing
	}
	if _, exists := g.consumers[consumer]; !exists {
		return 0, nil
	}
	delete(g.consumers, consumer)
	var n int64
	for id, pe := range g.pel {
		if pe.consumer == consumer {
			delete(g.pel, id)
			n++
		}
	}
	return n, nil
}

// -------- XREADGROUP --------

// StreamReadGroupNew 处理 XREADGROUP '>' 模式：服务组 lastDelivered 之后的
// 条目（count>0 截断），推进 lastDelivered；非 noack 时每条入 PEL（消费者
// 自动创建，投递时间 = now，count = 1）。返回 (条目, key是否存在, 错误)：
// key 不存在 → (nil,false,nil)（调用方回 null）；组不存在 → NOGROUP。
func (s *Store) StreamReadGroupNew(key, group, consumer string, count int64, noack bool) ([]StreamEntry, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return nil, false, nil
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return nil, true, ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return nil, true, errNoGroup(group, key)
	}
	lo := sort.Search(len(sv.entries), func(i int) bool {
		return sv.entries[i].ID.gte(g.lastDelivered) && sv.entries[i].ID != g.lastDelivered
	})
	out := sv.entries[lo:]
	if count > 0 && int64(len(out)) > count {
		out = out[:count]
	}
	if len(out) > 0 {
		g.lastDelivered = out[len(out)-1].ID
	}
	if !noack {
		now := time.Now().UnixMilli()
		g.ensureConsumer(consumer)
		for _, en := range out {
			g.pel[en.ID] = &pelEntry{consumer: consumer, deliveryMS: now, count: 1}
		}
	}
	return append([]StreamEntry(nil), out...), true, nil
}

// StreamReadGroupHistory 处理 XREADGROUP history 模式（显式 ID）：服务该消
// 费者 PEL 中 ID > start 的条目，每条 count++ 并刷新投递时间；已从流删除的
// 幽灵条目从 PEL 清除且不返回。key/组不存在 → NOGROUP。
func (s *Store) StreamReadGroupHistory(key, group, consumer string, start StreamID, count int64) ([]StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return nil, errNoGroup(group, key)
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return nil, ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return nil, errNoGroup(group, key)
	}
	// 收集该消费者的待确认 ID（> start），升序
	ids := make([]StreamID, 0, len(g.pel))
	for id, pe := range g.pel {
		if pe.consumer != consumer {
			continue
		}
		if id.gte(start) && id != start {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].less(ids[j]) })
	if count > 0 && int64(len(ids)) > count {
		ids = ids[:count]
	}
	now := time.Now().UnixMilli()
	g.ensureConsumer(consumer)
	out := make([]StreamEntry, 0, len(ids))
	for _, id := range ids {
		en, exists := sv.streamEntryAt(id)
		if !exists {
			delete(g.pel, id) // 幽灵清理（Redis 7 语义）
			continue
		}
		pe := g.pel[id]
		pe.deliveryMS = now
		pe.count++
		out = append(out, en)
	}
	return out, nil
}

// -------- XACK --------

// StreamXack 从组 PEL 移除指定条目并返回移除数。key/组不存在回 0 不报错
// （Redis 同）；非流 → ErrWrongType。
func (s *Store) StreamXack(key, group string, ids ...StreamID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return 0, nil
	}
	sv, isStream := e.val.(*streamVal)
	if !isStream {
		return 0, ErrWrongType
	}
	g, found := sv.groups[group]
	if !found {
		return 0, nil
	}
	var n int64
	for _, id := range ids {
		if _, in := g.pel[id]; in {
			delete(g.pel, id)
			n++
		}
	}
	return n, nil
}

// -------- XPENDING --------

// StreamXPendingSummary 处理 XPENDING key group（无参形态）。
func (s *Store) StreamXPendingSummary(key, group string) (PendingSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := validRO(s.m, key)
	if !ok {
		return PendingSummary{}, errNoGroup(group, key)
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return PendingSummary{}, ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return PendingSummary{}, errNoGroup(group, key)
	}
	sum := PendingSummary{Count: int64(len(g.pel))}
	per := map[string]int64{}
	for id, pe := range g.pel {
		if !sum.Has || id.less(sum.Min) {
			sum.Min = id
		}
		if !sum.Has || sum.Max.less(id) {
			sum.Max = id
		}
		sum.Has = true
		per[pe.consumer]++
	}
	for name, cnt := range per {
		sum.Consumers = append(sum.Consumers, PendingConsumer{Name: name, Count: cnt})
	}
	sort.Slice(sum.Consumers, func(i, j int) bool { return sum.Consumers[i].Name < sum.Consumers[j].Name })
	return sum, nil
}

// StreamXPendingDetail 处理 XPENDING key group start end count [consumer]。
// 端点复用 StreamBound（- / + / ( 排他 / 显式 ID）。ID 升序，count 截断。
func (s *Store) StreamXPendingDetail(key, group string, start, end StreamBound, count int64, consumer string) ([]PendingItem, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := validRO(s.m, key)
	if !ok {
		return nil, errNoGroup(group, key)
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return nil, ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return nil, errNoGroup(group, key)
	}
	now := time.Now().UnixMilli()
	items := make([]PendingItem, 0, len(g.pel))
	for id, pe := range g.pel {
		if !streamMinOK(start, id) || !streamMaxOK(end, id) {
			continue
		}
		if consumer != "" && pe.consumer != consumer {
			continue
		}
		items = append(items, PendingItem{ID: id, Consumer: pe.consumer,
			IdleMS: now - pe.deliveryMS, Count: pe.count})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID.less(items[j].ID) })
	if count >= 0 && int64(len(items)) > count {
		items = items[:count]
	}
	return items, nil
}

// -------- XCLAIM --------

// StreamXClaim 处理 XCLAIM：按 min-idle-time 认领 PEL 条目到 consumer。
// 语义见文件头。返回按请求顺序、仅含成功认领的结果。
func (s *Store) StreamXClaim(key, group, consumer string, minIdleMS int64, ids []StreamID, o ClaimOpts) ([]ClaimResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	if !ok {
		return nil, errNoGroup(group, key)
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return nil, ErrWrongType
	}
	g, found := groupOf(sv, group)
	if !found {
		return nil, errNoGroup(group, key)
	}
	now := time.Now().UnixMilli()
	g.ensureConsumer(consumer)
	out := make([]ClaimResult, 0, len(ids))
	for _, id := range ids {
		pe, inPEL := g.pel[id]
		en, inStream := sv.streamEntryAt(id)
		if !inStream {
			if inPEL {
				delete(g.pel, id) // 幽灵清理：条目已从流删除
			}
			continue // FORCE 也不为已删条目造幽灵（诚实取舍，见文件头）
		}
		if inPEL {
			if now-pe.deliveryMS < minIdleMS {
				continue // 空闲不足：跳过（回复不出现，PEL 不动）
			}
		} else if !o.Force {
			continue // 不在 PEL 且无 FORCE：跳过
		}
		if !inPEL {
			pe = &pelEntry{}
			g.pel[id] = pe
		}
		pe.consumer = consumer
		// JUSTID 抑制的是「自动刷新」（time=now / count++），显式 TIME/IDLE/
		// RETRYCOUNT 仍生效——Redis 传播形态正是 XCLAIM ... FORCE JUSTID
		// TIME <ms> RETRYCOUNT <n>，重放侧靠它精确恢复投递状态。
		switch {
		case o.SetTime:
			pe.deliveryMS = o.TimeMS
		case o.SetIdle:
			pe.deliveryMS = now - o.IdleMS
		case !o.JustID:
			pe.deliveryMS = now
		}
		if o.SetRetry {
			pe.count = o.Retry
		} else if !o.JustID {
			if inPEL {
				pe.count++
			} else {
				pe.count = 1 // FORCE 新置入视为首次投递
			}
		}
		out = append(out, ClaimResult{ID: id, Entry: en, OK: true})
	}
	return out, nil
}

// -------- 导出 / 帧化支撑 --------

// streamGroupsSorted 把 streamVal 的组状态导出为确定序列表（组名字典序、
// 消费者名字典序、PEL ID 升序）。caller 必须已持有 store 锁（RWMutex 不可
// 重入，Snapshot/Export 内联调用而非经 StreamGroups）。
func streamGroupsSorted(sv *streamVal) []GroupState {
	if len(sv.groups) == 0 {
		return nil
	}
	names := make([]string, 0, len(sv.groups))
	for name := range sv.groups {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]GroupState, 0, len(names))
	for _, name := range names {
		g := sv.groups[name]
		gs := GroupState{Name: name, LastDelivered: g.lastDelivered}
		for cname := range g.consumers {
			gs.Consumers = append(gs.Consumers, cname)
		}
		sort.Strings(gs.Consumers)
		for id, pe := range g.pel {
			gs.Pel = append(gs.Pel, PelState{ID: id, Consumer: pe.consumer,
				DeliveryMS: pe.deliveryMS, Count: pe.count})
		}
		sort.Slice(gs.Pel, func(i, j int) bool { return gs.Pel[i].ID.less(gs.Pel[j].ID) })
		out = append(out, gs)
	}
	return out
}

// StreamGroups 导出 key 上全部组状态（组名字典序；消费者名字典序；PEL 按
// ID 升序——RDB 字节级确定）。key 不存在/非流/无组 → nil。
func (s *Store) StreamGroups(key string) []GroupState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := validRO(s.m, key)
	if !ok {
		return nil
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return nil
	}
	return streamGroupsSorted(sv)
}

// StreamGroupPel 按请求顺序返回组 PEL 中命中的条目状态（canonicalFor 把
// XREADGROUP 投喂精确帧化为 XCLAIM FORCE JUSTID 用）。组不存在 → nil。
func (s *Store) StreamGroupPel(key, group string, ids []StreamID) []PelState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := validRO(s.m, key)
	if !ok {
		return nil
	}
	sv, isStream := lookupStream(e)
	if !isStream {
		return nil
	}
	g, found := groupOf(sv, group)
	if !found {
		return nil
	}
	out := make([]PelState, 0, len(ids))
	for _, id := range ids {
		if pe, in := g.pel[id]; in {
			out = append(out, PelState{ID: id, Consumer: pe.consumer,
				DeliveryMS: pe.deliveryMS, Count: pe.count})
		}
	}
	return out
}

// StreamRestoreGroups 为已重建的流附加组状态（RDB kind6 回放；启动路径无
// 并发写，直接覆盖式重建）。
func (s *Store) StreamRestoreGroups(key string, groups []GroupState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	sv, isStream := e.val.(*streamVal)
	if !ok || !isStream {
		return
	}
	if sv.groups == nil {
		sv.groups = map[string]*streamGroup{}
	}
	for _, gs := range groups {
		g := &streamGroup{name: gs.Name, lastDelivered: gs.LastDelivered,
			pel: map[StreamID]*pelEntry{}, consumers: map[string]*streamConsumer{}}
		for _, c := range gs.Consumers {
			g.consumers[c] = &streamConsumer{name: c}
		}
		for _, ps := range gs.Pel {
			g.pel[ps.ID] = &pelEntry{consumer: ps.Consumer, deliveryMS: ps.DeliveryMS, count: ps.Count}
		}
		sv.groups[gs.Name] = g
	}
}
