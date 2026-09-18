// Stream（Phase 11）：Redis 兼容的追加式日志类型核心。
//
// 结构：streamVal{entries 按 ID 升序的切片, last 迄今最后写入的 ID}。
// 条目查找二分定位、删除线性过滤（与全项目"原生 Go 结构 + 诚实映射"的
// 取舍一致：Redis 用 radix tree，我们用有序切片）。last 由 XADD 推进，
// XDEL/XTRIM 不回退；整个 key 被删除时随之消失（无悬空 last）。
//
// 语义对齐 Redis 7：
//   - ID 形如 <ms>-<seq>（uint64 各段）；自动 ID：ms = max(当前毫秒,
//     last.ms)，同/超 ms 时 seq = last.seq+1（跨 ms 归 0）；
//   - 显式 ID 必须严格大于 last（空流/新流视为 0-0），错误文与 Redis
//     同款："The ID specified in XADD must be greater than <last>"；
//   - XDEL/XTRIM 清空流即删除整个 key（Redis 7 不再保留空流）；
//   - 条目字段为扁平偶数序列（field,value,...），写入后不可变——范围读
//     返回的切片与 store 内部状态共享底层数组是安全的（只追加/整体摘除，
//     从不原地改写条目值）。
package store

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// errStreamIDTooSmallXADD 是显式 ID 不递增的 Redis 同文错误（last 为空流
// 零值时自然得到 "greater than 0-0"）。
func errStreamIDTooSmallXADD(last StreamID) error {
	return fmt.Errorf("ERR The ID specified in XADD must be greater than %s", last)
}

// errStreamIDInvalid 是 ID/bound 参数解析失败的 Redis 同文错误（server 层
// 解析参数时复用）。
var errStreamIDInvalid = errors.New("ERR Invalid stream ID specified as stream command argument")

// StreamID 是一条流条目的 <毫秒>-<序号> 标识，两段均为无符号 64 位。
type StreamID struct {
	MS  uint64
	Seq uint64
}

// ParseStreamID 解析 "ms-seq" 或裸 "ms"（seq=0）。空段、负数、溢出、
// 多余 '-' 均为非法。
func ParseStreamID(s string) (StreamID, bool) {
	if s == "" {
		return StreamID{}, false
	}
	msStr, seqStr := s, "0"
	if i := strings.IndexByte(s, '-'); i >= 0 {
		msStr, seqStr = s[:i], s[i+1:]
		if seqStr == "" {
			return StreamID{}, false
		}
	}
	ms, err := strconv.ParseUint(msStr, 10, 64)
	if err != nil {
		return StreamID{}, false
	}
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		return StreamID{}, false
	}
	return StreamID{MS: ms, Seq: seq}, true
}

// String 还原 "<ms>-<seq>" 形态。
func (id StreamID) String() string {
	return strconv.FormatUint(id.MS, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

// less 报告 id 是否严格小于 b。
func (id StreamID) less(b StreamID) bool {
	return id.MS < b.MS || (id.MS == b.MS && id.Seq < b.Seq)
}

// gte 报告 id 是否大于等于 b。
func (id StreamID) gte(b StreamID) bool { return !id.less(b) }

// IsZero 报告 id 是否为 0-0。
func (id StreamID) IsZero() bool { return id.MS == 0 && id.Seq == 0 }

// StreamEntry 是一条流条目：ID 加扁平偶数字段序列。
type StreamEntry struct {
	ID     StreamID
	Fields []string // field,value,...（偶数长度；写入后不可变）
}

// streamVal 是 stream 类型的存储值。
type streamVal struct {
	entries []StreamEntry // 按 ID 升序
	last    StreamID      // 迄今最后写入的 ID（XDEL/XTRIM 不回退）
}

// StreamBound 是 XRANGE 端点：-/+ 无穷、有限 ID、可选 "(" 排他。
type StreamBound struct {
	ID  StreamID
	Ex  bool
	Inf int // -1 = "-"，+1 = "+"，0 = 有限 ID
}

// ParseStreamBound 解析 XRANGE/XREVRANGE 端点："-"（负无穷）、"+"（正无穷）、
// "(" 前缀排他、"ms-seq"/裸 "ms"。裸数字作为 start 时 seq=0、作为 end 时
// 含该毫秒全部序号（seq=uint64max），与 Redis 一致。
func ParseStreamBound(s string, isStart bool) (StreamBound, error) {
	switch s {
	case "-":
		return StreamBound{Inf: -1}, nil
	case "+":
		return StreamBound{Inf: 1}, nil
	}
	ex := strings.HasPrefix(s, "(")
	if ex {
		s = s[1:]
	}
	id, ok := ParseStreamID(s)
	if !ok {
		return StreamBound{}, errStreamIDInvalid
	}
	if !strings.Contains(s, "-") && !ex {
		if !isStart {
			id.Seq = ^uint64(0) // end 裸数字：含该毫秒全部条目
		}
	}
	return StreamBound{ID: id, Ex: ex}, nil
}

// streamMinOK 是范围下界检查（"-" 恒真、"+" 恒假；Ex 为 "(" 排他）。
func streamMinOK(min StreamBound, id StreamID) bool {
	switch min.Inf {
	case -1:
		return true
	case 1:
		return false
	}
	if min.Ex {
		return id.gte(min.ID) && id != min.ID
	}
	return id.gte(min.ID)
}

// streamMaxOK 是范围上界检查（"+" 恒真、"-" 恒假；Ex 为 "(" 排他）。
func streamMaxOK(max StreamBound, id StreamID) bool {
	switch max.Inf {
	case 1:
		return true
	case -1:
		return false
	}
	if max.Ex {
		return id.less(max.ID)
	}
	return id.MS < max.ID.MS || (id.MS == max.ID.MS && id.Seq <= max.ID.Seq)
}

// -------- 命令支撑方法（全部由 server 在 applyMu 下串行调用） --------

// StreamAdd 向 key 追加一条条目。auto=true 时生成自动 ID；否则 id 为显式
// ID（必须严格递增）。mkStream=false（NOMKSTREAM）且 key 不存在时不创建，
// 返回 added=false。返回实际写入的 ID。
//   - key 存在但非流 → ErrWrongType
//   - 显式 ID <= last → Redis 同文错误，流不变
func (s *Store) StreamAdd(key string, auto bool, id StreamID, fields []string, mkStream bool) (StreamID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := validLocked(s.m, key)
	var sv *streamVal
	if ok {
		v, isStream := e.val.(*streamVal)
		if !isStream {
			return StreamID{}, false, ErrWrongType
		}
		sv = v
	} else {
		if !mkStream {
			return StreamID{}, false, nil
		}
		sv = &streamVal{}
	}
	var got StreamID
	if auto {
		now := uint64(time.Now().UnixMilli())
		switch {
		case now > sv.last.MS || sv.last.IsZero():
			got = StreamID{MS: now} // 跨毫秒（含空流）：seq 归 0
		default:
			got = StreamID{MS: sv.last.MS, Seq: sv.last.Seq + 1}
		}
	} else {
		if id.IsZero() {
			// Redis 对 0-0 固定报这条（不管流当前 last 是多少）
			return StreamID{}, false, errors.New("ERR The ID specified in XADD must be greater than 0-0")
		}
		if !sv.last.less(id) {
			return StreamID{}, false, errStreamIDTooSmallXADD(sv.last)
		}
		got = id
	}
	sv.entries = append(sv.entries, StreamEntry{ID: got, Fields: append([]string(nil), fields...)})
	sv.last = got
	if !ok {
		s.m[key] = entry{val: sv}
	}
	return got, true, nil
}

// StreamLen 返回流条目数（key 不存在为 0；非流 → ErrWrongType）。
func (s *Store) StreamLen(key string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := validRO(s.m, key)
	if !ok {
		return 0, nil
	}
	sv, isStream := e.val.(*streamVal)
	if !isStream {
		return 0, ErrWrongType
	}
	return int64(len(sv.entries)), nil
}

// StreamRange 按范围返回条目（rev=true 时从大到小）。count<0 表示不限制；
// count 截断发生在方向确定之后（XRANGE 取最小 count 条，XREVRANGE 取最大）。
// key 不存在返回空切片；非流 → ErrWrongType。
func (s *Store) StreamRange(key string, min, max StreamBound, rev bool, count int64) ([]StreamEntry, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	if !ok {
		s.mu.RUnlock()
		return []StreamEntry{}, nil
	}
	sv, isStream := e.val.(*streamVal)
	if !isStream {
		s.mu.RUnlock()
		return nil, ErrWrongType
	}
	entries := sv.entries
	s.mu.RUnlock()

	var out []StreamEntry
	for _, en := range entries {
		if streamMinOK(min, en.ID) && streamMaxOK(max, en.ID) {
			out = append(out, en)
		}
	}
	if rev {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	if count >= 0 && int64(len(out)) > count {
		out = out[:count]
	}
	if out == nil {
		out = []StreamEntry{}
	}
	return out, nil
}

// StreamDel 删除指定 ID 的条目，返回实际删除数。流被清空时整个 key 删除
// （Redis 7 语义）。非流 → ErrWrongType。
func (s *Store) StreamDel(key string, ids ...StreamID) (int64, error) {
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
	var n int64
	kept := sv.entries[:0]
	for _, en := range sv.entries {
		del := false
		for _, id := range ids {
			if en.ID == id {
				del = true
				break
			}
		}
		if del {
			n++
		} else {
			kept = append(kept, en)
		}
	}
	sv.entries = kept
	if len(sv.entries) == 0 {
		delete(s.m, key)
	}
	return n, nil
}

// StreamTrim 按策略修剪流并返回删除数：maxLen>=0 保留最新 maxLen 条；
// 否则 minID 非 nil 时删除 ID < minID 的条目（二者由 server 保证互斥）。
// 流被清空时整个 key 删除（Redis 7 语义）。
func (s *Store) StreamTrim(key string, maxLen int64, minID *StreamID) (int64, error) {
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
	var n int64
	var kept []StreamEntry
	if maxLen >= 0 {
		if int64(len(sv.entries)) <= maxLen {
			return 0, nil
		}
		cut := int64(len(sv.entries)) - maxLen
		kept = append([]StreamEntry(nil), sv.entries[cut:]...)
		n = cut
	} else {
		for _, en := range sv.entries {
			if en.ID.less(*minID) {
				n++
			} else {
				kept = append(kept, en)
			}
		}
		if kept == nil {
			kept = []StreamEntry{}
		}
	}
	sv.entries = kept
	if len(sv.entries) == 0 {
		delete(s.m, key)
	}
	return n, nil
}

// StreamRead 返回 ID 严格大于 last 的条目（XREAD 语义；count>0 截断）。
// 第二个返回值报告 key 是否存在（不存在视为无数据；非流 → ErrWrongType）。
func (s *Store) StreamRead(key string, last StreamID, count int64) ([]StreamEntry, bool, error) {
	s.mu.RLock()
	e, ok := validRO(s.m, key)
	if !ok {
		s.mu.RUnlock()
		return nil, false, nil
	}
	sv, isStream := e.val.(*streamVal)
	if !isStream {
		s.mu.RUnlock()
		return nil, true, ErrWrongType
	}
	// 二分定位第一个 > last 的条目
	lo := sort.Search(len(sv.entries), func(i int) bool {
		return sv.entries[i].ID.gte(last) && sv.entries[i].ID != last
	})
	out := sv.entries[lo:]
	s.mu.RUnlock()
	if count > 0 && int64(len(out)) > count {
		out = out[:count]
	}
	return out, true, nil
}

// StreamLastID 返回流的最后写入 ID（XREAD "$" 解析用；key 不存在返回
// 零值；非流 → ErrWrongType）。
func (s *Store) StreamLastID(key string) (StreamID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := validRO(s.m, key)
	if !ok {
		return StreamID{}, nil
	}
	sv, isStream := e.val.(*streamVal)
	if !isStream {
		return StreamID{}, ErrWrongType
	}
	return sv.last, nil
}

// StreamRestore 以给定条目（ID 升序，来自 RDB 导出/测试）整体重建流，
// 绕过逐条递增校验。last 取末尾条目 ID。
func (s *Store) StreamRestore(key string, entries []StreamEntry) {
	sv := &streamVal{entries: append([]StreamEntry(nil), entries...)}
	if len(entries) > 0 {
		sv.last = entries[len(entries)-1].ID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = entry{val: sv}
}
