// SORT（Phase 10）：SORT key [BY pattern] [LIMIT offset count]
// [GET pattern ...] [ASC|DESC] [ALPHA] [STORE dst]。
//
// 语义对齐 Redis：
//   - 数据源：list（原序）、set（成员字典序快照——Redis 内部序不可移植，
//     取确定序）、zset（跳表 rank 序，score asc）；key 不存在 → 空结果；
//     string 等其他类型 → WRONGTYPE。
//   - 排序键：无 BY 时以元素自身为键；BY pattern（`*` 替换为元素值）支持
//     `prefix*->field`（hash 字段）与 `prefix*`（string key）；引用缺失 →
//     数值序按 0、ALPHA 按空串（Redis 同）；`BY nosort` 保留原序（配合 GET
//     做批量取值）。数值序要求全部权重可解析为 float64，否则报 Redis 原文
//     错误；ALPHA 按字符串比较。等分以元素字典序打破（Redis 同），DESC 整体
//     反转含 tiebreak。
//   - GET pattern：`#` 取元素本身，其余同 BY 的取值规则；缺失 → null bulk。
//     多个 GET 依次展开为扁平数组。LIMIT 在排序后、GET 展开前生效，count<0
//     表示取到末尾。
//   - STORE dst：结果（GET 展开后）覆写为 list 写入 dst（空结果删除 dst，
//     回复 0；非空回复元素数）。这是写命令：apply 走 applyMu + canonicalFor
//     落 RPUSH/DEL 确定化帧；副本上报 READONLY；脚本内禁用（见 eval.go）。
package server

import (
	"sort"
	"strconv"
	"strings"

	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// sortHasStore reports whether SORT args contain the STORE option (used by
// eval's callFunc to reject SORT STORE from scripts).
func sortHasStore(args []string) bool {
	for i, a := range args {
		if strings.EqualFold(a, "STORE") && i+1 < len(args) {
			return true
		}
	}
	return false
}

// sortItem 是一个待排序元素及其排序权重。
type sortItem struct {
	el string  // 元素本身
	w  string  // BY/元素值字符串形态（ALPHA 用）
	wf float64 // 数值形态（数值序用；BY 引用缺失按 0，Redis 同）
}

func (s *Server) cmdSort(args []resp.Value) resp.Value {
	key := args[0].Str
	var byPat string
	var gets []string
	limitOff, limitCnt := int64(0), int64(-1)
	desc, alpha := false, false
	storeDst, hasStore := "", false
	for i := 1; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "BY":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			byPat = args[i+1].Str
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return syntaxErr()
			}
			off, ok1 := parseIntArg(args[i+1].Str, "sort")
			cnt, ok2 := parseIntArg(args[i+2].Str, "sort")
			if !ok1 || !ok2 {
				return notIntegerErr()
			}
			if off < 0 {
				return syntaxErr()
			}
			limitOff, limitCnt = off, cnt
			i += 2
		case "GET":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			gets = append(gets, args[i+1].Str)
			i++
		case "ASC":
		case "DESC":
			desc = true
		case "ALPHA":
			alpha = true
		case "STORE":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			storeDst, hasStore = args[i+1].Str, true
			i++
		default:
			return syntaxErr()
		}
	}

	// 数据源
	var elems []string
	switch s.store.Type(key) {
	case "none":
		elems = []string{}
	case "list":
		items, err := s.store.ListRange(key, 0, -1)
		if err != nil {
			return errReply(err)
		}
		elems = items
	case "set":
		ms, err := s.store.SetMembers(key)
		if err != nil {
			return errReply(err)
		}
		sort.Strings(ms) // 确定输入序（Redis 内部序不可移植）
		elems = ms
	case "zset":
		items, err := s.store.ZRange(key, 0, -1, false)
		if err != nil {
			return errReply(err)
		}
		elems = make([]string, len(items))
		for i, it := range items {
			elems[i] = it.Member
		}
	default:
		return errReply(store.ErrWrongType)
	}

	items := make([]sortItem, len(elems))
	for i, el := range elems {
		items[i] = sortItem{el: el, w: el}
	}

	// 排序键（nosort 保留原序，权重不参与比较）
	nosort := byPat != "" && strings.EqualFold(byPat, "nosort")
	if byPat != "" && !nosort {
		for i := range items {
			w, found := s.lookupSortPattern(byPat, items[i].el)
			items[i].w = w
			if alpha {
				continue
			}
			if !found {
				continue // 缺失引用按 0（Redis 同），wf 已是零值
			}
			f, err := strconv.ParseFloat(w, 64)
			if err != nil {
				return resp.Value{Type: resp.Error,
					Str: "ERR One or more scores can't be converted into double"}
			}
			items[i].wf = f
		}
	} else if !alpha && !nosort {
		for i := range items {
			f, err := strconv.ParseFloat(items[i].w, 64)
			if err != nil {
				return resp.Value{Type: resp.Error,
					Str: "ERR One or more scores can't be converted into double"}
			}
			items[i].wf = f
		}
	}

	// 排序
	if !nosort {
		sort.SliceStable(items, func(a, b int) bool {
			if desc {
				a, b = b, a
			}
			if alpha {
				if items[a].w != items[b].w {
					return items[a].w < items[b].w
				}
			} else if items[a].wf != items[b].wf {
				return items[a].wf < items[b].wf
			}
			return items[a].el < items[b].el // 等分按元素字典序（Redis 同）
		})
	}

	// LIMIT（排序后、GET 展开前）
	end := int64(len(items))
	if limitCnt >= 0 && limitOff+limitCnt < end {
		end = limitOff + limitCnt
	}
	if limitOff > int64(len(items)) {
		limitOff = int64(len(items))
	}
	sel := items[limitOff:end]

	// GET 展开为元素序列（缺失项跳过——list 元素不能为 null）
	expand := func() []string {
		var vals []string
		for _, it := range sel {
			if len(gets) == 0 {
				vals = append(vals, it.el)
				continue
			}
			for _, g := range gets {
				if g == "#" {
					vals = append(vals, it.el)
					continue
				}
				if v, ok := s.lookupSortPattern(g, it.el); ok {
					vals = append(vals, v)
				}
			}
		}
		return vals
	}

	if hasStore {
		if s.isReplica() {
			return readonlyErr()
		}
		vals := expand()
		s.store.Del(storeDst)
		if len(vals) > 0 {
			if _, err := s.store.ListPush(storeDst, false, vals...); err != nil {
				return errReply(err)
			}
			return resp.Value{Type: resp.Integer, Num: int64(len(vals))}
		}
		return resp.Value{Type: resp.Integer, Num: 0} // 空结果：dst 已删
	}

	// RESP 组装（缺失 GET 项 → null bulk）
	arr := make([]resp.Value, 0, len(sel)*maxInt(1, len(gets)))
	for _, it := range sel {
		if len(gets) == 0 {
			arr = append(arr, resp.Value{Type: resp.BulkString, Str: it.el})
			continue
		}
		for _, g := range gets {
			if g == "#" {
				arr = append(arr, resp.Value{Type: resp.BulkString, Str: it.el})
				continue
			}
			if v, ok := s.lookupSortPattern(g, it.el); ok {
				arr = append(arr, resp.Value{Type: resp.BulkString, Str: v})
			} else {
				arr = append(arr, resp.Value{Type: resp.BulkString, Null: true})
			}
		}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

// lookupSortPattern resolves a BY/GET pattern for one element: `*` is
// replaced by the element (first occurrence); `prefix*->field` reads a hash
// field, `prefix*` a string key. Missing or wrong-typed references report as
// missing (weight 0 / empty, null GET) — Redis treats them the same.
func (s *Server) lookupSortPattern(pat, el string) (string, bool) {
	if !strings.Contains(pat, "*") { // 无 *：固定 key（全体同权，Redis 同）
		return s.lookupSortVal(pat)
	}
	name := strings.Replace(pat, "*", el, 1)
	if idx := strings.Index(name, "->"); idx >= 0 {
		v, ok, err := s.store.HashGet(name[:idx], name[idx+2:])
		if err != nil || !ok {
			return "", false
		}
		return v, true
	}
	return s.lookupSortVal(name)
}

// lookupSortVal reads a string key for BY/GET, treating wrong types as
// missing.
func (s *Server) lookupSortVal(key string) (string, bool) {
	v, ok, err := s.store.Get(key)
	if err != nil || !ok {
		return "", false
	}
	return v, true
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
