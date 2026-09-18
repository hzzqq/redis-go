// Phase 9 新命令：SCAN/SSCAN/HSCAN/ZSCAN 游标遍历、OBJECT ENCODING、
// LMOVE/LINSERT/LPOS List 补全。
//
// SCAN 游标语义：Redis 用 reverse binary iteration（游标是字典桶位图），
// redis-go 的 store 是单个 Go map（桶结构不可见），这里采用「排序快照 +
// 数字偏移」：每次 SCAN 拿全量 live key 排序，cursor = 已扫描偏移，一次
// 扫描 COUNT 个 key、过滤后返回。保证：遍历期间一直存在的 key 恰好返回
// 一次；集合增删导致的漏/重与 Redis 一样不提供保证。代价是每次调用
// O(N log N) 的快照排序（Redis 是 O(1) 游标状态），教学实现可接受。
package server

import (
	"sort"
	"strconv"
	"strings"

	"github.com/hzzqq/redis-go/internal/resp"
)

// globMatch 是 Redis stringmatchlen 的 Go 直译：支持 * ? [abc] [^abc]
// [a-z] 区间与反斜杠转义；[ 后第一个字符的 ] 按字面量处理（[]] 匹配 "]"）。
func globMatch(pattern, str string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			for i := 0; i <= len(str); i++ {
				if globMatch(pattern, str[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(str) == 0 {
				return false
			}
			pattern, str = pattern[1:], str[1:]
		case '[':
			if len(str) == 0 {
				return false
			}
			c := str[0]
			pattern = pattern[1:]
			negate := false
			if len(pattern) > 0 && pattern[0] == '^' {
				negate = true
				pattern = pattern[1:]
			}
			matched := false
			first := true
			for {
				if len(pattern) == 0 {
					return false // 未闭合的 [
				}
				if pattern[0] == '\\' && len(pattern) > 1 {
					pattern = pattern[1:] // 转义：下一字符按字面量参与匹配
				} else if pattern[0] == ']' && !first {
					pattern = pattern[1:]
					break
				}
				first = false
				if len(pattern) == 0 {
					return false
				}
				lo := pattern[0]
				if len(pattern) >= 3 && pattern[1] == '-' && pattern[2] != ']' {
					hi := pattern[2]
					if lo <= c && c <= hi {
						matched = true
					}
					pattern = pattern[3:]
					continue
				}
				if lo == c {
					matched = true
				}
				pattern = pattern[1:]
			}
			if negate {
				matched = !matched
			}
			if !matched {
				return false
			}
			str = str[1:]
		case '\\':
			if len(pattern) < 2 || len(str) == 0 || str[0] != pattern[1] {
				return false
			}
			pattern, str = pattern[2:], str[1:]
		default:
			if len(str) == 0 || pattern[0] != str[0] {
				return false
			}
			pattern, str = pattern[1:], str[1:]
		}
	}
	return len(str) == 0
}

// scanOpts is the parsed option tail of the *SCAN family.
type scanOpts struct {
	match string
	count int64
	typ   string // SCAN only
}

// parseScanArgs parses MATCH/COUNT/TYPE tails (TYPE only when allowType).
func parseScanArgs(args []resp.Value, allowType bool) (scanOpts, resp.Value) {
	o := scanOpts{count: 10}
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "MATCH":
			if i+1 >= len(args) {
				return o, syntaxErr()
			}
			o.match = args[i+1].Str
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return o, syntaxErr()
			}
			n, ok := parseIntArg(args[i+1].Str, "scan")
			if !ok {
				return o, notIntegerErr()
			}
			if n <= 0 {
				return o, resp.Value{Type: resp.Error, Str: "ERR COUNT can't be negative"}
			}
			o.count = n
			i++
		case "TYPE":
			if !allowType || i+1 >= len(args) {
				return o, syntaxErr()
			}
			o.typ = strings.ToLower(args[i+1].Str)
			i++
		default:
			return o, syntaxErr()
		}
	}
	return o, resp.Value{}
}

// scanPage returns the [cursor, COUNT) window of sorted (already filtered by
// type/elements) keys, glob-filtered afterwards like Redis (MATCH is applied
// to the scanned window, so a page may return fewer elements than COUNT).
// cursor past the end yields the terminating (0, []) page.
func scanPage(sorted []string, cursor uint64, count int64, match string) (uint64, []string) {
	start := int(cursor)
	if start > len(sorted) {
		start = len(sorted)
	}
	end := start + int(count)
	if end > len(sorted) {
		end = len(sorted)
	}
	var out []string
	for _, k := range sorted[start:end] {
		if match == "" || globMatch(match, k) {
			out = append(out, k)
		}
	}
	if end >= len(sorted) {
		return 0, out
	}
	return uint64(end), out
}

// scanReply wraps a page as the two-element [cursor, elements] array.
func scanReply(next uint64, elems []resp.Value) resp.Value {
	return resp.Value{Type: resp.Array, Arr: []resp.Value{
		{Type: resp.BulkString, Str: strconv.FormatUint(next, 10)},
		{Type: resp.Array, Arr: elems},
	}}
}

func parseScanCursor(args []resp.Value, minArgs int, name string) (uint64, []resp.Value, resp.Value) {
	if len(args) < minArgs {
		return 0, nil, wrongArgs(name)
	}
	cursor, err := strconv.ParseUint(args[0].Str, 10, 64)
	if err != nil {
		return 0, nil, resp.Value{Type: resp.Error, Str: "ERR invalid cursor"}
	}
	return cursor, args[1:], resp.Value{}
}

// cmdScan handles SCAN cursor [MATCH pat] [COUNT n] [TYPE t].
func (s *Server) cmdScan(args []resp.Value) resp.Value {
	cursor, rest, errv := parseScanCursor(args, 1, "scan")
	if errv.Type == resp.Error {
		return errv
	}
	o, errv := parseScanArgs(rest, true)
	if errv.Type == resp.Error {
		return errv
	}
	keys := s.store.ScanAll(o.typ)
	next, page := scanPage(keys, cursor, o.count, o.match)
	arr := make([]resp.Value, len(page))
	for i, k := range page {
		arr[i] = resp.Value{Type: resp.BulkString, Str: k}
	}
	return scanReply(next, arr)
}

// cmdSScan handles SSCAN key cursor [MATCH pat] [COUNT n]: members of one
// set, sorted for a deterministic traversal (Redis makes no order promise).
func (s *Server) cmdSScan(args []resp.Value) resp.Value {
	cursor, rest, errv := parseScanCursor(args[1:], 1, "sscan")
	if errv.Type == resp.Error {
		return errv
	}
	o, errv := parseScanArgs(rest, false)
	if errv.Type == resp.Error {
		return errv
	}
	members, err := s.store.SetMembers(args[0].Str)
	if err != nil {
		return errReply(err)
	}
	sort.Strings(members)
	next, page := scanPage(members, cursor, o.count, o.match)
	return scanReply(next, bulkArray(page).Arr)
}

// cmdHScan handles HSCAN key cursor [MATCH pat] [COUNT n]: a flat
// [field, value, ...] array in insertion order (listpack-like), MATCH applies
// to field names.
func (s *Server) cmdHScan(args []resp.Value) resp.Value {
	cursor, rest, errv := parseScanCursor(args[1:], 1, "hscan")
	if errv.Type == resp.Error {
		return errv
	}
	o, errv := parseScanArgs(rest, false)
	if errv.Type == resp.Error {
		return errv
	}
	pairs, err := s.store.HashGetAll(args[0].Str)
	if err != nil {
		return errReply(err)
	}
	fields := make([]string, 0, len(pairs))
	byField := make(map[string]string, len(pairs))
	for _, p := range pairs {
		fields = append(fields, p[0])
		byField[p[0]] = p[1]
	}
	next, page := scanPage(fields, cursor, o.count, o.match)
	arr := make([]resp.Value, 0, 2*len(page))
	for _, f := range page {
		arr = append(arr,
			resp.Value{Type: resp.BulkString, Str: f},
			resp.Value{Type: resp.BulkString, Str: byField[f]})
	}
	return scanReply(next, arr)
}

// cmdZScan handles ZSCAN key cursor [MATCH pat] [COUNT n]: a flat
// [member, score, ...] array in skip-list order (score asc, member asc).
func (s *Server) cmdZScan(args []resp.Value) resp.Value {
	cursor, rest, errv := parseScanCursor(args[1:], 1, "zscan")
	if errv.Type == resp.Error {
		return errv
	}
	o, errv := parseScanArgs(rest, false)
	if errv.Type == resp.Error {
		return errv
	}
	items, err := s.store.ZRange(args[0].Str, 0, -1, false)
	if err != nil {
		return errReply(err)
	}
	members := make([]string, 0, len(items))
	byMember := make(map[string]float64, len(items))
	for _, it := range items {
		members = append(members, it.Member)
		byMember[it.Member] = it.Score
	}
	next, page := scanPage(members, cursor, o.count, o.match)
	arr := make([]resp.Value, 0, 2*len(page))
	for _, m := range page {
		arr = append(arr,
			resp.Value{Type: resp.BulkString, Str: m},
			resp.Value{Type: resp.BulkString, Str: formatScore(byMember[m])})
	}
	return scanReply(next, arr)
}

// cmdObject handles OBJECT ENCODING key (the only subcommand implemented).
// A missing key replies null; the encoding names map our storage structures
// onto the Redis 7 vocabulary (see store.Encoding).
func (s *Server) cmdObject(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return wrongArgs("object")
	}
	switch strings.ToUpper(args[0].Str) {
	case "ENCODING":
		if len(args) != 2 {
			return wrongArgs("object")
		}
		enc, ok := s.store.Encoding(args[1].Str)
		if !ok {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: enc}
	default:
		return resp.Value{Type: resp.Error, Str: "ERR Unknown subcommand or wrong number of arguments for '" +
			strings.ToLower(args[0].Str) + "'. Try OBJECT HELP."}
	}
}

// cmdLMove handles LMOVE src dst LEFT|RIGHT LEFT|RIGHT: one atomic
// pop+push (store lock covers both keys). A missing src replies null.
func (s *Server) cmdLMove(args []resp.Value) resp.Value {
	if len(args) != 4 {
		return wrongArgs("lmove")
	}
	srcLeft, ok := sideArg(args[2], "lmove")
	if !ok {
		return syntaxErr()
	}
	dstLeft, ok := sideArg(args[3], "lmove")
	if !ok {
		return syntaxErr()
	}
	val, moved, err := s.store.ListMove(args[0].Str, args[1].Str, srcLeft, dstLeft)
	if err != nil {
		return errReply(err)
	}
	if !moved {
		return resp.Value{Type: resp.BulkString, Null: true}
	}
	return resp.Value{Type: resp.BulkString, Str: val}
}

// sideArg parses the LEFT|RIGHT argument of LMOVE.
func sideArg(a resp.Value, _ string) (bool, bool) {
	switch strings.ToUpper(a.Str) {
	case "LEFT":
		return true, true
	case "RIGHT":
		return false, true
	}
	return false, false
}

// cmdLInsert handles LINSERT key BEFORE|AFTER pivot element: new length as
// an Integer; 0 = no key, -1 = pivot not found.
func (s *Server) cmdLInsert(args []resp.Value) resp.Value {
	if len(args) != 4 {
		return wrongArgs("linsert")
	}
	var before bool
	switch strings.ToUpper(args[1].Str) {
	case "BEFORE":
		before = true
	case "AFTER":
		before = false
	default:
		return syntaxErr()
	}
	n, err := s.store.ListInsert(args[0].Str, before, args[2].Str, args[3].Str)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdLPos handles LPOS key element [RANK rank] [COUNT count] [MAXLEN num].
// Without COUNT a single Integer reply (null when absent); with COUNT an
// array. rank==0 is rejected with the Redis wording; count<=0 with the
// Redis 6.2 wording (negative COUNTs are not implemented).
func (s *Server) cmdLPos(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("lpos")
	}
	rank, count, maxLen := int64(1), int64(-1), int64(0)
	withCount := false
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "RANK":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			n, ok := parseIntArg(args[i+1].Str, "lpos")
			if !ok {
				return notIntegerErr()
			}
			if n == 0 {
				return resp.Value{Type: resp.Error, Str: "ERR RANK can't be zero. Use 1 to start searching " +
					"from the first matching element in the head. Use -1 to start searching from the last " +
					"matching element in the tail. Alternatively, a zero means the same as 1."}
			}
			rank = n
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			n, ok := parseIntArg(args[i+1].Str, "lpos")
			if !ok {
				return notIntegerErr()
			}
			if n <= 0 {
				return resp.Value{Type: resp.Error, Str: "ERR COUNT can't be negative"}
			}
			count, withCount = n, true
			i++
		case "MAXLEN":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			n, ok := parseIntArg(args[i+1].Str, "lpos")
			if !ok {
				return notIntegerErr()
			}
			if n < 0 {
				return resp.Value{Type: resp.Error, Str: "ERR MAXLEN can't be negative"}
			}
			maxLen = n
			i++
		default:
			return syntaxErr()
		}
	}
	matches, err := s.store.ListPos(args[0].Str, args[1].Str, rank, count, maxLen)
	if err != nil {
		return errReply(err)
	}
	if !withCount {
		if len(matches) == 0 {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.Integer, Num: matches[0]}
	}
	return intArray(matches)
}

func intArray(nums []int64) resp.Value {
	arr := make([]resp.Value, len(nums))
	for i, n := range nums {
		arr[i] = resp.Value{Type: resp.Integer, Num: n}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}
