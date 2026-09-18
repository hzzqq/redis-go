// Phase 6 新命令：批量读/写、ZSet 范围族、随机成员、RDB 快照。
package server

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// cmdMGet handles MGET key [key ...]: one bulk per key; missing keys AND
// wrong-type keys reply null (Redis MGET never fails with WRONGTYPE).
func (s *Server) cmdMGet(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return wrongArgs("mget")
	}
	keys := fieldsOf(args)
	vals := s.store.MGet(keys)
	arr := make([]resp.Value, len(keys))
	for i, v := range vals {
		if v == nil {
			arr[i] = resp.Value{Type: resp.BulkString, Null: true}
			continue
		}
		arr[i] = resp.Value{Type: resp.BulkString, Str: *v}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

// cmdMSet handles MSET key val [key val ...]: sets every pair (overwriting
// any previous type, Redis semantics) in one store pass and replies OK.
// The AOF receives the MSET verbatim (canonicalWrite default branch).
func (s *Server) cmdMSet(args []resp.Value) resp.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return wrongArgs("mset")
	}
	pairs := make([][2]string, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs = append(pairs, [2]string{args[i].Str, args[i+1].Str})
	}
	s.store.MSet(pairs)
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

// cmdZRangeByScore handles ZRANGEBYSCORE/ZREVRANGEBYSCORE key min max
// [WITHSCORES] [LIMIT offset count]. In the REV form the first bound is the
// MAXIMUM (Redis signature) and iteration is score-descending.
func (s *Server) cmdZRangeByScore(args []resp.Value, rev bool) resp.Value {
	name := "zrangebyscore"
	if rev {
		name = "zrevrangebyscore"
	}
	if len(args) < 3 {
		return wrongArgs(name)
	}
	notFloat := resp.Value{Type: resp.Error, Str: "ERR min or max is not a float"}
	first, err := store.ParseZBound(args[1].Str)
	if err != nil {
		return notFloat
	}
	second, err := store.ParseZBound(args[2].Str)
	if err != nil {
		return notFloat
	}
	minB, maxB := first, second
	if rev {
		minB, maxB = second, first
	}
	withScores := false
	offset, count := int64(0), int64(-1)
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "WITHSCORES":
			if withScores {
				return syntaxErr()
			}
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return syntaxErr()
			}
			off, ok := parseIntArg(args[i+1].Str, name)
			if !ok || off < 0 {
				return notIntegerErr()
			}
			cnt, ok := parseIntArg(args[i+2].Str, name)
			if !ok {
				return notIntegerErr()
			}
			if cnt < 0 {
				return resp.Value{Type: resp.Error,
					Str: "ERR value is out of range, must be positive"}
			}
			offset, count = off, cnt
			i += 2
		default:
			return syntaxErr()
		}
	}
	var items []store.ZItem
	if rev {
		items, err = s.store.ZRevRangeByScore(args[0].Str, minB, maxB, offset, count)
	} else {
		items, err = s.store.ZRangeByScore(args[0].Str, minB, maxB, offset, count)
	}
	if err != nil {
		return errReply(err)
	}
	out := make([]string, 0, len(items)*2)
	for _, it := range items {
		out = append(out, it.Member)
		if withScores {
			out = append(out, formatScore(it.Score))
		}
	}
	return bulkArray(out)
}

// cmdZRangeByLex handles ZRANGEBYLEX/ZREVRANGEBYLEX key min/max [LIMIT ...].
// In the REV form the first bound is the LEXICALLY LARGEST endpoint. Bounds
// use Redis lex syntax: "-" / "+" / "[member" (inclusive) / "(member"
// (exclusive). Lex ranges are only meaningful when all members share one
// score (Redis documents the same caveat).
func (s *Server) cmdZRangeByLex(args []resp.Value, rev bool) resp.Value {
	name := "zrangebylex"
	if rev {
		name = "zrevrangebylex"
	}
	if len(args) < 3 {
		return wrongArgs(name)
	}
	badBound := resp.Value{Type: resp.Error, Str: "ERR min or max not valid string range item"}
	first, err := store.ParseZLexBound(args[1].Str)
	if err != nil {
		return badBound
	}
	second, err := store.ParseZLexBound(args[2].Str)
	if err != nil {
		return badBound
	}
	minB, maxB := first, second
	if rev {
		minB, maxB = second, first
	}
	offset, count := int64(0), int64(-1)
	for i := 3; i < len(args); i++ {
		if !strings.EqualFold(args[i].Str, "LIMIT") {
			return syntaxErr()
		}
		if i+2 >= len(args) {
			return syntaxErr()
		}
		off, ok := parseIntArg(args[i+1].Str, name)
		if !ok || off < 0 {
			return notIntegerErr()
		}
		cnt, ok := parseIntArg(args[i+2].Str, name)
		if !ok {
			return notIntegerErr()
		}
		if cnt < 0 {
			return resp.Value{Type: resp.Error,
				Str: "ERR value is out of range, must be positive"}
		}
		offset, count = off, cnt
		i += 2
	}
	var members []string
	if rev {
		members, err = s.store.ZRevRangeByLex(args[0].Str, minB, maxB, offset, count)
	} else {
		members, err = s.store.ZRangeByLex(args[0].Str, minB, maxB, offset, count)
	}
	if err != nil {
		return errReply(err)
	}
	return bulkArray(members)
}

// cmdZRandMember handles ZRANDMEMBER key [count [WITHSCORES]]: without count
// a single bulk (null when the key is missing); with count an array of
// members, interleaved with scores when WITHSCORES is given.
func (s *Server) cmdZRandMember(args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 3 {
		return wrongArgs("zrandmember")
	}
	withCount := len(args) >= 2
	var count int64
	if withCount {
		var err error
		count, err = strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return notIntegerErr()
		}
	}
	withScores := false
	if len(args) == 3 {
		if !strings.EqualFold(args[2].Str, "WITHSCORES") {
			return syntaxErr()
		}
		withScores = true
	}
	items, err := s.store.ZRandMember(args[0].Str, count, withCount)
	if err != nil {
		return errReply(err)
	}
	if !withCount {
		if len(items) == 0 {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: items[0].Member}
	}
	out := make([]string, 0, len(items)*2)
	for _, it := range items {
		out = append(out, it.Member)
		if withScores {
			out = append(out, formatScore(it.Score))
		}
	}
	return bulkArray(out)
}

// saveNow exports the current store and writes the RDB file. A single
// read-lock snapshot is a consistent point-in-time on its own — unlike the
// AOF rewrite, an RDB file is not composed with an existing log, so no
// applyMu is needed. Callers hold rdbMu.
func (s *Server) saveNow() error {
	entries := s.store.Export()
	if err := persist.SaveRDB(s.rdbPath, entries); err != nil {
		return err
	}
	s.lastSave.Store(time.Now().Unix())
	return nil
}

func rdbDisabledErr() resp.Value {
	return errReply(errors.New(
		"ERR RDB snapshot is disabled: start the server with -rdb <path>"))
}

// cmdSave handles SAVE: a synchronous RDB snapshot; the reply arrives after
// the file is complete (Redis SAVE semantics).
func (s *Server) cmdSave() resp.Value {
	if s.rdbPath == "" {
		return rdbDisabledErr()
	}
	s.rdbMu.Lock()
	defer s.rdbMu.Unlock()
	if err := s.saveNow(); err != nil {
		return resp.Value{Type: resp.Error, Str: "ERR " + err.Error()}
	}
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

// cmdBGSave handles BGSAVE: replies immediately and snapshots in a
// background goroutine (no fork on this runtime; the file lands atomically
// via rename, so a concurrent reader sees either the old or the new file).
func (s *Server) cmdBGSave() resp.Value {
	if s.rdbPath == "" {
		return rdbDisabledErr()
	}
	go func() {
		s.rdbMu.Lock()
		defer s.rdbMu.Unlock()
		if err := s.saveNow(); err != nil {
			log.Printf("bgsave: %v", err)
		}
	}()
	return resp.Value{Type: resp.SimpleString, Str: "Background saving started"}
}
