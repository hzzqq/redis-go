// Package server implements a Redis-compatible TCP server.
package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/hzzqq/redis-go/internal/persist"
	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// Server holds the in-memory store and dispatches commands.
type Server struct {
	store *store.Store
	aof   *persist.AOF // nil = persistence disabled
	addr  string       // listen address ("" until Listen is called)
}

// New returns a ready-to-serve in-memory Server.
func New() *Server {
	return &Server{store: store.New()}
}

// NewWithAOF returns a Server backed by an append-only file at path.
// Any existing file is replayed first (a truncated tail is tolerated:
// commands parsed before the truncation point are applied), then the file is
// reopened for appending.
func NewWithAOF(path string) (*Server, error) {
	cmds, err := persist.Load(path)
	if err != nil {
		log.Printf("warning: %v", err)
	}
	s := &Server{store: store.New()}
	for _, v := range cmds {
		s.dispatch(v)
	}
	a, err := persist.Open(path)
	if err != nil {
		return nil, err
	}
	s.aof = a
	return s, nil
}

// Close releases the AOF file handle, if any.
func (s *Server) Close() error {
	if s.aof == nil {
		return nil
	}
	return s.aof.Close()
}

// Listen accepts connections on addr (e.g. ":6379") until an error occurs.
func (s *Server) Listen(addr string) error {
	s.addr = addr
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return err
		}
		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	r := resp.NewReader(conn)
	w := bufio.NewWriter(conn)
	for {
		v, err := r.Read()
		if err != nil {
			return
		}
		reply := s.apply(v)
		if err := resp.WriteValue(w, reply); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
		if reply.Type == resp.SimpleString && reply.Str == "OK" && isQuit(v) {
			return
		}
	}
}

// apply executes v and, when AOF is enabled and the command succeeded,
// appends the canonical form of the write command to the file before the
// reply is returned (Redis executes, then propagates, then replies).
func (s *Server) apply(v resp.Value) resp.Value {
	reply := s.dispatch(v)
	if s.aof == nil || reply.Type == resp.Error {
		return reply
	}
	if canon, ok := canonicalWrite(v); ok {
		if err := s.aof.Log(canon); err != nil {
			return resp.Value{Type: resp.Error, Str: "ERR AOF write error: " + err.Error()}
		}
	}
	return reply
}

func isQuit(v resp.Value) bool {
	return v.Type == resp.Array && len(v.Arr) > 0 &&
		strings.EqualFold(v.Arr[0].Str, "QUIT")
}

// writeCmds is the set of commands that mutate state and therefore get
// logged to the AOF.
var writeCmds = map[string]bool{
	"SET": true, "SETEX": true, "DEL": true, "EXPIRE": true, "PEXPIREAT": true,
	"APPEND": true, "INCR": true, "DECR": true, "INCRBY": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true, "LSET": true, "LTRIM": true,
	"HSET": true, "HDEL": true, "HINCRBY": true,
	"SADD": true, "SREM": true,
	"FLUSHALL": true,
}

// canonicalWrite maps a successful write command to its persisted form.
// Relative TTLs are rewritten to absolute-millisecond forms so that state is
// correct after a restart regardless of elapsed time (same approach as Redis
// AOF propagation): SETEX → SET key val PXAT ms, EXPIRE → PEXPIREAT key ms,
// SET key val EX/PX n → SET key val PXAT ms. Other write commands are stored
// verbatim. The second return value is false for non-write commands.
func canonicalWrite(v resp.Value) (resp.Value, bool) {
	if v.Type != resp.Array || len(v.Arr) == 0 {
		return resp.Value{}, false
	}
	cmd := strings.ToUpper(v.Arr[0].Str)
	if !writeCmds[cmd] {
		return resp.Value{}, false
	}
	args := v.Arr[1:]
	switch cmd {
	case "SETEX":
		if len(args) != 3 {
			return v, true
		}
		sec, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil || sec <= 0 {
			return v, true
		}
		return respCmd("SET", args[0].Str, args[2].Str,
			"PXAT", msAt(time.Now().Add(time.Duration(sec)*time.Second))), true
	case "EXPIRE":
		if len(args) != 2 {
			return v, true
		}
		sec, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return v, true
		}
		return respCmd("PEXPIREAT", args[0].Str,
			msAt(time.Now().Add(time.Duration(sec)*time.Second))), true
	case "SET":
		out := make([]resp.Value, len(v.Arr))
		out[0] = resp.Value{Type: resp.BulkString, Str: "SET"}
		copy(out[1:], args)
		for i := 1; i < len(out)-1; i++ {
			opt := strings.ToUpper(out[i].Str)
			if opt != "EX" && opt != "PX" {
				continue
			}
			n, err := strconv.ParseInt(out[i+1].Str, 10, 64)
			if err != nil || n <= 0 {
				return v, true // replay would fail identically; keep verbatim
			}
			unit := time.Second
			if opt == "PX" {
				unit = time.Millisecond
			}
			out[i] = resp.Value{Type: resp.BulkString, Str: "PXAT"}
			out[i+1] = resp.Value{Type: resp.BulkString, Str: msAt(time.Now().Add(time.Duration(n) * unit))}
			break
		}
		return resp.Value{Type: resp.Array, Arr: out}, true
	default:
		return v, true
	}
}

func msAt(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

// respCmd builds an array-of-bulk-strings RESP value from plain strings.
func respCmd(parts ...string) resp.Value {
	arr := make([]resp.Value, len(parts))
	for i, p := range parts {
		arr[i] = resp.Value{Type: resp.BulkString, Str: p}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

func (s *Server) dispatch(v resp.Value) resp.Value {
	if v.Type != resp.Array || len(v.Arr) == 0 {
		return resp.Value{Type: resp.Error, Str: "ERR Protocol error: expected array command"}
	}
	cmd := strings.ToUpper(v.Arr[0].Str)
	args := v.Arr[1:]
	switch cmd {
	case "PING":
		if len(args) == 0 {
			return resp.Value{Type: resp.SimpleString, Str: "PONG"}
		}
		return resp.Value{Type: resp.BulkString, Str: args[0].Str}
	case "ECHO":
		if len(args) != 1 {
			return wrongArgs("echo")
		}
		return resp.Value{Type: resp.BulkString, Str: args[0].Str}
	case "GET":
		if len(args) != 1 {
			return wrongArgs("get")
		}
		val, ok, err := s.store.Get(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		if !ok {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: val}
	case "SET":
		return s.cmdSet(args)
	case "SETEX":
		return s.cmdSetEX(args)
	case "DEL":
		var n int64
		for _, a := range args {
			if s.store.Del(a.Str) {
				n++
			}
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "EXISTS":
		var n int64
		for _, a := range args {
			if s.store.Exists(a.Str) {
				n++
			}
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "EXPIRE":
		return s.cmdExpire(args)
	case "PEXPIREAT":
		return s.cmdPExpireAt(args)
	case "TTL":
		if len(args) != 1 {
			return wrongArgs("ttl")
		}
		rem, ok := s.store.TTL(args[0].Str)
		if !ok {
			return resp.Value{Type: resp.Integer, Num: -2}
		}
		if rem < 0 {
			return resp.Value{Type: resp.Integer, Num: -1}
		}
		return resp.Value{Type: resp.Integer, Num: rem}
	case "APPEND":
		return s.cmdAppend(args)
	case "INCR":
		return s.cmdIncrBy(args, 1, "incr")
	case "DECR":
		return s.cmdIncrBy(args, -1, "decr")
	case "INCRBY":
		return s.cmdIncrByWithAmount(args, "incrby")
	case "LPUSH":
		return s.cmdListPush(args, true)
	case "RPUSH":
		return s.cmdListPush(args, false)
	case "LPOP":
		return s.cmdListPop(args, true)
	case "RPOP":
		return s.cmdListPop(args, false)
	case "LLEN":
		if len(args) != 1 {
			return wrongArgs("llen")
		}
		n, err := s.store.ListLen(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "LRANGE":
		if len(args) != 3 {
			return wrongArgs("lrange")
		}
		start, ok := parseIntArg(args[1].Str, "lrange")
		if !ok {
			return notIntegerErr()
		}
		stop, ok := parseIntArg(args[2].Str, "lrange")
		if !ok {
			return notIntegerErr()
		}
		items, err := s.store.ListRange(args[0].Str, start, stop)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(items)
	case "LINDEX":
		if len(args) != 2 {
			return wrongArgs("lindex")
		}
		idx, ok := parseIntArg(args[1].Str, "lindex")
		if !ok {
			return notIntegerErr()
		}
		val, found, err := s.store.ListIndex(args[0].Str, idx)
		if err != nil {
			return errReply(err)
		}
		if !found {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: val}
	case "LSET":
		if len(args) != 3 {
			return wrongArgs("lset")
		}
		idx, ok := parseIntArg(args[1].Str, "lset")
		if !ok {
			return notIntegerErr()
		}
		if err := s.store.ListSet(args[0].Str, idx, args[2].Str); err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "LTRIM":
		if len(args) != 3 {
			return wrongArgs("ltrim")
		}
		start, ok := parseIntArg(args[1].Str, "ltrim")
		if !ok {
			return notIntegerErr()
		}
		stop, ok := parseIntArg(args[2].Str, "ltrim")
		if !ok {
			return notIntegerErr()
		}
		if err := s.store.ListTrim(args[0].Str, start, stop); err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "HSET":
		return s.cmdHSet(args)
	case "HGET":
		if len(args) != 2 {
			return wrongArgs("hget")
		}
		val, ok, err := s.store.HashGet(args[0].Str, args[1].Str)
		if err != nil {
			return errReply(err)
		}
		if !ok {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: val}
	case "HGETALL":
		if len(args) != 1 {
			return wrongArgs("hgetall")
		}
		pairs, err := s.store.HashGetAll(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		items := make([]string, 0, len(pairs)*2)
		for _, p := range pairs {
			items = append(items, p[0], p[1])
		}
		return bulkArray(items)
	case "HDEL":
		if len(args) < 2 {
			return wrongArgs("hdel")
		}
		n, err := s.store.HashDel(args[0].Str, fieldsOf(args[1:]))
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "HLEN":
		if len(args) != 1 {
			return wrongArgs("hlen")
		}
		n, err := s.store.HashLen(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "HEXISTS":
		if len(args) != 2 {
			return wrongArgs("hexists")
		}
		exists, err := s.store.HashExists(args[0].Str, args[1].Str)
		if err != nil {
			return errReply(err)
		}
		if exists {
			return resp.Value{Type: resp.Integer, Num: 1}
		}
		return resp.Value{Type: resp.Integer, Num: 0}
	case "HKEYS":
		if len(args) != 1 {
			return wrongArgs("hkeys")
		}
		keys, err := s.store.HashKeys(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(keys)
	case "HVALS":
		if len(args) != 1 {
			return wrongArgs("hvals")
		}
		vals, err := s.store.HashVals(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(vals)
	case "HINCRBY":
		if len(args) != 3 {
			return wrongArgs("hincrby")
		}
		delta, ok := parseIntArg(args[2].Str, "hincrby")
		if !ok {
			return notIntegerErr()
		}
		n, err := s.store.HashIncrBy(args[0].Str, args[1].Str, delta)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "SADD":
		return s.cmdSetMembers(args, true)
	case "SREM":
		return s.cmdSetMembers(args, false)
	case "SISMEMBER":
		if len(args) != 2 {
			return wrongArgs("sismember")
		}
		exists, err := s.store.SetIsMember(args[0].Str, args[1].Str)
		if err != nil {
			return errReply(err)
		}
		if exists {
			return resp.Value{Type: resp.Integer, Num: 1}
		}
		return resp.Value{Type: resp.Integer, Num: 0}
	case "SMEMBERS":
		if len(args) != 1 {
			return wrongArgs("smembers")
		}
		members, err := s.store.SetMembers(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return bulkArray(members)
	case "SCARD":
		if len(args) != 1 {
			return wrongArgs("scard")
		}
		n, err := s.store.SetCard(args[0].Str)
		if err != nil {
			return errReply(err)
		}
		return resp.Value{Type: resp.Integer, Num: n}
	case "TYPE":
		if len(args) != 1 {
			return wrongArgs("type")
		}
		return resp.Value{Type: resp.SimpleString, Str: s.store.Type(args[0].Str)}
	case "DBSIZE":
		if len(args) != 0 {
			return wrongArgs("dbsize")
		}
		return resp.Value{Type: resp.Integer, Num: s.store.DBSize()}
	case "INFO":
		return s.cmdInfo(args)
	case "CONFIG":
		return s.cmdConfig(args)
	case "FLUSHALL":
		s.store.Flush()
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "COMMAND":
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	case "QUIT":
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	default:
		return resp.Value{Type: resp.Error, Str: fmt.Sprintf("ERR unknown command '%s'", cmd)}
	}
}

func (s *Server) cmdSet(args []resp.Value) resp.Value {
	if len(args) < 2 {
		return wrongArgs("set")
	}
	key, val := args[0].Str, args[1].Str
	var exp time.Time // zero = no expiry
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "EX", "PX", "PXAT":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			n, err := strconv.ParseInt(args[i+1].Str, 10, 64)
			if err != nil {
				return resp.Value{Type: resp.Error, Str: "ERR invalid expire time in 'set' command"}
			}
			opt := strings.ToUpper(args[i].Str)
			if opt == "PXAT" {
				exp = time.UnixMilli(n) // past timestamps delete the key
			} else {
				if n <= 0 {
					return resp.Value{Type: resp.Error, Str: "ERR invalid expire time in 'set' command"}
				}
				unit := time.Second
				if opt == "PX" {
					unit = time.Millisecond
				}
				exp = time.Now().Add(time.Duration(n) * unit)
			}
			i++
		default:
			return syntaxErr()
		}
	}
	s.store.SetAt(key, val, exp)
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

func (s *Server) cmdSetEX(args []resp.Value) resp.Value {
	if len(args) != 3 {
		return wrongArgs("setex")
	}
	sec, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil || sec <= 0 {
		return resp.Value{Type: resp.Error, Str: "ERR invalid expire time in 'setex' command"}
	}
	s.store.Set(args[0].Str, args[2].Str, time.Duration(sec)*time.Second)
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

func (s *Server) cmdExpire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("expire")
	}
	sec, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	if s.store.Expire(args[0].Str, time.Duration(sec)*time.Second) {
		return resp.Value{Type: resp.Integer, Num: 1}
	}
	return resp.Value{Type: resp.Integer, Num: 0}
}

// cmdPExpireAt handles PEXPIREAT key ms (absolute expiry; past deletes key).
func (s *Server) cmdPExpireAt(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("pexpireat")
	}
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	if s.store.ExpireAt(args[0].Str, time.UnixMilli(ms)) {
		return resp.Value{Type: resp.Integer, Num: 1}
	}
	return resp.Value{Type: resp.Integer, Num: 0}
}

// cmdAppend handles APPEND key value: returns the new length (Integer).
func (s *Server) cmdAppend(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("append")
	}
	n, err := s.store.Append(args[0].Str, args[1].Str)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdIncrBy handles INCR/DECR (fixed delta, no extra args). cmdName is used
// for error messages.
func (s *Server) cmdIncrBy(args []resp.Value, delta int64, cmdName string) resp.Value {
	if len(args) != 1 {
		return wrongArgs(cmdName)
	}
	v, err := s.store.IncrBy(args[0].Str, delta)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: v}
}

// cmdIncrByWithAmount handles INCRBY key increment.
func (s *Server) cmdIncrByWithAmount(args []resp.Value, cmdName string) resp.Value {
	if len(args) != 2 {
		return wrongArgs(cmdName)
	}
	delta, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return notIntegerErr()
	}
	v, err := s.store.IncrBy(args[0].Str, delta)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: v}
}

// cmdListPush handles LPUSH/RPUSH key val [val...]: returns the new length.
func (s *Server) cmdListPush(args []resp.Value, front bool) resp.Value {
	if len(args) < 2 {
		return wrongArgs("lpush/rpush")
	}
	vals := make([]string, len(args)-1)
	for i, a := range args[1:] {
		vals[i] = a.Str
	}
	n, err := s.store.ListPush(args[0].Str, front, vals...)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdListPop handles LPOP/RPOP key [count]. Without count the reply is a
// single bulk (null when missing); with count it is an array (possibly empty).
func (s *Server) cmdListPop(args []resp.Value, front bool) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return wrongArgs("lpop/rpop")
	}
	var count int64 = 1
	withCount := len(args) == 2
	if withCount {
		var err error
		count, err = strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return notIntegerErr()
		}
	}
	popped, err := s.store.ListPop(args[0].Str, front, count)
	if err != nil {
		return errReply(err)
	}
	if !withCount {
		if len(popped) == 0 {
			return resp.Value{Type: resp.BulkString, Null: true}
		}
		return resp.Value{Type: resp.BulkString, Str: popped[0]}
	}
	return bulkArray(popped)
}

// cmdHSet handles HSET key field val [field val ...]: returns the number of
// newly added fields.
func (s *Server) cmdHSet(args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return wrongArgs("hset")
	}
	pairs := make([][2]string, 0, len(args)/2)
	for i := 1; i < len(args); i += 2 {
		pairs = append(pairs, [2]string{args[i].Str, args[i+1].Str})
	}
	n, err := s.store.HashSet(args[0].Str, pairs)
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

func fieldsOf(args []resp.Value) []string {
	fields := make([]string, len(args))
	for i, a := range args {
		fields[i] = a.Str
	}
	return fields
}

func parseIntArg(s, _ string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func bulkArray(items []string) resp.Value {
	arr := make([]resp.Value, len(items))
	for i, s := range items {
		arr[i] = resp.Value{Type: resp.BulkString, Str: s}
	}
	return resp.Value{Type: resp.Array, Arr: arr}
}

func errReply(err error) resp.Value {
	return resp.Value{Type: resp.Error, Str: err.Error()}
}

func notIntegerErr() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR value is not an integer or out of range"}
}

func wrongArgs(cmd string) resp.Value {
	return resp.Value{Type: resp.Error, Str: fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd)}
}

func syntaxErr() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR syntax error"}
}

// cmdSetMembers handles SADD/SREM key member [member ...]: both return the
// number of members actually added/removed.
func (s *Server) cmdSetMembers(args []resp.Value, add bool) resp.Value {
	name := "sadd"
	if !add {
		name = "srem"
	}
	if len(args) < 2 {
		return wrongArgs(name)
	}
	members := fieldsOf(args[1:])
	var n int64
	var err error
	if add {
		n, err = s.store.SetAdd(args[0].Str, members)
	} else {
		n, err = s.store.SetRem(args[0].Str, members)
	}
	if err != nil {
		return errReply(err)
	}
	return resp.Value{Type: resp.Integer, Num: n}
}

// infoSection is one block of the INFO reply (header line + fields + blank).
type infoSection struct {
	name string
	body string
}

// cmdInfo handles INFO [section]. Without an argument all sections are
// concatenated; with a section name only that block is returned (empty bulk
// for an unknown section, like Redis).
func (s *Server) cmdInfo(args []resp.Value) resp.Value {
	sections := s.infoSections()
	if len(args) > 0 {
		for _, sec := range sections {
			if strings.EqualFold(sec.name, args[0].Str) {
				return resp.Value{Type: resp.BulkString, Str: sec.body}
			}
		}
		return resp.Value{Type: resp.BulkString, Str: ""}
	}
	var b strings.Builder
	for _, sec := range sections {
		b.WriteString(sec.body)
	}
	return resp.Value{Type: resp.BulkString, Str: b.String()}
}

// infoSections builds the INFO blocks from live runtime state.
func (s *Server) infoSections() []infoSection {
	keys, expires := s.store.Stats()
	aofEnabled := "0"
	if s.aof != nil {
		aofEnabled = "1"
	}
	server := "# Server\r\n" +
		"redis_version:redis-go-0.3.0\r\n" +
		"redis_mode:standalone\r\n" +
		"os:" + runtime.GOOS + "\r\n" +
		"go_version:" + runtime.Version() + "\r\n" +
		"tcp_port:" + s.port() + "\r\n" +
		fmt.Sprintf("process_id:%d\r\n", os.Getpid()) +
		"\r\n"
	persistence := "# Persistence\r\n" +
		"aof_enabled:" + aofEnabled + "\r\n" +
		"\r\n"
	keyspace := "# Keyspace\r\n" +
		fmt.Sprintf("db0:keys=%d,expires=%d\r\n", keys, expires)
	return []infoSection{
		{"Server", server},
		{"Persistence", persistence},
		{"Keyspace", keyspace},
	}
}

// port extracts the TCP port from the listen address (default 6379).
func (s *Server) port() string {
	if s.addr == "" {
		return "6379"
	}
	if _, p, err := net.SplitHostPort(s.addr); err == nil && p != "" {
		return p
	}
	return s.addr
}

// cmdConfig handles CONFIG GET <param> with a small static registry
// (appendonly reflects the actual AOF state). CONFIG SET is rejected honestly
// because no option is runtime-mutable in redis-go yet. Unknown GET params
// yield an empty array, like Redis.
func (s *Server) cmdConfig(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return wrongArgs("config")
	}
	switch strings.ToUpper(args[0].Str) {
	case "GET":
		if len(args) != 2 {
			return wrongArgs("config")
		}
		name := strings.ToLower(args[1].Str)
		switch name {
		case "appendonly", "databases", "maxmemory", "save":
			return bulkArray([]string{name, s.configValue(name)})
		}
		return bulkArray(nil)
	case "SET":
		if len(args) < 2 {
			return wrongArgs("config")
		}
		return resp.Value{Type: resp.Error,
			Str: "ERR Unsupported CONFIG parameter: " + args[1].Str}
	default:
		return resp.Value{Type: resp.Error, Str: fmt.Sprintf(
			"ERR Unknown subcommand or wrong number of arguments for '%s'. Try CONFIG HELP.", args[0].Str)}
	}
}

// configValue resolves the CONFIG GET registry (all static except appendonly).
func (s *Server) configValue(name string) string {
	switch name {
	case "appendonly":
		if s.aof != nil {
			return "yes"
		}
		return "no"
	case "databases":
		return "16"
	case "maxmemory":
		return "0"
	case "save":
		return ""
	}
	return ""
}

// ensure io is used (kept for symmetry with future reader refactors)
var _ = io.EOF
