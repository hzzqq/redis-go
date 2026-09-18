// Package server implements a Redis-compatible TCP server (Phase 1).
package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// Server holds the in-memory store and dispatches commands.
type Server struct {
	store *store.Store
}

// New returns a ready-to-serve Server.
func New() *Server {
	return &Server{store: store.New()}
}

// Listen accepts connections on addr (e.g. ":6379") until an error occurs.
func (s *Server) Listen(addr string) error {
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
		reply := s.dispatch(v)
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

func isQuit(v resp.Value) bool {
	return v.Type == resp.Array && len(v.Arr) > 0 &&
		strings.EqualFold(v.Arr[0].Str, "QUIT")
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
		val, ok := s.store.Get(args[0].Str)
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
	case "FLUSHALL":
		s.store = store.New()
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
	ttl := time.Duration(0)
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i].Str) {
		case "EX":
			if i+1 >= len(args) {
				return syntaxErr()
			}
			sec, err := strconv.ParseInt(args[i+1].Str, 10, 64)
			if err != nil || sec <= 0 {
				return resp.Value{Type: resp.Error, Str: "ERR invalid expire time in 'set' command"}
			}
			ttl = time.Duration(sec) * time.Second
			i++
		default:
			return syntaxErr()
		}
	}
	s.store.Set(key, val, ttl)
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
		return resp.Value{Type: resp.Error, Str: "ERR value is not an integer or out of range"}
	}
	if s.store.Expire(args[0].Str, time.Duration(sec)*time.Second) {
		return resp.Value{Type: resp.Integer, Num: 1}
	}
	return resp.Value{Type: resp.Integer, Num: 0}
}

// cmdAppend 处理 APPEND key value：返回拼接后的新长度（Integer）。
func (s *Server) cmdAppend(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return wrongArgs("append")
	}
	n := s.store.Append(args[0].Str, args[1].Str)
	return resp.Value{Type: resp.Integer, Num: n}
}

// cmdIncrBy 处理 INCR/DECR（固定 delta，无额外参数）。cmdName 用于错误消息。
func (s *Server) cmdIncrBy(args []resp.Value, delta int64, cmdName string) resp.Value {
	if len(args) != 1 {
		return wrongArgs(cmdName)
	}
	v, err := s.store.IncrBy(args[0].Str, delta)
	if err != nil {
		return resp.Value{Type: resp.Error, Str: err.Error()}
	}
	return resp.Value{Type: resp.Integer, Num: v}
}

// cmdIncrByWithAmount 处理 INCRBY key increment：解析第二参数为 int64 后调用 IncrBy。
func (s *Server) cmdIncrByWithAmount(args []resp.Value, cmdName string) resp.Value {
	if len(args) != 2 {
		return wrongArgs(cmdName)
	}
	delta, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.Value{Type: resp.Error, Str: "ERR value is not an integer or out of range"}
	}
	v, err := s.store.IncrBy(args[0].Str, delta)
	if err != nil {
		return resp.Value{Type: resp.Error, Str: err.Error()}
	}
	return resp.Value{Type: resp.Integer, Num: v}
}

func wrongArgs(cmd string) resp.Value {
	return resp.Value{Type: resp.Error, Str: fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd)}
}

func syntaxErr() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR syntax error"}
}

// ensure io is used (kept for symmetry with future reader refactors)
var _ = io.EOF
