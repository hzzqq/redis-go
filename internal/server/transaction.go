// MULTI/EXEC/DISCARD 事务（Phase 6）。
//
// 语义对齐 Redis：
//   - MULTI 开启事务，后续命令排队（回复 +QUEUED），EXEC 一次性执行并把全部
//     结果合成一个数组返回；DISCARD 清空队列放弃事务；
//   - 队列期错误（未知命令 / 参数个数错误）立即报错并污染事务，EXEC 变为
//     EXECABORT；执行期错误（WRONGTYPE、非整数等）只出现在 EXEC 结果数组的
//     对应槽位，其余命令照常生效；
//   - MULTI/EXEC/DISCARD/QUIT 不入队（QUIT 还会清掉未执行的事务）；
//     SUBSCRIBE/UNSUBSCRIBE 不允许入队（事务本身不受污染）；
//   - 整个 EXEC 块在 applyMu 下执行，相对其他连接的写命令原子；AOF 以
//     MULTI...EXEC 块落盘，回放时未写完整的块整体丢弃（写入中途崩溃等价于
//     事务未发生）。
package server

import (
	"fmt"
	"strings"

	"github.com/hzzqq/redis-go/internal/resp"
	"github.com/hzzqq/redis-go/internal/store"
)

// cmdArity is the per-command argument count range (excluding the command
// name itself); max < 0 means unbounded. It backs queue-time validation for
// MULTI: unknown names and arity violations reply immediately and poison the
// transaction (EXEC → EXECABORT), matching Redis. Argument-shape errors
// (odd HSET pairs, bad scores, ...) are execution-time errors and surface in
// the EXEC reply array instead.
var cmdArity = map[string][2]int{
	"PING": {0, 1}, "ECHO": {1, 1}, "QUIT": {0, 0}, "COMMAND": {0, 0},
	"GET": {1, 1}, "SET": {2, -1}, "SETEX": {3, 3}, "DEL": {1, -1},
	"EXISTS": {1, -1}, "EXPIRE": {2, 2}, "PEXPIREAT": {2, 2}, "TTL": {1, 1},
	"APPEND": {2, 2}, "INCR": {1, 1}, "DECR": {1, 1}, "INCRBY": {2, 2},
	"LPUSH": {2, -1}, "RPUSH": {2, -1}, "LPOP": {1, 2}, "RPOP": {1, 2},
	"LLEN": {1, 1}, "LRANGE": {3, 3}, "LINDEX": {2, 2}, "LSET": {3, 3},
	"LTRIM": {3, 3},
	"HSET": {3, -1}, "HGET": {2, 2}, "HGETALL": {1, 1}, "HDEL": {2, -1},
	"HLEN": {1, 1}, "HEXISTS": {2, 2}, "HKEYS": {1, 1}, "HVALS": {1, 1},
	"HINCRBY": {3, 3},
	"SADD": {2, -1}, "SREM": {2, -1}, "SISMEMBER": {2, 2}, "SMEMBERS": {1, 1},
	"SCARD": {1, 1}, "SPOP": {1, 2}, "SRANDMEMBER": {1, 2},
	"SINTER": {1, -1}, "SUNION": {1, -1}, "SDIFF": {1, -1},
	"ZADD": {3, -1}, "ZSCORE": {2, 2}, "ZINCRBY": {3, 3}, "ZCARD": {1, 1},
	"ZRANK": {2, 2}, "ZREVRANK": {2, 2}, "ZCOUNT": {3, 3},
	"ZRANGE": {3, 4}, "ZREVRANGE": {3, 4}, "ZREM": {2, -1},
	"ZRANGEBYSCORE": {3, -1}, "ZREVRANGEBYSCORE": {3, -1},
	"ZRANGEBYLEX": {3, -1}, "ZREVRANGEBYLEX": {3, -1}, "ZRANDMEMBER": {1, 3},
	"MGET": {1, -1}, "MSET": {2, -1},
	"TYPE": {1, 1}, "DBSIZE": {0, 0}, "INFO": {0, 1}, "CONFIG": {2, -1},
	"BGREWRITEAOF": {0, 0}, "FLUSHALL": {0, 0},
	"SAVE": {0, 0}, "BGSAVE": {0, 0},
	"MULTI": {0, 0}, "EXEC": {0, 0}, "DISCARD": {0, 0},
	"SUBSCRIBE": {1, -1}, "UNSUBSCRIBE": {0, -1}, "PUBLISH": {2, 2},
}

// resetTxn clears the connection's transaction state.
func (cl *client) resetTxn() {
	cl.inMulti, cl.queue, cl.queueErr = false, nil, false
}

// queueForTxn handles one command inside MULTI: valid commands join the
// queue and reply +QUEUED; unknown/arity errors reply immediately and poison
// the transaction; SUBSCRIBE/UNSUBSCRIBE are rejected without poisoning.
func (s *Server) queueForTxn(cl *client, cmd string, v resp.Value) resp.Value {
	switch cmd {
	case "SUBSCRIBE", "UNSUBSCRIBE":
		return resp.Value{Type: resp.Error,
			Str: fmt.Sprintf("ERR %s is not allowed in transactions", strings.ToLower(cmd))}
	}
	arity, known := cmdArity[cmd]
	if !known {
		cl.queueErr = true
		return resp.Value{Type: resp.Error, Str: fmt.Sprintf("ERR unknown command '%s'", cmd)}
	}
	nargs := len(v.Arr) - 1
	if nargs < arity[0] || (arity[1] >= 0 && nargs > arity[1]) {
		cl.queueErr = true
		return resp.Value{Type: resp.Error,
			Str: fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd))}
	}
	cl.queue = append(cl.queue, v)
	return resp.Value{Type: resp.SimpleString, Str: "QUEUED"}
}

// execTransaction runs the queued commands as one atomic unit: the whole
// block executes under applyMu so no other client's write command can
// interleave (same guarantee as single write commands). The AOF receives a
// MULTI ... EXEC block of canonical forms; failed commands are skipped, the
// same way apply() skips them. Empty transactions log nothing.
func (s *Server) execTransaction(cl *client) resp.Value {
	defer cl.resetTxn()
	if cl.queueErr {
		return resp.Value{Type: resp.Error,
			Str: "EXECABORT Transaction discarded because of previous errors."}
	}
	if len(cl.queue) == 0 {
		return resp.Value{Type: resp.Array, Arr: []resp.Value{}}
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	replies := make([]resp.Value, 0, len(cl.queue))
	var block []resp.Value
	if s.aof != nil {
		block = append(block, respCmd("MULTI"))
	}
	for _, q := range cl.queue {
		r := s.dispatch(q)
		if isWriteCmd(q) && s.aof != nil && r.Type != resp.Error {
			if canon, ok := canonicalWrite(q, r); ok {
				block = append(block, canon)
			}
		}
		replies = append(replies, r)
	}
	if s.aof != nil {
		block = append(block, respCmd("EXEC"))
		for _, v := range block {
			if err := s.aof.Log(v); err != nil {
				return resp.Value{Type: resp.Error, Str: "ERR AOF write error: " + err.Error()}
			}
		}
	}
	return resp.Value{Type: resp.Array, Arr: replies}
}

// replay applies already-parsed AOF commands. MULTI...EXEC blocks are
// dispatched as one unit; a block left unterminated by a truncated tail (the
// crash happened mid-write) is discarded whole — an incompletely persisted
// transaction never happened. NON-block MULTI/EXEC tokens are consumed here
// too so stray wrappers never reach dispatch.
func (s *Server) replay(cmds []resp.Value) {
	var block []resp.Value
	inBlock := false
	for _, v := range cmds {
		name, ok := firstCmd(v)
		if !ok {
			continue
		}
		switch {
		case name == "MULTI":
			inBlock, block = true, block[:0]
		case inBlock && name == "EXEC":
			for _, bv := range block {
				s.dispatch(bv)
			}
			inBlock = false
		case inBlock:
			block = append(block, v)
		case name == "EXEC" || name == "DISCARD":
			// 无块的散落包装命令：忽略
		default:
			s.dispatch(v)
		}
	}
	// inBlock 仍为 true = 尾部有未完成事务，整块丢弃
}

// applyExported restores one RDB record into the store at load time.
func (s *Server) applyExported(en store.Exported) {
	switch en.Kind {
	case "string":
		s.store.SetAt(en.Key, en.Str, en.Expiry)
	case "list":
		s.store.ListPush(en.Key, false, en.List...)
		if !en.Expiry.IsZero() {
			s.store.ExpireAt(en.Key, en.Expiry)
		}
	case "set":
		s.store.SetAdd(en.Key, en.Set)
		if !en.Expiry.IsZero() {
			s.store.ExpireAt(en.Key, en.Expiry)
		}
	case "hash":
		s.store.HashSet(en.Key, en.Hash)
		if !en.Expiry.IsZero() {
			s.store.ExpireAt(en.Key, en.Expiry)
		}
	case "zset":
		s.store.ZAdd(en.Key, en.ZItems)
		if !en.Expiry.IsZero() {
			s.store.ExpireAt(en.Key, en.Expiry)
		}
	}
}
