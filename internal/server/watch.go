// WATCH/UNWATCH 事务乐观锁（Phase 8）。
//
// 语义对齐 Redis：
//   - WATCH key [key...]：登记本连接关注的 key，任何其他客户端（含自己在
//     事务外的普通写）修改其中任一 key，本连接的事务即被打上 dirty 标记；
//   - EXEC 时若 dirty：放弃整个队列并返回 null array *-1（事务未发生）；
//   - UNWATCH / EXEC / DISCARD / QUIT / 连接断开：清空全部 watch 与 dirty；
//   - MULTI 内 WATCH 直接报错（Redis 同文）；UNWATCH 即时生效（事务内
//     排队的 UNWATCH 与 EXEC 自身清 watch 等效，简化为连接级即时执行）。
//
// 实现采用 hub 模式（与 pub/sub 同款）：server.watchers 维护
// key → 关注它的连接集合；写命令执行成功后由 touchWatched 按命令的 key
// 参数把对应连接打 dirty。锁序：applyMu → watchMu。
package server

import (
	"github.com/hzzqq/redis-go/internal/resp"
)

// cmdWatch handles WATCH key [key ...].
func (s *Server) cmdWatch(cl *client, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return wrongArgs("watch")
	}
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, a := range args {
		key := a.Str
		if cl.watchKeys == nil {
			cl.watchKeys = make(map[string]struct{})
		}
		if _, dup := cl.watchKeys[key]; dup {
			continue
		}
		cl.watchKeys[key] = struct{}{}
		set, ok := s.watchers[key]
		if !ok {
			set = make(map[*client]struct{})
			s.watchers[key] = set
		}
		set[cl] = struct{}{}
	}
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

// cmdUnwatch handles UNWATCH: clears all watched keys and the dirty flag.
func (s *Server) cmdUnwatch(cl *client) resp.Value {
	s.clearWatch(cl)
	return resp.Value{Type: resp.SimpleString, Str: "OK"}
}

// clearWatch removes every watch registration of cl and resets dirtyCAS.
// Called by UNWATCH / EXEC / DISCARD / QUIT / dropClient.
func (s *Server) clearWatch(cl *client) {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for key := range cl.watchKeys {
		if set, ok := s.watchers[key]; ok {
			delete(set, cl)
			if len(set) == 0 {
				delete(s.watchers, key)
			}
		}
	}
	cl.watchKeys = nil
	cl.dirtyCAS = false
}

// touchWatched marks every client watching one of the command's keys as
// dirty-cas. except (may be nil) skips one client — used by EXEC so that a
// transaction's own writes don't abort itself (its watches are cleared right
// after anyway). Called under applyMu after the write committed.
func (s *Server) touchWatched(v resp.Value, except *client) {
	name, ok := firstCmd(v)
	if !ok {
		return
	}
	if name == "FLUSHALL" {
		s.watchMu.Lock()
		defer s.watchMu.Unlock()
		for _, set := range s.watchers {
			for cl := range set {
				if cl != except {
					cl.dirtyCAS = true
				}
			}
		}
		return
	}
	keys := writeCmdKeys(name, v.Arr[1:])
	if len(keys) == 0 {
		return
	}
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, key := range keys {
		for cl := range s.watchers[key] {
			if cl != except {
				cl.dirtyCAS = true
			}
		}
	}
}

// writeCmdKeys extracts the key arguments of one write command (already
// upper-cased name, args without the command word). Unknown shapes return
// nothing — WATCH coverage is best-effort per known command grammar.
func writeCmdKeys(name string, args []resp.Value) []string {
	switch name {
	case "MSET": // MSET k1 v1 k2 v2 ...
		var keys []string
		for i := 0; i+1 < len(args); i += 2 {
			keys = append(keys, args[i].Str)
		}
		return keys
	case "LMOVE": // 改动源和目标两个 key（args 不含命令名：src dst dir dir）
		if len(args) >= 2 {
			return []string{args[0].Str, args[1].Str}
		}
		return nil
	case "DEL", "MGET":
		keys := make([]string, 0, len(args))
		for _, a := range args {
			keys = append(keys, a.Str)
		}
		return keys
	case "FLUSHALL":
		return nil // 由 touchWatched 特判全量 touch
	default:
		if len(args) >= 1 {
			return []string{args[0].Str} // 常规写命令第一个参数即 key
		}
		return nil
	}
}

// watchCmdError is the Redis wording for WATCH inside MULTI.
func watchCmdError() resp.Value {
	return resp.Value{Type: resp.Error, Str: "ERR WATCH inside MULTI is not allowed"}
}
