// EVAL/EVALSHA/SCRIPT（Phase 8）：Lua 脚本（gopher-lua，Lua 5.1）。
//
// 语义对齐 Redis 的「效果复制」（effect replication）：脚本在主库原样执行，
// redis.call 里成功的写命令经 canonicalWrite 确定化后收集，脚本结束（无论
// 成功还是中途报错）后一次性落 AOF 并传播给副本——脚本本身不落盘、不原样
// 传播，副本与重启回放的都是「效果命令序列」（SPOP→SREM、相对 TTL→绝对
// 毫秒），重放一次 = 主库执行一次。
//
// 脚本期间全程持有 applyMu（与写命令同一临界区）：任何其他连接的写命令
// 无法插入，脚本天然原子；也因此脚本内的 BGREWRITEAOF 会自锁，列入禁表
// （Redis 同样禁止脚本内管理命令）。脚本中途报错不回滚：已执行的写效果
// 照常落盘传播（对齐 Redis「无回滚」哲学，且必须如此——否则内存与 AOF/
// 副本状态分叉）。
//
// 副本上脚本可跑：读命令直接执行；redis.call 写命令在调用处报 READONLY
// 并中止脚本（call）/返回 {err=}（pcall），效果收集为空、不传播。
//
// KEYS/ARGV 以 1-based Lua 表传入；回复与返回值双向转换对齐 Redis：
// 整数→number、bulk→string、nil bulk→false、数组→表、状态→{ok=}、
// 错误→{err=}；脚本返回 nil/false→null bulk、true→整数 1、number→整数、
// string→bulk、表→数组（{ok=}/{err=} 特判）。
package server

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/hzzqq/redis-go/internal/resp"
	lua "github.com/yuin/gopher-lua"
)

// scriptBlocklist 是脚本内禁用的命令：BGREWRITEAOF 自身要拿 applyMu，而
// 脚本全程持有 applyMu，放行即死锁；BLPOP 族会阻塞脚本（Redis 同禁：
// "This Redis command is not allowed from script"）。SORT+STORE 单独在
// callFunc 拒绝（纯读 SORT 放行）。
var scriptBlocklist = map[string]bool{
	"BGREWRITEAOF": true, "BLPOP": true, "BRPOP": true, "BRPOPLPUSH": true,
}

// oneLine 把错误文本压成单行（RESP 错误行不允许内嵌换行；Lua 编译/运行
// 错误文本常带换行与 traceback）。
func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\r", " ")
	return strings.ReplaceAll(s, "\n", " ")
}

// evalCtx 是单次脚本执行的上下文：收集脚本内成功写命令的 canonical 效果帧。
type evalCtx struct {
	s       *Server
	effects []resp.Value
}

// cmdEval 路由 EVAL/EVALSHA/SCRIPT（applyConn 顶层入口）。事务内这些命令
// 走 queueForTxn 入队、execTransaction 的 evalExec 分支执行。
func (s *Server) cmdEval(cmd string, args []resp.Value) resp.Value {
	if cmd == "SCRIPT" {
		return s.cmdScript(args)
	}
	script, keys, argv, errVal := s.evalParse(cmd, args)
	if errVal.Type == resp.Error {
		return errVal
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	reply, effects := s.evalLocked(script, keys, argv)
	if err := s.logAndPropagate(effects); err != nil && reply.Type != resp.Error {
		reply = resp.Value{Type: resp.Error, Str: "ERR AOF write error: " + err.Error()}
	}
	// 脚本写效果触碰其他连接的 WATCH（EVAL 本身不是 writeCmd，touch 不到）
	for _, e := range effects {
		s.touchWatched(e, nil)
	}
	return reply
}

// cmdScript handles SCRIPT LOAD/EXISTS/FLUSH：纯缓存操作（sha1 → 源码），
// 不碰数据、不传播；缓存仅存内存，重启丢失（Redis 同）。
func (s *Server) cmdScript(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return wrongArgs("script")
	}
	switch strings.ToUpper(args[0].Str) {
	case "LOAD":
		if len(args) != 2 {
			return wrongArgs("script load")
		}
		sum := sha1.Sum([]byte(args[1].Str))
		id := hex.EncodeToString(sum[:])
		s.scriptMu.Lock()
		s.scripts[id] = args[1].Str
		s.scriptMu.Unlock()
		return resp.Value{Type: resp.BulkString, Str: id}
	case "EXISTS":
		s.scriptMu.Lock()
		defer s.scriptMu.Unlock()
		out := make([]resp.Value, 0, len(args)-1)
		for _, a := range args[1:] {
			n := int64(0)
			if _, ok := s.scripts[a.Str]; ok {
				n = 1
			}
			out = append(out, resp.Value{Type: resp.Integer, Num: n})
		}
		return resp.Value{Type: resp.Array, Arr: out}
	case "FLUSH":
		s.scriptMu.Lock()
		s.scripts = make(map[string]string)
		s.scriptMu.Unlock()
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	}
	return resp.Value{Type: resp.Error, Str: "ERR Unknown SCRIPT subcommand or wrong # args."}
}

// evalExec 是事务内的 EVAL/EVALSHA 执行段（execTransaction 调用，已持有
// applyMu）：效果帧交回调用方并入 MULTI...EXEC 块落盘传播。
func (s *Server) evalExec(v resp.Value) (resp.Value, []resp.Value) {
	cmd, _ := firstCmd(v)
	script, keys, argv, errVal := s.evalParse(cmd, v.Arr[1:])
	if errVal.Type == resp.Error {
		return errVal, nil
	}
	return s.evalLocked(script, keys, argv)
}

// evalParse 解析 EVAL/EVALSHA 参数段（不含命令名）：script/numkeys/keys/argv。
// EVALSHA 未命中缓存时返回 NOSCRIPT 错误。
func (s *Server) evalParse(cmd string, args []resp.Value) (string, []resp.Value, []resp.Value, resp.Value) {
	if len(args) < 2 {
		return "", nil, nil, wrongArgs(strings.ToLower(cmd))
	}
	script := args[0].Str
	if cmd == "EVALSHA" {
		s.scriptMu.Lock()
		cached, ok := s.scripts[args[0].Str]
		s.scriptMu.Unlock()
		if !ok {
			return "", nil, nil, resp.Value{Type: resp.Error,
				Str: "NOSCRIPT No matching script. Please use EVAL."}
		}
		script = cached
	}
	nkeys, err := strconv.Atoi(args[1].Str)
	if err != nil || nkeys < 0 || len(args) < 2+nkeys {
		return "", nil, nil, resp.Value{Type: resp.Error,
			Str: "ERR Number of keys can't be negative or greater than arguments"}
	}
	return script, args[2 : 2+nkeys], args[2+nkeys:], resp.Value{}
}

// evalLocked 执行脚本并登记 sha1 缓存（EVAL/EVALSHA 都登记，Redis 同）。
// 调用方必须已持有 applyMu。
func (s *Server) evalLocked(script string, keys, argv []resp.Value) (resp.Value, []resp.Value) {
	sum := sha1.Sum([]byte(script))
	id := hex.EncodeToString(sum[:])
	s.scriptMu.Lock()
	s.scripts[id] = script
	s.scriptMu.Unlock()
	return s.evalRun(script, id, keys, argv)
}

// evalRun 跑一次脚本：新建 LState、装配 KEYS/ARGV/redis 表、执行、双向
// 转换。返回脚本返回值（resp 形式）与效果帧。调用方必须已持有 applyMu。
// 每次执行新建 LState（无跨调用状态，简单且无并发问题；Redis 每次执行
// 也是独立沙箱 reset）。
func (s *Server) evalRun(script, id string, keys, argv []resp.Value) (resp.Value, []resp.Value) {
	L := lua.NewState()
	defer L.Close()
	ec := &evalCtx{s: s}

	keyT, argT := L.NewTable(), L.NewTable()
	for i, k := range keys {
		keyT.RawSetInt(i+1, lua.LString(k.Str))
	}
	for i, a := range argv {
		argT.RawSetInt(i+1, lua.LString(a.Str))
	}
	L.SetGlobal("KEYS", keyT)
	L.SetGlobal("ARGV", argT)

	redis := L.NewTable()
	redis.RawSetString("call", L.NewFunction(ec.callFunc(true)))
	redis.RawSetString("pcall", L.NewFunction(ec.callFunc(false)))
	redis.RawSetString("status_reply", L.NewFunction(statusReplyFunc(true)))
	redis.RawSetString("error_reply", L.NewFunction(statusReplyFunc(false)))
	L.SetGlobal("redis", redis)

	fn, err := L.LoadString(script)
	if err != nil {
		return resp.Value{Type: resp.Error,
			Str: oneLine("ERR Error compiling script (call to f_" + id + "): " + err.Error())}, nil
	}
	L.Push(fn)
	if err := L.PCall(0, 1, nil); err != nil {
		return resp.Value{Type: resp.Error,
			Str: oneLine("ERR Error running script (call to f_" + id + "): " + err.Error())}, ec.effects
	}
	reply, cerr := luaToResp(L.Get(-1))
	if cerr != nil {
		return resp.Value{Type: resp.Error, Str: cerr.Error()}, ec.effects
	}
	return reply, ec.effects
}

// callFunc 构造 redis.call/redis.pcall 的 Go 实现：解析参数为 RESP 命令 →
// dispatch 执行（副本 READONLY 门在脚本内显式判，因 dispatch 不走 apply 的
// 写门）→ 成功写命令收集 canonical 效果 → 回复转 Lua。call 遇错误抛 Lua
// 异常中止脚本；pcall 把错误转成 {err=} 表返回。
func (ec *evalCtx) callFunc(isCall bool) func(*lua.LState) int {
	return func(L *lua.LState) int {
		n := L.GetTop()
		if n == 0 {
			L.RaiseError("Please specify at least one argument for redis.call()")
			return 0
		}
		parts := make([]string, 0, n)
		for i := 1; i <= n; i++ {
			switch v := L.Get(i).(type) {
			case lua.LString:
				parts = append(parts, string(v))
			case lua.LNumber:
				parts = append(parts, strconv.FormatFloat(float64(v), 'f', -1, 64))
			default:
				L.ArgError(i, "Lua redis lib command arguments must be strings or integers")
				return 0
			}
		}
		cmd := make([]resp.Value, len(parts))
		for i, p := range parts {
			cmd[i] = resp.Value{Type: resp.BulkString, Str: p}
		}
		v := resp.Value{Type: resp.Array, Arr: cmd}
		name := strings.ToUpper(parts[0])

		var reply resp.Value
		var setPre bool
		switch {
		case scriptBlocklist[name]:
			reply = resp.Value{Type: resp.Error,
				Str: "ERR This Redis command is not allowed from script: " + name}
		case name == "SORT" && sortHasStore(parts[1:]):
			// STORE 会写库且效果无法从脚本上下文确定性收集（读回时机受限），
			// 脚本内禁用；纯读 SORT 放行
			reply = resp.Value{Type: resp.Error,
				Str: "ERR SORT with STORE is not allowed from script"}
		case isWriteCmd(v) && ec.s.isReplica():
			reply = readonlyErr()
		default:
			setPre = ec.s.setPreState(v)
			reply = ec.s.dispatch(v)
		}
		// 成功写命令 → canonical 效果帧（与 AOF/传播同一确定化路径）
		if isWriteCmd(v) && reply.Type != resp.Error {
			if c, ok := ec.s.canonicalFor(v, reply, setPre); ok {
				ec.effects = append(ec.effects, c)
			}
		}
		if reply.Type == resp.Error {
			if isCall {
				L.RaiseError("%s", reply.Str) // call：错误中止脚本（Redis 同）
				return 0
			}
			t := L.NewTable()
			t.RawSetString("err", lua.LString(reply.Str))
			L.Push(t)
			return 1
		}
		L.Push(respToLua(reply))
		return 1
	}
}

// statusReplyFunc 构造 redis.status_reply/redis.error_reply。
func statusReplyFunc(ok bool) func(*lua.LState) int {
	return func(L *lua.LState) int {
		if L.GetTop() != 1 {
			L.RaiseError("wrong number of arguments")
			return 0
		}
		msg, lv := "", L.Get(1)
		if s, isStr := lv.(lua.LString); isStr {
			msg = string(s)
		} else if n, isNum := lv.(lua.LNumber); isNum {
			msg = strconv.FormatFloat(float64(n), 'f', -1, 64)
		} else {
			L.RaiseError("status/error reply argument must be a string or integer")
			return 0
		}
		t := L.NewTable()
		if ok {
			t.RawSetString("ok", lua.LString(msg))
		} else {
			t.RawSetString("err", lua.LString(msg))
		}
		L.Push(t)
		return 1
	}
}

// respToLua 把 RESP 回复转成 Lua 值（Redis 语义见包注释）。
func respToLua(v resp.Value) lua.LValue {
	switch v.Type {
	case resp.SimpleString:
		t := &lua.LTable{}
		t.RawSetString("ok", lua.LString(v.Str))
		return t
	case resp.Error:
		t := &lua.LTable{}
		t.RawSetString("err", lua.LString(v.Str))
		return t
	case resp.Integer:
		return lua.LNumber(v.Num)
	case resp.BulkString:
		if v.Null {
			return lua.LFalse
		}
		return lua.LString(v.Str)
	case resp.Array:
		t := &lua.LTable{}
		for i, e := range v.Arr {
			t.RawSetInt(i+1, respToLua(e))
		}
		return t
	}
	return lua.LNil
}

// luaToResp 把脚本返回值转成 RESP 回复（Redis 语义见包注释）。
func luaToResp(lv lua.LValue) (resp.Value, error) {
	switch t := lv.(type) {
	case *lua.LNilType:
		return resp.Value{Type: resp.BulkString, Null: true}, nil
	case lua.LBool:
		if bool(t) {
			return resp.Value{Type: resp.Integer, Num: 1}, nil
		}
		return resp.Value{Type: resp.BulkString, Null: true}, nil
	case lua.LNumber:
		return resp.Value{Type: resp.Integer, Num: int64(t)}, nil
	case lua.LString:
		return resp.Value{Type: resp.BulkString, Str: string(t)}, nil
	case *lua.LTable:
		// {ok=...} / {err=...} 特殊表优先（Redis 同）
		if okv := t.RawGetString("ok"); okv.Type() == lua.LTString {
			return resp.Value{Type: resp.SimpleString, Str: string(okv.(lua.LString))}, nil
		}
		if errv := t.RawGetString("err"); errv.Type() == lua.LTString {
			return resp.Value{Type: resp.Error, Str: string(errv.(lua.LString))}, nil
		}
		arr := []resp.Value{}
		for i := 1; ; i++ {
			ev := t.RawGetInt(i)
			if ev.Type() == lua.LTNil {
				break
			}
			cv, err := luaToResp(ev)
			if err != nil {
				return resp.Value{}, err
			}
			arr = append(arr, cv)
		}
		return resp.Value{Type: resp.Array, Arr: arr}, nil
	}
	return resp.Value{}, errors.New(
		"ERR user_script: invalid return value (only strings, numbers, booleans or tables are allowed)")
}
