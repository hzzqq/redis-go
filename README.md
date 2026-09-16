# redis-go · Go 复刻 Redis（Phase 1）

用 Go 从零复刻 Redis 的核心协议与内存数据模型，目标是**可被官方 `redis-cli` 直接连接验证**——这是后端岗的硬通货级项目。

> 路线定位（硬核加分线，来自 08-30 规划）：Go 复刻 Redis → raft-kv 深化 ShardKV → CMU BusTub。

## Phase 1 已交付（本提交）

- **RESP 协议**：`internal/resp` 实现完整解析 + 序列化（SimpleString / Error / Integer / BulkString / Array，含 null bulk）。
- **内存存储**：`internal/store` 并发安全 KV，支持 TTL（懒过期 + 1s 周期清扫）。
- **命令**：`PING` `ECHO` `GET` `SET [EX]` `SETEX` `DEL` `EXISTS` `EXPIRE` `TTL` `FLUSHALL` `COMMAND` `QUIT`。
- **服务器**：`internal/server` TCP 监听 `:6379`，命令分发，redis-cli 兼容。
- **测试**：`resp` + `store` 单测（纯标准库，无外部依赖）。

## 与 redis-cli 联调

```bash
# 需要 Go 1.22+
go build -o redis-go .
./redis-go -addr :6379 &

redis-cli ping            # PONG
redis-cli set foo bar ex 10
redis-cli get foo         # "bar"
redis-cli ttl foo         # (integer) 9
redis-cli exists foo      # (integer) 1
redis-cli del foo
```

## 架构

```
redis-cli ──TCP──▶ server.Listen
                      │
                      ▼
                 resp.Reader (解析命令数组)
                      │
                      ▼
                 server.dispatch (命令分发)
                      │
                      ▼
                 store.Store (并发 KV + TTL)
                      │
                      ▼
                 resp.WriteValue (序列化回复)
```

## 下一步（Phase 2+）

- [ ] `APPEND` / `INCR` / `DECR` / `INCRBY` 等字符串命令
- [ ] List / Hash / Set 数据结构
- [ ] `CONFIG GET/SET`、`INFO`、`DBSIZE`
- [ ] AOF / RDB 持久化
- [ ] -pub/sub 基础
- [ ] 用 `redis-benchmark` 跑性能基线

## 诚实说明

本机开发环境未安装 Go 工具链，源码经人工审阅 + 单测逻辑核对，但未在此机器上 `go build`/`go test` 实跑。请在装有 Go 1.22+ 的机器执行 `go test ./...` 验收。
