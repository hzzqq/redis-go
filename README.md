# redis-go · Go 复刻 Redis（Phase 3）

用 Go 从零复刻 Redis 的核心协议、内存数据模型与持久化，目标是**可被官方 `redis-cli` 直接连接验证**——这是后端岗的硬通货级项目。

> 路线定位（硬核加分线，来自 08-30 规划）：Go 复刻 Redis → raft-kv 深化 ShardKV → CMU BusTub。

## 已交付

### Phase 1 — 协议 + KV + TTL
- **RESP 协议**：`internal/resp` 实现完整解析 + 序列化（SimpleString / Error / Integer / BulkString / Array，含 null bulk）。
- **内存存储**：`internal/store` 并发安全 KV，支持 TTL（懒过期 + 1s 周期清扫）。
- **命令**：`PING` `ECHO` `GET` `SET [EX]` `SETEX` `DEL` `EXISTS` `EXPIRE` `TTL` `FLUSHALL` `COMMAND` `QUIT`。

### Phase 2 — 字符串命令
- `APPEND`（保留 TTL、过期 key 视作新 key）/ `INCR` `DECR` `INCRBY`（非整数报错、int64 溢出检测、失败不改原值）。

### Phase 3 — 多类型 + 持久化（本提交）
- **值类型化**：key 可持有 string / list / hash，跨类型操作返回 `WRONGTYPE`（与 Redis 一致）；SET 覆写任意类型；空集合自动删 key。
- **List**：`LPUSH` `RPUSH` `LPOP [count]` `RPOP [count]`（count 版按弹出序返回）`LLEN` `LRANGE`（负索引/越界钳制）`LINDEX` `LSET` `LTRIM`。
- **Hash**：`HSET`（返回新增字段数）`HGET` `HGETALL`（插入序）`HDEL` `HLEN` `HEXISTS` `HKEYS` `HVALS` `HINCRBY`（非整数/溢出检测）。
- **AOF 持久化**（`internal/persist`）：写命令以 RESP 编码追加落盘，启动时回放重建状态；容忍崩溃导致的截断尾部（截断前命令全部生效，对齐 Redis `aof-load-truncated yes`）。
- **TTL 绝对时间戳落盘**：`SETEX`/`SET EX` → `SET key val PXAT ms`、`EXPIRE` → `PEXPIREAT key ms`（Redis AOF 同款做法），重启后 TTL 不因回放耗时漂移；新增 `SET PX/PXAT`、`PEXPIREAT` 命令。
- **时序**：执行 → 落盘 → 回复（Redis 同款），落盘失败返回 `ERR AOF write error`。

## 与 redis-cli 联调

```bash
# 需要 Go 1.22+
go build -o redis-go .
./redis-go -addr :6379 &                    # 纯内存模式
./redis-go -addr :6379 -aof appendonly.aof  # AOF 持久化模式

redis-cli ping                       # PONG
redis-cli set foo bar ex 10
redis-cli get foo                    # "bar"
redis-cli rpush l a b c              # (integer) 3
redis-cli lrange l 0 -1              # 1) "a" 2) "b" 3) "c"
redis-cli hset h f1 v1 f2 v2         # (integer) 2
redis-cli hgetall h
redis-cli hincrby h n 5              # (integer) 5
redis-cli ttl foo                    # (integer) 9
```

重启后数据仍在（AOF 模式）：`foo`、列表、哈希全部回放，TTL 按真实流逝时间继续衰减。

## 架构

```
redis-cli ──TCP──▶ server.Listen
                      │
                      ▼
                 resp.Reader (解析命令数组)
                      │
                      ▼
                 server.dispatch (命令分发)
                      │              ▲
                      ▼              │ 回放（启动时）
                 store.Store ──── persist.Load
              (并发 KV + TTL          │
        string/list/hash + WRONGTYPE) ▼
                      │         persist.AOF.Log
                      ▼              │
                 resp.WriteValue ◀───┘
                 (序列化回复)
```

**并发模型**：写锁内原地变更，读锁内拷贝一切逃逸数据（string 天然不可变，slice/map 显式拷贝）；过期 key 写路径懒删除、读路径视作缺失（1s 周期清扫兜底物理删除）。

## 下一步（Phase 4+）

- [ ] Set / ZSet 数据结构
- [ ] `CONFIG GET/SET`、`INFO`、`DBSIZE`
- [ ] AOF 重写（rewrite，压缩文件体积）
- [ ] pub/sub 基础
- [ ] 用 `redis-benchmark` 跑性能基线

## 测试与验收

```
go build ./... && go vet ./... && go test ./...
```

- 2026-09-16：Phase 1（`04340e4`）+ Phase 2 INCR 族（`6967088`）。
- 2026-09-18：Phase 1/2 在 Go 1.22.5 实机验收通过（修 3 处缺陷，`c7e7e29`）；Phase 3 实机开发 + 全量测试通过 + 真实 TCP 冒烟（写入→杀进程→重启→状态回放一致，TTL 300s→288s 按真实时间衰减）。
