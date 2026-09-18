# redis-go · Go 复刻 Redis（Phase 4）

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

### Phase 3.5 — Set 基础 + 运维命令（`50f4d5b`）
- **Set**：`SADD` `SREM` `SISMEMBER` `SMEMBERS` `SCARD`（空集合自动删 key）。
- **TYPE / DBSIZE / INFO（Server/Persistence/Keyspace 三区块）/ CONFIG GET**。

### Phase 4 — ZSet 跳表 + Set 补差 + SPOP 重写（本提交）
- **ZSet（手写跳表）**：`internal/store/zset.go` 对齐 Redis `t_zskiplist`——dict（member→score）O(1) 查找 + 跳表（maxLevel 32、p=0.25）按 (score asc, member lex asc) 排序；**节点带 per-level span**，ZRANK/ZRANGE 走 O(log n) 排名算术而非遍历计数。
- **ZSet 命令**：`ZADD`（返回新增数，score 更新不计数）`ZSCORE` `ZINCRBY` `ZCARD` `ZRANK` `ZREVRANK` `ZCOUNT`（`-inf`/`+inf`/`(` 排他边界）`ZRANGE`/`ZREVRANGE`（负索引钳制、`WITHSCORES`）`ZREM`（删空删 key）；`TYPE` 识别 `zset`。
- **Set 补差**：`SPOP [count]`（随机弹出）`SRANDMEMBER [count]`（正数去重 / 负数可重复）`SINTER` `SUNION` `SDIFF`（排序输出）。
- **SPOP AOF 重写**：随机命令不能原样回放——落盘时按**实际弹出的成员**改写为 `SREM key m1 m2...`；什么都没弹出则不落盘。回放后状态与原库严格一致（TCP 冒烟实测）。
- **正确性验证**：2000 次随机插入（含大量同分 tiebreak 与覆盖更新）+ 随机删半，逐步与排序参照实现逐项对照（顺序遍历 / 每成员排名 / ZREVRANK / 按排名取元素）。

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
redis-cli sadd s a b c               # (integer) 3
redis-cli spop s                     # 随机弹出一个（AOF 记 SREM，重启不漂移）
redis-cli sinter s s2
redis-cli zadd lb 10 alice 8 bob     # (integer) 2
redis-cli zrange lb 0 -1 withscores  # bob 8 / alice 10
redis-cli zrevrange lb 0 0           # "alice"
redis-cli zrank lb bob               # (integer) 0
redis-cli zcount lb (8 +inf          # (integer) 1
redis-cli type lb                    # zset
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
              (并发 KV + TTL              │
      string/list/hash/set/zset           ▼
        + WRONGTYPE)          persist.AOF.Log
                      ▼              │
                 resp.WriteValue ◀───┘
                 (序列化回复)
```

**并发模型**：写锁内原地变更，读锁内拷贝一切逃逸数据（string 天然不可变，slice/map 显式拷贝）；过期 key 写路径懒删除、读路径视作缺失（1s 周期清扫兜底物理删除）。

## 下一步（Phase 5+）

- [x] Set / ZSet 数据结构
- [x] `CONFIG GET/SET`、`INFO`、`DBSIZE`
- [ ] AOF 重写（rewrite，压缩文件体积）
- [ ] pub/sub 基础
- [ ] 用 `redis-benchmark` 跑性能基线
- [ ] ZRANGEBYSCORE / ZRANGEBYLEX、ZRANDMEMBER、批量命令（MGET/MSET）

## 测试与验收

```
go build ./... && go vet ./... && go test ./...
```

- 2026-09-16：Phase 1（`04340e4`）+ Phase 2 INCR 族（`6967088`）。
- 2026-09-18：Phase 1/2 在 Go 1.22.5 实机验收通过（修 3 处缺陷，`c7e7e29`）；Phase 3 实机开发 + 全量测试通过 + 真实 TCP 冒烟（写入→杀进程→重启→状态回放一致，TTL 300s→288s 按真实时间衰减）。
- 2026-09-18：Phase 3.5（`50f4d5b`，Set 基础 + 运维命令）；Phase 4（`92817ee`）跳表 ZSet + Set 补差 + SPOP 重写，全量测试 + 25/25 TCP 冒烟（含重启回放后 SPOP 成员不复活）。
