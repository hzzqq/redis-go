# redis-go · Go 复刻 Redis（Phase 6）

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

### Phase 5 — AOF 重写 + pub/sub + 性能基线（本提交）
- **AOF 重写（`BGREWRITEAOF`）**：`store.Snapshot()` 单次读锁导出最小 canonical 命令集——每 key 恰好一条命令（string→`SET [PXAT]`、list→`RPUSH` 全量、hash→`HSET` 插入序、set→`SADD` 排序、zset→`ZADD` 跳表序），跳过已过期 key；`persist.AOF.Rewrite()` 以 tmp+fsync+**rename 原子替换**（Windows 先关旧句柄再改名，改名失败自动重开旧文件兜底）。实测 468B 历史压到 255B，重启回放状态严格一致。
- **重写并发安全**：写命令的 dispatch→落盘 区间由 `applyMu` 串行化（不变量：快照点不存在「已入库、未落盘」的命令，否则 RPUSH 会在快照+新日志中重复出现）；重写本身同步执行——回复到达时已完成（比 Redis 的异步语义更强）。读命令不加锁保持并发。
- **pub/sub**：`SUBSCRIBE`（确认行 `[subscribe, ch, count]`，count 逐条递增，多频道逐帧下发）/ `UNSUBSCRIBE`（无参数退订全部，空退订回 null 频道行）/ `PUBLISH`（返回接收者数，`[message, channel, payload]` 推送帧）。订阅模式下拒绝其余命令（Redis 同文错误）、`PING` 回 `[pong, msg]` 数组；断连 defer 自动退订。消息广播**不落 AOF**（Redis 亦不传播）。
- **实现机制**：hub 为 `map[channel]map[*client]struct{}`；每条连接一把出站写锁，命令回复与跨连接推送共用，保证 RESP 帧原子交错不损坏。
- **性能基线（`cmd/bench`，Windows 本机 n=100000 c=50）**：SET 24.8k ops/s（p50 2.0ms，瓶颈在 AOF 落盘）；GET 132k ops/s（p1）/ **252k ops/s**（pipeline 16，p50 0.13ms）。工具支持 `-n/-c/-pipeline/-size`，输出吞吐 + p50/p90/p99/max。

### Phase 6 — 事务 + RDB 快照 + 范围族（`9892632`/`292accc`）
- **MULTI/EXEC/DISCARD 事务**：连接级状态机——MULTI 后命令排队回 `+QUEUED`，EXEC 一次性执行并把全部结果合成数组。语义对齐 Redis：**队列期错误**（未知命令/参数个数，`cmdArity` 单表校验 ~74 命令）立即报错并污染事务，EXEC 变 `EXECABORT`（队列整体丢弃、一条不执行）；**执行期错误**（WRONGTYPE、非整数等）只占据 EXEC 结果数组的对应槽位，其余命令照常生效。MULTI/EXEC/DISCARD/QUIT 不入队（嵌套 MULTI 报错不污染；QUIT 清事务后断连）；SUBSCRIBE/UNSUBSCRIBE 事务内拒绝且不污染。
- **事务原子性**：整块在 `applyMu` 下执行，相对其他连接的写命令原子（与单写命令同一保证）；AOF 以 `MULTI...EXEC` 块一次落盘（失败命令跳过，与 apply 同策略），回放按块感知——**崩溃留下的未闭合块整体丢弃**（等价于事务未发生），空事务不落盘。
- **RDB 快照**：`internal/persist/rdb.go` 自研二进制格式（9B magic `REDIS0059` + payload + **CRC64/ECMA 校验**；注：自研格式，非 Redis 字节级兼容）——每 key 记录类型/k/TTL + 类型化 body，五类型全覆盖。`SaveRDB` 以 tmp+fsync+**rename 原子替换**；`SAVE` 同步（回复到达即完成）、`BGSAVE` 后台 goroutine 立即回复；启动加载 **先 RDB 后 AOF**（双开时 AOF 状态优先，对齐 Redis），CRC 不符/截断/坏 magic **拒启**（对齐 Redis 对损坏 RDB 的态度）。快照用单次读锁导出——RDB 文件自洽、不与既有日志组合，无需 applyMu。
- **范围族**：`ZRANGEBYSCORE`/`ZREVRANGEBYSCORE`（`-inf`/`+inf`/`(` 排他 + `LIMIT offset count` + `WITHSCORES`，rev 形态第一个参数是 max）与 `ZRANGEBYLEX`/`ZREVRANGEBYLEX`（`-`/`+`/`[m`/`(m` 边界）。跳表新增 `lastInRange`/`lastInLexRange`（镜像 first 的层间下降条件 + backward 链回走），反向遍历同为 O(log n) 起步。
- **批量与随机**：`MGET`（missing 与 wrong-type 均回 null，单次读锁）、`MSET`（单次写锁覆写任意类型，AOF 原样落盘）、`ZRANDMEMBER key [count [WITHSCORES]]`（正数 distinct / 负数可重复 / 缺 key 单形态 null）。
- **回归修复**：applyConn 事务化重写时丢失了非订阅模式的 SUBSCRIBE/UNSUBSCRIBE/PUBLISH 路由（PUBLISH 补进 dispatch，事务内亦可执行）——TCP 全量测试暴露并修复。

## 与 redis-cli 联调

```bash
# 需要 Go 1.22+
go build -o redis-go .
./redis-go -addr :6379 &                    # 纯内存模式
./redis-go -addr :6379 -aof appendonly.aof  # AOF 持久化模式
./redis-go -addr :6379 -rdb dump.rdb        # RDB 快照模式（启动加载 + SAVE/BGSAVE 落盘）

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
redis-cli subscribe news             # 进入订阅模式，收 [message, news, ...] 推送
# 另一个终端：
redis-cli publish news hello         # (integer) 1，订阅端实时收到 "hello"
redis-cli bgrewriteaof               # AOF 重写：每 key 一条 canonical 命令
redis-cli multi                      # OK，进入事务
redis-cli                            # 交互式逐条排队：
#  set t1 x   → QUEUED
#  incr ctr   → QUEUED
#  exec       # 1) OK  2) (integer) 1 —— 结果合成数组
redis-cli mset k1 v1 k2 v2           # OK
redis-cli mget k1 k2 missing         # 1) "v1" 2) "v2" 3) (nil)
redis-cli zadd lb 1 a 2 b 3 c
redis-cli zrangebyscore lb (1 3 withscores   # b/c/d 及其分数
redis-cli zrangebyscore lb -inf +inf limit 1 2
redis-cli zrevrangebyscore lb 3 2            # 降序：c b
redis-cli zrangebylex lb - +                 # 同分场景按成员字典序
redis-cli zrandmember lb 2 withscores
redis-cli bgsave                     # Background saving started（后台写 dump.rdb）
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

## 下一步（Phase 7）

- [x] MULTI/EXEC/DISCARD 事务、RDB 快照（SAVE/BGSAVE + 启动加载）
- [x] ZRANGEBYSCORE / ZRANGEBYLEX 族、ZRANDMEMBER、批量命令（MGET/MSET）
- [ ] 主从复制（全量 RDB 同步 + 命令流传播）
- [ ] WATCH/UNWATCH（事务乐观锁，可选）

## 测试与验收

```
go build ./... && go vet ./... && go test ./...
```

- 2026-09-16：Phase 1（`04340e4`）+ Phase 2 INCR 族（`6967088`）。
- 2026-09-18：Phase 1/2 在 Go 1.22.5 实机验收通过（修 3 处缺陷，`c7e7e29`）；Phase 3 实机开发 + 全量测试通过 + 真实 TCP 冒烟（写入→杀进程→重启→状态回放一致，TTL 300s→288s 按真实时间衰减）。
- 2026-09-18：Phase 3.5（`50f4d5b`，Set 基础 + 运维命令）；Phase 4（`92817ee`）跳表 ZSet + Set 补差 + SPOP 重写，全量测试 + 25/25 TCP 冒烟（含重启回放后 SPOP 成员不复活）。
- 2026-09-18：Phase 5（`75211cd`+`c247c8d`+`c974934`）AOF 重写 + pub/sub + 压测基线；全量测试 3 轮通过 + 25/25 TCP 冒烟（重写压缩 468→255B、重启回放一致、pub/sub 不落盘、二次重启 DBSIZE=8）。
- 2026-09-18：Phase 6（`9892632`+`292accc`）MULTI/EXEC 事务 + RDB 快照 + ZRANGEBYSCORE 族 + MGET/MSET/ZRANDMEMBER；全量测试 3 轮通过 + 34/34 TCP 冒烟（EXECABORT 不执行、RDB 重启回放含 TTL、CRC 损坏拒启、事务 AOF 块重启一致）。
