# redis-go · Go 复刻 Redis（Phase 9）

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

### Phase 7 — 主从复制（本提交）
- **复制协议（对齐 Redis 2.6「断线即全量重同步」）**：副本 `REPLICAOF host port` → TCP 连主库 → `REPLCONF listening-port`/`capa`（主库一律 +OK）→ `PSYNC ? -1` → 主库回 `+FULLRESYNC <replid> 0`，随后把此刻全量 RDB 作为一个 RESP bulk 帧发送 → 副本 `Flush` + 逐条载入 → 之后串行回放命令流；连接断开则 2s 退避重连，重新全量同步。无部分重同步/backlog/ACK（有意简化：回环/局域网场景 TCP 错误即可探活）。
- **传播挂点 = AOF 落盘点**：写命令的 `dispatch→canonical→log(AOF)+propagate(replicas)` 全在 `applyMu` 临界区内原子完成，同一份 canonical 形式（SPOP→SREM 确定化、相对 TTL→`PEXPIREAT` 绝对化）同时供 AOF 落盘与副本传播，状态天然一致。事务以 `MULTI...EXEC` 帧序列传播，副本端 `replay`/`applyMasterBlock` 同款块感知整体原子回放。
- **快照/注册原子性**：`handlePSYNC` 在 `applyMu` 临界区内 `Export + EncodeRDB + 注册副本链路`——快照前的命令在 RDB、快照后的命令全传播，无丢失窗口。副本出站走独立 writer goroutine（临界区内只加锁入队，慢副本不拖写路径）；积压 >256MB 断开（对齐 `client-output-buffer-limit replica`）；副本连接升级后 `handle()` 回复一律静默。
- **副本语义**：READONLY 拒写（Redis 同文错误；事务内写命令在 EXEC 槽位返回 READONLY 执行期错误）；`REPLICAOF NO ONE` 晋升为主库（数据保留）；拒绝复制到自身（防命令流自激死循环）；副本 AOF 基线重写——全量同步后立即把基线 `Snapshot→Rewrite` 进 AOF，重启后「基线+命令流」完整。级联复制（A←B←C）天然支持：中间节点两段 INFO 都输出。
- **回归修复（冒烟驱动）**：① `handlePSYNC` 只注册 `s.replicas[cl]` 漏设 `cl.replicaLink`，导致副本断开后主库永不摘除链路（`dropClient` 依赖该字段）——补一行赋值并加回归测试；② `store.ListPush` 多参数头插顺序与 Redis 不符（`LPUSH l a b` 应得 `[b,a]` 而非 `[a,b]`）——改为逆序 append 并补 store/server 两层用例；③ `replicationSection` 对级联中间节点只走 replica 分支丢了 `connected_slaves`——重构为两段都输出。
- **运维命令**：`REPLCONF`/`PSYNC`/`SYNC`（连接级处理）、`-replicaof host:port` 启动参数、`INFO replication`（role/master_host/master_port/master_link_status/connected_slaves/slaveN/master_replid）。

### Phase 8 — WATCH 乐观锁 + 部分重同步 + Lua 脚本（本提交）
- **WATCH/UNWATCH 事务乐观锁**：hub 模式（`watchers map[key]map[*client]`，锁序 applyMu→watchMu）——写命令（含 EVAL 脚本效果）执行成功后 `touchWatched` 按命令 key 把关注连接打 dirty；EXEC 时 dirty 即放弃整个队列回 **null array `*-1`**（Redis 同款；TCP 冒烟暴露原实现回 `$-1`，已修 resp 编码器区分 null array/bulk）。UNWATCH/EXEC/DISCARD/QUIT/断连清 watch；MULTI 内 WATCH 直接报错（Redis 同文）。
- **部分重同步（repl-backlog + PSYNC offset + REPLCONF ACK）**：主库维护 **1 MiB 环形 backlog**（feed 在 applyMu 临界区，与命令提交同一原子区间）；副本断线重连发 `PSYNC <replid> <offset>`——replid 匹配且 offset 仍在 backlog → `+CONTINUE` 只续传增量帧（不 Flush、不重写 AOF 基线），否则退化全量。offset 按**帧重编码长度**计量（与 propagate 同一编码器，不受 bufio 4KB 预读影响）；流循环**复用握手同一个 resp.Reader**——新建 Reader 会丢掉旧 bufio 已预读的字节，曾致 CONTINUE 增量整段丢失（回归测试暴露）。主库每秒 `REPLCONF GETACK` 心跳收集副本 ack，`INFO replication` 输出 `slaveN ... offset=` 与 `master_repl_offset`。`REPLICAOF NO ONE` 晋升时保留上游 replid/offset，重挂**同一主库**自动走部分重同步、异主库退化全量（对齐真实 Redis replid 保留语义）。
- **Lua 脚本（EVAL/EVALSHA/SCRIPT）**：gopher-lua（纯 Go Lua 5.1）——`redis.call/pcall/status_reply/error_reply` + KEYS/ARGV 表注入。**效果复制**：脚本内写命令经 canonicalWrite 确定化（SPOP→SREM、相对 TTL→PXAT）落 AOF + 传播副本，**脚本本身不落盘不原样传播**（重启/副本重放状态严格一致，实测）；全程持 applyMu 保证原子；**无回滚**（脚本报错时先前效果照常生效并传播，对齐 Redis——不传播会分叉）；副本上脚本可读、写调用回 READONLY 中止；事务内 EVAL 效果并入 MULTI...EXEC 块；`SCRIPT LOAD/EXISTS/FLUSH` + `EVALSHA`（未命中回 NOSCRIPT，sha 缓存进程内、重启丢失，Redis 同）；脚本内禁 BGREWRITEAOF（applyMu 重入死锁）。Lua↔RESP 转换矩阵（nil/false→null bulk、true→整数 1、`{ok=}`/`{err=}`→状态/错误），错误文本单行净化（RESP 错误行禁内嵌换行）。
- **基线扩展（cmd/bench，Windows 本机）**：`-cmd` 新增 `EVAL`/`EVALSHA`/`CAS`（WATCH→MULTI→SET→EXEC，4 次往返/op）。SET 188.8k ops/s（c=50 p=1）；GET **474.4k ops/s**（pipeline 16）；EVAL 8.0k / EVALSHA 7.5k ops/s（c=20，LState 每次新建是瓶颈，语义正确优先不做池化）；CAS 19.9k ops/s（c=10）。
- **工程**：go.mod 升 **go 1.23**（gopher-lua v1.1.0 要求）；新增 eval_test.go 8 用例 + psync_test.go 4 用例（backlog 环形语义 / 全量→部分→回退三路径 / ACK 收敛）。

### Phase 9 — SCAN 游标族 + List 补全 + appendfsync + 过期选项矩阵（本提交）
- **SCAN/SSCAN/HSCAN/ZSCAN**：`SCAN cursor [MATCH p] [COUNT n] [TYPE t]` 与三集合变体（HSCAN/ZSCAN 扁平 `[field, value, ...]`）。游标语义为**排序快照 + 偏移**：keyspace 排序后按 COUNT 前进，与 Redis 的 reverse-binary-iteration 不同——无保证跨库迭代不重不漏，但在**稳定 keyspace 上恰好一次全覆盖**（`globMatch` 为 stringmatchlen 直译：`*?[a-z][^..]\转义`、`[]]` 首字符字面量）；MATCH 过滤后一页可能为空（真实 Redis 同行为），COUNT 是扫描量而非返回量。SSCAN/HSCAN/ZSCAN 的 cursor 解析需先剥 key（冒烟前单测暴露的参数错位）。
- **List 补全**：`LMOVE src dst LEFT|RIGHT LEFT|RIGHT`（单写锁原子；同 key 自轮转保 TTL；弹空源删 key；新 dst 无 TTL / 旧 dst 保 TTL——与 RPOP+LPUSH 组合的语义差异）`LINSERT key BEFORE|AFTER pivot el`（0=无 key、-1=pivot 不存在）`LPOS key el [RANK n] [COUNT n] [MAXLEN n]`（RANK 0 报 Redis 原文长错误；COUNT 回 Integer 数组、无匹配 null；**MAXLEN 限制总比较量含收集阶段**——单循环实现对齐 t_list.c，先定位后收集的两段式会绕过 MAXLEN）。
- **appendfsync 三档**：`-appendfsync always|everysec|no`（默认 everysec）+ 运行期 `CONFIG SET`/`GET appendfsync` + `INFO persistence` 新增 `aof_fsync:`。`always` 每次 Log 同步 fsync；`everysec` 后台 goroutine 1s ticker（**崩溃最多丢 ~1s 已确认写入**，Redis 默认权衡）；`no` 只到页缓存。启停生命周期：切到 everysec 启 goroutine、切走即停、`Close` 等待退出；非法值回退 no。
- **EXPIRE 选项 + SET 收尾 + OBJECT**：`EXPIRE/PEXPIREAT/PEXPIRE/EXPIREAT key n NX|XX|GT|LT`（NX/XX 与 GT/LT 冲突报 Redis 同文错误；**key 不存在一律 0**；无 TTL key 上 GT 失败/LT 成功；条件落盘规则见下）`OBJECT ENCODING`（int/embstr≤44/raw、listpack≤128、intset 全整数≤512、skiplist，阈值对齐 Redis）`SET` 选项矩阵收尾（`NX/XX/GET/KEEPTTL` 组合、`NX+XX`/`KEEPTTL+EX|PX` syntax error、GET 对 WRONGTYPE 回错）。
- **canonicalWrite 修复（SET NX+GET 消歧）**：`SET k v NX GET` 在**新 key** 上成功时旧值为 null、回复与「NX 失败」同为 null bulk——原实现误判为失败不落盘，**重启丢 key**。修法：dispatch 前捕获 key 存在性快照（`setPreState`，仅 NX+GET 组合访问 store），5 个 canonicalWrite 调用点（apply/事务/主库流/事务块/EVAL 效果）统一传入。落盘规则同步细化：NX 无 GET / XX（含 XX+GET）失败不落盘；GET 无 NX/XX（无条件 SET、旧值 null）落盘。
- **回归修复（单测驱动）**：`cmdLMove` 参数下标错位（args 不含命令名，却按下标 1/2 取方向词）——`LMOVE` 一律 syntax error 且 WATCH 触碰连带失效；`store.ListPos` MAXLEN 两段式实现绕过收集阶段限制——重写为单循环；`TestCanonicalWritePhase9` 误用 dispatch（纯执行不落盘）——写命令改走 apply。
- **基准与对照（cmd/bench，Windows 本机 n=100k）**：无 AOF——SET 250.0k（c=50 p=1）/ GET 264.2k / GET **623.8k**（pipeline 16）/ EVAL 9.5k / EVALSHA 9.6k（c=20）/ CAS 25.9k（c=10）。AOF 对照：everysec SET 38.1k、always SET 963 ops/s（Windows 每次 fsync p50 52ms，凸显三档策略的实际代价）。**redis-benchmark 对照**：本机无 redis-server/redis-benchmark 可用（docker 守护进程损坏、WSL 不支持），对照表以同参数复测自基线替代，等价命令见下节。
- **测试**：新增 27 个测试函数（store/cmds9_test.go 10 + server/cmds9_test.go 13 + persist/fsync_test.go 4），全量 build/vet/test 3 轮 + 83 断言 TCP 冒烟（SCAN 分页/MATCH/TYPE、SSCAN/HSCAN/ZSCAN、OBJECT、LMOVE 轮转跨 key、LINSERT、LPOS 选项、SET/EXPIRE 选项矩阵、everysec 重启回放、LMOVE/EXPIRE/SET NX GET 副本传播）。

## 与 redis-cli 联调

```bash
# 需要 Go 1.23+
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

# ── 主从复制（Phase 7）──
./redis-go -addr :6380 -aof master.aof                       # 终端 1：主库
./redis-go -addr :6381 -aof replica.aof -replicaof :6380     # 终端 2：副本（启动即挂载）
redis-cli -p 6380 info replication      # role:master, connected_slaves:1
redis-cli -p 6381 info replication      # role:replica, master_link_status:online
redis-cli -p 6380 set k v               # 写主库
redis-cli -p 6381 get k                 # "v" —— 命令流实时到达副本
redis-cli -p 6381 set x y               # READONLY ...（副本拒写）
redis-cli -p 6381 replicaof no one      # OK —— 晋升为主库，恢复可写
# 杀掉副本重启（同参数）→ 自动重连 + 全量重同步；杀掉主库重启 → 副本 2s 退避重连
redis-cli -p 6381 replicaof 127.0.0.1 6380
# 短暂断线后重挂同一主库 → +CONTINUE 部分重同步，只补断线期间的增量（主库日志 "partial resync, resumed"）

# ── WATCH 乐观锁 + Lua 脚本（Phase 8）──
redis-cli watch k                    # OK
# 另一终端：redis-cli set k other     # 之后 multi → set k v → exec 回 (nil)（乐观锁中止，一条不执行）
redis-cli multi                      # set k v → QUEUED → exec → (nil)
redis-cli unwatch                    # OK，重新 WATCH 后无干扰则正常提交
redis-cli eval "return redis.call('SET', KEYS[1], ARGV[1])" 1 gk gv
                                     # "OK"；AOF/副本收到的是 SET gk gv（效果复制，脚本不落盘）
redis-cli eval "return {KEYS[1], ARGV[1]}" 1 a b    # 1) "a"  2) "b"
redis-cli script load "return ARGV[1]"              # 40 位 sha
redis-cli evalsha <sha> 0 hello      # "hello"；未登记的 sha 回 NOSCRIPT 错误

# ── SCAN 游标族 + List 补全 + 过期选项（Phase 9）──
redis-cli scan 0 count 10                 # 1) "10"  2) 1) "key:0" ...（游标 0 = 一轮结束）
redis-cli scan 0 match "user:*" type string
redis-cli sscan myset 0 count 100
redis-cli hscan myhash 0 match "f*"       # 扁平 1) "f1" 2) "v1" ...
redis-cli zscan lb 0                      # 扁平 member/score 交替
redis-cli object encoding counter         # "int"（小整数）；embstr ≤44B；更长达 "raw"
redis-cli lmove src dst left right        # 头弹尾推；同 key 即轮转
redis-cli linsert mylist before "c" "b"   # (integer) 3；pivot 不存在回 -1
redis-cli lpos mylist "a" rank 2 count 10 maxlen 100   # 1) (integer) 0 2) (integer) 2 ...
redis-cli expire k 100 nx                 # 已有 TTL 回 0；xx/gt/lt 同理
redis-cli set k v nx get keepttl          # 组合选项；NX+XX / KEEPTTL+EX 回 syntax error
redis-cli config get appendfsync          # everysec（默认）；CONFIG SET 运行期切换
redis-cli info persistence | grep aof_fsync

# redis-benchmark 等价命令（本机无真实 Redis 可用时，用 cmd/bench 同参数复测自基线）：
#   redis-benchmark -n 100000 -c 50 -t set,get
#   redis-benchmark -n 100000 -c 50 -P 16 -t get
#   redis-benchmark -n 100000 -c 20 -t eval   （EVAL "return 1" 0）
#   redis-benchmark -n 100000 -c 10 --eval cas.lua
go run ./cmd/bench -addr 127.0.0.1:6379 -n 100000 -c 50 -cmd SET
go run ./cmd/bench -addr 127.0.0.1:6379 -n 100000 -c 50 -pipeline 16 -cmd GET

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

**复制模型**：写命令在 `applyMu` 临界区内「执行 → canonical 化 → AOF 落盘 + 副本入队 + repl-backlog feed」一步完成；每条副本连接一个独立 writer goroutine 出站（队列积压超 256MB 断开）；副本断线重连先试 `PSYNC <replid> <offset>` 部分重同步（1 MiB 环形 backlog 续传增量，超出范围退化全量 RDB）；主库每秒 REPLCONF GETACK 收集副本进度（INFO 可观测）。锁序：`applyMu → replMu → link.mu`。

## 下一步（Phase 10）

- [x] 主从复制（全量 RDB 同步 + 命令流传播、级联、REPLICAOF/READONLY/INFO replication）
- [x] WATCH/UNWATCH 乐观锁、部分重同步（repl-backlog + PSYNC CONTINUE + REPLCONF ACK）、Lua 脚本（EVAL/EVALSHA/SCRIPT）
- [x] SCAN/SSCAN/HSCAN/ZSCAN 游标遍历
- [x] List 补全：LMOVE / LINSERT / LPOS
- [x] appendfsync everysec（后台 fsync + 崩溃丢失窗口语义）
- [x] EXPIRE NX/XX/GT/LT 选项、SET 选项矩阵收尾、OBJECT ENCODING
- [x] redis-benchmark 对照基线（真实 Redis 不可用，以同参数复测自基线 + 等价命令文档化替代）

可选后续：阻塞命令（BLPOP/BRPOP/BRPOPLPUSH）、SORT 命令、STREAM 类型、AOF everysec 的 30s 兜底 fsync（Redis fsync 停滞保护）、真实 Redis 同机对照（待可用环境）。

## 测试与验收

```
go build ./... && go vet ./... && go test ./...
```

- 2026-09-16：Phase 1（`04340e4`）+ Phase 2 INCR 族（`6967088`）。
- 2026-09-18：Phase 1/2 在 Go 1.22.5 实机验收通过（修 3 处缺陷，`c7e7e29`）；Phase 3 实机开发 + 全量测试通过 + 真实 TCP 冒烟（写入→杀进程→重启→状态回放一致，TTL 300s→288s 按真实时间衰减）。
- 2026-09-18：Phase 3.5（`50f4d5b`，Set 基础 + 运维命令）；Phase 4（`92817ee`）跳表 ZSet + Set 补差 + SPOP 重写，全量测试 + 25/25 TCP 冒烟（含重启回放后 SPOP 成员不复活）。
- 2026-09-18：Phase 5（`75211cd`+`c247c8d`+`c974934`）AOF 重写 + pub/sub + 压测基线；全量测试 3 轮通过 + 25/25 TCP 冒烟（重写压缩 468→255B、重启回放一致、pub/sub 不落盘、二次重启 DBSIZE=8）。
- 2026-09-18：Phase 6（`9892632`+`292accc`）MULTI/EXEC 事务 + RDB 快照 + ZRANGEBYSCORE 族 + MGET/MSET/ZRANDMEMBER；全量测试 3 轮通过 + 34/34 TCP 冒烟（EXECABORT 不执行、RDB 重启回放含 TTL、CRC 损坏拒启、事务 AOF 块重启一致）。
- 2026-09-18：Phase 7 主从复制：全量测试 3 轮通过（新增 replication_test.go 7 用例 + rdb 字节级 round-trip）+ 32/32 双实例 TCP 冒烟（五类型 + TTL 传播、事务块传播、SPOP 确定化、杀副本重启重同步、杀主库重启重连、REPLICAOF NO ONE 晋升）；冒烟另暴露并修复 3 处回归（见 Phase 7 章节）。
- 2026-09-18：Phase 8 WATCH 乐观锁 + 部分重同步（repl-backlog/PSYNC CONTINUE/REPLCONF ACK）+ Lua 脚本（EVAL/EVALSHA/SCRIPT 效果复制）+ bench 扩展（EVAL/EVALSHA/CAS）：全量测试 3 轮通过（新增 eval_test.go 8 用例 + psync_test.go 4 用例）+ 双实例 TCP 冒烟（五类型 + TTL 传播、EVAL 效果传播与重启恢复、WATCH 中止/UNWATCH 恢复、SCRIPT 族、部分重同步主/副日志断言、副本重启走全量、ACK offset 收敛）；冒烟另暴露 EXEC 乐观锁中止回复应为 `*-1` null array（原为 `$-1`，已修 resp 编码器并更新回归测试）。
- 2026-09-18：Phase 9 SCAN/SSCAN/HSCAN/ZSCAN 游标族 + LMOVE/LINSERT/LPOS + appendfsync always/everysec/no + EXPIRE NX/XX/GT/LT + SET NX/XX/GET/KEEPTTL 收尾 + OBJECT ENCODING：全量测试 3 轮通过（新增 27 个测试函数）+ 83 断言 TCP 冒烟（SCAN 分页/MATCH/TYPE、三集合 SSCAN、OBJECT、LMOVE 轮转/跨 key、LPOS 选项、SET/EXPIRE 选项矩阵、everysec 重启回放、副本传播）。单测另暴露并修复 2 处缺陷：cmdLMove 参数下标错位（LMOVE 全体 syntax error + WATCH 触碰连带失效）、canonicalWrite 把 `SET NX GET` 新 key 成功（旧值 null）误判为 NX 失败不落盘（重启丢 key，dispatch 前 key 存在性快照消歧，5 调用点统一）。基准复测：无 AOF SET 250k/GET-p16 624k/EVAL 9.5k/CAS 25.9k ops/s；AOF everysec 38.1k、always 963 ops/s（Windows fsync p50 52ms）。
