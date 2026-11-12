# Changelog

本文件记录 TaskQ 的重要变更。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## 2026-11-12 调用方离开时在途的 lease 不再隐身；写入换 owner

Tenuto 的无故障压测（3 个 matching 副本、Workflow Task 长轮询每秒以新期限重新 poll）里 8 个任务已入队却没有被
任何消费者领到，60s visibility timeout 之前由 Tenuto 的兜底补投；故障注入里 owner 被杀后约 250 次写入以
`frontend: enqueue: max retries exceeded` 失败。

### 修复

- 在途 lease 丢失：owner 在应答前检查 ctx，检查通过之后、应答到达之前调用方的期限到期或取消 RPC，gRPC 丢掉应答，
  lease 在 owner 那里已是已投递，等 visibility timeout 才重新出现。owner 的期限按收到请求的时刻计算，比调用方
  晚一个单程延迟；waiter 按 FIFO 取任务，最先拿到任务的正是等得最久、最接近期限的 waiter。Frontend 的 poll RPC
  改为不随调用方 ctx 取消（`frontend/frontend.go`）：长轮询的 `PollTimeoutMs` 不超过 ctx 的剩余时间，由 owner
  按时结束；已有 owner 胜出或 ctx 被取消时，用新的 `CancelPoll` RPC 请 owner 结束轮询并原地归还已领取的 lease
  （`PollQueueRequest.poll_id`，`matching/poll_queue.go` 的 `pollRegistry`）；读回的 lease 已无人要时立即以
  `undelivered` 归还。竞速败者在后台归还，胜者不再等 2ms 的收集窗口。
- `DispatchTask` 不随 poll 的 ctx 取消（`queue/task_runtime.go`）：MySQL 驱动可能在 UPDATE 已提交之后才报告取消，
  此时行已隐藏而内存不知道这次投递（日志 `queue tryPoll failed … dispatch task N: context canceled`）。调用方已
  离开时的读取取消不再记 ERROR。
- 写入换 owner：`EnqueueWithOptions` 在同一个分区上重试 3 次，owner 宕机、成员表尚未剔除它期间全部失败。任务可以
  写进任一可写分区（`RoutingKey` 定向由消费侧按 key 过滤，与分区无关），失败的 owner 在本次调用内不再选择，重试
  换到其他 owner 的分区；只剩失败过的 owner 时退避后重新解析路由。错误信息带上最后一次失败的原因。

### 测试

- `frontend`：`TestFrontend_LeaseIssuedAfterCallerDeadlineIsReturned`、`TestFrontend_LeaseIssuedWhileCallerCancelsIsReturned`
  （owner 在调用方离开后签发 lease，lease 必须以 undelivered 归还）、`TestFrontend_EnqueueFailsOverToAnotherOwner`；
  修复前三项都失败。`TestFrontend_MultipleOwnersReturnSimultaneousLoser` 改为等待后台归还。
- `queue`：`TestPhysicalQueue_PollerCancelDuringDispatchKeepsTaskDeliverable`（驱动在提交后报告取消），修复前任务隐身。
- `matching`：`CancelPoll` 结束等待中的轮询、先于 `PollQueue` 到达的墓碑、被结束的轮询原地归还 lease。

## 2026-11-12 respool 的老行与新行同事务删除

Tenuto 的多副本故障注入里，一条被消费方反复 `Nack` 的任务在 `taskq_tasks` 里变成 88 份。respool 写入新行后老行仍以
`dispatched_at` 非空留在库里，只在 owner 的内存里记为已确认，等 GC 把 `ackLevel` 推过它才删除。分区易主（锁续约
超时）时新 owner 从库里的 `ack_level` 开始，没有这份内存：`ResetTimedOutTasks` 把这些老行当作超时未确认的投递
重新放出，每条老行与它的新行各投递一次。反复 `Nack` 的任务每 respool 一次留下一条老行，每次易主都按老行数翻倍。

### 修复

- `TaskRow.RespoolOf`：respool 重写的行带上老行的 `taskID`；`CreateTasksAndAdvanceRange` 在写入新行、推进 range 的
  同一事务里删除老行（`queue/writer.go`、`store/gormstore/store.go`）。
- 新 `TaskStore.MinTaskIDAfter`：返回 `ackLevel` 之上最小的现存 `taskID`。GC 每轮先让 `ackLevel` 越过这之前（且不超过
  `maxReadLevel`）在库里已没有行的 ID，本进程仍在投递、重写或待重投的任务挡住推进（`queue/gc.go`、
  `queue/task_runtime.go`、`queue/delivery_state.go`）。接管分区的进程由此越过老 owner 删除的老行。
- 已确认（`Ack`）但 `ackLevel` 尚未落库的行在易主后仍会重投一次：确认只在内存里，这是 at-least-once 的既有语义，
  不在本次范围内；`MaxAttempts` 耗尽丢弃的任务同样保留老行。

### 测试

- `queue`：`TestPhysicalQueue_TakeoverDoesNotRedeliverRespooledTask`（respool 后不落库 `ackLevel` 即换 owner：只投递
  新行一次，持久化 `ackLevel` 越过已删除的老行）。修复前投递两次；只删除老行而不越过空洞时 `ackLevel` 停在 0。
- `store/gormstore`：`TestSQLiteStoreRespoolRetiresOldRowAtomically`（fence 失败时老行保留，成功时只剩新行，
  `MinTaskIDAfter` 的边界）；`integration` 标签的 MySQL 测试补充同一断言。

## 2026-08-04 为 `GetActiveMembers` 补索引

SQL 审查用 EXPLAIN ANALYZE 实测发现服务发现的高频查询走全表扫描。

`MembershipStore.GetActiveMembers`（`membership.go:90`）的条件是
`WHERE heartbeat_at >= ? AND expires_at > ?` —— **不带 `service_name`**，
而既有 `idx_membership_active (service_name, heartbeat_at, expires_at)`
的前缀列正是 `service_name`，因此用不上；`idx_membership_expires
(expires_at)` 单列只覆盖一半条件。实测 5000 行的表扫满 5000 行才返回
128 行，随实例数线性劣化。

新增 `idx_membership_heartbeat (heartbeat_at, expires_at)` 覆盖该查询的
两个范围条件。审查实测扫描行数从 5000 降到 271。

存量库（本仓无 migration 机制）请手工执行：

```sql
ALTER TABLE `taskq_membership_members`
  ADD INDEX `idx_membership_heartbeat` (`heartbeat_at`, `expires_at`);
```

`idx_membership_active` 暂予保留：审查实测它从未被优化器选中，但删除属于
独立的清理决策，需确认无其他查询依赖后再做，本次不动。

## 2026-08-04 新增 `schema.sql` 建库脚本

### 新增

`store/gormstore/schema.sql`：6 张表（`taskq_queue_info`、`taskq_tasks`、
`taskq_partition_config`、`taskq_partition_lock`、`taskq_partition_lock_members`、
`taskq_membership_members`）的完整 `CREATE TABLE` 语句，供生产受控建库使用。

此前仓库没有任何 SQL 文件，建表只能靠 `AutoMigrate`，生产环境无法把 schema 变更纳入
受控发布流程。

**与 `AutoMigrate` 的关系是并存而非替代**：测试与示例继续走 `Config{AutoMigrate: true}`，
生产建议 `AutoMigrate: false` 并由 DBA 执行脚本。

脚本内容对真实 MySQL 8 跑 `AutoMigrate` 后 `SHOW CREATE TABLE` 导出，与 `AutoMigrate`
建出的 DDL 逐列逐索引一致（含索引名），故按脚本建库后再跑 `AutoMigrate` 不会补出
重复索引。三张可配表用默认表名，经 `Config` 改名时需同步修改脚本。

### 测试

新增 `store/gormstore/schema_guard_test.go`：用 GORM `schema.Parse` 反射模型列，
与解析 `schema.sql` 得到的列做**表级 + 列级双向比对**，任一方向漂移即失败。
不依赖数据库，随默认 `go test` 运行。

## 2026-08-03 表名统一加 `taskq_` 前缀

### 破坏性变更

TaskQ 的三张表加上 `taskq_` 前缀，与已有的 `taskq_partition_lock` /
`taskq_partition_lock_members` / `taskq_membership_members` 命名对齐，避免与宿主库中的
同名表（尤其是极易撞名的 `tasks`）冲突。

| 旧表名 | 新表名 |
| --- | --- |
| `queue_info` | `taskq_queue_info` |
| `partition_config` | `taskq_partition_config` |
| `tasks` | `taskq_tasks` |

`taskq_partition_lock`、`taskq_partition_lock_members`、`taskq_membership_members`
三张表本就带前缀，**未改动**；`gormstore.Config` 的 `LockTable` / `MemberTable` /
`MembershipTable` 仍可配，默认值不变。

### 升级步骤

TaskQ 没有内置 migration 机制（建表走 GORM `AutoMigrate`），存量库需**手工执行**改名。
停写后执行：

```sql
RENAME TABLE
  `queue_info`       TO `taskq_queue_info`,
  `partition_config` TO `taskq_partition_config`,
  `tasks`            TO `taskq_tasks`;
```

注意事项：

- `RENAME TABLE` 会保留数据、索引与自增值，是原子操作，但**不向后兼容**：旧版本进程连上
  改名后的库会因找不到 `tasks` 等表而失败。请先停掉全部 TaskQ 进程再改名，避免新旧版本
  混跑。
- 若依赖 `AutoMigrate`，**不要**跳过改名直接启动新版本 —— 那会建出一套空的
  `taskq_*` 表，旧数据留在原表里成为孤儿。
- 回滚即反向 `RENAME TABLE`。

## 2026-08-02

修复 matching 层「reconcile 持写锁跨阻塞式关停冻结全节点」的 HIGH 级缺陷，并为
「ackLevel 长期停滞导致 GC 停摆」补上可观测性。

### 修复

- **reconcile 持 `pm.mu` 写锁跨分区关停，冻结整个节点并触发重复投递**：
  `partitionManager.reconcile` 原在写锁内逐个 `handle.close()`，而 `close()` 会同步执行
  `manager.Offline()`（DB `Release`，dbleader 侧 2s 超时）再 `manager.Stop()`（排空分区
  goroutine）。ring 重排时若有 N 个分区待卸载且恰逢 DB 抖动，写锁被持有约 N×2s，期间所有
  `AddTask` / `PollTask` / `AckTask` 阻塞在 `pm.mu.RLock()` 上 → 整节点冻结 → `AckTask`
  超时 → 任务被判未确认而**重复投递**。修复为「锁内收集待关 handle 并从 map 摘除、锁外并发
  close」，与同文件既有的 `unloadPartition` 对齐。摘除后并发 `getOrLoadPartition` 重建的是
  另一个 handle 实例，因此不会误关新建句柄。（`matching/partition_mgr.go`）
- **`partitionManager.close()` 同形问题**：整体关停同样持锁串行。改为锁内摘除、锁外用
  `WaitGroup` 并发关闭，保留「返回前全部分区已关完」的关停语义，但不再让关停期间的 RPC 堵在
  `pm.mu` 上。（`matching/partition_mgr.go`）

### 新增

- **ackLevel 停滞可观测性**：`ackLevel` 被一条长期无人消费的任务钉住时，GC 无法回收任何行
  （GC 只删 `task_id <= ackLevel`），`tasks` 表无界增长，缩容 drain 判定
  （`ack_level == next_task_id-1`）也永不满足 —— 此前该状态**完全静默**。现由 GC 侧检测水位
  停滞超过 1 分钟后告警一次，报告挡路任务的 `task_id`、所处状态
  （`outstanding` / `respooling` / `redelivery` / `unconsumed`）、`routing_key` 与滞留时长。
  （`queue/gc.go`、`queue/delivery_state.go`）

  **未改变投递语义**：定位后确认「redelivery 项钉住 ackLevel」并非根因 —— 一条带 routing key
  且从未被任何 poller 取走的任务同样把水位钉死（此时 `redelivery` 为空），因此丢弃或重写
  redelivery 项并不能解除钉住，反而会破坏 F3 铁律（新行未落库前绝不 ack 老行）。零值
  `RoutingDeadline` 表示**永久**按 routing key 定向是既有设计（见 README），未订阅该 key 属于
  使用方配置问题，正解是设置 `RoutingDeadline` 使其到期回落为普通任务。

### 测试

- `matching/reconcile_unload_blocking_test.go`：注入慢 `Release` 的 LockStore 复现 DB 抖动，
  断言 reconcile 卸载 6 个慢分区期间并发 RPC 不被 `pm.mu` 长时间阻塞（修复前实测阻塞
  1.66s，修复后 375ns），并断言 `close()` 并发关停且返回前全部分区已关闭。
- `queue/ack_level_stall_test.go`：断言 redelivery 滞留项、从未被消费的 keyed 任务都能被
  定位到，正常推进时不误报，以及 GC 停滞检测仅在超过阈值后告警一次。

## [Unreleased] — 2026-07-23

引入 **respool 投递重试机制**：任务投递失败时以新 `taskID` 重写一条新任务再确认老行，
解除单条反复失败任务对 `ackLevel` 的永久冻结。机制对齐 Temporal matching 层的
`respoolTaskAfterError`。

### 新增

- **respool 机制**：`Nack` / 可见性超时不再原地放回，而是以更高的 `taskID` 重写任务并
  `attempt+1`，老行在新行落库后才 ack。`ackLevel` 不再被单条毒丸任务钉死，`acked` 内存与
  `tasks` 表持续 GC。（`queue/task_runtime.go`、`queue/delivery_state.go`、`queue/writer.go`）
- **`tasks.attempt` 列**：持久化每条任务的投递失败次数（首次写入为 0），进程重启 / 分区
  易主后计数不丢。（`queue/store.go`、`store/gormstore/store.go`）
- **`matching.Config.MaxAttempts`**：单任务投递次数上限。**默认 `0` 表示不限次数**；设为
  正数才启用上限，超过后丢弃任务并记录一条 `error` 日志。（`matching/matching.go`，经
  `partition_mgr.go` 贯通到 `queue.NewPhysicalQueue`）
- **`PhysicalQueue.ReturnLease`** 与 gRPC `NackTaskRequest.undelivered` 字段：用于归还
  “从未交付给消费者”的 lease（多分区 poll 竞速败者、调用方取消），**原地退回、不消耗
  attempt**。（`queue/physical_queue.go`、`matching/matching.proto`、`matching/server.go`）
- `deliveryState.respooling` 状态集，与 `redelivery` 一样阻止 `ackLevel` 推进，直到新行落库。
- 生产迁移 SQL：`ALTER TABLE tasks ADD COLUMN attempt`（MySQL / SQLite，见 README 生产装配章节）。

### 变更 / 行为变化

> ⚠️ 相对上一版本，以下为**破坏性行为变化**，升级前请确认：

- **默认不再丢弃任务**：`MaxAttempts` 默认 `0`（不限）。毒丸任务的推荐处理方式是消费者
  **自行持久化后回 `Ack`**（而非 `Nack`），TaskQ 作为运输层不主动删除数据。
- **`Nack` 返回后任务不再立即可 poll**：新行由 writer 异步落库，需等下一轮批量 flush。
- **同分区消费顺序进一步弱化**：respool 把重试任务排到队尾（新 `taskID`）。TaskQ 本就不
  保证跨分区顺序。
- **老 token 错误码变化**：老 attempt 的 token 在其 delivery 退役后，`Ack` / `Nack` 返回从
  `ErrStaleToken` 变为 `ErrTaskNotOutstanding`。
- gRPC `NackTask` 新增 `undelivered` 字段（字段号 4）。向后兼容：旧节点忽略该字段、退化为
  旧行为，滚动升级安全。

### 修复

- **内部归还租约被误计 attempt 导致静默丢数据**：`Nack` 原为零成本原地退回，matching 与
  frontend 都将其用作“归还未使用 lease”的内部原语。respool 落地后每次归还都 `attempt+1`
  并重写新行，导致任务输掉若干次 poll 竞速后被静默删除。修复为内部归还统一走
  `ReturnLease` / `undelivered=true`，不消耗 attempt。（`matching/poll_queue.go`、
  `matching/server.go`、`frontend/frontend.go`）
- **可见性超时路径在锁内无上限逐条 DB 往返挂住分区**：批量超时时 `resetTimedOutTasks` 持
  `transitionMu` 做 N 次 `DispatchTask`。修复为单轮扫描封顶 500 条、下轮续扫
  （`VisibilityResetOptions.MaxReset`）。（`queue/task_runtime.go`、`store/gormstore/store.go`）
- **`close()` 与 respool 投递的检查-发送竞态**：可能导致 `respooling` 条目永不结算、
  `ackLevel` 卡死。修复为 respool 生产者纳入 `senders` 屏障，与 `enqueue` 对齐。
  （`queue/writer.go`）

### 测试

- 新增 `queue/respool_test.go`（T3–T10）、`queue/return_lease_test.go`，覆盖 respool 解冻
  ackLevel、attempt 保真、写失败可恢复、writer goroutine 内 nack 不死锁、channel 满降级、
  内部归还不计 attempt 等。
- `queue/delivery_state_test.go` 新增 respooling 阻塞 / 推进用例（T1/T2）。
- `store/gormstore/store_test.go` 新增 attempt 列往返（T11）。
- `matching/matching_test.go` 新增 `undelivered` 分派用例与「默认不限次数」回归。

### 文档

- README.md / README.html 补充 respool 语义、`attempt` 列、`MaxAttempts` 说明、状态机
  （新增 `respooling`）、行为变化，并明确 Frontend 为链接进 worker 的 **SDK**（非独立服务）。
