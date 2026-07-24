# Changelog

本文件记录 TaskQ 的重要变更。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

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
