# 测试用例说明

本文档说明当前仓库里已有自动化测试所覆盖的行为，以及尚未写成自动化用例、但系统必须满足的行为。

判定口径：

- `go test ./...` 全部通过
- `go test -race ./...` 全部通过
- 下文列出的“通过标准”全部成立

## 当前结果

截至当前代码状态，以下命令应通过：

```bash
go test ./...
go test -race ./...
```

## 一、仓库级约束

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 旧版 `PartitionedQueue` 方案已经完全移除 | 旧文件不存在，`queue/physical_queue.go` 不再出现旧转发逻辑标记 | `TestLegacyPartitionedQueueArtifactsAreRemoved` |
| README 不再描述已经删除的旧模型 | README 不出现旧类型名、旧转发术语、旧示例配置 | `TestReadmeDoesNotMentionLegacyPartitionedQueueModel` |
| 仓库内不使用 `log.Printf` | 所有 Go 文件中都不出现 `log.Printf(` | `TestRepositoryDoesNotUseLogPrintf` |

## 二、Queue 包

### 2.1 存储层

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 队列元数据可创建并正确读回 | `CreateQueue` 后 `GetQueue` 返回一致的 `range_id`、`next_task_id` | `TestCreateAndGetQueue` |
| `UpdateQueue` 必须遵守 CAS | 正确的旧 `range_id` 可以更新，过期的 `range_id` 必须返回 `ErrConditionFailed` | `TestUpdateQueue_CAS` |
| 任务写入后可按范围读取 | `CreateTasks` 后 `GetTasks` 返回正确数量和顺序 | `TestCreateAndGetTasks` |
| 已派发任务默认不可再次读取 | `DispatchTask` 后 `GetTasks` 不再返回该任务，`UnDispatchTask` 后恢复可读 | `TestDispatchAndGetTasks` |
| 超时派发任务可以被重置 | `ResetTimedOutTasks` 返回超时任务 ID，并使任务重新可读 | `TestResetTimedOutTasks` |
| 批量删除只删除目标范围 | `DeleteTasks(maxTaskID)` 只删除小于等于该值的任务 | `TestDeleteTasks` |
| 单条删除只删除目标任务 | `DeleteTask(taskID)` 后该任务不可再读 | `TestDeleteTask` |
| 对不存在任务的派发/回退/删除必须返回错误 | 缺失行时分别返回 `errTaskNotFound` | `TestDispatchTaskMissingRowReturnsError`, `TestUnDispatchTaskMissingRowReturnsError`, `TestDeleteTaskMissingRowReturnsError` |
| 分区之间必须隔离 | 同名队列不同分区互不串读 | `TestPartitionIsolation` |

### 2.2 底层 Lease 存储兼容接口

这组用例只验证 `queue.Store` 里保留的 lease 读写接口仍然可用，方便兼容旧调用方。
当前运行时分区的启动、停止、交接，已经改成由 `matching` 内部的 `dbleader` 锁控制。

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 首次获取 lease 时可自动建行 | 空库上 `AcquireLease` 可成功创建并返回分区元信息 | `TestAcquireLease_Basic`, `TestAcquireLease_AutoCreate` |
| 同一分区同一时刻只允许一个 owner 持有 lease | 第二个 owner 获取同一分区时返回 `ErrLeaseHeld` | `TestAcquireLease_Conflict` |
| 同一个 owner 可以重复获取作为续期 | 同 owner 再次 `AcquireLease` 成功 | `TestAcquireLease_SameOwnerRenew` |
| 过期 lease 可以被新 owner 接管 | TTL 到期后新 owner 可以成功获取 | `TestAcquireLease_Expired` |
| 正确 owner 可以释放 lease | 释放后其他 owner 能成功接管 | `TestReleaseLease` |
| 错误 owner 不能释放 lease | 返回 `ErrLeaseHeld` | `TestReleaseLeaseWrongOwnerReturnsErrLeaseHeld` |
| 续期必须校验 owner | 正确 owner 续期成功，错误 owner 续期失败 | `TestRenewLease` |
| 并发首次建分区时只允许一个成功拿到 lease | 并发竞争下只能成功或返回 `ErrLeaseHeld`，不能出现异常错误 | `TestAcquireLease_ConcurrentFirstCreate` |

### 2.3 投递状态机

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 已派发任务必须进入 outstanding 集合 | `markDispatched` 后任务在 outstanding 中，并且不再被视为“可直接读取” | `TestDeliveryStateMarkDispatchedTracksOutstanding` |
| `ack_level` 只能按连续区间推进 | 先 ack 后序任务时不推进；前序补齐后一次性推进到连续末尾 | `TestDeliveryStateAckAdvancesLevel` |
| `Nack` 后任务必须进入重投递队列 | `beginNack/finishNack` 后任务进入 `redelivery` | `TestDeliveryStateNackMovesTaskToRedelivery` |
| 红投递队列中的任务会阻塞 `ack_level` 推进 | 前面任务未完成重投递前，后续 ack 不得推进 `ack_level` | `TestDeliveryStateRedeliveryBlocksAckLevel` |
| 重复 ack / nack 必须被拒绝 | 二次 ack 或 nack 返回 `ErrTaskNotOutstanding` | `TestDeliveryStateRejectsDuplicateAckAndNack` |

### 2.4 PhysicalQueue 行为

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 基本入队和取队正常 | 写入后可以取到任务和 token | `TestPhysicalQueue_AddAndPoll` |
| waiter 存在时支持即时匹配 | 先挂起等待，再写任务，等待方能马上收到 | `TestPhysicalQueue_SyncMatch` |
| `Ack` 完成确认，`Nack` 触发重投递 | nack 后同一任务可以再次被取出 | `TestPhysicalQueue_AckAndNack` |
| 新建队列本身不能直接接收写入 | 只有 writer task 跑起来后才能成功 `AddTask` | `TestPhysicalQueue_NewQueueDoesNotAcceptWritesUntilWriterTaskRuns` |
| 阻塞等待在无任务时不会误返回 | 空队列下 waiter 在超时前不应收到任务 | `TestPhysicalQueue_BlockingPoll` |
| 已经可读的任务不会在注册 waiter 时丢失 | waiter 注册后能立刻拿到已经存在的任务 | `TestPhysicalQueue_RegisterWaiterImmediatelyGetsReadableTask` |
| 队列关闭后必须停止对外服务 | 关闭后不再返回任务，`Ack/Nack` 返回 `ErrQueueClosed` | `TestPhysicalQueue_CanceledQueueStopsServingAndRejectsAckNack` |
| 非法 token 必须被拒绝 | 无法解析的 token 返回 `ErrInvalidToken` | `TestPhysicalQueue_AckRejectsInvalidToken` |
| 分区不匹配的 token 必须被拒绝 | token 中的分区号与当前分区不一致时返回 `ErrInvalidToken` | `TestPhysicalQueue_AckRejectsTokenForDifferentPartition` |
| 重复 ack / nack 必须被拒绝 | 第二次调用返回 `ErrTaskNotOutstanding` | `TestPhysicalQueue_AckRejectsDuplicateToken`, `TestPhysicalQueue_NackRejectsDuplicateToken` |
| 派发失败不能导致任务被跳过 | 某次派发失败后，后续重试仍必须能拿到该任务 | `TestPhysicalQueue_TryPollDoesNotSkipTaskAfterDispatchFailure` |
| GC 不能在未启动时自己运行 | 未显式启动 GC task 前，`ackLevel` 不得自己推进 | `TestPhysicalQueue_GCDoesNotRunUntilExplicitlyStarted` |
| GC 启动后必须真正生效 | 显式启动 GC task 后，`ackLevel` 会推进并落库 | `TestPhysicalQueue_GCRunsAfterExplicitStart` |
| 可见性扫描不能在未启动时自己运行 | 未显式启动扫描 task 前，超时任务不得自己回到可取状态 | `TestPhysicalQueue_VisibilityScanDoesNotRunUntilExplicitlyStarted` |
| 可见性扫描启动后必须真正生效 | 显式启动扫描 task 后，超时任务会重新可取 | `TestPhysicalQueue_VisibilityScanRunsAfterExplicitStart` |

### 2.5 Queue 回归用例

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 非 CAS 存储错误不能被误判成所有权丢失 | 原始错误类型必须保留 | `TestBugRepro_UpdateQueueKeepsNonCASFailures` |
| 同一条任务不能被并发重复发放 | 并发 `TryPoll` 时，同一任务最多只有一个成功结果 | `TestBugRepro_TryPollShouldNotDeliverSameTaskConcurrently` |
| 重启或重新接管后不能丢掉库里已有未派发任务 | 新建 `PhysicalQueue` 后仍能继续读取旧任务 | `TestBugRepro_NewQueueInstanceShouldReadExistingUndispatchedTasks` |
| 两个 `PhysicalQueue` 实例交接窗口内，同一条任务不能被同时发出去 | 两个实例并发拉取时，同一任务最多只允许一个成功结果 | `TestBugRepro_LeaseHandoverWindowDoesNotDoubleDeliverSameTask` |

## 三、Matching 包

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 基本写入与拉取正常 | `AddTask` 后 `PollTask` 能拿到同一任务 | `TestMatchingServer_AddAndPoll` |
| 空队列非阻塞拉取返回空 | `PollTask` 返回 `Empty=true`，而不是错误 | `TestMatchingServer_PollEmpty` |
| `AckTask` 可以确认已取出的任务 | 已取出的 token 可以正常 ack | `TestMatchingServer_AckTask` |
| `NackTask` 会触发重投递 | nack 后再次拉取能拿到同一任务 | `TestMatchingServer_NackRedelivery` |
| 长轮询能在任务到来后立即返回 | 任务延后写入时，挂起中的 `PollTask` 能及时返回 | `TestMatchingServer_LongPoll` |
| 服务停止时，进行中的长轮询不能卡死 | `Stop` 必须能返回，进行中的长轮询必须被中断 | `TestMatchingServer_StopUnblocksInFlightLongPoll` |
| `AckTask` / `NackTask` 必须校验 token 分区 | token 分区和请求分区不一致时返回 `InvalidArgument` | `TestMatchingServer_AckTaskRejectsTokenPartitionMismatch`, `TestMatchingServer_NackTaskRejectsTokenPartitionMismatch` |
| 已关闭的分区实例必须能重新加载 | 旧实例关闭后再次访问，会重新创建新实例 | `TestPartitionManager_ReloadsClosedPartition` |
| 分区实例启动必须等到真正可服务后才能对外返回 | 首次加载分区时，请求不会拿到“句柄已建好但还没真正启动”的空实例 | `TestMatchingServer_AddAndPoll`, `TestPartitionManager_ReloadsClosedPartition` |
| 分区服务 readiness 必须覆盖 writer、GC、可见性扫描三个后台 task | 分区只有在这些 task 都进入运行态后才算真正 ready | `TestMatchingServer_AddAndPoll`, `TestFrontend_EndToEnd` |
| 非 owner 节点不能接收该分区请求 | 返回 `FailedPrecondition` | `TestMatchingServer_NotOwner` |
| 未配置队列必须被拒绝 | 对未知队列写入返回 `NotFound` | `TestBugRepro_AddTaskRejectsUnknownQueue` |
| 越界分区必须被拒绝 | 对非法分区写入返回 `InvalidArgument` | `TestBugRepro_AddTaskRejectsOutOfRangePartition` |
| 成员查询短暂失败时不能直接卸掉已服务分区 | 失败后保留当前已加载分区，避免无谓抖动 | `TestBugRepro_ReconcileKeepsLoadedPartitionsWhenOwnershipLookupFails` |

## 四、Frontend 包

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 端到端写入、拉取、确认可用 | `Enqueue -> Dequeue -> Ack` 全链路正常 | `TestFrontend_EndToEnd` |
| `Nack` 后任务可再次被消费 | 再次 `Dequeue` 得到相同任务 | `TestFrontend_NackRedelivery` |
| 多次写入不能丢任务或重复计数 | 连续写入多条任务后，全部都能被消费一次 | `TestFrontend_MultipleEnqueue` |
| 多 worker 并发消费最终能吃完所有任务 | 并发 worker 最终消费数等于生产数 | `TestFrontend_WorkerConcurrent` |
| 长轮询 worker 能在任务到来后及时被唤醒 | 不需要等满超时时间 | `TestFrontend_WorkerLongPoll` |
| 空队列时应返回明确的“暂无任务”错误 | 返回 `ErrNoTask` | `TestFrontend_DequeueEmpty_ReturnsErrNoTask` |
| 所有分区都失败时，不能把真实故障伪装成空队列 | 必须把底层错误返回给调用方 | `TestFrontend_DequeueReturnsUnderlyingErrorWhenAllPartitionsFail` |
| 同一个 resolver 上允许存在多个 Frontend 实例 | 多实例创建成功，不互相冲突 | `TestFrontend_AllowsMultipleInstancesOnSameResolver` |
| 路由信息变更后，写入可以自动刷新并重试 | 旧节点停掉并释放锁后，`Enqueue` 经过刷新仍能成功落到新节点 | `TestFrontend_EnqueueRetriesAfterResolverRefresh` |
| 路由信息变更后，确认也可以自动刷新并重试 | 旧节点停掉并释放锁后，`Ack` 经过刷新仍能成功落到新节点 | `TestFrontend_AckRetriesAfterResolverRefresh` |
| 路由信息变更后，拒绝确认也可以自动刷新并重试 | 旧节点停掉并释放锁后，`Nack` 经过刷新仍能成功落到新节点 | `TestFrontend_NackRetriesAfterResolverRefresh` |
| 调用方取消后，`Dequeue` 必须尽快退出 | 调用方 context 超时或取消后，应快速返回对应取消错误 | `TestFrontend_DequeueReturnsPromptlyWhenCallerContextCanceled` |
| 长轮询不能漏掉其他分区后来出现的任务 | 某个分区后到任务时，整体 `Dequeue` 仍能收到 | `TestBugRepro_DequeueLongPollMissesOtherPartitions` |
| 所有权转移类错误必须进入重试集合 | `Aborted` 错误会被识别为可重试 | `TestBugRepro_IsRetryableForOwnershipLoss` |

## 五、Membership 包

### 5.1 HostInfo 与 HashRing

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| `HostInfo` 的地址与身份返回正确 | `GetAddress` 和 `Identity` 一致且不变 | `TestHostInfoBasic` |
| draining 标记必须可识别 | draining 为 true 的节点必须被识别为 draining | `TestHostInfoDraining` |
| 相同状态校验值一致，不同状态校验值不同 | 相同输入得到相同 checksum，状态变化会改变 checksum | `TestHostInfoChecksum` |
| 空哈希环不能返回伪结果 | `Lookup` 找不到，`LookupN` 返回空 | `TestHashRingEmpty` |
| 同一个 key 的查找结果必须稳定 | 多次 `Lookup` 结果一致 | `TestHashRingLookupDeterministic` |
| `LookupN` 的第一个结果必须等于 `Lookup` | 首副本规则一致 | `TestHashRingLookupNFirst` |
| 删除节点后不应再命中已删除节点 | 删除后任何 key 都不能再返回该节点 | `TestHashRingRemoveMember` |
| 哈希分布不能明显失衡 | 多 key 打散后，每个节点都能分到基本量级的 key | `TestHashRingDistribution` |

### 5.2 ServiceResolver

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 正常成员集下可以完成路由 | `Lookup` 返回非空地址 | `TestServiceResolverLookup` |
| 空成员集必须返回明确错误 | 返回 `ErrInsufficientHosts` | `TestServiceResolverEmpty` |
| 总成员数与可用成员数必须区分 | draining 节点计入总数，但不计入可用数 | `TestServiceResolverMemberCount` |
| 成员变更必须发出事件 | 监听器能收到 `HostsAdded` 变化 | `TestServiceResolverChangedEvent` |
| 重复停止不得崩溃 | 第二次 `Stop` 不能 panic | `TestServiceResolverDoubleStop` |
| 重复监听器名必须被拒绝 | 返回 `ErrListenerAlreadyExist` | `TestServiceResolverDuplicateListener` |
| draining 节点不能继续参与正常路由 | `Lookup` 不能返回 draining 节点 | `TestBugRepro_LookupShouldSkipDrainingHosts` |

### 5.3 Monitor

| 必须满足的行为 | 通过标准 | 对应用例 |
|---|---|---|
| 启动后可初始化并停止 | `Start` 后可正常初始化，`Stop` 可正常结束 | `TestMonitorStartStop` |
| 可按 service 获取 resolver | 已配置 service 可取到 resolver，未知 service 返回 `ErrUnknownService` | `TestMonitorGetResolver` |
| 设置 draining 后可用成员数下降 | 本节点 draining 后，`AvailableMemberCount` 变为 0 | `TestMonitorSetDraining` |
| `EvictSelfAt` 返回值不能重复计算传播时间 | 返回值不应比“计划离开时间 + 传播时间”更大一截 | `TestEvictSelfAtReturnValue` |
| 心跳会持续写入存储 | 启动一段时间后，upsert 次数大于 0 | `TestMonitorHeartbeat` |
| 多 service 场景下，各 service 都会注册和可被路由 | 多 service 都能写心跳并被 resolver 查到 | `TestMonitorMultiServiceNames` |
| 未来加入时间必须保留 | `buildUpsertRequest` 不能把未来 `joinTime` 提前清空 | `TestMonitorBuildUpsertRequestPreservesFutureJoinTime` |
| 计划未来下线时，不能立刻进入 draining | 到达下线时间之前，最后一次 upsert 仍应保持可服务 | `TestBugRepro_EvictSelfAtDoesNotDrainImmediatelyForFutureDeadline` |
| 可达成员列表必须去重 | 同地址多 service 只返回一条 | `TestBugRepro_GetReachableMembersDeduplicatesAddresses` |
| 启动阶段遇到短暂存储失败时必须重试恢复 | 在有限次瞬时失败后仍能成功初始化 | `TestBugRepro_StartShouldRetryTransientMembershipStoreFailures` |
| 运行中成员同步持续失败时必须保活 | 超过重试次数后不停机，沿用上一次 ring 快照继续服务并后台重试 | `TestBugRepro_RuntimeSyncFailuresKeepMonitorAlive` |
| 运行中心跳持续失败时必须保活 | 超过重试次数后不停机；写不进去只影响「别人看不见我」，对等节点按心跳超时切走流量 | `TestBugRepro_RuntimeHeartbeatFailuresKeepMonitorAlive` |

## 六、当前没有自动化用例、但必须满足的行为

下列行为当前没有现成自动化用例，或覆盖还不完整。它们仍然是必须满足的要求。

| 必须符合的行为 | 当前状态 | 建议补测方式 |
|---|---|---|
| `PhysicalQueue.Close()` 必须在可接受时间内返回，不能因为底层存储调用卡死而一直挂住 | 未覆盖 | 用会阻塞写入、清理或超时扫描调用的假存储，启动队列后调用 `Close()`，断言它在预设超时时间内返回；即使后台调用仍未结束，也不能无限等待 |
| 启动阶段如果重试耗尽，`WaitUntilInitialized` 必须返回明确失败，而不是一直挂住 | 未覆盖 | 让 `UpsertMember` 或 `GetActiveMembers` 持续失败直到超过最大重试次数，断言 `WaitUntilInitialized` 返回非空失败，并且不会超过调用方设定的等待时间 |
| 长轮询等待时间当前按设计不设服务端上限，必须遵从调用方 context、任务到达或服务停止这三类退出条件 | 未覆盖 | 发起超长 `PollTask/Dequeue`，分别验证任务到达、调用方取消、服务停止三种退出路径 |
| payload 当前按设计不设应用层大小上限，至少要保证在系统允许范围内不会被代码层主动拒绝 | 未覆盖 | 使用多种大小的 payload 做写入、读取与确认，记录实际可工作范围 |
| 默认明文连接是当前设计选择，至少要保证不显式配置 TLS 时端到端仍可工作 | 未覆盖 | 用默认 `DialOpts` 建连，验证 `frontend` 与 `matching` 全链路可用 |
