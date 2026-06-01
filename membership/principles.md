# Membership 原理说明

## 核心原理

`membership` 的本质是三件事：

1. 所有节点通过 DB 心跳形成成员视图。
2. 所有节点用同一份成员视图构建一致性哈希环。
3. 所有节点用同一个 `Lookup(key)` 规则算出 key 的目标节点。

只要成员视图一致，任意节点计算同一个 key 都会得到同一个 owner。

```text
MemberStore 活跃成员:
  10.0.0.1:7235
  10.0.0.2:7235
  10.0.0.3:7235

每个节点本地构建同一张 hashRing

Lookup("queue-a/3") => 10.0.0.2:7235
```

`membership` 自己只提供成员发现和路由判断。真正的请求发送由 `frontend` 做，真正的分区运行权由 `matching` 里的 `dbleader` 锁和 `queue` 里的 `rangeID` 机制保证。

## 成员发现

每个 TaskQ 节点启动 `Monitor` 后，会为自己承担的每个 service 写一条心跳记录：

```text
serviceName
rpcAddress
rpcPort
hostID
sessionStart
recordExpiry
draining
startAt
stopAt
```

写入路径：

```text
Monitor.Start()
  -> writeHeartbeats()
  -> MemberStore.UpsertMember(...)
```

后台续约路径：

```text
heartbeatLoop
  -> 每 10s + 0~5s jitter
  -> writeHeartbeats()
  -> MemberStore.UpsertMember(...)
```

其他节点通过轮询读取活跃成员：

```text
pollLoop
  -> 每 pollInterval
  -> syncMembers()
  -> MemberStore.GetActiveMembers(20s)
```

`GetActiveMembers(20s)` 的含义是：返回最近 20 秒内有心跳的节点。

## 哈希环如何工作

`ServiceResolver` 为每个 service 维护一张独立的一致性哈希环。

假设 matching service 有 3 个节点：

```text
A = 10.0.0.1:7235
B = 10.0.0.2:7235
C = 10.0.0.3:7235
```

每个真实节点会生成多个虚拟节点：

```text
A-0, A-1, A-2, ... A-199
B-0, B-1, B-2, ... B-199
C-0, C-1, C-2, ... C-199
```

这些虚拟节点按 hash 值排序成一个环。查找 key 时：

```text
hash("queue-a/3")
  -> 在环上顺时针找到第一个虚拟节点
  -> 返回这个虚拟节点对应的真实节点地址
```

代码路径：

```text
ServiceResolver.Lookup(key)
  -> hashRing.Lookup(key)
  -> farm.Fingerprint32(key)
  -> 二分查找 vnode
  -> 返回 HostInfo
```

虚拟节点的作用是让 key 分布更均匀，并且让节点增减时只迁移一部分 key。

## 成员变化如何收敛

每次 `Monitor.syncMembers()` 拉到新成员列表后，会调用：

```text
serviceResolver.setMembers(hosts)
```

`setMembers` 做三件事：

1. 对比旧成员和新成员，生成 `ChangedEvent`。
2. 用新成员重建一张新的 `hashRing`。
3. 用 `atomic.Value` 一次性替换旧的 `ringAndHosts`。

调用方读取时无锁：

```text
Lookup(key)
  -> atomic load 当前 ringAndHosts
  -> 在当前哈希环上查 key
```

成员变化会通知监听者：

```text
ChangedEvent:
  HostsAdded
  HostsRemoved
  HostsChanged
```

`matching` 收到事件后执行 `reconcile()`，重新判断本节点应该加载哪些分区。`frontend` 收到节点删除事件后清理旧连接。

## Draining 如何工作

优雅下线时，节点调用：

```text
EvictSelfAt(asOf)
  -> 到时间后 SetDraining(true)
  -> UpsertMember(draining=true)
```

其他节点轮询到这条记录后，会保留这个节点的成员信息，同时把它从哈希环里移除：

```text
host.draining = true
  -> hosts 视图里仍存在
  -> hashRing 里移除
  -> Lookup(key) 会落到其他节点
```

这样新请求会切走，监听者仍然能看到节点状态变化。

## 崩溃后的 20 秒窗口

节点崩溃后，它无法主动写 `draining=true`。其他节点只能通过心跳过期发现它下线。

典型时间线：

```text
T=0s      节点 A 崩溃，最后一次心跳仍在 MemberStore
T=0~20s   GetActiveMembers(20s) 仍可能返回 A
T=20s     A 的心跳超过 healthyHostLastHeartbeatCutoff
T=20~25s  其他节点下一次 pollLoop 读到新成员列表
T≈25s     哈希环移除 A，原本属于 A 的 key 映射到 B/C
```

如果 `pollInterval=5s`，崩溃感知窗口约为：

```text
healthyHostLastHeartbeatCutoff + pollInterval = 20s + 5s = 25s
```

这个窗口里，`frontend.Lookup(queue/partition)` 可能仍然返回崩溃节点 A。请求发给 A 后会表现为 gRPC 连接失败、超时、`Unavailable` 等错误。

这类请求失败由调用方重试处理。当前 `frontend` 内部有短重试：

```text
defaultMaxRetries   = 3
defaultRetryBackoff = 50ms
```

短重试可以覆盖瞬时 owner 切换或连接抖动。完整覆盖节点崩溃收敛窗口，需要业务调用层使用更长的退避重试，或者调小 heartbeat / cutoff / pollInterval，用更多 DB 读写换更快故障发现。

## 一致性分成两层

### 路由视图一致性

`membership` 维护的是最终收敛的路由视图。

每个节点独立轮询 DB，所以成员视图可能短暂不同：

```text
节点 B 已经看到 A 下线
节点 C 还没看到 A 下线
```

短暂分歧期内，不同节点对同一个 key 的 `Lookup` 结果可能不同。随着所有节点完成下一轮 poll，成员视图会收敛到同一份列表。

### 分区所有权一致性

分区的真实运行权由 `matching` 里的 `dbleader` 锁决定。

即使两个节点因为 membership 视图短暂分歧，都觉得自己应该接管同一个分区，也只有拿到 DB 锁的节点能启动 `PhysicalQueue`。

```text
节点 B: Lookup(queue-a/3) == B，尝试接管
节点 C: Lookup(queue-a/3) == C，尝试接管

dbleader 锁:
  B 成功 -> B 运行分区
  C 失败 -> C 等待下一轮
```

所以：

```text
membership = 路由建议和减少 DB 锁竞争
dbleader   = 运行期真实 owner
rangeID    = 写入侧旧 owner 围栏
```

## rangeID 的作用

`rangeID` 是分区元数据的乐观锁版本号，用来挡住旧 owner 的残留写入。

它解决这个场景：

```text
节点 A 曾经拥有 partition-3
节点 B 已经接管 partition-3
节点 A 上残留的 goroutine 仍尝试写 DB
```

典型流程：

```text
1. 节点 A 加载 partition-3
2. A 读取 queue_info.range_id = 10
3. A 写分区元数据时带上 prevRangeID=10

   UPDATE queue_info
   SET range_id = 11, ...
   WHERE queue_name = ? AND partition_id = ? AND range_id = 10

4. 节点 B 接管 partition-3，并把 range_id 推进到 11
5. 节点 A 继续拿旧的 range_id=10 写 DB
6. UPDATE 影响 0 行
7. A 得到 ErrOwnershipLost，关闭本地分区
```

`dbleader` 决定谁能启动和续跑分区任务。`rangeID` 保证过期 owner 的 DB 写入失败。

## 请求失败如何处理

membership 崩溃窗口内，可能发生：

```text
frontend Lookup 仍返回 A
A 已经崩溃
gRPC 请求失败
frontend 重试
下一次 Lookup 仍可能返回 A
直到成员视图收敛后 Lookup 返回 B/C
```

当前 frontend 会对这些 gRPC 状态做短重试：

```text
FailedPrecondition
Unavailable
Aborted
```

节点崩溃场景下，调用方应该把 `Enqueue`、`Dequeue`、`Ack`、`Nack` 当成可能临时失败的远程调用处理。建议外层重试时间覆盖：

```text
healthyHostLastHeartbeatCutoff + pollInterval
```

示例：

```text
20s + 5s = 25s
```

如果系统要求更快切换，可以调整：

```text
heartbeat interval 更短
healthyHostLastHeartbeatCutoff 更短
pollInterval 更短
```

代价是更高的 DB 写心跳和读成员表压力。

## 一次完整运作过程

```text
1. 节点启动 Monitor
2. Monitor 写入自己的心跳
3. Monitor 读取 MemberStore 里的活跃成员
4. ServiceResolver 为每个 service 建哈希环
5. frontend 用 Lookup(queue/partition) 找 matching 节点
6. matching 用 Lookup(queue/partition) 判断本节点是否应该加载分区
7. matching 拿 dbleader 锁
8. 拿锁成功后启动 PhysicalQueue
9. PhysicalQueue 写 DB 时用 rangeID 做乐观锁校验
10. 节点加入、下线、draining 都会触发哈希环重建和分区重新分配
```

`membership` 的最终作用是让所有节点在大多数时间对“哪个 key 属于哪个节点”达成一致，并在成员变化后自动收敛。
