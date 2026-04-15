# 分区 DB Lease 设计

## 目标

在 Matching 节点加载分区前，通过 DB lease 独占该分区。防止两个节点同时加载同一分区，并在节点崩溃后自动释放所有权。

## 现状

当前系统有两层分区路由：
1. **一致性哈希**（内存）：`ServiceResolver.Lookup(partitionKey)` 决定分区应归属哪个节点
2. **rangeID CAS**（DB）：`queue_info.range_id` 乐观锁，写入时检测冲突

问题：一致性哈希是 soft 分配，两个节点可能在成员视图不一致时同时认为自己拥有分区并加载它。rangeID CAS 只在写入时才失败，不能阻止加载。

## 方案

在 `queue_info` 表增加 `lease_owner` + `lease_expires_at`，形成前置独占保护。

### 参数

| 参数 | 值 | 说明 |
|------|-----|------|
| Lease TTL | 15s | 过期后其他节点可接管 |
| 续期间隔 | 5s | 3 次续期机会 |

### 生命周期

```
NewPhysicalQueue()
  → AcquireLease(owner, ttl)
    → UPDATE queue_info SET lease_owner=?, lease_expires_at=?, range_id=range_id+1
       WHERE ... AND (lease_owner IS NULL OR lease_expires_at < ? OR lease_owner = ?)
    → 成功 → 继续初始化，启动后台续期 goroutine
    → 失败 → 返回 ErrLeaseHeld

后台续期 goroutine（每 5s）:
  → RenewLease(owner, ttl)
    → UPDATE queue_info SET lease_expires_at=?
       WHERE ... AND lease_owner = ?
    → 失败 → cancel context → 所有后台组件停止

Close()
  → 停止续期 goroutine
  → ReleaseLease(owner)
    → UPDATE queue_info SET lease_owner=NULL, lease_expires_at=NULL
       WHERE ... AND lease_owner = ?

节点崩溃
  → 不续期 → 15s 后 lease_expires_at 到期 → 其他节点 AcquireLease 成功
```

### 与 rangeID CAS 的关系

两层保护互补，不移除 rangeID：
- **Lease**：前置保护，拿不到就不加载
- **RangeID CAS**：后置兜底，万一 lease 逻辑有 bug，写入时仍能检测冲突

获取 lease 时同时 increment rangeID（合并为一个原子 UPDATE），所以 AcquireLease 自带 CAS 抢占效果。

### Owner 标识

使用 `hostID`（Monitor 生成的 UUID bytes，hex 编码）作为 `lease_owner`。同一节点重启后 hostID 变化，不会误匹配旧 lease。

## 修改文件

### 1. `queue/store.go` — Store 接口新增 3 个方法

```go
AcquireLease(ctx context.Context, queueName string, partitionID int, owner string, ttl time.Duration) (*QueueInfo, error)
RenewLease(ctx context.Context, queueName string, partitionID int, owner string, ttl time.Duration) error
ReleaseLease(ctx context.Context, queueName string, partitionID int, owner string) error
```

新增错误：
```go
var ErrLeaseHeld = errors.New("partition lease held by another owner")
```

### 2. `queue/store_sqlite.go` — Schema 和实现

Schema 增加两列：
```sql
ALTER TABLE queue_info ADD COLUMN lease_owner TEXT;
ALTER TABLE queue_info ADD COLUMN lease_expires_at TIMESTAMP;
```

`AcquireLease`：获取 lease + increment rangeID + 返回 QueueInfo（合并原有的 GetQueue+UpdateQueue 流程）。
`RenewLease`：仅更新 `lease_expires_at`。
`ReleaseLease`：清空 `lease_owner` 和 `lease_expires_at`。

### 3. `queue/physical_queue.go` — 集成 lease

`NewPhysicalQueue` 签名新增 `leaseOwner string` 参数：
```go
func NewPhysicalQueue(ctx, store, queueName, partitionID, visTimeout, leaseOwner) (*PhysicalQueue, error)
```

流程变化：
- 原有的 `GetQueue → CreateQueue → UpdateQueue（rangeID++）` 合并为 `AcquireLease`
- 新增后台 `leaseRenewLoop` goroutine
- `Close()` 中调用 `ReleaseLease`

### 4. `matching/partition_mgr.go` — 传递 leaseOwner

`newPartitionManager` 新增 `leaseOwner string` 参数，`NewPhysicalQueue` 调用时传入。

`reconcile()` 中：lease 获取失败（`ErrLeaseHeld`）视为该分区暂时不可用，跳过（不 log error，属于正常竞争）。

### 5. `matching/matching.go` — 传递 leaseOwner

`Config` 新增 `LeaseOwner string`（从 Monitor 的 hostID 获取）。传递给 `newPartitionManager`。

## 验证

```bash
go build ./...
go test ./queue/ -v -race
go test ./matching/ -v -race
go test ./frontend/ -v -race
go run ./example/
```

测试场景：
- 单节点正常获取/续期/释放 lease
- 两次加载同一分区，第二次返回 ErrLeaseHeld
- lease 过期后可被其他 owner 获取
- Close 后 lease 释放
