# TaskQ 设计文档

## Monitor 多 ServiceName 支持

### 背景

分布式任务队列中，一个进程可能同时承担多个服务角色（如 `matching` + `history`），
共用一个 gRPC 端口，注册多个 service handler。

### API

```go
// 单角色
monitor := membership.NewMonitor(
    []string{"matching"},
    store,
    "10.0.0.1:7235",  // broadcastRPCAddress，所有角色共用
    5*time.Second,     // pollInterval
    3*time.Second,     // propagationTime
    time.Time{},       // joinTime
    200,               // replicaPoints
)

// 多角色（共用端口）
monitor := membership.NewMonitor(
    []string{"matching", "history"},
    store,
    "10.0.0.1:7235",
    5*time.Second,
    3*time.Second,
    time.Time{},
    200,
)
```

### 行为

- **心跳**：为每个 serviceName 各写一条心跳记录到 MemberStore，端口统一从 `broadcastRPCAddress` 获取
- **哈希环**：为 `serviceNames` 中的每个角色维护独立的一致性哈希环
- **Lookup**：`monitor.GetResolver("matching")` 和 `monitor.GetResolver("history")` 返回各自独立的环
- **Draining**：`SetDraining(true)` 会更新所有 serviceName 的记录

### 数据库记录示例

一个节点注册两个角色后，MemberStore 中存在两条记录：

| service_name | rpc_address | rpc_port | host_id | draining |
|-------------|-------------|----------|---------|----------|
| matching    | 10.0.0.1    | 7235     | abc123  | false    |
| history     | 10.0.0.1    | 7235     | abc123  | false    |
