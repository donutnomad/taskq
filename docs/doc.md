# TaskQ 设计文档

## Monitor 多 ServiceName 支持

### 背景

分布式任务队列中，一个进程可能同时承担多个服务角色（如 `matching` + `history`），
共用一个 gRPC 端口，注册多个 service handler。

### API

```go
// 单角色
monitor := membership.NewMonitorWithConfig(membership.MonitorConfig{
    AdvertisedServices: []string{"matching"},
    WatchedServices:    []string{"matching"},
    Store:              store,
    BroadcastAddr:      netip.MustParseAddrPort("10.0.0.1:7235"),
    PollInterval:       5*time.Second,
    PropagationTime:    3*time.Second,
    ReplicaPoints:      200,
})

// 多角色（共用端口）
monitor := membership.NewMonitorWithConfig(membership.MonitorConfig{
    AdvertisedServices: []string{"matching", "history"},
    WatchedServices:    []string{"matching", "history"},
    Store:              store,
    BroadcastAddr:      netip.MustParseAddrPort("10.0.0.1:7235"),
    PollInterval:       5*time.Second,
    PropagationTime:    3*time.Second,
    ReplicaPoints:      200,
})

// 独立 Worker 只维护 Matching 路由视图
observer := membership.NewMonitorWithConfig(membership.MonitorConfig{
    WatchedServices: []string{"matching"},
    Store:           store,
    PollInterval:    5*time.Second,
    PropagationTime: 3*time.Second,
    ReplicaPoints:   200,
})
```

### 行为

- **心跳**：为每个 `AdvertisedServices` 角色写一条心跳记录到 MemberStore
- **哈希环**：为每个 `WatchedServices` 角色维护独立的一致性哈希环
- **Lookup**：`monitor.GetResolver("matching")` 和 `monitor.GetResolver("history")` 返回各自独立的环
- **Draining**：`SetDraining(true)` 会更新所有 advertised service 记录

### 数据库记录示例

一个节点注册两个角色后，MemberStore 中存在两条记录：

| service_name | rpc_address | rpc_port | host_id | draining |
|-------------|-------------|----------|---------|----------|
| matching    | 10.0.0.1    | 7235     | abc123  | false    |
| history     | 10.0.0.1    | 7235     | abc123  | false    |
