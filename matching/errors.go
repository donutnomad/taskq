package matching

import "errors"

// ErrNotOwner 本节点不拥有请求的分区
var ErrNotOwner = errors.New("partition not owned by this node")

// ErrInvalidPartition 请求了越界的分区（读侧 partID 超出 [0, numRead)）
var ErrInvalidPartition = errors.New("invalid partition")

// ErrPartitionNotWritable 写入的分区号 >= 当前写分区数（缩容态高位分区已停写）。
// matching 作为权威闸门当场硬拒绝，调用方应刷新配置后重试到合法写分区。
var ErrPartitionNotWritable = errors.New("partition not writable")

// ErrPartitionConfigUnavailable 分区配置暂时不可知：store 查询失败，且本节点没有任何
// 该 queue 的历史缓存值可用（典型场景：节点刚重启、内存缓存为空，恰逢 DB 抖动）。
//
// 此时**绝不能**退回 bootstrap 默认分区数继续服务：默认值可能大于该 queue 真实的
// read/write（例如已被 ResizePartitions 安全缩容到 2，默认值却是 4），闸门会因此放行
// 对已退役分区的写入；DB 恢复后这些分区重新落在 [numRead, ...) 之外，失去 owner，
// 其中的任务永不被 poll、GC 也停摆——即静默丢数据。宁可拒绝服务。
//
// 映射到 gRPC Unavailable（frontend 的 isRetryable 已包含 Unavailable，会自动重试）。
var ErrPartitionConfigUnavailable = errors.New("partition config unavailable")
