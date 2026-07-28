package queue

import (
	"context"
	"time"
)

// TaskRow 数据库任务行
type TaskRow struct {
	TaskID          int64
	QueueName       string
	PartitionID     int
	RoutingKey      string
	RoutingDeadline time.Time
	Data            []byte
	Attempt         int // 投递失败重试次数，首次写入为 0
	CreatedAt       time.Time
	DispatchedAt    *time.Time // nil = 未分发
}

// TaskQueryOptions controls which routed tasks are visible to a poller.
// Unkeyed tasks and deadlines expired at RoutingFallbackAt are always visible.
type TaskQueryOptions struct {
	AcceptedRoutingKeys []string
	RoutingFallbackAt   time.Time
}

// VisibilityResetOptions defines the timed-out delivery window and task IDs
// already acknowledged above the contiguous AckLevel.
type VisibilityResetOptions struct {
	AckLevel          int64
	VisibilityTimeout time.Duration
	AcknowledgedIDs   []int64
	// MaxReset 限制单轮扫描处理的候选任务数（按 task_id 升序取最小的若干条），
	// 0 表示不限。上限让一轮扫描的重置与调用方随后的逐条重新隐藏都保持有界。
	MaxReset int
}

// QueueInfo 队列元数据
type QueueInfo struct {
	QueueName   string
	PartitionID int
	RangeID     int64
	AckLevel    int64
	NextTaskID  int64
}

// QueuePartitionConfig 表示某个 queue 的分区 override 配置（读/写分区数）。
type QueuePartitionConfig struct {
	QueueName string
	Read      int
	Write     int
}

// QueueMetaStore 管理 queue_info 元数据。
type QueueMetaStore interface {
	CreateQueue(ctx context.Context, info *QueueInfo) error
	GetQueue(ctx context.Context, queueName string, partitionID int) (*QueueInfo, error)
	AllocateTaskIDRange(ctx context.Context,
		queueName string, partitionID int,
		newNextTaskID int64, prevRangeID int64) (newRangeID int64, err error)
	AdvanceAckLevelAndDeleteTasks(ctx context.Context,
		queueName string, partitionID int,
		newAckLevel int64, prevRangeID int64) (newRangeID int64, deleted int64, err error)
	// ResolvePartitions 读时取值：优先 per-queue override 行，缺失则 fallback 到
	// 全局默认行。queue 没有自己的 override 行是常态，不物化。
	ResolvePartitions(ctx context.Context, queueName string) (read, write int, err error)
	// SetPartitions upsert 某 queue 的 override 行，硬校验 1<=write<=read。
	SetPartitions(ctx context.Context, queueName string, read, write int) error
	// SetDefaultPartitions 更新全局默认行，硬校验 1<=write<=read 且 grow-only：
	// 新值任一维度小于当前默认值则拒绝。
	SetDefaultPartitions(ctx context.Context, read, write int) error
	// EnsureDefaultPartitions 幂等 bootstrap 全局默认行，已存在则不覆盖。
	EnsureDefaultPartitions(ctx context.Context, read, write int) error
	// ListPartitionOverrides 返回所有 override 行（不含全局默认），供 reconcile 枚举。
	ListPartitionOverrides(ctx context.Context) ([]QueuePartitionConfig, error)
	// ListQueueNames 返回 queue_info 中出现过的所有 queue 名（DISTINCT），
	// 供 reconcile 枚举已物化分区的 queue。
	ListQueueNames(ctx context.Context) ([]string, error)
}

// TaskStore 管理 tasks 表中的任务数据。
type TaskStore interface {
	CreateTasks(ctx context.Context, tasks []*TaskRow) error
	GetTasks(ctx context.Context, queueName string, partitionID int,
		minTaskID, maxTaskID int64, limit int) ([]*TaskRow, error)
	GetTasksWithOptions(ctx context.Context, queueName string, partitionID int,
		minTaskID, maxTaskID int64, limit int, opts TaskQueryOptions) ([]*TaskRow, error)
	DispatchTask(ctx context.Context, queueName string, partitionID int,
		taskID int64, dispatchedAt time.Time) error
	UnDispatchTask(ctx context.Context, queueName string, partitionID int,
		taskID int64) error
	ResetTimedOutTasks(ctx context.Context, queueName string, partitionID int,
		opts VisibilityResetOptions) ([]int64, error)
}

// Store 持久化存储接口
type Store interface {
	QueueMetaStore
	TaskStore
	// CreateTasksAndAdvanceRange atomically fences ownership, advances the next
	// task ID, and persists the matching task batch.
	CreateTasksAndAdvanceRange(ctx context.Context,
		queueName string, partitionID int,
		tasks []*TaskRow, newNextTaskID int64, prevRangeID int64) (newRangeID int64, err error)
	Close() error
}
