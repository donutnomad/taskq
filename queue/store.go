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
