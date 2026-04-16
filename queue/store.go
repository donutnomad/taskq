package queue

import (
	"context"
	"time"
)

// TaskRow 数据库任务行
type TaskRow struct {
	TaskID       int64
	QueueName    string
	PartitionID  int
	Data         []byte
	CreatedAt    time.Time
	DispatchedAt *time.Time // nil = 未分发
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
	UpdateQueue(ctx context.Context, info *QueueInfo, prevRangeID int64) error
}

// TaskStore 管理 tasks 表中的任务数据。
type TaskStore interface {
	CreateTasks(ctx context.Context, tasks []*TaskRow) error
	GetTasks(ctx context.Context, queueName string, partitionID int,
		minTaskID, maxTaskID int64, limit int) ([]*TaskRow, error)
	DispatchTask(ctx context.Context, queueName string, partitionID int,
		taskID int64, dispatchedAt time.Time) error
	UnDispatchTask(ctx context.Context, queueName string, partitionID int,
		taskID int64) error
	ResetTimedOutTasks(ctx context.Context, queueName string, partitionID int,
		ackLevel int64, visTimeout time.Duration) ([]int64, error)
	DeleteTasks(ctx context.Context, queueName string, partitionID int,
		maxTaskID int64) (int64, error)
	DeleteTask(ctx context.Context, queueName string, partitionID int,
		taskID int64) error
}

// Store 持久化存储接口
type Store interface {
	QueueMetaStore
	TaskStore
	Close() error
}
