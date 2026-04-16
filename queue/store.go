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

// LeaseStore 管理分区 lease。
type LeaseStore interface {
	// AcquireLease 获取分区 lease（不存在则创建 queue_info）。
	// 成功时同时递增 rangeID 并返回 QueueInfo。
	// 如果 lease 被其他 owner 持有且未过期，返回 ErrLeaseHeld。
	AcquireLease(ctx context.Context, queueName string, partitionID int,
		owner string, ttl time.Duration) (*QueueInfo, error)
	// RenewLease 续期 lease。owner 不匹配时返回 ErrLeaseHeld。
	RenewLease(ctx context.Context, queueName string, partitionID int,
		owner string, ttl time.Duration) error
	// ReleaseLease 释放 lease。仅当 owner 匹配时清空。
	ReleaseLease(ctx context.Context, queueName string, partitionID int,
		owner string) error
}

// Store 持久化存储接口
type Store interface {
	QueueMetaStore
	TaskStore
	LeaseStore
	Close() error
}
