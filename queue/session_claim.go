package queue

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"gorm.io/gorm"
)

func claimQueueOwnership(ctx context.Context, store Store, queueName string, partitionID int) (*QueueInfo, error) {
	info, err := loadOrCreateQueue(ctx, store, queueName, partitionID)
	if err != nil {
		return nil, err
	}

	for {
		newRangeID, err := store.AllocateTaskIDRange(ctx, queueName, partitionID, info.NextTaskID, info.RangeID)
		if err == nil {
			next := *info
			next.RangeID = newRangeID
			return &next, nil
		} else if !errors.Is(err, ErrConditionFailed) {
			return nil, err
		}

		info, err = store.GetQueue(ctx, queueName, partitionID)
		if err != nil {
			return nil, err
		}
	}
}

func loadOrCreateQueue(ctx context.Context, store Store, queueName string, partitionID int) (*QueueInfo, error) {
	info, err := store.GetQueue(ctx, queueName, partitionID)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	create := &QueueInfo{
		QueueName:   queueName,
		PartitionID: partitionID,
		RangeID:     1,
		AckLevel:    0,
		NextTaskID:  1,
	}
	if err := store.CreateQueue(ctx, create); err != nil && !isAlreadyExists(err) {
		return nil, err
	}
	return store.GetQueue(ctx, queueName, partitionID)
}

// isAlreadyExists 识别 CreateQueue 撞上主键/唯一约束的情形（并发首次创建同一分区）。
// 优先用 gorm.ErrDuplicatedKey——gorm 的 TranslateError 会把 SQLite 与 MySQL 的唯一键
// 冲突统一翻译成它；但调用方注入的 *gorm.DB 未必开启 TranslateError，且 Store 不限于
// gorm 实现，故保留按驱动措辞的字符串兜底。
func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || // SQLite
		strings.Contains(msg, "Duplicate entry") || // MySQL
		strings.Contains(msg, "duplicate key") // 通用兜底
}
