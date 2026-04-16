package queue

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

func claimQueueOwnership(ctx context.Context, store Store, queueName string, partitionID int) (*QueueInfo, error) {
	info, err := loadOrCreateQueue(ctx, store, queueName, partitionID)
	if err != nil {
		return nil, err
	}

	for {
		next := *info
		next.RangeID = info.RangeID + 1
		if err := store.UpdateQueue(ctx, &next, info.RangeID); err == nil {
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

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
