package queue

import (
	"context"
	"errors"
	"sync"
)

// ownershipState 序列化所有需要 rangeID CAS 的 DB 操作。
// writer 和 gc 共享同一个实例，确保 rangeID 在多进程竞争下正确递增。
type ownershipState struct {
	mu      sync.Mutex
	rangeID int64
	store   Store
}

func newOwnershipState(store Store, rangeID int64) *ownershipState {
	return &ownershipState{store: store, rangeID: rangeID}
}

func (o *ownershipState) allocateTaskIDRange(ctx context.Context, queueName string, partitionID int, newNextTaskID int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	prev := o.rangeID
	newRangeID, err := o.store.AllocateTaskIDRange(ctx, queueName, partitionID, newNextTaskID, prev)
	if err != nil {
		if errors.Is(err, ErrConditionFailed) {
			return ErrOwnershipLost
		}
		return err
	}
	o.rangeID = newRangeID
	return nil
}

func (o *ownershipState) advanceAckLevelAndDelete(ctx context.Context, queueName string, partitionID int, newAckLevel int64) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	prev := o.rangeID
	newRangeID, deleted, err := o.store.AdvanceAckLevelAndDeleteTasks(ctx, queueName, partitionID, newAckLevel, prev)
	if err != nil {
		if errors.Is(err, ErrConditionFailed) {
			return 0, ErrOwnershipLost
		}
		return 0, err
	}
	o.rangeID = newRangeID
	return deleted, nil
}
