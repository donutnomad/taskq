package queue

import (
	"context"
	"fmt"
	"sync"
)

// ErrOwnershipLost 表示当前进程已失去分区所有权（另一个进程抢占了 rangeID）。
var ErrOwnershipLost = fmt.Errorf("ownership lost: %w", ErrConditionFailed)

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

// getRangeID 返回当前 rangeID（加锁）
func (o *ownershipState) getRangeID() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.rangeID
}

// updateQueue 使用 CAS 更新 queue_info，成功后递增 rangeID。
// 如果 CAS 失败（rangeID 不匹配），说明另一个进程已抢占所有权。
func (o *ownershipState) updateQueue(ctx context.Context, info *QueueInfo) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	prevRangeID := o.rangeID
	newRangeID := prevRangeID + 1
	info.RangeID = newRangeID

	if err := o.store.UpdateQueue(ctx, info, prevRangeID); err != nil {
		return ErrOwnershipLost
	}

	o.rangeID = newRangeID
	return nil
}
