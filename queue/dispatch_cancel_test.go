package queue

import (
	"context"
	"testing"
	"time"
)

// cancelAfterReadStore 模拟 poller 在读到任务之后、DispatchTask 之前离开，且驱动在 UPDATE 已提交后才报告
// ctx 取消（MySQL 驱动在取消时可能已经执行完语句）。
type cancelAfterReadStore struct {
	Store
	cancel context.CancelFunc
}

func (s *cancelAfterReadStore) GetTasksWithOptions(
	ctx context.Context,
	queueName string,
	partitionID int,
	minTaskID int64,
	maxTaskID int64,
	limit int,
	opts TaskQueryOptions,
) ([]*TaskRow, error) {
	rows, err := s.Store.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	s.cancel()
	return rows, err
}

func (s *cancelAfterReadStore) DispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64, dispatchedAt time.Time) error {
	if err := s.Store.DispatchTask(context.WithoutCancel(ctx), queueName, partitionID, taskID, dispatchedAt); err != nil {
		return err
	}
	return ctx.Err()
}

// poller 离开不得让已提交的 DispatchTask 变成内存不知道的投递：任务要么作为租约交给调用方（由调用方归还），
// 要么仍可被下一次 poll 领取，不能隐身到可见性超时。
func TestPhysicalQueue_PollerCancelDuringDispatchKeepsTaskDeliverable(t *testing.T) {
	inner := newTestStore(t)
	pollCtx, cancel := context.WithCancel(context.Background())
	store := &cancelAfterReadStore{Store: inner, cancel: cancel}
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)

	if err := pq.AddTask(context.Background(), []byte("dispatch-cancel")); err != nil {
		t.Fatal(err)
	}
	lease, ok, err := pq.TryPollWithOptions(pollCtx, PollOptions{})
	if err == nil && ok {
		if err := pq.ReturnLease(context.Background(), lease.Token); err != nil {
			t.Fatalf("return lease: %v", err)
		}
	}

	store.cancel = func() {}
	if _, ok, err := pq.TryPollWithOptions(context.Background(), PollOptions{}); err != nil || !ok {
		t.Fatalf("re-poll ok = %v, err = %v; the canceled dispatch hid the task until the visibility timeout", ok, err)
	}
}
