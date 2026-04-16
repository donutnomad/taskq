//go:build bugrepro

package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

type updateQueueErrorStore struct {
	err error
}

func (s *updateQueueErrorStore) CreateQueue(context.Context, *QueueInfo) error { return nil }
func (s *updateQueueErrorStore) GetQueue(context.Context, string, int) (*QueueInfo, error) {
	return nil, nil
}
func (s *updateQueueErrorStore) UpdateQueue(context.Context, *QueueInfo, int64) error { return s.err }
func (s *updateQueueErrorStore) CreateTasks(context.Context, []*TaskRow) error        { return nil }
func (s *updateQueueErrorStore) GetTasks(context.Context, string, int, int64, int64, int) ([]*TaskRow, error) {
	return nil, nil
}
func (s *updateQueueErrorStore) DispatchTask(context.Context, string, int, int64, time.Time) error {
	return nil
}
func (s *updateQueueErrorStore) UnDispatchTask(context.Context, string, int, int64) error { return nil }
func (s *updateQueueErrorStore) ResetTimedOutTasks(context.Context, string, int, int64, time.Duration) ([]int64, error) {
	return nil, nil
}
func (s *updateQueueErrorStore) DeleteTasks(context.Context, string, int, int64) (int64, error) {
	return 0, nil
}
func (s *updateQueueErrorStore) DeleteTask(context.Context, string, int, int64) error { return nil }
func (s *updateQueueErrorStore) AcquireLease(context.Context, string, int, string, time.Duration) (*QueueInfo, error) {
	return nil, nil
}
func (s *updateQueueErrorStore) RenewLease(context.Context, string, int, string, time.Duration) error {
	return nil
}
func (s *updateQueueErrorStore) ReleaseLease(context.Context, string, int, string) error { return nil }
func (s *updateQueueErrorStore) Close() error                                            { return nil }

func TestBugRepro_UpdateQueueKeepsNonCASFailures(t *testing.T) {
	store := &updateQueueErrorStore{err: context.DeadlineExceeded}
	ownership := newOwnershipState(store, 7)

	err := ownership.updateQueue(context.Background(), &QueueInfo{
		QueueName:   "q",
		PartitionID: 0,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected original storage error, got %v", err)
	}
	if errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("expected non-CAS failure to stay distinguishable, got %v", err)
	}
}
