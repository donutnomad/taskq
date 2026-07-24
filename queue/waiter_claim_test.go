package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

type dispatchBarrierStore struct {
	Store
	dispatched chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (s *dispatchBarrierStore) DispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64, dispatchedAt time.Time) error {
	if err := s.Store.DispatchTask(ctx, queueName, partitionID, taskID, dispatchedAt); err != nil {
		return err
	}
	s.once.Do(func() { close(s.dispatched) })
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPollWaiterDeliveryWinsConcurrentCancellation(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	waiter := pq.RegisterWaiter()

	if err := pq.AddTask(context.Background(), []byte("claimed")); err != nil {
		t.Fatal(err)
	}
	if waiter.Cancel() {
		t.Fatal("expected completed delivery to win cancellation")
	}

	result, ok := waiter.Claim()
	if !ok {
		t.Fatal("expected delivered task to remain claimable")
	}
	if result.Row == nil || string(result.Row.Data) != "claimed" {
		t.Fatalf("unexpected claimed result: %+v", result)
	}
}

func TestPollWaiterCancellationMakesTaskImmediatelyReadable(t *testing.T) {
	store := &dispatchBarrierStore{
		Store:      newTestStore(t),
		dispatched: make(chan struct{}),
		release:    make(chan struct{}),
	}
	pq, err := newPhysicalQueue(context.Background(), store, "claim-race-q", 0, time.Minute, defaultMaxAttempts, "owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	waiter := pq.RegisterWaiter()

	addDone := make(chan error, 1)
	go func() {
		addDone <- pq.AddTask(context.Background(), []byte("available"))
	}()
	select {
	case <-store.dispatched:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatched task")
	}
	if !waiter.Cancel() {
		t.Fatal("expected timeout cancellation to win before delivery claim")
	}
	close(store.release)
	if err := <-addDone; err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	row, _, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || row == nil || string(row.Data) != "available" {
		t.Fatalf("expected canceled waiter task to be immediately readable, got ok=%v row=%+v", ok, row)
	}
}
