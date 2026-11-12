package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

// R1：ReturnLease 归还未交付的租约时原地恢复可投递，既不消耗 attempt 也不重写新行。
func TestPhysicalQueue_ReturnLeaseDoesNotConsumeAttempt(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("undelivered")); err != nil {
		t.Fatal(err)
	}
	original, token, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}

	if err := pq.ReturnLease(ctx, token); err != nil {
		t.Fatal(err)
	}

	rows := undispatchedTestTasks(t, store, "respool-q")
	if len(rows) != 1 || rows[0].TaskID != original.TaskID || rows[0].Attempt != 0 {
		t.Fatalf("visible rows = %+v, want only the original row %d at attempt 0", rows, original.TaskID)
	}
	if pq.runtime.ackLevel() != 0 {
		t.Fatalf("ack level = %d, want 0 for an in-place return", pq.runtime.ackLevel())
	}

	returned, _, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("re-poll ok = %v, err = %v", ok, err)
	}
	if returned.TaskID != original.TaskID || returned.Attempt != original.Attempt {
		t.Fatalf("returned row = %+v, want the original row %+v", returned, original)
	}
}

// R2：ReturnLease 与 Nack 一样在成功后唤醒等待者，任务按原样交付给它。
func TestPhysicalQueue_ReturnLeaseWakesWaiters(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("waiter-target")); err != nil {
		t.Fatal(err)
	}
	original, token, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}

	waiter, err := pq.registerWaiter(PollOptions{}, PollMark{})
	if err != nil {
		t.Fatal(err)
	}
	if err := pq.ReturnLease(ctx, token); err != nil {
		t.Fatal(err)
	}

	select {
	case <-waiter.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("ReturnLease did not wake the registered waiter")
	}
	result, claimed := waiter.Claim()
	if !claimed || result.Err != nil {
		t.Fatalf("claimed = %v, err = %v", claimed, result.Err)
	}
	if result.Row.TaskID != original.TaskID || result.Row.Attempt != original.Attempt {
		t.Fatalf("delivered row = %+v, want the original row %+v", result.Row, original)
	}
}

// R3：ReturnLease 的 token 校验与 Nack 保持一致。
func TestPhysicalQueue_ReturnLeaseValidatesToken(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("token-checks")); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}

	if err := pq.ReturnLease(ctx, token); err != nil {
		t.Fatal(err)
	}
	if err := pq.ReturnLease(ctx, token); !errors.Is(err, ErrTaskNotOutstanding) {
		t.Fatalf("second return error = %v, want ErrTaskNotOutstanding", err)
	}
	foreign := formatToken("respool-q", pq.partitionID+1, 1, pq.ownershipEpoch, 1)
	if err := pq.ReturnLease(ctx, foreign); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("foreign partition token error = %v, want ErrInvalidToken", err)
	}
	stale := formatToken("respool-q", pq.partitionID, 1, pq.ownershipEpoch+1, 1)
	if err := pq.ReturnLease(ctx, stale); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale epoch token error = %v, want ErrStaleToken", err)
	}
}
