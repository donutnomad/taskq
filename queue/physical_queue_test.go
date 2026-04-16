package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type flakyDispatchStore struct {
	Store
	mu               sync.Mutex
	failDispatchFor  map[int64]error
	dispatchAttempts map[int64]int
}

type conditionFailedUpdateStore struct {
	Store
	failUpdate bool
}

func (s *conditionFailedUpdateStore) UpdateQueue(ctx context.Context, info *QueueInfo, prevRangeID int64) error {
	if s.failUpdate {
		return ErrConditionFailed
	}
	return s.Store.UpdateQueue(ctx, info, prevRangeID)
}

func newFlakyDispatchStore(base Store) *flakyDispatchStore {
	return &flakyDispatchStore{
		Store:            base,
		failDispatchFor:  make(map[int64]error),
		dispatchAttempts: make(map[int64]int),
	}
}

func (s *flakyDispatchStore) DispatchTask(
	ctx context.Context,
	queueName string,
	partitionID int,
	taskID int64,
	dispatchedAt time.Time,
) error {
	s.mu.Lock()
	err := s.failDispatchFor[taskID]
	if err != nil {
		delete(s.failDispatchFor, taskID)
	}
	s.dispatchAttempts[taskID]++
	s.mu.Unlock()

	if err != nil {
		return err
	}
	return s.Store.DispatchTask(ctx, queueName, partitionID, taskID, dispatchedAt)
}

func newTestPhysicalQueue(t *testing.T) (*PhysicalQueue, Store) {
	t.Helper()
	s := newTestStore(t)
	ctx := context.Background()
	pq, err := newPhysicalQueue(ctx, s, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })
	return pq, s
}

func startTestWriterTask(t *testing.T, pq *PhysicalQueue) {
	t.Helper()
	go func() {
		_ = pq.RunWriter(pq.ctx)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		pq.writer.stateMu.RLock()
		started := pq.writer.started
		pq.writer.stateMu.RUnlock()
		if started {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for writer task to start")
}

func tickTestRun(ctx context.Context, interval time.Duration, f func(context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := f(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := f(ctx); err != nil {
				return err
			}
		}
	}
}

func startTestGCTask(pq *PhysicalQueue) {
	go func() {
		_ = tickTestRun(pq.ctx, 20*time.Millisecond, pq.RunGCOnce)
	}()
}

func startTestVisibilityTask(pq *PhysicalQueue) {
	go func() {
		_ = tickTestRun(pq.ctx, 20*time.Millisecond, func(ctx context.Context) error {
			pq.RunVisibilityScanOnce(ctx)
			return nil
		})
	}()
}

func TestPhysicalQueue_NewQueueDoesNotAcceptWritesUntilWriterTaskRuns(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pq.close() })

	if err := pq.AddTask(context.Background(), []byte("task")); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected AddTask to reject writes before writer task starts, got %v", err)
	}
}

func TestPhysicalQueue_GCDoesNotRunUntilExplicitlyStarted(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if err := pq.Ack(ctx, token); err != nil {
		t.Fatal(err)
	}

	time.Sleep(defaultGCInterval + 300*time.Millisecond)

	info, err := s.GetQueue(ctx, "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 0 {
		t.Fatalf("expected ack level to stay at 0 before GC task starts, got %d", info.AckLevel)
	}
}

func TestPhysicalQueue_GCRunsAfterExplicitStart(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	startTestGCTask(pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if err := pq.Ack(ctx, token); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		info, err := s.GetQueue(ctx, "test-q", 0)
		if err != nil {
			t.Fatal(err)
		}
		if info.AckLevel == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	info, err := s.GetQueue(ctx, "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("expected ack level to advance after GC task starts, got %d", info.AckLevel)
}

func TestPhysicalQueue_VisibilityScanDoesNotRunUntilExplicitlyStarted(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 50*time.Millisecond, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, _, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}

	time.Sleep(defaultVisScanInterval + 200*time.Millisecond)

	if row, _, ok := pq.TryPoll(ctx); ok || row != nil {
		t.Fatalf("expected timed out task to stay hidden before visibility task starts, got ok=%v row=%+v", ok, row)
	}
}

func TestPhysicalQueue_VisibilityScanRunsAfterExplicitStart(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 50*time.Millisecond, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	startTestVisibilityTask(pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, _, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		row, _, ok := pq.TryPoll(ctx)
		if ok {
			if string(row.Data) != "task" {
				t.Fatalf("expected redelivered task, got %q", row.Data)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("expected timed out task to be redelivered after visibility task starts")
}

func TestPhysicalQueue_WriterOwnershipLossDoesNotLocallyCancelQueue(t *testing.T) {
	base := newTestStore(t)
	store := &conditionFailedUpdateStore{Store: base}

	pq, err := newPhysicalQueue(context.Background(), store, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	store.failUpdate = true
	err = pq.AddTask(context.Background(), []byte("task"))
	if !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("expected ErrOwnershipLost, got %v", err)
	}
	if pq.IsClosed() {
		t.Fatal("expected ownership loss to be reported upward instead of locally canceling the queue")
	}
}

func TestPhysicalQueue_GCOwnershipLossDoesNotLocallyCancelQueue(t *testing.T) {
	base := newTestStore(t)
	store := &conditionFailedUpdateStore{Store: base}

	pq, err := newPhysicalQueue(context.Background(), store, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}
	_, token, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if err := pq.Ack(ctx, token); err != nil {
		t.Fatal(err)
	}

	store.failUpdate = true
	if err := pq.gc.collect(ctx); !errors.Is(err, ErrOwnershipLost) {
		t.Fatalf("expected ErrOwnershipLost, got %v", err)
	}

	if pq.IsClosed() {
		t.Fatal("expected GC ownership loss to be reported upward instead of locally canceling the queue")
	}
}

func TestPhysicalQueue_AddTaskWaitsUntilTaskBecomesReadable(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	enteredFlush := make(chan struct{})
	releaseFlush := make(chan struct{})
	originalOnFlush := pq.writer.onFlush
	pq.writer.onFlush = func(rows []*TaskRow) {
		close(enteredFlush)
		<-releaseFlush
		if originalOnFlush != nil {
			originalOnFlush(rows)
		}
	}

	addDone := make(chan error, 1)
	go func() {
		addDone <- pq.AddTask(context.Background(), []byte("task"))
	}()

	select {
	case <-enteredFlush:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for flush hook")
	}

	select {
	case err := <-addDone:
		t.Fatalf("expected AddTask to stay blocked until task is readable, got %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFlush)

	select {
	case err := <-addDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for AddTask to finish")
	}

	row, _, ok := pq.TryPoll(context.Background())
	if !ok || row == nil {
		t.Fatal("expected task to be readable after AddTask returns")
	}
}

func TestPhysicalQueue_AddAndPoll(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.addTask(ctx, []byte(`{"type":"workflow"}`)); err != nil {
		t.Fatal(err)
	}

	row, token, ok := pq.tryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if row == nil || token == "" {
		t.Fatal("expected non-nil row and token")
	}
}

func TestPhysicalQueue_SyncMatch(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	// 先注册 waiter
	ch := make(chan PollResult, 1)
	pq.registerWaiter(ch)

	// 写入任务，应通过 sync match 投递给 waiter
	if err := pq.addTask(ctx, []byte(`{"type":"activity"}`)); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-ch:
		if result.Row == nil {
			t.Fatal("expected task in sync match result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for sync match")
	}
}

func TestPhysicalQueue_AckAndNack(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	pq.addTask(ctx, []byte("task1"))
	pq.addTask(ctx, []byte("task2"))

	r1, tok1, _ := pq.tryPoll(ctx)
	r2, tok2, _ := pq.tryPoll(ctx)
	_ = r1
	_ = r2

	// ack task 1
	pq.ack(ctx, tok1)

	// nack task 2
	pq.nack(ctx, tok2)

	// nack 后 task2 应可再次被 poll
	r3, _, ok := pq.tryPoll(ctx)
	if !ok {
		t.Fatal("expected redelivered task after nack")
	}
	if string(r3.Data) != "task2" {
		t.Fatalf("expected task2, got %s", r3.Data)
	}
}

func TestPhysicalQueue_BlockingPoll(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch := make(chan PollResult, 1)
	pq.registerWaiter(ch)

	select {
	case <-ch:
		t.Fatal("should not get task")
	case <-ctx.Done():
		// 正常超时
	}
}

func TestPhysicalQueue_RegisterWaiterImmediatelyGetsReadableTask(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if _, _, ok := pq.tryPoll(ctx); ok {
		t.Fatal("expected empty queue before reproducing the wait registration gap")
	}

	if err := pq.addTask(ctx, []byte("already-readable")); err != nil {
		t.Fatal(err)
	}

	ch := make(chan PollResult, 1)
	elem := pq.registerWaiter(ch)
	defer pq.removeWaiter(elem)

	select {
	case result := <-ch:
		if result.Err != nil {
			t.Fatalf("expected readable task, got error: %v", result.Err)
		}
		if result.Row == nil || string(result.Row.Data) != "already-readable" {
			t.Fatalf("expected already-readable task, got %+v", result.Row)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected waiter registration to pick up an already-readable task")
	}
}

func TestPhysicalQueue_CanceledQueueStopsServingAndRejectsAckNack(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.addTask(ctx, []byte("task-1")); err != nil {
		t.Fatal(err)
	}
	if err := pq.addTask(ctx, []byte("task-2")); err != nil {
		t.Fatal(err)
	}

	_, tok1, ok := pq.tryPoll(ctx)
	if !ok {
		t.Fatal("expected first task")
	}
	_, tok2, ok := pq.tryPoll(ctx)
	if !ok {
		t.Fatal("expected second task")
	}

	pq.cancel()

	if row, _, ok := pq.tryPoll(ctx); ok || row != nil {
		t.Fatalf("expected canceled queue to stop serving tasks, got ok=%v row=%+v", ok, row)
	}
	if err := pq.ack(ctx, tok1); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed from ack after cancellation, got %v", err)
	}
	if err := pq.nack(ctx, tok2); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed from nack after cancellation, got %v", err)
	}
}

func TestPhysicalQueue_AckRejectsInvalidToken(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)

	err := pq.Ack(context.Background(), "bad-token")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestPhysicalQueue_AckRejectsTokenForDifferentPartition(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}

	err := pq.Ack(ctx, "99:1")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for mismatched partition, got %v", err)
	}

	if err := pq.Ack(ctx, token); err != nil {
		t.Fatalf("expected original token to remain valid, got %v", err)
	}
}

func TestPhysicalQueue_AckRejectsDuplicateToken(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if err := pq.Ack(ctx, token); err != nil {
		t.Fatalf("expected first ack to succeed, got %v", err)
	}

	err := pq.Ack(ctx, token)
	if !errors.Is(err, ErrTaskNotOutstanding) {
		t.Fatalf("expected ErrTaskNotOutstanding, got %v", err)
	}
}

func TestPhysicalQueue_NackRejectsDuplicateToken(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if err := pq.Nack(ctx, token); err != nil {
		t.Fatalf("expected first nack to succeed, got %v", err)
	}

	err := pq.Nack(ctx, token)
	if !errors.Is(err, ErrTaskNotOutstanding) {
		t.Fatalf("expected ErrTaskNotOutstanding, got %v", err)
	}
}

func TestPhysicalQueue_TryPollDoesNotSkipTaskAfterDispatchFailure(t *testing.T) {
	base := newTestStore(t)
	store := newFlakyDispatchStore(base)
	store.failDispatchFor[1] = errors.New("transient dispatch failure")

	ctx := context.Background()
	pq, err := newPhysicalQueue(ctx, store, "test-q", 0, 60*time.Second, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	if err := pq.AddTask(ctx, []byte("task-1")); err != nil {
		t.Fatal(err)
	}

	if row, _, ok := pq.TryPoll(ctx); ok || row != nil {
		t.Fatalf("expected first poll to fail closed over transient dispatch error, got ok=%v row=%+v", ok, row)
	}

	row, _, ok := pq.TryPoll(ctx)
	if !ok {
		t.Fatal("expected task after transient dispatch failure")
	}
	if string(row.Data) != "task-1" {
		t.Fatalf("expected task-1, got %s", row.Data)
	}
}
