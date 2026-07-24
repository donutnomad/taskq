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

type failOnceCreateTasksStore struct {
	Store
	mu  sync.Mutex
	err error
}

type createTasksBarrierStore struct {
	Store
	entered chan struct{}
	release chan struct{}
	err     error
}

func (s *createTasksBarrierStore) CreateTasksAndAdvanceRange(context.Context, string, int, []*TaskRow, int64, int64) (int64, error) {
	close(s.entered)
	<-s.release
	return 0, s.err
}

func (s *conditionFailedUpdateStore) AllocateTaskIDRange(ctx context.Context, queueName string, partitionID int, newNextTaskID int64, prevRangeID int64) (int64, error) {
	if s.failUpdate {
		return 0, ErrConditionFailed
	}
	return s.Store.AllocateTaskIDRange(ctx, queueName, partitionID, newNextTaskID, prevRangeID)
}

func (s *conditionFailedUpdateStore) AdvanceAckLevelAndDeleteTasks(ctx context.Context, queueName string, partitionID int, newAckLevel int64, prevRangeID int64) (int64, int64, error) {
	if s.failUpdate {
		return 0, 0, ErrConditionFailed
	}
	return s.Store.AdvanceAckLevelAndDeleteTasks(ctx, queueName, partitionID, newAckLevel, prevRangeID)
}

func (s *conditionFailedUpdateStore) CreateTasksAndAdvanceRange(ctx context.Context, queueName string, partitionID int, tasks []*TaskRow, newNextTaskID int64, prevRangeID int64) (int64, error) {
	if s.failUpdate {
		return 0, ErrConditionFailed
	}
	return s.Store.CreateTasksAndAdvanceRange(ctx, queueName, partitionID, tasks, newNextTaskID, prevRangeID)
}

func (s *failOnceCreateTasksStore) CreateTasks(ctx context.Context, tasks []*TaskRow) error {
	s.mu.Lock()
	err := s.err
	s.err = nil
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.Store.CreateTasks(ctx, tasks)
}

func (s *failOnceCreateTasksStore) CreateTasksAndAdvanceRange(ctx context.Context, queueName string, partitionID int, tasks []*TaskRow, newNextTaskID int64, prevRangeID int64) (int64, error) {
	s.mu.Lock()
	err := s.err
	s.err = nil
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return s.Store.CreateTasksAndAdvanceRange(ctx, queueName, partitionID, tasks, newNextTaskID, prevRangeID)
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
	pq, err := newPhysicalQueue(ctx, s, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
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

// awaitTestReadLevel 等待 writer 把 respool 重写出的新行发布为可读。
// respool 由 writer goroutine 异步收尾，nack/可见性扫描返回时新行尚未落库。
func awaitTestReadLevel(t *testing.T, pq *PhysicalQueue, level int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		pq.runtime.state.mu.Lock()
		current := pq.runtime.state.maxReadLevel
		pq.runtime.state.mu.Unlock()
		if current >= level {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for read level %d, current %d", level, current)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// awaitTestPoll 轮询直到取到任务，用于等待异步落库的 respool 新行。
func awaitTestPoll(t *testing.T, pq *PhysicalQueue, opts PollOptions) (*TaskRow, string, bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		row, token, ok, err := pq.tryPollWithOptions(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return row, token, true
		}
		if time.Now().After(deadline) {
			return nil, "", false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// blockTestWriterQueue 把 writer 卡在 flush 内并写满 reqCh，
// 使随后的 respool 投递必然失败并降级为原地 redelivery。
func blockTestWriterQueue(t *testing.T, pq *PhysicalQueue) func() {
	t.Helper()

	entered := make(chan struct{})
	release := make(chan struct{})
	var blockOnce, releaseOnce sync.Once
	original := pq.writer.onFlush
	pq.writer.onFlush = func(rows []*TaskRow) {
		blockOnce.Do(func() {
			close(entered)
			<-release
		})
		if original != nil {
			original(rows)
		}
	}

	go func() { _ = pq.AddTask(pq.ctx, []byte("writer-block")) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not enter flush")
	}
	for len(pq.writer.reqCh) < cap(pq.writer.reqCh) {
		pq.writer.reqCh <- nil
	}

	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return unblock
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
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pq.close() })

	if err := pq.AddTask(context.Background(), []byte("task")); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected AddTask to reject writes before writer task starts, got %v", err)
	}
}

func TestCreateTasksRollbackOnFailure(t *testing.T) {
	base := newTestStore(t)
	createErr := errors.New("create tasks failed")
	store := &failOnceCreateTasksStore{
		Store: base,
		err:   createErr,
	}
	pq, err := newPhysicalQueue(context.Background(), store, "rollback-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("failed-task")); !errors.Is(err, createErr) {
		t.Fatalf("expected first add to return create failure, got %v", err)
	}
	if err := pq.AddTask(ctx, []byte("retry-task")); err != nil {
		t.Fatal(err)
	}

	row, _, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected retried task")
	}
	if row.TaskID != 1 {
		t.Fatalf("expected retry to reuse task ID 1, got %d", row.TaskID)
	}
	if string(row.Data) != "retry-task" {
		t.Fatalf("expected retry-task, got %q", row.Data)
	}
}

func TestPhysicalQueue_RestartKeepsTaskIDsContiguousForGC(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	first, err := newPhysicalQueue(ctx, store, "restart-q", 0, time.Minute, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, first)
	if err := first.AddTask(ctx, []byte("first")); err != nil {
		t.Fatal(err)
	}
	first.Close()

	second, err := newPhysicalQueue(ctx, store, "restart-q", 0, time.Minute, defaultMaxAttempts, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, second)
	t.Cleanup(second.Close)
	if err := second.AddTask(ctx, []byte("second")); err != nil {
		t.Fatal(err)
	}

	for wantID := int64(1); wantID <= 2; wantID++ {
		row, token, ok, err := second.TryPoll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || row == nil {
			t.Fatalf("task %d was unavailable", wantID)
		}
		if row.TaskID != wantID {
			t.Fatalf("task ID = %d, want %d", row.TaskID, wantID)
		}
		if err := second.Ack(ctx, token); err != nil {
			t.Fatal(err)
		}
	}

	if err := second.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := store.GetQueue(ctx, "restart-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 2 {
		t.Fatalf("ack level = %d, want 2", info.AckLevel)
	}
}

func TestPhysicalQueue_CloseStopsWriterWithIndependentContext(t *testing.T) {
	store := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), store, "close-q", 0, time.Minute, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		runDone <- pq.RunWriter(context.Background())
	}()
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pq.WaitUntilWriterStarted(waitCtx); err != nil {
		t.Fatal(err)
	}

	pq.Close()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer remained running after queue close")
	}
}

func TestPhysicalQueue_ClosePreservesAcceptedFlushResult(t *testing.T) {
	store := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), store, "flush-close-q", 0, time.Minute, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)

	enteredFlush := make(chan struct{})
	releaseFlush := make(chan struct{})
	originalOnFlush := pq.writer.onFlush
	pq.writer.onFlush = func(rows []*TaskRow) {
		close(enteredFlush)
		<-releaseFlush
		originalOnFlush(rows)
	}

	addDone := make(chan error, 1)
	go func() {
		addDone <- pq.AddTask(context.Background(), []byte("task"))
	}()
	select {
	case <-enteredFlush:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}

	pq.Close()
	select {
	case err := <-addDone:
		t.Fatalf("accepted write returned before flush completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFlush)

	select {
	case err := <-addDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted write did not receive its flush result")
	}
}

func TestPhysicalQueue_ClosePreservesAcceptedFlushError(t *testing.T) {
	writeErr := errors.New("write failed")
	store := &createTasksBarrierStore{
		Store:   newTestStore(t),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		err:     writeErr,
	}
	pq, err := newPhysicalQueue(context.Background(), store, "flush-error-close-q", 0, time.Minute, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)

	addDone := make(chan error, 1)
	go func() {
		addDone <- pq.AddTask(context.Background(), []byte("task"))
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}

	pq.Close()
	select {
	case err := <-addDone:
		t.Fatalf("accepted write returned before flush completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)

	select {
	case err := <-addDone:
		if !errors.Is(err, writeErr) {
			t.Fatalf("accepted write error = %v, want %v", err, writeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("accepted write did not receive its flush error")
	}
}

func TestPhysicalQueue_GCDoesNotRunUntilExplicitlyStarted(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
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
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
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

	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
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
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 50*time.Millisecond, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, _, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task")
	}

	time.Sleep(defaultVisScanInterval + 200*time.Millisecond)

	if row, _, ok, err := pq.TryPoll(ctx); err != nil || ok || row != nil {
		t.Fatalf("expected timed out task to stay hidden before visibility task starts, got ok=%v row=%+v err=%v", ok, row, err)
	}
}

func TestPhysicalQueue_VisibilityScanRunsAfterExplicitStart(t *testing.T) {
	s := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 50*time.Millisecond, defaultMaxAttempts, "test-owner")
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

	_, _, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		row, _, ok, err := pq.TryPoll(ctx)
		if err != nil {
			t.Fatal(err)
		}
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

	pq, err := newPhysicalQueue(context.Background(), store, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
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

	pq, err := newPhysicalQueue(context.Background(), store, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
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
	pq, err := newPhysicalQueue(context.Background(), s, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
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

	row, _, ok, err := pq.TryPoll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
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

	row, token, ok, err := pq.tryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
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
	waiter, err := pq.registerWaiter(PollOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 写入任务，应通过 sync match 投递给 waiter
	if err := pq.addTask(ctx, []byte(`{"type":"activity"}`)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-waiter.Ready():
		result, ok := waiter.Claim()
		if !ok {
			t.Fatal("expected task delivery claim")
		}
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

	r1, tok1, _, err := pq.tryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r2, tok2, _, err := pq.tryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = r1
	_ = r2

	// ack task 1
	pq.ack(ctx, tok1)

	// nack task 2
	pq.nack(ctx, tok2)

	// nack 后 task2 以新 taskID 重写，writer 落库后应可再次被 poll
	r3, _, ok := awaitTestPoll(t, pq, PollOptions{})
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

	waiter, err := pq.registerWaiter(PollOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Cancel()

	select {
	case <-waiter.Ready():
		t.Fatal("should not get task")
	case <-ctx.Done():
		// 正常超时
	}
}

func TestPhysicalQueue_RegisterWaiterImmediatelyGetsReadableTask(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if _, _, ok, err := pq.tryPoll(ctx); err != nil || ok {
		t.Fatalf("expected empty queue before reproducing the wait registration gap, got ok=%v err=%v", ok, err)
	}

	if err := pq.addTask(ctx, []byte("already-readable")); err != nil {
		t.Fatal(err)
	}

	waiter, err := pq.registerWaiter(PollOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Cancel()

	select {
	case <-waiter.Ready():
		result, ok := waiter.Claim()
		if !ok {
			t.Fatal("expected readable task delivery claim")
		}
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

	_, tok1, ok, err := pq.tryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected first task")
	}
	_, tok2, ok, err := pq.tryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected second task")
	}

	pq.cancel()

	if row, _, ok, err := pq.tryPoll(ctx); !errors.Is(err, ErrQueueClosed) || ok || row != nil {
		t.Fatalf("expected canceled queue to stop serving tasks, got ok=%v row=%+v err=%v", ok, row, err)
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

	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task")
	}

	err = pq.Ack(ctx, "99:1")
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for mismatched partition, got %v", err)
	}

	if err := pq.Ack(ctx, token); err != nil {
		t.Fatalf("expected original token to remain valid, got %v", err)
	}
}

func TestPhysicalQueue_TokenIncludesQueueAndOwnershipEpoch(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task")
	}

	parsed, err := ParseTaskToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.QueueName != "test-q" {
		t.Fatalf("expected queue name test-q, got %q", parsed.QueueName)
	}
	if parsed.PartitionID != 0 {
		t.Fatalf("expected partition 0, got %d", parsed.PartitionID)
	}
	if parsed.TaskID != 1 {
		t.Fatalf("expected task 1, got %d", parsed.TaskID)
	}
	if parsed.OwnershipEpoch == 0 {
		t.Fatal("expected ownership epoch")
	}
	if parsed.DeliveryID == 0 {
		t.Fatal("expected delivery ID")
	}
}

func TestPhysicalQueue_AckRejectsTokenForDifferentQueue(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	pqA, err := newPhysicalQueue(ctx, store, "queue-a", 0, 60*time.Second, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pqA)
	defer pqA.Close()

	pqB, err := newPhysicalQueue(ctx, store, "queue-b", 0, 60*time.Second, defaultMaxAttempts, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pqB)
	defer pqB.Close()

	if err := pqA.AddTask(ctx, []byte("task-a")); err != nil {
		t.Fatal(err)
	}
	if err := pqB.AddTask(ctx, []byte("task-b")); err != nil {
		t.Fatal(err)
	}

	_, tokenA, ok, err := pqA.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected queue-a task")
	}
	_, tokenB, ok, err := pqB.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected queue-b task")
	}

	err = pqB.Ack(ctx, tokenA)
	if !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("expected ErrInvalidToken for mismatched queue, got %v", err)
	}
	if err := pqB.Ack(ctx, tokenB); err != nil {
		t.Fatalf("expected queue-b token to remain valid, got %v", err)
	}
}

func TestPhysicalQueue_AckRejectsTokenFromPreviousOwnershipEpoch(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	pq1, err := newPhysicalQueue(ctx, store, "epoch-q", 0, 60*time.Second, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq1)

	if err := pq1.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}
	_, oldToken, ok, err := pq1.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task")
	}
	pq1.Close()

	pq2, err := newPhysicalQueue(ctx, store, "epoch-q", 0, 60*time.Second, defaultMaxAttempts, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq2)
	defer pq2.Close()

	err = pq2.Ack(ctx, oldToken)
	if !errors.Is(err, ErrStaleToken) {
		t.Fatalf("expected ErrStaleToken, got %v", err)
	}
}

func TestPhysicalQueue_AckRejectsDuplicateToken(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("task")); err != nil {
		t.Fatal(err)
	}

	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task")
	}
	if err := pq.Ack(ctx, token); err != nil {
		t.Fatalf("expected first ack to succeed, got %v", err)
	}

	err = pq.Ack(ctx, token)
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

	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task")
	}
	if err := pq.Nack(ctx, token); err != nil {
		t.Fatalf("expected first nack to succeed, got %v", err)
	}

	err = pq.Nack(ctx, token)
	if !errors.Is(err, ErrTaskNotOutstanding) {
		t.Fatalf("expected ErrTaskNotOutstanding, got %v", err)
	}
}

func TestPhysicalQueue_TryPollDoesNotSkipTaskAfterDispatchFailure(t *testing.T) {
	base := newTestStore(t)
	store := newFlakyDispatchStore(base)
	dispatchErr := errors.New("transient dispatch failure")
	store.failDispatchFor[1] = dispatchErr

	ctx := context.Background()
	pq, err := newPhysicalQueue(ctx, store, "test-q", 0, 60*time.Second, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(func() { pq.close() })

	if err := pq.AddTask(ctx, []byte("task-1")); err != nil {
		t.Fatal(err)
	}

	if row, _, ok, err := pq.TryPoll(ctx); !errors.Is(err, dispatchErr) || ok || row != nil {
		t.Fatalf("expected first poll to return dispatch error, got ok=%v row=%+v err=%v", ok, row, err)
	}

	row, _, ok, err := pq.TryPoll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected task after transient dispatch failure")
	}
	if string(row.Data) != "task-1" {
		t.Fatalf("expected task-1, got %s", row.Data)
	}
}
