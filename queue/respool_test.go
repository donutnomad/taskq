package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// respoolFailureStore 只让 respool 重写（attempt > 0）的批次写入失败。
type respoolFailureStore struct {
	Store
	err error
}

func (s *respoolFailureStore) CreateTasksAndAdvanceRange(
	ctx context.Context,
	queueName string,
	partitionID int,
	tasks []*TaskRow,
	newNextTaskID int64,
	prevRangeID int64,
) (int64, error) {
	for _, task := range tasks {
		if task.Attempt > 0 {
			return 0, s.err
		}
	}
	return s.Store.CreateTasksAndAdvanceRange(ctx, queueName, partitionID, tasks, newNextTaskID, prevRangeID)
}

// taskReadBarrierStore 在任务读取返回非空结果时挂起调用方，
// 用来把 writer goroutine 精确地停在 onFlush → tryDeliverToWaiters 内部。
type taskReadBarrierStore struct {
	Store
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newTaskReadBarrierStore(base Store) *taskReadBarrierStore {
	return &taskReadBarrierStore{
		Store:   base,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *taskReadBarrierStore) GetTasksWithOptions(
	ctx context.Context,
	queueName string,
	partitionID int,
	minTaskID, maxTaskID int64,
	limit int,
	opts TaskQueryOptions,
) ([]*TaskRow, error) {
	rows, err := s.Store.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	if err != nil || len(rows) == 0 || !s.armed.Load() {
		return rows, err
	}
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
		return rows, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func newRespoolTestQueue(t *testing.T, store Store, visTimeout time.Duration, maxAttempts int) *PhysicalQueue {
	t.Helper()
	// maxAttempts 直接经构造函数传入，不再事后改写 pq.runtime.maxAttempts，
	// 这样 AttemptsExhaustedDropsTask 真正验证构造参数贯通到 taskRuntime。
	pq, err := newPhysicalQueue(context.Background(), store, "respool-q", 0, visTimeout, maxAttempts, "owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	return pq
}

func awaitTestCondition(t *testing.T, message string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", message)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// undispatchedTestTasks 返回分区内所有对 poller 可见的行。
func undispatchedTestTasks(t *testing.T, store Store, queueName string) []*TaskRow {
	t.Helper()
	rows, err := store.GetTasks(context.Background(), queueName, 0, 0, 1<<30, 100)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// T3：一条反复失败的任务不再钉死 ackLevel。
func TestPhysicalQueue_NackRespoolsAndUnblocksAckLevel(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	for _, data := range []string{"task-1", "task-2", "task-3"} {
		if err := pq.AddTask(ctx, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	tokens := make([]string, 0, 3)
	for range 3 {
		_, token, ok, err := pq.tryPoll(ctx)
		if err != nil || !ok {
			t.Fatalf("poll ok = %v, err = %v", ok, err)
		}
		tokens = append(tokens, token)
	}

	if err := pq.Nack(ctx, tokens[0]); err != nil {
		t.Fatal(err)
	}
	if err := pq.Ack(ctx, tokens[1]); err != nil {
		t.Fatal(err)
	}
	if err := pq.Ack(ctx, tokens[2]); err != nil {
		t.Fatal(err)
	}

	awaitTestCondition(t, "ack level to pass the respooled task", func() bool {
		return pq.runtime.ackLevel() == 3
	})

	rows := undispatchedTestTasks(t, store, "respool-q")
	if len(rows) != 1 {
		t.Fatalf("expected exactly one readable respooled row, got %+v", rows)
	}
	if rows[0].TaskID != 4 || rows[0].Attempt != 1 || string(rows[0].Data) != "task-1" {
		t.Fatalf("respooled row = %+v, want task ID 4 with attempt 1 and task-1 payload", rows[0])
	}
}

// T4：respool 只改写 taskID 与 attempt，其余投递语义原样保留。
func TestPhysicalQueue_RespoolPreservesDataAndCreatedAt(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	deadline := time.Now().Add(time.Hour)
	if err := pq.AddTaskWithOptions(ctx, []byte("routed-payload"), EnqueueOptions{
		RoutingKey: "worker-a", RoutingDeadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}

	opts := PollOptions{AcceptedRoutingKeys: []string{"worker-a"}}
	original, token, ok, err := pq.tryPollWithOptions(ctx, opts)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}
	if err := pq.Nack(ctx, token); err != nil {
		t.Fatal(err)
	}

	respooled, _, ok := awaitTestPoll(t, pq, opts)
	if !ok {
		t.Fatal("respooled task never became pollable")
	}

	if respooled.TaskID == original.TaskID {
		t.Fatalf("respool should rewrite the task under a new ID, got %d", respooled.TaskID)
	}
	if respooled.Attempt != original.Attempt+1 {
		t.Fatalf("attempt = %d, want %d", respooled.Attempt, original.Attempt+1)
	}
	if string(respooled.Data) != string(original.Data) {
		t.Fatalf("data = %q, want %q", respooled.Data, original.Data)
	}
	if respooled.RoutingKey != original.RoutingKey {
		t.Fatalf("routing key = %q, want %q", respooled.RoutingKey, original.RoutingKey)
	}
	if !respooled.RoutingDeadline.Equal(original.RoutingDeadline) {
		t.Fatalf("routing deadline = %v, want %v", respooled.RoutingDeadline, original.RoutingDeadline)
	}
	if !respooled.CreatedAt.Equal(original.CreatedAt) {
		t.Fatalf("created at = %v, want the original %v", respooled.CreatedAt, original.CreatedAt)
	}
}

// T5：重试次数耗尽后丢弃任务，并照常放行 ackLevel。
func TestPhysicalQueue_AttemptsExhaustedDropsTask(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, 1)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("poison")); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}
	if err := pq.Nack(ctx, token); err != nil {
		t.Fatal(err)
	}

	if pq.runtime.ackLevel() != 1 {
		t.Fatalf("ack level = %d, want 1 after the exhausted task was dropped", pq.runtime.ackLevel())
	}
	if rows := undispatchedTestTasks(t, store, "respool-q"); len(rows) != 0 {
		t.Fatalf("exhausted task must not be rewritten, got %+v", rows)
	}
	if _, _, ok, err := pq.tryPoll(ctx); err != nil || ok {
		t.Fatalf("dropped task stayed pollable: ok = %v, err = %v", ok, err)
	}
}

// T6：可见性超时也走 respool，消费者崩溃不再钉死 ackLevel。
func TestPhysicalQueue_VisibilityTimeoutRespools(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, 20*time.Millisecond, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("abandoned")); err != nil {
		t.Fatal(err)
	}
	original, _, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}

	time.Sleep(30 * time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)

	respooled, _, ok := awaitTestPoll(t, pq, PollOptions{})
	if !ok {
		t.Fatal("respooled task never became pollable")
	}
	if respooled.TaskID == original.TaskID {
		t.Fatalf("visibility timeout should respool under a new task ID, got %d", respooled.TaskID)
	}
	if respooled.Attempt != 1 || string(respooled.Data) != "abandoned" {
		t.Fatalf("respooled row = %+v, want attempt 1 with the original payload", respooled)
	}
	if pq.runtime.ackLevel() != original.TaskID {
		t.Fatalf("ack level = %d, want %d", pq.runtime.ackLevel(), original.TaskID)
	}
}

// T7：新行写入失败时绝不 ack 老任务，老任务仍可恢复投递。
func TestPhysicalQueue_RespoolWriteFailureKeepsTaskRecoverable(t *testing.T) {
	writeErr := errors.New("respool write failed")
	base := newTestStore(t)
	store := &respoolFailureStore{Store: base, err: writeErr}
	pq := newRespoolTestQueue(t, store, 20*time.Millisecond, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("recoverable")); err != nil {
		t.Fatal(err)
	}
	original, token, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}
	if err := pq.Nack(ctx, token); err != nil {
		t.Fatal(err)
	}

	awaitTestCondition(t, "the failed respool to be aborted", func() bool {
		pq.runtime.state.mu.Lock()
		defer pq.runtime.state.mu.Unlock()
		return len(pq.runtime.state.respooling) == 0
	})

	pq.runtime.state.mu.Lock()
	acked := pq.runtime.state.acked[original.TaskID]
	pending := pq.runtime.state.inRedeliveryLocked(original.TaskID)
	pq.runtime.state.mu.Unlock()
	if acked {
		t.Fatal("task was acked even though its respooled row never landed")
	}
	if !pending {
		t.Fatal("task should stay pending redelivery after a failed respool write")
	}
	if pq.runtime.ackLevel() != 0 {
		t.Fatalf("ack level = %d, want 0", pq.runtime.ackLevel())
	}

	// 可见性扫描把老行放回可见状态，任务仍然可以再次投递。
	time.Sleep(30 * time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)
	recovered, _, ok := awaitTestPoll(t, pq, PollOptions{})
	if !ok {
		t.Fatal("task was lost after the respool write failed")
	}
	if recovered.TaskID != original.TaskID || recovered.Attempt != original.Attempt {
		t.Fatalf("recovered row = %+v, want the original row %+v", recovered, original)
	}
}

// T8：waiter 在投递瞬间消失属于瞬时错误，原地重投递、不消耗 attempt、不写新行。
func TestPhysicalQueue_WaiterDisappearedDoesNotConsumeAttempt(t *testing.T) {
	base := newTestStore(t)
	store := newTaskReadBarrierStore(base)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	// worker-a 的 waiter 不接受 worker-b 的任务，worker-b 的 waiter 撤销后投递必然落空。
	if _, err := pq.registerWaiter(PollOptions{AcceptedRoutingKeys: []string{"worker-a"}}); err != nil {
		t.Fatal(err)
	}
	vanishing, err := pq.registerWaiter(PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil {
		t.Fatal(err)
	}

	store.armed.Store(true)
	addDone := make(chan error, 1)
	go func() {
		addDone <- pq.AddTaskWithOptions(ctx, []byte("vanishing-waiter"), EnqueueOptions{RoutingKey: "worker-b"})
	}()

	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer never reached the waiter delivery path")
	}
	if !vanishing.Cancel() {
		t.Fatal("target waiter was already served")
	}
	close(store.release)

	select {
	case err := <-addDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AddTask never completed")
	}

	opts := PollOptions{AcceptedRoutingKeys: []string{"worker-b"}}
	row, _, ok := awaitTestPoll(t, pq, opts)
	if !ok {
		t.Fatal("task was not returned after the waiter disappeared")
	}
	if row.TaskID != 1 || row.Attempt != 0 {
		t.Fatalf("row = %+v, want the original task 1 with attempt 0", row)
	}
	if rows := undispatchedTestTasks(t, store, "respool-q"); len(rows) != 0 {
		t.Fatalf("internal redelivery must not rewrite the task, got %+v", rows)
	}
}

// T9：writer goroutine 停在投递路径上、写入队列已满时，外部 Nack 必须照常返回。
// respool 一旦改成阻塞投递，这里就会与 writer 互等而死锁。
func TestPhysicalQueue_NackFromWriterGoroutineDoesNotDeadlock(t *testing.T) {
	base := newTestStore(t)
	store := newTaskReadBarrierStore(base)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTaskWithOptions(ctx, []byte("outstanding"), EnqueueOptions{RoutingKey: "worker-b"}); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := pq.tryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}

	if _, err := pq.registerWaiter(PollOptions{AcceptedRoutingKeys: []string{"worker-a"}}); err != nil {
		t.Fatal(err)
	}
	vanishing, err := pq.registerWaiter(PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil {
		t.Fatal(err)
	}

	store.armed.Store(true)
	addDone := make(chan error, 1)
	go func() {
		addDone <- pq.AddTaskWithOptions(ctx, []byte("delivery-victim"), EnqueueOptions{RoutingKey: "worker-b"})
	}()
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer never reached the waiter delivery path")
	}
	// 屏障已经把 writer 摁在投递路径内部，之后的读取不再拦截，
	// 否则会连 Nack 自己的 wakeWaiters 一起拦下，测不出真正的死锁边。
	store.armed.Store(false)
	if !vanishing.Cancel() {
		t.Fatal("target waiter was already served")
	}

	// writer 卡在自己的投递回调里，写入队列同时被填满：
	// 此刻 respool 无论如何都不能等待 writer。
	for len(pq.writer.reqCh) < cap(pq.writer.reqCh) {
		pq.writer.reqCh <- nil
	}

	nackDone := make(chan error, 1)
	go func() { nackDone <- pq.Nack(ctx, token) }()
	select {
	case err := <-nackDone:
		if err != nil {
			t.Fatalf("nack with a blocked writer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Nack blocked on the writer it was called from")
	}

	close(store.release)
	select {
	case err := <-addDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer never finished the delivery path")
	}

	// 两条任务都必须仍然可投递。
	seen := make(map[string]bool)
	for range 2 {
		row, _, ok := awaitTestPoll(t, pq, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
		if !ok {
			t.Fatalf("expected both tasks to stay deliverable, got %v", seen)
		}
		seen[string(row.Data)] = true
	}
	if !seen["outstanding"] || !seen["delivery-victim"] {
		t.Fatalf("redelivered tasks = %v", seen)
	}
}

// T10：写入队列满时 respool 降级为原地 redelivery，Nack 仍然成功。
func TestPhysicalQueue_RespoolChannelFullDegradesToRedelivery(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTask(ctx, []byte("degraded")); err != nil {
		t.Fatal(err)
	}
	original, token, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok = %v, err = %v", ok, err)
	}

	unblock := blockTestWriterQueue(t, pq)
	if err := pq.Nack(ctx, token); err != nil {
		t.Fatalf("nack with a full writer queue: %v", err)
	}

	row, _, ok, err := pq.tryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("degraded redelivery poll ok = %v, err = %v", ok, err)
	}
	if row.TaskID != original.TaskID || row.Attempt != original.Attempt {
		t.Fatalf("degraded redelivery row = %+v, want the original row %+v", row, original)
	}
	if pq.runtime.ackLevel() != 0 {
		t.Fatalf("ack level = %d, want 0 for an in-place redelivery", pq.runtime.ackLevel())
	}
	unblock()
}
