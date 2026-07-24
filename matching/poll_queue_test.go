package matching

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type pollCleanupBarrierStore struct {
	Store
	loserPartition int
	armed          atomic.Bool
	started        chan struct{}
	release        chan struct{}
	startOnce      sync.Once
	releaseOnce    sync.Once
}

// awaitTestLease 轮询直到取到任务。Nack 以新 taskID 重写任务（respool），
// 新行由 writer 异步落库，因此 Nack 返回后任务不会立刻可读。
func awaitTestLease(t *testing.T, handle *partitionHandle) (queue.Lease, bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		lease, ok, err := handle.TryPollWithOptions(context.Background(), queue.PollOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return lease, true
		}
		if time.Now().After(deadline) {
			return queue.Lease{}, false
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestWaitForQueuePollIgnoresInactiveWaiterSelectCases(t *testing.T) {
	store := newTestSQLiteStore(t)
	first, err := queue.NewPhysicalQueue(context.Background(), store, "test-q", 0, time.Minute, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := queue.NewPhysicalQueue(context.Background(), store, "test-q", 1, time.Minute, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)

	waiters := []queuePollWaiter{
		{target: queuePollTarget{partitionID: 0}, waiter: first.RegisterWaiter(), active: true},
		{target: queuePollTarget{partitionID: 1}, waiter: second.RegisterWaiter(), active: true},
	}
	first.Close()

	_, err = waitForQueuePoll(context.Background(), 20*time.Millisecond, waiters, nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("status = %v, error = %v", status.Code(err), err)
	}
}

// GetTasksWithOptions 拦截 loser 分区的清理路径：归还租约后会同步 wakeWaiters，
// 只要该分区挂着 waiter 就必然读一次任务表，这是归还流程内唯一可拦截的点。
func (s *pollCleanupBarrierStore) GetTasksWithOptions(
	ctx context.Context,
	queueName string,
	partitionID int,
	minTaskID, maxTaskID int64,
	limit int,
	opts queue.TaskQueryOptions,
) ([]*queue.TaskRow, error) {
	rows, err := s.Store.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	if partitionID != s.loserPartition || !s.armed.Load() {
		return rows, err
	}
	s.startOnce.Do(func() { close(s.started) })
	select {
	case <-s.release:
		return rows, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *pollCleanupBarrierStore) releaseCleanup() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func TestMatchingServer_PollQueueReturnsTaskFromNonFirstPartition(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	client := newTestClient(t, address)

	result := make(chan *PollQueueResponse, 1)
	errs := make(chan error, 1)
	go func() {
		response, err := client.PollQueue(context.Background(), &PollQueueRequest{
			QueueName:     "test-q",
			PartitionIds:  []int32{0, 1},
			PollTimeoutMs: 3000,
		})
		if err != nil {
			errs <- err
			return
		}
		result <- response
	}()

	time.Sleep(100 * time.Millisecond)
	if _, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName: "test-q", PartitionId: 1, Data: []byte("partition-one"),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case response := <-result:
		if response.Empty || response.PartitionId != 1 || string(response.Data) != "partition-one" {
			t.Fatalf("response = %+v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("PollQueue did not wake for a task on the non-first partition")
	}
}

func TestMatchingServer_PollQueueReturnsDeliveredLoserImmediately(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	client := newTestClient(t, address)
	ctx := context.Background()

	for partitionID, data := range [][]byte{[]byte("partition-zero"), []byte("partition-one")} {
		if _, err := client.AddTask(ctx, &AddTaskRequest{
			QueueName: "test-q", PartitionId: int32(partitionID), Data: data,
		}); err != nil {
			t.Fatal(err)
		}
	}

	winner, err := client.PollQueue(ctx, &PollQueueRequest{
		QueueName: "test-q", PartitionIds: []int32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	loserPartition := int32(1)
	if winner.PartitionId == loserPartition {
		loserPartition = 0
	}
	loser, err := client.PollTask(ctx, &PollTaskRequest{
		QueueName: "test-q", PartitionId: loserPartition,
	})
	if err != nil {
		t.Fatal(err)
	}
	if loser.Empty {
		t.Fatal("delivered PollQueue loser was not immediately redeliverable")
	}
}

func TestMatchingServer_PollQueueUsesRotatedPartitionOrderAcrossTaskIDSpaces(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	client := newTestClient(t, address)
	ctx := context.Background()

	for range 3 {
		if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("advance-p0")}); err != nil {
			t.Fatal(err)
		}
		first, err := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 0})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.AckTask(ctx, &AckTaskRequest{QueueName: "test-q", PartitionId: 0, Token: first.Token}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("higher-local-id")}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 1, Data: []byte("lower-local-id")}); err != nil {
		t.Fatal(err)
	}

	response, err := client.PollQueue(ctx, &PollQueueRequest{
		QueueName: "test-q", PartitionIds: []int32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.PartitionId != 0 || string(response.Data) != "higher-local-id" {
		t.Fatalf("response = %+v, want first rotated partition despite its higher local task ID", response)
	}
}

func TestMatchingServer_PollQueueTimeoutLeavesNoStaleWaiter(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	client := newTestClient(t, address)

	response, err := client.PollQueue(context.Background(), &PollQueueRequest{
		QueueName: "test-q", PartitionIds: []int32{0, 1}, PollTimeoutMs: 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Empty {
		t.Fatalf("timeout response = %+v", response)
	}
	if _, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName: "test-q", PartitionId: 1, Data: []byte("after-timeout"),
	}); err != nil {
		t.Fatal(err)
	}
	response, err = client.PollQueue(context.Background(), &PollQueueRequest{
		QueueName: "test-q", PartitionIds: []int32{0, 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Empty || string(response.Data) != "after-timeout" {
		t.Fatalf("post-timeout response = %+v", response)
	}
}

func TestMatchingServer_PollQueueRejectsOversizedPartitionList(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	client := newTestClient(t, address)

	_, err := client.PollQueue(context.Background(), &PollQueueRequest{
		QueueName: "test-q", PartitionIds: []int32{0, 0, 0},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status = %v, error = %v", status.Code(err), err)
	}
}

// visibleTestTasks 返回分区内对 poller 可见（未分发）的行。
func visibleTestTasks(t *testing.T, store Store, queueName string, partitionID int) []*queue.TaskRow {
	t.Helper()
	rows, err := store.GetTasks(context.Background(), queueName, partitionID, 0, 1<<30, 100)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// PollQueue 竞速的败者从未交付给消费者，归还它不得消耗投递次数、不得重写新行。
func TestMatchingPollQueueLoserReturnDoesNotConsumeAttempt(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	server, _ := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	ctx := context.Background()

	handles := make([]*partitionHandle, 2)
	candidates := make([]queuePollCandidate, 2)
	for partitionID := range 2 {
		handle, err := server.mgr.getOrLoadPartition(ctx, "test-q", partitionID)
		if err != nil {
			t.Fatal(err)
		}
		handles[partitionID] = handle
		if err := handle.AddTask(ctx, []byte{byte(partitionID)}); err != nil {
			t.Fatal(err)
		}
		lease, ok, err := handle.TryPollWithOptions(ctx, queue.PollOptions{})
		if err != nil || !ok {
			t.Fatalf("partition %d lease: ok = %v, err = %v", partitionID, ok, err)
		}
		candidates[partitionID] = queuePollCandidate{
			target: queuePollTarget{partitionID: int32(partitionID), handle: handle},
			lease:  lease,
		}
	}

	response, err := finishQueuePoll(ctx, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if response.PartitionId != 0 {
		t.Fatalf("winner partition = %d, want 0", response.PartitionId)
	}

	// 败者的老行原地恢复可见，没有更高 taskID 的重写行。
	rows := visibleTestTasks(t, store, "test-q", 1)
	if len(rows) != 1 || rows[0].TaskID != 1 || rows[0].Attempt != 0 {
		t.Fatalf("loser partition rows = %+v, want only the original task 1 at attempt 0", rows)
	}
	row, _, ok, err := handles[1].TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("loser re-poll ok = %v, err = %v", ok, err)
	}
	if row.TaskID != 1 || row.Attempt != 0 {
		t.Fatalf("loser row = %+v, want the original task 1 at attempt 0", row)
	}
}

// 调用方在投递瞬间取消时归还的 PollTask 租约同样从未交付，不能消耗投递次数。
func TestMatchingPollTaskCancelReturnDoesNotConsumeAttempt(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	server, _ := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 1})
	ctx := context.Background()

	handle, err := server.mgr.getOrLoadPartition(ctx, "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.AddTask(ctx, []byte("canceled-poll")); err != nil {
		t.Fatal(err)
	}
	lease, ok, err := handle.TryPollWithOptions(ctx, queue.PollOptions{})
	if err != nil || !ok {
		t.Fatalf("lease: ok = %v, err = %v", ok, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := finishPollTask(canceled, handle, lease); status.Code(err) != codes.Canceled {
		t.Fatalf("PollTask finalization status = %v, err = %v", status.Code(err), err)
	}

	rows := visibleTestTasks(t, store, "test-q", 0)
	if len(rows) != 1 || rows[0].TaskID != 1 || rows[0].Attempt != 0 {
		t.Fatalf("partition rows = %+v, want only the original task 1 at attempt 0", rows)
	}
	row, _, ok, err := handle.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("re-poll ok = %v, err = %v", ok, err)
	}
	if row.TaskID != 1 || row.Attempt != 0 {
		t.Fatalf("returned row = %+v, want the original task 1 at attempt 0", row)
	}
}

func TestMatchingPollFinalizationReturnsLeaseWhenRequestIsCanceled(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	server, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	client := newTestClient(t, address)
	ctx := context.Background()

	targets := make([]queuePollTarget, 0, 2)
	for partitionID := range 2 {
		if _, err := client.AddTask(ctx, &AddTaskRequest{
			QueueName: "test-q", PartitionId: int32(partitionID), Data: []byte{byte(partitionID)},
		}); err != nil {
			t.Fatal(err)
		}
		handle, err := server.mgr.getOrLoadPartition(ctx, "test-q", partitionID)
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, queuePollTarget{partitionID: int32(partitionID), handle: handle})
	}

	first, ok, err := targets[0].handle.TryPollWithOptions(ctx, queue.PollOptions{})
	if err != nil || !ok {
		t.Fatalf("first lease: ok = %v, err = %v", ok, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := finishPollTask(canceled, targets[0].handle, first); status.Code(err) != codes.Canceled {
		t.Fatalf("PollTask finalization status = %v, err = %v", status.Code(err), err)
	}
	returned, ok := awaitTestLease(t, targets[0].handle)
	if !ok || len(returned.Data) != 1 || returned.Data[0] != 0 {
		t.Fatalf("returned PollTask lease = %+v, ok = %v", returned, ok)
	}
	if err := targets[0].handle.Nack(ctx, returned.Token); err != nil {
		t.Fatal(err)
	}

	candidates := make([]queuePollCandidate, 0, 2)
	for _, target := range targets {
		lease, ok := awaitTestLease(t, target.handle)
		if !ok {
			t.Fatalf("partition %d lease: ok = %v", target.partitionID, ok)
		}
		candidates = append(candidates, queuePollCandidate{target: target, lease: lease})
	}
	if _, err := finishQueuePoll(canceled, candidates); status.Code(err) != codes.Canceled {
		t.Fatalf("PollQueue finalization status = %v, err = %v", status.Code(err), err)
	}
	for _, target := range targets {
		lease, ok := awaitTestLease(t, target.handle)
		if !ok || lease.Token == "" {
			t.Fatalf("returned partition %d lease = %+v, ok = %v", target.partitionID, lease, ok)
		}
	}
}

func TestMatchingPollFinalizationRechecksCancellationAfterLoserCleanup(t *testing.T) {
	base := newTestSQLiteStore(t)
	store := &pollCleanupBarrierStore{
		Store:          base,
		loserPartition: 1,
		started:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	resolver := newStubResolver("127.0.0.1:9999")
	server, _ := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	ctx := context.Background()
	handles := make([]*partitionHandle, 2)
	candidates := make([]queuePollCandidate, 2)
	for partitionID := range 2 {
		handle, err := server.mgr.getOrLoadPartition(ctx, "test-q", partitionID)
		if err != nil {
			t.Fatal(err)
		}
		handles[partitionID] = handle
		if err := handles[partitionID].AddTask(ctx, []byte{byte(partitionID)}); err != nil {
			t.Fatal(err)
		}
		lease, ok, err := handles[partitionID].TryPollWithOptions(ctx, queue.PollOptions{})
		if err != nil || !ok {
			t.Fatalf("partition %d lease: ok=%v err=%v", partitionID, ok, err)
		}
		candidates[partitionID] = queuePollCandidate{
			target: queuePollTarget{partitionID: int32(partitionID), handle: handles[partitionID]},
			lease:  lease,
		}
	}

	// loser 分区挂一个 waiter，归还租约内部的 wakeWaiters 才会读任务表并撞上屏障。
	loserWaiter, err := handles[store.loserPartition].RegisterWaiter()
	if err != nil {
		t.Fatal(err)
	}
	store.armed.Store(true)

	pollCtx, cancel := context.WithCancel(ctx)
	resultCh := make(chan error, 1)
	go func() {
		_, err := finishQueuePoll(pollCtx, candidates)
		resultCh <- err
	}()

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("loser cleanup did not start")
	}
	cancel()
	select {
	case err := <-resultCh:
		t.Fatalf("poll finalization returned before cleanup release: %v", err)
	default:
	}
	// 归还是原地生效的，先收回 waiter，避免刚归还的任务被它顺手领走。
	if !loserWaiter.Cancel() {
		t.Fatal("loser waiter consumed a task during cleanup")
	}
	store.releaseCleanup()

	if err := <-resultCh; status.Code(err) != codes.Canceled {
		t.Fatalf("poll finalization status = %v, err = %v", status.Code(err), err)
	}

	for partitionID, handle := range handles {
		lease, ok := awaitTestLease(t, handle)
		if !ok || len(lease.Data) != 1 || lease.Data[0] != byte(partitionID) {
			t.Fatalf("returned partition %d lease = %+v, ok=%v", partitionID, lease, ok)
		}
		if err := handle.Ack(ctx, lease.Token); err != nil {
			t.Fatalf("ack returned partition %d lease: %v", partitionID, err)
		}
	}
}
