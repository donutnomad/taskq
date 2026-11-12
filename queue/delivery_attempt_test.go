package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type transitionBarrierStore struct {
	Store
	nackReached  chan struct{}
	nackRelease  chan struct{}
	resetReached chan struct{}
	resetRelease chan struct{}
	observeReads atomic.Bool
	readObserved chan struct{}
	readOnce     sync.Once
}

func (s *transitionBarrierStore) GetTasksWithOptions(
	ctx context.Context,
	queueName string,
	partitionID int,
	minTaskID, maxTaskID int64,
	limit int,
	opts TaskQueryOptions,
) ([]*TaskRow, error) {
	rows, err := s.Store.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	if err == nil && len(rows) > 0 && s.observeReads.Load() {
		s.readOnce.Do(func() { close(s.readObserved) })
	}
	return rows, err
}

func (s *transitionBarrierStore) UnDispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64) error {
	if err := s.Store.UnDispatchTask(ctx, queueName, partitionID, taskID); err != nil {
		return err
	}
	close(s.nackReached)
	select {
	case <-s.nackRelease:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *transitionBarrierStore) ResetTimedOutTasks(
	ctx context.Context,
	queueName string,
	partitionID int,
	opts VisibilityResetOptions,
) ([]int64, error) {
	ids, err := s.Store.ResetTimedOutTasks(ctx, queueName, partitionID, opts)
	if err != nil || len(ids) == 0 {
		return ids, err
	}
	close(s.resetReached)
	select {
	case <-s.resetRelease:
		return ids, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// addUnmatchedRoutedTask 写入一条只有 routing key "elsewhere" 的 poller 才能领取的任务。窗口里有一条本进程没投递过的
// 行，后续不带 key 的 poll 因此必须读库，测试才能观察到读取与进行中的状态转换的竞争；没有它时内存记账证明窗口里
// 没有可见行，poll 不读库，竞争不会发生。
func addUnmatchedRoutedTask(t *testing.T, pq *PhysicalQueue) {
	t.Helper()
	if err := pq.AddTaskWithOptions(context.Background(), []byte("unmatched"), EnqueueOptions{RoutingKey: "elsewhere"}); err != nil {
		t.Fatal(err)
	}
}

func newTransitionTestQueue(t *testing.T, store Store, visTimeout time.Duration) *PhysicalQueue {
	t.Helper()
	pq, err := newPhysicalQueue(context.Background(), store, "delivery-attempt-q", 0, visTimeout, defaultMaxAttempts, "owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	return pq
}

func TestPhysicalQueue_PreviousDeliveryCannotAckOrNackRedelivery(t *testing.T) {
	pq := newTransitionTestQueue(t, newTestStore(t), 20*time.Millisecond)
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("redelivered")); err != nil {
		t.Fatal(err)
	}

	_, firstToken, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("first poll ok = %v, err = %v", ok, err)
	}
	time.Sleep(30 * time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)

	_, secondToken, ok := awaitTestPoll(t, pq, PollOptions{})
	if !ok {
		t.Fatal("respooled task never became pollable")
	}
	if firstToken == secondToken {
		t.Fatal("redelivery reused the previous delivery token")
	}
	// 可见性超时会 respool 出新 taskID，老 taskID 随之退役。
	if err := pq.Ack(ctx, firstToken); !errors.Is(err, ErrTaskNotOutstanding) {
		t.Fatalf("previous delivery ack error = %v, want ErrTaskNotOutstanding", err)
	}
	if err := pq.Nack(ctx, firstToken); !errors.Is(err, ErrTaskNotOutstanding) {
		t.Fatalf("previous delivery nack error = %v, want ErrTaskNotOutstanding", err)
	}
	if err := pq.Ack(ctx, secondToken); err != nil {
		t.Fatalf("current delivery ack: %v", err)
	}
}

func TestPhysicalQueue_VisibilityScanPreservesOutOfOrderAcknowledgedTask(t *testing.T) {
	store := newTestStore(t)
	pq := newTransitionTestQueue(t, store, 20*time.Millisecond)
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("task-1")); err != nil {
		t.Fatal(err)
	}
	if err := pq.AddTask(ctx, []byte("task-2")); err != nil {
		t.Fatal(err)
	}

	_, firstToken, firstOK, err := pq.TryPoll(ctx)
	if err != nil || !firstOK {
		t.Fatalf("first poll ok = %v, err = %v", firstOK, err)
	}
	_, secondToken, secondOK, err := pq.TryPoll(ctx)
	if err != nil || !secondOK {
		t.Fatalf("second poll ok = %v, err = %v", secondOK, err)
	}
	if err := pq.Ack(ctx, secondToken); err != nil {
		t.Fatalf("out-of-order ack: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)
	row, redeliveryToken, ok := awaitTestPoll(t, pq, PollOptions{})
	if !ok {
		t.Fatal("respooled task never became pollable")
	}
	if string(row.Data) != "task-1" {
		t.Fatalf("redelivery data = %q, want task-1", row.Data)
	}
	if redeliveryToken == firstToken {
		t.Fatal("Task1 redelivery reused its previous delivery token")
	}
	if err := pq.Ack(ctx, redeliveryToken); err != nil {
		t.Fatalf("Task1 redelivery ack: %v", err)
	}
	if duplicate, _, ok, err := pq.TryPoll(ctx); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("visibility scan exposed acknowledged Task2 or a duplicate of the respooled Task1: %+v", duplicate)
	}
	if err := pq.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := store.GetQueue(ctx, "delivery-attempt-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Task1 超时后被 respool 成任务 3，老行随 respool 完成一并 ack。
	if info.AckLevel != 3 {
		t.Fatalf("ack level = %d, want 3", info.AckLevel)
	}
}

func TestPhysicalQueue_NackTransitionPreventsConcurrentRedispatch(t *testing.T) {
	store := &transitionBarrierStore{
		Store:        newTestStore(t),
		nackReached:  make(chan struct{}),
		nackRelease:  make(chan struct{}),
		resetReached: make(chan struct{}),
		resetRelease: make(chan struct{}),
		readObserved: make(chan struct{}),
	}
	pq := newTransitionTestQueue(t, store, time.Minute)
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("nack-race")); err != nil {
		t.Fatal(err)
	}
	addUnmatchedRoutedTask(t, pq)
	_, firstToken, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("first poll ok = %v, err = %v", ok, err)
	}
	// 写满 writer 队列，让 Nack 降级为 UnDispatchTask + 原地 redelivery，
	// 该降级路径是唯一仍会把老行放回可见状态的 nack 路径。
	blockTestWriterQueue(t, pq)

	nackDone := make(chan error, 1)
	go func() { nackDone <- pq.Nack(ctx, firstToken) }()
	select {
	case <-store.nackReached:
	case <-time.After(time.Second):
		t.Fatal("Nack did not reach the transition barrier")
	}

	store.observeReads.Store(true)
	type pollResult struct {
		token string
		ok    bool
		err   error
	}
	pollDone := make(chan pollResult, 1)
	go func() {
		_, token, ok, err := pq.TryPoll(ctx)
		pollDone <- pollResult{token: token, ok: ok, err: err}
	}()
	select {
	case <-store.readObserved:
	case <-time.After(time.Second):
		t.Fatal("concurrent poll did not observe the undispatched row")
	}
	select {
	case result := <-pollDone:
		t.Fatalf("poll crossed an incomplete Nack transition: %+v", result)
	default:
	}

	close(store.nackRelease)
	if err := <-nackDone; err != nil {
		t.Fatal(err)
	}
	result := <-pollDone
	if result.err != nil || !result.ok {
		t.Fatalf("redelivery poll = %+v", result)
	}
	if err := pq.Ack(ctx, result.token); err != nil {
		t.Fatalf("redelivery ack: %v", err)
	}
	if err := pq.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := store.GetQueue(ctx, "delivery-attempt-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 1 {
		t.Fatalf("ack level = %d, want 1", info.AckLevel)
	}
}

func TestPhysicalQueue_VisibilityResetPreventsConcurrentRedispatch(t *testing.T) {
	store := &transitionBarrierStore{
		Store:        newTestStore(t),
		nackReached:  make(chan struct{}),
		nackRelease:  make(chan struct{}),
		resetReached: make(chan struct{}),
		resetRelease: make(chan struct{}),
		readObserved: make(chan struct{}),
	}
	pq := newTransitionTestQueue(t, store, 20*time.Millisecond)
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("visibility-race")); err != nil {
		t.Fatal(err)
	}
	addUnmatchedRoutedTask(t, pq)
	if _, _, ok, err := pq.TryPoll(ctx); err != nil || !ok {
		t.Fatalf("first poll ok = %v, err = %v", ok, err)
	}
	time.Sleep(30 * time.Millisecond)

	scanDone := make(chan struct{})
	go func() {
		pq.RunVisibilityScanOnce(ctx)
		close(scanDone)
	}()
	select {
	case <-store.resetReached:
	case <-time.After(time.Second):
		t.Fatal("visibility scan did not reach the transition barrier")
	}

	store.observeReads.Store(true)
	type pollResult struct {
		token string
		ok    bool
		err   error
	}
	pollDone := make(chan pollResult, 1)
	go func() {
		_, token, ok, err := pq.TryPoll(ctx)
		pollDone <- pollResult{token: token, ok: ok, err: err}
	}()
	select {
	case <-store.readObserved:
	case <-time.After(time.Second):
		t.Fatal("concurrent poll did not observe the reset row")
	}
	select {
	case result := <-pollDone:
		t.Fatalf("poll crossed an incomplete visibility transition: %+v", result)
	default:
	}

	close(store.resetRelease)
	<-scanDone
	result := <-pollDone
	if result.err != nil {
		t.Fatalf("redelivery poll = %+v", result)
	}
	token := result.token
	if !result.ok {
		// respool 的新行由 writer 异步落库，这次 poll 可能早于新行可读。
		_, respooledToken, ok := awaitTestPoll(t, pq, PollOptions{})
		if !ok {
			t.Fatal("respooled task never became pollable")
		}
		token = respooledToken
	}
	if err := pq.Ack(ctx, token); err != nil {
		t.Fatalf("redelivery ack: %v", err)
	}
}
