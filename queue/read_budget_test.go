package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================
// 读库预算：分区的内存状态能证明没有可见行时 poll、waiter 登记与可见性扫描不读库
// ============================================
// - Happy Path: 每一行都已投递或确认时，poll、投递尝试与可见性扫描都不读库；新行落库后 poll 照常领取。
// - Happy Path: 同一次 poll 扫描之后没有新的可见行时，登记 waiter 不再读库；之后写入的行投递给它。
// - Error Case: 扫描与登记之间有行落库，登记时补读一次库并投递给新 waiter（没有丢失唤醒）。
// - Error Case: 一次 poll 的读库进行中时，同一分区的其它 poll 不排队，在自己的期限内领到任务。
// - Error Case: 放回可见状态的 UPDATE 已提交而驱动报错时，任务照常可读。
// - Edge Case: 有投递早于可见性超时线时扫描照常读库并 respool。
// ============================================

// countingStore 统计热路径上的存储调用次数。
type countingStore struct {
	Store
	reads    atomic.Int64
	dispatch atomic.Int64
	resets   atomic.Int64
}

func (s *countingStore) GetTasksWithOptions(ctx context.Context, queueName string, partitionID int, minTaskID, maxTaskID int64, limit int, opts TaskQueryOptions) ([]*TaskRow, error) {
	s.reads.Add(1)
	return s.Store.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
}

func (s *countingStore) DispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64, dispatchedAt time.Time) error {
	s.dispatch.Add(1)
	return s.Store.DispatchTask(ctx, queueName, partitionID, taskID, dispatchedAt)
}

func (s *countingStore) ResetTimedOutTasks(ctx context.Context, queueName string, partitionID int, opts VisibilityResetOptions) ([]int64, error) {
	s.resets.Add(1)
	return s.Store.ResetTimedOutTasks(ctx, queueName, partitionID, opts)
}

func newCountingQueue(t *testing.T, visTimeout time.Duration) (*PhysicalQueue, *countingStore) {
	t.Helper()
	store := &countingStore{Store: newTestStore(t)}
	pq, err := newPhysicalQueue(context.Background(), store, "budget-q", 0, visTimeout, defaultMaxAttempts, "owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	return pq, store
}

func TestPhysicalQueue_NoReadsWhileEveryRowIsHidden(t *testing.T) {
	pq, store := newCountingQueue(t, time.Minute)
	ctx := context.Background()

	// 空分区：没有窗口。
	for range 3 {
		if _, _, ok, err := pq.TryPoll(ctx); err != nil || ok {
			t.Fatalf("empty poll ok=%v err=%v", ok, err)
		}
	}
	pq.RunVisibilityScanOnce(ctx)
	if reads, resets := store.reads.Load(), store.resets.Load(); reads != 0 || resets != 0 {
		t.Fatalf("empty partition: reads=%d resets=%d, want 0", reads, resets)
	}

	for _, payload := range []string{"a", "b"} {
		if err := pq.AddTask(ctx, []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	_, first, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("first poll ok=%v err=%v", ok, err)
	}
	_, second, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("second poll ok=%v err=%v", ok, err)
	}
	if err := pq.Ack(ctx, first); err != nil {
		t.Fatal(err)
	}

	// 一行已确认、一行投递中：窗口里没有可见行。
	before := store.reads.Load()
	for range 5 {
		if _, _, ok, err := pq.TryPoll(ctx); err != nil || ok {
			t.Fatalf("poll over hidden rows ok=%v err=%v", ok, err)
		}
	}
	waiter := pq.RegisterWaiter()
	pq.RunVisibilityScanOnce(ctx)
	if reads := store.reads.Load() - before; reads != 0 {
		t.Fatalf("reads over hidden rows = %d, want 0", reads)
	}
	if resets := store.resets.Load(); resets != 0 {
		t.Fatalf("visibility scans that read the store = %d, want 0 (no delivery is near its timeout)", resets)
	}

	// 新行落库后照常投递给等待中的 waiter。
	if err := pq.AddTask(ctx, []byte("c")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiter.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not served after a new row was written")
	}
	result, claimed := waiter.Claim()
	if !claimed || result.Err != nil || string(result.Row.Data) != "c" {
		t.Fatalf("waiter result = %+v claimed=%v", result, claimed)
	}
	if err := pq.Ack(ctx, second); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalQueue_RegisterAfterScanSkipsRereadWithoutChange(t *testing.T) {
	pq, store := newCountingQueue(t, time.Minute)
	ctx := context.Background()
	// 一条只有 key "elsewhere" 才能领取的行：窗口里有本进程没投递过的行，不带 key 的 poll 必须读库。
	addUnmatchedRoutedTask(t, pq)

	mark := pq.PollMark()
	if _, ok, err := pq.TryPollWithOptions(ctx, PollOptions{}); err != nil || ok {
		t.Fatalf("scan ok=%v err=%v", ok, err)
	}
	if reads := store.reads.Load(); reads != 1 {
		t.Fatalf("scan reads = %d, want 1", reads)
	}
	waiter, err := pq.RegisterWaiterAfterScan(PollOptions{}, mark)
	if err != nil {
		t.Fatal(err)
	}
	if reads := store.reads.Load(); reads != 1 {
		t.Fatalf("reads after registering behind an unchanged scan = %d, want 1", reads)
	}

	if err := pq.AddTask(ctx, []byte("later")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiter.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("waiter registered after the scan was not served")
	}
	if result, claimed := waiter.Claim(); !claimed || result.Err != nil || string(result.Row.Data) != "later" {
		t.Fatalf("waiter result = %+v claimed=%v", result, claimed)
	}
}

func TestPhysicalQueue_RegisterAfterScanRereadsWhenRowsArrived(t *testing.T) {
	pq, _ := newCountingQueue(t, time.Minute)
	ctx := context.Background()
	addUnmatchedRoutedTask(t, pq)

	mark := pq.PollMark()
	if _, ok, err := pq.TryPollWithOptions(ctx, PollOptions{}); err != nil || ok {
		t.Fatalf("scan ok=%v err=%v", ok, err)
	}
	// 扫描之后、登记之前落库的行：落库时还没有 waiter，只能由登记补读。
	if err := pq.AddTask(ctx, []byte("between")); err != nil {
		t.Fatal(err)
	}
	waiter, err := pq.RegisterWaiterAfterScan(PollOptions{}, mark)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiter.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("row written between the scan and the registration was not delivered")
	}
	if result, claimed := waiter.Claim(); !claimed || result.Err != nil || string(result.Row.Data) != "between" {
		t.Fatalf("waiter result = %+v claimed=%v", result, claimed)
	}

	// 另一个分区实例的 mark 不能让登记跳过读库。
	other, _ := newCountingQueue(t, time.Minute)
	if err := pq.AddTask(ctx, []byte("foreign-mark")); err != nil {
		t.Fatal(err)
	}
	foreign, err := pq.RegisterWaiterAfterScan(PollOptions{}, other.PollMark())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-foreign.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("registration with another queue's mark skipped the read")
	}
}

// readBarrierStore 让第一次读到行的读取停在 entered/release 之间，模拟库里一次慢的读取（MySQL 上一次往返）。
type readBarrierStore struct {
	*countingStore
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *readBarrierStore) GetTasksWithOptions(ctx context.Context, queueName string, partitionID int, minTaskID, maxTaskID int64, limit int, opts TaskQueryOptions) ([]*TaskRow, error) {
	rows, err := s.countingStore.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	if err != nil || len(rows) == 0 || !s.armed.Load() {
		return rows, err
	}
	first := false
	s.once.Do(func() { first = true; close(s.entered) })
	if first {
		<-s.release
	}
	return rows, err
}

// 一次 poll 的读库不让同一分区的其它 poll 排队：MySQL 上 Tenuto 的 poll 期限只有几毫秒到一秒，排在别人的读取
// 之后的 poll 拿到读取机会时期限已到，读取以 ctx 错误结束；poll 持续到达时分区上的任务全部领不到，等可见性超时
// （读取曾经串行过：Tenuto 的 TestTaskQShortDeadlinePollsLoseNoDelivery 在 MySQL 上
// 300 个任务只领到 202 个）。
func TestPhysicalQueue_PollDoesNotQueueBehindAnotherPollsRead(t *testing.T) {
	base := &countingStore{Store: newTestStore(t)}
	store := &readBarrierStore{countingStore: base, entered: make(chan struct{}), release: make(chan struct{})}
	pq, err := newPhysicalQueue(context.Background(), store, "budget-q", 0, time.Minute, defaultMaxAttempts, "owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	ctx := context.Background()
	for _, payload := range []string{"a", "b"} {
		if err := pq.AddTask(ctx, []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}

	store.armed.Store(true)
	slow := make(chan error, 1)
	go func() {
		_, _, _, err := pq.TryPoll(ctx)
		slow <- err
	}()
	<-store.entered
	// 慢读取在第二个 poll 的期限之后才结束；排在它后面的 poll 会错过期限。
	time.AfterFunc(400*time.Millisecond, func() { close(store.release) })
	defer func() { <-slow }()

	pollCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	row, _, ok, err := pq.TryPoll(pollCtx)
	if err != nil || !ok {
		t.Fatalf("poll while another poll's read is in flight: ok=%v err=%v, want a task within its deadline", ok, err)
	}
	if row == nil {
		t.Fatal("no row")
	}
}

// 放回可见状态的 UPDATE 已提交、驱动却报告了错误时，任务回到 outstanding；它在库里是可见的，poll 必须照常读到。
// 窗口记账若把它算作已隐藏，这个任务在可见性超时之前不再被读取。
type committedUndispatchErrorStore struct {
	*countingStore
	fail atomic.Bool
}

func (s *committedUndispatchErrorStore) UnDispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64) error {
	if err := s.countingStore.UnDispatchTask(ctx, queueName, partitionID, taskID); err != nil {
		return err
	}
	if s.fail.Load() {
		return context.DeadlineExceeded
	}
	return nil
}

func TestPhysicalQueue_ReturnCommittedDespiteErrorStaysPollable(t *testing.T) {
	store := &committedUndispatchErrorStore{countingStore: &countingStore{Store: newTestStore(t)}}
	pq, err := newPhysicalQueue(context.Background(), store, "budget-q", 0, time.Minute, defaultMaxAttempts, "owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("returned")); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok=%v err=%v", ok, err)
	}
	store.fail.Store(true)
	if err := pq.ReturnLease(ctx, token); err == nil {
		t.Fatal("ReturnLease reported success although the store returned an error")
	}
	store.fail.Store(false)

	row, _, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok || string(row.Data) != "returned" {
		t.Fatalf("task whose return committed despite an error: row=%+v ok=%v err=%v, want it pollable", row, ok, err)
	}
}

func TestPhysicalQueue_VisibilityScanReadsWhenDeliveryNearsTimeout(t *testing.T) {
	pq, store := newCountingQueue(t, 20*time.Millisecond)
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("slow")); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := pq.TryPoll(ctx); err != nil || !ok {
		t.Fatalf("poll ok=%v err=%v", ok, err)
	}
	time.Sleep(30 * time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)
	if resets := store.resets.Load(); resets != 1 {
		t.Fatalf("visibility scans that read the store = %d, want 1", resets)
	}
	row, _, ok := awaitTestPoll(t, pq, PollOptions{})
	if !ok || string(row.Data) != "slow" || row.Attempt != 1 {
		t.Fatalf("timed out delivery was not respooled: row=%+v ok=%v", row, ok)
	}
}
