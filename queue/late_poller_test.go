package queue

import (
	"context"
	"testing"
	"time"
)

// heldDispatchStore 让第一次 DispatchTask 在 UPDATE 提交后停住，直到 release 关闭：投递方持有 transitionMu，
// 其他状态转换在锁上排队，模拟数据库变慢时同一分区的投递与归还排成长队。
type heldDispatchStore struct {
	Store
	held    chan struct{}
	release chan struct{}
	first   bool
	// reads 收到第一次投递停住之后每次读库的完成。
	reads chan struct{}
}

func (s *heldDispatchStore) GetTasksWithOptions(ctx context.Context, queueName string, partitionID int, minTaskID, maxTaskID int64, limit int, opts TaskQueryOptions) ([]*TaskRow, error) {
	rows, err := s.Store.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	select {
	case <-s.held:
		s.reads <- struct{}{}
	default:
	}
	return rows, err
}

func (s *heldDispatchStore) DispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64, dispatchedAt time.Time) error {
	if err := s.Store.DispatchTask(ctx, queueName, partitionID, taskID, dispatchedAt); err != nil {
		return err
	}
	if !s.first {
		s.first = true
		close(s.held)
		<-s.release
	}
	return nil
}

func newHeldDispatchQueue(t *testing.T) (*PhysicalQueue, *heldDispatchStore) {
	t.Helper()
	store := &heldDispatchStore{Store: newTestStore(t), held: make(chan struct{}), release: make(chan struct{}), reads: make(chan struct{}, 8)}
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	return pq, store
}

// holdTransitionLock 投递一个任务并让它的 DispatchTask 停在锁内，返回该投递的结果。
func holdTransitionLock(t *testing.T, pq *PhysicalQueue, store *heldDispatchStore) <-chan PollResult {
	t.Helper()
	first := make(chan PollResult, 1)
	go func() {
		row, token, _, err := pq.TryPoll(context.Background())
		first <- PollResult{Row: row, Token: token, Err: err}
	}()
	select {
	case <-store.held:
	case <-time.After(time.Second):
		t.Fatal("first dispatch did not reach the store")
	}
	return first
}

// poller 在锁上排队期间离开：轮到它时不再投递，行保持可见，下一个 poller 直接领到。此前它照常投递，租约只能
// 被调用方归还，库变慢时作废的投递与归还占满这把锁，分区上的任务一个都领不到。
func TestPhysicalQueue_PollerThatLeftWhileQueuedDoesNotDispatch(t *testing.T) {
	pq, store := newHeldDispatchQueue(t)
	ctx := context.Background()
	for _, data := range []string{"held", "late"} {
		if err := pq.AddTask(ctx, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	first := holdTransitionLock(t, pq, store)

	lateCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	late := make(chan PollResult, 1)
	go func() {
		row, token, _, err := pq.TryPoll(lateCtx)
		late <- PollResult{Row: row, Token: token, Err: err}
	}()
	// 读库完成后它只剩排队拿锁；期限在排队期间到期。
	<-store.reads
	<-lateCtx.Done()
	close(store.release)
	if result := <-first; result.Err != nil || string(result.Row.Data) != "held" {
		t.Fatalf("first poll = %+v", result)
	}
	if result := <-late; result.Row != nil {
		t.Fatalf("poller whose deadline passed while queued for the transition lock got %q; the lease can only be returned", result.Row.Data)
	}

	row, _, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok || string(row.Data) != "late" {
		t.Fatalf("next poll row=%+v ok=%v err=%v, want the untouched task", row, ok, err)
	}
}

// 归还租约的调用方在锁上排队期间到期：归还照常完成，任务立即可再领取。此前 UPDATE 随到期的 ctx 失败，任务
// 留在 outstanding，而唯一持有租约的一方已经放弃它，任务隐身到可见性超时。
func TestPhysicalQueue_ReturnWhoseCallerExpiredWhileQueuedCompletes(t *testing.T) {
	pq, store := newHeldDispatchQueue(t)
	store.first = true // 先不停住：这次投递产生要归还的租约
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("returned")); err != nil {
		t.Fatal(err)
	}
	_, token, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("poll ok=%v err=%v", ok, err)
	}
	if err := pq.AddTask(ctx, []byte("held")); err != nil {
		t.Fatal(err)
	}
	store.first = false
	first := holdTransitionLock(t, pq, store)

	returnCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() { returned <- pq.ReturnLease(returnCtx, token) }()
	<-returnCtx.Done()
	close(store.release)
	if result := <-first; result.Err != nil {
		t.Fatal(result.Err)
	}
	if err := <-returned; err != nil {
		t.Fatalf("return whose caller expired while queued: %v; the task stays hidden until the visibility timeout", err)
	}

	row, _, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok || string(row.Data) != "returned" {
		t.Fatalf("re-poll row=%+v ok=%v err=%v, want the returned task", row, ok, err)
	}
}
