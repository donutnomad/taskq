package queue

import (
	"context"
	"errors"
	"sync"
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

type getTasksBarrierStore struct {
	Store

	mu      sync.Mutex
	hits    int
	release chan struct{}
}

func newGetTasksBarrierStore(base Store) *getTasksBarrierStore {
	return &getTasksBarrierStore{
		Store:   base,
		release: make(chan struct{}),
	}
}

func (s *getTasksBarrierStore) GetTasks(
	ctx context.Context,
	queueName string,
	partitionID int,
	minTaskID int64,
	maxTaskID int64,
	limit int,
) ([]*TaskRow, error) {
	rows, err := s.Store.GetTasks(ctx, queueName, partitionID, minTaskID, maxTaskID, limit)
	if err != nil || len(rows) == 0 {
		return rows, err
	}

	s.mu.Lock()
	s.hits++
	if s.hits == 2 {
		close(s.release)
	}
	release := s.release
	s.mu.Unlock()

	select {
	case <-release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return rows, nil
}

func TestBugRepro_TryPollShouldNotDeliverSameTaskConcurrently(t *testing.T) {
	base := newTestStore(t)
	store := newGetTasksBarrierStore(base)

	pq, err := newPhysicalQueue(context.Background(), store, "dup-q", 0, 60*time.Second, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pq.Close() })

	if err := pq.AddTask(context.Background(), []byte("task-1")); err != nil {
		t.Fatal(err)
	}

	type pollResult struct {
		ok    bool
		token string
		err   error
	}

	start := make(chan struct{})
	results := make(chan pollResult, 2)
	for range 2 {
		go func() {
			<-start
			_, token, ok := pq.TryPoll(context.Background())
			results <- pollResult{ok: ok, token: token}
		}()
	}
	close(start)

	got := make([]pollResult, 0, 2)
	timeout := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case result := <-results:
			got = append(got, result)
		case <-timeout:
			t.Fatal("timed out waiting for concurrent poll results")
		}
	}

	successes := 0
	for _, result := range got {
		if result.err != nil {
			t.Fatalf("expected polling to complete without transport error, got %v", result.err)
		}
		if result.ok {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one successful poll, got %d results: %+v", successes, got)
	}
}

func TestBugRepro_NewQueueInstanceShouldReadExistingUndispatchedTasks(t *testing.T) {
	store := newTestStore(t)

	pq1, err := newPhysicalQueue(context.Background(), store, "persisted-q", 0, 60*time.Second, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := pq1.AddTask(context.Background(), []byte("persisted-task")); err != nil {
		t.Fatal(err)
	}
	pq1.Close()

	pq2, err := newPhysicalQueue(context.Background(), store, "persisted-q", 0, 60*time.Second, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pq2.Close() })

	row, _, ok := pq2.TryPoll(context.Background())
	if !ok || row == nil {
		t.Fatal("expected reopened queue to read existing undispatched task")
	}
	if string(row.Data) != "persisted-task" {
		t.Fatalf("expected persisted-task, got %q", row.Data)
	}
}

func TestBugRepro_LeaseHandoverWindowDoesNotDoubleDeliverSameTask(t *testing.T) {
	base := newTestStore(t)
	barrierStore := newGetTasksBarrierStore(base)

	pqOld, err := newPhysicalQueue(context.Background(), barrierStore, "handover-q", 0, 60*time.Second, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	defer pqOld.Close()

	if err := pqOld.AddTask(context.Background(), []byte("task-1")); err != nil {
		t.Fatal(err)
	}

	sqlite, ok := base.(*sqliteStore)
	if !ok {
		t.Fatal("expected sqlite store")
	}
	if _, err := sqlite.db.Exec(
		`UPDATE queue_info SET lease_expires_at = ? WHERE queue_name = ? AND partition_id = ?`,
		time.Now().Add(-time.Second),
		"handover-q",
		0,
	); err != nil {
		t.Fatalf("expire old lease: %v", err)
	}

	pqNew, err := newPhysicalQueue(context.Background(), barrierStore, "handover-q", 0, 60*time.Second, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	defer pqNew.Close()

	type pollResult struct {
		ok    bool
		token string
	}

	start := make(chan struct{})
	results := make(chan pollResult, 2)

	go func() {
		<-start
		_, token, ok := pqOld.TryPoll(context.Background())
		results <- pollResult{ok: ok, token: token}
	}()
	go func() {
		<-start
		_, token, ok := pqNew.TryPoll(context.Background())
		results <- pollResult{ok: ok, token: token}
	}()
	close(start)

	got := make([]pollResult, 0, 2)
	timeout := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case result := <-results:
			got = append(got, result)
		case <-timeout:
			t.Fatal("timed out waiting for handover poll results")
		}
	}

	successes := 0
	for _, result := range got {
		if result.ok {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one delivery during lease handover, got %d results: %+v", successes, got)
	}
}
