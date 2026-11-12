package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type rangeUpdateErrorStore struct {
	err error
}

func (s *rangeUpdateErrorStore) CreateQueue(context.Context, *QueueInfo) error { return nil }
func (s *rangeUpdateErrorStore) GetQueue(context.Context, string, int) (*QueueInfo, error) {
	return nil, nil
}
func (s *rangeUpdateErrorStore) AllocateTaskIDRange(context.Context, string, int, int64, int64) (int64, error) {
	return 0, s.err
}
func (s *rangeUpdateErrorStore) AdvanceAckLevelAndDeleteTasks(context.Context, string, int, int64, int64) (int64, int64, error) {
	return 0, 0, nil
}
func (s *rangeUpdateErrorStore) ResolvePartitions(context.Context, string) (int, int, error) {
	return 0, 0, nil
}
func (s *rangeUpdateErrorStore) SetPartitions(context.Context, string, int, int) error { return nil }
func (s *rangeUpdateErrorStore) SetDefaultPartitions(context.Context, int, int) error  { return nil }
func (s *rangeUpdateErrorStore) EnsureDefaultPartitions(context.Context, int, int) error {
	return nil
}
func (s *rangeUpdateErrorStore) ListPartitionOverrides(context.Context) ([]QueuePartitionConfig, error) {
	return nil, nil
}
func (s *rangeUpdateErrorStore) ListQueueNames(context.Context) ([]string, error) { return nil, nil }
func (s *rangeUpdateErrorStore) CreateTasks(context.Context, []*TaskRow) error    { return nil }
func (s *rangeUpdateErrorStore) CreateTasksAndAdvanceRange(context.Context, string, int, []*TaskRow, int64, int64) (int64, error) {
	return 0, s.err
}
func (s *rangeUpdateErrorStore) GetTasks(context.Context, string, int, int64, int64, int) ([]*TaskRow, error) {
	return nil, nil
}
func (s *rangeUpdateErrorStore) GetTasksWithOptions(context.Context, string, int, int64, int64, int, TaskQueryOptions) ([]*TaskRow, error) {
	return nil, nil
}
func (s *rangeUpdateErrorStore) DispatchTask(context.Context, string, int, int64, time.Time) error {
	return nil
}
func (s *rangeUpdateErrorStore) UnDispatchTask(context.Context, string, int, int64) error {
	return nil
}
func (s *rangeUpdateErrorStore) ResetTimedOutTasks(context.Context, string, int, VisibilityResetOptions) ([]int64, error) {
	return nil, nil
}
func (s *rangeUpdateErrorStore) MinTaskIDAfter(context.Context, string, int, int64) (int64, bool, error) {
	return 0, false, nil
}
func (s *rangeUpdateErrorStore) Close() error { return nil }

func TestBugRepro_CreateTasksAndAdvanceRangeKeepsNonCASFailures(t *testing.T) {
	store := &rangeUpdateErrorStore{err: context.DeadlineExceeded}
	ownership := newOwnershipState(store, 7)

	err := ownership.createTasksAndAdvanceRange(context.Background(), "q", 0, []*TaskRow{{TaskID: 1}}, 2)
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
	return s.waitForConcurrentReads(ctx, rows, err)
}

func (s *getTasksBarrierStore) GetTasksWithOptions(
	ctx context.Context,
	queueName string,
	partitionID int,
	minTaskID int64,
	maxTaskID int64,
	limit int,
	opts TaskQueryOptions,
) ([]*TaskRow, error) {
	rows, err := s.Store.GetTasksWithOptions(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	return s.waitForConcurrentReads(ctx, rows, err)
}

func (s *getTasksBarrierStore) waitForConcurrentReads(ctx context.Context, rows []*TaskRow, err error) ([]*TaskRow, error) {
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

	pq, err := newPhysicalQueue(context.Background(), store, "dup-q", 0, 60*time.Second, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
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
			_, token, ok, err := pq.TryPoll(context.Background())
			results <- pollResult{ok: ok, token: token, err: err}
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

	pq1, err := newPhysicalQueue(context.Background(), store, "persisted-q", 0, 60*time.Second, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq1)
	if err := pq1.AddTask(context.Background(), []byte("persisted-task")); err != nil {
		t.Fatal(err)
	}
	pq1.Close()

	pq2, err := newPhysicalQueue(context.Background(), store, "persisted-q", 0, 60*time.Second, defaultMaxAttempts, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq2)
	t.Cleanup(func() { pq2.Close() })

	row, _, ok, err := pq2.TryPoll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
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

	pqOld, err := newPhysicalQueue(context.Background(), barrierStore, "handover-q", 0, 60*time.Second, defaultMaxAttempts, "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pqOld)
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

	pqNew, err := newPhysicalQueue(context.Background(), barrierStore, "handover-q", 0, 60*time.Second, defaultMaxAttempts, "owner-b")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pqNew)
	defer pqNew.Close()

	type pollResult struct {
		ok    bool
		token string
	}

	start := make(chan struct{})
	results := make(chan pollResult, 2)

	go func() {
		<-start
		_, token, ok, _ := pqOld.TryPoll(context.Background())
		results <- pollResult{ok: ok, token: token}
	}()
	go func() {
		<-start
		_, token, ok, _ := pqNew.TryPoll(context.Background())
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
