package gormstore

import (
	"context"
	"errors"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/queue"
)

func newSQLiteTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewSQLite(":memory:", Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLiteStoreImplementsContracts(t *testing.T) {
	var _ queue.Store = new(Store)
	var _ dbleader.LockStore = new(Store)
}

func TestSQLiteStoreCreateAndGetQueue(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	info := &queue.QueueInfo{QueueName: "test", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 1}
	if err := s.CreateQueue(ctx, info); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetQueue(ctx, "test", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.RangeID != 1 || got.NextTaskID != 1 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestSQLiteStoreAllocateTaskIDRangeCAS(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	info := &queue.QueueInfo{QueueName: "test", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 1}
	if err := s.CreateQueue(ctx, info); err != nil {
		t.Fatal(err)
	}

	newRangeID, err := s.AllocateTaskIDRange(ctx, "test", 0, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if newRangeID != 2 {
		t.Fatalf("expected range ID 2, got %d", newRangeID)
	}
	got, err := s.GetQueue(ctx, "test", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.NextTaskID != 10 || got.AckLevel != 0 {
		t.Fatalf("unexpected queue info: %+v", got)
	}
	if _, err := s.AllocateTaskIDRange(ctx, "test", 0, 11, 1); !errors.Is(err, queue.ErrConditionFailed) {
		t.Fatalf("expected ErrConditionFailed, got %v", err)
	}
}

func TestSQLiteStoreAdvanceAckLevelAndDeleteTasksAtomic(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	info := &queue.QueueInfo{QueueName: "atomic-q", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 4}
	if err := s.CreateQueue(ctx, info); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tasks := []*queue.TaskRow{
		{TaskID: 1, QueueName: "atomic-q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "atomic-q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
		{TaskID: 3, QueueName: "atomic-q", PartitionID: 0, Data: []byte("c"), CreatedAt: now},
	}
	if err := s.CreateTasks(ctx, tasks); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Exec(`
		CREATE TRIGGER fail_task_delete
		BEFORE DELETE ON tasks
		BEGIN
			SELECT RAISE(ABORT, 'delete blocked');
		END;
	`).Error; err != nil {
		t.Fatal(err)
	}

	_, _, err := s.AdvanceAckLevelAndDeleteTasks(ctx, "atomic-q", 0, 2, 1)
	if err == nil {
		t.Fatal("expected delete failure")
	}

	got, err := s.GetQueue(ctx, "atomic-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.RangeID != 1 || got.AckLevel != 0 {
		t.Fatalf("expected queue_info rollback to range=1 ack=0, got %+v", got)
	}
	rows, err := s.GetTasks(ctx, "atomic-q", 0, 0, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected all tasks to remain after rollback, got %d", len(rows))
	}
}

func TestSQLiteStoreAdvanceAckLevelMonotonic(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	info := &queue.QueueInfo{QueueName: "mono-q", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 6}
	if err := s.CreateQueue(ctx, info); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	tasks := []*queue.TaskRow{
		{TaskID: 1, QueueName: "mono-q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "mono-q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
		{TaskID: 3, QueueName: "mono-q", PartitionID: 0, Data: []byte("c"), CreatedAt: now},
		{TaskID: 4, QueueName: "mono-q", PartitionID: 0, Data: []byte("d"), CreatedAt: now},
		{TaskID: 5, QueueName: "mono-q", PartitionID: 0, Data: []byte("e"), CreatedAt: now},
	}
	if err := s.CreateTasks(ctx, tasks); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdvanceAckLevelAndDeleteTasks(ctx, "mono-q", 0, 4, 1); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetQueue(ctx, "mono-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AdvanceAckLevelAndDeleteTasks(ctx, "mono-q", 0, 2, got.RangeID); err != nil {
		t.Fatal(err)
	}

	got, err = s.GetQueue(ctx, "mono-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.AckLevel != 4 {
		t.Fatalf("expected ack level to stay at 4, got %d", got.AckLevel)
	}
	rows, err := s.GetTasks(ctx, "mono-q", 0, 0, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].TaskID != 5 {
		t.Fatalf("expected only task 5 to remain, got %+v", rows)
	}
}

func TestSQLiteStoreTasksDispatchAndReset(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	past := time.Now().Add(-2 * time.Minute)
	tasks := []*queue.TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: past},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: past},
		{TaskID: 1, QueueName: "q", PartitionID: 1, Data: []byte("p1"), CreatedAt: past},
	}
	if err := s.CreateTasks(ctx, tasks); err != nil {
		t.Fatal(err)
	}
	if err := s.DispatchTask(ctx, "q", 0, 1, past); err != nil {
		t.Fatal(err)
	}
	if err := s.DispatchTask(ctx, "q", 0, 2, past); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected partition 0 dispatched tasks to be hidden, got %+v", got)
	}
	got, err = s.GetTasks(ctx, "q", 1, 0, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || string(got[0].Data) != "p1" {
		t.Fatalf("partition isolation failed: %+v", got)
	}

	ids, err := s.ResetTimedOutTasks(ctx, "q", 0, 0, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 timed out, got %d", len(ids))
	}
	got, err = s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 tasks after reset, got %d", len(got))
	}
}

func TestSQLiteStoreMissingTaskErrors(t *testing.T) {
	s := newSQLiteTestStore(t)

	err := s.DispatchTask(context.Background(), "q", 0, 42, time.Now())
	if !errors.Is(err, queue.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
	err = s.UnDispatchTask(context.Background(), "q", 0, 42)
	if !errors.Is(err, queue.ErrTaskNotFound) {
		t.Fatalf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestSQLiteStoreLockLifecycle(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()

	acquired, err := s.TryAcquire(ctx, "lock-a", "node-a", 30)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired.Acquired || acquired.LeaderIP != "node-a" || acquired.Version != 1 {
		t.Fatalf("unexpected first acquire: %+v", acquired)
	}
	leader, err := s.GetLeaderAddr(ctx, "lock-a")
	if err != nil {
		t.Fatal(err)
	}
	if leader != "node-a" {
		t.Fatalf("expected node-a leader, got %q", leader)
	}
	blocked, err := s.TryAcquire(ctx, "lock-a", "node-b", 30)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Acquired || blocked.LeaderIP != "node-a" {
		t.Fatalf("expected node-b acquire to observe node-a leader, got %+v", blocked)
	}
	if ok, err := s.Renew(ctx, "lock-a", "node-a", 30); err != nil || !ok {
		t.Fatalf("expected node-a renew success, ok=%v err=%v", ok, err)
	}
	if ok, err := s.Renew(ctx, "lock-a", "node-b", 30); err != nil || ok {
		t.Fatalf("expected node-b renew to fail cleanly, ok=%v err=%v", ok, err)
	}
	if err := s.ForceAssign(ctx, "lock-a", "node-c", 30); err != nil {
		t.Fatal(err)
	}
	leader, err = s.GetLeaderAddr(ctx, "lock-a")
	if err != nil {
		t.Fatal(err)
	}
	if leader != "node-c" {
		t.Fatalf("expected node-c leader after force assign, got %q", leader)
	}
	if err := s.Release(ctx, "lock-a", "node-c"); err != nil {
		t.Fatal(err)
	}
	leader, err = s.GetLeaderAddr(ctx, "lock-a")
	if err != nil {
		t.Fatal(err)
	}
	if leader != "" {
		t.Fatalf("expected no leader after release, got %q", leader)
	}
}

func TestSQLiteStoreForceAssignMissingLock(t *testing.T) {
	s := newSQLiteTestStore(t)

	err := s.ForceAssign(context.Background(), "missing", "node-a", 30)
	if !errors.Is(err, dbleader.ErrLockNotFound) {
		t.Fatalf("expected ErrLockNotFound, got %v", err)
	}
}

func TestSQLiteStoreMemberLifecycle(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()

	if err := s.UpsertMember(ctx, dbleader.MemberInfo{
		LockName:       "lock-members",
		InstanceID:     "instance-a",
		NodeIP:         "node-a",
		MacAddress:     "mac-a",
		Hostname:       "host-a",
		PID:            123,
		OS:             "test-os",
		GoVersion:      "go-test",
		ServiceVersion: "v1",
		Description:    "first",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertMember(ctx, dbleader.MemberInfo{
		LockName:   "lock-members",
		InstanceID: "instance-b",
		NodeIP:     "node-b",
	}); err != nil {
		t.Fatal(err)
	}

	members, err := s.ListMembers(ctx, "lock-members", 15)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("expected two active members, got %+v", members)
	}

	if err := s.DB().Exec(
		`UPDATE taskq_partition_lock_members SET last_seen = ? WHERE instance_id = ?`,
		time.Now().Add(-time.Minute),
		"instance-b",
	).Error; err != nil {
		t.Fatal(err)
	}

	members, err = s.ListMembers(ctx, "lock-members", 15)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].InstanceID != "instance-a" {
		t.Fatalf("expected only instance-a active, got %+v", members)
	}

	removed, err := s.PurgeStaleMembers(ctx, "lock-members", 15)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("expected one stale member purged, got %d", removed)
	}
}
