package gormstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"gorm.io/gorm"
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
	var _ membership.MemberStore = new(MembershipStore)
	var _ membership.ExpiredMemberPruner = new(MembershipStore)
}

func TestSQLiteStoreRoutingKeyVisibilityAndIndex(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	now := time.Now()
	tasks := []*queue.TaskRow{
		{TaskID: 1, QueueName: "routed-q", PartitionID: 0, RoutingKey: "worker-a", Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "routed-q", PartitionID: 0, Data: []byte("shared"), CreatedAt: now},
		{TaskID: 3, QueueName: "routed-q", PartitionID: 0, RoutingKey: "worker-b", Data: []byte("b"), CreatedAt: now},
	}
	if err := s.CreateTasks(ctx, tasks); err != nil {
		t.Fatal(err)
	}

	rows, err := s.GetTasksWithOptions(ctx, "routed-q", 0, 0, 3, 10, queue.TaskQueryOptions{
		AcceptedRoutingKeys: []string{"worker-b", "worker-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].TaskID != 2 || rows[1].TaskID != 3 || rows[1].RoutingKey != "worker-b" {
		t.Fatalf("visible tasks = %+v", rows)
	}
	if !s.DB().Migrator().HasIndex(&taskRow{}, "idx_tasks_visibility") {
		t.Fatal("missing routed task visibility index")
	}
}

func TestSQLiteStoreExpiredRoutingDeadlineVisibilityAndIndex(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.CreateTasks(ctx, []*queue.TaskRow{
		{TaskID: 1, QueueName: "deadline-q", PartitionID: 0, RoutingKey: "future", RoutingDeadline: now.Add(time.Hour), Data: []byte("future"), CreatedAt: now},
		{TaskID: 2, QueueName: "deadline-q", PartitionID: 0, RoutingKey: "expired", RoutingDeadline: now.Add(-time.Hour), Data: []byte("expired"), CreatedAt: now},
		{TaskID: 3, QueueName: "deadline-q", PartitionID: 0, Data: []byte("shared"), CreatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.GetTasksWithOptions(ctx, "deadline-q", 0, 0, 3, 10, queue.TaskQueryOptions{
		AcceptedRoutingKeys: []string{"other"}, RoutingFallbackAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].TaskID != 2 || rows[1].TaskID != 3 || rows[0].RoutingDeadline.IsZero() {
		t.Fatalf("visible deadline rows = %+v", rows)
	}
	if !s.DB().Migrator().HasIndex(&taskRow{}, "idx_tasks_routing_deadline") {
		t.Fatal("missing expired routing visibility index")
	}
}

func TestSQLiteStoreRoutingKeySurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "taskq.db")
	first, err := NewSQLite(path, Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := first.CreateTasks(context.Background(), []*queue.TaskRow{{
		TaskID: 1, QueueName: "restart-q", PartitionID: 0,
		RoutingKey: "worker-a", Data: []byte("persisted"), CreatedAt: now,
	}}); err != nil {
		first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := NewSQLite(path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if rows, err := second.GetTasksWithOptions(context.Background(), "restart-q", 0, 0, 1, 1, queue.TaskQueryOptions{
		AcceptedRoutingKeys: []string{"worker-b"},
	}); err != nil {
		t.Fatal(err)
	} else if len(rows) != 0 {
		t.Fatalf("worker-b rows = %+v", rows)
	}
	rows, err := second.GetTasksWithOptions(context.Background(), "restart-q", 0, 0, 1, 1, queue.TaskQueryOptions{
		AcceptedRoutingKeys: []string{"worker-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].RoutingKey != "worker-a" || string(rows[0].Data) != "persisted" {
		t.Fatalf("persisted rows = %+v", rows)
	}
}

func TestSQLiteStoreRoutingDeadlineSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deadline.db")
	first, err := NewSQLite(path, Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(-time.Hour).UTC()
	if err := first.CreateTasks(context.Background(), []*queue.TaskRow{{
		TaskID: 1, QueueName: "restart-deadline-q", PartitionID: 0,
		RoutingKey: "sticky-worker", RoutingDeadline: deadline,
		Data: []byte("fallback"), CreatedAt: time.Now(),
	}}); err != nil {
		first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := NewSQLite(path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	rows, err := second.GetTasksWithOptions(context.Background(), "restart-deadline-q", 0, 0, 1, 1, queue.TaskQueryOptions{
		AcceptedRoutingKeys: []string{"other-worker"}, RoutingFallbackAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].RoutingDeadline.IsZero() || string(rows[0].Data) != "fallback" {
		t.Fatalf("restart fallback rows = %+v", rows)
	}
}

func TestSQLiteStoreMembershipUpsertAndList(t *testing.T) {
	s := newSQLiteTestStore(t)
	memberStore := s.Membership()
	ctx := context.Background()
	address := net.ParseIP("10.20.30.40")

	err := memberStore.UpsertMember(ctx, membership.UpsertMemberRequest{
		ServiceName:  "matching",
		RPCAddress:   address,
		RPCPort:      7235,
		HostID:       []byte("host-a"),
		SessionStart: time.Unix(100, 0).UTC(),
		RecordExpiry: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = memberStore.UpsertMember(ctx, membership.UpsertMemberRequest{
		ServiceName:  "matching",
		RPCAddress:   address,
		RPCPort:      7235,
		HostID:       []byte("host-b"),
		SessionStart: time.Unix(200, 0).UTC(),
		RecordExpiry: time.Hour,
		Draining:     true,
		StartAt:      123,
		StopAt:       456,
	})
	if err != nil {
		t.Fatal(err)
	}

	members, err := memberStore.GetActiveMembers(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 {
		t.Fatalf("active members = %d, want 1", len(members))
	}
	member := members[0]
	if !member.RPCAddress.Equal(address) || member.RPCPort != 7235 || member.ServiceName != "matching" ||
		!member.Draining || member.StartAt != 123 || member.StopAt != 456 {
		t.Fatalf("active member = %+v", member)
	}
}

func TestSQLiteStoreMembershipExpiryAndBoundedPrune(t *testing.T) {
	s := newSQLiteTestStore(t)
	memberStore := s.Membership()
	ctx := context.Background()

	for i := range 3 {
		err := memberStore.UpsertMember(ctx, membership.UpsertMemberRequest{
			ServiceName:  "matching",
			RPCAddress:   net.ParseIP(fmt.Sprintf("10.0.0.%d", i+1)),
			RPCPort:      7235,
			HostID:       []byte{byte(i + 1)},
			SessionStart: time.Now().UTC(),
			RecordExpiry: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)

	members, err := memberStore.GetActiveMembers(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 0 {
		t.Fatalf("active members = %d, want 0", len(members))
	}
	if err := memberStore.PruneExpiredMembers(ctx, 2); err != nil {
		t.Fatal(err)
	}
	var remaining int64
	if err := s.DB().Table(defaultMembershipTable).Count(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("remaining expired members = %d, want 1", remaining)
	}
	if err := memberStore.PruneExpiredMembers(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().Table(defaultMembershipTable).Count(&remaining).Error; err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("remaining expired members = %d, want 0", remaining)
	}
}

func TestSQLiteStoreMembershipPrunePreservesConcurrentRenewal(t *testing.T) {
	s := newSQLiteTestStore(t)
	memberStore := s.Membership()
	ctx := context.Background()
	address := net.ParseIP("10.0.0.9")
	err := memberStore.UpsertMember(ctx, membership.UpsertMemberRequest{
		ServiceName:  "matching",
		RPCAddress:   address,
		RPCPort:      7235,
		HostID:       []byte("host-a"),
		SessionStart: time.Now().UTC(),
		RecordExpiry: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	const callback = "test:renew_membership_before_prune_delete"
	err = s.DB().Callback().Delete().Before("gorm:delete").Register(callback, func(tx *gorm.DB) {
		if err := tx.Session(&gorm.Session{NewDB: true}).Table(defaultMembershipTable).
			Where("service_name = ? AND rpc_address = ? AND rpc_port = ?", "matching", address.String(), 7235).
			Update("expires_at", time.Now().UTC().Add(time.Hour)).Error; err != nil {
			tx.AddError(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.DB().Callback().Delete().Remove(callback) })

	if err := memberStore.PruneExpiredMembers(ctx, 1); err != nil {
		t.Fatal(err)
	}
	members, err := memberStore.GetActiveMembers(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || !members[0].RPCAddress.Equal(address) {
		t.Fatalf("concurrently renewed member was pruned: %+v", members)
	}
}

func TestSQLiteStoreMembershipConcurrentHeartbeats(t *testing.T) {
	s := newSQLiteTestStore(t)
	memberStore := s.Membership()
	ctx := context.Background()
	address := net.ParseIP("10.30.40.50")

	const writers = 32
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- memberStore.UpsertMember(ctx, membership.UpsertMemberRequest{
				ServiceName:  "matching",
				RPCAddress:   address,
				RPCPort:      7235,
				HostID:       []byte{byte(i + 1)},
				SessionStart: time.Unix(int64(i+1), 0).UTC(),
				RecordExpiry: time.Hour,
				Draining:     i%2 == 0,
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	members, err := memberStore.GetActiveMembers(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || !members[0].RPCAddress.Equal(address) {
		t.Fatalf("active members = %+v, want one member at %s", members, address)
	}
}

func TestSQLiteStoreMembershipRejectsIncompleteHeartbeats(t *testing.T) {
	s := newSQLiteTestStore(t)
	memberStore := s.Membership()
	valid := membership.UpsertMemberRequest{
		ServiceName:  "matching",
		RPCAddress:   net.ParseIP("10.0.0.1"),
		RPCPort:      7235,
		HostID:       []byte("host-a"),
		SessionStart: time.Now().UTC(),
		RecordExpiry: time.Hour,
	}
	tests := []struct {
		name   string
		mutate func(*membership.UpsertMemberRequest)
	}{
		{name: "service", mutate: func(req *membership.UpsertMemberRequest) { req.ServiceName = "" }},
		{name: "address", mutate: func(req *membership.UpsertMemberRequest) { req.RPCAddress = nil }},
		{name: "port", mutate: func(req *membership.UpsertMemberRequest) { req.RPCPort = 0 }},
		{name: "host ID", mutate: func(req *membership.UpsertMemberRequest) { req.HostID = nil }},
		{name: "session start", mutate: func(req *membership.UpsertMemberRequest) { req.SessionStart = time.Time{} }},
		{name: "record expiry", mutate: func(req *membership.UpsertMemberRequest) { req.RecordExpiry = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := valid
			test.mutate(&req)
			if err := memberStore.UpsertMember(context.Background(), req); err == nil {
				t.Fatal("UpsertMember accepted an incomplete heartbeat")
			}
		})
	}
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

func TestSQLiteStoreCreateTasksAndAdvanceRangeRollsBackTogether(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	info := &queue.QueueInfo{QueueName: "atomic-write-q", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 1}
	if err := s.CreateQueue(ctx, info); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	duplicate := &queue.TaskRow{
		TaskID: 1, QueueName: "atomic-write-q", PartitionID: 0, Data: []byte("existing"), CreatedAt: now,
	}
	if err := s.CreateTasks(ctx, []*queue.TaskRow{duplicate}); err != nil {
		t.Fatal(err)
	}

	_, err := s.CreateTasksAndAdvanceRange(
		ctx,
		"atomic-write-q",
		0,
		[]*queue.TaskRow{{
			TaskID: 1, QueueName: "atomic-write-q", PartitionID: 0, Data: []byte("duplicate"), CreatedAt: now,
		}},
		2,
		1,
	)
	if err == nil {
		t.Fatal("expected duplicate task insert to fail")
	}

	got, err := s.GetQueue(ctx, "atomic-write-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.RangeID != 1 || got.NextTaskID != 1 {
		t.Fatalf("queue info = %+v, want range=1 next_task_id=1", got)
	}
	rows, err := s.GetTasks(ctx, "atomic-write-q", 0, 0, 10, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || string(rows[0].Data) != "existing" {
		t.Fatalf("tasks = %+v, want original task", rows)
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

	ids, err := s.ResetTimedOutTasks(ctx, "q", 0, queue.VisibilityResetOptions{
		AckLevel:          0,
		VisibilityTimeout: 60 * time.Second,
	})
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

func TestSQLiteStoreTaskAttemptRoundTrip(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.CreateTasks(ctx, []*queue.TaskRow{
		{TaskID: 1, QueueName: "attempt-q", PartitionID: 0, Data: []byte("first"), CreatedAt: now},
		{TaskID: 2, QueueName: "attempt-q", PartitionID: 0, Data: []byte("retried"), Attempt: 3, CreatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetTasks(ctx, "attempt-q", 0, 0, 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 tasks, got %+v", got)
	}
	if got[0].TaskID != 1 || got[0].Attempt != 0 {
		t.Fatalf("first attempt = %+v", got[0])
	}
	if got[1].TaskID != 2 || got[1].Attempt != 3 {
		t.Fatalf("retried attempt = %+v", got[1])
	}
}

func TestSQLiteStoreResetTimedOutTasksScalesAcknowledgedFilter(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	past := time.Now().Add(-2 * time.Minute)
	const taskCount = 620
	const acknowledgedCount = 610

	tasks := make([]*queue.TaskRow, taskCount)
	acknowledged := make([]int64, acknowledgedCount)
	for i := range taskCount {
		taskID := int64(i + 1)
		dispatchedAt := past
		tasks[i] = &queue.TaskRow{
			TaskID: taskID, QueueName: "large-ack-filter-q", PartitionID: 0,
			Data: []byte("task"), CreatedAt: past, DispatchedAt: &dispatchedAt,
		}
		if i < acknowledgedCount {
			acknowledged[i] = taskID
		}
	}
	if err := s.CreateTasks(ctx, tasks); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ResetTimedOutTasks(ctx, "large-ack-filter-q", 0, queue.VisibilityResetOptions{
		VisibilityTimeout: time.Minute,
		AcknowledgedIDs:   acknowledged,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != taskCount-acknowledgedCount || ids[0] != acknowledgedCount+1 || ids[len(ids)-1] != taskCount {
		t.Fatalf("reset IDs = %v, want only unacknowledged tail", ids)
	}
	visible, err := s.GetTasks(ctx, "large-ack-filter-q", 0, 0, taskCount, taskCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(visible) != taskCount-acknowledgedCount {
		t.Fatalf("visible tasks = %d, want %d", len(visible), taskCount-acknowledgedCount)
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
