//go:build integration

package gormstore

import (
	"context"
	"errors"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/queue"
	tcMysql "github.com/testcontainers/testcontainers-go/modules/mysql"
)

func newMySQLTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	container, err := tcMysql.Run(ctx,
		"mysql:8.4",
		tcMysql.WithDatabase("taskq"),
		tcMysql.WithUsername("taskq"),
		tcMysql.WithPassword("taskq"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate mysql container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "parseTime=true", "loc=UTC")
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewMySQL(dsn, Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMySQLStoreQueueCASAndDelivery(t *testing.T) {
	s := newMySQLTestStore(t)
	ctx := context.Background()
	info := &queue.QueueInfo{QueueName: "mysql-q", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 1}
	if err := s.CreateQueue(ctx, info); err != nil {
		t.Fatal(err)
	}
	newRangeID, err := s.AllocateTaskIDRange(ctx, "mysql-q", 0, 10, 1)
	if err != nil {
		t.Fatal(err)
	}
	if newRangeID != 2 {
		t.Fatalf("expected range ID 2, got %d", newRangeID)
	}
	if _, err := s.AllocateTaskIDRange(ctx, "mysql-q", 0, 11, 1); !errors.Is(err, queue.ErrConditionFailed) {
		t.Fatalf("expected ErrConditionFailed, got %v", err)
	}

	past := time.Now().Add(-2 * time.Minute)
	if err := s.CreateTasks(ctx, []*queue.TaskRow{
		{TaskID: 1, QueueName: "mysql-q", PartitionID: 0, Data: []byte("a"), CreatedAt: past},
		{TaskID: 2, QueueName: "mysql-q", PartitionID: 0, Data: []byte("b"), CreatedAt: past},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DispatchTask(ctx, "mysql-q", 0, 1, past); err != nil {
		t.Fatal(err)
	}
	ids, err := s.ResetTimedOutTasks(ctx, "mysql-q", 0, 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected task 1 reset, got %+v", ids)
	}

	rangeID, deleted, err := s.AdvanceAckLevelAndDeleteTasks(ctx, "mysql-q", 0, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if rangeID != 3 || deleted != 1 {
		t.Fatalf("expected range=3 deleted=1, got range=%d deleted=%d", rangeID, deleted)
	}
}

func TestMySQLStoreLockAndMembers(t *testing.T) {
	s := newMySQLTestStore(t)
	ctx := context.Background()

	acquired, err := s.TryAcquire(ctx, "lock-a", "node-a", 30)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired.Acquired || acquired.LeaderIP != "node-a" || acquired.Version != 1 {
		t.Fatalf("unexpected first acquire: %+v", acquired)
	}
	blocked, err := s.TryAcquire(ctx, "lock-a", "node-b", 30)
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Acquired || blocked.LeaderIP != "node-a" {
		t.Fatalf("expected node-b acquire to observe node-a leader, got %+v", blocked)
	}
	if err := s.ForceAssign(ctx, "lock-a", "node-c", 30); err != nil {
		t.Fatal(err)
	}
	leader, err := s.GetLeaderAddr(ctx, "lock-a")
	if err != nil {
		t.Fatal(err)
	}
	if leader != "node-c" {
		t.Fatalf("expected node-c leader, got %q", leader)
	}

	if err := s.UpsertMember(ctx, dbleader.MemberInfo{LockName: "lock-a", InstanceID: "a", NodeIP: "node-a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertMember(ctx, dbleader.MemberInfo{LockName: "lock-a", InstanceID: "b", NodeIP: "node-b"}); err != nil {
		t.Fatal(err)
	}
	members, err := s.ListMembers(ctx, "lock-a", 15)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("expected two members, got %+v", members)
	}
	if err := s.DB().Exec(
		`UPDATE taskq_partition_lock_members SET last_seen = ? WHERE instance_id = ?`,
		time.Now().Add(-time.Minute),
		"b",
	).Error; err != nil {
		t.Fatal(err)
	}
	removed, err := s.PurgeStaleMembers(ctx, "lock-a", 15)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("expected one stale member purged, got %d", removed)
	}
}
