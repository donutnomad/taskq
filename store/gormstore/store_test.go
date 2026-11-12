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

func TestSQLiteStoreEnsureDefaultPartitionsIdempotent(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()

	if err := s.EnsureDefaultPartitions(ctx, 4, 4); err != nil {
		t.Fatal(err)
	}
	// 重复调用不覆盖已有值。
	if err := s.EnsureDefaultPartitions(ctx, 8, 6); err != nil {
		t.Fatal(err)
	}
	read, write, err := s.ResolvePartitions(ctx, "any-queue")
	if err != nil {
		t.Fatal(err)
	}
	if read != 4 || write != 4 {
		t.Fatalf("default partitions = read=%d write=%d, want 4/4 (EnsureDefaultPartitions must not overwrite)", read, write)
	}
}

func TestSQLiteStoreResolvePartitionsMissingDefault(t *testing.T) {
	s := newSQLiteTestStore(t)
	// _default_ 行不存在时（未 bootstrap），无 override 的 queue 解析必须报错，
	// 而非静默返回 0（会导致下游 %0 panic）。
	if _, _, err := s.ResolvePartitions(context.Background(), "unbootstrapped"); err == nil {
		t.Fatal("expected error when default partition config is missing")
	}
}

func TestSQLiteStoreResolvePartitionsOverrideAndFallback(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	if err := s.EnsureDefaultPartitions(ctx, 4, 4); err != nil {
		t.Fatal(err)
	}

	// 无 override → fallback 全局默认。
	read, write, err := s.ResolvePartitions(ctx, "plain-queue")
	if err != nil {
		t.Fatal(err)
	}
	if read != 4 || write != 4 {
		t.Fatalf("fallback = read=%d write=%d, want 4/4", read, write)
	}

	// 有 override → 返回 override。
	if err := s.SetPartitions(ctx, "hot-queue", 8, 6); err != nil {
		t.Fatal(err)
	}
	read, write, err = s.ResolvePartitions(ctx, "hot-queue")
	if err != nil {
		t.Fatal(err)
	}
	if read != 8 || write != 6 {
		t.Fatalf("override = read=%d write=%d, want 8/6", read, write)
	}
	// 其他 queue 仍走默认，不受 override 影响。
	read, write, err = s.ResolvePartitions(ctx, "cold-queue")
	if err != nil {
		t.Fatal(err)
	}
	if read != 4 || write != 4 {
		t.Fatalf("unrelated queue = read=%d write=%d, want 4/4", read, write)
	}
}

func TestSQLiteStoreSetPartitionsUpsertAndValidation(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	if err := s.EnsureDefaultPartitions(ctx, 4, 4); err != nil {
		t.Fatal(err)
	}

	if err := s.SetPartitions(ctx, "q", 6, 3); err != nil {
		t.Fatal(err)
	}
	// upsert：再次调用更新既有行。
	if err := s.SetPartitions(ctx, "q", 10, 10); err != nil {
		t.Fatal(err)
	}
	read, write, err := s.ResolvePartitions(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if read != 10 || write != 10 {
		t.Fatalf("upserted override = read=%d write=%d, want 10/10", read, write)
	}

	// 校验 1<=write<=read：非法值必须返回错误。
	for _, tc := range []struct {
		name        string
		read, write int
	}{
		{"write>read", 4, 5},
		{"write<1", 4, 0},
		{"negative", -1, -1},
	} {
		if err := s.SetPartitions(ctx, "q", tc.read, tc.write); err == nil {
			t.Fatalf("%s: expected validation error for read=%d write=%d", tc.name, tc.read, tc.write)
		}
	}
	// 非法写入被拒后，既有值保持不变。
	read, write, err = s.ResolvePartitions(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if read != 10 || write != 10 {
		t.Fatalf("override after rejected writes = read=%d write=%d, want 10/10", read, write)
	}
}

func TestSQLiteStoreSetDefaultPartitionsGrowOnly(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	if err := s.EnsureDefaultPartitions(ctx, 4, 4); err != nil {
		t.Fatal(err)
	}

	// 增大成功。
	if err := s.SetDefaultPartitions(ctx, 8, 6); err != nil {
		t.Fatal(err)
	}
	read, write, err := s.ResolvePartitions(ctx, "any")
	if err != nil {
		t.Fatal(err)
	}
	if read != 8 || write != 6 {
		t.Fatalf("grown default = read=%d write=%d, want 8/6", read, write)
	}

	// 任一维度缩小都被拒绝。
	for _, tc := range []struct {
		name        string
		read, write int
	}{
		{"shrink read", 6, 6},
		{"shrink write", 8, 4},
		{"shrink both", 4, 4},
	} {
		if err := s.SetDefaultPartitions(ctx, tc.read, tc.write); err == nil {
			t.Fatalf("%s: expected grow-only rejection for read=%d write=%d", tc.name, tc.read, tc.write)
		}
	}
	// 不变式违反（write>read）也被拒绝。
	if err := s.SetDefaultPartitions(ctx, 8, 9); err == nil {
		t.Fatal("expected rejection for write>read")
	}
	// 被拒后默认值保持不变。
	read, write, err = s.ResolvePartitions(ctx, "any")
	if err != nil {
		t.Fatal(err)
	}
	if read != 8 || write != 6 {
		t.Fatalf("default after rejected shrinks = read=%d write=%d, want 8/6", read, write)
	}
}

func TestSQLiteStoreSetDefaultPartitionsMissing(t *testing.T) {
	s := newSQLiteTestStore(t)
	// _default_ 未初始化时，SetDefaultPartitions 必须报错而非静默创建
	// （条件 UPDATE 命中 0 行，读回确认行不存在）。
	if err := s.SetDefaultPartitions(context.Background(), 4, 4); err == nil {
		t.Fatal("expected error when default partition config is missing")
	}
}

func TestSQLiteStoreSetDefaultPartitionsEqualAndBoundary(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	if err := s.EnsureDefaultPartitions(ctx, 4, 4); err != nil {
		t.Fatal(err)
	}

	// grow-only 边界：相等值允许（CAS 的 <= 相等成立），幂等成功。
	if err := s.SetDefaultPartitions(ctx, 4, 4); err != nil {
		t.Fatalf("equal values must be allowed (grow-only boundary, not shrink): %v", err)
	}
	// 仅增大 read、write 保持不变（write 维相等）：允许。
	if err := s.SetDefaultPartitions(ctx, 8, 4); err != nil {
		t.Fatal(err)
	}
	// 仅增大 write、read 保持不变（read 维相等）：允许。
	if err := s.SetDefaultPartitions(ctx, 8, 6); err != nil {
		t.Fatal(err)
	}
	read, write, err := s.ResolvePartitions(ctx, "any")
	if err != nil {
		t.Fatal(err)
	}
	if read != 8 || write != 6 {
		t.Fatalf("grown default = read=%d write=%d, want 8/6", read, write)
	}

	// 缩小任一维（另一维不变）都被 CAS 条件挡下，且值保持不变。
	if err := s.SetDefaultPartitions(ctx, 7, 6); err == nil {
		t.Fatal("expected grow-only rejection when shrinking read")
	}
	if err := s.SetDefaultPartitions(ctx, 8, 5); err == nil {
		t.Fatal("expected grow-only rejection when shrinking write")
	}
	read, write, err = s.ResolvePartitions(ctx, "any")
	if err != nil {
		t.Fatal(err)
	}
	if read != 8 || write != 6 {
		t.Fatalf("default after rejected shrinks = read=%d write=%d, want 8/6", read, write)
	}
}

func TestSQLiteStoreListPartitionOverridesExcludesDefault(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	if err := s.EnsureDefaultPartitions(ctx, 4, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPartitions(ctx, "q-a", 6, 4); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPartitions(ctx, "q-b", 8, 8); err != nil {
		t.Fatal(err)
	}

	overrides, err := s.ListPartitionOverrides(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(overrides) != 2 {
		t.Fatalf("overrides = %+v, want 2 rows (no _default_)", overrides)
	}
	got := make(map[string]queue.QueuePartitionConfig, len(overrides))
	for _, o := range overrides {
		if o.QueueName == defaultPartitionScope {
			t.Fatalf("ListPartitionOverrides must not return the default row: %+v", o)
		}
		got[o.QueueName] = o
	}
	if a := got["q-a"]; a.Read != 6 || a.Write != 4 {
		t.Fatalf("q-a override = %+v, want read=6 write=4", a)
	}
	if b := got["q-b"]; b.Read != 8 || b.Write != 8 {
		t.Fatalf("q-b override = %+v, want read=8 write=8", b)
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

// respool 的新行与老行退役同事务：写入后老行不在库里，MinTaskIDAfter 越过它找到新行；写入失败时老行保留。
func TestSQLiteStoreRespoolRetiresOldRowAtomically(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	if err := s.CreateQueue(ctx, &queue.QueueInfo{QueueName: "respool-q", PartitionID: 0, RangeID: 1, NextTaskID: 2}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := s.CreateTasks(ctx, []*queue.TaskRow{{TaskID: 1, QueueName: "respool-q", Data: []byte("task"), CreatedAt: now}}); err != nil {
		t.Fatal(err)
	}
	respooled := []*queue.TaskRow{{TaskID: 2, QueueName: "respool-q", Data: []byte("task"), Attempt: 1, CreatedAt: now, RespoolOf: 1}}

	if _, err := s.CreateTasksAndAdvanceRange(ctx, "respool-q", 0, respooled, 3, 99); !errors.Is(err, queue.ErrConditionFailed) {
		t.Fatalf("fenced write err = %v, want ErrConditionFailed", err)
	}
	if minTaskID, found, err := s.MinTaskIDAfter(ctx, "respool-q", 0, 0); err != nil || !found || minTaskID != 1 {
		t.Fatalf("after fenced write: min=%d found=%v err=%v, want the old row kept", minTaskID, found, err)
	}

	if _, err := s.CreateTasksAndAdvanceRange(ctx, "respool-q", 0, respooled, 3, 1); err != nil {
		t.Fatal(err)
	}
	if minTaskID, found, err := s.MinTaskIDAfter(ctx, "respool-q", 0, 0); err != nil || !found || minTaskID != 2 {
		t.Fatalf("after respool: min=%d found=%v err=%v, want only the new row", minTaskID, found, err)
	}
	if _, found, err := s.MinTaskIDAfter(ctx, "respool-q", 0, 2); err != nil || found {
		t.Fatalf("above the last row: found=%v err=%v", found, err)
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
		BEFORE DELETE ON taskq_tasks
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

// GrowPartitions 是外部调用方（管理端点、启动落配置）的入口：首次写入插入行，
// 之后只允许增大。缩容必须走 matching 的 ResizePartitions 编排。
func TestSQLiteStoreGrowPartitionsIsGrowOnly(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()

	// override 行不存在 → 首次写入直接插入（不依赖 `_default_` 是否 bootstrap）。
	if err := s.GrowPartitions(ctx, "q", 4, 4); err != nil {
		t.Fatal(err)
	}
	read, write, err := s.ResolvePartitions(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if read != 4 || write != 4 {
		t.Fatalf("first write = read=%d write=%d, want 4/4", read, write)
	}

	// 增大成功。
	if err := s.GrowPartitions(ctx, "q", 8, 6); err != nil {
		t.Fatal(err)
	}
	// 相等是幂等 no-op，不报错。
	if err := s.GrowPartitions(ctx, "q", 8, 6); err != nil {
		t.Fatalf("idempotent rewrite rejected: %v", err)
	}

	// 任一维度缩小都被拒，且既有值不变。
	for _, tc := range []struct {
		name        string
		read, write int
	}{
		{"shrink read", 6, 6},
		{"shrink write", 8, 4},
		{"shrink both", 4, 4},
	} {
		err := s.GrowPartitions(ctx, "q", tc.read, tc.write)
		if !errors.Is(err, ErrPartitionShrinkRejected) {
			t.Fatalf("%s: err = %v, want ErrPartitionShrinkRejected", tc.name, err)
		}
	}
	read, write, err = s.ResolvePartitions(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if read != 8 || write != 6 {
		t.Fatalf("after rejected shrinks = read=%d write=%d, want 8/6", read, write)
	}

	// 不变式 1<=write<=read 仍然强校验。
	if err := s.GrowPartitions(ctx, "q", 8, 9); err == nil {
		t.Fatal("expected rejection for write>read")
	}
	// 保留 scope 名不可用作 queue 名。
	if err := s.GrowPartitions(ctx, defaultPartitionScope, 8, 8); err == nil {
		t.Fatal("expected rejection for reserved scope name")
	}
}

// SetPartitions 必须保持无条件语义：matching 的 shrinkPartitions 第 4 步依赖它减小 read。
// 给它加 grow-only 会破坏合法缩容，这里把该契约钉死。
func TestSQLiteStoreSetPartitionsStaysUnconditionalForResize(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()

	if err := s.SetPartitions(ctx, "q", 8, 8); err != nil {
		t.Fatal(err)
	}
	// 编排器第 1 步：只减 write。
	if err := s.SetPartitions(ctx, "q", 8, 2); err != nil {
		t.Fatalf("resize step 1 (shrink write) rejected: %v", err)
	}
	// 编排器第 4 步：drain 确认后减 read。
	if err := s.SetPartitions(ctx, "q", 2, 2); err != nil {
		t.Fatalf("resize step 4 (shrink read) rejected: %v", err)
	}
	read, write, err := s.ResolvePartitions(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if read != 2 || write != 2 {
		t.Fatalf("after orchestrated shrink = read=%d write=%d, want 2/2", read, write)
	}
}

// 并发 GrowPartitions 不得丢更新：单条条件 UPDATE 做 CAS，最终值必须是最大的那个，
// 而不是最后一个写入者的值。
func TestSQLiteStoreGrowPartitionsConcurrentKeepsMax(t *testing.T) {
	s := newSQLiteTestStore(t)
	ctx := context.Background()
	if err := s.GrowPartitions(ctx, "q", 2, 2); err != nil {
		t.Fatal(err)
	}

	targets := []int{4, 16, 8, 32, 6}
	var wg sync.WaitGroup
	for _, n := range targets {
		wg.Go(func() {
			// 缩容请求被拒是预期结果，这里只关心最终值不被回退。
			_ = s.GrowPartitions(ctx, "q", n, n)
		})
	}
	wg.Wait()

	read, write, err := s.ResolvePartitions(ctx, "q")
	if err != nil {
		t.Fatal(err)
	}
	if read != 32 || write != 32 {
		t.Fatalf("concurrent grow settled at read=%d write=%d, want 32/32 (lost update)", read, write)
	}
}
