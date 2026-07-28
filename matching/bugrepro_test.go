package matching

import (
	"context"
	"errors"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/membership"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// 自动分区改造后 queue 按需自动创建：向从未声明的 queue 发 AddTask 不再报 NotFound，
// 而是 fallback 到全局默认分区数、自动物化分区并入队（对比改造前的 ErrUnknownQueue）。
func TestBugRepro_AddTaskAutoCreatesUnknownQueue(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"known-q": 1})

	client := newTestClient(t, addr)
	if _, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName:   "unknown-q",
		PartitionId: 0,
		Data:        []byte("payload"),
	}); err != nil {
		t.Fatalf("expected unknown queue to be auto-created and accepted, got %v", err)
	}
}

func TestBugRepro_AddTaskRejectsOutOfRangePartition(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"known-q": 1})

	client := newTestClient(t, addr)
	_, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName:   "known-q",
		PartitionId: 1,
		Data:        []byte("payload"),
	})
	if err == nil {
		t.Fatal("expected out-of-range write partition to be rejected")
	}
	// AddTask 按 numWrite 硬拒绝高位写分区（读写闸门分裂），返回 FailedPrecondition。
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for out-of-range write partition, got %v", err)
	}
}

type flakyLookupResolver struct {
	stubResolver
	lookupErr error
}

type ownershipFlipResolver struct {
	lookups int
	self    membership.HostInfo
	other   membership.HostInfo
}

func (r *ownershipFlipResolver) Lookup(string) (membership.HostInfo, error) {
	r.lookups++
	if r.lookups == 1 {
		return r.self, nil
	}
	return r.other, nil
}

func (*ownershipFlipResolver) AddListener(string, chan<- *membership.ChangedEvent) error { return nil }
func (*ownershipFlipResolver) RemoveListener(string) error                               { return nil }

func TestBugRepro_SlowPartitionLoadRechecksOwnershipUnderLock(t *testing.T) {
	store := newTestSQLiteStore(t)
	if err := store.EnsureDefaultPartitions(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	}
	resolver := &ownershipFlipResolver{
		self:  membership.NewHostInfoFromAddress("127.0.0.1:9999"),
		other: membership.NewHostInfoFromAddress("127.0.0.2:9999"),
	}
	pm := newPartitionManager(
		context.Background(),
		store,
		store.(dbleader.LockStore),
		"127.0.0.1:9999",
		"owner-a",
		resolver,
		time.Minute,
		defaultMaxAttempts,
	)
	t.Cleanup(pm.close)

	if _, err := pm.getOrLoadPartition(context.Background(), "known-q", 0, accessRead); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("expected ownership change to abort partition load, got %v", err)
	}
	if len(pm.partitions) != 0 {
		t.Fatalf("loaded partitions = %d, want 0", len(pm.partitions))
	}
}

func (r *flakyLookupResolver) Lookup(key string) (membership.HostInfo, error) {
	if r.lookupErr != nil {
		return nil, r.lookupErr
	}
	return r.stubResolver.Lookup(key)
}

func TestBugRepro_ReconcileKeepsLoadedPartitionsWhenOwnershipLookupFails(t *testing.T) {
	store := newTestSQLiteStore(t)
	if err := store.EnsureDefaultPartitions(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	}
	resolver := &flakyLookupResolver{stubResolver: *newStubResolver("127.0.0.1:9999")}
	pm := newPartitionManager(
		context.Background(),
		store,
		store.(dbleader.LockStore),
		"127.0.0.1:9999",
		"owner-a",
		resolver,
		time.Minute,
		defaultMaxAttempts,
	)
	t.Cleanup(pm.close)

	pq, err := pm.getOrLoadPartition(context.Background(), "known-q", 0, accessRead)
	if err != nil {
		t.Fatalf("expected partition to load before reproducing reconcile issue: %v", err)
	}

	resolver.lookupErr = errors.New("membership lookup failed")
	pm.reconcile()

	if !pq.isActive() {
		t.Fatal("expected transient membership lookup failure to keep loaded partition alive")
	}
	if _, ok := pm.partitions[partitionKey("known-q", 0)]; !ok {
		t.Fatal("expected loaded partition to remain registered after transient lookup failure")
	}
}

func TestBugRepro_LoadedPartitionRechecksOwnership(t *testing.T) {
	store := newTestSQLiteStore(t)
	if err := store.EnsureDefaultPartitions(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	}
	resolver := newStubResolver("127.0.0.1:9999")
	pm := newPartitionManager(
		context.Background(),
		store,
		store.(dbleader.LockStore),
		"127.0.0.1:9999",
		"owner-a",
		resolver,
		time.Minute,
		defaultMaxAttempts,
	)
	t.Cleanup(pm.close)

	handle, err := pm.getOrLoadPartition(context.Background(), "known-q", 0, accessRead)
	if err != nil {
		t.Fatal(err)
	}
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress("127.0.0.2:9999")}

	if _, err := pm.getOrLoadPartition(context.Background(), "known-q", 0, accessRead); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("expected loaded partition to reject stale ownership, got %v", err)
	}
	if !handle.isClosed() {
		t.Fatal("expected stale partition handle to close")
	}
}

func TestBugRepro_PeriodicReconcileConvergesAfterDroppedEvent(t *testing.T) {
	store := newTestSQLiteStore(t)
	if err := store.EnsureDefaultPartitions(context.Background(), 1, 1); err != nil {
		t.Fatal(err)
	}
	resolver := newStubResolver("127.0.0.1:9999")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pm := newPartitionManager(
		ctx,
		store,
		store.(dbleader.LockStore),
		"127.0.0.1:9999",
		"owner-a",
		resolver,
		time.Minute,
		defaultMaxAttempts,
	)
	t.Cleanup(pm.close)

	handle, err := pm.getOrLoadPartition(context.Background(), "known-q", 0, accessRead)
	if err != nil {
		t.Fatal(err)
	}
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress("127.0.0.2:9999")}

	server := &Server{
		cfg:      Config{ReconcileInterval: 10 * time.Millisecond},
		mgr:      pm,
		changeCh: make(chan *membership.ChangedEvent),
		ctx:      ctx,
	}
	done := make(chan struct{})
	go func() {
		server.watchMembershipChanges()
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if handle.isClosed() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("periodic reconcile did not unload the stale partition")
}
