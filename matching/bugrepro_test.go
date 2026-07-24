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

func TestBugRepro_AddTaskRejectsUnknownQueue(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"known-q": 1})

	client := newTestClient(t, addr)
	_, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName:   "unknown-q",
		PartitionId: 0,
		Data:        []byte("payload"),
	})
	if err == nil {
		t.Fatal("expected unknown queue to be rejected")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound for unknown queue, got %v", err)
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
		t.Fatal("expected out-of-range partition to be rejected")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for out-of-range partition, got %v", err)
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
		map[string]int{"known-q": 1},
	)
	t.Cleanup(pm.close)

	if _, err := pm.getOrLoadPartition(context.Background(), "known-q", 0); !errors.Is(err, ErrNotOwner) {
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
		map[string]int{"known-q": 1},
	)
	t.Cleanup(pm.close)

	pq, err := pm.getOrLoadPartition(context.Background(), "known-q", 0)
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
		map[string]int{"known-q": 1},
	)
	t.Cleanup(pm.close)

	handle, err := pm.getOrLoadPartition(context.Background(), "known-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress("127.0.0.2:9999")}

	if _, err := pm.getOrLoadPartition(context.Background(), "known-q", 0); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("expected loaded partition to reject stale ownership, got %v", err)
	}
	if !handle.isClosed() {
		t.Fatal("expected stale partition handle to close")
	}
}

func TestBugRepro_PeriodicReconcileConvergesAfterDroppedEvent(t *testing.T) {
	store := newTestSQLiteStore(t)
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
		map[string]int{"known-q": 1},
	)
	t.Cleanup(pm.close)

	handle, err := pm.getOrLoadPartition(context.Background(), "known-q", 0)
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
