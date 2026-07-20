package membership

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// stubStore 是内存版 MemberStore，用于测试
type stubStore struct {
	mu      sync.Mutex
	members []MemberRecord
	upserts []UpsertMemberRequest
}

type upsertErrorStore struct {
	stubStore
	err error
}

func (s *upsertErrorStore) UpsertMember(context.Context, UpsertMemberRequest) error {
	return s.err
}

func (s *stubStore) UpsertMember(_ context.Context, req UpsertMemberRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, req)
	for i, m := range s.members {
		if m.RPCAddress.Equal(req.RPCAddress) && m.RPCPort == req.RPCPort && m.ServiceName == req.ServiceName {
			s.members[i] = MemberRecord{
				RPCAddress:  req.RPCAddress,
				RPCPort:     req.RPCPort,
				ServiceName: req.ServiceName,
				Draining:    req.Draining,
				StartAt:     req.StartAt,
				StopAt:      req.StopAt,
			}
			return nil
		}
	}
	s.members = append(s.members, MemberRecord{
		RPCAddress:  req.RPCAddress,
		RPCPort:     req.RPCPort,
		ServiceName: req.ServiceName,
		Draining:    req.Draining,
		StartAt:     req.StartAt,
		StopAt:      req.StopAt,
	})
	return nil
}

func (s *stubStore) GetActiveMembers(_ context.Context, _ time.Duration) ([]MemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]MemberRecord, len(s.members))
	copy(result, s.members)
	return result, nil
}

func (s *stubStore) PruneExpiredMembers(_ context.Context, _ int) error { return nil }

func TestMonitorStartStop(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("127.0.0.1"), RPCPort: 7233, ServiceName: "matching"},
	}

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("127.0.0.1:7233"),
		5*time.Second,
		3*time.Second,
		time.Time{},
		200,
	)
	m.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}

	reachable := m.(interface {
		GetReachableMembers() ([]string, error)
	})
	members, err := reachable.GetReachableMembers()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) == 0 {
		t.Fatal("expected at least 1 member")
	}

	m.Stop()
}

func TestMonitorConcurrentStopIsIdempotent(t *testing.T) {
	store := &stubStore{}
	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("127.0.0.1:7233"),
		50*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Go(m.Stop)
	wg.Go(m.Stop)
	wg.Wait()
	m.Stop()
}

func TestMonitorGetResolver(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
		{RPCAddress: net.ParseIP("10.0.0.2"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	resolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	host, err := resolver.Lookup("some-key")
	if err != nil {
		t.Fatal(err)
	}
	if host.GetAddress() == "" {
		t.Fatal("empty address")
	}

	_, err = m.GetResolver("unknown-service")
	if err != ErrUnknownService {
		t.Fatalf("expected ErrUnknownService, got %v", err)
	}

	m.Stop()
}

func TestMonitorSetDraining(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	if err := m.SetDraining(true); err != nil {
		t.Fatal(err)
	}

	// 等待下次轮询同步
	time.Sleep(300 * time.Millisecond)

	resolver, _ := m.GetResolver("matching")
	observable := resolver.(interface {
		AvailableMemberCount() int
	})
	if observable.AvailableMemberCount() != 0 {
		t.Fatalf("expected 0 available after draining, got %d", observable.AvailableMemberCount())
	}

	m.Stop()
}

func TestEvictSelfAtReturnValue(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	propagationTime := 3 * time.Second
	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		propagationTime,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)
	defer m.Stop()

	// EvictSelfAt 5 秒后离开，propagationTime=3s
	// 期望返回值约为 until + propagationTime = 5s + 3s = 8s
	// Bug: 返回值是 until + 2*propagationTime = 5s + 6s = 11s
	evictAt := time.Now().Add(5 * time.Second)
	waitDuration, err := m.EvictSelfAt(evictAt)
	if err != nil {
		t.Fatal(err)
	}

	maxExpected := 5*time.Second + propagationTime + 500*time.Millisecond
	if waitDuration > maxExpected {
		t.Fatalf("EvictSelfAt returned %v, expected at most ~%v (double-counting propagationTime)", waitDuration, maxExpected)
	}
	store.mu.Lock()
	last := store.upserts[len(store.upserts)-1]
	store.mu.Unlock()
	if last.StopAt != evictAt.Unix() {
		t.Fatalf("stop time = %d, want %d", last.StopAt, evictAt.Unix())
	}
	if last.Draining {
		t.Fatal("future eviction marked the member draining immediately")
	}
	if err := m.SetDraining(false); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	last = store.upserts[len(store.upserts)-1]
	store.mu.Unlock()
	if last.StopAt != 0 {
		t.Fatalf("stop time after resume = %d, want 0", last.StopAt)
	}
}

func TestEvictSelfAtPastDeadlineDrainsImmediately(t *testing.T) {
	store := &stubStore{}
	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	waitDuration, err := m.EvictSelfAt(time.Now().Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if waitDuration != time.Second {
		t.Fatalf("wait duration = %v, want %v", waitDuration, time.Second)
	}
	store.mu.Lock()
	last := store.upserts[len(store.upserts)-1]
	store.mu.Unlock()
	if !last.Draining {
		t.Fatal("past eviction deadline did not persist draining state")
	}
}

func TestEvictSelfAtReturnsScheduledHeartbeatFailure(t *testing.T) {
	upsertErr := errors.New("upsert failed")
	store := &upsertErrorStore{err: upsertErr}
	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)

	if _, err := m.EvictSelfAt(time.Now().Add(time.Hour)); !errors.Is(err, upsertErr) {
		t.Fatalf("eviction error = %v, want %v", err, upsertErr)
	}
}

func TestMonitorHeartbeat(t *testing.T) {
	store := &stubStore{}

	m := NewMonitor(
		[]string{"history"},
		store,
		netip.MustParseAddrPort("10.0.0.5:7234"),
		50*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	time.Sleep(200 * time.Millisecond)
	store.mu.Lock()
	upsertCount := len(store.upserts)
	store.mu.Unlock()
	if upsertCount == 0 {
		t.Fatal("expected at least 1 heartbeat upsert")
	}

	m.Stop()
	fmt.Printf("heartbeat count: %d\n", upsertCount)
}

func TestMonitorMultiServiceNames(t *testing.T) {
	store := &stubStore{}

	m := NewMonitor(
		[]string{"matching", "history"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	// 验证心跳为两个 serviceName 各写了记录
	store.mu.Lock()
	serviceSet := make(map[string]bool)
	for _, req := range store.upserts {
		serviceSet[req.ServiceName] = true
	}
	store.mu.Unlock()

	if !serviceSet["matching"] {
		t.Fatal("expected heartbeat for matching")
	}
	if !serviceSet["history"] {
		t.Fatal("expected heartbeat for history")
	}

	// 等待同步，使 resolver 能 Lookup 到本节点
	time.Sleep(300 * time.Millisecond)

	// 验证两个 resolver 都能 Lookup 到本节点
	matchingResolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	host, err := matchingResolver.Lookup("some-key")
	if err != nil {
		t.Fatal(err)
	}
	if host.GetAddress() != "10.0.0.1:7235" {
		t.Fatalf("expected 10.0.0.1:7235, got %s", host.GetAddress())
	}

	historyResolver, err := m.GetResolver("history")
	if err != nil {
		t.Fatal(err)
	}
	host2, err := historyResolver.Lookup("another-key")
	if err != nil {
		t.Fatal(err)
	}
	if host2.GetAddress() != "10.0.0.1:7235" {
		t.Fatalf("expected 10.0.0.1:7235, got %s", host2.GetAddress())
	}

	m.Stop()
}

func TestMonitorObserverWatchesServiceWithoutAdvertisingIt(t *testing.T) {
	store := &stubStore{members: []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}}
	monitor := NewMonitorWithConfig(MonitorConfig{
		WatchedServices: []string{"matching"},
		Store:           store,
		PollInterval:    50 * time.Millisecond,
		PropagationTime: time.Second,
		ReplicaPoints:   200,
	})
	monitor.Start()
	defer monitor.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := monitor.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	resolver, err := monitor.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	host, err := resolver.Lookup("workflow-tasks/0")
	if err != nil {
		t.Fatal(err)
	}
	if host.GetAddress() != "10.0.0.1:7235" {
		t.Fatalf("matching host = %q", host.GetAddress())
	}

	store.mu.Lock()
	upsertCount := len(store.upserts)
	store.mu.Unlock()
	if upsertCount != 0 {
		t.Fatalf("observer heartbeat upserts = %d, want 0", upsertCount)
	}
}

func TestMonitorCanAdvertiseAndWatchSameService(t *testing.T) {
	store := &stubStore{}
	monitor := NewMonitorWithConfig(MonitorConfig{
		AdvertisedServices: []string{"matching"},
		WatchedServices:    []string{"matching"},
		Store:              store,
		BroadcastAddr:      netip.MustParseAddrPort("10.0.0.1:7235"),
		PollInterval:       50 * time.Millisecond,
		PropagationTime:    time.Second,
		ReplicaPoints:      200,
	})
	monitor.Start()
	defer monitor.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := monitor.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	resolver, err := monitor.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	host, err := resolver.Lookup("workflow-tasks/0")
	if err != nil {
		t.Fatal(err)
	}
	if host.GetAddress() != "10.0.0.1:7235" {
		t.Fatalf("matching host = %q", host.GetAddress())
	}

	store.mu.Lock()
	upserts := append([]UpsertMemberRequest(nil), store.upserts...)
	store.mu.Unlock()
	if len(upserts) != 1 || upserts[0].ServiceName != "matching" {
		t.Fatalf("heartbeat upserts = %+v", upserts)
	}
}

func TestMonitorBuildUpsertRequestPreservesFutureJoinTime(t *testing.T) {
	store := &stubStore{}
	joinAt := time.Now().Add(20 * time.Second).Truncate(time.Second)

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		joinAt,
		200,
	).(*monitor)

	req := m.buildUpsertRequest("matching")
	if req.StartAt != joinAt.Unix() {
		t.Fatalf("expected future join time %d to be preserved, got %d", joinAt.Unix(), req.StartAt)
	}
}

func TestMonitorFiltersMembersOutsideSchedulingWindow(t *testing.T) {
	now := time.Now()
	store := &stubStore{members: []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
		{RPCAddress: net.ParseIP("10.0.0.2"), RPCPort: 7235, ServiceName: "matching", StartAt: now.Add(time.Hour).Unix()},
		{RPCAddress: net.ParseIP("10.0.0.3"), RPCPort: 7235, ServiceName: "matching", StopAt: now.Add(-time.Hour).Unix()},
	}}
	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	resolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	observable := resolver.(interface{ MemberCount() int })
	if observable.MemberCount() != 1 {
		t.Fatalf("member count = %d, want 1 active member", observable.MemberCount())
	}
}
