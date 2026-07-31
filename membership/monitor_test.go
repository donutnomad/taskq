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

// flakyStore 是可在运行时切换故障的 stubStore：用于验证 DB 抖动期间 Monitor 不停机。
// 故意复用内嵌 stubStore 的 mu（不另开锁），使 members/upserts 与 failing/getCalls
// 处于同一把锁下，避免测试自身产生数据竞争。
type flakyStore struct {
	stubStore

	failing  bool
	getCalls int
}

func (s *flakyStore) setFailing(failing bool) {
	s.mu.Lock()
	s.failing = failing
	s.mu.Unlock()
}

func (s *flakyStore) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCalls
}

// isFailing 读故障标志。注意不能在持锁期间调用 stubStore 的方法——它们会再次取同一把锁。
func (s *flakyStore) isFailing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failing
}

func (s *flakyStore) GetActiveMembers(ctx context.Context, within time.Duration) ([]MemberRecord, error) {
	s.mu.Lock()
	s.getCalls++
	failing := s.failing
	s.mu.Unlock()
	if failing {
		return nil, errors.New("db unavailable")
	}
	return s.stubStore.GetActiveMembers(ctx, within)
}

func (s *flakyStore) UpsertMember(ctx context.Context, req UpsertMemberRequest) error {
	if s.isFailing() {
		return errors.New("db unavailable")
	}
	return s.stubStore.UpsertMember(ctx, req)
}

// DB 抖动持续到超出重试预算后，Monitor 必须保留上一次的 ring 快照继续服务，
// 而不是停机把 ring 清空让所有 Lookup 返回 ErrInsufficientHosts；DB 恢复后自愈。
func TestMonitorServesStaleRingWhilePollFails(t *testing.T) {
	store := &flakyStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	// 压缩重试参数，让「重试预算耗尽」在测试时限内发生。
	restore := shrinkRetryParamsForTest(t)
	defer restore()

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		20*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}
	resolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}

	// 进入故障态，等待若干个 poll 周期把重试预算耗尽。
	store.setFailing(true)
	before := store.calls()
	deadline := time.Now().Add(2 * time.Second)
	for store.calls() < before+8 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	// 关键断言：ring 仍然可用，Lookup 不返回 ErrInsufficientHosts。
	host, err := resolver.Lookup("workflow-tasks/0")
	if err != nil {
		t.Fatalf("lookup during db outage = %v, want stale ring to keep serving", err)
	}
	if host.GetAddress() != "10.0.0.1:7235" {
		t.Fatalf("stale host = %q, want 10.0.0.1:7235", host.GetAddress())
	}

	// DB 恢复后必须自愈：新成员能被后续 poll 同步进来。
	store.mu.Lock()
	store.members = append(store.members, MemberRecord{
		RPCAddress: net.ParseIP("10.0.0.2"), RPCPort: 7235, ServiceName: "matching",
	})
	store.mu.Unlock()
	store.setFailing(false)

	observable := resolver.(interface{ MemberCount() int })
	deadline = time.Now().Add(2 * time.Second)
	for observable.MemberCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if observable.MemberCount() != 2 {
		t.Fatalf("member count after recovery = %d, want 2 (monitor did not self-heal)", observable.MemberCount())
	}
}

// 心跳写失败同样不得停机：写不进去只影响别人看见我，本节点对其它节点的路由视图必须完好，
// 且 DB 恢复后心跳自动继续。
func TestMonitorSurvivesHeartbeatFailure(t *testing.T) {
	store := &flakyStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.9"), RPCPort: 7235, ServiceName: "matching"},
	}

	restore := shrinkRetryParamsForTest(t)
	defer restore()

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		20*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}

	store.setFailing(true)
	before := store.calls()
	deadline := time.Now().Add(2 * time.Second)
	for store.calls() < before+8 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	store.setFailing(false)

	// Monitor 未停机：lifecycle 仍存活，poll 循环仍在推进。
	if err := m.WaitUntilInitialized(context.Background()); err != nil {
		t.Fatalf("monitor stopped after heartbeat failures: %v", err)
	}
	resolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Lookup("workflow-tasks/0"); err != nil {
		t.Fatalf("lookup after heartbeat outage = %v, want ring intact", err)
	}

	// 恢复后心跳继续写入。
	store.mu.Lock()
	store.upserts = nil
	store.mu.Unlock()
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		n := len(store.upserts)
		store.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("heartbeats did not resume after db recovery")
}

// shrinkRetryParamsForTest 压缩重试/心跳参数，让故障路径在测试时限内触发。
func shrinkRetryParamsForTest(t *testing.T) func() {
	t.Helper()
	prevAttempts, prevBase, prevMax := membershipRetryMaxAttempts, membershipRetryBaseDelay, membershipRetryMaxDelay
	prevInterval, prevJitter := membershipHeartbeatInterval, membershipHeartbeatJitterMax
	membershipRetryMaxAttempts = 2
	membershipRetryBaseDelay = time.Millisecond
	membershipRetryMaxDelay = 2 * time.Millisecond
	membershipHeartbeatInterval = 20 * time.Millisecond
	membershipHeartbeatJitterMax = 0
	return func() {
		membershipRetryMaxAttempts, membershipRetryBaseDelay, membershipRetryMaxDelay = prevAttempts, prevBase, prevMax
		membershipHeartbeatInterval, membershipHeartbeatJitterMax = prevInterval, prevJitter
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
