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

func TestBugRepro_EvictSelfAtDoesNotDrainImmediatelyForFutureDeadline(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		3*time.Second,
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

	waitDuration, err := m.EvictSelfAt(time.Now().Add(20 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if waitDuration <= 15*time.Second {
		t.Fatalf("expected EvictSelfAt to preserve a future deadline, got %v", waitDuration)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.upserts) == 0 {
		t.Fatal("expected at least one upsert")
	}
	last := store.upserts[len(store.upserts)-1]
	if last.Draining {
		t.Fatal("expected node to keep serving until the scheduled eviction time")
	}
}

func TestBugRepro_GetReachableMembersDeduplicatesAddresses(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "history"},
	}

	m := NewMonitor(
		[]string{"matching", "history"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)

	reachable := m.(interface {
		GetReachableMembers() ([]string, error)
	})
	members, err := reachable.GetReachableMembers()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != "10.0.0.1:7235" {
		t.Fatalf("expected one unique reachable member, got %v", members)
	}
}

func TestBugRepro_LookupShouldSkipDrainingHosts(t *testing.T) {
	resolver := newServiceResolver("matching", 200)
	resolver.setMembers([]*hostInfo{
		newHostInfo("10.0.0.1:7235", false, 0, 0),
		newHostInfo("10.0.0.2:7235", true, 0, 0),
	})

	for i := range 1000 {
		host, err := resolver.Lookup(fmt.Sprintf("key-%d", i))
		if err != nil {
			t.Fatalf("unexpected lookup error: %v", err)
		}
		if hi, ok := host.(*hostInfo); ok && hi.draining {
			t.Fatalf("expected lookup to avoid draining hosts, got %s for key-%d", hi.GetAddress(), i)
		}
	}
}

type retryingMemberStore struct {
	stubStore

	mu           sync.Mutex
	upsertErrors []error
	getErrors    []error
	upsertCalls  int
	getCalls     int
}

func (s *retryingMemberStore) UpsertMember(ctx context.Context, req UpsertMemberRequest) error {
	s.mu.Lock()
	s.upsertCalls++
	var err error
	if len(s.upsertErrors) > 0 {
		err = s.upsertErrors[0]
		s.upsertErrors = s.upsertErrors[1:]
	}
	s.mu.Unlock()

	if err != nil {
		return err
	}
	return s.stubStore.UpsertMember(ctx, req)
}

func (s *retryingMemberStore) GetActiveMembers(ctx context.Context, within time.Duration) ([]MemberRecord, error) {
	s.mu.Lock()
	s.getCalls++
	var err error
	if len(s.getErrors) > 0 {
		err = s.getErrors[0]
		s.getErrors = s.getErrors[1:]
	}
	s.mu.Unlock()

	if err != nil {
		return nil, err
	}
	return s.stubStore.GetActiveMembers(ctx, within)
}

func setMonitorRetryPolicyForTest(t *testing.T, attempts int, baseDelay, maxDelay, heartbeatInterval, heartbeatJitter time.Duration) {
	t.Helper()

	prevAttempts := membershipRetryMaxAttempts
	prevBaseDelay := membershipRetryBaseDelay
	prevMaxDelay := membershipRetryMaxDelay
	prevHeartbeatInterval := membershipHeartbeatInterval
	prevHeartbeatJitter := membershipHeartbeatJitterMax

	membershipRetryMaxAttempts = attempts
	membershipRetryBaseDelay = baseDelay
	membershipRetryMaxDelay = maxDelay
	membershipHeartbeatInterval = heartbeatInterval
	membershipHeartbeatJitterMax = heartbeatJitter

	t.Cleanup(func() {
		membershipRetryMaxAttempts = prevAttempts
		membershipRetryBaseDelay = prevBaseDelay
		membershipRetryMaxDelay = prevMaxDelay
		membershipHeartbeatInterval = prevHeartbeatInterval
		membershipHeartbeatJitterMax = prevHeartbeatJitter
	})
}

func TestBugRepro_StartShouldRetryTransientMembershipStoreFailures(t *testing.T) {
	setMonitorRetryPolicyForTest(t, 3, 5*time.Millisecond, 20*time.Millisecond, time.Hour, 0)

	store := &retryingMemberStore{
		upsertErrors: []error{errors.New("upsert failed"), errors.New("upsert failed")},
	}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		time.Hour,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatalf("expected monitor to recover from transient startup failures, got %v", err)
	}

	store.mu.Lock()
	upsertCalls := store.upsertCalls
	store.mu.Unlock()
	if upsertCalls < 3 {
		t.Fatalf("expected startup to retry failed writes, got %d upsert attempts", upsertCalls)
	}
}

// 运行期 sync 失败耗尽重试后，Monitor 必须继续存活并沿用上一次 ring 快照。
//
// 本用例此前断言的是相反行为（失败即停机）。停机的代价与收益不对等：DB 抖动约 8 秒
// 就让节点所有 Lookup 永久返回 ErrInsufficientHosts，且 Start() 拒绝重启（status != 0），
// 无自愈路径。读不到成员表只意味着「看不见别人」，上一次快照仍是真实存在过的成员视图，
// 继续用它是 matching resolvePartitions 已经确立的 stale-while-error 策略。
// 启动期失败仍然 fatal（见 TestBugRepro_StartShouldRetryTransientMembershipStoreFailures），
// 因为那时没有任何可用快照可退化。
func TestBugRepro_RuntimeSyncFailuresKeepMonitorAlive(t *testing.T) {
	setMonitorRetryPolicyForTest(t, 3, 5*time.Millisecond, 20*time.Millisecond, time.Hour, 0)

	store := &retryingMemberStore{
		getErrors: []error{
			nil,
			errors.New("sync failed"),
			errors.New("sync failed"),
			errors.New("sync failed"),
		},
	}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		10*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatalf("expected monitor to initialize before runtime sync failure test, got %v", err)
	}

	// 等注入的错误全部被消费掉（重试预算已耗尽至少一轮）。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		remaining := len(store.getErrors)
		store.mu.Unlock()
		if remaining == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if m.(*monitor).lifecycleCtx.Err() != nil {
		t.Fatal("monitor stopped after runtime sync failures; expected it to serve the stale ring")
	}
	resolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Lookup("workflow-tasks/0"); err != nil {
		t.Fatalf("lookup after sync failures = %v, want stale ring to keep serving", err)
	}
}

// 运行期心跳写失败同样不得停机：写不进去只影响别人看见我，他们会按心跳超时把流量切走；
// 自杀反而连带毁掉本节点对其它节点的路由视图。
func TestBugRepro_RuntimeHeartbeatFailuresKeepMonitorAlive(t *testing.T) {
	setMonitorRetryPolicyForTest(t, 3, 5*time.Millisecond, 20*time.Millisecond, 10*time.Millisecond, 0)

	store := &retryingMemberStore{
		upsertErrors: []error{
			nil,
			errors.New("heartbeat failed"),
			errors.New("heartbeat failed"),
			errors.New("heartbeat failed"),
		},
	}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		[]string{"matching"},
		store,
		netip.MustParseAddrPort("10.0.0.1:7235"),
		time.Hour,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatalf("expected monitor to initialize before runtime heartbeat failure test, got %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		remaining := len(store.upsertErrors)
		store.mu.Unlock()
		if remaining == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if m.(*monitor).lifecycleCtx.Err() != nil {
		t.Fatal("monitor stopped after heartbeat failures; expected it to keep serving and retry")
	}
	resolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Lookup("workflow-tasks/0"); err != nil {
		t.Fatalf("lookup after heartbeat failures = %v, want ring intact", err)
	}
}
