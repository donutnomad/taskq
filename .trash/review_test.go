package ringpop

import (
	"context"
	"math"
	"math/rand"
	"net"
	"testing"
	"time"
)

// ===== Issue 1: serviceResolver.Stop() double-close panics =====
// serviceResolver.Stop() calls close(r.shutdownCh) unconditionally.
// Calling Stop() twice will panic with "close of closed channel".
func TestServiceResolverDoubleStopPanics(t *testing.T) {
	r := newServiceResolver("test", 100)
	r.Start()

	// First stop - should be fine
	r.Stop()

	// Second stop - should NOT panic, but currently does
	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("ISSUE: serviceResolver.Stop() panics on double call: %v", rec)
		}
	}()
	r.Stop()
}

// ===== Issue 2: EvictSelfAt double-adds propagationTime =====
// The code does:
//
//	leaveAfter := until + m.propagationTime
//	return leaveAfter + m.propagationTime, nil
//
// This returns until + 2*propagationTime instead of until + propagationTime
func TestEvictSelfAtReturnValue(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}
	propagationTime := 3 * time.Second

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
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

	// Schedule eviction 5 seconds from now (under maxScheduledEventTimeSeconds=15)
	evictAt := time.Now().Add(5 * time.Second)
	leaveAfter, err := m.EvictSelfAt(evictAt)
	if err != nil {
		t.Fatal(err)
	}

	// Expected: leaveAfter should be approximately until + propagationTime = 5s + 3s = 8s
	// Actual (bug): leaveAfter = until + 2*propagationTime = 5s + 6s = 11s
	expectedMax := 5*time.Second + propagationTime + 500*time.Millisecond // with some tolerance
	if leaveAfter > expectedMax {
		t.Fatalf("ISSUE: EvictSelfAt returned %v, expected at most ~%v (double-counting propagationTime)", leaveAfter, expectedMax)
	}
}

// ===== Issue 3: heartbeatLoop jitter is nanoseconds, not seconds =====
// The code does: jitter := time.Duration(math.Round(rand.Float64() * 5))
// This produces 0-5 nanoseconds. Then (10 + jitter) * time.Second becomes
// (10ns + 0~5ns) * 1e9 = ~10 seconds with no meaningful jitter.
//
// Actually, wait - let's trace this more carefully:
// jitter = time.Duration(math.Round(rand.Float64() * 5)) => time.Duration(0..5) => 0..5 nanoseconds
// (10 + jitter) * time.Second => (10 + Duration(0..5ns)) is a type error...
// Actually 10 is untyped int, so: time.Duration(10) + jitter = Duration(10ns + 0..5ns) = 10..15 ns
// Then * time.Second = (10..15 ns) * time.Second(1e9) = 10..15 seconds
// So the "jitter" is 0 to 5 SECONDS but by accident through overflow math!
//
// Let's verify this numerically:
func TestHeartbeatJitterCalculation(t *testing.T) {
	// Reproduce the exact calculation from heartbeatLoop
	// jitter := time.Duration(math.Round(rand.Float64() * 5))
	// sleep := (10 + jitter) * time.Second

	// Test with the max jitter value (rand.Float64() returns just under 1.0)
	maxRand := 0.9999
	jitter := time.Duration(math.Round(maxRand * 5)) // = time.Duration(5) = 5 nanoseconds
	sleep := (10 + jitter) * time.Second

	// Because Go's Duration arithmetic: (10 + 5ns) = 15ns, then 15ns * 1e9 = 15 seconds
	// This is 15 seconds, which accidentally works! But the semantics are wrong.
	// The minimum sleep is: time.Duration(10) * time.Second = 10 nanoseconds * 1e9 = 10 seconds
	// The maximum sleep is: time.Duration(15) * time.Second = 15 nanoseconds * 1e9 = 15 seconds
	//
	// So the range [10s, 15s] is accidentally correct due to Duration multiplication!
	// The "10" literal is interpreted as time.Duration(10) = 10ns, not 10 seconds.
	// But (10ns + 5ns) * time.Second = 15ns * 1e9 = 15 seconds. It works by coincidence.

	if sleep < 10*time.Second || sleep > 15*time.Second {
		t.Fatalf("unexpected sleep: %v (expected 10-15s)", sleep)
	}

	// However, the jitter granularity is only integer seconds (0,1,2,3,4,5)
	// because math.Round rounds to integers. This means only 6 possible values.
	// A proper implementation would use fractional seconds for better distribution.
	uniqueValues := map[time.Duration]bool{}
	for range 10000 {
		j := time.Duration(math.Round(rand.Float64() * 5))
		s := (10 + j) * time.Second
		uniqueValues[s] = true
	}
	if len(uniqueValues) <= 6 {
		t.Logf("NOTE: heartbeat jitter has only %d distinct values (0-5 integer seconds). Consider using sub-second granularity for better jitter distribution.", len(uniqueValues))
	}
}

// ===== Issue 4: Monitor double-Stop via serviceResolver panic =====
// When Monitor.Stop() is called, it calls ring.Stop() for each resolver.
// If Monitor.Stop() is called twice, it returns early due to status check.
// But if two goroutines race on Stop(), the mu.Lock prevents double-entry.
// Let's verify Monitor.Stop() is safe to call twice.
func TestMonitorDoubleStop(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	// Monitor.Stop checks status, so second call returns early.
	// But the first call triggers serviceResolver.Stop() which closes shutdownCh.
	// This should not panic since Monitor guards with status check.
	m.Stop()

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("ISSUE: Monitor.Stop() panics on double call: %v", rec)
		}
	}()
	m.Stop()
}

// ===== Issue 5: Concurrent setMembers and Lookup race =====
// setMembers is called from syncMembers under m.mu, but Lookup reads
// ringAndHosts via atomic.Value. There's no explicit race between these
// since atomic.Value provides safe concurrent access. Let's verify with -race.
func TestServiceResolverConcurrentAccess(t *testing.T) {
	r := newServiceResolver("test", 100)
	r.Start()
	defer r.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 100 {
			hosts := []*hostInfo{
				makeHost("10.0.0.1:7235", false),
			}
			if i%2 == 0 {
				hosts = append(hosts, makeHost("10.0.0.2:7235", false))
			}
			r.setMembers(hosts)
		}
	}()

	for range 100 {
		_, _ = r.Lookup("some-key")
		_ = r.MemberCount()
		_ = r.AvailableMemberCount()
	}
	<-done
}

// ===== Issue 6: setMembers with nil should clear members and emit removal events =====
func TestServiceResolverSetMembersNilClearsAndEmits(t *testing.T) {
	r := newServiceResolver("test", 100)
	r.Start()
	defer r.Stop()

	ch := make(chan *ChangedEvent, 5)
	_ = r.AddListener("test", ch)

	// Add members
	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", false),
	})

	// Drain the add event
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for add event")
	}

	if r.MemberCount() != 2 {
		t.Fatalf("expected 2 members, got %d", r.MemberCount())
	}

	// Set nil - should clear and emit removal event
	r.setMembers(nil)

	if r.MemberCount() != 0 {
		t.Fatalf("expected 0 members after nil setMembers, got %d", r.MemberCount())
	}

	select {
	case ev := <-ch:
		if len(ev.HostsRemoved) != 2 {
			t.Fatalf("expected 2 removed in event, got %d", len(ev.HostsRemoved))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for removal event after nil setMembers")
	}
}

// ===== Issue 7: Monitor GetReachableMembers variable shadowing =====
// In the original plan, GetReachableMembers has:
//
//	for _, m := range members { ... }
//
// where m shadows the receiver. The actual code uses 'rec' which is correct.
// Let's verify it works properly.
func TestMonitorGetReachableMembersNoShadowing(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
		{RPCAddress: net.ParseIP("10.0.0.2"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)
	defer m.Stop()

	members, err := m.GetReachableMembers()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("expected 2 reachable members, got %d", len(members))
	}
}

// ===== Issue 8: EvictSelfAt boundary - well above maxScheduledEventTimeSeconds =====
// The condition is: until.Seconds() >= maxScheduledEventTimeSeconds (15)
// When clearly above 15 seconds, it bypasses the timer and calls SetDraining immediately.
// When clearly below 15 seconds, it uses the timer (and has the double-propagationTime bug).
func TestEvictSelfAtBoundary(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)
	defer m.Stop()

	// Well above 15s - triggers immediate drain with leaveAfter=0
	leaveAfter, err := m.EvictSelfAt(time.Now().Add(30 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if leaveAfter != 0 {
		t.Fatalf("expected 0 for above-boundary case, got %v", leaveAfter)
	}
}

// ===== Issue 9: EvictSelfAt with negative time (past time) =====
func TestEvictSelfAtPastTime(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)
	defer m.Stop()

	// Past time - until <= 0, should drain immediately
	leaveAfter, err := m.EvictSelfAt(time.Now().Add(-1 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if leaveAfter != 0 {
		t.Fatalf("expected 0 for past time, got %v", leaveAfter)
	}
}

// ===== Issue 10: syncMembers startAt/stopAt filtering =====
func TestSyncMembersFilteringStartAt(t *testing.T) {
	futureUnix := time.Now().Add(1 * time.Hour).Unix()
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching", StartAt: 0},          // active
		{RPCAddress: net.ParseIP("10.0.0.2"), RPCPort: 7235, ServiceName: "matching", StartAt: futureUnix}, // not yet active
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)
	defer m.Stop()

	resolver, _ := m.GetResolver("matching")
	// Only the first member (startAt=0) should be visible
	if resolver.MemberCount() != 1 {
		t.Fatalf("expected 1 member (future startAt filtered), got %d", resolver.MemberCount())
	}
}

func TestSyncMembersFilteringStopAt(t *testing.T) {
	pastUnix := time.Now().Add(-1 * time.Hour).Unix()
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching", StopAt: 0},        // no stop
		{RPCAddress: net.ParseIP("10.0.0.2"), RPCPort: 7235, ServiceName: "matching", StopAt: pastUnix}, // already stopped
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)
	defer m.Stop()

	resolver, _ := m.GetResolver("matching")
	// Only the first member (stopAt=0) should be visible
	if resolver.MemberCount() != 1 {
		t.Fatalf("expected 1 member (past stopAt filtered), got %d", resolver.MemberCount())
	}
}
