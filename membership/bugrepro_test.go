package membership

import (
	"context"
	"net"
	"net/netip"
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

	members, err := m.GetReachableMembers()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0] != "10.0.0.1:7235" {
		t.Fatalf("expected one unique reachable member, got %v", members)
	}
}
