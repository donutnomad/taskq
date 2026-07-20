package membership

import (
	"sync"
	"testing"
	"time"
)

func TestServiceResolverConcurrentSetMembersAndLookup(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 200 {
			hosts := []*hostInfo{makeHost("10.0.0.1:7235", false)}
			if i%2 == 0 {
				hosts = append(hosts, makeHost("10.0.0.2:7235", false))
			}
			r.setMembers(hosts)
		}
	})
	wg.Go(func() {
		for range 200 {
			_, _ = r.Lookup("concurrent-key")
			_ = r.MemberCount()
			_ = r.AvailableMemberCount()
		}
	})
	wg.Wait()
}

func TestServiceResolverNilMembersClearRingAndNotifyRemovals(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()
	changes := make(chan *ChangedEvent, 2)
	if err := r.AddListener("clear", changes); err != nil {
		t.Fatal(err)
	}
	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", false),
	})
	select {
	case <-changes:
	case <-time.After(time.Second):
		t.Fatal("initial membership event timed out")
	}

	r.setMembers(nil)
	if r.MemberCount() != 0 {
		t.Fatalf("member count = %d, want 0", r.MemberCount())
	}
	select {
	case event := <-changes:
		if len(event.HostsRemoved) != 2 {
			t.Fatalf("removed hosts = %d, want 2", len(event.HostsRemoved))
		}
	case <-time.After(time.Second):
		t.Fatal("removal event timed out")
	}
}

func makeHost(addr string, draining bool) *hostInfo {
	return newHostInfo(addr, draining, 0, 0)
}

func TestServiceResolverLookup(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", false),
	})

	host, err := r.Lookup("namespace:queue:1")
	if err != nil {
		t.Fatal(err)
	}
	if host.GetAddress() == "" {
		t.Fatal("expected non-empty address")
	}
}

func TestServiceResolverEmpty(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	_, err := r.Lookup("any-key")
	if err != ErrInsufficientHosts {
		t.Fatalf("expected ErrInsufficientHosts, got %v", err)
	}
}

func TestServiceResolverMemberCount(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", true),
	})

	if r.MemberCount() != 2 {
		t.Fatalf("expected 2, got %d", r.MemberCount())
	}
	if r.AvailableMemberCount() != 1 {
		t.Fatalf("expected 1 available, got %d", r.AvailableMemberCount())
	}
}

func TestServiceResolverListsMembersAndAvailableMembers(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", true),
		makeHost("10.0.0.3:7235", false),
	})

	members := r.Members()
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	available := r.AvailableMembers()
	if len(available) != 2 {
		t.Fatalf("expected 2 available members, got %d", len(available))
	}
	for _, host := range available {
		if host.(*hostInfo).draining {
			t.Fatalf("expected available members to skip draining hosts, got %+v", host)
		}
	}
}

func TestServiceResolverLookupN(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	if got := r.LookupN("empty", 2); got != nil {
		t.Fatalf("expected nil LookupN result on empty resolver, got %+v", got)
	}
	if got := r.LookupN("empty", 0); got != nil {
		t.Fatalf("expected nil LookupN result for non-positive count, got %+v", got)
	}

	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", false),
		makeHost("10.0.0.3:7235", false),
	})

	hosts := r.LookupN("namespace:queue:1", 2)
	if len(hosts) != 2 {
		t.Fatalf("expected 2 lookup results, got %d", len(hosts))
	}
}

func TestServiceResolverChangedEvent(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	ch := make(chan *ChangedEvent, 5)
	_ = r.AddListener("test", ch)

	r.setMembers([]*hostInfo{makeHost("10.0.0.1:7235", false)})

	select {
	case ev := <-ch:
		if len(ev.HostsAdded) != 1 {
			t.Fatalf("expected 1 added, got %d", len(ev.HostsAdded))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ChangedEvent")
	}
}

func TestServiceResolverEventuallyNotifiesSlowListener(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	ch := make(chan *ChangedEvent)
	if err := r.AddListener("slow", ch); err != nil {
		t.Fatal(err)
	}

	r.setMembers([]*hostInfo{makeHost("10.0.0.1:7235", false)})

	select {
	case event := <-ch:
		if len(event.HostsAdded) != 1 {
			t.Fatalf("expected coalesced membership change, got %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("slow listener never received the pending membership change")
	}
}

func TestMergeChangedEventsProducesOneNetChangePerAddress(t *testing.T) {
	hostBefore := NewHostInfoFromAddress("10.0.0.1:7235")
	hostAfter := newHostInfo("10.0.0.1:7235", true, 0, 0)
	tests := []struct {
		name    string
		current *ChangedEvent
		next    *ChangedEvent
		added   int
		removed int
		changed int
	}{
		{
			name:    "add then remove cancels",
			current: &ChangedEvent{HostsAdded: []HostInfo{hostBefore}},
			next:    &ChangedEvent{HostsRemoved: []HostInfo{hostAfter}},
		},
		{
			name:    "remove then add becomes changed",
			current: &ChangedEvent{HostsRemoved: []HostInfo{hostBefore}},
			next:    &ChangedEvent{HostsAdded: []HostInfo{hostAfter}},
			changed: 1,
		},
		{
			name:    "change then remove becomes removed",
			current: &ChangedEvent{HostsChanged: []HostInfo{hostBefore}},
			next:    &ChangedEvent{HostsRemoved: []HostInfo{hostAfter}},
			removed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := mergeChangedEvents(tt.current, tt.next)
			if len(merged.HostsAdded) != tt.added || len(merged.HostsRemoved) != tt.removed || len(merged.HostsChanged) != tt.changed {
				t.Fatalf("merged event = %+v, want added=%d removed=%d changed=%d", merged, tt.added, tt.removed, tt.changed)
			}
		})
	}
}

func TestServiceResolverDoubleStop(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Stop()

	// 第二次调用 Stop() 不应 panic
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("double Stop() panicked: %v", p)
		}
	}()
	r.Stop()
}

func TestServiceResolverRejectsListenerAfterStop(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Stop()

	if err := r.AddListener("late", make(chan *ChangedEvent, 1)); err != ErrResolverStopped {
		t.Fatalf("expected ErrResolverStopped, got %v", err)
	}
}

func TestServiceResolverDuplicateListener(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	ch := make(chan *ChangedEvent, 1)
	if err := r.AddListener("dup", ch); err != nil {
		t.Fatal(err)
	}
	if err := r.AddListener("dup", ch); err != ErrListenerAlreadyExist {
		t.Fatalf("expected ErrListenerAlreadyExist, got %v", err)
	}
}

func TestServiceResolverRemoveListener(t *testing.T) {
	r := newServiceResolver("matching", 200)
	defer r.Stop()

	ch := make(chan *ChangedEvent, 1)
	if err := r.AddListener("removed", ch); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoveListener("removed"); err != nil {
		t.Fatal(err)
	}

	r.setMembers([]*hostInfo{makeHost("10.0.0.1:7235", false)})
	select {
	case event := <-ch:
		t.Fatalf("expected removed listener to receive no event, got %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}
