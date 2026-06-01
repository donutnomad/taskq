package membership

import (
	"testing"
	"time"
)

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
