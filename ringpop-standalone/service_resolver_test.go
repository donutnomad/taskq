package ringpop

import (
	"testing"
	"time"
)

func makeHost(addr string, draining bool) *hostInfo {
	return newHostInfo(addr, draining, 0, 0)
}

func TestServiceResolverLookup(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
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
	r.Start()
	defer r.Stop()

	_, err := r.Lookup("any-key")
	if err != ErrInsufficientHosts {
		t.Fatalf("expected ErrInsufficientHosts, got %v", err)
	}
}

func TestServiceResolverMemberCount(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
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

func TestServiceResolverChangedEvent(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
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

func TestServiceResolverDuplicateListener(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
	defer r.Stop()

	ch := make(chan *ChangedEvent, 1)
	if err := r.AddListener("dup", ch); err != nil {
		t.Fatal(err)
	}
	if err := r.AddListener("dup", ch); err != ErrListenerAlreadyExist {
		t.Fatalf("expected ErrListenerAlreadyExist, got %v", err)
	}
}
