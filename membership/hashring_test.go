package membership

import (
	"fmt"
	"testing"
)

func TestHashRingEmpty(t *testing.T) {
	ring := newHashRing(100)
	_, found := ring.Lookup("any-key")
	if found {
		t.Fatal("expected not found on empty ring")
	}
	if got := ring.LookupN("any-key", 3); len(got) != 0 {
		t.Fatalf("expected empty, got %d", len(got))
	}
}

func TestHashRingLookupDeterministic(t *testing.T) {
	ring := newHashRing(100)
	ring.AddMember("host-a:7233")
	ring.AddMember("host-b:7233")
	ring.AddMember("host-c:7233")

	addr1, _ := ring.Lookup("namespace:queue:1")
	addr2, _ := ring.Lookup("namespace:queue:1")
	if addr1 != addr2 {
		t.Fatalf("non-deterministic: %s vs %s", addr1, addr2)
	}
}

func TestHashRingLookupDeterministicWhenVNodesCollide(t *testing.T) {
	constantHash := func([]byte) uint32 { return 42 }
	left := newHashRingWithHash(2, constantHash)
	right := newHashRingWithHash(2, constantHash)

	left.AddMember("host-b:7233")
	left.AddMember("host-a:7233")
	right.AddMember("host-a:7233")
	right.AddMember("host-b:7233")

	leftOwner, leftFound := left.Lookup("same-key")
	rightOwner, rightFound := right.Lookup("same-key")
	if !leftFound || !rightFound {
		t.Fatal("expected both rings to resolve the key")
	}
	if leftOwner != rightOwner {
		t.Fatalf("colliding vnode order changed owner: left=%q right=%q", leftOwner, rightOwner)
	}
}

func TestHashRingLookupNFirst(t *testing.T) {
	ring := newHashRing(100)
	ring.AddMember("host-a:7233")
	ring.AddMember("host-b:7233")
	ring.AddMember("host-c:7233")

	single, _ := ring.Lookup("some-key")
	multi := ring.LookupN("some-key", 2)
	if len(multi) == 0 || multi[0] != single {
		t.Fatalf("LookupN[0] must equal Lookup: single=%s multi=%v", single, multi)
	}
}

func TestHashRingRemoveMember(t *testing.T) {
	ring := newHashRing(100)
	ring.AddMember("host-a:7233")
	ring.AddMember("host-b:7233")

	ring.RemoveMember("host-a:7233")
	for i := range 1000 {
		addr, found := ring.Lookup(fmt.Sprintf("key-%d", i))
		if !found {
			t.Fatal("should find host-b")
		}
		if addr == "host-a:7233" {
			t.Fatal("removed member should not appear")
		}
	}
}

func TestHashRingDistribution(t *testing.T) {
	ring := newHashRing(200)
	members := []string{"host-a:7233", "host-b:7233", "host-c:7233"}
	for _, m := range members {
		ring.AddMember(m)
	}

	counts := map[string]int{}
	for i := range 3000 {
		addr, _ := ring.Lookup(fmt.Sprintf("key-%d", i))
		counts[addr]++
	}
	for _, m := range members {
		if counts[m] < 150 {
			t.Errorf("poor distribution for %s: %d/3000", m, counts[m])
		}
	}
}
