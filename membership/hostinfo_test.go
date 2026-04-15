package membership

import "testing"

func TestHostInfoBasic(t *testing.T) {
	h := newHostInfo("10.0.0.1:7233", false, 0, 0)
	if h.GetAddress() != "10.0.0.1:7233" {
		t.Fatalf("unexpected address: %s", h.GetAddress())
	}
	if h.Identity() != "10.0.0.1:7233" {
		t.Fatalf("unexpected identity: %s", h.Identity())
	}
}

func TestHostInfoDraining(t *testing.T) {
	h1 := newHostInfo("10.0.0.1:7233", false, 0, 0)
	h2 := newHostInfo("10.0.0.1:7233", true, 0, 0)
	if isDraining(h1) {
		t.Fatal("h1 should not be draining")
	}
	if !isDraining(h2) {
		t.Fatal("h2 should be draining")
	}
}

func TestHostInfoChecksum(t *testing.T) {
	h1 := newHostInfo("10.0.0.1:7233", false, 0, 0)
	h2 := newHostInfo("10.0.0.1:7233", false, 0, 0)
	h3 := newHostInfo("10.0.0.1:7233", true, 0, 0)
	if h1.labelsChecksum != h2.labelsChecksum {
		t.Fatal("same state should produce same checksum")
	}
	if h1.labelsChecksum == h3.labelsChecksum {
		t.Fatal("different draining should produce different checksum")
	}
}
