package matching

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/donutnomad/taskq/membership"
)

type failingListenerResolver struct {
	*stubResolver
	err error
}

func (r *failingListenerResolver) AddListener(string, chan<- *membership.ChangedEvent) error {
	return r.err
}

type resolverMonitor struct {
	resolver membership.ServiceResolver
}

func (m *resolverMonitor) Start()                                       {}
func (m *resolverMonitor) Stop()                                        {}
func (m *resolverMonitor) EvictSelfAt(time.Time) (time.Duration, error) { return 0, nil }
func (m *resolverMonitor) GetResolver(string) (membership.ServiceResolver, error) {
	return m.resolver, nil
}
func (m *resolverMonitor) WaitUntilInitialized(context.Context) error { return nil }
func (m *resolverMonitor) SetDraining(bool) error                     { return nil }

func TestMatchingStartRollsBackWhenListenerRegistrationFails(t *testing.T) {
	store := newTestSQLiteStore(t)
	listenAddr := unusedLocalAddress(t)
	listenerErr := errors.New("listener registration failed")
	resolver := &failingListenerResolver{
		stubResolver: newStubResolver(listenAddr),
		err:          listenerErr,
	}
	server, err := NewServer(Config{
		Store:         store,
		Monitor:       &resolverMonitor{resolver: resolver},
		ListenAddr:    listenAddr,
		BroadcastAddr: listenAddr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	if err := server.Start(); !errors.Is(err, listenerErr) {
		t.Fatalf("expected listener error, got %v", err)
	}
	assertFailedStartCleanedUp(t, server)
}

func TestMatchingStartRollsBackWhenListenFails(t *testing.T) {
	store := newTestSQLiteStore(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	listenAddr := listener.Addr().String()
	resolver := newStubResolver(listenAddr)
	server, err := NewServer(Config{
		Store:         store,
		Monitor:       &stubMonitor{resolver: resolver},
		ListenAddr:    listenAddr,
		BroadcastAddr: listenAddr,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	if err := server.Start(); err == nil {
		t.Fatal("expected listen failure")
	}
	if len(resolver.listeners) != 0 {
		t.Fatalf("expected failed start to remove listener, got %d", len(resolver.listeners))
	}
	assertFailedStartCleanedUp(t, server)
}

func unusedLocalAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func assertFailedStartCleanedUp(t *testing.T, server *Server) {
	t.Helper()
	select {
	case <-server.ctx.Done():
	default:
		t.Fatal("expected failed start to cancel server lifecycle")
	}
	server.mgr.mu.RLock()
	partitionCount := len(server.mgr.partitions)
	server.mgr.mu.RUnlock()
	if partitionCount != 0 {
		t.Fatalf("expected failed start to release partitions, got %d", partitionCount)
	}
}
