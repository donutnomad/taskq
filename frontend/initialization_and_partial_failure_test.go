package frontend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type partitionResolver struct {
	defaultHost membership.HostInfo
	partition1  membership.HostInfo
	listeners   map[string]chan<- *membership.ChangedEvent
}

func (r *partitionResolver) Lookup(key string) (membership.HostInfo, error) {
	if strings.HasSuffix(key, "/1") {
		return r.partition1, nil
	}
	return r.defaultHost, nil
}

func (r *partitionResolver) AddListener(name string, ch chan<- *membership.ChangedEvent) error {
	if _, exists := r.listeners[name]; exists {
		return membership.ErrListenerAlreadyExist
	}
	r.listeners[name] = ch
	return nil
}

func (r *partitionResolver) RemoveListener(name string) error {
	delete(r.listeners, name)
	return nil
}

func TestNewWithContextHonorsInitialQueueInfoDeadline(t *testing.T) {
	resolver := newStubResolver("127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewWithContext(ctx, Config{
		Monitor:   &stubMonitor{resolver: resolver},
		QueueName: "init-q",
		DialOpts:  []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled initialization, got %v", err)
	}
	if len(resolver.listeners) != 0 {
		t.Fatalf("expected canceled initialization to avoid listener registration, got %d", len(resolver.listeners))
	}
}

func TestFrontendDequeueReportsPartialPartitionFailure(t *testing.T) {
	frontend, _ := newPartiallyAvailableFrontend(t, "partial-q")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, _, err := frontend.Dequeue(ctx)
	if err == nil {
		t.Fatal("expected partial partition failure")
	}
	if errors.Is(err, ErrNoTask) {
		t.Fatalf("expected diagnostic partition error, got %v", err)
	}
	if !strings.Contains(err.Error(), "partitions [1]") {
		t.Fatalf("expected failed partition in error, got %v", err)
	}
}

func TestFrontendDequeuePrefersTaskOverPartialPartitionFailure(t *testing.T) {
	frontend, matchingAddr := newPartiallyAvailableFrontend(t, "priority-q")
	conn, err := grpc.NewClient(matchingAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := matching.NewMatchingServiceClient(conn)
	if _, err := client.AddTask(context.Background(), &matching.AddTaskRequest{
		QueueName: "priority-q", PartitionId: 0, Data: []byte("ready"),
	}); err != nil {
		t.Fatal(err)
	}

	data, _, err := frontend.Dequeue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "ready" {
		t.Fatalf("expected ready task, got %q", data)
	}
}

func newPartiallyAvailableFrontend(t *testing.T, queueName string) (*Frontend, string) {
	t.Helper()
	store := newTestSQLiteStore(t)
	matchingResolver := newStubResolver()
	matchingAddr := startMatchingServer(t, store, matchingResolver, map[string]int{queueName: 2})
	deadAddr := unusedFrontendAddress(t)
	resolver := &partitionResolver{
		defaultHost: membership.NewHostInfoFromAddress(matchingAddr),
		partition1:  membership.NewHostInfoFromAddress(deadAddr),
		listeners:   make(map[string]chan<- *membership.ChangedEvent),
	}
	frontend, err := NewWithContext(context.Background(), Config{
		Monitor:     &refreshAwareMonitor{resolver: resolver},
		QueueName:   queueName,
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(frontend.Close)
	return frontend, matchingAddr
}

func unusedFrontendAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(fmt.Errorf("close unused listener: %w", err))
	}
	return addr
}
