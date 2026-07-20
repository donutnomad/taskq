package frontend

import (
	"context"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/store/gormstore"
)

func TestFrontendObserverMonitorConsumesQueueWithoutAdvertisingMatching(t *testing.T) {
	store, err := gormstore.NewSQLite(filepath.Join(t.TempDir(), "taskq.db"), gormstore.Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	address := unusedFrontendAddress(t)
	addressPort, err := netip.ParseAddrPort(address)
	if err != nil {
		t.Fatal(err)
	}
	matchingMonitor := membership.NewMonitorWithConfig(membership.MonitorConfig{
		AdvertisedServices: []string{"matching"},
		WatchedServices:    []string{"matching"},
		Store:              store.Membership(),
		BroadcastAddr:      addressPort,
		PollInterval:       20 * time.Millisecond,
		PropagationTime:    50 * time.Millisecond,
		ReplicaPoints:      100,
	})
	matchingMonitor.Start()
	defer matchingMonitor.Stop()
	waitForMonitorInitialization(t, matchingMonitor)

	server, err := matching.NewServer(matching.Config{
		Store:         store,
		Monitor:       matchingMonitor,
		ListenAddr:    address,
		BroadcastAddr: address,
		QueueConfigs:  map[string]int{"workflow-tasks": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	observer := membership.NewMonitorWithConfig(membership.MonitorConfig{
		WatchedServices: []string{"matching"},
		Store:           store.Membership(),
		PollInterval:    20 * time.Millisecond,
		PropagationTime: 50 * time.Millisecond,
		ReplicaPoints:   100,
	})
	observer.Start()
	defer observer.Stop()
	waitForMonitorInitialization(t, observer)

	client, err := New(Config{
		ResolverProvider: observer,
		QueueName:        "workflow-tasks",
		PollTimeout:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Enqueue(ctx, []byte("workflow-task")); err != nil {
		t.Fatal(err)
	}
	data, token, err := client.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "workflow-task" {
		t.Fatalf("task data = %q", data)
	}
	if err := client.Ack(ctx, token); err != nil {
		t.Fatal(err)
	}

	var matchingRecords int64
	if err := store.DB().Table("taskq_membership_members").
		Where("service_name = ?", "matching").
		Count(&matchingRecords).Error; err != nil {
		t.Fatal(err)
	}
	if matchingRecords != 1 {
		t.Fatalf("matching membership records = %d, want 1", matchingRecords)
	}
}

func waitForMonitorInitialization(t *testing.T, monitor membership.Monitor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := monitor.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}
}
