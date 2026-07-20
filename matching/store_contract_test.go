package matching

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/donutnomad/taskq/store/gormstore"
)

var _ Store = (*gormstore.Store)(nil)

func TestMatchingStopLeavesStoreConnectionOwnedByCaller(t *testing.T) {
	store, err := gormstore.NewSQLite(filepath.Join(t.TempDir(), "matching-store.db"), gormstore.Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	resolver := newStubResolver("127.0.0.1:1")
	server, err := NewServer(Config{
		Store:         store,
		Monitor:       &stubMonitor{resolver: resolver},
		BroadcastAddr: "127.0.0.1:1",
		QueueConfigs:  map[string]int{"store-contract-q": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	server.Stop()

	db, err := store.DB().DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("expected caller-owned store connection to remain open: %v", err)
	}
}
