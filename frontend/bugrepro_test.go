//go:build bugrepro

package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestBugRepro_DequeueLongPollMissesOtherPartitions(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	addr := startMatchingServer(t, store, resolver, map[string]int{"poll-q": 2})

	fe, err := New(Config{
		Monitor:     &stubMonitor{resolver: resolver},
		QueueName:   "poll-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := matching.NewMatchingServiceClient(conn)

	type result struct {
		data  []byte
		token string
		err   error
	}
	resultCh := make(chan result, 1)
	go func() {
		data, token, err := fe.Dequeue(context.Background())
		resultCh <- result{data: data, token: token, err: err}
	}()

	time.Sleep(100 * time.Millisecond)

	_, err = client.AddTask(context.Background(), &matching.AddTaskRequest{
		QueueName:   "poll-q",
		PartitionId: 0,
		Data:        []byte("other-partition"),
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-resultCh:
		if got.err != nil {
			t.Fatalf("expected dequeue to receive task from another partition, got error: %v", got.err)
		}
		if string(got.data) != "other-partition" {
			t.Fatalf("expected other-partition, got %q", got.data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dequeue never returned after task arrived on another partition")
	}
}

func TestBugRepro_IsRetryableForOwnershipLoss(t *testing.T) {
	if !isRetryable(status.Error(codes.Aborted, "ownership lost")) {
		t.Fatal("expected Aborted ownership-loss errors to be retried")
	}
}
