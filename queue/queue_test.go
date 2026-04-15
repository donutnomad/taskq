package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestQueue(t *testing.T, partitions int) *PartitionedQueue {
	t.Helper()
	s, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	q, err := New(ctx, Options{
		QueueName:  "test",
		Store:      s,
		Partitions: partitions,
		VisTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { q.Close() })
	return q
}

func TestPartitionedQueue_EnqueueDequeueAck(t *testing.T) {
	q := newTestQueue(t, 4)
	ctx := context.Background()

	raw := []byte(`{"key":"value"}`)
	if err := q.EnqueueRaw(raw); err != nil {
		t.Fatal(err)
	}

	row, token, err := q.DequeueRaw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(row) != string(raw) {
		t.Fatalf("data mismatch: %s", row)
	}

	if err := q.Ack(token); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionedQueue_NackRedelivery(t *testing.T) {
	q := newTestQueue(t, 1)
	ctx := context.Background()

	q.EnqueueRaw([]byte("task-data"))

	_, token, _ := q.DequeueRaw(ctx)

	q.Nack(token)

	// 应该能再次消费到
	row, _, err := q.DequeueRaw(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(row) != "task-data" {
		t.Fatalf("expected redelivered task, got %s", row)
	}
}

func TestPartitionedQueue_ConcurrentProducerConsumer(t *testing.T) {
	q := newTestQueue(t, 4)
	ctx := context.Background()
	total := 100

	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q.EnqueueRaw([]byte(fmt.Sprintf("task-%d", i)))
		}(i)
	}
	wg.Wait()

	var consumed atomic.Int64
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if consumed.Load() >= int64(total) {
					return
				}
				dCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				_, token, err := q.DequeueRaw(dCtx)
				cancel()
				if err != nil {
					continue
				}
				q.Ack(token)
				consumed.Add(1)
			}
		}()
	}
	wg.Wait()

	if consumed.Load() != int64(total) {
		t.Fatalf("expected %d consumed, got %d", total, consumed.Load())
	}
}

func TestPartitionedQueue_VisibilityTimeout(t *testing.T) {
	q := newTestQueue(t, 1)
	ctx := context.Background()

	q.EnqueueRaw([]byte("timeout-task"))

	// 消费但不 ack
	_, _, _ = q.DequeueRaw(ctx)

	// 等待 visTimeout (2s) + scan interval (1s)
	time.Sleep(4 * time.Second)

	dCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	row, token, err := q.DequeueRaw(dCtx)
	if err != nil {
		t.Fatalf("expected redelivered task after visibility timeout, got err: %v", err)
	}
	if string(row) != "timeout-task" {
		t.Fatalf("wrong data: %s", row)
	}
	q.Ack(token)
}

func TestPartitionedQueue_AddPartition(t *testing.T) {
	q := newTestQueue(t, 2)
	ctx := context.Background()

	if err := q.AddPartition(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		q.EnqueueRaw([]byte(fmt.Sprintf("task-%d", i)))
	}

	for i := 0; i < 3; i++ {
		dCtx, cancel := context.WithTimeout(ctx, time.Second)
		_, token, err := q.DequeueRaw(dCtx)
		cancel()
		if err != nil {
			t.Fatalf("task %d: %v", i, err)
		}
		q.Ack(token)
	}
}
