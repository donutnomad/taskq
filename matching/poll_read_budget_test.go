package matching

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/taskq/store/gormstore"
	"gorm.io/gorm"
)

// ============================================
// PollQueue 的读库预算（SQLite 上的真实 SQL）
// ============================================
// - Happy Path: 每个分区的行都已投递未确认时，长轮询不读任务表（此前每个分区扫描与登记 waiter 各读一次）。
// - Happy Path: 分区里只有别的 routing key 的行时，长轮询每个分区只读一次（扫描），登记 waiter 不再重读。
// ============================================

// taskReadCounter 统计 poller 读取待投递任务的 SELECT（taskq_tasks 上带 dispatched_at IS NULL 的查询）与
// 可见性扫描的 SELECT（dispatched_at IS NOT NULL）。
type taskReadCounter struct {
	pollReads atomic.Int64
	visReads  atomic.Int64
}

func countTaskReads(t *testing.T, db *gorm.DB) *taskReadCounter {
	t.Helper()
	counter := &taskReadCounter{}
	err := db.Callback().Query().After("gorm:query").Register("test:count_task_reads", func(tx *gorm.DB) {
		sql := tx.Statement.SQL.String()
		if !strings.Contains(sql, "taskq_tasks") {
			return
		}
		switch {
		case strings.Contains(sql, "dispatched_at IS NULL"):
			counter.pollReads.Add(1)
		case strings.Contains(sql, "dispatched_at IS NOT NULL"):
			counter.visReads.Add(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return counter
}

func newCountingSQLiteServer(t *testing.T, partitions int) (MatchingServiceClient, *taskReadCounter) {
	t.Helper()
	store, err := gormstore.NewSQLite(filepath.Join(t.TempDir(), "taskq.db"), gormstore.Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	counter := countTaskReads(t, store.DB())
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"budget-q": partitions})
	return newTestClient(t, address), counter
}

// measurePollReads 发一次 200ms 的空长轮询，返回期间 poller 的读库次数。可见性扫描每秒一次，它的投递尝试也读
// 任务表：测量窗口里发生了扫描时重测。
func measurePollReads(t *testing.T, client MatchingServiceClient, counter *taskReadCounter, partitionIDs []int32) int64 {
	t.Helper()
	for range 5 {
		pollBefore, visBefore := counter.pollReads.Load(), counter.visReads.Load()
		response, err := client.PollQueue(context.Background(), &PollQueueRequest{
			QueueName: "budget-q", PartitionIds: partitionIDs, PollTimeoutMs: 200,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !response.Empty {
			t.Fatalf("idle poll returned a task: %+v", response)
		}
		if counter.visReads.Load() == visBefore {
			return counter.pollReads.Load() - pollBefore
		}
	}
	t.Fatal("every measurement overlapped a visibility scan")
	return 0
}

func TestMatchingServer_LongPollDoesNotReadHiddenRows(t *testing.T) {
	client, counter := newCountingSQLiteServer(t, 4)
	ctx := context.Background()
	partitions := []int32{0, 1, 2, 3}
	for _, partitionID := range partitions {
		if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "budget-q", PartitionId: partitionID, Data: []byte("in-flight")}); err != nil {
			t.Fatal(err)
		}
	}
	for range partitions {
		response, err := client.PollQueue(ctx, &PollQueueRequest{QueueName: "budget-q", PartitionIds: partitions})
		if err != nil || response.Empty {
			t.Fatalf("taking the in-flight tasks: response=%+v err=%v", response, err)
		}
	}

	if reads := measurePollReads(t, client, counter, partitions); reads != 0 {
		t.Fatalf("long poll over in-flight rows read the task table %d times, want 0", reads)
	}

	// 新任务照常唤醒长轮询。
	result := make(chan *PollQueueResponse, 1)
	go func() {
		response, err := client.PollQueue(ctx, &PollQueueRequest{QueueName: "budget-q", PartitionIds: partitions, PollTimeoutMs: 3000})
		if err != nil {
			t.Error(err)
		}
		result <- response
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "budget-q", PartitionId: 2, Data: []byte("fresh")}); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-result:
		if response == nil || response.Empty || string(response.Data) != "fresh" {
			t.Fatalf("response = %+v", response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll was not woken by a new task")
	}
}

func TestMatchingServer_LongPollReadsEachPartitionOnce(t *testing.T) {
	client, counter := newCountingSQLiteServer(t, 4)
	ctx := context.Background()
	partitions := []int32{0, 1, 2, 3}
	for _, partitionID := range partitions {
		if _, err := client.AddTask(ctx, &AddTaskRequest{
			QueueName: "budget-q", PartitionId: partitionID, Data: []byte("routed"), RoutingKey: "elsewhere",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if reads := measurePollReads(t, client, counter, partitions); reads != int64(len(partitions)) {
		t.Fatalf("long poll read the task table %d times, want %d (one scan per partition)", reads, len(partitions))
	}
}
