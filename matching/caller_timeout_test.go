package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/taskq/queue"
)

// slowReadStore 让开启后的读库照常读到行，再拖到 ctx 结束（至多 200ms）才返回：扫描读到行时调用方已经离开。
type slowReadStore struct {
	Store
	slow atomic.Bool
}

func (s *slowReadStore) GetTasksWithOptions(ctx context.Context, queueName string, partitionID int, minTaskID, maxTaskID int64, limit int, opts queue.TaskQueryOptions) ([]*queue.TaskRow, error) {
	rows, err := s.Store.GetTasksWithOptions(context.WithoutCancel(ctx), queueName, partitionID, minTaskID, maxTaskID, limit, opts)
	if s.slow.Load() {
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}
	return rows, err
}

// poll RPC 的 ctx 比调用方的期限长（Frontend 不随调用方取消 RPC），owner 按 caller_timeout_ms 判断调用方是否
// 还在：期限过后读到的行不再投递，应答为空，行原样留给下一个 poller。此前照常投递，租约到达时调用方已经离开，
// 只能再花一次归还。
func TestMatchingServer_PollQueueDoesNotDispatchAfterCallerTimeout(t *testing.T) {
	store := &slowReadStore{Store: newTestSQLiteStore(t)}
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 1})
	client := newTestClient(t, address)
	ctx := context.Background()

	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	store.slow.Store(true)
	response, err := client.PollQueue(ctx, &PollQueueRequest{QueueName: "test-q", PartitionIds: []int32{0}, CallerTimeoutMs: 20})
	store.slow.Store(false)
	if err != nil {
		t.Fatal(err)
	}
	if !response.Empty {
		t.Fatalf("poll whose caller timed out during the scan = %+v, want empty", response)
	}

	response, err = client.PollQueue(ctx, &PollQueueRequest{QueueName: "test-q", PartitionIds: []int32{0}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Empty || string(response.Data) != "kept" {
		t.Fatalf("follow-up response = %+v, want the untouched task", response)
	}
}
