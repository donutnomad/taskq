package matching

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/taskq/queue"
)

// CancelPoll 结束等待中的 PollQueue：以 empty 应答，不留下会吞掉后续任务的 waiter。
func TestMatchingServer_CancelPollEndsWaitingPoll(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	client := newTestClient(t, address)
	ctx := context.Background()

	result := make(chan *PollQueueResponse, 1)
	errs := make(chan error, 1)
	go func() {
		response, err := client.PollQueue(ctx, &PollQueueRequest{
			QueueName: "test-q", PartitionIds: []int32{0, 1}, PollTimeoutMs: 10_000, PollId: "poll-1",
		})
		if err != nil {
			errs <- err
			return
		}
		result <- response
	}()
	// 两种到达顺序结果相同：轮询已登记时被结束，尚未登记时登记即遇到墓碑。
	time.Sleep(100 * time.Millisecond)
	if _, err := client.CancelPoll(ctx, &CancelPollRequest{PollId: "poll-1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	case response := <-result:
		if !response.Empty {
			t.Fatalf("canceled poll response = %+v, want empty", response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CancelPoll did not end the waiting poll")
	}

	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 1, Data: []byte("after-cancel")}); err != nil {
		t.Fatal(err)
	}
	response, err := client.PollQueue(ctx, &PollQueueRequest{QueueName: "test-q", PartitionIds: []int32{0, 1}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Empty || string(response.Data) != "after-cancel" {
		t.Fatalf("post-cancel response = %+v", response)
	}
}

// CancelPoll 先于 PollQueue 到达时留下墓碑：PollQueue 立即以 empty 结束，不领取任务。
func TestMatchingServer_CancelPollBeforePollQueue(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	_, address := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 1})
	client := newTestClient(t, address)
	ctx := context.Background()

	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("kept")}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CancelPoll(ctx, &CancelPollRequest{PollId: "poll-early"}); err != nil {
		t.Fatal(err)
	}
	response, err := client.PollQueue(ctx, &PollQueueRequest{
		QueueName: "test-q", PartitionIds: []int32{0}, PollTimeoutMs: 10_000, PollId: "poll-early",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Empty {
		t.Fatalf("pre-canceled poll response = %+v, want empty", response)
	}
	response, err = client.PollQueue(ctx, &PollQueueRequest{QueueName: "test-q", PartitionIds: []int32{0}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Empty || string(response.Data) != "kept" {
		t.Fatalf("follow-up response = %+v, want the untouched task", response)
	}
}

// 已被 CancelPoll 结束的轮询手里的租约由 owner 原地归还，任务立即可再领取，不消耗 attempt。
func TestMatchingPollFinalizationReturnsLeaseWhenPollIsStopped(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	server, _ := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 1})
	ctx := context.Background()

	handle, err := server.mgr.getOrLoadPartition(ctx, "test-q", 0, accessRead)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.AddTask(ctx, []byte("stopped")); err != nil {
		t.Fatal(err)
	}
	lease, ok, err := handle.TryPollWithOptions(ctx, queue.PollOptions{})
	if err != nil || !ok {
		t.Fatalf("lease ok = %v, err = %v", ok, err)
	}
	stop := make(chan struct{})
	close(stop)
	response, err := finishQueuePoll(ctx, stop, []queuePollCandidate{{target: queuePollTarget{partitionID: 0, handle: handle}, lease: lease}})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Empty {
		t.Fatalf("stopped poll response = %+v, want empty", response)
	}
	row, _, ok, err := handle.TryPoll(ctx)
	if err != nil || !ok {
		t.Fatalf("re-poll ok = %v, err = %v", ok, err)
	}
	if row.Attempt != 0 {
		t.Fatalf("returned row attempt = %d, want 0", row.Attempt)
	}
}
