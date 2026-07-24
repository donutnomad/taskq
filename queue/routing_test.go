package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPhysicalQueue_KeyedTaskRequiresAcceptedRoutingKey(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.AddTaskWithOptions(ctx, []byte("worker-a-task"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}

	if lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("worker-b received an ineligible lease: %+v", lease)
	}

	lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("worker-a did not receive its keyed task")
	}
	if string(lease.Data) != "worker-a-task" || lease.RoutingKey != "worker-a" || lease.Token == "" {
		t.Fatalf("unexpected lease: %+v", lease)
	}
}

func TestPhysicalQueue_RoutingDeadlineFallsBackInPlace(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()
	deadline := time.Now().Add(200 * time.Millisecond)
	if err := pq.AddTaskWithOptions(ctx, []byte("sticky-task"), EnqueueOptions{
		RoutingKey: "sticky-worker", RoutingDeadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}

	if lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"other-worker"}}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("task fell back before deadline: %+v", lease)
	}
	time.Sleep(time.Until(deadline) + 10*time.Millisecond)

	lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"other-worker"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(lease.Data) != "sticky-task" || lease.RoutingKey != "sticky-worker" || !lease.RoutingFallback {
		t.Fatalf("fallback lease = %+v, ok = %v", lease, ok)
	}
	parsed, err := ParseTaskToken(lease.Token)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.TaskID != 1 {
		t.Fatalf("fallback task ID = %d, want original ID 1", parsed.TaskID)
	}
}

func TestPhysicalQueue_RoutingDeadlineRequiresRoutingKey(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	err := pq.AddTaskWithOptions(context.Background(), []byte("invalid"), EnqueueOptions{
		RoutingDeadline: time.Now().Add(time.Second),
	})
	if !errors.Is(err, ErrInvalidRoutingDeadline) {
		t.Fatalf("enqueue error = %v", err)
	}
	if err := pq.AddTaskWithOptions(context.Background(), []byte("ordinary"), EnqueueOptions{}); err != nil {
		t.Fatalf("zero deadline ordinary task: %v", err)
	}
}

func TestPhysicalQueue_VisibilityScanWakesWaiterAfterRoutingDeadline(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()
	deadline := time.Now().Add(200 * time.Millisecond)
	if err := pq.AddTaskWithOptions(ctx, []byte("fallback-wakeup"), EnqueueOptions{
		RoutingKey: "sticky-worker", RoutingDeadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	waiter, err := pq.RegisterWaiterWithOptions(PollOptions{AcceptedRoutingKeys: []string{"other-worker"}})
	if err != nil {
		t.Fatal(err)
	}
	defer waiter.Cancel()

	select {
	case <-waiter.Ready():
		t.Fatal("waiter woke before routing deadline")
	default:
	}
	time.Sleep(time.Until(deadline) + 10*time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)

	select {
	case <-waiter.Ready():
		result, claimed := waiter.Claim()
		if !claimed || result.Row == nil || string(result.Row.Data) != "fallback-wakeup" {
			t.Fatalf("waiter result = %+v, claimed = %v", result, claimed)
		}
	case <-time.After(time.Second):
		t.Fatal("visibility scan did not wake fallback waiter")
	}
}

func TestPhysicalQueue_NackKeepsExpiredTaskInFallback(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()
	if err := pq.AddTaskWithOptions(ctx, []byte("fallback-retry"), EnqueueOptions{
		RoutingKey: "sticky-worker", RoutingDeadline: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	first, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok || !first.RoutingFallback {
		t.Fatalf("first lease = %+v, ok = %v, err = %v", first, ok, err)
	}
	if err := pq.Nack(ctx, first.Token); err != nil {
		t.Fatal(err)
	}
	awaitTestReadLevel(t, pq, 2)
	retry, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil || !ok || !retry.RoutingFallback || string(retry.Data) != "fallback-retry" {
		t.Fatalf("retry lease = %+v, ok = %v, err = %v", retry, ok, err)
	}
	firstToken, _ := ParseTaskToken(first.Token)
	retryToken, _ := ParseTaskToken(retry.Token)
	// respool 以新 taskID 重写任务，路由元数据必须原样保留。
	if retryToken.TaskID <= firstToken.TaskID {
		t.Fatalf("fallback Nack should respool to a higher task ID: %d -> %d", firstToken.TaskID, retryToken.TaskID)
	}
}

func TestPhysicalQueue_FallbackAckUnblocksContiguousGC(t *testing.T) {
	pq, store := newTestPhysicalQueue(t)
	ctx := context.Background()
	deadline := time.Now().Add(200 * time.Millisecond)
	if err := pq.AddTaskWithOptions(ctx, []byte("sticky-first"), EnqueueOptions{
		RoutingKey: "sticky-worker", RoutingDeadline: deadline,
	}); err != nil {
		t.Fatal(err)
	}
	if err := pq.AddTask(ctx, []byte("shared-second")); err != nil {
		t.Fatal(err)
	}
	second, secondToken, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok || string(second.Data) != "shared-second" {
		t.Fatalf("second row = %+v, ok = %v, err = %v", second, ok, err)
	}
	if err := pq.Ack(ctx, secondToken); err != nil {
		t.Fatal(err)
	}
	if err := pq.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := store.GetQueue(ctx, "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 0 {
		t.Fatalf("ack level advanced before fallback task completed: %d", info.AckLevel)
	}
	time.Sleep(time.Until(deadline) + 10*time.Millisecond)
	first, ok, err := pq.TryPollWithOptions(ctx, PollOptions{})
	if err != nil || !ok || !first.RoutingFallback || string(first.Data) != "sticky-first" {
		t.Fatalf("fallback lease = %+v, ok = %v, err = %v", first, ok, err)
	}
	if err := pq.Ack(ctx, first.Token); err != nil {
		t.Fatal(err)
	}
	if err := pq.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err = store.GetQueue(ctx, "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 2 {
		t.Fatalf("ack level = %d, want 2", info.AckLevel)
	}
}

func TestPhysicalQueue_RoutingKeyLimitUsesUTF8ByteLength(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	maxASCII := strings.Repeat("a", MaxRoutingKeyBytes)
	if err := pq.AddTaskWithOptions(ctx, []byte("accepted"), EnqueueOptions{RoutingKey: maxASCII}); err != nil {
		t.Fatalf("max-length routing key: %v", err)
	}

	tooLongUTF8 := strings.Repeat("界", MaxRoutingKeyBytes/len("界")+1)
	if err := pq.AddTaskWithOptions(ctx, []byte("rejected"), EnqueueOptions{RoutingKey: tooLongUTF8}); !errors.Is(err, ErrInvalidRoutingKey) {
		t.Fatalf("enqueue error = %v", err)
	}
	if _, _, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{tooLongUTF8}}); !errors.Is(err, ErrInvalidRoutingKey) {
		t.Fatalf("poll error = %v", err)
	}
}

func TestPhysicalQueue_KeyedTaskWakesEarliestEligibleWaiter(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	ineligible, err := pq.RegisterWaiterWithOptions(PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil {
		t.Fatal(err)
	}
	eligible, err := pq.RegisterWaiterWithOptions(PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil {
		t.Fatal(err)
	}
	defer ineligible.Cancel()
	defer eligible.Cancel()

	if err := pq.AddTaskWithOptions(ctx, []byte("worker-b-task"), EnqueueOptions{RoutingKey: "worker-b"}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-eligible.Ready():
		result, ok := eligible.Claim()
		if !ok || result.Row == nil || string(result.Row.Data) != "worker-b-task" {
			t.Fatalf("eligible waiter result = %+v, claimed = %v", result, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("eligible waiter did not receive keyed task")
	}

	select {
	case <-ineligible.Ready():
		t.Fatal("ineligible waiter received keyed task")
	default:
	}
}

func TestPhysicalQueue_UnmatchedHeadDoesNotBlockEligibleTask(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()

	if err := pq.AddTaskWithOptions(ctx, []byte("worker-b-first"), EnqueueOptions{RoutingKey: "worker-b"}); err != nil {
		t.Fatal(err)
	}
	if err := pq.AddTaskWithOptions(ctx, []byte("worker-a-second"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}

	leaseA, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(leaseA.Data) != "worker-a-second" {
		t.Fatalf("worker-a lease = %+v, ok = %v", leaseA, ok)
	}

	leaseB, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(leaseB.Data) != "worker-b-first" {
		t.Fatalf("worker-b lease = %+v, ok = %v", leaseB, ok)
	}
}

func TestPhysicalQueue_NackPreservesRoutingEligibility(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()
	if err := pq.AddTaskWithOptions(ctx, []byte("retry-a"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}

	first, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok {
		t.Fatalf("first lease = %+v, ok = %v, err = %v", first, ok, err)
	}
	if err := pq.Nack(ctx, first.Token); err != nil {
		t.Fatal(err)
	}
	awaitTestReadLevel(t, pq, 2)
	if lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("worker-b received targeted redelivery: %+v", lease)
	}

	retry, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok {
		t.Fatalf("retry lease = %+v, ok = %v, err = %v", retry, ok, err)
	}
	if string(retry.Data) != "retry-a" || retry.RoutingKey != "worker-a" {
		t.Fatalf("unexpected retry lease: %+v", retry)
	}
}

func TestPhysicalQueue_OutOfOrderRoutedAckAdvancesGCContiguously(t *testing.T) {
	pq, store := newTestPhysicalQueue(t)
	ctx := context.Background()
	if err := pq.AddTaskWithOptions(ctx, []byte("first-b"), EnqueueOptions{RoutingKey: "worker-b"}); err != nil {
		t.Fatal(err)
	}
	if err := pq.AddTaskWithOptions(ctx, []byte("second-a"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}

	second, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok {
		t.Fatalf("second lease = %+v, ok = %v, err = %v", second, ok, err)
	}
	if err := pq.Ack(ctx, second.Token); err != nil {
		t.Fatal(err)
	}
	if err := pq.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := store.GetQueue(ctx, "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 0 {
		t.Fatalf("ack level advanced across an unacked routed task: %d", info.AckLevel)
	}

	first, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil || !ok {
		t.Fatalf("first lease = %+v, ok = %v, err = %v", first, ok, err)
	}
	if err := pq.Ack(ctx, first.Token); err != nil {
		t.Fatal(err)
	}
	if err := pq.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err = store.GetQueue(ctx, "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 2 {
		t.Fatalf("ack level = %d, want 2", info.AckLevel)
	}
}

func TestPhysicalQueue_VisibilityScanPreservesOutOfOrderAck(t *testing.T) {
	store := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), store, "acked-visibility-q", 0, 20*time.Millisecond, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	ctx := context.Background()

	if err := pq.AddTaskWithOptions(ctx, []byte("blocked-first"), EnqueueOptions{RoutingKey: "worker-b"}); err != nil {
		t.Fatal(err)
	}
	if err := pq.AddTaskWithOptions(ctx, []byte("acked-second"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	second, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok {
		t.Fatalf("second poll = %+v, ok = %v, err = %v", second, ok, err)
	}
	if err := pq.Ack(ctx, second.Token); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)

	if duplicate, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("visibility scan redelivered an acknowledged task: %+v", duplicate)
	}
	first, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}})
	if err != nil || !ok || string(first.Data) != "blocked-first" {
		t.Fatalf("first poll = %+v, ok = %v, err = %v", first, ok, err)
	}
	if err := pq.Ack(ctx, first.Token); err != nil {
		t.Fatal(err)
	}
	if err := pq.RunGCOnce(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := store.GetQueue(ctx, "acked-visibility-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if info.AckLevel != 2 {
		t.Fatalf("ack level = %d, want 2", info.AckLevel)
	}
}

func TestPhysicalQueue_UnkeyedTasksRemainVisibleToEveryPoller(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()
	if err := pq.AddTask(ctx, []byte("shared-a")); err != nil {
		t.Fatal(err)
	}
	if err := pq.AddTask(ctx, []byte("shared-legacy")); err != nil {
		t.Fatal(err)
	}

	lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok || string(lease.Data) != "shared-a" || lease.RoutingKey != "" {
		t.Fatalf("routed poll lease = %+v, ok = %v, err = %v", lease, ok, err)
	}
	row, _, ok, err := pq.TryPoll(ctx)
	if err != nil || !ok || string(row.Data) != "shared-legacy" {
		t.Fatalf("legacy poll row = %+v, ok = %v, err = %v", row, ok, err)
	}
}

func TestPhysicalQueue_MultipleAcceptedKeysUseTaskIDFIFO(t *testing.T) {
	pq, _ := newTestPhysicalQueue(t)
	ctx := context.Background()
	for _, task := range []struct {
		data string
		key  string
	}{
		{data: "first-b", key: "worker-b"},
		{data: "second-a", key: "worker-a"},
	} {
		if err := pq.AddTaskWithOptions(ctx, []byte(task.data), EnqueueOptions{RoutingKey: task.key}); err != nil {
			t.Fatal(err)
		}
	}
	opts := PollOptions{AcceptedRoutingKeys: []string{"worker-a", "worker-b", "worker-a", ""}}
	first, ok, err := pq.TryPollWithOptions(ctx, opts)
	if err != nil || !ok || string(first.Data) != "first-b" {
		t.Fatalf("first lease = %+v, ok = %v, err = %v", first, ok, err)
	}
	second, ok, err := pq.TryPollWithOptions(ctx, opts)
	if err != nil || !ok || string(second.Data) != "second-a" {
		t.Fatalf("second lease = %+v, ok = %v, err = %v", second, ok, err)
	}
}

func TestPhysicalQueue_VisibilityRedeliveryPreservesRoutingEligibility(t *testing.T) {
	store := newTestStore(t)
	pq, err := newPhysicalQueue(context.Background(), store, "routed-timeout-q", 0, 20*time.Millisecond, defaultMaxAttempts, "test-owner")
	if err != nil {
		t.Fatal(err)
	}
	startTestWriterTask(t, pq)
	t.Cleanup(pq.Close)
	ctx := context.Background()

	if err := pq.AddTaskWithOptions(ctx, []byte("timeout-a"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}}); err != nil || !ok {
		t.Fatalf("initial poll ok = %v, err = %v", ok, err)
	}
	time.Sleep(30 * time.Millisecond)
	pq.RunVisibilityScanOnce(ctx)
	awaitTestReadLevel(t, pq, 2)

	if lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-b"}}); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatalf("worker-b received timed-out routed task: %+v", lease)
	}
	lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok || string(lease.Data) != "timeout-a" || lease.RoutingKey != "worker-a" {
		t.Fatalf("redelivery lease = %+v, ok = %v, err = %v", lease, ok, err)
	}
}
