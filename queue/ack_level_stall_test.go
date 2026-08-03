package queue

import (
	"context"
	"testing"
	"time"
)

// B-1：滞留在 redelivery 里的 unroutable 任务能被定位到（此前完全静默）。
func TestAckLevelBlockerReportsStalledRedelivery(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	// 一条定向到 worker-a、无 RoutingDeadline 的任务 + 三条普通任务。
	if err := pq.AddTaskWithOptions(ctx, []byte("keyed"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"t2", "t3", "t4"} {
		if err := pq.AddTask(ctx, []byte(d)); err != nil {
			t.Fatal(err)
		}
	}

	// 取出 keyed 任务再原地退回，使其落入 redelivery。
	lease, ok, err := pq.TryPollWithOptions(ctx, PollOptions{AcceptedRoutingKeys: []string{"worker-a"}})
	if err != nil || !ok {
		t.Fatalf("poll keyed task: ok=%v err=%v", ok, err)
	}
	if err := pq.ReturnLease(ctx, lease.Token); err != nil {
		t.Fatal(err)
	}

	// 之后只有不订阅 worker-a 的 poller，keyed 任务再也不会被取走。
	for range 3 {
		l, ok, err := pq.TryPollWithOptions(ctx, PollOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if err := pq.Ack(ctx, l.Token); err != nil {
			t.Fatal(err)
		}
	}

	if got := pq.runtime.ackLevel(); got != 0 {
		t.Fatalf("ack level should stay pinned at 0, got %d", got)
	}

	blocker := pq.runtime.state.ackLevelBlocker(time.Now())
	if blocker.taskID != 1 {
		t.Fatalf("expected task 1 to block ack level, got %d", blocker.taskID)
	}
	if blocker.state != "redelivery" {
		t.Fatalf("expected blocker state redelivery, got %q", blocker.state)
	}
	if blocker.routingKey != "worker-a" {
		t.Fatalf("expected blocker routing key worker-a, got %q", blocker.routingKey)
	}
}

// B-2：钉住 ackLevel 的根因不是 redelivery 列表本身。
// 一条从未被任何 poller 取走的 keyed 任务同样把水位钉死，此时 redelivery 为空 ——
// 这说明「把 redelivery 项丢掉/重写」并不能解除钉住，可观测性才是正确的落点。
func TestAckLevelBlockerReportsNeverConsumedKeyedTask(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	if err := pq.AddTaskWithOptions(ctx, []byte("keyed"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"t2", "t3", "t4"} {
		if err := pq.AddTask(ctx, []byte(d)); err != nil {
			t.Fatal(err)
		}
	}

	// 从头到尾都不 poll worker-a。
	for range 3 {
		l, ok, err := pq.TryPollWithOptions(ctx, PollOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if err := pq.Ack(ctx, l.Token); err != nil {
			t.Fatal(err)
		}
	}

	if got := pq.runtime.ackLevel(); got != 0 {
		t.Fatalf("ack level should stay pinned at 0, got %d", got)
	}

	state := pq.runtime.state
	state.mu.Lock()
	pending := len(state.redelivery)
	state.mu.Unlock()
	if pending != 0 {
		t.Fatalf("expected empty redelivery, got %d", pending)
	}

	blocker := state.ackLevelBlocker(time.Now())
	if blocker.taskID != 1 || blocker.state != "unconsumed" {
		t.Fatalf("expected task 1 unconsumed, got task %d state %q", blocker.taskID, blocker.state)
	}
}

// B-3：一切正常推进时不误报。
func TestAckLevelBlockerSilentWhenNothingBlocks(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)
	ctx := context.Background()

	for _, d := range []string{"t1", "t2"} {
		if err := pq.AddTask(ctx, []byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		l, ok, err := pq.TryPollWithOptions(ctx, PollOptions{})
		if err != nil || !ok {
			t.Fatalf("poll: ok=%v err=%v", ok, err)
		}
		if err := pq.Ack(ctx, l.Token); err != nil {
			t.Fatal(err)
		}
	}

	if got := pq.runtime.ackLevel(); got != 2 {
		t.Fatalf("expected ack level 2, got %d", got)
	}
	if blocker := pq.runtime.state.ackLevelBlocker(time.Now()); blocker.state != "" {
		t.Fatalf("expected no blocker, got %+v", blocker)
	}
}

// B-4：GC 侧的停滞检测在水位推进时不告警、在长期停滞时告警一次。
func TestGCReportsAckLevelStallOnlyAfterThreshold(t *testing.T) {
	store := newTestStore(t)
	pq := newRespoolTestQueue(t, store, time.Minute, defaultMaxAttempts)

	if err := pq.AddTaskWithOptions(context.Background(), []byte("keyed"), EnqueueOptions{RoutingKey: "worker-a"}); err != nil {
		t.Fatal(err)
	}

	gc := pq.gc
	// 首轮只是建立基线，不应判定为停滞。
	gc.reportAckLevelStall(pq.runtime.ackLevel())
	if gc.warned {
		t.Fatal("first observation must not warn")
	}

	// 把首次观察时间推到阈值之前，模拟长期停滞。
	gc.lastAdvance = time.Now().Add(-2 * ackLevelStallWarnAfter)
	gc.reportAckLevelStall(pq.runtime.ackLevel())
	if !gc.warned {
		t.Fatal("expected stall warning after threshold")
	}

	// 已告警过就不再刷屏。
	gc.warned = false
	gc.lastAdvance = time.Now()
	gc.reportAckLevelStall(pq.runtime.ackLevel())
	if gc.warned {
		t.Fatal("must not warn again before threshold elapses")
	}

	// 水位推进后重置停滞计时。
	gc.lastAdvance = time.Now().Add(-2 * ackLevelStallWarnAfter)
	gc.reportAckLevelStall(pq.runtime.ackLevel() + 5)
	if gc.warned {
		t.Fatal("advancing ack level must reset the stall timer")
	}
}
