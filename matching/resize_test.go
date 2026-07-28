package matching

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fastResizeWaits 把 ResizePartitions 的等待参数调小，让测试不必真等生产级的 5s 屏障。
// 生产默认值（defaultResize*）不受影响。
func fastResizeWaits(t *testing.T, srv *Server, drainTimeout time.Duration) {
	t.Helper()
	srv.mgr.resizeQuiesceDelay = 150 * time.Millisecond
	srv.mgr.resizeStablePeriod = 200 * time.Millisecond
	srv.mgr.resizeDrainTimeout = drainTimeout
}

func addTask(t *testing.T, client MatchingServiceClient, queueName string, partitionID int, data string) {
	t.Helper()
	if _, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName:   queueName,
		PartitionId: int32(partitionID),
		Data:        []byte(data),
	}); err != nil {
		t.Fatalf("add task to %s/%d: %v", queueName, partitionID, err)
	}
}

// pollOne 从指定分区拉取一条任务，返回 token 与 data；无任务返回 ok=false。
func pollOne(t *testing.T, client MatchingServiceClient, queueName string, partitionID int) (token string, data string, ok bool) {
	t.Helper()
	resp, err := client.PollTask(context.Background(), &PollTaskRequest{
		QueueName:   queueName,
		PartitionId: int32(partitionID),
	})
	if err != nil {
		t.Fatalf("poll %s/%d: %v", queueName, partitionID, err)
	}
	if resp.Empty {
		return "", "", false
	}
	return resp.Token, string(resp.Data), true
}

// consumeAll 轮询 [0, numPartitions) 并 ack 全部可见任务，返回消费到的 data 集合。
func consumeAll(t *testing.T, client MatchingServiceClient, queueName string, numPartitions int) []string {
	t.Helper()
	var got []string
	for partitionID := range numPartitions {
		for {
			token, data, ok := pollOne(t, client, queueName, partitionID)
			if !ok {
				break
			}
			got = append(got, data)
			if _, err := client.AckTask(context.Background(), &AckTaskRequest{
				QueueName:   queueName,
				PartitionId: int32(partitionID),
				Token:       token,
			}); err != nil {
				t.Fatalf("ack %s/%d: %v", queueName, partitionID, err)
			}
		}
	}
	return got
}

func resolveCounts(t *testing.T, store Store, queueName string) (read, write int) {
	t.Helper()
	read, write, err := store.ResolvePartitions(context.Background(), queueName)
	if err != nil {
		t.Fatalf("resolve partitions %q: %v", queueName, err)
	}
	return read, write
}

// waitWriteCount 等待 store 中的 write 分区数变为期望值（编排器第 1 步生效）。
func waitWriteCount(t *testing.T, store Store, queueName string, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, write := resolveCounts(t, store, queueName); write == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("write partitions did not reach %d in time", want)
}

// ---- 扩容 ----

func TestResizePartitions_GrowMakesNewPartitionsUsable(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9801")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9801", map[string]int{"grow-q": 2})
	fastResizeWaits(t, srv, 10*time.Second)
	client := newTestClient(t, addr)

	// 扩容前：只有 p0/p1 可写。
	addTask(t, client, "grow-q", 0, "before-0")
	addTask(t, client, "grow-q", 1, "before-1")

	// 扩容过程中持续入队：模拟 frontend 按「当前 write」选分区，任何时刻都合法。
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var duringMu sync.Mutex
	during := 0
	wg.Go(func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			r, w, err := store.ResolvePartitions(context.Background(), "grow-q")
			if err != nil || w <= 0 || w > r {
				t.Errorf("bad partition counts during grow: read=%d write=%d err=%v", r, w, err)
				return
			}
			partitionID := i % w
			if _, err := client.AddTask(context.Background(), &AddTaskRequest{
				QueueName:   "grow-q",
				PartitionId: int32(partitionID),
				Data:        fmt.Appendf(nil, "during-%d", i),
			}); err != nil {
				// 快照与服务端闸门之间的竞态最坏是瞬时 FailedPrecondition，重试即可；
				// 其它错误说明扩容协议有问题。
				if status.Code(err) != codes.FailedPrecondition {
					t.Errorf("enqueue during grow: %v", err)
					return
				}
				continue
			}
			duringMu.Lock()
			during++
			duringMu.Unlock()
			i++
			time.Sleep(5 * time.Millisecond)
		}
	})

	if err := srv.ResizePartitions(context.Background(), "grow-q", 4); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("grow: %v", err)
	}
	close(stop)
	wg.Wait()

	read, write := resolveCounts(t, store, "grow-q")
	if read != 4 || write != 4 {
		t.Fatalf("after grow want read=4 write=4, got read=%d write=%d", read, write)
	}

	// 新分区可写可读。
	addTask(t, client, "grow-q", 3, "after-3")

	duringMu.Lock()
	wantTotal := 2 + during + 1
	duringMu.Unlock()

	got := consumeAll(t, client, "grow-q", 4)
	if len(got) != wantTotal {
		t.Fatalf("want %d tasks consumed after grow, got %d", wantTotal, len(got))
	}
	if !containsString(got, "after-3") {
		t.Fatal("task written to the newly added partition 3 was not consumable")
	}
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// ---- 缩容成功 ----

func TestResizePartitions_ShrinkWhenHighPartitionsEmpty(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9802")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9802", map[string]int{"shrink-q": 4})
	fastResizeWaits(t, srv, 10*time.Second)
	client := newTestClient(t, addr)

	// 只往低位分区写，高位分区无数据。
	addTask(t, client, "shrink-q", 0, "keep-0")
	addTask(t, client, "shrink-q", 1, "keep-1")

	if err := srv.ResizePartitions(context.Background(), "shrink-q", 2); err != nil {
		t.Fatalf("shrink: %v", err)
	}

	read, write := resolveCounts(t, store, "shrink-q")
	if read != 2 || write != 2 {
		t.Fatalf("after shrink want read=2 write=2, got read=%d write=%d", read, write)
	}

	// 低位分区继续正常工作。
	addTask(t, client, "shrink-q", 1, "post-1")
	got := consumeAll(t, client, "shrink-q", 2)
	if len(got) != 3 {
		t.Fatalf("want 3 tasks consumed after shrink, got %d: %v", len(got), got)
	}
}

// ---- 缩容被未 ack 的任务阻塞 ----

func TestResizePartitions_ShrinkBlockedByUnackedTask(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9803")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9803", map[string]int{"blocked-q": 4})
	fastResizeWaits(t, srv, time.Second)
	client := newTestClient(t, addr)

	// 高位分区 3 上留一条已投递未 ack 的任务，ackLevel 被钉住。
	addTask(t, client, "blocked-q", 3, "stuck")
	token, data, ok := pollOne(t, client, "blocked-q", 3)
	if !ok || data != "stuck" {
		t.Fatalf("expected to poll the stuck task, ok=%v data=%q", ok, data)
	}

	err := srv.ResizePartitions(context.Background(), "blocked-q", 2)
	if err == nil {
		t.Fatal("expected shrink to fail while partition 3 still has an unacked task")
	}
	if !errors.Is(err, ErrPartitionDrainTimeout) {
		t.Fatalf("expected ErrPartitionDrainTimeout, got %v", err)
	}
	if !strings.Contains(err.Error(), "partition 3") {
		t.Fatalf("drain error must report the blocking partition, got: %v", err)
	}

	// 关键：read 没有被减小，高位分区仍被 own，数据不孤立。
	read, write := resolveCounts(t, store, "blocked-q")
	if read != 4 {
		t.Fatalf("read partitions must stay 4 while drain is incomplete, got %d", read)
	}
	if write != 2 {
		t.Fatalf("write partitions should already be 2 after step 1, got %d", write)
	}

	// 分区 3 仍可读可 ack。
	if _, err := client.AckTask(context.Background(), &AckTaskRequest{
		QueueName: "blocked-q", PartitionId: 3, Token: token,
	}); err != nil {
		t.Fatalf("ack on the not-yet-retired partition 3: %v", err)
	}

	// ack 之后再缩容成功。DB ack_level 由 GC（partitionGCInterval=5s）持久化，
	// 因此这里必须给足够长的 drain 超时。
	srv.mgr.resizeDrainTimeout = 30 * time.Second
	if err := srv.ResizePartitions(context.Background(), "blocked-q", 2); err != nil {
		t.Fatalf("shrink after ack: %v", err)
	}
	read, write = resolveCounts(t, store, "blocked-q")
	if read != 2 || write != 2 {
		t.Fatalf("after successful shrink want read=2 write=2, got read=%d write=%d", read, write)
	}
}

// ---- 缩容期间高位写被拒 ----

func TestResizePartitions_ShrinkRejectsWritesToHighPartitions(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9804")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9804", map[string]int{"gate-q": 4})
	fastResizeWaits(t, srv, 3*time.Second)
	client := newTestClient(t, addr)

	// 让 drain 卡住，把 (read=4, write=2) 这个中间态维持足够长，便于观察写闸门。
	addTask(t, client, "gate-q", 3, "stuck")
	if _, _, ok := pollOne(t, client, "gate-q", 3); !ok {
		t.Fatal("expected to poll the stuck task")
	}

	resizeErr := make(chan error, 1)
	go func() {
		resizeErr <- srv.ResizePartitions(context.Background(), "gate-q", 2)
	}()

	// 第 1 步（只减 write）生效后，高位分区写必须被硬拒。
	waitWriteCount(t, store, "gate-q", 2)

	var lastErr error
	deadline := time.Now().Add(2 * time.Second)
	rejected := false
	for time.Now().Before(deadline) {
		_, lastErr = client.AddTask(context.Background(), &AddTaskRequest{
			QueueName: "gate-q", PartitionId: 3, Data: []byte("late-write"),
		})
		if lastErr != nil && status.Code(lastErr) == codes.FailedPrecondition {
			rejected = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !rejected {
		t.Fatalf("write to partition 3 must be rejected with FailedPrecondition, got %v", lastErr)
	}

	// 低位分区仍然可写。
	addTask(t, client, "gate-q", 1, "still-ok")

	if err := <-resizeErr; !errors.Is(err, ErrPartitionDrainTimeout) {
		t.Fatalf("expected drain timeout, got %v", err)
	}
	if read, _ := resolveCounts(t, store, "gate-q"); read != 4 {
		t.Fatalf("read must stay 4 after a failed shrink, got %d", read)
	}
}

// ---- 迟到 Ack/Nack 幂等 ----

func TestResizePartitions_LateAckOnRetiredPartitionIsIdempotent(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9805")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9805", map[string]int{"late-q": 4})
	fastResizeWaits(t, srv, 30*time.Second)
	client := newTestClient(t, addr)

	// 在高位分区消费并 ack 一条任务，拿到一个真实 token。
	addTask(t, client, "late-q", 3, "done")
	token, _, ok := pollOne(t, client, "late-q", 3)
	if !ok {
		t.Fatal("expected to poll the task from partition 3")
	}
	if _, err := client.AckTask(context.Background(), &AckTaskRequest{
		QueueName: "late-q", PartitionId: 3, Token: token,
	}); err != nil {
		t.Fatalf("first ack: %v", err)
	}

	if err := srv.ResizePartitions(context.Background(), "late-q", 2); err != nil {
		t.Fatalf("shrink: %v", err)
	}

	// 缩容后指向已退役分区 3 的迟到重试必须幂等成功，而不是 InvalidArgument。
	if _, err := client.AckTask(context.Background(), &AckTaskRequest{
		QueueName: "late-q", PartitionId: 3, Token: token,
	}); err != nil {
		t.Fatalf("late ack on retired partition must be idempotent success, got %v", err)
	}
	if _, err := client.NackTask(context.Background(), &NackTaskRequest{
		QueueName: "late-q", PartitionId: 3, Token: token,
	}); err != nil {
		t.Fatalf("late nack on retired partition must be idempotent success, got %v", err)
	}

	// 明显非法的分区号仍然被拒。
	if _, err := client.AckTask(context.Background(), &AckTaskRequest{
		QueueName: "late-q", PartitionId: maxHistoricalPartitionID, Token: fmt.Sprintf("%d:1", maxHistoricalPartitionID),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("out-of-range partition must stay InvalidArgument, got %v", err)
	}
}

// ---- drain 判定 ----

func TestIsPartitionDrained(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9806")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9806", map[string]int{"drain-q": 4})
	client := newTestClient(t, addr)
	ctx := context.Background()

	// 从未物化的分区（queue_info 无行，store 返回 sql.ErrNoRows）视为已排空。
	drained, err := srv.mgr.isPartitionDrained(ctx, "never-materialized-q", 7)
	if err != nil {
		t.Fatalf("drain check on unmaterialized partition: %v", err)
	}
	if !drained {
		t.Fatal("a partition that was never materialized must count as drained")
	}

	// 有未 ack 的任务 → 未排空。
	addTask(t, client, "drain-q", 0, "outstanding")
	if _, _, ok := pollOne(t, client, "drain-q", 0); !ok {
		t.Fatal("expected to poll the task")
	}
	drained, err = srv.mgr.isPartitionDrained(ctx, "drain-q", 0)
	if err != nil {
		t.Fatalf("drain check: %v", err)
	}
	if drained {
		t.Fatal("partition with an outstanding task must not count as drained")
	}
}

func TestResizePartitions_Validation(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9807")
	srv, _ := startTestServer(t, store, resolver, "127.0.0.1:9807", map[string]int{"valid-q": 2})
	fastResizeWaits(t, srv, time.Second)
	ctx := context.Background()

	if err := srv.ResizePartitions(ctx, "valid-q", 0); err == nil {
		t.Fatal("target < 1 must be rejected")
	}
	if err := srv.ResizePartitions(ctx, "", 2); err == nil {
		t.Fatal("empty queue name must be rejected")
	}
	// target 等于当前值是 no-op，不应触碰任何屏障。
	start := time.Now()
	if err := srv.ResizePartitions(ctx, "valid-q", 2); err != nil {
		t.Fatalf("no-op resize: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("no-op resize should return immediately, took %s", elapsed)
	}
}
