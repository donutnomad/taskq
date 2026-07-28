package matching

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errResolveUnavailable 模拟 DB 抖动：ResolvePartitions 查询失败。
var errResolveUnavailable = errors.New("simulated db outage")

// flakyConfigStore 可按开关让 ResolvePartitions 失败，其余操作透传。
// 用于复现「matching 节点重启（内存配置缓存为空）恰逢 DB 抖动」的窗口。
type flakyConfigStore struct {
	Store
	failResolve atomic.Bool
}

func (s *flakyConfigStore) ResolvePartitions(ctx context.Context, queueName string) (int, int, error) {
	if s.failResolve.Load() {
		return 0, 0, errResolveUnavailable
	}
	return s.Store.ResolvePartitions(ctx, queueName)
}

func newFlakyConfigStore(t *testing.T) *flakyConfigStore {
	t.Helper()
	return &flakyConfigStore{Store: newTestSQLiteStore(t)}
}

func requireCode(t *testing.T, err error, want codes.Code, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected %s, got success", what, want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("%s: expected %s, got %s (%v)", what, want, got, err)
	}
}

// simulateColdRestart 清掉本节点的分区配置缓存，等价于进程刚重启、内存缓存为空。
func simulateColdRestart(srv *Server, queueName string) {
	srv.mgr.invalidateConfigCache(queueName)
}

// TestPartitionConfig_NoCacheAndDBErrorRefusesWrites 是本次修复的核心回归：
// 无缓存 + DB 出错时，写闸门必须拒绝服务（Unavailable），绝不能退回 defaultPartitions=4。
// 旧行为会用 4 放行对 p2/p3 的写入——而该 queue 已被安全缩容到 2，
// DB 恢复后 p2/p3 失去 owner，其中的任务永久孤立。
func TestPartitionConfig_NoCacheAndDBErrorRefusesWrites(t *testing.T) {
	store := newFlakyConfigStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"shrunk-q": 2})
	client := newTestClient(t, addr)

	store.failResolve.Store(true)
	simulateColdRestart(srv, "shrunk-q")

	// p3 落在 defaultPartitions=4 的范围内但超出真实 write=2：旧的 fallback 会放行写入。
	_, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName: "shrunk-q", PartitionId: 3, Data: []byte("must-not-be-orphaned"),
	})
	requireCode(t, err, codes.Unavailable, "AddTask to retired partition 3 with no cached config")

	// 合法分区同样必须拒绝：配置不可知时无法证明 p0 仍可写。
	_, err = client.AddTask(context.Background(), &AddTaskRequest{
		QueueName: "shrunk-q", PartitionId: 0, Data: []byte("x"),
	})
	requireCode(t, err, codes.Unavailable, "AddTask to partition 0 with no cached config")

	// 写入既然被拒，DB 恢复后不应有任何任务残留在已退役的 p3。
	store.failResolve.Store(false)
	if _, _, ok := pollOne(t, client, "shrunk-q", 0); ok {
		t.Fatal("no task should have been accepted while config was unknown")
	}
}

// TestPartitionConfig_NoCacheAndDBErrorRefusesReads 覆盖读侧闸门与 PollQueue。
func TestPartitionConfig_NoCacheAndDBErrorRefusesReads(t *testing.T) {
	store := newFlakyConfigStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"read-q": 2})
	client := newTestClient(t, addr)

	store.failResolve.Store(true)
	simulateColdRestart(srv, "read-q")

	_, err := client.PollTask(context.Background(), &PollTaskRequest{QueueName: "read-q", PartitionId: 0})
	requireCode(t, err, codes.Unavailable, "PollTask with no cached config")

	_, err = client.PollQueue(context.Background(), &PollQueueRequest{
		QueueName: "read-q", PartitionIds: []int32{0, 1},
	})
	requireCode(t, err, codes.Unavailable, "PollQueue with no cached config")
}

// TestPartitionConfig_NoCacheAndDBErrorGetQueueInfoUnavailable 确认 frontend 拿不到
// 一个被放大的猜测快照，而是 Unavailable（frontend 侧据此保留旧快照）。
func TestPartitionConfig_NoCacheAndDBErrorGetQueueInfoUnavailable(t *testing.T) {
	store := newFlakyConfigStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"info-q": 2})
	client := newTestClient(t, addr)

	store.failResolve.Store(true)
	simulateColdRestart(srv, "info-q")

	_, err := client.GetQueueInfo(context.Background(), &GetQueueInfoRequest{QueueName: "info-q"})
	requireCode(t, err, codes.Unavailable, "GetQueueInfo with no cached config")
}

// TestPartitionConfig_NoCacheAndDBErrorAckNotSilentlyIdempotent 确认 S4 的迟到 Ack 幂等
// 判定不会在配置不可知时误判：不能把一个可能仍在服务的分区上的 Ack 当作「退役分区」吞掉。
func TestPartitionConfig_NoCacheAndDBErrorAckNotSilentlyIdempotent(t *testing.T) {
	store := newFlakyConfigStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"ack-q": 2})
	client := newTestClient(t, addr)

	addTask(t, client, "ack-q", 0, "payload")
	token, _, ok := pollOne(t, client, "ack-q", 0)
	if !ok {
		t.Fatal("expected a task to poll")
	}

	store.failResolve.Store(true)
	simulateColdRestart(srv, "ack-q")

	_, err := client.AckTask(context.Background(), &AckTaskRequest{
		QueueName: "ack-q", PartitionId: 0, Token: token,
	})
	requireCode(t, err, codes.Unavailable, "AckTask with no cached config")

	_, err = client.NackTask(context.Background(), &NackTaskRequest{
		QueueName: "ack-q", PartitionId: 0, Token: token,
	})
	requireCode(t, err, codes.Unavailable, "NackTask with no cached config")

	// DB 恢复后 Ack 必须仍然生效（此前既没被吞掉，也没被错误地当成幂等成功）。
	store.failResolve.Store(false)
	if _, err := client.AckTask(context.Background(), &AckTaskRequest{
		QueueName: "ack-q", PartitionId: 0, Token: token,
	}); err != nil {
		t.Fatalf("ack after db recovery: %v", err)
	}
}

// TestPartitionConfig_StaleCacheServesDuringDBError 验证 stale-while-error：
// 有缓存（哪怕早已过 TTL）时继续用旧值正常服务。旧值是该 queue 真实存在过的配置，
// 不会凭空放大到从未配置过的分区号，因此比拒绝服务更优。
func TestPartitionConfig_StaleCacheServesDuringDBError(t *testing.T) {
	store := newFlakyConfigStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"stale-q": 2})
	client := newTestClient(t, addr)

	// 预热缓存，然后把它人为置为「早已过期」，避免测试等待 partitionConfigTTL。
	addTask(t, client, "stale-q", 0, "warm-up")
	srv.mgr.configMu.Lock()
	cached, ok := srv.mgr.configCache["stale-q"]
	if !ok {
		srv.mgr.configMu.Unlock()
		t.Fatal("expected partition config to be cached after AddTask")
	}
	cached.at = time.Now().Add(-time.Hour)
	srv.mgr.configCache["stale-q"] = cached
	srv.mgr.configMu.Unlock()

	store.failResolve.Store(true)

	// 过期缓存仍被采用：合法分区照常服务。
	addTask(t, client, "stale-q", 1, "served-from-stale-cache")
	if _, data, ok := pollOne(t, client, "stale-q", 1); !ok || data != "served-from-stale-cache" {
		t.Fatalf("expected stale cache to keep serving, got ok=%v data=%q", ok, data)
	}

	// 关键：用的是 stale 值 2，而不是 defaultPartitions=4。
	// 若退回 4，对 p2 的写会被放行（FailedPrecondition 才是正确结果）。
	_, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName: "stale-q", PartitionId: 2, Data: []byte("beyond-stale-write-count"),
	})
	requireCode(t, err, codes.FailedPrecondition, "AddTask beyond stale write count")

	// GetQueueInfo 同样返回 stale 值而非报错。
	info, err := client.GetQueueInfo(context.Background(), &GetQueueInfoRequest{QueueName: "stale-q"})
	if err != nil {
		t.Fatalf("GetQueueInfo with stale cache: %v", err)
	}
	if info.NumReadPartitions != 2 || info.NumWritePartitions != 2 {
		t.Fatalf("expected stale config read=2 write=2, got read=%d write=%d",
			info.NumReadPartitions, info.NumWritePartitions)
	}
}

// TestPartitionConfig_ReconcileSkipsQueueOnResolveError 验证 reconcile 遇到 resolve 错误时
// 保持该 queue 的分区归属现状：绝不能因为读不到配置就卸载正在服务的分区。
func TestPartitionConfig_ReconcileSkipsQueueOnResolveError(t *testing.T) {
	store := newFlakyConfigStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"recon-q": 2})
	client := newTestClient(t, addr)

	addTask(t, client, "recon-q", 0, "survives-reconcile")

	loaded := func() []string {
		srv.mgr.mu.RLock()
		defer srv.mgr.mu.RUnlock()
		keys := make([]string, 0, len(srv.mgr.partitions))
		for key, handle := range srv.mgr.partitions {
			if !handle.isClosed() {
				keys = append(keys, key)
			}
		}
		return keys
	}
	before := len(loaded())
	if before == 0 {
		t.Fatal("expected at least one loaded partition before reconcile")
	}

	store.failResolve.Store(true)
	simulateColdRestart(srv, "recon-q")
	srv.mgr.reconcile()

	if after := len(loaded()); after != before {
		t.Fatalf("reconcile unloaded partitions on resolve error: before=%d after=%d", before, after)
	}

	// 分区从未被卸载/关闭，DB 恢复后立即可服务，且任务仍在。
	store.failResolve.Store(false)
	if _, data, ok := pollOne(t, client, "recon-q", 0); !ok || data != "survives-reconcile" {
		t.Fatalf("expected partition to keep serving its task, got ok=%v data=%q", ok, data)
	}
}
