package matching

import (
	"context"
	"sync"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
)

// slowReleaseLockStore 复现 DB 抖动：Release（Manager.Offline 的同步 DB 调用）每次耗时
// releaseDelay。真实环境里 dbleader 侧对 Release 有 2s 超时，卸载 N 个分区就是 N×2s。
type slowReleaseLockStore struct {
	stepDownLockStore
	releaseDelay time.Duration

	mu       sync.Mutex
	releases int
}

func (s *slowReleaseLockStore) Release(ctx context.Context, lock string, nodeIP string) error {
	time.Sleep(s.releaseDelay)
	s.mu.Lock()
	s.releases++
	s.mu.Unlock()
	return s.stepDownLockStore.Release(ctx, lock, nodeIP)
}

func (s *slowReleaseLockStore) releaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releases
}

// newReconcileTestManager 构造一个只做 partition 管理、不起 gRPC 的 partitionManager。
// queueName 会被写入分区配置，否则 reconcile 会因 resolve 失败把该 queue 判为 skipped
// 而跳过卸载（保持现状语义），测试就压不到卸载路径。
func newReconcileTestManager(t *testing.T, lockStore dbleader.LockStore, resolver *stubResolver, selfAddr, queueName string, numPartitions int) *partitionManager {
	t.Helper()
	store := newTestSQLiteStore(t)
	if err := store.SetPartitions(context.Background(), queueName, numPartitions, numPartitions); err != nil {
		t.Fatal(err)
	}
	pm := newPartitionManager(
		context.Background(), store, lockStore, selfAddr, "owner-1",
		resolver, 30*time.Second, 0,
	)
	t.Cleanup(pm.close)
	return pm
}

// A：reconcile 卸载多个分区时，close() 必须在 pm.mu 之外执行。
//
// 红/绿判据：修复前 reconcile 持写锁逐个串行 close，N 个慢分区 → 写锁被占 N×releaseDelay，
// 期间任何走 pm.mu 的 RPC 路径（这里用 getOrLoadPartition 的读锁快路径代表）都被堵死。
// 修复后锁内只摘除、锁外并发 close，RPC 几乎不受影响。
func TestReconcileDoesNotHoldLockAcrossPartitionClose(t *testing.T) {
	const (
		numPartitions = 6
		releaseDelay  = 300 * time.Millisecond
	)

	lockStore := &slowReleaseLockStore{releaseDelay: releaseDelay}
	// 自己不拥有任何分区（stubResolver 只有一个成员且地址不等于 selfAddr），
	// 这样 reconcile 会把已加载的分区全部判为「不再属于本节点」并卸载。
	resolver := newStubResolver("127.0.0.1:19999")
	pm := newReconcileTestManager(t, lockStore, resolver, "127.0.0.1:18888", "unload-q", numPartitions)

	// 预先塞入 N 个已加载分区（直接建 handle，避开 owner 校验）。
	pm.mu.Lock()
	for i := range numPartitions {
		key := partitionKey("unload-q", i)
		h := newPartitionHandle(pm.store, pm.lockStore, "unload-q", i, pm.visTimeout, pm.maxAttempts, pm.leaseOwner)
		h.start()
		pm.partitions[key] = h
	}
	pm.mu.Unlock()

	// 让各 handle 真正拿到 leadership，否则 Offline 不会走到 Release。
	time.Sleep(400 * time.Millisecond)

	reconcileDone := make(chan struct{})
	go func() {
		defer close(reconcileDone)
		pm.reconcile()
	}()

	// 等 reconcile 进入卸载阶段（首个 Release 已开始阻塞）。
	time.Sleep(150 * time.Millisecond)

	// 模拟并发 RPC：取 pm.mu 读锁。修复前这里会被写锁堵住整个卸载时长。
	start := time.Now()
	pm.mu.RLock()
	_ = len(pm.partitions)
	pm.mu.RUnlock()
	lockWait := time.Since(start)

	// 单个 close 的时长是 releaseDelay；只要 RPC 等待远小于它，就说明没有跨 close 持锁。
	if lockWait > releaseDelay/2 {
		t.Fatalf("RPC blocked on pm.mu for %v during reconcile unload; "+
			"expected close() to run outside the lock", lockWait)
	}

	select {
	case <-reconcileDone:
	case <-time.After(10 * time.Second):
		t.Fatal("reconcile did not finish")
	}

	// 卸载确实发生了（否则上面的「没被阻塞」毫无意义）。
	pm.mu.RLock()
	remaining := len(pm.partitions)
	pm.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected all partitions unloaded, %d remain", remaining)
	}
	if got := lockStore.releaseCount(); got == 0 {
		t.Fatal("expected slow Release to be exercised, got 0 calls")
	}
	t.Logf("rpc lock wait=%v, releases=%d", lockWait, lockStore.releaseCount())
}

// A：close() 是整体关停，必须等所有分区关完再返回，但同样不应持锁串行。
func TestPartitionManagerCloseWaitsForAllPartitionsWithoutSerialLock(t *testing.T) {
	const (
		numPartitions = 6
		releaseDelay  = 300 * time.Millisecond
	)

	lockStore := &slowReleaseLockStore{releaseDelay: releaseDelay}
	resolver := newStubResolver("127.0.0.1:19999")
	store := newTestSQLiteStore(t)
	pm := newPartitionManager(
		context.Background(), store, lockStore, "127.0.0.1:18888", "owner-1",
		resolver, 30*time.Second, 0,
	)

	pm.mu.Lock()
	handles := make([]*partitionHandle, 0, numPartitions)
	for i := range numPartitions {
		h := newPartitionHandle(pm.store, pm.lockStore, "close-q", i, pm.visTimeout, pm.maxAttempts, pm.leaseOwner)
		h.start()
		handles = append(handles, h)
		pm.partitions[partitionKey("close-q", i)] = h
	}
	pm.mu.Unlock()

	time.Sleep(400 * time.Millisecond)

	start := time.Now()
	pm.close()
	elapsed := time.Since(start)

	// 并发关停：总时长应接近单个 releaseDelay，而非 N 倍。
	if elapsed > time.Duration(numPartitions)*releaseDelay {
		t.Fatalf("close() took %v, expected concurrent shutdown well under %v",
			elapsed, time.Duration(numPartitions)*releaseDelay)
	}

	// 整体关停语义：返回后每个分区都必须已关闭。
	for i, h := range handles {
		if !h.isClosed() {
			t.Fatalf("partition %d still open after close() returned", i)
		}
	}
	pm.mu.RLock()
	remaining := len(pm.partitions)
	pm.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected partitions map drained, %d remain", remaining)
	}
	t.Logf("close() elapsed=%v for %d partitions", elapsed, numPartitions)
}
