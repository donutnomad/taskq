package matching

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
)

const (
	partitionStartTimeout           = 1500 * time.Millisecond
	partitionGCInterval             = 5 * time.Second
	partitionVisibilityScanInterval = time.Second
	// partitionConfigTTL 是分区配置（read/write 分区数、已知 queue 列表）缓存的存活时间。
	// 全集群一致、有上界：matching 与 frontend 共用同一常量，缩容静默期以它为基准（见 S4）。
	partitionConfigTTL = 5 * time.Second
)

// partitionAccess 区分请求是写入（AddTask）还是读取（Poll/Ack/Nack），
// 决定 partitionID 按 numWrite 还是 numRead 校验（读写闸门分裂，缩容安全核心）。
type partitionAccess int

const (
	accessRead  partitionAccess = iota // Poll/PollQueue/Ack/Nack：放行 [0, numRead)
	accessWrite                        // AddTask：硬拒绝 partID >= numWrite
)

// partitionKey 返回用于一致性哈希查找的 key
func partitionKey(queueName string, partitionID int) string {
	return fmt.Sprintf("%s/%d", queueName, partitionID)
}

type partitionManager struct {
	mu          sync.RWMutex
	store       queue.Store
	lockStore   dbleader.LockStore
	selfAddr    string // 本节点广播地址
	leaseOwner  string // DB lease 标识
	resolver    membership.ServiceResolver
	visTimeout  time.Duration
	maxAttempts int
	partitions  map[string]*partitionHandle // key = partitionKey(...)
	ctx         context.Context
	cancel      context.CancelFunc

	// configCache 缓存每个 queue 的 read/write 分区数，短 TTL 过期重查（store-backed resolver）。
	configMu    sync.Mutex
	configCache map[string]partitionCounts

	// queueListCache 缓存已知 queue 名列表（供 reconcile 枚举），短 TTL。
	queueListMu    sync.Mutex
	queueListCache []string
	queueListAt    time.Time

	// 以下三个时长是 ResizePartitions 编排的等待参数，生产使用 default* 常量，
	// 仅测试为了跑得快才注入更小值。生产环境不要调小：它们与 partitionConfigTTL /
	// partitionGCInterval 绑定，调小等于削弱缩容的安全屏障。
	resizeQuiesceDelay time.Duration
	resizeStablePeriod time.Duration
	resizeDrainTimeout time.Duration
}

// partitionCounts 是某 queue 分区配置的缓存项。
type partitionCounts struct {
	read  int
	write int
	at    time.Time
}

type partitionHandle struct {
	mu          sync.RWMutex
	queueName   string
	partitionID int
	manager     *dbleader.Manager
	active      *queue.PhysicalQueue
	stateCh     chan struct{}
	closed      bool
}

type partitionLeaderTask struct {
	handle      *partitionHandle
	store       queue.Store
	queueName   string
	partitionID int
	visTimeout  time.Duration
	maxAttempts int
	leaseOwner  string
}

func (t *partitionLeaderTask) Name() string {
	return fmt.Sprintf("partition-%s-%d", t.queueName, t.partitionID)
}

func tickRun(ctx context.Context, interval time.Duration, f func(context.Context) error) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := f(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := f(ctx); err != nil {
				return err
			}
		}
	}
}

func (t *partitionLeaderTask) Start(ctx context.Context) error {
	pq, err := queue.NewPhysicalQueue(ctx, t.store, t.queueName, t.partitionID, t.visTimeout, t.maxAttempts, t.leaseOwner)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 3)
	var wg sync.WaitGroup
	active := false

	defer func() {
		cancel()
		wg.Wait()
		if active {
			t.handle.clearActive()
			return
		}
		pq.Close()
	}()

	reportErr := func(err error) {
		if err == nil || runCtx.Err() != nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
	}

	wg.Go(func() {
		reportErr(pq.RunWriter(runCtx))
	})

	if err := pq.WaitUntilWriterStarted(runCtx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	t.handle.setActive(pq)
	active = true

	wg.Go(func() {
		reportErr(tickRun(runCtx, partitionGCInterval, pq.RunGCOnce))
	})

	wg.Go(func() {
		reportErr(tickRun(runCtx, partitionVisibilityScanInterval, func(ctx context.Context) error {
			pq.RunVisibilityScanOnce(ctx)
			return nil
		}))
	})

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func newPartitionHandle(
	store queue.Store,
	lockStore dbleader.LockStore,
	queueName string,
	partitionID int,
	visTimeout time.Duration,
	maxAttempts int,
	leaseOwner string,
) *partitionHandle {
	handle := &partitionHandle{
		queueName:   queueName,
		partitionID: partitionID,
		stateCh:     make(chan struct{}),
	}
	handle.manager = dbleader.NewManager(
		lockStore,
		partitionKey(queueName, partitionID),
		leaseOwner,
		[]dbleader.LeaderTask{&partitionLeaderTask{
			handle:      handle,
			store:       store,
			queueName:   queueName,
			partitionID: partitionID,
			visTimeout:  visTimeout,
			maxAttempts: maxAttempts,
			leaseOwner:  leaseOwner,
		}},
		dbleader.WithLeaseTTL(3*time.Second),
		dbleader.WithHeartbeat(time.Second),
		dbleader.WithRetryInterval(200*time.Millisecond),
	)
	return handle
}

func (h *partitionHandle) start() {
	h.manager.Start()
}

func (h *partitionHandle) close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	h.notifyStateChangeLocked(false)
	h.mu.Unlock()

	h.manager.Offline()
	h.manager.Stop()
	h.clearActive()
}

func (h *partitionHandle) setActive(pq *queue.PhysicalQueue) {
	h.mu.Lock()
	h.active = pq
	h.notifyStateChangeLocked(true)
	h.mu.Unlock()
}

func (h *partitionHandle) clearActive() {
	h.mu.Lock()
	pq := h.active
	h.active = nil
	if !h.closed {
		h.notifyStateChangeLocked(true)
	}
	h.mu.Unlock()

	if pq != nil {
		pq.Close()
	}
}

func (h *partitionHandle) notifyStateChangeLocked(renew bool) {
	if h.stateCh != nil {
		close(h.stateCh)
	}
	if renew {
		h.stateCh = make(chan struct{})
	} else {
		h.stateCh = nil
	}
}

func (h *partitionHandle) isClosed() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.closed
}

func (h *partitionHandle) isActive() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.active != nil && !h.active.IsClosed()
}

func (h *partitionHandle) waitUntilActive(ctx context.Context) (*queue.PhysicalQueue, error) {
	for {
		if pq, err := h.getActive(); err == nil {
			return pq, nil
		}

		h.mu.RLock()
		ch := h.stateCh
		closed := h.closed
		h.mu.RUnlock()

		if closed {
			return nil, queue.ErrQueueClosed
		}
		if ch == nil {
			return nil, queue.ErrQueueClosed
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}

func (h *partitionHandle) getActive() (*queue.PhysicalQueue, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.active == nil || h.active.IsClosed() {
		return nil, queue.ErrQueueClosed
	}
	return h.active, nil
}

func (h *partitionHandle) AddTask(ctx context.Context, data []byte) error {
	return h.AddTaskWithOptions(ctx, data, queue.EnqueueOptions{})
}

func (h *partitionHandle) AddTaskWithOptions(ctx context.Context, data []byte, opts queue.EnqueueOptions) error {
	pq, err := h.getActive()
	if err != nil {
		return err
	}
	return pq.AddTaskWithOptions(ctx, data, opts)
}

func (h *partitionHandle) TryPoll(ctx context.Context) (*queue.TaskRow, string, bool, error) {
	pq, err := h.getActive()
	if err != nil {
		return nil, "", false, err
	}
	return pq.TryPoll(ctx)
}

func (h *partitionHandle) TryPollWithOptions(ctx context.Context, opts queue.PollOptions) (queue.Lease, bool, error) {
	pq, err := h.getActive()
	if err != nil {
		return queue.Lease{}, false, err
	}
	return pq.TryPollWithOptions(ctx, opts)
}

func (h *partitionHandle) RegisterWaiter() (*queue.PollWaiter, error) {
	return h.RegisterWaiterWithOptions(queue.PollOptions{})
}

func (h *partitionHandle) RegisterWaiterWithOptions(opts queue.PollOptions) (*queue.PollWaiter, error) {
	pq, err := h.getActive()
	if err != nil {
		return nil, err
	}
	return pq.RegisterWaiterWithOptions(opts)
}

func (h *partitionHandle) Ack(ctx context.Context, token string) error {
	pq, err := h.getActive()
	if err != nil {
		return err
	}
	return pq.Ack(ctx, token)
}

func (h *partitionHandle) Nack(ctx context.Context, token string) error {
	pq, err := h.getActive()
	if err != nil {
		return err
	}
	return pq.Nack(ctx, token)
}

// ReturnLease 归还未交付给消费者的租约，不消耗投递次数，见 queue.PhysicalQueue.ReturnLease。
func (h *partitionHandle) ReturnLease(ctx context.Context, token string) error {
	pq, err := h.getActive()
	if err != nil {
		return err
	}
	return pq.ReturnLease(ctx, token)
}

func newPartitionManager(
	ctx context.Context,
	store queue.Store,
	lockStore dbleader.LockStore,
	selfAddr string,
	leaseOwner string,
	resolver membership.ServiceResolver,
	visTimeout time.Duration,
	maxAttempts int,
) *partitionManager {
	ctx, cancel := context.WithCancel(ctx)
	return &partitionManager{
		store:       store,
		lockStore:   lockStore,
		selfAddr:    selfAddr,
		leaseOwner:  leaseOwner,
		resolver:    resolver,
		visTimeout:  visTimeout,
		maxAttempts: maxAttempts,
		partitions:  make(map[string]*partitionHandle),
		configCache: make(map[string]partitionCounts),

		resizeQuiesceDelay: defaultResizeQuiesceDelay,
		resizeStablePeriod: defaultResizeStablePeriod,
		resizeDrainTimeout: defaultResizeDrainTimeout,

		ctx:    ctx,
		cancel: cancel,
	}
}

// resolvePartitions 读时取值某 queue 的 read/write 分区数，带短 TTL 缓存。
// 缓存未命中或过期则查 store（override ?? 全局默认）。
//
// store 出错时的策略（缩容安全核心）：
//   - 有缓存（哪怕已过 TTL）→ 继续用旧值（stale-while-error）。旧值是这个 queue 真实
//     存在过的配置，不会凭空放大到从未配置过的分区号，因此不会把写放行到未知分区。
//   - 无任何缓存 → 返回 ErrPartitionConfigUnavailable，让调用方拒绝服务。**不再**退回
//     bootstrap 默认值：默认值可能大于真实配置，会往已缩容退役的分区写入并孤立数据
//     （见 ErrPartitionConfigUnavailable）。
func (pm *partitionManager) resolvePartitions(queueName string) (read, write int, err error) {
	now := time.Now()
	pm.configMu.Lock()
	if c, ok := pm.configCache[queueName]; ok && now.Sub(c.at) < partitionConfigTTL {
		pm.configMu.Unlock()
		return c.read, c.write, nil
	}
	pm.configMu.Unlock()

	r, w, resolveErr := pm.store.ResolvePartitions(pm.ctx, queueName)
	if resolveErr != nil {
		pm.configMu.Lock()
		cached, ok := pm.configCache[queueName]
		pm.configMu.Unlock()
		if ok {
			slog.Warn("matching resolve partitions failed, serving stale config",
				"queue", queueName, "num_read", cached.read, "num_write", cached.write,
				"stale_for", now.Sub(cached.at), "err", resolveErr)
			return cached.read, cached.write, nil
		}
		slog.Error("matching resolve partitions failed with no cached config, refusing to serve",
			"queue", queueName, "err", resolveErr)
		return 0, 0, fmt.Errorf("%w: queue %q: %w", ErrPartitionConfigUnavailable, queueName, resolveErr)
	}
	pm.configMu.Lock()
	pm.configCache[queueName] = partitionCounts{read: r, write: w, at: now}
	pm.configMu.Unlock()
	return r, w, nil
}

// resolveReadPartitions 返回 queue 的读分区数（Poll/Ack/Nack/reconcile 用）。
func (pm *partitionManager) resolveReadPartitions(queueName string) (int, error) {
	read, _, err := pm.resolvePartitions(queueName)
	return read, err
}

// listKnownQueues 返回本轮 reconcile 应枚举的 queue 名集合：
// 已物化分区的 queue（DISTINCT queue_name FROM queue_info）∪ 有 override 的 queue。
// 带短 TTL 缓存，避免每 5s×N 节点全扫。store 出错且无缓存时返回 (nil,false)，reconcile 据此跳过本轮。
func (pm *partitionManager) listKnownQueues() ([]string, bool) {
	pm.queueListMu.Lock()
	if pm.queueListCache != nil && time.Since(pm.queueListAt) < partitionConfigTTL {
		names := pm.queueListCache
		pm.queueListMu.Unlock()
		return names, true
	}
	pm.queueListMu.Unlock()

	names, err := pm.store.ListQueueNames(pm.ctx)
	if err != nil {
		slog.Error("matching list queue names failed", "err", err)
		pm.queueListMu.Lock()
		cached := pm.queueListCache
		pm.queueListMu.Unlock()
		return cached, cached != nil
	}
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[n] = struct{}{}
	}
	// override 行是补充来源；查询失败不致命，用 queue_info 名单先行。
	if overrides, err := pm.store.ListPartitionOverrides(pm.ctx); err != nil {
		slog.Error("matching list partition overrides failed", "err", err)
	} else {
		for _, o := range overrides {
			set[o.QueueName] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for n := range set {
		result = append(result, n)
	}
	pm.queueListMu.Lock()
	pm.queueListCache = result
	pm.queueListAt = time.Now()
	pm.queueListMu.Unlock()
	return result, true
}

// isOwner 检查本节点是否拥有指定分区
func (pm *partitionManager) isOwner(queueName string, partitionID int) (bool, error) {
	host, err := pm.resolver.Lookup(partitionKey(queueName, partitionID))
	if err != nil {
		return false, err
	}
	return host.GetAddress() == pm.selfAddr, nil
}

// validatePartition 按操作类型分裂校验 partitionID（读写闸门分裂，缩容安全核心）：
//   - accessWrite（AddTask）：按 numWrite 硬拒绝 partID >= numWrite（返回 ErrPartitionNotWritable
//     → FailedPrecondition）。缩容态下滞后 frontend 往高位分区的写被当场挡在 DB 外，避免孤立数据。
//   - accessRead（Poll/PollQueue/Ack/Nack）：按 numRead 放行 [0, numRead)，否则高位待排空分区无法被消费。
//
// queue 未知不再报错——resolver fallback 到全局默认，不物化。
//
// 配置不可知（store 出错且无缓存）时返回 ErrPartitionConfigUnavailable → Unavailable，
// 读写一律拒绝。绝不能因为读不到配置就用猜测值放行写入。
func (pm *partitionManager) validatePartition(queueName string, partitionID int, access partitionAccess) error {
	if partitionID < 0 {
		return ErrInvalidPartition
	}
	read, write, err := pm.resolvePartitions(queueName)
	if err != nil {
		return err
	}
	if access == accessWrite {
		if partitionID >= write {
			return ErrPartitionNotWritable
		}
		return nil
	}
	if partitionID >= read {
		return ErrInvalidPartition
	}
	return nil
}

// maxHistoricalPartitionID 是「历史上可能存在过的分区号」的合理上界。
// 超过它的 partID 不可能来自任何一次真实的分区配置，按非法处理。
const maxHistoricalPartitionID = 1 << 16

// isRetiredReadPartition 判断 partID 是否是「曾经在 read 范围内、已被缩容退役」的分区。
//
// 用途：缩容完成后，指向已弃分区的**迟到 Ack/Nack 重试**会撞上 accessRead 闸门
// （partID >= numRead）拿到 InvalidArgument——而 InvalidArgument 不在 frontend 的重试
// 白名单里，消费者会对一个其实早已成功 ack 的任务收到硬错误。
//
// 判定理由：缩容能推进到「减 read」这一步，前提是编排器已确认该分区完全排空
// （ack_level == next_task_id-1，稳定跨过一个 GC 周期）。因此任何仍指向该分区的
// Ack/Nack 必然是重复请求，语义上等价于已完成，返回幂等成功是正确的。
//
// 已知代价：无法区分「退役分区的迟到 ack」与「客户端凭空构造的越界 partID」，
// 后者也会拿到幂等成功而非 InvalidArgument。这是刻意取舍——宁可放过一个伪造的
// no-op ack，也不能让正常消费者在缩容后收到不可重试的硬错误。负数与超过
// maxHistoricalPartitionID 的明显非法值仍然被拒。
//
// 配置不可知（store 出错且无缓存）时返回错误而非 true：不能在读不到 numRead 的情况下
// 把一个可能仍在服务的分区上的 Ack 判成「退役分区幂等成功」——那会静默吞掉真实的 Ack。
// 调用方应据此返回 Unavailable 让消费者重试。
func (pm *partitionManager) isRetiredReadPartition(queueName string, partitionID int) (bool, error) {
	if partitionID < 0 || partitionID >= maxHistoricalPartitionID {
		return false, nil
	}
	read, err := pm.resolveReadPartitions(queueName)
	if err != nil {
		return false, err
	}
	return partitionID >= read, nil
}

// getOrLoadPartition 获取分区句柄，如果本节点拥有但尚未加载则延迟加载
func (pm *partitionManager) getOrLoadPartition(ctx context.Context, queueName string, partitionID int, access partitionAccess) (*partitionHandle, error) {
	if err := pm.validatePartition(queueName, partitionID, access); err != nil {
		return nil, err
	}

	key := partitionKey(queueName, partitionID)

	// 快路径：读锁查找
	pm.mu.RLock()
	handle, ok := pm.partitions[key]
	pm.mu.RUnlock()
	if ok && !handle.isClosed() {
		isOwner, err := pm.isOwner(queueName, partitionID)
		if err != nil {
			return nil, fmt.Errorf("lookup owner for %s: %w", key, err)
		}
		if !isOwner {
			pm.unloadPartition(key, handle)
			return nil, ErrNotOwner
		}
		if err := pm.waitUntilServing(ctx, handle); err != nil {
			return nil, err
		}
		return handle, nil
	}

	// 检查所有权
	isOwner, err := pm.isOwner(queueName, partitionID)
	if err != nil {
		return nil, fmt.Errorf("lookup owner for %s: %w", key, err)
	}
	if !isOwner {
		return nil, ErrNotOwner
	}

	// 慢路径：写锁加载
	pm.mu.Lock()
	isOwner, err = pm.isOwner(queueName, partitionID)
	if err != nil {
		pm.mu.Unlock()
		return nil, fmt.Errorf("lookup owner for %s: %w", key, err)
	}
	if !isOwner {
		stale := pm.partitions[key]
		delete(pm.partitions, key)
		pm.mu.Unlock()
		if stale != nil {
			stale.close()
		}
		return nil, ErrNotOwner
	}

	// double check
	if handle, ok := pm.partitions[key]; ok {
		if handle.isClosed() {
			delete(pm.partitions, key)
		} else {
			pm.mu.Unlock()
			if err := pm.waitUntilServing(ctx, handle); err != nil {
				return nil, err
			}
			return handle, nil
		}
	}

	handle = newPartitionHandle(pm.store, pm.lockStore, queueName, partitionID, pm.visTimeout, pm.maxAttempts, pm.leaseOwner)
	handle.start()
	pm.partitions[key] = handle
	pm.mu.Unlock()

	if err := pm.waitUntilServing(ctx, handle); err != nil {
		if errors.Is(err, queue.ErrQueueClosed) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, queue.ErrQueueClosed
		}
		return nil, err
	}
	return handle, nil
}

func (pm *partitionManager) unloadPartition(key string, handle *partitionHandle) {
	pm.mu.Lock()
	current, ok := pm.partitions[key]
	if ok && current == handle {
		delete(pm.partitions, key)
	}
	pm.mu.Unlock()
	if ok && current == handle {
		handle.close()
	}
}

func (pm *partitionManager) waitUntilServing(ctx context.Context, handle *partitionHandle) error {
	waitCtx, cancel := context.WithTimeout(ctx, partitionStartTimeout)
	defer cancel()

	if _, err := handle.waitUntilActive(waitCtx); err != nil {
		if errors.Is(err, context.Canceled) && pm.ctx.Err() != nil {
			return queue.ErrQueueClosed
		}
		return err
	}
	return nil
}

type partitionID struct {
	queueName   string
	partitionID int
}

// reconcile 对比哈希环分配结果，加载新分区、卸载失去的分区
func (pm *partitionManager) reconcile() {
	// 枚举已知 queue（queue_info ∪ override），对每个按 numRead own [0, numRead)。
	queueNames, ok := pm.listKnownQueues()
	if !ok {
		slog.Error("matching reconcile aborted: cannot list known queues")
		return
	}

	// 计算本节点应拥有的分区。
	// skipped 记录本轮 resolve 失败的 queue：它们的分区归属状态保持现状（既不卸载已加载的，
	// 也不加载新的）。配置不可知时卸载分区会让在服务的分区失去 owner。
	owned := make(map[string]partitionID)
	skipped := make(map[string]struct{})
	for _, queueName := range queueNames {
		numRead, err := pm.resolveReadPartitions(queueName)
		if err != nil {
			slog.Error("matching reconcile skipping queue: partition config unavailable", "queue", queueName, "err", err)
			skipped[queueName] = struct{}{}
			continue
		}
		for i := range numRead {
			isOwner, err := pm.isOwner(queueName, i)
			if err != nil {
				slog.Error("matching reconcile aborted: ownership lookup failed", "queue", queueName, "partition_id", i, "err", err)
				return
			}
			if isOwner {
				owned[partitionKey(queueName, i)] = partitionID{queueName, i}
			}
		}
	}

	pm.mu.Lock()

	// 卸载不再属于本节点的分区（本轮 resolve 失败的 queue 除外，保持现状）。
	// 锁内只做摘除，close() 留到锁外：close() 会同步走 Offline()（DB Release，有秒级
	// 超时）再 Stop()（排空分区 goroutine），持锁串行执行会让所有 RPC 阻塞在
	// pm.mu 上整整数十秒，进而 Ack 超时、任务被判未确认而重复投递。
	var unloading []*partitionHandle
	for key, handle := range pm.partitions {
		if _, ok := owned[key]; ok {
			continue
		}
		if _, ok := skipped[handle.queueName]; ok {
			continue
		}
		slog.Info("matching unloading partition", "partition", key)
		unloading = append(unloading, handle)
		delete(pm.partitions, key)
	}

	// 加载新分配给本节点的分区
	for key, pid := range owned {
		if _, ok := pm.partitions[key]; ok {
			continue
		}
		handle := newPartitionHandle(pm.store, pm.lockStore, pid.queueName, pid.partitionID, pm.visTimeout, pm.maxAttempts, pm.leaseOwner)
		handle.start()
		slog.Info("matching loaded partition", "partition", key)
		pm.partitions[key] = handle
	}
	pm.mu.Unlock()

	// 已从 map 摘除的 handle 不会再被 getOrLoadPartition 取到（并发重建的是另一个
	// handle 实例），因此这里关的一定是自己摘下来的那个，无需再做身份比对。
	// 并发关闭多个分区，避免逐个 Offline 超时串行累加。
	var wg sync.WaitGroup
	for _, handle := range unloading {
		wg.Go(handle.close)
	}
	wg.Wait()
}

// close 关闭所有分区。返回前保证全部分区已关完（整体关停语义），但不持 pm.mu 等待：
// 锁内摘除、锁外并发 close，避免关停期间把所有 RPC 堵在 pm.mu 上。
func (pm *partitionManager) close() {
	pm.cancel()
	pm.mu.Lock()
	handles := make([]*partitionHandle, 0, len(pm.partitions))
	for key, handle := range pm.partitions {
		handles = append(handles, handle)
		delete(pm.partitions, key)
	}
	pm.mu.Unlock()

	var wg sync.WaitGroup
	for _, handle := range handles {
		wg.Go(handle.close)
	}
	wg.Wait()
}
