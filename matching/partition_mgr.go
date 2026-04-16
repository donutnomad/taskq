package matching

import (
	"container/list"
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
)

// partitionKey 返回用于一致性哈希查找的 key
func partitionKey(queueName string, partitionID int) string {
	return fmt.Sprintf("%s/%d", queueName, partitionID)
}

type partitionManager struct {
	mu           sync.RWMutex
	store        queue.Store
	lockStore    dbleader.LockStore
	selfAddr     string // 本节点广播地址
	leaseOwner   string // DB lease 标识
	resolver     membership.ServiceResolver
	visTimeout   time.Duration
	partitions   map[string]*partitionHandle // key = partitionKey(...)
	queueConfigs map[string]int              // queueName → numPartitions
	ctx          context.Context
	cancel       context.CancelFunc
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
	pq, err := queue.NewPhysicalQueue(ctx, t.store, t.queueName, t.partitionID, t.visTimeout, t.leaseOwner)
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
		if errors.Is(err, queue.ErrOwnershipLost) {
			return dbleader.StepDown(err)
		}
		return err
	}
}

func newPartitionHandle(
	store queue.Store,
	lockStore dbleader.LockStore,
	queueName string,
	partitionID int,
	visTimeout time.Duration,
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
	pq, err := h.getActive()
	if err != nil {
		return err
	}
	return pq.AddTask(ctx, data)
}

func (h *partitionHandle) TryPoll(ctx context.Context) (*queue.TaskRow, string, bool, error) {
	pq, err := h.getActive()
	if err != nil {
		return nil, "", false, err
	}
	row, token, ok := pq.TryPoll(ctx)
	return row, token, ok, nil
}

func (h *partitionHandle) RegisterWaiter(ch chan queue.PollResult) (*queue.PhysicalQueue, *list.Element, error) {
	pq, err := h.getActive()
	if err != nil {
		return nil, nil, err
	}
	return pq, pq.RegisterWaiter(ch), nil
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

func newPartitionManager(
	ctx context.Context,
	store queue.Store,
	lockStore dbleader.LockStore,
	selfAddr string,
	leaseOwner string,
	resolver membership.ServiceResolver,
	visTimeout time.Duration,
	queueConfigs map[string]int,
) *partitionManager {
	ctx, cancel := context.WithCancel(ctx)
	return &partitionManager{
		store:        store,
		lockStore:    lockStore,
		selfAddr:     selfAddr,
		leaseOwner:   leaseOwner,
		resolver:     resolver,
		visTimeout:   visTimeout,
		partitions:   make(map[string]*partitionHandle),
		queueConfigs: queueConfigs,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// isOwner 检查本节点是否拥有指定分区
func (pm *partitionManager) isOwner(queueName string, partitionID int) (bool, error) {
	host, err := pm.resolver.Lookup(partitionKey(queueName, partitionID))
	if err != nil {
		return false, err
	}
	return host.GetAddress() == pm.selfAddr, nil
}

func (pm *partitionManager) validatePartition(queueName string, partitionID int) error {
	numPartitions, ok := pm.queueConfigs[queueName]
	if !ok {
		return ErrUnknownQueue
	}
	if partitionID < 0 || partitionID >= numPartitions {
		return ErrInvalidPartition
	}
	return nil
}

// getOrLoadPartition 获取分区句柄，如果本节点拥有但尚未加载则延迟加载
func (pm *partitionManager) getOrLoadPartition(ctx context.Context, queueName string, partitionID int) (*partitionHandle, error) {
	if err := pm.validatePartition(queueName, partitionID); err != nil {
		return nil, err
	}

	key := partitionKey(queueName, partitionID)

	// 快路径：读锁查找
	pm.mu.RLock()
	handle, ok := pm.partitions[key]
	pm.mu.RUnlock()
	if ok && !handle.isClosed() {
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

	handle = newPartitionHandle(pm.store, pm.lockStore, queueName, partitionID, pm.visTimeout, pm.leaseOwner)
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
	// 计算本节点应拥有的分区
	owned := make(map[string]partitionID)
	for queueName, numParts := range pm.queueConfigs {
		for i := range numParts {
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
	defer pm.mu.Unlock()

	// 卸载不再属于本节点的分区
	for key, handle := range pm.partitions {
		if _, ok := owned[key]; !ok {
			slog.Info("matching unloading partition", "partition", key)
			handle.close()
			delete(pm.partitions, key)
		}
	}

	// 加载新分配给本节点的分区
	for key, pid := range owned {
		if _, ok := pm.partitions[key]; ok {
			continue
		}
		handle := newPartitionHandle(pm.store, pm.lockStore, pid.queueName, pid.partitionID, pm.visTimeout, pm.leaseOwner)
		handle.start()
		slog.Info("matching loaded partition", "partition", key)
		pm.partitions[key] = handle
	}
}

// getQueuePartitionCount 返回队列的分区数
func (pm *partitionManager) getQueuePartitionCount(queueName string) (int, bool) {
	n, ok := pm.queueConfigs[queueName]
	return n, ok
}

// close 关闭所有分区
func (pm *partitionManager) close() {
	pm.cancel()
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for key, handle := range pm.partitions {
		handle.close()
		delete(pm.partitions, key)
	}
}
