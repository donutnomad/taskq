package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultPartitions = 4
	defaultVisTimeout = 60 * time.Second
)

// Options 队列配置
type Options struct {
	QueueName  string
	Store      Store
	Partitions int
	VisTimeout time.Duration
	BufferSize int
}

// PartitionedQueue 多分区队列，对外实现 TaskQueue 接口。
type PartitionedQueue struct {
	mu         sync.RWMutex
	partitions []*PhysicalQueue
	numParts   int

	enqueueIdx atomic.Int64
	pollIdx    atomic.Int64

	opts   Options
	ctx    context.Context
	cancel context.CancelFunc
}

// New 创建并启动一个多分区 PartitionedQueue。
func New(ctx context.Context, opts Options) (*PartitionedQueue, error) {
	if opts.QueueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if opts.Partitions <= 0 {
		opts.Partitions = defaultPartitions
	}
	if opts.VisTimeout <= 0 {
		opts.VisTimeout = defaultVisTimeout
	}

	ctx, cancel := context.WithCancel(ctx)
	pq := &PartitionedQueue{
		numParts: opts.Partitions,
		opts:     opts,
		ctx:      ctx,
		cancel:   cancel,
	}

	pq.partitions = make([]*PhysicalQueue, opts.Partitions)
	for i := 0; i < opts.Partitions; i++ {
		p, err := newPhysicalQueue(ctx, opts.Store, opts.QueueName, i, opts.VisTimeout)
		if err != nil {
			for j := 0; j < i; j++ {
				pq.partitions[j].close()
			}
			cancel()
			return nil, fmt.Errorf("create partition %d: %w", i, err)
		}
		pq.partitions[i] = p
	}

	// 为非 root 分区设置转发到 root
	pq.setupForwarding()

	return pq, nil
}

// EnqueueRaw 写入原始字节数据，round-robin 选分区
func (pq *PartitionedQueue) EnqueueRaw(data []byte) error {
	pq.mu.RLock()
	n := pq.numParts
	parts := pq.partitions
	pq.mu.RUnlock()

	idx := int(pq.enqueueIdx.Add(1)) % n
	return parts[idx].addTask(pq.ctx, data)
}

// DequeueRaw 消费原始字节数据。
// 采用 Temporal 风格的转发模型：round-robin 选分区 → 尝试本地 → 尝试 root → 在 root 等待。
// 每次 dequeue 最多碰 2 个分区的锁，waiter 只注册在 root 上。
func (pq *PartitionedQueue) DequeueRaw(ctx context.Context) ([]byte, string, error) {
	pq.mu.RLock()
	n := pq.numParts
	parts := pq.partitions
	pq.mu.RUnlock()

	root := parts[0]

	// 1. round-robin 选一个分区，尝试本地消费
	idx := int(pq.pollIdx.Add(1)) % n
	if row, token, ok := parts[idx].tryPoll(ctx); ok {
		return row.Data, token, nil
	}

	// 2. 本地没有，尝试 root（如果本地不是 root）
	if idx != 0 {
		if row, token, ok := root.tryPoll(ctx); ok {
			return row.Data, token, nil
		}
	}

	// 3. 都没有，只在 root 注册 waiter 等待
	ch := make(chan PollResult, 1)
	elem := root.registerWaiter(ch)

	select {
	case result := <-ch:
		root.removeWaiter(elem)
		if result.Err != nil {
			return nil, "", result.Err
		}
		return result.Row.Data, result.Token, nil
	case <-ctx.Done():
		root.removeWaiter(elem)
		return nil, "", ctx.Err()
	case <-pq.ctx.Done():
		root.removeWaiter(elem)
		return nil, "", ErrQueueClosed
	}
}

// Ack 确认消费
func (pq *PartitionedQueue) Ack(token string) error {
	pid, _, err := parseToken(token)
	if err != nil {
		return err
	}
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	if pid < 0 || pid >= len(pq.partitions) {
		return fmt.Errorf("invalid partition %d", pid)
	}
	return pq.partitions[pid].ack(pq.ctx, token)
}

// Nack 拒绝消费，立即重投递
func (pq *PartitionedQueue) Nack(token string) error {
	pid, _, err := parseToken(token)
	if err != nil {
		return err
	}
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	if pid < 0 || pid >= len(pq.partitions) {
		return fmt.Errorf("invalid partition %d", pid)
	}
	return pq.partitions[pid].nack(pq.ctx, token)
}

// AddPartition 动态增加分区（只增不减）
func (pq *PartitionedQueue) AddPartition() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	newID := len(pq.partitions)
	p, err := newPhysicalQueue(pq.ctx, pq.opts.Store, pq.opts.QueueName, newID, pq.opts.VisTimeout)
	if err != nil {
		return fmt.Errorf("create partition %d: %w", newID, err)
	}
	// 新分区设置转发到 root
	root := pq.partitions[0]
	p.forwardFn = func() { pq.forwardToRoot(root, p) }
	pq.partitions = append(pq.partitions, p)
	pq.numParts = len(pq.partitions)
	return nil
}

// setupForwarding 为所有非 root 分区设置转发回调。
// 当非 root 分区有任务但无本地 waiter 时，尝试唤醒 root 上等待的 poller。
func (pq *PartitionedQueue) setupForwarding() {
	if len(pq.partitions) <= 1 {
		return
	}
	root := pq.partitions[0]
	for i := 1; i < len(pq.partitions); i++ {
		source := pq.partitions[i]
		source.forwardFn = func() { pq.forwardToRoot(root, source) }
	}
}

// forwardToRoot 从 source 分区取任务，投递给 root 上等待的 poller。
// 锁顺序：root.mu → source.mu（via tryPoll），不会死锁。
func (pq *PartitionedQueue) forwardToRoot(root, source *PhysicalQueue) {
	root.mu.Lock()
	defer root.mu.Unlock()

	for root.waiters.Len() > 0 {
		row, token, ok := source.tryPoll(source.ctx)
		if !ok {
			break
		}

		elem := root.waiters.Front()
		ch := elem.Value.(chan PollResult)
		root.waiters.Remove(elem)

		select {
		case ch <- PollResult{Row: row, Token: token}:
		default:
		}
	}
}

// Close 关闭队列所有后台组件
func (pq *PartitionedQueue) Close() error {
	pq.cancel()
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	for _, p := range pq.partitions {
		p.close()
	}
	return nil
}
