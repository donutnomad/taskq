package queue

import (
	"container/list"
	"context"
	"log/slog"
	"sync"
	"time"
)

// PollResult 长轮询结果
type PollResult struct {
	Row   *TaskRow
	Token string
	Err   error
}

// PhysicalQueue 单分区队列核心，负责装配各个子组件。
type PhysicalQueue struct {
	store       Store
	queueName   string
	partitionID int
	ownership   *ownershipState
	visTimeout  time.Duration

	waiters *waiterRegistry
	runtime *taskRuntime
	writer  *taskWriter
	gc      *taskGC
	visScan *visibilityScanner

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func newPhysicalQueue(
	ctx context.Context,
	store Store,
	queueName string,
	partitionID int,
	visTimeout time.Duration,
	_ string,
) (*PhysicalQueue, error) {
	info, err := claimQueueOwnership(ctx, store, queueName, partitionID)
	if err != nil {
		return nil, err
	}

	queueCtx, cancel := context.WithCancel(ctx)
	pq := &PhysicalQueue{
		store:       store,
		queueName:   queueName,
		partitionID: partitionID,
		ownership:   newOwnershipState(store, info.RangeID),
		visTimeout:  visTimeout,
		waiters:     newWaiterRegistry(),
		ctx:         queueCtx,
		cancel:      cancel,
	}

	pq.runtime = newTaskRuntime(pq, info.AckLevel)
	if info.NextTaskID > 1 {
		pq.runtime.setMaxReadLevel(info.NextTaskID - 1)
	}
	pq.writer = newTaskWriter(pq, info.NextTaskID, pq.onFlush)
	pq.gc = newTaskGC(pq, pq.runtime)
	pq.visScan = newVisibilityScanner(pq, pq.runtime)

	return pq, nil
}

func (pq *PhysicalQueue) addTask(ctx context.Context, data []byte) error {
	if pq.IsClosed() {
		return ErrQueueClosed
	}

	_, err := pq.writer.enqueue(data)
	if err != nil && pq.IsClosed() {
		return ErrQueueClosed
	}
	return err
}

func (pq *PhysicalQueue) onFlush(rows []*TaskRow) {
	for _, row := range rows {
		pq.runtime.setMaxReadLevel(row.TaskID)
	}
	pq.runtime.tryDeliverToWaiters()
}

func (pq *PhysicalQueue) tryPoll(ctx context.Context) (*TaskRow, string, bool) {
	if pq.IsClosed() {
		return nil, "", false
	}

	row, token, ok, err := pq.runtime.tryGetTask(ctx)
	if err != nil {
		slog.Error("queue tryPoll failed", "queue", pq.queueName, "partition_id", pq.partitionID, "err", err)
		return nil, "", false
	}
	return row, token, ok
}

func (pq *PhysicalQueue) registerWaiter(ch chan PollResult) *list.Element {
	if pq.IsClosed() {
		sendPollResult(ch, PollResult{Err: ErrQueueClosed})
		return nil
	}

	row, token, ok, err := pq.runtime.tryGetTask(pq.ctx)
	if err != nil {
		slog.Error("queue registerWaiter immediate poll failed", "queue", pq.queueName, "partition_id", pq.partitionID, "err", err)
	} else if ok {
		if sendPollResult(ch, PollResult{Row: row, Token: token}) {
			return nil
		}
		if nackErr := pq.runtime.nack(pq.ctx, row.TaskID); nackErr != nil {
			slog.Error("queue failed to return task after waiter immediate send failure", "queue", pq.queueName, "partition_id", pq.partitionID, "task_id", row.TaskID, "err", nackErr)
		}
		return nil
	}

	return pq.waiters.add(ch)
}

func (pq *PhysicalQueue) removeWaiter(elem *list.Element) {
	pq.waiters.remove(elem)
}

func (pq *PhysicalQueue) wakeWaiters() {
	pq.runtime.tryDeliverToWaiters()
}

func (pq *PhysicalQueue) ack(ctx context.Context, token string) error {
	if pq.IsClosed() {
		return ErrQueueClosed
	}

	partitionID, taskID, err := parseToken(token)
	if err != nil {
		return err
	}
	if err := validateTokenPartition(pq.partitionID, partitionID); err != nil {
		return err
	}

	_ = ctx
	return pq.runtime.ack(taskID)
}

func (pq *PhysicalQueue) nack(ctx context.Context, token string) error {
	if pq.IsClosed() {
		return ErrQueueClosed
	}

	partitionID, taskID, err := parseToken(token)
	if err != nil {
		return err
	}
	if err := validateTokenPartition(pq.partitionID, partitionID); err != nil {
		return err
	}

	if err := pq.runtime.nack(ctx, taskID); err != nil {
		return err
	}
	pq.wakeWaiters()
	return nil
}

func (pq *PhysicalQueue) close() {
	pq.closeOnce.Do(func() {
		pq.cancel()
		pq.waiters.close()
		pq.writer.close()
		pq.visScan.close()
		pq.gc.close()
	})
}

func (pq *PhysicalQueue) runWriter(ctx context.Context) error {
	return pq.writer.run(ctx)
}

func (pq *PhysicalQueue) waitUntilWriterStarted(ctx context.Context) error {
	return pq.writer.waitUntilStarted(ctx)
}

func (pq *PhysicalQueue) runGCOnce(ctx context.Context) error {
	return pq.gc.collect(ctx)
}

func (pq *PhysicalQueue) runVisibilityScanOnce(ctx context.Context) {
	pq.visScan.scan(ctx)
}

// NewPhysicalQueue 创建单个分区的物理队列。
// 分布式模式下由 matching 服务调用，每个 PhysicalQueue 独立管理一个分区。
func NewPhysicalQueue(
	ctx context.Context,
	store Store,
	queueName string,
	partitionID int,
	visTimeout time.Duration,
	leaseOwner string,
) (*PhysicalQueue, error) {
	return newPhysicalQueue(ctx, store, queueName, partitionID, visTimeout, leaseOwner)
}

// AddTask 写入任务到本分区
func (pq *PhysicalQueue) AddTask(ctx context.Context, data []byte) error {
	return pq.addTask(ctx, data)
}

// TryPoll 非阻塞尝试获取任务
func (pq *PhysicalQueue) TryPoll(ctx context.Context) (*TaskRow, string, bool) {
	return pq.tryPoll(ctx)
}

// RegisterWaiter 注册等待 channel，有任务时通过该 channel 投递
func (pq *PhysicalQueue) RegisterWaiter(ch chan PollResult) *list.Element {
	return pq.registerWaiter(ch)
}

// RemoveWaiter 取消等待注册
func (pq *PhysicalQueue) RemoveWaiter(elem *list.Element) {
	pq.removeWaiter(elem)
}

// Ack 确认消费完成
func (pq *PhysicalQueue) Ack(ctx context.Context, token string) error {
	return pq.ack(ctx, token)
}

// Nack 拒绝消费，触发重投递
func (pq *PhysicalQueue) Nack(ctx context.Context, token string) error {
	return pq.nack(ctx, token)
}

// Close 关闭分区，停止所有后台组件
func (pq *PhysicalQueue) Close() {
	pq.close()
}

// IsClosed 返回分区是否已停止服务。
func (pq *PhysicalQueue) IsClosed() bool {
	return pq.ctx.Err() != nil
}

// RunWriter 运行本分区的写入循环，通常由 leader task 托管。
func (pq *PhysicalQueue) RunWriter(ctx context.Context) error {
	return pq.runWriter(ctx)
}

// WaitUntilWriterStarted 等待写入循环真正进入运行态。
func (pq *PhysicalQueue) WaitUntilWriterStarted(ctx context.Context) error {
	return pq.waitUntilWriterStarted(ctx)
}

// RunGCOnce 执行一次 GC。
func (pq *PhysicalQueue) RunGCOnce(ctx context.Context) error {
	return pq.runGCOnce(ctx)
}

// RunVisibilityScanOnce 执行一次可见性扫描。
func (pq *PhysicalQueue) RunVisibilityScanOnce(ctx context.Context) {
	pq.runVisibilityScanOnce(ctx)
}

// ParseToken 解析 ack token，返回分区 ID 和任务 ID。
// token 格式为 "partitionID:taskID"。
func ParseToken(token string) (partitionID int, taskID int64, err error) {
	return parseToken(token)
}
