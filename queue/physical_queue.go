package queue

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// defaultMaxAttempts 是投递次数上限的一个参考值；传入 maxAttempts <= 0 表示不限次数。
const defaultMaxAttempts = 10

// PollResult 长轮询结果
type PollResult struct {
	Row   *TaskRow
	Token string
	Err   error
}

// Lease returns the delivery state captured when the task was dispatched.
func (r PollResult) Lease() Lease {
	if r.Row == nil {
		return Lease{Token: r.Token}
	}
	return newLease(r.Row, r.Token, taskDispatchTime(r.Row))
}

// EnqueueOptions controls task delivery routing.
type EnqueueOptions struct {
	RoutingKey      string
	RoutingDeadline time.Time
}

// Validate checks whether the options are portable across supported stores.
func (o EnqueueOptions) Validate() error {
	if err := validateRoutingKey(o.RoutingKey); err != nil {
		return err
	}
	if o.RoutingKey == "" && !o.RoutingDeadline.IsZero() {
		return ErrInvalidRoutingDeadline
	}
	return nil
}

// PollOptions declares the routing keys accepted by a poller.
type PollOptions struct {
	AcceptedRoutingKeys []string
}

// Validate checks whether every accepted key is portable across supported stores.
func (o PollOptions) Validate() error {
	_, err := o.Normalize()
	return err
}

// Normalize validates accepted keys, removes empty entries, and deduplicates
// them while preserving first-seen order.
func (o PollOptions) Normalize() (PollOptions, error) {
	return normalizePollOptions(o)
}

// Lease is a task delivery that must be completed with Ack or Nack.
type Lease struct {
	Data            []byte
	Token           string
	RoutingKey      string
	RoutingDeadline time.Time
	RoutingFallback bool
}

// PhysicalQueue 单分区队列核心，负责装配各个子组件。
type PhysicalQueue struct {
	store          Store
	queueName      string
	partitionID    int
	ownership      *ownershipState
	ownershipEpoch int64
	visTimeout     time.Duration

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
	maxAttempts int,
	_ string,
) (*PhysicalQueue, error) {
	info, err := claimQueueOwnership(ctx, store, queueName, partitionID)
	if err != nil {
		return nil, err
	}

	queueCtx, cancel := context.WithCancel(ctx)
	pq := &PhysicalQueue{
		store:          store,
		queueName:      queueName,
		partitionID:    partitionID,
		ownership:      newOwnershipState(store, info.RangeID),
		ownershipEpoch: info.RangeID,
		visTimeout:     visTimeout,
		waiters:        newWaiterRegistry(),
		ctx:            queueCtx,
		cancel:         cancel,
	}

	pq.runtime = newTaskRuntime(pq, info.AckLevel, maxAttempts)
	if info.NextTaskID > 1 {
		pq.runtime.setMaxReadLevel(info.NextTaskID - 1)
	}
	pq.writer = newTaskWriter(pq, info.NextTaskID, pq.onFlush)
	pq.gc = newTaskGC(pq, pq.runtime)
	pq.visScan = newVisibilityScanner(pq, pq.runtime)

	return pq, nil
}

func (pq *PhysicalQueue) addTask(ctx context.Context, data []byte) error {
	return pq.addTaskWithOptions(ctx, data, EnqueueOptions{})
}

func (pq *PhysicalQueue) addTaskWithOptions(ctx context.Context, data []byte, opts EnqueueOptions) error {
	if pq.IsClosed() {
		return ErrQueueClosed
	}
	if err := opts.Validate(); err != nil {
		return err
	}
	if !opts.RoutingDeadline.IsZero() {
		opts.RoutingDeadline = opts.RoutingDeadline.UTC()
	}

	_, err := pq.writer.enqueue(data, opts)
	return err
}

func (pq *PhysicalQueue) onFlush(rows []*TaskRow) {
	for _, row := range rows {
		pq.runtime.setMaxReadLevel(row.TaskID)
	}
	pq.runtime.tryDeliverToWaiters()
}

func (pq *PhysicalQueue) tryPoll(ctx context.Context) (*TaskRow, string, bool, error) {
	return pq.tryPollWithOptions(ctx, PollOptions{})
}

func (pq *PhysicalQueue) tryPollWithOptions(ctx context.Context, opts PollOptions) (*TaskRow, string, bool, error) {
	if pq.IsClosed() {
		return nil, "", false, ErrQueueClosed
	}
	normalized, err := opts.Normalize()
	if err != nil {
		return nil, "", false, err
	}

	row, token, ok, err := pq.runtime.tryGetTask(ctx, normalized)
	if err != nil {
		// 调用方已离开时读取被取消是正常结束，不是故障。
		if ctx.Err() == nil {
			slog.Error("queue tryPoll failed", "queue", pq.queueName, "partition_id", pq.partitionID, "err", err)
		}
		return nil, "", false, err
	}
	return row, token, ok, nil
}

func (pq *PhysicalQueue) registerWaiter(opts PollOptions) (*PollWaiter, error) {
	normalized, err := opts.Normalize()
	if err != nil {
		return nil, err
	}
	waiter := pq.waiters.add(normalized)
	if !pq.IsClosed() {
		pq.runtime.tryDeliverToWaiters()
	}
	return waiter, nil
}

func (pq *PhysicalQueue) wakeWaiters() {
	pq.runtime.tryDeliverToWaiters()
}

func (pq *PhysicalQueue) ack(ctx context.Context, token string) error {
	if pq.IsClosed() {
		return ErrQueueClosed
	}

	parsed, err := ParseTaskToken(token)
	if err != nil {
		return err
	}
	if err := validateTaskToken(pq.queueName, pq.partitionID, pq.ownershipEpoch, parsed); err != nil {
		return err
	}

	_ = ctx
	return pq.runtime.ack(parsed.TaskID, parsed.DeliveryID)
}

func (pq *PhysicalQueue) nack(ctx context.Context, token string) error {
	if pq.IsClosed() {
		return ErrQueueClosed
	}

	parsed, err := ParseTaskToken(token)
	if err != nil {
		return err
	}
	if err := validateTaskToken(pq.queueName, pq.partitionID, pq.ownershipEpoch, parsed); err != nil {
		return err
	}

	if err := pq.runtime.nack(ctx, parsed.TaskID, parsed.DeliveryID); err != nil {
		return err
	}
	pq.wakeWaiters()
	return nil
}

// returnLease 归还一个从未交付给消费者的租约：任务原地回到可投递状态，
// 既不消耗 attempt 也不以新 taskID 重写行。
func (pq *PhysicalQueue) returnLease(ctx context.Context, token string) error {
	if pq.IsClosed() {
		return ErrQueueClosed
	}

	parsed, err := ParseTaskToken(token)
	if err != nil {
		return err
	}
	if err := validateTaskToken(pq.queueName, pq.partitionID, pq.ownershipEpoch, parsed); err != nil {
		return err
	}

	if err := pq.runtime.returnTask(ctx, parsed.TaskID, parsed.DeliveryID); err != nil {
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
// maxAttempts 为单条任务的总投递次数上限，超过后任务被丢弃；0 表示不限次数。
func NewPhysicalQueue(
	ctx context.Context,
	store Store,
	queueName string,
	partitionID int,
	visTimeout time.Duration,
	maxAttempts int,
	leaseOwner string,
) (*PhysicalQueue, error) {
	return newPhysicalQueue(ctx, store, queueName, partitionID, visTimeout, maxAttempts, leaseOwner)
}

// AddTask 写入任务到本分区
func (pq *PhysicalQueue) AddTask(ctx context.Context, data []byte) error {
	return pq.addTask(ctx, data)
}

// AddTaskWithOptions writes a task with delivery routing metadata.
func (pq *PhysicalQueue) AddTaskWithOptions(ctx context.Context, data []byte, opts EnqueueOptions) error {
	return pq.addTaskWithOptions(ctx, data, opts)
}

// TryPoll 非阻塞尝试获取任务
func (pq *PhysicalQueue) TryPoll(ctx context.Context) (*TaskRow, string, bool, error) {
	return pq.tryPoll(ctx)
}

// TryPollWithOptions returns the first task visible to the poller's routing keys.
func (pq *PhysicalQueue) TryPollWithOptions(ctx context.Context, opts PollOptions) (Lease, bool, error) {
	row, token, ok, err := pq.tryPollWithOptions(ctx, opts)
	if err != nil || !ok {
		return Lease{}, ok, err
	}
	return newLease(row, token, taskDispatchTime(row)), true, nil
}

// RegisterWaiter registers an atomic delivery claim for a long poll.
func (pq *PhysicalQueue) RegisterWaiter() *PollWaiter {
	waiter, _ := pq.registerWaiter(PollOptions{})
	return waiter
}

// RegisterWaiterWithOptions registers a routed long-poll delivery claim.
func (pq *PhysicalQueue) RegisterWaiterWithOptions(opts PollOptions) (*PollWaiter, error) {
	return pq.registerWaiter(opts)
}

// Ack 确认消费完成
func (pq *PhysicalQueue) Ack(ctx context.Context, token string) error {
	return pq.ack(ctx, token)
}

// Nack 拒绝消费，触发重投递
func (pq *PhysicalQueue) Nack(ctx context.Context, token string) error {
	return pq.nack(ctx, token)
}

// ReturnLease 归还一个未交付给消费者的租约，任务原地恢复可投递。
//
// 与 Nack 的区别：Nack 表示消费方明确拒绝了任务，会把 attempt 加一并以新
// taskID 重写任务，次数耗尽后丢弃；ReturnLease 用于任务根本没交到消费者手里
// 的内部仲裁（多分区 poll 竞速的败者、调用方 ctx 取消），不消耗任何投递配额，
// 也不产生新行。
func (pq *PhysicalQueue) ReturnLease(ctx context.Context, token string) error {
	return pq.returnLease(ctx, token)
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
