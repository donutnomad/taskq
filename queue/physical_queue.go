package queue

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrQueueClosed 队列已关闭
var ErrQueueClosed = errors.New("queue closed")

// PollResult 长轮询结果
type PollResult struct {
	Row   *TaskRow
	Token string
	Err   error
}

const (
	defaultLeaseTTL   = 15 * time.Second
	defaultLeaseRenew = 5 * time.Second
)

// PhysicalQueue 单分区队列核心，包含 sync match 和 DB 管理。
type PhysicalQueue struct {
	mu          sync.Mutex
	store       Store
	queueName   string
	partitionID int
	ownership   *ownershipState
	visTimeout  time.Duration
	leaseOwner  string

	waiters   *list.List // chan PollResult
	forwardFn func()     // 非 root 分区：本地无 waiter 时转发到 root
	dbMgr     *dbTaskManager
	writer    *taskWriter
	gc        *taskGC
	visScan   *visibilityScanner
	leaseWg   sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func newPhysicalQueue(ctx context.Context, store Store, queueName string,
	partitionID int, visTimeout time.Duration, leaseOwner string) (*PhysicalQueue, error) {

	// 通过 DB lease 获取分区独占权（同时递增 rangeID）
	info, err := store.AcquireLease(ctx, queueName, partitionID, leaseOwner, defaultLeaseTTL)
	if err != nil {
		return nil, err
	}

	ownership := newOwnershipState(store, info.RangeID)

	ctx, cancel := context.WithCancel(ctx)
	pq := &PhysicalQueue{
		store:       store,
		queueName:   queueName,
		partitionID: partitionID,
		ownership:   ownership,
		visTimeout:  visTimeout,
		leaseOwner:  leaseOwner,
		waiters:     list.New(),
		ctx:         ctx,
		cancel:      cancel,
	}

	pq.dbMgr = newDBTaskManager(store, queueName, partitionID, info.AckLevel, visTimeout)
	pq.writer = newTaskWriter(ctx, store, queueName, partitionID,
		ownership, info.NextTaskID, pq.onFlush)
	pq.gc = newTaskGC(ctx, store, queueName, partitionID, ownership, pq.dbMgr)
	pq.visScan = newVisibilityScanner(ctx, store, queueName, partitionID,
		pq.dbMgr, visTimeout, pq.wakeWaiters)
	pq.leaseWg.Add(1)
	go pq.leaseRenewLoop()

	return pq, nil
}

// addTask 写入 DB，sync match 在 onFlush 回调中完成
func (pq *PhysicalQueue) addTask(ctx context.Context, data []byte) error {
	_, err := pq.writer.enqueue(data)
	return err
}

// onFlush writer flush 后的回调，尝试 sync match。
// 本地有 waiter 时直接投递；否则转发到 root 分区。
func (pq *PhysicalQueue) onFlush(rows []*TaskRow) {
	pq.mu.Lock()

	needForward := false
	for _, row := range rows {
		pq.dbMgr.setMaxReadLevel(row.TaskID)

		if pq.waiters.Len() == 0 {
			needForward = true
			continue
		}

		// 取出一个 waiter
		elem := pq.waiters.Front()
		ch := elem.Value.(chan PollResult)
		pq.waiters.Remove(elem)

		// dispatch
		taskRow, ok := pq.dbMgr.getTask(pq.ctx)
		if !ok {
			pq.waiters.PushFront(ch)
			needForward = true
			continue
		}
		token := fmt.Sprintf("%d:%d", pq.partitionID, taskRow.TaskID)
		select {
		case ch <- PollResult{Row: taskRow, Token: token}:
		default:
		}
	}

	pq.mu.Unlock()

	if needForward && pq.forwardFn != nil {
		pq.forwardFn()
	}
}

// tryPoll 非阻塞尝试获取任务
func (pq *PhysicalQueue) tryPoll(ctx context.Context) (*TaskRow, string, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	row, ok := pq.dbMgr.getTask(ctx)
	if !ok {
		return nil, "", false
	}
	token := fmt.Sprintf("%d:%d", pq.partitionID, row.TaskID)
	return row, token, true
}

// registerWaiter 注册一个等待 channel，有任务时通过该 channel 投递
func (pq *PhysicalQueue) registerWaiter(ch chan PollResult) *list.Element {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.waiters.PushBack(ch)
}

// removeWaiter 取消注册
func (pq *PhysicalQueue) removeWaiter(elem *list.Element) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.waiters.Remove(elem)
}

// wakeWaiters 有任务可读时唤醒 waiters（visibilityScanner/nack 后调用）。
// 本地 waiter 优先；仍有可读任务但无本地 waiter 时转发到 root。
func (pq *PhysicalQueue) wakeWaiters() {
	pq.mu.Lock()

	for pq.waiters.Len() > 0 && pq.dbMgr.hasReadable() {
		elem := pq.waiters.Front()
		ch := elem.Value.(chan PollResult)
		pq.waiters.Remove(elem)

		row, ok := pq.dbMgr.getTask(pq.ctx)
		if !ok {
			pq.waiters.PushFront(ch)
			break
		}
		token := fmt.Sprintf("%d:%d", pq.partitionID, row.TaskID)
		select {
		case ch <- PollResult{Row: row, Token: token}:
		default:
		}
	}

	shouldForward := pq.forwardFn != nil && pq.waiters.Len() == 0 && pq.dbMgr.hasReadable()
	pq.mu.Unlock()

	if shouldForward {
		pq.forwardFn()
	}
}

func (pq *PhysicalQueue) ack(ctx context.Context, token string) error {
	_, taskID, err := parseToken(token)
	if err != nil {
		return err
	}
	pq.dbMgr.ack(ctx, taskID)
	return nil
}

func (pq *PhysicalQueue) nack(ctx context.Context, token string) error {
	_, taskID, err := parseToken(token)
	if err != nil {
		return err
	}
	pq.dbMgr.nack(ctx, taskID)
	pq.wakeWaiters()
	return nil
}

func (pq *PhysicalQueue) leaseRenewLoop() {
	defer pq.leaseWg.Done()
	timer := time.NewTimer(defaultLeaseRenew)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := pq.store.RenewLease(pq.ctx, pq.queueName, pq.partitionID,
				pq.leaseOwner, defaultLeaseTTL); err != nil {
				pq.cancel() // 续期失败 → 停止所有后台组件
				return
			}
			timer.Reset(defaultLeaseRenew)
		case <-pq.ctx.Done():
			return
		}
	}
}

func (pq *PhysicalQueue) close() {
	pq.cancel()
	// 唤醒所有 waiters
	pq.mu.Lock()
	for e := pq.waiters.Front(); e != nil; e = e.Next() {
		ch := e.Value.(chan PollResult)
		select {
		case ch <- PollResult{Err: ErrQueueClosed}:
		default:
		}
	}
	pq.waiters.Init()
	pq.mu.Unlock()

	pq.writer.close()
	pq.visScan.close()
	pq.gc.close()
	pq.leaseWg.Wait()
	// 释放 lease（使用新 context，因为 pq.ctx 已取消）
	pq.store.ReleaseLease(context.Background(), pq.queueName, pq.partitionID, pq.leaseOwner)
}

// parseToken "partitionID:taskID" → (partitionID, taskID, error)
func parseToken(token string) (int, int64, error) {
	var pid int
	var tid int64
	n, err := fmt.Sscanf(token, "%d:%d", &pid, &tid)
	if err != nil || n != 2 {
		return 0, 0, fmt.Errorf("invalid ack token: %q", token)
	}
	return pid, tid, nil
}

// ---- 导出 API（供 matching 服务使用）----

// NewPhysicalQueue 创建单个分区的物理队列。
// 分布式模式下由 matching 服务调用，每个 PhysicalQueue 独立管理一个分区。
// leaseOwner 用于 DB lease 独占分区，如果 lease 被其他节点持有则返回 ErrLeaseHeld。
func NewPhysicalQueue(ctx context.Context, store Store, queueName string,
	partitionID int, visTimeout time.Duration, leaseOwner string) (*PhysicalQueue, error) {
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

// ParseToken 解析 ack token，返回分区 ID 和任务 ID。
// token 格式为 "partitionID:taskID"。
func ParseToken(token string) (partitionID int, taskID int64, err error) {
	return parseToken(token)
}
