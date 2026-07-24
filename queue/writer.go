package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

type writeRequest struct {
	data            []byte
	routingKey      string
	routingDeadline time.Time
	createdAt       time.Time // 零值表示由 flush 取当前时间
	attempt         int
	respoolOf       int64      // 非 0 表示本次写入是该老任务的 respool 重写
	taskID          int64      // flush 后填入
	done            chan error // respool 请求为 nil，无人等待结果
}

type taskWriter struct {
	pq *PhysicalQueue

	mu     sync.Mutex
	nextID int64

	reqCh   chan *writeRequest
	onFlush func(rows []*TaskRow) // sync match 回调

	stateMu sync.RWMutex
	started bool
	closed  bool
	ctx     context.Context
	done    chan struct{}
	startCh chan struct{}
	senders sync.WaitGroup
}

func newTaskWriter(pq *PhysicalQueue, nextTaskID int64, onFlush func([]*TaskRow)) *taskWriter {
	return &taskWriter{
		pq:      pq,
		nextID:  nextTaskID,
		reqCh:   make(chan *writeRequest, 256),
		onFlush: onFlush,
		done:    make(chan struct{}),
		startCh: make(chan struct{}),
	}
}

func (w *taskWriter) enqueue(data []byte, opts EnqueueOptions) (int64, error) {
	w.stateMu.Lock()
	if !w.started || w.closed {
		w.stateMu.Unlock()
		return 0, ErrQueueClosed
	}
	w.senders.Add(1)
	done := w.done
	w.stateMu.Unlock()

	req := &writeRequest{
		data: data, routingKey: opts.RoutingKey, routingDeadline: opts.RoutingDeadline,
		done: make(chan error, 1),
	}
	select {
	case w.reqCh <- req:
		w.senders.Done()
	case <-done:
		w.senders.Done()
		return 0, ErrQueueClosed
	}
	err := <-req.done
	return req.taskID, err
}

// tryEnqueueRespool 非阻塞地把 respool 重写请求投入写入批次。
// 该方法可能在 writer goroutine 自身（onFlush → tryDeliverToWaiters → nack）以及
// transitionMu 之下被调用，因此绝不能等待任何返回 channel；队列满或已关闭时返回
// false，由调用方降级为原地 redelivery。
func (w *taskWriter) tryEnqueueRespool(row *TaskRow, respoolOf int64) bool {
	// senders.Add 必须在 stateMu 之下、且确认未关闭后进行，与 enqueue 对齐：
	// 这样 close() 的 senders.Wait() 屏障同样覆盖 respool 生产者。否则 close()
	// 可能在本次发送前排空并返回，随后本次发送落入缓冲区却无人接收，
	// respooling 条目将永远得不到 finishRespool/abortRespool 结算。
	w.stateMu.RLock()
	ready := w.started && !w.closed
	if ready {
		w.senders.Add(1)
	}
	w.stateMu.RUnlock()
	if !ready {
		return false
	}
	defer w.senders.Done()

	req := &writeRequest{
		data:            row.Data,
		routingKey:      row.RoutingKey,
		routingDeadline: row.RoutingDeadline,
		createdAt:       row.CreatedAt,
		attempt:         row.Attempt,
		respoolOf:       respoolOf,
	}
	select {
	case w.reqCh <- req:
		return true
	default:
		return false
	}
}

func (w *taskWriter) run(ctx context.Context) error {
	w.stateMu.Lock()
	if w.closed {
		w.stateMu.Unlock()
		return ErrQueueClosed
	}
	if w.started {
		done := w.done
		w.stateMu.Unlock()
		select {
		case <-ctx.Done():
		case <-done:
		}
		return nil
	}
	w.started = true
	w.ctx = ctx
	close(w.startCh)
	w.stateMu.Unlock()

	defer w.close()

	for {
		select {
		case req := <-w.reqCh:
			if req == nil {
				continue
			}
			batch := []*writeRequest{req}
			drain := true
			for drain {
				select {
				case r := <-w.reqCh:
					if r == nil {
						continue
					}
					batch = append(batch, r)
				default:
					drain = false
				}
			}
			if err := w.flush(batch); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		case <-w.done:
			return nil
		}
	}
}

func (w *taskWriter) waitUntilStarted(ctx context.Context) error {
	select {
	case <-w.startCh:
		return nil
	case <-w.done:
		return ErrQueueClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *taskWriter) flush(batch []*writeRequest) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	startID := w.nextID
	rows := make([]*TaskRow, len(batch))
	for i, req := range batch {
		taskID := startID + int64(i)
		createdAt := req.createdAt
		if createdAt.IsZero() {
			createdAt = now
		}
		rows[i] = &TaskRow{
			TaskID:          taskID,
			QueueName:       w.pq.queueName,
			PartitionID:     w.pq.partitionID,
			RoutingKey:      req.routingKey,
			RoutingDeadline: req.routingDeadline,
			Data:            req.data,
			Attempt:         req.attempt,
			CreatedAt:       createdAt,
		}
		req.taskID = taskID
	}

	newNextID := startID + int64(len(batch))
	err := w.pq.ownership.createTasksAndAdvanceRange(
		w.ctx,
		w.pq.queueName,
		w.pq.partitionID,
		rows,
		newNextID,
	)
	if err != nil {
		for _, req := range batch {
			if req.done != nil {
				req.done <- err
			}
			// 新行没落库，老任务绝不能被 ack，落回 redelivery 保持可恢复。
			if req.respoolOf != 0 {
				w.pq.runtime.state.abortRespool(req.respoolOf)
			}
		}
		if errors.Is(err, ErrOwnershipLost) {
			return err
		}
		return nil
	}
	w.nextID = newNextID
	// 新行已落库，此时才允许 ack 老行并放行 ackLevel。
	for _, req := range batch {
		if req.respoolOf != 0 {
			w.pq.runtime.state.finishRespool(req.respoolOf)
		}
	}
	// 成功返回给写入方之前，先把新任务发布到本地可读状态。
	if w.onFlush != nil {
		w.onFlush(rows)
	}
	for _, req := range batch {
		if req.done != nil {
			req.done <- nil
		}
	}
	return nil
}

func (w *taskWriter) close() {
	w.stateMu.Lock()
	if w.closed {
		w.stateMu.Unlock()
		return
	}
	w.closed = true
	done := w.done
	w.stateMu.Unlock()

	close(done)
	w.senders.Wait()

	for {
		select {
		case req := <-w.reqCh:
			if req == nil {
				continue
			}
			if req.done != nil {
				req.done <- ErrQueueClosed
			}
			if req.respoolOf != 0 {
				w.pq.runtime.state.abortRespool(req.respoolOf)
			}
		default:
			return
		}
	}
}
