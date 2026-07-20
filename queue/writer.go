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
	taskID          int64 // flush 后填入
	done            chan error
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
		rows[i] = &TaskRow{
			TaskID:          taskID,
			QueueName:       w.pq.queueName,
			PartitionID:     w.pq.partitionID,
			RoutingKey:      req.routingKey,
			RoutingDeadline: req.routingDeadline,
			Data:            req.data,
			CreatedAt:       now,
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
			req.done <- err
		}
		if errors.Is(err, ErrOwnershipLost) {
			return err
		}
		return nil
	}
	w.nextID = newNextID
	// 成功返回给写入方之前，先把新任务发布到本地可读状态。
	if w.onFlush != nil {
		w.onFlush(rows)
	}
	for _, req := range batch {
		req.done <- nil
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
			if req != nil {
				req.done <- ErrQueueClosed
			}
		default:
			return
		}
	}
}
