package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultBlockSize = 100

type writeRequest struct {
	data   []byte
	taskID int64 // flush 后填入
	done   chan error
}

type taskWriter struct {
	pq *PhysicalQueue

	mu        sync.Mutex
	nextID    int64
	maxID     int64
	blockSize int64

	reqCh   chan *writeRequest
	onFlush func(rows []*TaskRow) // sync match 回调

	stateMu sync.RWMutex
	started bool
	closed  bool
	ctx     context.Context
	done    chan struct{}
	startCh chan struct{}
}

func newTaskWriter(pq *PhysicalQueue, nextTaskID int64, onFlush func([]*TaskRow)) *taskWriter {
	return &taskWriter{
		pq:        pq,
		nextID:    nextTaskID,
		maxID:     nextTaskID,
		blockSize: defaultBlockSize,
		reqCh:     make(chan *writeRequest, 256),
		onFlush:   onFlush,
		done:      make(chan struct{}),
		startCh:   make(chan struct{}),
	}
}

func (w *taskWriter) enqueue(data []byte) (int64, error) {
	w.stateMu.RLock()
	started := w.started
	closed := w.closed
	done := w.done
	w.stateMu.RUnlock()

	if !started || closed {
		return 0, ErrQueueClosed
	}

	req := &writeRequest{data: data, done: make(chan error, 1)}
	select {
	case w.reqCh <- req:
	case <-done:
		return 0, ErrQueueClosed
	}
	select {
	case err := <-req.done:
		return req.taskID, err
	case <-done:
		select {
		case err := <-req.done:
			return req.taskID, err
		default:
			return 0, ErrQueueClosed
		}
	}
}

func (w *taskWriter) run(ctx context.Context) error {
	w.stateMu.Lock()
	if w.closed {
		w.stateMu.Unlock()
		return ErrQueueClosed
	}
	if w.started {
		w.stateMu.Unlock()
		<-ctx.Done()
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

	needed := int64(len(batch))
	if w.nextID+needed > w.maxID {
		if err := w.allocateRange(needed); err != nil {
			for _, req := range batch {
				req.done <- fmt.Errorf("allocate range: %w", err)
			}
			if errors.Is(err, ErrOwnershipLost) {
				return err
			}
			return nil
		}
	}

	now := time.Now()
	startID := w.nextID
	rows := make([]*TaskRow, len(batch))
	for i, req := range batch {
		taskID := startID + int64(i)
		rows[i] = &TaskRow{
			TaskID:      taskID,
			QueueName:   w.pq.queueName,
			PartitionID: w.pq.partitionID,
			Data:        req.data,
			CreatedAt:   now,
		}
		req.taskID = taskID
	}

	err := w.pq.store.CreateTasks(w.ctx, rows)
	if err != nil {
		for _, req := range batch {
			req.done <- err
		}
		return nil
	}
	w.nextID = startID + int64(len(batch))
	// 成功返回给写入方之前，先把新任务发布到本地可读状态。
	if w.onFlush != nil {
		w.onFlush(rows)
	}
	for _, req := range batch {
		req.done <- nil
	}
	return nil
}

// allocateRange 通过 ownershipState CAS 分配新的 taskID 范围。
// 每次成功后 rangeID 递增，确保多进程竞争下能检测到所有权丢失。
func (w *taskWriter) allocateRange(minSize int64) error {
	size := w.blockSize
	if size < minSize {
		size = minSize
	}
	newNextTaskID := w.nextID + size
	if err := w.pq.ownership.allocateTaskIDRange(w.ctx, w.pq.queueName, w.pq.partitionID, newNextTaskID); err != nil {
		return err
	}
	w.maxID = newNextTaskID
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
