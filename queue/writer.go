package queue

import (
	"context"
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
	store       Store
	queueName   string
	partitionID int
	ownership   *ownershipState

	mu        sync.Mutex
	nextID    int64
	maxID     int64
	blockSize int64

	reqCh   chan *writeRequest
	onFlush func(rows []*TaskRow) // sync match 回调

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newTaskWriter(ctx context.Context, store Store, queueName string,
	partitionID int, ownership *ownershipState, nextTaskID int64, onFlush func([]*TaskRow)) *taskWriter {
	ctx, cancel := context.WithCancel(ctx)
	w := &taskWriter{
		store:       store,
		queueName:   queueName,
		partitionID: partitionID,
		ownership:   ownership,
		nextID:      nextTaskID,
		maxID:       nextTaskID,
		blockSize:   defaultBlockSize,
		reqCh:       make(chan *writeRequest, 256),
		onFlush:     onFlush,
		ctx:         ctx,
		cancel:      cancel,
	}
	w.wg.Add(1)
	go w.loop()
	return w
}

func (w *taskWriter) enqueue(data []byte) (int64, error) {
	req := &writeRequest{data: data, done: make(chan error, 1)}
	select {
	case w.reqCh <- req:
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	}
	select {
	case err := <-req.done:
		return req.taskID, err
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	}
}

func (w *taskWriter) loop() {
	defer w.wg.Done()
	for {
		select {
		case req := <-w.reqCh:
			batch := []*writeRequest{req}
			drain := true
			for drain {
				select {
				case r := <-w.reqCh:
					batch = append(batch, r)
				default:
					drain = false
				}
			}
			w.flush(batch)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *taskWriter) flush(batch []*writeRequest) {
	w.mu.Lock()
	defer w.mu.Unlock()

	needed := int64(len(batch))
	if w.nextID+needed > w.maxID {
		if err := w.allocateRange(needed); err != nil {
			for _, req := range batch {
				req.done <- fmt.Errorf("allocate range: %w", err)
			}
			return
		}
	}

	now := time.Now()
	rows := make([]*TaskRow, len(batch))
	for i, req := range batch {
		rows[i] = &TaskRow{
			TaskID:      w.nextID,
			QueueName:   w.queueName,
			PartitionID: w.partitionID,
			Data:        req.data,
			CreatedAt:   now,
		}
		req.taskID = w.nextID
		w.nextID++
	}

	err := w.store.CreateTasks(w.ctx, rows)
	for _, req := range batch {
		req.done <- err
	}

	if err == nil && w.onFlush != nil {
		w.onFlush(rows)
	}
}

// allocateRange 通过 ownershipState CAS 分配新的 taskID 范围。
// 每次成功后 rangeID 递增，确保多进程竞争下能检测到所有权丢失。
func (w *taskWriter) allocateRange(minSize int64) error {
	size := w.blockSize
	if size < minSize {
		size = minSize
	}
	newNextTaskID := w.nextID + size
	cur, err := w.store.GetQueue(w.ctx, w.queueName, w.partitionID)
	if err != nil {
		return err
	}
	info := &QueueInfo{
		QueueName:   w.queueName,
		PartitionID: w.partitionID,
		AckLevel:    cur.AckLevel,
		NextTaskID:  newNextTaskID,
	}
	if err := w.ownership.updateQueue(w.ctx, info); err != nil {
		return err
	}
	w.maxID = newNextTaskID
	return nil
}

func (w *taskWriter) close() {
	w.cancel()
	w.wg.Wait()
}
