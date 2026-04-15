package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type outstandingTask struct {
	row        *TaskRow
	ackToken   string
	dispatchAt time.Time
}

type dbTaskManager struct {
	mu          sync.Mutex
	store       Store
	queueName   string
	partitionID int
	visTimeout  time.Duration

	ackLevel     int64
	readLevel    int64
	maxReadLevel int64

	outstanding map[int64]*outstandingTask
	redelivery  []*TaskRow

	// ack 完成的 taskID 集合，用于连续推进
	acked map[int64]bool
}

func newDBTaskManager(store Store, queueName string, partitionID int,
	ackLevel int64, visTimeout time.Duration) *dbTaskManager {
	return &dbTaskManager{
		store:       store,
		queueName:   queueName,
		partitionID: partitionID,
		visTimeout:  visTimeout,
		ackLevel:    ackLevel,
		readLevel:   ackLevel,
		outstanding: make(map[int64]*outstandingTask),
		acked:       make(map[int64]bool),
	}
}

func (dm *dbTaskManager) setMaxReadLevel(level int64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if level > dm.maxReadLevel {
		dm.maxReadLevel = level
	}
}

// getTask 尝试获取一个任务（非阻塞），优先 redelivery，其次 DB。
func (dm *dbTaskManager) getTask(ctx context.Context) (*TaskRow, bool) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// 优先 redelivery
	if len(dm.redelivery) > 0 {
		row := dm.redelivery[0]
		dm.redelivery = dm.redelivery[1:]
		if err := dm.dispatch(ctx, row); err != nil {
			// dispatch 失败，放回 redelivery 头部
			dm.redelivery = append([]*TaskRow{row}, dm.redelivery...)
			log.Printf("queue: dispatch redelivery DB error: %v", err)
			return nil, false
		}
		return row, true
	}

	// 从 DB 读取
	if dm.readLevel >= dm.maxReadLevel {
		return nil, false
	}

	tasks, err := dm.store.GetTasks(ctx, dm.queueName, dm.partitionID,
		dm.readLevel, dm.maxReadLevel, 1)
	if err != nil {
		log.Printf("queue: getTask DB error: %v", err)
		return nil, false
	}
	if len(tasks) == 0 {
		return nil, false
	}

	row := tasks[0]
	dm.readLevel = row.TaskID
	if err := dm.dispatch(ctx, row); err != nil {
		log.Printf("queue: dispatch DB error: %v", err)
		return nil, false
	}
	return row, true
}

func (dm *dbTaskManager) dispatch(ctx context.Context, row *TaskRow) error {
	now := time.Now()
	if err := dm.store.DispatchTask(ctx, dm.queueName, dm.partitionID, row.TaskID, now); err != nil {
		return err
	}
	dm.outstanding[row.TaskID] = &outstandingTask{
		row:        row,
		ackToken:   fmt.Sprintf("%d:%d", dm.partitionID, row.TaskID),
		dispatchAt: now,
	}
	return nil
}

func (dm *dbTaskManager) ack(ctx context.Context, taskID int64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	delete(dm.outstanding, taskID)

	// 如果 taskID <= ackLevel（redelivery 任务被重消费后 ack），直接删 DB 行
	if taskID <= dm.ackLevel {
		dm.store.DeleteTask(ctx, dm.queueName, dm.partitionID, taskID)
		return
	}

	dm.acked[taskID] = true
	dm.advanceAckLevel()
}

func (dm *dbTaskManager) advanceAckLevel() {
	for {
		next := dm.ackLevel + 1
		// 如果 next 在 outstanding 中（还没处理完），停止
		if _, inFlight := dm.outstanding[next]; inFlight {
			break
		}
		// 如果 next 在 redelivery 中，必须阻塞推进（防止 GC 删除未消费的任务）
		if dm.inRedelivery(next) {
			break
		}
		// 如果 next 已 ack，推进
		if dm.acked[next] {
			dm.ackLevel = next
			delete(dm.acked, next)
			continue
		}
		// next 还没被读取到（readLevel 之后），停止
		break
	}
}

func (dm *dbTaskManager) inRedelivery(taskID int64) bool {
	for _, r := range dm.redelivery {
		if r.TaskID == taskID {
			return true
		}
	}
	return false
}

func (dm *dbTaskManager) nack(ctx context.Context, taskID int64) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	ot, ok := dm.outstanding[taskID]
	if !ok {
		return
	}
	delete(dm.outstanding, taskID)

	// DB 中重置 dispatched_at
	dm.store.UnDispatchTask(ctx, dm.queueName, dm.partitionID, taskID)

	// 放入 redelivery
	dm.redelivery = append(dm.redelivery, ot.row)
}

// moveToRedelivery 原子地将超时任务从 outstanding 移入 redelivery（visibilityScanner 用）。
// 如果任务已被 ack/nack 移除 outstanding，返回 false 不做任何操作。
func (dm *dbTaskManager) moveToRedelivery(taskID int64) bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	ot, ok := dm.outstanding[taskID]
	if !ok {
		return false
	}
	delete(dm.outstanding, taskID)
	dm.redelivery = append(dm.redelivery, ot.row)
	return true
}

// getAckLevel 返回当前 ackLevel
func (dm *dbTaskManager) getAckLevel() int64 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.ackLevel
}

// hasReadable 检查是否有未读任务
func (dm *dbTaskManager) hasReadable() bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return len(dm.redelivery) > 0 || dm.readLevel < dm.maxReadLevel
}
