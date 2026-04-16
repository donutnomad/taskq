package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type taskRuntime struct {
	pq    *PhysicalQueue
	state *deliveryState
}

func newTaskRuntime(pq *PhysicalQueue, ackLevel int64) *taskRuntime {
	return &taskRuntime{
		pq:    pq,
		state: newDeliveryState(ackLevel),
	}
}

func (rt *taskRuntime) setMaxReadLevel(level int64) {
	rt.state.setMaxReadLevel(level)
}

func (rt *taskRuntime) tryGetTask(ctx context.Context) (*TaskRow, string, bool, error) {
	for {
		if row := rt.state.takeRedelivery(); row != nil {
			task, token, err := rt.dispatchTask(ctx, row, false)
			if err == nil {
				return task, token, true, nil
			}
			if errors.Is(err, ErrTaskNotFound) {
				continue
			}
			rt.state.requeueRedeliveryFront(row)
			return nil, "", false, err
		}

		minTaskID, maxTaskID, ok := rt.state.nextReadWindow()
		if !ok {
			return nil, "", false, nil
		}

		tasks, err := rt.pq.store.GetTasks(
			ctx,
			rt.pq.queueName,
			rt.pq.partitionID,
			minTaskID,
			maxTaskID,
			1,
		)
		if err != nil {
			return nil, "", false, fmt.Errorf("get tasks: %w", err)
		}
		if len(tasks) == 0 {
			return nil, "", false, nil
		}

		row, token, err := rt.dispatchTask(ctx, tasks[0], true)
		if errors.Is(err, ErrTaskNotFound) {
			continue
		}
		if err != nil {
			return nil, "", false, err
		}
		return row, token, true, nil
	}
}

func (rt *taskRuntime) dispatchTask(ctx context.Context, row *TaskRow, advanceReadLevel bool) (*TaskRow, string, error) {
	dispatchAt := time.Now()
	if err := rt.pq.store.DispatchTask(
		ctx,
		rt.pq.queueName,
		rt.pq.partitionID,
		row.TaskID,
		dispatchAt,
	); err != nil {
		return nil, "", fmt.Errorf("dispatch task %d: %w", row.TaskID, err)
	}

	rt.state.markDispatched(row, dispatchAt, advanceReadLevel)
	return row, formatToken(rt.pq.partitionID, row.TaskID), nil
}

func (rt *taskRuntime) ack(taskID int64) error {
	return rt.state.ack(taskID)
}

func (rt *taskRuntime) nack(ctx context.Context, taskID int64) error {
	ot, err := rt.state.beginNack(taskID)
	if err != nil {
		return err
	}

	if err := rt.pq.store.UnDispatchTask(ctx, rt.pq.queueName, rt.pq.partitionID, taskID); err != nil {
		rt.state.restoreOutstanding(taskID, ot)
		return fmt.Errorf("undispatch task %d: %w", taskID, err)
	}

	rt.state.finishNack(ot)
	return nil
}

func (rt *taskRuntime) moveTimedOutTasksToRedelivery(taskIDs []int64) {
	for _, taskID := range taskIDs {
		rt.state.moveToRedelivery(taskID)
	}
}

func (rt *taskRuntime) ackLevel() int64 {
	return rt.state.ackLevelValue()
}

func (rt *taskRuntime) hasReadable() bool {
	return rt.state.hasReadable()
}

func (rt *taskRuntime) tryDeliverToWaiters() {
	for rt.pq.waiters.hasWaiters() {
		row, token, ok, err := rt.tryGetTask(rt.pq.ctx)
		if err != nil {
			slog.Error("queue failed to fetch task for waiter delivery", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "err", err)
			return
		}
		if !ok {
			return
		}

		ch, ok := rt.pq.waiters.pop()
		if !ok {
			if err := rt.nack(rt.pq.ctx, row.TaskID); err != nil {
				slog.Error("queue failed to return task after waiter disappeared", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "task_id", row.TaskID, "err", err)
			}
			return
		}

		if sendPollResult(ch, PollResult{Row: row, Token: token}) {
			continue
		}

		if err := rt.nack(rt.pq.ctx, row.TaskID); err != nil {
			slog.Error("queue failed to return task after waiter send failure", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "task_id", row.TaskID, "err", err)
		}
	}
}
