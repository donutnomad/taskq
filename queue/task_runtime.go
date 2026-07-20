package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type taskRuntime struct {
	pq             *PhysicalQueue
	state          *deliveryState
	transitionMu   sync.Mutex
	nextDeliveryID int64
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

func (rt *taskRuntime) tryGetTask(ctx context.Context, opts PollOptions) (*TaskRow, string, bool, error) {
	for {
		fallbackAt := time.Now().UTC()
		minTaskID, maxTaskID, ok := rt.state.nextReadWindow()
		if !ok {
			return nil, "", false, nil
		}

		tasks, err := rt.pq.store.GetTasksWithOptions(
			ctx,
			rt.pq.queueName,
			rt.pq.partitionID,
			minTaskID,
			maxTaskID,
			1,
			TaskQueryOptions{AcceptedRoutingKeys: opts.AcceptedRoutingKeys, RoutingFallbackAt: fallbackAt},
		)
		if err != nil {
			return nil, "", false, fmt.Errorf("get tasks: %w", err)
		}
		if len(tasks) == 0 {
			return nil, "", false, nil
		}

		row, token, err := rt.dispatchTask(ctx, tasks[0])
		if errors.Is(err, ErrTaskNotFound) {
			continue
		}
		if err != nil {
			return nil, "", false, err
		}
		return row, token, true, nil
	}
}

func (rt *taskRuntime) dispatchTask(ctx context.Context, row *TaskRow) (*TaskRow, string, error) {
	rt.transitionMu.Lock()
	defer rt.transitionMu.Unlock()

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

	row.DispatchedAt = &dispatchAt
	rt.nextDeliveryID++
	deliveryID := rt.nextDeliveryID
	rt.state.markDispatched(row, dispatchAt, deliveryID)
	return row, formatToken(rt.pq.queueName, rt.pq.partitionID, row.TaskID, rt.pq.ownershipEpoch, deliveryID), nil
}

func (rt *taskRuntime) ack(taskID, deliveryID int64) error {
	rt.transitionMu.Lock()
	defer rt.transitionMu.Unlock()
	return rt.state.ack(taskID, deliveryID)
}

func (rt *taskRuntime) nack(ctx context.Context, taskID, deliveryID int64) error {
	rt.transitionMu.Lock()
	defer rt.transitionMu.Unlock()

	ot, err := rt.state.beginNack(taskID, deliveryID)
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

func (rt *taskRuntime) resetTimedOutTasks(ctx context.Context, visTimeout time.Duration) error {
	rt.transitionMu.Lock()
	defer rt.transitionMu.Unlock()

	taskIDs, err := rt.pq.store.ResetTimedOutTasks(
		ctx,
		rt.pq.queueName,
		rt.pq.partitionID,
		VisibilityResetOptions{
			AckLevel:          rt.state.ackLevelValue(),
			VisibilityTimeout: visTimeout,
			AcknowledgedIDs:   rt.state.ackedTaskIDs(),
		},
	)
	if err != nil {
		return err
	}
	for _, taskID := range taskIDs {
		rt.state.moveToRedelivery(taskID)
	}
	return nil
}

func (rt *taskRuntime) ackLevel() int64 {
	return rt.state.ackLevelValue()
}

func (rt *taskRuntime) tryDeliverToWaiters() {
	for {
		opts, hasWaiters := rt.pq.waiters.pollOptions()
		if !hasWaiters {
			return
		}
		row, token, ok, err := rt.tryGetTask(rt.pq.ctx, opts)
		if err != nil {
			slog.Error("queue failed to fetch task for waiter delivery", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "err", err)
			return
		}
		if !ok {
			return
		}

		if !rt.pq.waiters.deliver(PollResult{Row: row, Token: token}) {
			parsed, parseErr := ParseTaskToken(token)
			if parseErr != nil {
				slog.Error("queue failed to parse internally generated task token", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "task_id", row.TaskID, "err", parseErr)
				return
			}
			if err := rt.nack(rt.pq.ctx, row.TaskID, parsed.DeliveryID); err != nil {
				slog.Error("queue failed to return task after waiter disappeared", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "task_id", row.TaskID, "err", err)
			}
			return
		}
	}
}
