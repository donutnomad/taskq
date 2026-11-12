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
	// maxAttempts 为投递次数上限，0 表示不限次数。
	maxAttempts int
}

func newTaskRuntime(pq *PhysicalQueue, ackLevel int64, maxAttempts int) *taskRuntime {
	return &taskRuntime{
		pq:          pq,
		state:       newDeliveryState(ackLevel),
		maxAttempts: maxAttempts,
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
	// 不随 poll 的 ctx 取消：取消可能在 UPDATE 已提交后才被驱动报告，此时行在库里已隐藏而内存不知道
	// 这次投递，任务要等可见性超时才重新出现。投递完成后由调用方按 ctx 决定交付还是归还租约。
	if err := rt.pq.store.DispatchTask(
		context.WithoutCancel(ctx),
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

// nack 处理消费方拒绝：以新 taskID 重写任务，让 ackLevel 不被反复失败的任务钉死。
// 老行保持 dispatched_at 非空，对 poller 天然不可见，因此这里不调用 UnDispatchTask。
func (rt *taskRuntime) nack(ctx context.Context, taskID, deliveryID int64) error {
	rt.transitionMu.Lock()
	defer rt.transitionMu.Unlock()

	ot, err := rt.state.beginRespool(taskID, deliveryID)
	if err != nil {
		return err
	}
	if rt.tryRespoolLocked(taskID, ot) {
		return nil
	}
	return rt.degradeToRedeliveryLocked(ctx, taskID, ot)
}

// degradeToRedeliveryLocked 让 respooling 中的老行重新对 poller 可见并落回 redelivery，
// 即 respool 之前既有的原地重投递路径。调用方必须持有 transitionMu。
func (rt *taskRuntime) degradeToRedeliveryLocked(ctx context.Context, taskID int64, ot *outstandingTask) error {
	if err := rt.pq.store.UnDispatchTask(ctx, rt.pq.queueName, rt.pq.partitionID, taskID); err != nil {
		rt.state.restoreOutstanding(taskID, ot)
		return fmt.Errorf("undispatch task %d: %w", taskID, err)
	}
	rt.state.abortRespool(taskID)
	return nil
}

// returnTask 把任务原地放回可投递状态，不消耗 attempt，也不重写新行。
// 仅用于任务从未交付给消费者的瞬时错误：waiter 在投递瞬间消失，或
// 上层归还未使用的租约（见 PhysicalQueue.ReturnLease）。
func (rt *taskRuntime) returnTask(ctx context.Context, taskID, deliveryID int64) error {
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

// tryRespoolLocked 以更高的 taskID 重写任务，调用方必须持有 transitionMu 且任务已在
// respooling 中。返回 false 表示重写请求投递不出去，需要调用方降级为原地 redelivery。
func (rt *taskRuntime) tryRespoolLocked(taskID int64, ot *outstandingTask) bool {
	attempt := ot.row.Attempt + 1
	if rt.maxAttempts > 0 && attempt >= rt.maxAttempts {
		slog.Error("queue dropping task after exhausting delivery attempts",
			"queue", rt.pq.queueName, "partition_id", rt.pq.partitionID,
			"task_id", taskID, "attempts", attempt, "max_attempts", rt.maxAttempts)
		rt.state.finishRespool(taskID)
		return true
	}

	next := &TaskRow{
		QueueName:       ot.row.QueueName,
		PartitionID:     ot.row.PartitionID,
		RoutingKey:      ot.row.RoutingKey,
		RoutingDeadline: ot.row.RoutingDeadline,
		Data:            ot.row.Data,
		Attempt:         attempt,
		CreatedAt:       ot.row.CreatedAt,
	}
	// 必须是非阻塞投递：本方法可能运行在 writer goroutine 自身。
	return rt.pq.writer.tryEnqueueRespool(next, taskID)
}

// visibilityResetScanLimit 限制单轮可见性扫描重置并 respool 的任务条数。
// resetTimedOutTasks 全程持有 transitionMu，且循环内对每条超时任务各做一次
// DispatchTask（重新隐藏老行）的独立 DB 往返；消费者集群批量崩溃时超时任务可达
// 数万条，无上限会把整个分区（所有 poll、writer goroutine、进而所有阻塞在
// writer 上的 AddTask）挡住数秒。可见性扫描是每秒一次的周期任务，单轮封顶 +
// 下轮继续即可排空积压，同时把单轮锁持有时间限制在亚秒级。取值与 store 的
// visibilityResetBatch 对齐，使一轮扫描至多触发一条批量重置 UPDATE。
const visibilityResetScanLimit = 500

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
			AcknowledgedIDs:   rt.state.resetExclusionIDs(),
			MaxReset:          visibilityResetScanLimit,
		},
	)
	if err != nil {
		return err
	}
	for _, taskID := range taskIDs {
		ot, ok := rt.state.moveToRespooling(taskID)
		if !ok {
			// 本进程不认识该投递（例如重启后重新接管），保持既有的原地重投递语义。
			continue
		}
		// 重置已经把老行放回可见状态，respool 前必须重新隐藏它，
		// 否则老行与重写出的新行会被同时投递，超时一次就翻倍一次。
		if err := rt.pq.store.DispatchTask(ctx, rt.pq.queueName, rt.pq.partitionID, taskID, time.Now()); err != nil {
			slog.Error("queue failed to hide timed out task before respool", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "task_id", taskID, "err", err)
			rt.state.abortRespool(taskID)
			continue
		}
		if rt.tryRespoolLocked(taskID, ot) {
			continue
		}
		if err := rt.degradeToRedeliveryLocked(ctx, taskID, ot); err != nil {
			slog.Error("queue failed to return timed out task after respool was rejected", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "task_id", taskID, "err", err)
		}
	}
	return nil
}

// skipRetiredTasks 让 ackLevel 越过已被 respool 删除的老行。本进程 respool 的老行在 finishRespool 时
// 已记为 acked；接管分区的新进程没有这份内存，只能从库里看出这些 ID 没有行：ackLevel 之上最小的现存
// task_id 之前的 ID（不超过已落库的 maxReadLevel）都已删除。
func (rt *taskRuntime) skipRetiredTasks(ctx context.Context) error {
	ackLevel, maxReadLevel := rt.state.ackLevelValue(), rt.state.maxReadLevelValue()
	if ackLevel >= maxReadLevel {
		return nil
	}
	minTaskID, found, err := rt.pq.store.MinTaskIDAfter(ctx, rt.pq.queueName, rt.pq.partitionID, ackLevel)
	if err != nil {
		return err
	}
	upTo := maxReadLevel
	if found {
		upTo = min(minTaskID-1, maxReadLevel)
	}
	if upTo > ackLevel {
		rt.state.skipAbsent(upTo)
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
			if err := rt.returnTask(rt.pq.ctx, row.TaskID, parsed.DeliveryID); err != nil {
				slog.Error("queue failed to return task after waiter disappeared", "queue", rt.pq.queueName, "partition_id", rt.pq.partitionID, "task_id", row.TaskID, "err", err)
			}
			return
		}
	}
}
