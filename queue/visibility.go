package queue

import (
	"context"
	"log/slog"
	"time"
)

const defaultVisScanInterval = 1 * time.Second

type visibilityScanner struct {
	pq      *PhysicalQueue
	runtime *taskRuntime
}

func newVisibilityScanner(pq *PhysicalQueue, runtime *taskRuntime) *visibilityScanner {
	return &visibilityScanner{
		pq:      pq,
		runtime: runtime,
	}
}

func (vs *visibilityScanner) scan(ctx context.Context) {
	ids, err := vs.pq.store.ResetTimedOutTasks(
		ctx,
		vs.pq.queueName,
		vs.pq.partitionID,
		vs.runtime.ackLevel(),
		vs.pq.visTimeout,
	)
	if err != nil {
		slog.Error("queue visibility scan failed", "queue", vs.pq.queueName, "partition_id", vs.pq.partitionID, "err", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	vs.runtime.moveTimedOutTasksToRedelivery(ids)
	vs.runtime.tryDeliverToWaiters()
}

func (vs *visibilityScanner) close() {
}
