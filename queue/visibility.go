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
	defer vs.runtime.tryDeliverToWaiters()
	if err := vs.runtime.resetTimedOutTasks(ctx, vs.pq.visTimeout); err != nil {
		slog.Error("queue visibility scan failed", "queue", vs.pq.queueName, "partition_id", vs.pq.partitionID, "err", err)
	}
}

func (vs *visibilityScanner) close() {
}
