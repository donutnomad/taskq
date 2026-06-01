package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

const defaultGCInterval = 5 * time.Second

type taskGC struct {
	pq      *PhysicalQueue
	runtime *taskRuntime
}

func newTaskGC(pq *PhysicalQueue, runtime *taskRuntime) *taskGC {
	return &taskGC{
		pq:      pq,
		runtime: runtime,
	}
}

func (g *taskGC) collect(ctx context.Context) error {
	ackLevel := g.runtime.ackLevel()
	if ackLevel <= 0 {
		return nil
	}
	deleted, err := g.pq.ownership.advanceAckLevelAndDelete(ctx, g.pq.queueName, g.pq.partitionID, ackLevel)
	if err != nil {
		if errors.Is(err, ErrOwnershipLost) {
			return err
		}
		slog.Error("queue gc advance ack level failed", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "ack_level", ackLevel, "err", err)
		return nil
	}
	if deleted > 0 {
		slog.Debug("queue gc deleted tasks", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "ack_level", ackLevel, "deleted", deleted)
	}
	return nil
}

func (g *taskGC) close() {
}
