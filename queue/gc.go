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
	if _, err := g.pq.store.DeleteTasks(ctx, g.pq.queueName, g.pq.partitionID, ackLevel); err != nil {
		slog.Error("queue gc delete failed", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "ack_level", ackLevel, "err", err)
		return nil
	}

	// 通过 ownershipState CAS 同步 ackLevel 到 queue_info
	info, err := g.pq.store.GetQueue(ctx, g.pq.queueName, g.pq.partitionID)
	if err != nil {
		slog.Error("queue gc load queue info failed", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "err", err)
		return nil
	}
	if info.AckLevel < ackLevel {
		info.AckLevel = ackLevel
		if err := g.pq.ownership.updateQueue(ctx, info); err != nil {
			if errors.Is(err, ErrOwnershipLost) {
				return err
			}
			slog.Error("queue gc persist ack level failed", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "ack_level", ackLevel, "err", err)
		}
	}
	return nil
}

func (g *taskGC) close() {
}
