package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const defaultGCInterval = 5 * time.Second

type taskGC struct {
	pq      *PhysicalQueue
	runtime *taskRuntime

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newTaskGC(pq *PhysicalQueue, runtime *taskRuntime) *taskGC {
	ctx, cancel := context.WithCancel(pq.ctx)
	g := &taskGC{
		pq:      pq,
		runtime: runtime,
		ctx:     ctx,
		cancel:  cancel,
	}
	g.wg.Add(1)
	go g.loop()
	return g
}

func (g *taskGC) loop() {
	defer g.wg.Done()
	timer := time.NewTimer(defaultGCInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			g.run()
			timer.Reset(defaultGCInterval)
		case <-g.ctx.Done():
			return
		}
	}
}

func (g *taskGC) run() {
	ackLevel := g.runtime.ackLevel()
	if ackLevel <= 0 {
		return
	}
	if _, err := g.pq.store.DeleteTasks(g.ctx, g.pq.queueName, g.pq.partitionID, ackLevel); err != nil {
		slog.Error("queue gc delete failed", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "ack_level", ackLevel, "err", err)
		return
	}

	// 通过 ownershipState CAS 同步 ackLevel 到 queue_info
	info, err := g.pq.store.GetQueue(g.ctx, g.pq.queueName, g.pq.partitionID)
	if err != nil {
		slog.Error("queue gc load queue info failed", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "err", err)
		return
	}
	if info.AckLevel < ackLevel {
		info.AckLevel = ackLevel
		if err := g.pq.ownership.updateQueue(g.ctx, info); err != nil {
			if errors.Is(err, ErrOwnershipLost) {
				g.pq.cancel()
				return
			}
			slog.Error("queue gc persist ack level failed", "queue", g.pq.queueName, "partition_id", g.pq.partitionID, "ack_level", ackLevel, "err", err)
		}
	}
}

func (g *taskGC) close() {
	g.cancel()
	g.wg.Wait()
}
