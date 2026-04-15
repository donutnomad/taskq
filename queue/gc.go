package queue

import (
	"context"
	"sync"
	"time"
)

const defaultGCInterval = 5 * time.Second

type taskGC struct {
	store       Store
	queueName   string
	partitionID int
	dbManager   *dbTaskManager
	ownership   *ownershipState

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newTaskGC(ctx context.Context, store Store, queueName string,
	partitionID int, ownership *ownershipState, dbManager *dbTaskManager) *taskGC {
	ctx, cancel := context.WithCancel(ctx)
	g := &taskGC{
		store:       store,
		queueName:   queueName,
		partitionID: partitionID,
		dbManager:   dbManager,
		ownership:   ownership,
		ctx:         ctx,
		cancel:      cancel,
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
	ackLevel := g.dbManager.getAckLevel()
	if ackLevel <= 0 {
		return
	}
	g.store.DeleteTasks(g.ctx, g.queueName, g.partitionID, ackLevel)

	// 通过 ownershipState CAS 同步 ackLevel 到 queue_info
	info, err := g.store.GetQueue(g.ctx, g.queueName, g.partitionID)
	if err != nil {
		return
	}
	if info.AckLevel < ackLevel {
		info.AckLevel = ackLevel
		g.ownership.updateQueue(g.ctx, info)
	}
}

func (g *taskGC) close() {
	g.cancel()
	g.wg.Wait()
}
