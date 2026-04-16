package queue

import (
	"context"
	"log"
	"sync"
	"time"
)

const defaultVisScanInterval = 1 * time.Second

type visibilityScanner struct {
	pq      *PhysicalQueue
	runtime *taskRuntime

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newVisibilityScanner(pq *PhysicalQueue, runtime *taskRuntime) *visibilityScanner {
	ctx, cancel := context.WithCancel(pq.ctx)
	vs := &visibilityScanner{
		pq:      pq,
		runtime: runtime,
		ctx:     ctx,
		cancel:  cancel,
	}
	vs.wg.Add(1)
	go vs.loop()
	return vs
}

func (vs *visibilityScanner) loop() {
	defer vs.wg.Done()
	timer := time.NewTimer(defaultVisScanInterval)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			vs.scan()
			timer.Reset(defaultVisScanInterval)
		case <-vs.ctx.Done():
			return
		}
	}
}

func (vs *visibilityScanner) scan() {
	ids, err := vs.pq.store.ResetTimedOutTasks(
		vs.ctx,
		vs.pq.queueName,
		vs.pq.partitionID,
		vs.runtime.ackLevel(),
		vs.pq.visTimeout,
	)
	if err != nil {
		log.Printf("queue: visibility scan failed for %s/%d: %v", vs.pq.queueName, vs.pq.partitionID, err)
		return
	}
	if len(ids) == 0 {
		return
	}
	vs.runtime.moveTimedOutTasksToRedelivery(ids)
	vs.runtime.tryDeliverToWaiters()
}

func (vs *visibilityScanner) close() {
	vs.cancel()
	vs.wg.Wait()
}
