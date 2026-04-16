package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type leaseController struct {
	pq *PhysicalQueue
	wg sync.WaitGroup
}

func newLeaseController(pq *PhysicalQueue) *leaseController {
	lc := &leaseController{pq: pq}
	lc.wg.Add(1)
	go lc.loop()
	return lc
}

func (lc *leaseController) loop() {
	defer lc.wg.Done()

	timer := time.NewTimer(defaultLeaseRenew)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			err := lc.pq.store.RenewLease(
				lc.pq.ctx,
				lc.pq.queueName,
				lc.pq.partitionID,
				lc.pq.leaseOwner,
				defaultLeaseTTL,
			)
			if err != nil {
				slog.Error("queue lease renew failed", "queue", lc.pq.queueName, "partition_id", lc.pq.partitionID, "err", err)
				lc.pq.cancel()
				return
			}
			timer.Reset(defaultLeaseRenew)
		case <-lc.pq.ctx.Done():
			return
		}
	}
}

func (lc *leaseController) wait() {
	lc.wg.Wait()
}

func (lc *leaseController) release() {
	err := lc.pq.store.ReleaseLease(
		context.Background(),
		lc.pq.queueName,
		lc.pq.partitionID,
		lc.pq.leaseOwner,
	)
	if err != nil && !errors.Is(err, ErrLeaseHeld) {
		slog.Error("queue lease release failed", "queue", lc.pq.queueName, "partition_id", lc.pq.partitionID, "err", err)
	}
}
