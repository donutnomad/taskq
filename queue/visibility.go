package queue

import (
	"context"
	"sync"
	"time"
)

const defaultVisScanInterval = 1 * time.Second

type visibilityScanner struct {
	store       Store
	queueName   string
	partitionID int
	dbManager   *dbTaskManager
	visTimeout  time.Duration
	onReset     func() // 有任务被重置时调用（唤醒 waitingPollers）

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newVisibilityScanner(ctx context.Context, store Store, queueName string,
	partitionID int, dbManager *dbTaskManager, visTimeout time.Duration, onReset func()) *visibilityScanner {
	ctx, cancel := context.WithCancel(ctx)
	vs := &visibilityScanner{
		store:       store,
		queueName:   queueName,
		partitionID: partitionID,
		dbManager:   dbManager,
		visTimeout:  visTimeout,
		onReset:     onReset,
		ctx:         ctx,
		cancel:      cancel,
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
	ackLevel := vs.dbManager.getAckLevel()
	ids, err := vs.store.ResetTimedOutTasks(vs.ctx, vs.queueName, vs.partitionID,
		ackLevel, vs.visTimeout)
	if err != nil || len(ids) == 0 {
		return
	}
	for _, id := range ids {
		vs.dbManager.moveToRedelivery(id)
	}
	if vs.onReset != nil {
		vs.onReset()
	}
}

func (vs *visibilityScanner) close() {
	vs.cancel()
	vs.wg.Wait()
}
