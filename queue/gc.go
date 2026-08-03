package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

const defaultGCInterval = 5 * time.Second

// ackLevelStallWarnAfter 是 ackLevel 停滞多久开始告警的阈值。
// ackLevel 卡住意味着 GC 无法删除任何行（GC 只删 task_id <= ackLevel），tasks 表持续增长，
// 缩容的 drain 判定也永远不满足；这两者都是静默的，必须让它可被发现。
const ackLevelStallWarnAfter = time.Minute

type taskGC struct {
	pq      *PhysicalQueue
	runtime *taskRuntime

	// 上一轮观察到的 ackLevel 与其首次被观察到的时刻，用于识别停滞。
	// 仅由 GC 单 goroutine 周期性调用，无需加锁。
	lastAckLevel int64
	lastAdvance  time.Time
	warned       bool
}

func newTaskGC(pq *PhysicalQueue, runtime *taskRuntime) *taskGC {
	return &taskGC{
		pq:      pq,
		runtime: runtime,
	}
}

// reportAckLevelStall 在 ackLevel 长时间不推进时报告挡路的任务及其成因。
// 典型场景：一条带 routing key 的任务长期无人订阅该 key（worker 未上线 / key 写错 /
// 未设 RoutingDeadline），它会一直挡住水位，GC 因此完全停摆。
func (g *taskGC) reportAckLevelStall(ackLevel int64) {
	now := time.Now()
	if ackLevel != g.lastAckLevel || g.lastAdvance.IsZero() {
		g.lastAckLevel = ackLevel
		g.lastAdvance = now
		g.warned = false
		return
	}
	stalledFor := now.Sub(g.lastAdvance)
	if stalledFor < ackLevelStallWarnAfter || g.warned {
		return
	}
	blocker := g.runtime.state.ackLevelBlocker(now)
	if blocker.state == "" {
		return
	}
	g.warned = true
	slog.Warn("queue ack level stalled, gc cannot reclaim tasks",
		"queue", g.pq.queueName, "partition_id", g.pq.partitionID,
		"ack_level", ackLevel, "stalled_for", stalledFor,
		"blocked_by_task_id", blocker.taskID, "blocker_state", blocker.state,
		"blocker_routing_key", blocker.routingKey, "blocker_age", blocker.dispatchAge)
}

func (g *taskGC) collect(ctx context.Context) error {
	ackLevel := g.runtime.ackLevel()
	g.reportAckLevelStall(ackLevel)
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
