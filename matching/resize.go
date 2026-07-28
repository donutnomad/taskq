package matching

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const (
	// defaultResizeQuiesceDelay 是「写收敛静默期」（缩容）与「read 传播期」（扩容）的默认时长。
	// 必须 > partitionConfigTTL：配置变更只靠各节点的 TTL 缓存过期来传播，
	// 静默期短于 TTL 就等于在滞后节点仍按旧配置写高位分区时继续推进流程。
	defaultResizeQuiesceDelay = partitionConfigTTL + time.Second
	// defaultResizeStablePeriod 是 drain 判定必须「连续稳定」持续的时长。
	// 必须 > partitionGCInterval：DB 的 ack_level 由 GC 每 partitionGCInterval 持久化一次，
	// 是内存 ackLevel 的滞后下界。只有稳定跨过一个完整 GC 周期，才能排除
	// 「瞬时读到已排空、随后滞后的写/ack 状态才落库」的误判。
	defaultResizeStablePeriod = partitionGCInterval + time.Second
	// defaultResizeDrainTimeout 是 drain 轮询的总超时。反复失败的 poison 任务
	// （MaxAttempts=0 默认不限次重投）会让高位分区永远排不空，必须有上界并上报卡点。
	defaultResizeDrainTimeout = 2 * time.Minute
)

// ErrPartitionDrainTimeout 表示缩容时高位分区在超时内未能排空（或未能稳定排空）。
// 出现该错误时 read 分区数**没有**被减小，数据不会被孤立，修复卡住的任务后可重试缩容。
var ErrPartitionDrainTimeout = errors.New("partition drain timeout")

// ResizePartitions 安全地把 queueName 的分区数调整为 target（read 与 write 最终都等于 target）。
//
// 扩容（target > 当前 read）：先加 read → 等待传播 → 再加 write。
// 缩容（target < 当前 read）：只减 write → 写收敛静默期 → drain 稳定确认 → 才减 read。
//
// 缩容路径的四步严格有序、不可压缩：跳过静默期或 drain 直接减 read 会让仍有任务的高位分区
// 失去 owner——任务永不被消费、GC 也停摆，即静默丢数据。因此**绝不要**为了缩容直接调用
// Store.SetPartitions 减小 read，必须走本编排器。
//
// 缩容中途失败（drain 超时、context 取消、DB 错误）时停在 (read=旧值, write=target)：
// 这是安全中间态——高位分区停写但仍被 own、仍可 poll/ack、GC 仍在跑，可直接重试。
func (s *Server) ResizePartitions(ctx context.Context, queueName string, target int) error {
	return s.mgr.resizePartitions(ctx, queueName, target)
}

func (pm *partitionManager) resizePartitions(ctx context.Context, queueName string, target int) error {
	if queueName == "" {
		return fmt.Errorf("matching: resize partitions: queue name is required")
	}
	if target < 1 {
		return fmt.Errorf("matching: resize partitions: target must be >= 1, got %d", target)
	}

	// 当前值必须直读 store，绝不能用 TTL 缓存：滞后的缓存会让编排基于过期的 read/write
	// 决策（例如把「减 read」误判成 no-op，或跳过 drain），直接导致高位分区被孤立。
	curRead, curWrite, err := pm.store.ResolvePartitions(ctx, queueName)
	if err != nil {
		return fmt.Errorf("matching: resize partitions %q: resolve current: %w", queueName, err)
	}

	switch {
	case target > curRead:
		return pm.growPartitions(ctx, queueName, target, curWrite)
	case target < curRead:
		return pm.shrinkPartitions(ctx, queueName, target, curRead)
	case curWrite != target:
		// read 已经是 target，只是 write 落后（上一次扩容中断在第 1 步）：补齐 write 即可。
		return pm.setPartitions(ctx, queueName, target, target)
	default:
		return nil
	}
}

// growPartitions 扩容：先 read 后 write。
// 先加 read 保证新分区在被写之前就已经被某个节点 own 并可被 poll，
// 任何落到新分区的任务都一定有人消费。
func (pm *partitionManager) growPartitions(ctx context.Context, queueName string, target, curWrite int) error {
	// 1) 只加 read。新分区此刻还在 [0, write) 之外，不会有任务写进去。
	if err := pm.setPartitions(ctx, queueName, target, curWrite); err != nil {
		return err
	}
	// 2) 等 > partitionConfigTTL，让各 matching 节点 reconcile own 新分区、frontend 刷新快照。
	if err := sleepCtx(ctx, pm.resizeQuiesceDelay); err != nil {
		return fmt.Errorf("matching: resize partitions %q: propagate new read: %w", queueName, err)
	}
	// 3) 再加 write，写开始命中新分区。
	return pm.setPartitions(ctx, queueName, target, target)
}

// shrinkPartitions 缩容：只减 write → 写收敛静默期 → drain 稳定 → 才减 read。
// 四步之间的等待不可省略、不可压缩，理由见 ResizePartitions 注释。
func (pm *partitionManager) shrinkPartitions(ctx context.Context, queueName string, target, curRead int) error {
	// 1) 只减 write，read 保持不变。matching 立刻按新 numWrite 硬拒 partID >= target 的写；
	//    高位分区仍在 [0, read) 内，仍被 own、仍可 poll/ack、GC 仍在推进 ack_level。
	if err := pm.setPartitions(ctx, queueName, curRead, target); err != nil {
		return err
	}

	// 2) 写收敛静默期：等滞后 frontend 的分区快照过期 + 在途 AddTask RPC 落定。
	//    这一步是「停止写高位分区」从最终一致收敛为全局事实的唯一保证。
	if err := sleepCtx(ctx, pm.resizeQuiesceDelay); err != nil {
		return fmt.Errorf("matching: resize partitions %q: write quiesce: %w", queueName, err)
	}

	// 3) drain：确认 [target, curRead) 全部排空，且稳定持续 > 一个 GC 周期。
	if err := pm.waitPartitionsDrained(ctx, queueName, target, curRead); err != nil {
		return err
	}

	// 4) 全部排空后才减 read，此时卸载高位分区不会遗留任何任务。
	//    减 read 前重新确认配置仍是本次编排在第 1 步写下的那个：Store.SetPartitions 是
	//    无条件 upsert，若期间有人（并发的扩容/手工改配置）把 read 调大，直接写回
	//    read=target 会把新增的高位分区连同其中的任务一起孤立。宁可报错让人重来。
	nowRead, nowWrite, err := pm.store.ResolvePartitions(ctx, queueName)
	if err != nil {
		return fmt.Errorf("matching: resize partitions %q: reverify before shrinking read: %w", queueName, err)
	}
	if nowRead == target && nowWrite == target {
		// 已经有人（并发的同目标缩容）完成了这一步，结果一致，直接认为成功。
		return nil
	}
	if nowRead != curRead || nowWrite != target {
		return fmt.Errorf(
			"matching: resize partitions %q: config changed during drain (expected read=%d write=%d, got read=%d write=%d), aborting before shrinking read",
			queueName, curRead, target, nowRead, nowWrite)
	}
	return pm.setPartitions(ctx, queueName, target, target)
}

// setPartitions 写入新配置并立刻失效本节点缓存。
// 本节点的闸门（AddTask 按 numWrite）因此无需再等 TTL；其它节点仍靠 TTL 过期感知，
// 这正是静默期存在的理由——本地失效只是加速，不能替代静默期。
func (pm *partitionManager) setPartitions(ctx context.Context, queueName string, read, write int) error {
	if err := pm.store.SetPartitions(ctx, queueName, read, write); err != nil {
		return fmt.Errorf("matching: set partitions %q read=%d write=%d: %w", queueName, read, write, err)
	}
	pm.invalidateConfigCache(queueName)
	slog.Info("matching partition config updated", "queue", queueName, "num_read", read, "num_write", write)
	return nil
}

func (pm *partitionManager) invalidateConfigCache(queueName string) {
	pm.configMu.Lock()
	delete(pm.configCache, queueName)
	pm.configMu.Unlock()
}

// partitionDrainState 是某个分区一次排空探测的结果，同时携带诊断信息。
type partitionDrainState struct {
	partitionID int
	// materialized 为 false 表示 queue_info 行不存在（该分区从未被写过）。
	materialized bool
	ackLevel     int64
	nextTaskID   int64
	drained      bool
}

// partitionDrainState 判定单个分区是否已排空。
//
// 判定式 ack_level == next_task_id - 1 意为「所有已分配出去的 taskID 都已连续确认」。
// DB 的 ack_level 由 GC 周期性持久化，是内存 ackLevel 的**滞后下界**，因此该判定保守安全：
// 只会把「其实已排空」误判为「未排空」，绝不会反过来。任何 dispatched-未 ack、
// respooling 中、或等待重投的任务都会把 ackLevel 钉在其之前。
func (pm *partitionManager) partitionDrainState(ctx context.Context, queueName string, partitionID int) (partitionDrainState, error) {
	info, err := pm.store.GetQueue(ctx, queueName, partitionID)
	if errors.Is(err, sql.ErrNoRows) {
		// queue_info 行从未物化 → 该分区从未被 own/写入过 → 没有任何数据，视为已排空。
		return partitionDrainState{partitionID: partitionID, drained: true}, nil
	}
	if err != nil {
		return partitionDrainState{partitionID: partitionID}, fmt.Errorf("matching: drain check %s/%d: %w", queueName, partitionID, err)
	}
	return partitionDrainState{
		partitionID:  partitionID,
		materialized: true,
		ackLevel:     info.AckLevel,
		nextTaskID:   info.NextTaskID,
		drained:      info.AckLevel == info.NextTaskID-1,
	}, nil
}

// isPartitionDrained 判定分区是否已排空，见 partitionDrainState。
func (pm *partitionManager) isPartitionDrained(ctx context.Context, queueName string, partitionID int) (bool, error) {
	state, err := pm.partitionDrainState(ctx, queueName, partitionID)
	if err != nil {
		return false, err
	}
	return state.drained, nil
}

// waitPartitionsDrained 轮询 [from, to) 直到全部排空、且该状态连续稳定持续
// resizeStablePeriod（> GC 间隔）。任一分区回到未排空则稳定计时归零。
// 超时返回 ErrPartitionDrainTimeout 并上报卡住的分区号与 ack_level/next_task_id。
func (pm *partitionManager) waitPartitionsDrained(ctx context.Context, queueName string, from, to int) error {
	start := time.Now()
	deadline := start.Add(pm.resizeDrainTimeout)
	interval := pm.drainPollInterval()
	var stableSince time.Time

	for {
		var blocker *partitionDrainState
		for partitionID := from; partitionID < to; partitionID++ {
			state, err := pm.partitionDrainState(ctx, queueName, partitionID)
			if err != nil {
				// DB 探测失败绝不能当作「已排空」放行，直接失败、保留旧 read。
				return err
			}
			if !state.drained {
				blocker = &state
				break
			}
		}

		now := time.Now()
		if blocker == nil {
			if stableSince.IsZero() {
				stableSince = now
			}
			if now.Sub(stableSince) >= pm.resizeStablePeriod {
				return nil
			}
		} else {
			stableSince = time.Time{}
		}

		if !now.Before(deadline) {
			if blocker != nil {
				return fmt.Errorf(
					"%w: queue %q partition %d not drained after %s: ack_level=%d next_task_id=%d, oldest unacked task_id=%d",
					ErrPartitionDrainTimeout, queueName, blocker.partitionID,
					now.Sub(start).Round(time.Millisecond),
					blocker.ackLevel, blocker.nextTaskID, blocker.ackLevel+1)
			}
			return fmt.Errorf(
				"%w: queue %q partitions [%d,%d) drained but not stable for %s within %s",
				ErrPartitionDrainTimeout, queueName, from, to, pm.resizeStablePeriod,
				now.Sub(start).Round(time.Millisecond))
		}

		if err := sleepCtx(ctx, interval); err != nil {
			return fmt.Errorf("matching: resize partitions %q: drain wait: %w", queueName, err)
		}
	}
}

// drainPollInterval 由稳定期推导，避免再引入一个配置项：
// 保证一个稳定期内至少探测若干次，同时不高频打 DB。
func (pm *partitionManager) drainPollInterval() time.Duration {
	return min(max(pm.resizeStablePeriod/5, 20*time.Millisecond), time.Second)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
