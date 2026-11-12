package queue

import (
	"sync"
	"time"
)

type outstandingTask struct {
	row        *TaskRow
	dispatchAt time.Time
	deliveryID int64
}

type deliveryState struct {
	mu sync.Mutex

	ackLevel     int64
	maxReadLevel int64

	outstanding map[int64]*outstandingTask
	// respooling 中的任务正在以新 taskID 重写，新行落库前必须阻止 ackLevel 推进。
	respooling map[int64]*outstandingTask
	// uncertain 是 outstanding 里库中可见性不确定的任务：放回可见状态的 UPDATE 报错后任务回到 outstanding，
	// 而 UPDATE 可能已经提交（驱动在提交后才报告取消或超时）。窗口记账把它们算作未知，照常读库；任务再次
	// 投递、确认或离开 outstanding 时移出。
	uncertain  map[int64]struct{}
	redelivery []*TaskRow
	acked      map[int64]bool

	// visibleSeq 在每次可能让行对 poller 变为可见的状态变化之后加一：新行落库、租约原地归还、respool 失败落回
	// redelivery、可见性扫描重置了行。poll 在扫描前记下它，注册 waiter 时它没有变就说明扫描之后没有新的可见行，
	// 不必再读一次库。
	visibleSeq uint64
}

func newDeliveryState(ackLevel int64) *deliveryState {
	return &deliveryState{
		ackLevel:    ackLevel,
		outstanding: make(map[int64]*outstandingTask),
		uncertain:   make(map[int64]struct{}),
		respooling:  make(map[int64]*outstandingTask),
		acked:       make(map[int64]bool),
	}
}

func (s *deliveryState) setMaxReadLevel(level int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if level > s.maxReadLevel {
		s.maxReadLevel = level
		s.visibleSeq++
	}
}

// markVisible 记录一次本进程内存之外的可见性变化（可见性扫描把行放回了可见状态）。
func (s *deliveryState) markVisible() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.visibleSeq++
}

func (s *deliveryState) visibleSeqValue() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.visibleSeq
}

// nextReadWindow 返回待读窗口 (ackLevel, maxReadLevel]。ok=false 表示窗口里没有可能对 poller 可见的行，不必读库。
func (s *deliveryState) nextReadWindow() (int64, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unknownLocked() == 0 {
		return 0, 0, false
	}
	return s.ackLevel, s.maxReadLevel, true
}

// unknownLocked 返回窗口 (ackLevel, maxReadLevel] 里本进程不能证明已隐藏的 task ID 个数。
//
// 窗口里的每个 ID 要么在本进程的某个集合里，要么不在：outstanding（已投递，库里 dispatched_at 非空；uncertain 里的除外）、
// respooling（老行已隐藏，新行写入中）、acked（库里已隐藏或已随 respool 删除）三者互斥，且都只含窗口内的 ID，
// 它们的行对任何 poller 都不可见。其余 ID——redelivery 里的、本进程从未读过的（新写入的、接管分区前写入的）、
// 已被 respool 删除而 ackLevel 尚未越过的——都可能可见，必须读库。计数只会把隐藏的行算作未知、从不反过来，
// 结果为 0 时窗口里确定没有可见行。
func (s *deliveryState) unknownLocked() int64 {
	window := s.maxReadLevel - s.ackLevel
	if window <= 0 {
		return 0
	}
	hidden := int64(len(s.outstanding) - len(s.uncertain) + len(s.respooling) + len(s.acked))
	if hidden >= window {
		if hidden > window {
			// 三个集合只含窗口内互不相同的 ID，超过窗口说明记账有误：按全部未知处理，照常读库。
			return window
		}
		return 0
	}
	return window - hidden
}

// mayHaveTimedOut 报告可见性扫描能否找到超时的投递：窗口里有本进程不认识的行（可能由上一个 owner 投递），或者
// 有 outstanding 投递的时刻早于 now-visTimeout+slack。acked 与 respooling 的行被扫描排除，ackLevel 及以下不扫描；
// 返回 false 时扫描必然什么都不重置。slack 吸收内存与库里 dispatched_at 精度的差异。
func (s *deliveryState) mayHaveTimedOut(now time.Time, visTimeout, slack time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.unknownLocked() > 0 {
		return true
	}
	cutoff := now.Add(-visTimeout + slack)
	for _, ot := range s.outstanding {
		if ot.dispatchAt.Before(cutoff) {
			return true
		}
	}
	return false
}

func (s *deliveryState) markDispatched(row *TaskRow, dispatchAt time.Time, deliveryID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.outstanding[row.TaskID] = &outstandingTask{
		row:        row,
		dispatchAt: dispatchAt,
		deliveryID: deliveryID,
	}
	delete(s.uncertain, row.TaskID)
	for i, pending := range s.redelivery {
		if pending.TaskID == row.TaskID {
			s.redelivery = append(s.redelivery[:i], s.redelivery[i+1:]...)
			break
		}
	}
}

func (s *deliveryState) ack(taskID, deliveryID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ot, ok := s.outstanding[taskID]
	if !ok {
		return ErrTaskNotOutstanding
	}
	if ot.deliveryID != deliveryID {
		return ErrStaleToken
	}

	delete(s.outstanding, taskID)
	delete(s.uncertain, taskID)
	s.acked[taskID] = true
	s.advanceAckLevelLocked()
	return nil
}

func (s *deliveryState) beginNack(taskID, deliveryID int64) (*outstandingTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ot, ok := s.outstanding[taskID]
	if !ok {
		return nil, ErrTaskNotOutstanding
	}
	if ot.deliveryID != deliveryID {
		return nil, ErrStaleToken
	}
	delete(s.outstanding, taskID)
	delete(s.uncertain, taskID)
	return ot, nil
}

func (s *deliveryState) finishNack(ot *outstandingTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redelivery = append(s.redelivery, ot.row)
	s.visibleSeq++
}

// restoreOutstanding 在放回可见状态的 UPDATE 报错后把任务放回 outstanding。UPDATE 可能已经提交，任务记为
// uncertain：窗口记账把它算作未知、照常读库，读到可见的行就重新投递。
func (s *deliveryState) restoreOutstanding(taskID int64, ot *outstandingTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.respooling, taskID)
	s.outstanding[taskID] = ot
	s.uncertain[taskID] = struct{}{}
	s.visibleSeq++
}

// beginRespool 校验投递身份后把任务移入 respooling，等待新行落库。
func (s *deliveryState) beginRespool(taskID, deliveryID int64) (*outstandingTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ot, ok := s.outstanding[taskID]
	if !ok {
		return nil, ErrTaskNotOutstanding
	}
	if ot.deliveryID != deliveryID {
		return nil, ErrStaleToken
	}
	s.beginRespoolLocked(taskID, ot)
	return ot, nil
}

// moveToRespooling 供可见性超时使用，超时方没有投递身份可校验。
func (s *deliveryState) moveToRespooling(taskID int64) (*outstandingTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ot, ok := s.outstanding[taskID]
	if !ok {
		return nil, false
	}
	s.beginRespoolLocked(taskID, ot)
	return ot, true
}

func (s *deliveryState) beginRespoolLocked(taskID int64, ot *outstandingTask) {
	delete(s.outstanding, taskID)
	delete(s.uncertain, taskID)
	s.respooling[taskID] = ot
}

// finishRespool 在新行确认落库（或任务被丢弃）后 ack 老行并放行 ackLevel。
func (s *deliveryState) finishRespool(taskID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.respooling, taskID)
	s.acked[taskID] = true
	s.advanceAckLevelLocked()
}

// abortRespool 在新行未能落库时把老任务落回 redelivery，绝不 ack。
func (s *deliveryState) abortRespool(taskID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ot, ok := s.respooling[taskID]
	if !ok {
		return
	}
	delete(s.respooling, taskID)
	s.redelivery = append(s.redelivery, ot.row)
	s.visibleSeq++
}

// resetExclusionIDs 返回不允许被可见性扫描重置为可见的任务 ID。
func (s *deliveryState) resetExclusionIDs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]int64, 0, len(s.acked)+len(s.respooling))
	for taskID := range s.acked {
		ids = append(ids, taskID)
	}
	for taskID := range s.respooling {
		ids = append(ids, taskID)
	}
	return ids
}

func (s *deliveryState) ackLevelValue() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackLevel
}

// blockerStats 描述当前挡住 ackLevel 推进的那条任务（ackLevel+1）的状态，
// 供 GC 侧发现「ackLevel 长期不前进」时报告成因。
type blockerStats struct {
	taskID      int64
	state       string // outstanding / respooling / redelivery / unconsumed
	routingKey  string
	dispatchAge time.Duration
}

// ackLevelBlocker 返回挡住 ackLevel 的任务及其状态。
//
// 注意：ackLevel 是连续水位，任何一条未 ack 的任务都会挡住它 —— 不只是 redelivery 里的。
// 一条带 routing key、从未被任何 poller 取走的任务同样会一直挡住（这是定向投递的固有语义，
// 见 README「零值 deadline 表示永久按 routing key 定向」），因此 state 用 unconsumed 表示
// 「不在本进程任何投递集合里，仍等待匹配的 poller」。
func (s *deliveryState) ackLevelBlocker(now time.Time) blockerStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := s.ackLevel + 1
	if next > s.maxReadLevel {
		return blockerStats{}
	}
	if ot, ok := s.outstanding[next]; ok {
		return blockerStats{next, "outstanding", ot.row.RoutingKey, now.Sub(ot.dispatchAt)}
	}
	if ot, ok := s.respooling[next]; ok {
		return blockerStats{next, "respooling", ot.row.RoutingKey, now.Sub(ot.dispatchAt)}
	}
	for _, row := range s.redelivery {
		if row.TaskID == next {
			return blockerStats{next, "redelivery", row.RoutingKey, now.Sub(taskDispatchTime(row))}
		}
	}
	return blockerStats{taskID: next, state: "unconsumed"}
}

func (s *deliveryState) maxReadLevelValue() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxReadLevel
}

// skipAbsent 把 ackLevel 推过 (ackLevel, upTo] 里在库中已没有行的 taskID。调用方保证这个区间在库里
// 没有任何行：这些 ID 的任务已被 respool 重写并删除。本进程仍在投递或重写中的任务挡住推进。
func (s *deliveryState) skipAbsent(upTo int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.ackLevel < upTo {
		next := s.ackLevel + 1
		if _, inFlight := s.outstanding[next]; inFlight {
			break
		}
		if _, rewriting := s.respooling[next]; rewriting {
			break
		}
		if s.inRedeliveryLocked(next) {
			break
		}
		delete(s.acked, next)
		s.ackLevel = next
	}
	s.advanceAckLevelLocked()
}

func (s *deliveryState) advanceAckLevelLocked() {
	for {
		next := s.ackLevel + 1
		if _, inFlight := s.outstanding[next]; inFlight {
			return
		}
		if _, rewriting := s.respooling[next]; rewriting {
			return
		}
		if s.inRedeliveryLocked(next) {
			return
		}
		if s.acked[next] {
			s.ackLevel = next
			delete(s.acked, next)
			continue
		}
		return
	}
}

func (s *deliveryState) inRedeliveryLocked(taskID int64) bool {
	for _, row := range s.redelivery {
		if row.TaskID == taskID {
			return true
		}
	}
	return false
}
