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
	redelivery []*TaskRow
	acked      map[int64]bool
}

func newDeliveryState(ackLevel int64) *deliveryState {
	return &deliveryState{
		ackLevel:    ackLevel,
		outstanding: make(map[int64]*outstandingTask),
		respooling:  make(map[int64]*outstandingTask),
		acked:       make(map[int64]bool),
	}
}

func (s *deliveryState) setMaxReadLevel(level int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if level > s.maxReadLevel {
		s.maxReadLevel = level
	}
}

func (s *deliveryState) nextReadWindow() (int64, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ackLevel >= s.maxReadLevel {
		return 0, 0, false
	}
	return s.ackLevel, s.maxReadLevel, true
}

func (s *deliveryState) markDispatched(row *TaskRow, dispatchAt time.Time, deliveryID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.outstanding[row.TaskID] = &outstandingTask{
		row:        row,
		dispatchAt: dispatchAt,
		deliveryID: deliveryID,
	}
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
	return ot, nil
}

func (s *deliveryState) finishNack(ot *outstandingTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redelivery = append(s.redelivery, ot.row)
}

func (s *deliveryState) restoreOutstanding(taskID int64, ot *outstandingTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.respooling, taskID)
	s.outstanding[taskID] = ot
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
