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
	redelivery  []*TaskRow
	acked       map[int64]bool
}

func newDeliveryState(ackLevel int64) *deliveryState {
	return &deliveryState{
		ackLevel:    ackLevel,
		outstanding: make(map[int64]*outstandingTask),
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
	s.outstanding[taskID] = ot
}

func (s *deliveryState) moveToRedelivery(taskID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	ot, ok := s.outstanding[taskID]
	if !ok {
		return false
	}
	delete(s.outstanding, taskID)
	s.redelivery = append(s.redelivery, ot.row)
	return true
}

func (s *deliveryState) ackedTaskIDs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]int64, 0, len(s.acked))
	for taskID := range s.acked {
		ids = append(ids, taskID)
	}
	return ids
}

func (s *deliveryState) ackLevelValue() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackLevel
}

func (s *deliveryState) advanceAckLevelLocked() {
	for {
		next := s.ackLevel + 1
		if _, inFlight := s.outstanding[next]; inFlight {
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
