package queue

import (
	"sync"
	"time"
)

type outstandingTask struct {
	row        *TaskRow
	dispatchAt time.Time
}

type deliveryState struct {
	mu sync.Mutex

	ackLevel     int64
	readLevel    int64
	maxReadLevel int64

	outstanding map[int64]*outstandingTask
	redelivery  []*TaskRow
	acked       map[int64]bool
}

func newDeliveryState(ackLevel int64) *deliveryState {
	return &deliveryState{
		ackLevel:    ackLevel,
		readLevel:   ackLevel,
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

	if s.readLevel >= s.maxReadLevel {
		return 0, 0, false
	}
	return s.readLevel, s.maxReadLevel, true
}

func (s *deliveryState) markDispatched(row *TaskRow, dispatchAt time.Time, advanceReadLevel bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if advanceReadLevel && row.TaskID > s.readLevel {
		s.readLevel = row.TaskID
	}
	s.outstanding[row.TaskID] = &outstandingTask{
		row:        row,
		dispatchAt: dispatchAt,
	}
}

func (s *deliveryState) takeRedelivery() *TaskRow {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.redelivery) == 0 {
		return nil
	}

	row := s.redelivery[0]
	s.redelivery = s.redelivery[1:]
	return row
}

func (s *deliveryState) requeueRedeliveryFront(row *TaskRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.redelivery = append([]*TaskRow{row}, s.redelivery...)
}

func (s *deliveryState) ack(taskID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.outstanding[taskID]; !ok {
		return ErrTaskNotOutstanding
	}

	delete(s.outstanding, taskID)
	s.acked[taskID] = true
	s.advanceAckLevelLocked()
	return nil
}

func (s *deliveryState) beginNack(taskID int64) (*outstandingTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ot, ok := s.outstanding[taskID]
	if !ok {
		return nil, ErrTaskNotOutstanding
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

func (s *deliveryState) ackLevelValue() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ackLevel
}

func (s *deliveryState) hasReadable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.redelivery) > 0 || s.readLevel < s.maxReadLevel
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
