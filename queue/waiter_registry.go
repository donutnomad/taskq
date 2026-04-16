package queue

import (
	"container/list"
	"sync"
)

type waiterRegistry struct {
	mu      sync.Mutex
	waiters *list.List
	closed  bool
}

func newWaiterRegistry() *waiterRegistry {
	return &waiterRegistry{waiters: list.New()}
}

func (wr *waiterRegistry) add(ch chan PollResult) *list.Element {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if wr.closed {
		sendPollResult(ch, PollResult{Err: ErrQueueClosed})
		return nil
	}

	return wr.waiters.PushBack(ch)
}

func (wr *waiterRegistry) remove(elem *list.Element) {
	if elem == nil {
		return
	}

	wr.mu.Lock()
	defer wr.mu.Unlock()
	wr.waiters.Remove(elem)
}

func (wr *waiterRegistry) hasWaiters() bool {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	return !wr.closed && wr.waiters.Len() > 0
}

func (wr *waiterRegistry) pop() (chan PollResult, bool) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if wr.closed || wr.waiters.Len() == 0 {
		return nil, false
	}

	elem := wr.waiters.Front()
	ch := elem.Value.(chan PollResult)
	wr.waiters.Remove(elem)
	return ch, true
}

func (wr *waiterRegistry) close() {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if wr.closed {
		return
	}
	wr.closed = true

	for elem := wr.waiters.Front(); elem != nil; elem = elem.Next() {
		ch := elem.Value.(chan PollResult)
		sendPollResult(ch, PollResult{Err: ErrQueueClosed})
	}
	wr.waiters.Init()
}

func sendPollResult(ch chan PollResult, result PollResult) bool {
	select {
	case ch <- result:
		return true
	default:
		return false
	}
}
