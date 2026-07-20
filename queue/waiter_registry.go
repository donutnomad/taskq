package queue

import (
	"container/list"
	"sync"
	"time"
)

type pollWaiterState uint8

const (
	pollWaiterWaiting pollWaiterState = iota
	pollWaiterDelivered
	pollWaiterClaimed
	pollWaiterCanceled
)

// PollWaiter arbitrates delivery and cancellation under the registry lock.
// A timeout that loses to delivery can still claim the dispatched task.
type PollWaiter struct {
	registry *waiterRegistry
	ready    chan struct{}
	elem     *list.Element
	state    pollWaiterState
	result   PollResult
	accepted routingKeySet
}

func (w *PollWaiter) Ready() <-chan struct{} {
	return w.ready
}

func (w *PollWaiter) Cancel() bool {
	if w == nil || w.registry == nil {
		return false
	}
	return w.registry.cancel(w)
}

func (w *PollWaiter) Claim() (PollResult, bool) {
	if w == nil || w.registry == nil {
		return PollResult{}, false
	}
	return w.registry.claim(w)
}

type waiterRegistry struct {
	mu      sync.Mutex
	waiters *list.List
	closed  bool
}

func newWaiterRegistry() *waiterRegistry {
	return &waiterRegistry{waiters: list.New()}
}

func (wr *waiterRegistry) add(opts PollOptions) *PollWaiter {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	waiter := &PollWaiter{
		registry: wr,
		ready:    make(chan struct{}),
		state:    pollWaiterWaiting,
		accepted: newRoutingKeySet(opts.AcceptedRoutingKeys),
	}
	if wr.closed {
		wr.deliverLocked(waiter, PollResult{Err: ErrQueueClosed})
		return waiter
	}
	waiter.elem = wr.waiters.PushBack(waiter)
	return waiter
}

func (wr *waiterRegistry) deliver(result PollResult) bool {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if wr.closed || wr.waiters.Len() == 0 {
		return false
	}
	for elem := wr.waiters.Front(); elem != nil; elem = elem.Next() {
		waiter := elem.Value.(*PollWaiter)
		if result.Row != nil && !waiter.accepted.accepts(result.Row, time.Now()) {
			continue
		}
		wr.waiters.Remove(elem)
		waiter.elem = nil
		wr.deliverLocked(waiter, result)
		return true
	}
	return false
}

func (wr *waiterRegistry) pollOptions() (PollOptions, bool) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if wr.closed || wr.waiters.Len() == 0 {
		return PollOptions{}, false
	}
	keys := make(routingKeySet)
	for elem := wr.waiters.Front(); elem != nil; elem = elem.Next() {
		for key := range elem.Value.(*PollWaiter).accepted {
			keys[key] = struct{}{}
		}
	}
	accepted := make([]string, 0, len(keys))
	for key := range keys {
		accepted = append(accepted, key)
	}
	return PollOptions{AcceptedRoutingKeys: accepted}, true
}

func (wr *waiterRegistry) cancel(waiter *PollWaiter) bool {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if waiter.state != pollWaiterWaiting {
		return false
	}
	if waiter.elem != nil {
		wr.waiters.Remove(waiter.elem)
		waiter.elem = nil
	}
	waiter.state = pollWaiterCanceled
	return true
}

func (wr *waiterRegistry) claim(waiter *PollWaiter) (PollResult, bool) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if waiter.state != pollWaiterDelivered {
		return PollResult{}, false
	}
	waiter.state = pollWaiterClaimed
	return waiter.result, true
}

func (wr *waiterRegistry) close() {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	if wr.closed {
		return
	}
	wr.closed = true
	for elem := wr.waiters.Front(); elem != nil; {
		next := elem.Next()
		waiter := elem.Value.(*PollWaiter)
		wr.waiters.Remove(elem)
		waiter.elem = nil
		wr.deliverLocked(waiter, PollResult{Err: ErrQueueClosed})
		elem = next
	}
}

func (wr *waiterRegistry) deliverLocked(waiter *PollWaiter, result PollResult) {
	waiter.result = result
	waiter.state = pollWaiterDelivered
	close(waiter.ready)
}
