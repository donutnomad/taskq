package matching

import (
	"sync"
	"testing"
)

// close() 与 setActive 竞态时，setActive 必须不复活 stateCh、也不接管 pq。
// 无守卫时 close() 已把 stateCh 置 nil，setActive 会 make 一个新的，而随后的
// clearActive 因 closed 为真跳过通知 —— 该 channel 永不被 close。
func TestPartitionHandleSetActiveAfterCloseDoesNotResurrectStateCh(t *testing.T) {
	h := &partitionHandle{queueName: "q", partitionID: 0, stateCh: make(chan struct{})}

	h.mu.Lock()
	h.closed = true
	h.notifyStateChangeLocked(false) // 模拟 close() 的置 nil
	h.mu.Unlock()

	h.setActive(nil) // 竞态到来的 setActive

	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.stateCh != nil {
		t.Fatal("setActive resurrected stateCh on a closed handle; it will never be closed (leak)")
	}
	if h.active != nil {
		t.Fatal("setActive published a queue onto a closed handle; clearActive already ran, so it leaks")
	}
}

// 并发交错：无论 setActive 与 close 谁先，最终 stateCh 都必须为 nil（已被 close 掉）。
func TestPartitionHandleSetActiveCloseRaceLeavesNoLiveStateCh(t *testing.T) {
	for range 200 {
		h := &partitionHandle{queueName: "q", partitionID: 0, stateCh: make(chan struct{})}
		var wg sync.WaitGroup
		wg.Go(func() {
			h.mu.Lock()
			if !h.closed {
				h.closed = true
				h.notifyStateChangeLocked(false)
			}
			h.mu.Unlock()
		})
		wg.Go(func() { h.setActive(nil) })
		wg.Wait()

		h.mu.RLock()
		live := h.stateCh != nil && h.closed
		h.mu.RUnlock()
		if live {
			t.Fatal("closed handle left a live stateCh that nobody will close")
		}
	}
}
