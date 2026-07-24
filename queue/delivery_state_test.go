package queue

import (
	"testing"
	"time"
)

func TestDeliveryStateMarkDispatchedTracksOutstanding(t *testing.T) {
	state := newDeliveryState(0)
	row := &TaskRow{TaskID: 1}

	state.setMaxReadLevel(1)
	state.markDispatched(row, time.Now(), 1)

	if _, ok := state.outstanding[1]; !ok {
		t.Fatal("expected task in outstanding")
	}
}

func TestDeliveryStateAckAdvancesLevel(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, 1)
	state.markDispatched(&TaskRow{TaskID: 2}, now, 2)
	state.markDispatched(&TaskRow{TaskID: 3}, now, 3)

	if err := state.ack(2, 2); err != nil {
		t.Fatalf("ack task 2: %v", err)
	}
	if state.ackLevelValue() != 0 {
		t.Fatalf("ackLevel should stay at 0, got %d", state.ackLevelValue())
	}

	if err := state.ack(1, 1); err != nil {
		t.Fatalf("ack task 1: %v", err)
	}
	if state.ackLevelValue() != 2 {
		t.Fatalf("ackLevel should advance to 2, got %d", state.ackLevelValue())
	}

	if err := state.ack(3, 3); err != nil {
		t.Fatalf("ack task 3: %v", err)
	}
	if state.ackLevelValue() != 3 {
		t.Fatalf("ackLevel should advance to 3, got %d", state.ackLevelValue())
	}
}

func TestDeliveryStateNackMovesTaskToRedelivery(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1, Data: []byte("a")}, now, 1)
	state.markDispatched(&TaskRow{TaskID: 2, Data: []byte("b")}, now, 2)

	ot, err := state.beginNack(1, 1)
	if err != nil {
		t.Fatalf("beginNack task 1: %v", err)
	}
	state.finishNack(ot)

	if len(state.redelivery) != 1 || state.redelivery[0].TaskID != 1 {
		t.Fatalf("expected task 1 in redelivery, got %+v", state.redelivery)
	}
	if len(state.outstanding) != 1 {
		t.Fatalf("expected 1 outstanding task, got %d", len(state.outstanding))
	}
}

func TestDeliveryStateRedeliveryBlocksAckLevel(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, 1)
	state.markDispatched(&TaskRow{TaskID: 2}, now, 2)
	state.markDispatched(&TaskRow{TaskID: 3}, now, 3)

	ot, err := state.beginNack(1, 1)
	if err != nil {
		t.Fatalf("beginNack task 1: %v", err)
	}
	state.finishNack(ot)

	if err := state.ack(2, 2); err != nil {
		t.Fatalf("ack task 2: %v", err)
	}
	if err := state.ack(3, 3); err != nil {
		t.Fatalf("ack task 3: %v", err)
	}
	if state.ackLevelValue() != 0 {
		t.Fatalf("ackLevel should stay blocked at 0, got %d", state.ackLevelValue())
	}

	state.markDispatched(&TaskRow{TaskID: 1}, now, 4)

	if err := state.ack(1, 4); err != nil {
		t.Fatalf("ack task 1: %v", err)
	}
	if state.ackLevelValue() != 3 {
		t.Fatalf("ackLevel should advance to 3, got %d", state.ackLevelValue())
	}
}

func TestDeliveryStateRespoolingBlocksAckLevel(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, 1)
	state.markDispatched(&TaskRow{TaskID: 2}, now, 2)
	state.markDispatched(&TaskRow{TaskID: 3}, now, 3)

	if _, err := state.beginRespool(1, 2); err != ErrStaleToken {
		t.Fatalf("stale respool error = %v, want ErrStaleToken", err)
	}
	if _, err := state.beginRespool(1, 1); err != nil {
		t.Fatalf("beginRespool task 1: %v", err)
	}
	if _, err := state.beginRespool(1, 1); err != ErrTaskNotOutstanding {
		t.Fatalf("duplicate respool error = %v, want ErrTaskNotOutstanding", err)
	}

	if err := state.ack(2, 2); err != nil {
		t.Fatalf("ack task 2: %v", err)
	}
	if err := state.ack(3, 3); err != nil {
		t.Fatalf("ack task 3: %v", err)
	}
	// 新行未落库前绝不能 ack 老行，ackLevel 必须停在 0。
	if state.ackLevelValue() != 0 {
		t.Fatalf("ackLevel should stay blocked at 0 while task 1 is respooling, got %d", state.ackLevelValue())
	}
}

func TestDeliveryStateFinishRespoolAdvancesAckLevel(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, 1)
	state.markDispatched(&TaskRow{TaskID: 2}, now, 2)
	state.markDispatched(&TaskRow{TaskID: 3}, now, 3)

	if _, err := state.beginRespool(1, 1); err != nil {
		t.Fatalf("beginRespool task 1: %v", err)
	}
	if err := state.ack(2, 2); err != nil {
		t.Fatalf("ack task 2: %v", err)
	}
	if err := state.ack(3, 3); err != nil {
		t.Fatalf("ack task 3: %v", err)
	}

	state.finishRespool(1)

	if state.ackLevelValue() != 3 {
		t.Fatalf("ackLevel should advance to 3 after respool finished, got %d", state.ackLevelValue())
	}
	if len(state.respooling) != 0 {
		t.Fatalf("respooling should be empty, got %+v", state.respooling)
	}
	if len(state.acked) != 0 {
		t.Fatalf("acked entries should be released by the advance, got %+v", state.acked)
	}
}

func TestDeliveryStateAbortRespoolKeepsTaskUnacked(t *testing.T) {
	state := newDeliveryState(0)
	state.markDispatched(&TaskRow{TaskID: 1, Data: []byte("a")}, time.Now(), 1)

	if _, err := state.beginRespool(1, 1); err != nil {
		t.Fatalf("beginRespool task 1: %v", err)
	}
	state.abortRespool(1)

	if state.acked[1] {
		t.Fatal("aborted respool must not ack the original task")
	}
	if !state.inRedeliveryLocked(1) {
		t.Fatalf("aborted respool should leave task 1 in redelivery, got %+v", state.redelivery)
	}
	if state.ackLevelValue() != 0 {
		t.Fatalf("ackLevel = %d, want 0", state.ackLevelValue())
	}
}

func TestDeliveryStateResetExclusionIDsCoversRespooling(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, 1)
	state.markDispatched(&TaskRow{TaskID: 2}, now, 2)
	state.markDispatched(&TaskRow{TaskID: 3}, now, 3)

	if _, err := state.beginRespool(2, 2); err != nil {
		t.Fatalf("beginRespool task 2: %v", err)
	}
	if err := state.ack(3, 3); err != nil {
		t.Fatalf("ack task 3: %v", err)
	}

	excluded := make(map[int64]bool)
	for _, taskID := range state.resetExclusionIDs() {
		excluded[taskID] = true
	}
	if !excluded[2] || !excluded[3] || excluded[1] {
		t.Fatalf("reset exclusion IDs = %v, want {2,3}", state.resetExclusionIDs())
	}
}

func TestDeliveryStateRejectsDuplicateAckAndNack(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, 1)
	if err := state.ack(1, 1); err != nil {
		t.Fatalf("first ack failed: %v", err)
	}
	if err := state.ack(1, 1); err != ErrTaskNotOutstanding {
		t.Fatalf("expected ErrTaskNotOutstanding from duplicate ack, got %v", err)
	}
	if _, err := state.beginNack(1, 1); err != ErrTaskNotOutstanding {
		t.Fatalf("expected ErrTaskNotOutstanding from duplicate nack, got %v", err)
	}
}

func TestDeliveryStateRejectsPreviousDelivery(t *testing.T) {
	state := newDeliveryState(0)
	state.markDispatched(&TaskRow{TaskID: 1}, time.Now(), 2)

	if err := state.ack(1, 1); err != ErrStaleToken {
		t.Fatalf("stale ack error = %v, want ErrStaleToken", err)
	}
	if _, err := state.beginNack(1, 1); err != ErrStaleToken {
		t.Fatalf("stale nack error = %v, want ErrStaleToken", err)
	}
	if err := state.ack(1, 2); err != nil {
		t.Fatalf("current delivery ack: %v", err)
	}
}
