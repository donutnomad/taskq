package queue

import (
	"testing"
	"time"
)

func TestDeliveryStateMarkDispatchedTracksOutstanding(t *testing.T) {
	state := newDeliveryState(0)
	row := &TaskRow{TaskID: 1}

	state.setMaxReadLevel(1)
	state.markDispatched(row, time.Now(), true)

	if _, ok := state.outstanding[1]; !ok {
		t.Fatal("expected task in outstanding")
	}
	if state.hasReadable() {
		t.Fatal("expected no readable task after dispatching the only known task")
	}
}

func TestDeliveryStateAckAdvancesLevel(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, true)
	state.markDispatched(&TaskRow{TaskID: 2}, now, true)
	state.markDispatched(&TaskRow{TaskID: 3}, now, true)

	if err := state.ack(2); err != nil {
		t.Fatalf("ack task 2: %v", err)
	}
	if state.ackLevelValue() != 0 {
		t.Fatalf("ackLevel should stay at 0, got %d", state.ackLevelValue())
	}

	if err := state.ack(1); err != nil {
		t.Fatalf("ack task 1: %v", err)
	}
	if state.ackLevelValue() != 2 {
		t.Fatalf("ackLevel should advance to 2, got %d", state.ackLevelValue())
	}

	if err := state.ack(3); err != nil {
		t.Fatalf("ack task 3: %v", err)
	}
	if state.ackLevelValue() != 3 {
		t.Fatalf("ackLevel should advance to 3, got %d", state.ackLevelValue())
	}
}

func TestDeliveryStateNackMovesTaskToRedelivery(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1, Data: []byte("a")}, now, true)
	state.markDispatched(&TaskRow{TaskID: 2, Data: []byte("b")}, now, true)

	ot, err := state.beginNack(1)
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

	state.markDispatched(&TaskRow{TaskID: 1}, now, true)
	state.markDispatched(&TaskRow{TaskID: 2}, now, true)
	state.markDispatched(&TaskRow{TaskID: 3}, now, true)

	ot, err := state.beginNack(1)
	if err != nil {
		t.Fatalf("beginNack task 1: %v", err)
	}
	state.finishNack(ot)

	if err := state.ack(2); err != nil {
		t.Fatalf("ack task 2: %v", err)
	}
	if err := state.ack(3); err != nil {
		t.Fatalf("ack task 3: %v", err)
	}
	if state.ackLevelValue() != 0 {
		t.Fatalf("ackLevel should stay blocked at 0, got %d", state.ackLevelValue())
	}

	row := state.takeRedelivery()
	if row == nil || row.TaskID != 1 {
		t.Fatalf("expected redelivery task 1, got %+v", row)
	}
	state.markDispatched(row, now, false)

	if err := state.ack(1); err != nil {
		t.Fatalf("ack task 1: %v", err)
	}
	if state.ackLevelValue() != 3 {
		t.Fatalf("ackLevel should advance to 3, got %d", state.ackLevelValue())
	}
}

func TestDeliveryStateRejectsDuplicateAckAndNack(t *testing.T) {
	state := newDeliveryState(0)
	now := time.Now()

	state.markDispatched(&TaskRow{TaskID: 1}, now, true)
	if err := state.ack(1); err != nil {
		t.Fatalf("first ack failed: %v", err)
	}
	if err := state.ack(1); err != ErrTaskNotOutstanding {
		t.Fatalf("expected ErrTaskNotOutstanding from duplicate ack, got %v", err)
	}
	if _, err := state.beginNack(1); err != ErrTaskNotOutstanding {
		t.Fatalf("expected ErrTaskNotOutstanding from duplicate nack, got %v", err)
	}
}
