package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newTestStore(t *testing.T) Store {
	t.Helper()
	s, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestCreateAndGetQueue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	info := &QueueInfo{QueueName: "test", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 1}
	if err := s.CreateQueue(ctx, info); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetQueue(ctx, "test", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.RangeID != 1 || got.NextTaskID != 1 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestUpdateQueue_CAS(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	info := &QueueInfo{QueueName: "test", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 1}
	s.CreateQueue(ctx, info)

	// 成功更新
	info.RangeID = 2
	if err := s.UpdateQueue(ctx, info, 1); err != nil {
		t.Fatal(err)
	}
	// 冲突
	if err := s.UpdateQueue(ctx, info, 1); !errors.Is(err, ErrConditionFailed) {
		t.Fatalf("expected ErrConditionFailed, got %v", err)
	}
}

func TestCreateAndGetTasks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	tasks := []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
		{TaskID: 3, QueueName: "q", PartitionID: 0, Data: []byte("c"), CreatedAt: now},
	}
	if err := s.CreateTasks(ctx, tasks); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(got))
	}
}

func TestDispatchAndGetTasks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	tasks := []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
	}
	s.CreateTasks(ctx, tasks)

	// Dispatch task 1
	s.DispatchTask(ctx, "q", 0, 1, now)

	// GetTasks 只返回未分发的
	got, _ := s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if len(got) != 1 || got[0].TaskID != 2 {
		t.Fatalf("expected only task 2, got %+v", got)
	}

	// UnDispatch task 1
	s.UnDispatchTask(ctx, "q", 0, 1)
	got, _ = s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if len(got) != 2 {
		t.Fatalf("expected 2 tasks after undispatch, got %d", len(got))
	}
}

func TestResetTimedOutTasks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	past := time.Now().Add(-2 * time.Minute)
	tasks := []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: past},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: past},
	}
	s.CreateTasks(ctx, tasks)
	s.DispatchTask(ctx, "q", 0, 1, past)
	s.DispatchTask(ctx, "q", 0, 2, past)

	// visTimeout=60s, 两条都超时
	ids, err := s.ResetTimedOutTasks(ctx, "q", 0, 0, 60*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 timed out, got %d", len(ids))
	}

	// 重置后 GetTasks 能读到
	got, _ := s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if len(got) != 2 {
		t.Fatalf("expected 2 tasks after reset, got %d", len(got))
	}
}

func TestDeleteTasks(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	tasks := []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
		{TaskID: 3, QueueName: "q", PartitionID: 0, Data: []byte("c"), CreatedAt: now},
	}
	s.CreateTasks(ctx, tasks)
	n, err := s.DeleteTasks(ctx, "q", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 deleted, got %d", n)
	}
	got, _ := s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if len(got) != 1 || got[0].TaskID != 3 {
		t.Fatalf("expected task 3, got %+v", got)
	}
}

func TestDeleteTask(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	tasks := []*TaskRow{
		{TaskID: 5, QueueName: "q", PartitionID: 0, Data: []byte("x"), CreatedAt: now},
	}
	s.CreateTasks(ctx, tasks)
	if err := s.DeleteTask(ctx, "q", 0, 5); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetTasks(ctx, "q", 0, 0, 10, 100)
	if len(got) != 0 {
		t.Fatalf("expected 0, got %d", len(got))
	}
}

func TestPartitionIsolation(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now()
	// 两个分区各写一条
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("p0"), CreatedAt: now},
	})
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 1, Data: []byte("p1"), CreatedAt: now},
	})
	// 各分区只能读到自己的
	got0, _ := s.GetTasks(ctx, "q", 0, 0, 10, 100)
	got1, _ := s.GetTasks(ctx, "q", 1, 0, 10, 100)
	if len(got0) != 1 || string(got0[0].Data) != "p0" {
		t.Fatalf("partition 0: %+v", got0)
	}
	if len(got1) != 1 || string(got1[0].Data) != "p1" {
		t.Fatalf("partition 1: %+v", got1)
	}
}
