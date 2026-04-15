package queue

import (
	"context"
	"testing"
	"time"
)

func setupDBManager(t *testing.T) (*dbTaskManager, Store) {
	t.Helper()
	s := newTestStore(t)
	ctx := context.Background()
	s.CreateQueue(ctx, &QueueInfo{
		QueueName: "q", PartitionID: 0, RangeID: 1, AckLevel: 0, NextTaskID: 100,
	})
	dm := newDBTaskManager(s, "q", 0, 0, 60*time.Second)
	return dm, s
}

func TestDBManager_GetTask(t *testing.T) {
	dm, s := setupDBManager(t)
	ctx := context.Background()
	now := time.Now()
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("hello"), CreatedAt: now},
	})
	dm.setMaxReadLevel(1)

	row, ok := dm.getTask(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if row.TaskID != 1 || string(row.Data) != "hello" {
		t.Fatalf("unexpected: %+v", row)
	}
	if _, ok := dm.outstanding[int64(1)]; !ok {
		t.Fatal("expected in outstanding")
	}
}

func TestDBManager_GetTaskRedeliveryFirst(t *testing.T) {
	dm, s := setupDBManager(t)
	ctx := context.Background()
	now := time.Now()
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("normal"), CreatedAt: now},
	})
	dm.setMaxReadLevel(1)
	dm.mu.Lock()
	dm.redelivery = append(dm.redelivery, &TaskRow{TaskID: 99, QueueName: "q", PartitionID: 0, Data: []byte("redeliver")})
	dm.mu.Unlock()

	row, ok := dm.getTask(ctx)
	if !ok {
		t.Fatal("expected task")
	}
	if row.TaskID != 99 {
		t.Fatalf("expected redelivery task 99, got %d", row.TaskID)
	}
}

func TestDBManager_AckAdvancesLevel(t *testing.T) {
	dm, s := setupDBManager(t)
	ctx := context.Background()
	now := time.Now()
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
		{TaskID: 3, QueueName: "q", PartitionID: 0, Data: []byte("c"), CreatedAt: now},
	})
	dm.setMaxReadLevel(3)

	dm.getTask(ctx) // task 1
	dm.getTask(ctx) // task 2
	dm.getTask(ctx) // task 3

	// 乱序 ack
	dm.ack(ctx, 2)
	if dm.ackLevel != 0 {
		t.Fatalf("ackLevel should be 0, got %d", dm.ackLevel)
	}
	dm.ack(ctx, 1)
	if dm.ackLevel != 2 {
		t.Fatalf("ackLevel should be 2 (continuous), got %d", dm.ackLevel)
	}
	dm.ack(ctx, 3)
	if dm.ackLevel != 3 {
		t.Fatalf("ackLevel should be 3, got %d", dm.ackLevel)
	}
}

func TestDBManager_NackToRedelivery(t *testing.T) {
	dm, s := setupDBManager(t)
	ctx := context.Background()
	now := time.Now()
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
	})
	dm.setMaxReadLevel(2)

	dm.getTask(ctx) // task 1
	dm.getTask(ctx) // task 2

	dm.nack(ctx, 1)

	if len(dm.redelivery) != 1 || dm.redelivery[0].TaskID != 1 {
		t.Fatalf("expected task 1 in redelivery, got %+v", dm.redelivery)
	}
	if len(dm.outstanding) != 1 {
		t.Fatalf("expected 1 outstanding, got %d", len(dm.outstanding))
	}
}

func TestDBManager_NackBlocksAckLevel(t *testing.T) {
	dm, s := setupDBManager(t)
	ctx := context.Background()
	now := time.Now()
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
		{TaskID: 3, QueueName: "q", PartitionID: 0, Data: []byte("c"), CreatedAt: now},
	})
	dm.setMaxReadLevel(3)

	dm.getTask(ctx) // task 1
	dm.getTask(ctx) // task 2
	dm.getTask(ctx) // task 3

	dm.nack(ctx, 1)
	dm.ack(ctx, 2)
	dm.ack(ctx, 3)

	// task 1 在 redelivery 中，必须阻塞 ackLevel 推进，防止 GC 删除未消费的任务
	if dm.ackLevel != 0 {
		t.Fatalf("ackLevel should be 0 (blocked by redelivery task 1), got %d", dm.ackLevel)
	}

	// 重新消费 redelivery 中的 task 1 并 ack
	row, ok := dm.getTask(ctx) // 从 redelivery 取出 task 1
	if !ok || row.TaskID != 1 {
		t.Fatalf("expected redelivery task 1, got %v ok=%v", row, ok)
	}
	dm.ack(ctx, 1)

	// 现在 ackLevel 应推进到 3
	if dm.ackLevel != 3 {
		t.Fatalf("ackLevel should be 3 after all acked, got %d", dm.ackLevel)
	}
}

func TestDBManager_NackThenCrash_TaskSurvives(t *testing.T) {
	dm, s := setupDBManager(t)
	ctx := context.Background()
	now := time.Now()
	s.CreateTasks(ctx, []*TaskRow{
		{TaskID: 1, QueueName: "q", PartitionID: 0, Data: []byte("a"), CreatedAt: now},
		{TaskID: 2, QueueName: "q", PartitionID: 0, Data: []byte("b"), CreatedAt: now},
		{TaskID: 3, QueueName: "q", PartitionID: 0, Data: []byte("c"), CreatedAt: now},
	})
	dm.setMaxReadLevel(3)

	dm.getTask(ctx) // task 1
	dm.getTask(ctx) // task 2
	dm.getTask(ctx) // task 3

	// nack task 2，ack task 1 和 3
	dm.nack(ctx, 2)
	dm.ack(ctx, 1)
	dm.ack(ctx, 3)

	// ackLevel 应停在 1（task 2 在 redelivery 中，阻塞推进）
	if dm.ackLevel != 1 {
		t.Fatalf("ackLevel should be 1 (blocked by redelivery task 2), got %d", dm.ackLevel)
	}

	// 模拟 GC：删除 taskID <= ackLevel 的任务
	s.DeleteTasks(ctx, "q", 0, dm.ackLevel)

	// task 2 必须仍在 DB 中（因为 ackLevel=1，GC 只删除 task 1）
	tasks, _ := s.GetTasks(ctx, "q", 0, 1, 3, 10)
	found := false
	for _, task := range tasks {
		if task.TaskID == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("task 2 should survive in DB after GC (ackLevel=1), but it was deleted")
	}
}
