package queue

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestAcquireLease_Basic(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// 行不存在时 AcquireLease 应自动创建并获取 lease
	info, err := s.AcquireLease(ctx, "q", 0, "owner-a", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if info.QueueName != "q" || info.PartitionID != 0 {
		t.Fatalf("unexpected info: %+v", info)
	}
	if info.RangeID < 2 {
		t.Fatalf("expected rangeID >= 2 after acquire, got %d", info.RangeID)
	}
}

func TestAcquireLease_AutoCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// 验证在完全空的 DB 上，AcquireLease 能创建 queue_info 行并获取 lease
	info, err := s.AcquireLease(ctx, "new-queue", 5, "node-1", 15*time.Second)
	if err != nil {
		t.Fatalf("AcquireLease on non-existent queue should auto-create, got %v", err)
	}
	if info.QueueName != "new-queue" || info.PartitionID != 5 {
		t.Fatalf("unexpected info: %+v", info)
	}

	// 获取后验证 DB 中 queue_info 行确实存在
	got, err := s.GetQueue(ctx, "new-queue", 5)
	if err != nil {
		t.Fatalf("GetQueue after AcquireLease should succeed: %v", err)
	}
	if got.RangeID != info.RangeID {
		t.Fatalf("rangeID mismatch: AcquireLease=%d, GetQueue=%d", info.RangeID, got.RangeID)
	}
}

func TestAcquireLease_Conflict(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// owner-a 获取 lease
	_, err := s.AcquireLease(ctx, "q", 0, "owner-a", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// owner-b 尝试获取同一分区，应失败
	_, err = s.AcquireLease(ctx, "q", 0, "owner-b", 15*time.Second)
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
}

func TestAcquireLease_SameOwnerRenew(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// owner-a 获取 lease
	_, err := s.AcquireLease(ctx, "q", 0, "owner-a", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// owner-a 再次获取（相当于续期），应成功
	_, err = s.AcquireLease(ctx, "q", 0, "owner-a", 15*time.Second)
	if err != nil {
		t.Fatalf("same owner should re-acquire: %v", err)
	}
}

func TestAcquireLease_Expired(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// owner-a 获取极短 TTL 的 lease
	_, err := s.AcquireLease(ctx, "q", 0, "owner-a", 1*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// 等待过期
	time.Sleep(10 * time.Millisecond)

	// owner-b 应能获取
	_, err = s.AcquireLease(ctx, "q", 0, "owner-b", 15*time.Second)
	if err != nil {
		t.Fatalf("expected success after lease expired, got %v", err)
	}
}

func TestReleaseLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.AcquireLease(ctx, "q", 0, "owner-a", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 释放
	if err := s.ReleaseLease(ctx, "q", 0, "owner-a"); err != nil {
		t.Fatal(err)
	}

	// owner-b 应能获取
	_, err = s.AcquireLease(ctx, "q", 0, "owner-b", 15*time.Second)
	if err != nil {
		t.Fatalf("expected success after release, got %v", err)
	}
}

func TestReleaseLeaseWrongOwnerReturnsErrLeaseHeld(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.AcquireLease(ctx, "q", 0, "owner-a", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = s.ReleaseLease(ctx, "q", 0, "owner-b")
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
}

func TestRenewLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.AcquireLease(ctx, "q", 0, "owner-a", 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 续期成功
	if err := s.RenewLease(ctx, "q", 0, "owner-a", 15*time.Second); err != nil {
		t.Fatalf("renew should succeed: %v", err)
	}

	// 不同 owner 续期应失败
	if err := s.RenewLease(ctx, "q", 0, "owner-b", 15*time.Second); err != ErrLeaseHeld {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
}

func TestAcquireLease_ConcurrentFirstCreate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "lease-race.db")

	storeA, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer storeA.Close()

	storeB, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer storeB.Close()

	const attempts = 20
	var wg sync.WaitGroup
	errCh := make(chan error, attempts*2)

	for i := range attempts {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := storeA.AcquireLease(context.Background(), "race-q", i, "owner-a", 15*time.Second)
			errCh <- err
		}()
		go func() {
			defer wg.Done()
			_, err := storeB.AcquireLease(context.Background(), "race-q", i, "owner-b", 15*time.Second)
			errCh <- err
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err == nil || errors.Is(err, ErrLeaseHeld) {
			continue
		}
		t.Fatalf("expected only success or ErrLeaseHeld during concurrent first-create, got %v", err)
	}
}
