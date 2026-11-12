package matching

import (
	"context"
	"errors"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
)

type countingLockStore struct {
	dbleader.LockStore
	upserts, purges int
	failUpsert      bool
}

func (s *countingLockStore) UpsertMember(context.Context, dbleader.MemberInfo) error {
	s.upserts++
	if s.failUpsert {
		return errors.New("db down")
	}
	return nil
}

func (s *countingLockStore) PurgeStaleMembers(context.Context, string, int) (int64, error) {
	s.purges++
	return 0, nil
}

// 分区锁每秒一次的成员心跳与过期清理按 5s / 15s 写库：成员行在 15s 判活阈值内保持活跃，写入失败的心跳下一次照常写。
func TestMemberThrottledLockStoreWritesMembersAtTheirOwnCadence(t *testing.T) {
	base := &countingLockStore{}
	store := newMemberThrottledLockStore(base)
	now := time.Unix(1000, 0)
	store.now = func() time.Time { return now }
	ctx := context.Background()
	member := dbleader.MemberInfo{LockName: "q/0", InstanceID: "node-a"}
	other := dbleader.MemberInfo{LockName: "q/1", InstanceID: "node-a"}

	for second := range 16 {
		now = time.Unix(1000+int64(second), 0)
		if err := store.UpsertMember(ctx, member); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PurgeStaleMembers(ctx, "q/0", 15); err != nil {
			t.Fatal(err)
		}
	}
	// t=0,5,10,15 刷新；t=0,15 清理。
	if base.upserts != 4 || base.purges != 2 {
		t.Fatalf("upserts=%d purges=%d over 16 heartbeats, want 4 and 2", base.upserts, base.purges)
	}
	// 另一把锁独立计时。
	if err := store.UpsertMember(ctx, other); err != nil || base.upserts != 5 {
		t.Fatalf("first heartbeat of another lock: upserts=%d err=%v", base.upserts, err)
	}

	base.failUpsert = true
	now = now.Add(partitionMemberHeartbeat)
	if err := store.UpsertMember(ctx, member); err == nil {
		t.Fatal("failed upsert reported success")
	}
	base.failUpsert = false
	now = now.Add(time.Second)
	if err := store.UpsertMember(ctx, member); err != nil || base.upserts != 7 {
		t.Fatalf("heartbeat after a failed upsert: upserts=%d err=%v, want it written", base.upserts, err)
	}
}
