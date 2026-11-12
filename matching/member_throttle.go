package matching

import (
	"context"
	"sync"
	"time"

	dbleader "github.com/donutnomad/dbleader"
)

const (
	// partitionMemberHeartbeat 是分区锁成员行的刷新间隔。dbleader 按锁的心跳（1s，续约用）刷新成员行、清理过期成员，
	// 每个分区每秒各一条 INSERT 与 DELETE；成员行只供观测，判活阈值是 15s（dbleader 的 staleAfterSec 下限）。每 5s
	// 刷新一次，行在阈值内保持活跃，还容忍两次刷新失败。
	partitionMemberHeartbeat = 5 * time.Second
	// partitionMemberPurgeInterval 是清理过期成员行的间隔，与判活阈值相同。
	partitionMemberPurgeInterval = 15 * time.Second
)

// memberThrottledLockStore 只节流分区锁的成员行写入；加锁、续约、释放与读取原样转给底层存储。
type memberThrottledLockStore struct {
	dbleader.LockStore
	now func() time.Time

	mu       sync.Mutex
	upserted map[string]time.Time // lock_name + instance_id → 上次刷新
	purged   map[string]time.Time // lock_name → 上次清理
}

func newMemberThrottledLockStore(store dbleader.LockStore) *memberThrottledLockStore {
	return &memberThrottledLockStore{
		LockStore: store,
		now:       time.Now,
		upserted:  make(map[string]time.Time),
		purged:    make(map[string]time.Time),
	}
}

// due 报告 key 距上次记录是否已过 interval；是时记下本次。
func (s *memberThrottledLockStore) due(last map[string]time.Time, key string, interval time.Duration) bool {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if at, ok := last[key]; ok && now.Sub(at) < interval {
		return false
	}
	last[key] = now
	return true
}

// forget 撤销一次失败的记录，下一次心跳照常写入。
func (s *memberThrottledLockStore) forget(last map[string]time.Time, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(last, key)
}

func (s *memberThrottledLockStore) UpsertMember(ctx context.Context, m dbleader.MemberInfo) error {
	key := m.LockName + "\x00" + m.InstanceID
	if !s.due(s.upserted, key, partitionMemberHeartbeat) {
		return nil
	}
	if err := s.LockStore.UpsertMember(ctx, m); err != nil {
		s.forget(s.upserted, key)
		return err
	}
	return nil
}

func (s *memberThrottledLockStore) PurgeStaleMembers(ctx context.Context, lockName string, staleAfterSec int) (int64, error) {
	if !s.due(s.purged, lockName, partitionMemberPurgeInterval) {
		return 0, nil
	}
	removed, err := s.LockStore.PurgeStaleMembers(ctx, lockName, staleAfterSec)
	if err != nil {
		s.forget(s.purged, lockName)
	}
	return removed, err
}
