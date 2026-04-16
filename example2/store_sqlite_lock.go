package example2

import (
	"context"
	"database/sql"
	"errors"
	"time"

	dbleader "github.com/donutnomad/dbleader"
)

var _ dbleader.LockStore = (*sqliteStore)(nil)

func (s *sqliteStore) TryAcquire(ctx context.Context, lockName, nodeIP string, ttlSeconds int) (dbleader.AcquireResult, error) {
	nowMs := time.Now().UnixMilli()
	expireAtMs := nowMs + int64(ttlSeconds)*1000

	row := s.db.QueryRowContext(
		ctx,
		`INSERT INTO taskq_partition_lock (lock_name, leader_ip, expire_at_ms, version)
		 VALUES (?, ?, ?, 1)
		 ON CONFLICT(lock_name) DO UPDATE SET
		   version = CASE
		     WHEN taskq_partition_lock.expire_at_ms < ? THEN taskq_partition_lock.version + 1
		     ELSE taskq_partition_lock.version
		   END,
		   leader_ip = CASE
		     WHEN taskq_partition_lock.expire_at_ms < ? THEN excluded.leader_ip
		     ELSE taskq_partition_lock.leader_ip
		   END,
		   expire_at_ms = CASE
		     WHEN taskq_partition_lock.expire_at_ms < ? OR taskq_partition_lock.leader_ip = excluded.leader_ip THEN ?
		     ELSE taskq_partition_lock.expire_at_ms
		   END
		 RETURNING leader_ip, version`,
		lockName,
		nodeIP,
		expireAtMs,
		nowMs,
		nowMs,
		nowMs,
		expireAtMs,
	)

	var leaderIP string
	var version uint64
	if err := row.Scan(&leaderIP, &version); err != nil {
		return dbleader.AcquireResult{}, err
	}
	return dbleader.AcquireResult{
		Acquired: leaderIP == nodeIP,
		LeaderIP: leaderIP,
		Version:  version,
	}, nil
}

func (s *sqliteStore) Renew(ctx context.Context, lockName, nodeIP string, ttlSeconds int) (bool, error) {
	nowMs := time.Now().UnixMilli()
	expireAtMs := nowMs + int64(ttlSeconds)*1000
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE taskq_partition_lock
		 SET expire_at_ms = ?
		 WHERE lock_name = ? AND leader_ip = ? AND expire_at_ms >= ?`,
		expireAtMs,
		lockName,
		nodeIP,
		nowMs,
	)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *sqliteStore) Release(ctx context.Context, lockName, nodeIP string) (bool, error) {
	nowMs := time.Now().UnixMilli()
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE taskq_partition_lock
		 SET expire_at_ms = ?
		 WHERE lock_name = ? AND leader_ip = ? AND expire_at_ms >= ?`,
		nowMs-1,
		lockName,
		nodeIP,
		nowMs,
	)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *sqliteStore) GetLeaderAddr(ctx context.Context, lockName string) (string, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT leader_ip
		 FROM taskq_partition_lock
		 WHERE lock_name = ? AND expire_at_ms >= ?`,
		lockName,
		time.Now().UnixMilli(),
	)

	var leaderIP string
	if err := row.Scan(&leaderIP); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return leaderIP, nil
}
