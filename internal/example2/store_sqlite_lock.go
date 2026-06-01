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

func (s *sqliteStore) Release(ctx context.Context, lockName, nodeIP string) error {
	nowMs := time.Now().UnixMilli()
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE taskq_partition_lock
		 SET expire_at_ms = ?
		 WHERE lock_name = ? AND leader_ip = ? AND expire_at_ms >= ?`,
		nowMs-1,
		lockName,
		nodeIP,
		nowMs,
	)
	return err
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

func (s *sqliteStore) ForceAssign(ctx context.Context, lockName, newLeaderIP string, ttlSeconds int) error {
	expireAtMs := time.Now().UnixMilli() + int64(ttlSeconds)*1000
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE taskq_partition_lock
		 SET leader_ip = ?, version = version + 1, expire_at_ms = ?
		 WHERE lock_name = ?`,
		newLeaderIP,
		expireAtMs,
		lockName,
	)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return dbleader.ErrLockNotFound
	}
	return nil
}

func (s *sqliteStore) UpsertMember(ctx context.Context, m dbleader.MemberInfo) error {
	nowMs := time.Now().UnixMilli()
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO taskq_partition_lock_members
		   (lock_name, instance_id, node_ip, mac_address, hostname, pid,
		    os, go_version, service_version, description, start_time_ms, last_seen_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(lock_name, instance_id) DO UPDATE SET
		   last_seen_ms = excluded.last_seen_ms,
		   node_ip = excluded.node_ip,
		   hostname = excluded.hostname,
		   pid = excluded.pid`,
		m.LockName,
		m.InstanceID,
		m.NodeIP,
		m.MacAddress,
		m.Hostname,
		m.PID,
		m.OS,
		m.GoVersion,
		m.ServiceVersion,
		m.Description,
		nowMs,
		nowMs,
	)
	return err
}

func (s *sqliteStore) ListMembers(ctx context.Context, lockName string, staleAfterSec int) ([]dbleader.MemberInfo, error) {
	cutoff := time.Now().UnixMilli() - int64(staleAfterSec)*1000
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT lock_name, instance_id, node_ip, mac_address, hostname, pid,
		        os, go_version, service_version, description, start_time_ms, last_seen_ms
		 FROM taskq_partition_lock_members
		 WHERE lock_name = ? AND last_seen_ms >= ?`,
		lockName,
		cutoff,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := []dbleader.MemberInfo{}
	for rows.Next() {
		var member dbleader.MemberInfo
		var startMs int64
		var lastSeenMs int64
		if err := rows.Scan(
			&member.LockName,
			&member.InstanceID,
			&member.NodeIP,
			&member.MacAddress,
			&member.Hostname,
			&member.PID,
			&member.OS,
			&member.GoVersion,
			&member.ServiceVersion,
			&member.Description,
			&startMs,
			&lastSeenMs,
		); err != nil {
			return nil, err
		}
		member.StartTime = time.UnixMilli(startMs)
		member.LastSeen = time.UnixMilli(lastSeenMs)
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return members, nil
}

func (s *sqliteStore) PurgeStaleMembers(ctx context.Context, lockName string, staleAfterSec int) (int64, error) {
	cutoff := time.Now().UnixMilli() - int64(staleAfterSec)*1000
	res, err := s.db.ExecContext(
		ctx,
		`DELETE FROM taskq_partition_lock_members
		 WHERE lock_name = ? AND last_seen_ms < ?`,
		lockName,
		cutoff,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
