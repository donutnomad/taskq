package queue

import (
	"context"
	"fmt"
	"time"
)

func (s *sqliteStore) AcquireLease(
	ctx context.Context,
	queueName string,
	partitionID int,
	owner string,
	ttl time.Duration,
) (*QueueInfo, error) {
	now := time.Now()
	expiresAt := now.Add(ttl)

	if _, err := s.db.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO queue_info
		 (queue_name, partition_id, range_id, ack_level, next_task_id)
		 VALUES (?, ?, 1, 0, 1)`,
		queueName,
		partitionID,
	); err != nil {
		return nil, fmt.Errorf("ensure queue for lease: %w", err)
	}

	res, err := s.db.ExecContext(
		ctx,
		`UPDATE queue_info
		 SET lease_owner = ?, lease_expires_at = ?, range_id = range_id + 1
		 WHERE queue_name = ? AND partition_id = ?
		   AND (lease_owner IS NULL OR lease_expires_at < ? OR lease_owner = ?)`,
		owner,
		expiresAt,
		queueName,
		partitionID,
		now,
		owner,
	)
	if err != nil {
		return nil, err
	}
	if err := requireAffectedRows(res, ErrLeaseHeld); err != nil {
		return nil, err
	}

	return s.GetQueue(ctx, queueName, partitionID)
}

func (s *sqliteStore) RenewLease(
	ctx context.Context,
	queueName string,
	partitionID int,
	owner string,
	ttl time.Duration,
) error {
	expiresAt := time.Now().Add(ttl)
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE queue_info SET lease_expires_at = ?
		 WHERE queue_name = ? AND partition_id = ? AND lease_owner = ?`,
		expiresAt,
		queueName,
		partitionID,
		owner,
	)
	if err != nil {
		return err
	}
	return requireAffectedRows(res, ErrLeaseHeld)
}

func (s *sqliteStore) ReleaseLease(ctx context.Context, queueName string, partitionID int, owner string) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE queue_info SET lease_owner = NULL, lease_expires_at = NULL
		 WHERE queue_name = ? AND partition_id = ? AND lease_owner = ?`,
		queueName,
		partitionID,
		owner,
	)
	if err != nil {
		return err
	}
	return requireAffectedRows(res, ErrLeaseHeld)
}
