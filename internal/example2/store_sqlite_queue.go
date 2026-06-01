package example2

import (
	"context"

	"github.com/donutnomad/taskq/queue"
)

func (s *sqliteStore) CreateQueue(ctx context.Context, info *queue.QueueInfo) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO queue_info (queue_name, partition_id, range_id, ack_level, next_task_id)
		 VALUES (?, ?, ?, ?, ?)`,
		info.QueueName,
		info.PartitionID,
		info.RangeID,
		info.AckLevel,
		info.NextTaskID,
	)
	return err
}

func (s *sqliteStore) GetQueue(ctx context.Context, queueName string, partitionID int) (*queue.QueueInfo, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT queue_name, partition_id, range_id, ack_level, next_task_id
		 FROM queue_info WHERE queue_name = ? AND partition_id = ?`,
		queueName,
		partitionID,
	)

	info := &queue.QueueInfo{}
	if err := row.Scan(
		&info.QueueName,
		&info.PartitionID,
		&info.RangeID,
		&info.AckLevel,
		&info.NextTaskID,
	); err != nil {
		return nil, err
	}
	return info, nil
}

func (s *sqliteStore) AllocateTaskIDRange(ctx context.Context, queueName string, partitionID int, newNextTaskID int64, prevRangeID int64) (int64, error) {
	newRangeID := prevRangeID + 1
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE queue_info
		 SET range_id = ?, next_task_id = ?
		 WHERE queue_name = ? AND partition_id = ? AND range_id = ?`,
		newRangeID,
		newNextTaskID,
		queueName,
		partitionID,
		prevRangeID,
	)
	if err != nil {
		return 0, err
	}
	if err := requireAffectedRows(res, queue.ErrConditionFailed); err != nil {
		return 0, err
	}
	return newRangeID, nil
}

func (s *sqliteStore) AdvanceAckLevelAndDeleteTasks(ctx context.Context, queueName string, partitionID int, newAckLevel int64, prevRangeID int64) (int64, int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()

	newRangeID := prevRangeID + 1
	res, err := tx.ExecContext(
		ctx,
		`UPDATE queue_info
		 SET range_id = ?, ack_level = ?
		 WHERE queue_name = ? AND partition_id = ?
		   AND range_id = ?
		   AND ack_level < ?`,
		newRangeID,
		newAckLevel,
		queueName,
		partitionID,
		prevRangeID,
		newAckLevel,
	)
	if err != nil {
		return 0, 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	if affected == 0 {
		var curRangeID int64
		var curAckLevel int64
		if err := tx.QueryRowContext(
			ctx,
			`SELECT range_id, ack_level
			 FROM queue_info
			 WHERE queue_name = ? AND partition_id = ?`,
			queueName,
			partitionID,
		).Scan(&curRangeID, &curAckLevel); err != nil {
			return 0, 0, err
		}
		if curRangeID != prevRangeID {
			return 0, 0, queue.ErrConditionFailed
		}
		return prevRangeID, 0, nil
	}

	delRes, err := tx.ExecContext(
		ctx,
		`DELETE FROM tasks
		 WHERE queue_name = ? AND partition_id = ? AND task_id <= ?`,
		queueName,
		partitionID,
		newAckLevel,
	)
	if err != nil {
		return 0, 0, err
	}
	deleted, err := delRes.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return newRangeID, deleted, nil
}
