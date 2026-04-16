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

func (s *sqliteStore) UpdateQueue(ctx context.Context, info *queue.QueueInfo, prevRangeID int64) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE queue_info
		 SET range_id = ?, ack_level = ?, next_task_id = ?
		 WHERE queue_name = ? AND partition_id = ? AND range_id = ?`,
		info.RangeID,
		info.AckLevel,
		info.NextTaskID,
		info.QueueName,
		info.PartitionID,
		prevRangeID,
	)
	if err != nil {
		return err
	}
	return requireAffectedRows(res, queue.ErrConditionFailed)
}
