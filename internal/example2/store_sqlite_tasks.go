package example2

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/donutnomad/taskq/queue"
)

func (s *sqliteStore) CreateTasks(ctx context.Context, tasks []*queue.TaskRow) error {
	if len(tasks) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(
		ctx,
		`INSERT INTO tasks (task_id, queue_name, partition_id, data, created_at) VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, task := range tasks {
		if _, err := stmt.ExecContext(
			ctx,
			task.TaskID,
			task.QueueName,
			task.PartitionID,
			task.Data,
			task.CreatedAt,
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *sqliteStore) GetTasks(
	ctx context.Context,
	queueName string,
	partitionID int,
	minTaskID int64,
	maxTaskID int64,
	limit int,
) ([]*queue.TaskRow, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT task_id, queue_name, partition_id, data, created_at
		 FROM tasks
		 WHERE queue_name = ? AND partition_id = ?
		   AND task_id > ? AND task_id <= ?
		   AND dispatched_at IS NULL
		 ORDER BY task_id ASC LIMIT ?`,
		queueName,
		partitionID,
		minTaskID,
		maxTaskID,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*queue.TaskRow
	for rows.Next() {
		task := &queue.TaskRow{}
		if err := rows.Scan(
			&task.TaskID,
			&task.QueueName,
			&task.PartitionID,
			&task.Data,
			&task.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, task)
	}
	return result, rows.Err()
}

func (s *sqliteStore) DispatchTask(
	ctx context.Context,
	queueName string,
	partitionID int,
	taskID int64,
	dispatchedAt time.Time,
) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE tasks SET dispatched_at = ?
		 WHERE queue_name = ? AND partition_id = ? AND task_id = ? AND dispatched_at IS NULL`,
		dispatchedAt,
		queueName,
		partitionID,
		taskID,
	)
	if err != nil {
		return err
	}
	return requireAffectedRows(res, queue.ErrTaskNotFound)
}

func (s *sqliteStore) UnDispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE tasks SET dispatched_at = NULL
		 WHERE queue_name = ? AND partition_id = ? AND task_id = ?`,
		queueName,
		partitionID,
		taskID,
	)
	if err != nil {
		return err
	}
	return requireAffectedRows(res, queue.ErrTaskNotFound)
}

func (s *sqliteStore) ResetTimedOutTasks(
	ctx context.Context,
	queueName string,
	partitionID int,
	ackLevel int64,
	visTimeout time.Duration,
) ([]int64, error) {
	cutoff := time.Now().Add(-visTimeout)
	rows, err := s.db.QueryContext(
		ctx,
		`UPDATE tasks SET dispatched_at = NULL
		 WHERE queue_name = ? AND partition_id = ?
		   AND task_id > ?
		   AND dispatched_at IS NOT NULL
		   AND dispatched_at < ?
		 RETURNING task_id`,
		queueName,
		partitionID,
		ackLevel,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("reset timed out tasks: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *sqliteStore) DeleteTasks(ctx context.Context, queueName string, partitionID int, maxTaskID int64) (int64, error) {
	res, err := s.db.ExecContext(
		ctx,
		`DELETE FROM tasks WHERE queue_name = ? AND partition_id = ? AND task_id <= ?`,
		queueName,
		partitionID,
		maxTaskID,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *sqliteStore) DeleteTask(ctx context.Context, queueName string, partitionID int, taskID int64) error {
	res, err := s.db.ExecContext(
		ctx,
		`DELETE FROM tasks WHERE queue_name = ? AND partition_id = ? AND task_id = ?`,
		queueName,
		partitionID,
		taskID,
	)
	if err != nil {
		return err
	}
	return requireAffectedRows(res, queue.ErrTaskNotFound)
}

func requireAffectedRows(res sql.Result, errWhenZero error) error {
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errWhenZero
	}
	return nil
}
