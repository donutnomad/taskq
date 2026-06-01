package queue

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

const testSQLiteSchema = `
CREATE TABLE IF NOT EXISTS queue_info (
	queue_name       TEXT NOT NULL,
	partition_id     INTEGER NOT NULL,
	range_id         INTEGER NOT NULL DEFAULT 1,
	ack_level        INTEGER NOT NULL DEFAULT 0,
	next_task_id     INTEGER NOT NULL DEFAULT 1,
	lease_owner      TEXT,
	lease_expires_at TIMESTAMP,
	PRIMARY KEY (queue_name, partition_id)
);
CREATE TABLE IF NOT EXISTS tasks (
	queue_name    TEXT NOT NULL,
	partition_id  INTEGER NOT NULL,
	task_id       INTEGER NOT NULL,
	data          BLOB NOT NULL,
	created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	dispatched_at TIMESTAMP,
	PRIMARY KEY (queue_name, partition_id, task_id)
);
CREATE INDEX IF NOT EXISTS idx_tasks_dispatch
	ON tasks (queue_name, partition_id, dispatched_at)
	WHERE dispatched_at IS NOT NULL;
`

type sqliteStore struct {
	db *sql.DB
}

func newTestStore(t *testing.T) Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(testSQLiteSchema); err != nil {
		db.Close()
		t.Fatal(err)
	}
	store := &sqliteStore{db: db}
	t.Cleanup(func() { store.Close() })
	return store
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}

func (s *sqliteStore) CreateQueue(ctx context.Context, info *QueueInfo) error {
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

func (s *sqliteStore) GetQueue(ctx context.Context, queueName string, partitionID int) (*QueueInfo, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT queue_name, partition_id, range_id, ack_level, next_task_id
		 FROM queue_info WHERE queue_name = ? AND partition_id = ?`,
		queueName,
		partitionID,
	)

	info := &QueueInfo{}
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
	if err := requireTestAffectedRows(res, ErrConditionFailed); err != nil {
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
			return 0, 0, ErrConditionFailed
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

func (s *sqliteStore) CreateTasks(ctx context.Context, tasks []*TaskRow) error {
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
) ([]*TaskRow, error) {
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

	var result []*TaskRow
	for rows.Next() {
		task := &TaskRow{}
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
	return requireTestAffectedRows(res, ErrTaskNotFound)
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
	return requireTestAffectedRows(res, ErrTaskNotFound)
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

func requireTestAffectedRows(res sql.Result, errWhenZero error) error {
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return errWhenZero
	}
	return nil
}
