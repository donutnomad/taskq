package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteSchema = `
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

// NewSQLiteStore 创建 SQLite 存储实例
func NewSQLiteStore(dsn string) (Store, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return &sqliteStore{db: db}, nil
}

func (s *sqliteStore) CreateQueue(ctx context.Context, info *QueueInfo) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO queue_info (queue_name, partition_id, range_id, ack_level, next_task_id)
		 VALUES (?, ?, ?, ?, ?)`,
		info.QueueName, info.PartitionID, info.RangeID, info.AckLevel, info.NextTaskID)
	return err
}

func (s *sqliteStore) GetQueue(ctx context.Context, queueName string, partitionID int) (*QueueInfo, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT queue_name, partition_id, range_id, ack_level, next_task_id
		 FROM queue_info WHERE queue_name = ? AND partition_id = ?`,
		queueName, partitionID)
	info := &QueueInfo{}
	if err := row.Scan(&info.QueueName, &info.PartitionID, &info.RangeID,
		&info.AckLevel, &info.NextTaskID); err != nil {
		return nil, err
	}
	return info, nil
}

func (s *sqliteStore) UpdateQueue(ctx context.Context, info *QueueInfo, prevRangeID int64) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE queue_info
		 SET range_id = ?, ack_level = ?, next_task_id = ?
		 WHERE queue_name = ? AND partition_id = ? AND range_id = ?`,
		info.RangeID, info.AckLevel, info.NextTaskID,
		info.QueueName, info.PartitionID, prevRangeID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConditionFailed
	}
	return nil
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
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO tasks (task_id, queue_name, partition_id, data, created_at) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, t := range tasks {
		if _, err := stmt.ExecContext(ctx, t.TaskID, t.QueueName, t.PartitionID, t.Data, t.CreatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStore) GetTasks(ctx context.Context, queueName string, partitionID int,
	minTaskID, maxTaskID int64, limit int) ([]*TaskRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT task_id, queue_name, partition_id, data, created_at
		 FROM tasks
		 WHERE queue_name = ? AND partition_id = ?
		   AND task_id > ? AND task_id <= ?
		   AND dispatched_at IS NULL
		 ORDER BY task_id ASC LIMIT ?`,
		queueName, partitionID, minTaskID, maxTaskID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*TaskRow
	for rows.Next() {
		r := &TaskRow{}
		if err := rows.Scan(&r.TaskID, &r.QueueName, &r.PartitionID, &r.Data, &r.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *sqliteStore) DispatchTask(ctx context.Context, queueName string, partitionID int,
	taskID int64, dispatchedAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET dispatched_at = ?
		 WHERE queue_name = ? AND partition_id = ? AND task_id = ?`,
		dispatchedAt, queueName, partitionID, taskID)
	return err
}

func (s *sqliteStore) UnDispatchTask(ctx context.Context, queueName string, partitionID int,
	taskID int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET dispatched_at = NULL
		 WHERE queue_name = ? AND partition_id = ? AND task_id = ?`,
		queueName, partitionID, taskID)
	return err
}

func (s *sqliteStore) ResetTimedOutTasks(ctx context.Context, queueName string, partitionID int,
	ackLevel int64, visTimeout time.Duration) ([]int64, error) {
	cutoff := time.Now().Add(-visTimeout)
	rows, err := s.db.QueryContext(ctx,
		`UPDATE tasks SET dispatched_at = NULL
		 WHERE queue_name = ? AND partition_id = ?
		   AND task_id > ?
		   AND dispatched_at IS NOT NULL
		   AND dispatched_at < ?
		 RETURNING task_id`,
		queueName, partitionID, ackLevel, cutoff)
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

func (s *sqliteStore) DeleteTasks(ctx context.Context, queueName string, partitionID int,
	maxTaskID int64) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM tasks WHERE queue_name = ? AND partition_id = ? AND task_id <= ?`,
		queueName, partitionID, maxTaskID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *sqliteStore) DeleteTask(ctx context.Context, queueName string, partitionID int,
	taskID int64) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM tasks WHERE queue_name = ? AND partition_id = ? AND task_id = ?`,
		queueName, partitionID, taskID)
	return err
}

func (s *sqliteStore) AcquireLease(ctx context.Context, queueName string, partitionID int,
	owner string, ttl time.Duration) (*QueueInfo, error) {

	now := time.Now()
	expiresAt := now.Add(ttl)

	// 确保 queue_info 行存在
	_, err := s.GetQueue(ctx, queueName, partitionID)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if err := s.CreateQueue(ctx, &QueueInfo{
			QueueName:   queueName,
			PartitionID: partitionID,
			RangeID:     1,
			AckLevel:    0,
			NextTaskID:  1,
		}); err != nil {
			return nil, fmt.Errorf("create queue for lease: %w", err)
		}
	}

	// 获取 lease + 递增 rangeID（原子操作）
	res, err := s.db.ExecContext(ctx,
		`UPDATE queue_info
		 SET lease_owner = ?, lease_expires_at = ?, range_id = range_id + 1
		 WHERE queue_name = ? AND partition_id = ?
		   AND (lease_owner IS NULL OR lease_expires_at < ? OR lease_owner = ?)`,
		owner, expiresAt,
		queueName, partitionID,
		now, owner)
	if err != nil {
		return nil, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, ErrLeaseHeld
	}

	// 读取最新状态
	return s.GetQueue(ctx, queueName, partitionID)
}

func (s *sqliteStore) RenewLease(ctx context.Context, queueName string, partitionID int,
	owner string, ttl time.Duration) error {

	expiresAt := time.Now().Add(ttl)
	res, err := s.db.ExecContext(ctx,
		`UPDATE queue_info SET lease_expires_at = ?
		 WHERE queue_name = ? AND partition_id = ? AND lease_owner = ?`,
		expiresAt, queueName, partitionID, owner)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseHeld
	}
	return nil
}

func (s *sqliteStore) ReleaseLease(ctx context.Context, queueName string, partitionID int,
	owner string) error {

	_, err := s.db.ExecContext(ctx,
		`UPDATE queue_info SET lease_owner = NULL, lease_expires_at = NULL
		 WHERE queue_name = ? AND partition_id = ? AND lease_owner = ?`,
		queueName, partitionID, owner)
	return err
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}
