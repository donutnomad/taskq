package queue

import (
	"database/sql"
	"fmt"

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
CREATE TABLE IF NOT EXISTS taskq_partition_lock (
	lock_name    TEXT NOT NULL PRIMARY KEY,
	leader_ip    TEXT NOT NULL,
	expire_at_ms INTEGER NOT NULL,
	version      INTEGER NOT NULL DEFAULT 0
);
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
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return &sqliteStore{db: db}, nil
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}
