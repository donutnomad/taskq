package gormstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/queue"
	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	defaultLockTable       = "taskq_partition_lock"
	defaultMemberTable     = "taskq_partition_lock_members"
	defaultMembershipTable = "taskq_membership_members"
	visibilityResetBatch   = 500
)

// Config controls Store initialization. AutoMigrate is explicit so production
// callers can own schema rollout while tests and examples can opt in.
type Config struct {
	AutoMigrate     bool
	LockTable       string
	MemberTable     string
	MembershipTable string
}

// Store is a GORM-backed implementation of queue.Store and dbleader.LockStore.
type Store struct {
	db              *gorm.DB
	lockTable       string
	memberTable     string
	membershipTable string
}

var _ queue.Store = (*Store)(nil)
var _ dbleader.LockStore = (*Store)(nil)

func New(db *gorm.DB, cfg Config) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("gormstore: db is required")
	}
	s := &Store{
		db:              db,
		lockTable:       cfg.LockTable,
		memberTable:     cfg.MemberTable,
		membershipTable: cfg.MembershipTable,
	}
	if s.lockTable == "" {
		s.lockTable = defaultLockTable
	}
	if s.memberTable == "" {
		s.memberTable = defaultMemberTable
	}
	if s.membershipTable == "" {
		s.membershipTable = defaultMembershipTable
	}
	if cfg.AutoMigrate {
		if err := s.AutoMigrate(context.Background()); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func NewSQLite(dsn string, cfg Config) (*Store, error) {
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(1)
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}
	if err := db.Exec("PRAGMA busy_timeout=5000").Error; err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	s, err := New(db, cfg)
	if err != nil {
		sqlDB.Close()
		return nil, err
	}
	return s, nil
}

func NewMySQL(dsn string, cfg Config) (*Store, error) {
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	s, err := New(db, cfg)
	if err != nil {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			sqlDB.Close()
		}
		return nil, err
	}
	return s, nil
}

func (s *Store) DB() *gorm.DB {
	return s.db
}

func (s *Store) Close() error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func (s *Store) AutoMigrate(ctx context.Context) error {
	db := s.db.WithContext(ctx)
	if err := db.AutoMigrate(&queueInfoRow{}, &taskRow{}); err != nil {
		return err
	}
	if err := db.Table(s.lockTable).AutoMigrate(&lockRow{}); err != nil {
		return err
	}
	if err := db.Table(s.memberTable).AutoMigrate(&memberRow{}); err != nil {
		return err
	}
	return db.Table(s.membershipTable).AutoMigrate(&membershipMemberRow{})
}

type queueInfoRow struct {
	QueueName   string `gorm:"column:queue_name;primaryKey;size:191"`
	PartitionID int    `gorm:"column:partition_id;primaryKey"`
	RangeID     int64  `gorm:"column:range_id;not null;default:1"`
	AckLevel    int64  `gorm:"column:ack_level;not null;default:0"`
	NextTaskID  int64  `gorm:"column:next_task_id;not null;default:1"`
}

func (queueInfoRow) TableName() string { return "queue_info" }

type taskRow struct {
	QueueName       string     `gorm:"column:queue_name;primaryKey;size:191;index:idx_tasks_visibility,priority:1;index:idx_tasks_routing_deadline,priority:1"`
	PartitionID     int        `gorm:"column:partition_id;primaryKey;index:idx_tasks_visibility,priority:2;index:idx_tasks_routing_deadline,priority:2"`
	TaskID          int64      `gorm:"column:task_id;primaryKey;index:idx_tasks_visibility,priority:5;index:idx_tasks_routing_deadline,priority:5"`
	RoutingKey      string     `gorm:"column:routing_key;not null;default:'';size:191;index:idx_tasks_visibility,priority:4"`
	RoutingDeadline *time.Time `gorm:"column:routing_deadline;index:idx_tasks_routing_deadline,priority:4"`
	Data            []byte     `gorm:"column:data;not null"`
	CreatedAt       time.Time  `gorm:"column:created_at;not null"`
	DispatchedAt    *time.Time `gorm:"column:dispatched_at;index:idx_tasks_visibility,priority:3;index:idx_tasks_routing_deadline,priority:3"`
}

func (taskRow) TableName() string { return "tasks" }

type lockRow struct {
	LockName   string `gorm:"column:lock_name;primaryKey;size:191"`
	LeaderIP   string `gorm:"column:leader_ip;not null;size:191"`
	ExpireAtMS int64  `gorm:"column:expire_at_ms;not null"`
	Version    uint64 `gorm:"column:version;not null;default:0"`
}

type memberRow struct {
	LockName       string    `gorm:"column:lock_name;primaryKey;size:191;index:idx_members_lock_lastseen,priority:1"`
	InstanceID     string    `gorm:"column:instance_id;primaryKey;size:191"`
	NodeIP         string    `gorm:"column:node_ip;not null;size:191"`
	MacAddress     string    `gorm:"column:mac_address;not null;size:64;default:''"`
	Hostname       string    `gorm:"column:hostname;not null;size:191;default:''"`
	PID            int       `gorm:"column:pid;not null;default:0"`
	OS             string    `gorm:"column:os;not null;size:32;default:''"`
	GoVersion      string    `gorm:"column:go_version;not null;size:64;default:''"`
	ServiceVersion string    `gorm:"column:service_version;not null;size:64;default:''"`
	Description    string    `gorm:"column:description;not null;size:256;default:''"`
	StartTime      time.Time `gorm:"column:start_time;not null"`
	LastSeen       time.Time `gorm:"column:last_seen;not null;index:idx_members_lock_lastseen,priority:2"`
}

func toQueueInfo(row queueInfoRow) *queue.QueueInfo {
	return &queue.QueueInfo{
		QueueName:   row.QueueName,
		PartitionID: row.PartitionID,
		RangeID:     row.RangeID,
		AckLevel:    row.AckLevel,
		NextTaskID:  row.NextTaskID,
	}
}

func fromTask(row *queue.TaskRow) taskRow {
	return taskRow{
		QueueName:       row.QueueName,
		PartitionID:     row.PartitionID,
		TaskID:          row.TaskID,
		RoutingKey:      row.RoutingKey,
		RoutingDeadline: nullableTime(row.RoutingDeadline),
		Data:            row.Data,
		CreatedAt:       row.CreatedAt,
		DispatchedAt:    row.DispatchedAt,
	}
}

func toTask(row taskRow) *queue.TaskRow {
	return &queue.TaskRow{
		TaskID:          row.TaskID,
		QueueName:       row.QueueName,
		PartitionID:     row.PartitionID,
		RoutingKey:      row.RoutingKey,
		RoutingDeadline: valueTime(row.RoutingDeadline),
		Data:            row.Data,
		CreatedAt:       row.CreatedAt,
		DispatchedAt:    row.DispatchedAt,
	}
}

func nullableTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func valueTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

func (s *Store) CreateQueue(ctx context.Context, info *queue.QueueInfo) error {
	return s.db.WithContext(ctx).Create(&queueInfoRow{
		QueueName:   info.QueueName,
		PartitionID: info.PartitionID,
		RangeID:     info.RangeID,
		AckLevel:    info.AckLevel,
		NextTaskID:  info.NextTaskID,
	}).Error
}

func (s *Store) GetQueue(ctx context.Context, queueName string, partitionID int) (*queue.QueueInfo, error) {
	var row queueInfoRow
	err := s.db.WithContext(ctx).
		Where("queue_name = ? AND partition_id = ?", queueName, partitionID).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, err
	}
	return toQueueInfo(row), nil
}

func (s *Store) AllocateTaskIDRange(ctx context.Context, queueName string, partitionID int, newNextTaskID int64, prevRangeID int64) (int64, error) {
	newRangeID := prevRangeID + 1
	result := s.db.WithContext(ctx).Model(&queueInfoRow{}).
		Where("queue_name = ? AND partition_id = ? AND range_id = ?", queueName, partitionID, prevRangeID).
		Updates(map[string]any{"range_id": newRangeID, "next_task_id": newNextTaskID})
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected == 0 {
		return 0, queue.ErrConditionFailed
	}
	return newRangeID, nil
}

func (s *Store) AdvanceAckLevelAndDeleteTasks(ctx context.Context, queueName string, partitionID int, newAckLevel int64, prevRangeID int64) (int64, int64, error) {
	newRangeID := prevRangeID + 1
	var deleted int64
	advanced := false
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&queueInfoRow{}).
			Where("queue_name = ? AND partition_id = ? AND range_id = ? AND ack_level < ?", queueName, partitionID, prevRangeID, newAckLevel).
			Updates(map[string]any{"range_id": newRangeID, "ack_level": newAckLevel})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			var cur queueInfoRow
			err := tx.Where("queue_name = ? AND partition_id = ?", queueName, partitionID).First(&cur).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return sql.ErrNoRows
			}
			if err != nil {
				return err
			}
			if cur.RangeID != prevRangeID {
				return queue.ErrConditionFailed
			}
			return nil
		}
		advanced = true

		del := tx.Where("queue_name = ? AND partition_id = ? AND task_id <= ?", queueName, partitionID, newAckLevel).
			Delete(&taskRow{})
		if del.Error != nil {
			return del.Error
		}
		deleted = del.RowsAffected
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if !advanced {
		return prevRangeID, 0, nil
	}
	return newRangeID, deleted, nil
}

func (s *Store) CreateTasks(ctx context.Context, tasks []*queue.TaskRow) error {
	if len(tasks) == 0 {
		return nil
	}
	rows := make([]taskRow, len(tasks))
	for i, task := range tasks {
		rows[i] = fromTask(task)
	}
	return s.db.WithContext(ctx).Create(&rows).Error
}

func (s *Store) CreateTasksAndAdvanceRange(
	ctx context.Context,
	queueName string,
	partitionID int,
	tasks []*queue.TaskRow,
	newNextTaskID int64,
	prevRangeID int64,
) (int64, error) {
	if len(tasks) == 0 {
		return prevRangeID, nil
	}
	rows := make([]taskRow, len(tasks))
	for i, task := range tasks {
		rows[i] = fromTask(task)
	}

	newRangeID := prevRangeID + 1
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&queueInfoRow{}).
			Where("queue_name = ? AND partition_id = ? AND range_id = ?", queueName, partitionID, prevRangeID).
			Updates(map[string]any{"range_id": newRangeID, "next_task_id": newNextTaskID})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return queue.ErrConditionFailed
		}
		return tx.Create(&rows).Error
	})
	if err != nil {
		return 0, err
	}
	return newRangeID, nil
}

func (s *Store) GetTasks(ctx context.Context, queueName string, partitionID int, minTaskID, maxTaskID int64, limit int) ([]*queue.TaskRow, error) {
	return s.getTasks(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, nil)
}

func (s *Store) GetTasksWithOptions(ctx context.Context, queueName string, partitionID int, minTaskID, maxTaskID int64, limit int, opts queue.TaskQueryOptions) ([]*queue.TaskRow, error) {
	return s.getTasks(ctx, queueName, partitionID, minTaskID, maxTaskID, limit, &opts)
}

func (s *Store) getTasks(ctx context.Context, queueName string, partitionID int, minTaskID, maxTaskID int64, limit int, opts *queue.TaskQueryOptions) ([]*queue.TaskRow, error) {
	var rows []taskRow
	query := s.db.WithContext(ctx).
		Where("queue_name = ? AND partition_id = ? AND task_id > ? AND task_id <= ? AND dispatched_at IS NULL", queueName, partitionID, minTaskID, maxTaskID)
	if opts != nil {
		fallbackAt := opts.RoutingFallbackAt
		if !fallbackAt.IsZero() {
			fallbackAt = fallbackAt.UTC()
		}
		hasKeys := len(opts.AcceptedRoutingKeys) > 0
		hasFallback := !fallbackAt.IsZero()
		switch {
		case hasKeys && hasFallback:
			query = query.Where("(routing_key = ? OR routing_key IN ? OR (routing_deadline IS NOT NULL AND routing_deadline <= ?))", "", opts.AcceptedRoutingKeys, fallbackAt)
		case hasKeys:
			query = query.Where("(routing_key = ? OR routing_key IN ?)", "", opts.AcceptedRoutingKeys)
		case hasFallback:
			query = query.Where("(routing_key = ? OR (routing_deadline IS NOT NULL AND routing_deadline <= ?))", "", fallbackAt)
		default:
			query = query.Where("routing_key = ?", "")
		}
	}
	err := query.
		Order("task_id ASC").
		Limit(limit).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]*queue.TaskRow, len(rows))
	for i, row := range rows {
		out[i] = toTask(row)
	}
	return out, nil
}

func (s *Store) DispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64, dispatchedAt time.Time) error {
	result := s.db.WithContext(ctx).Model(&taskRow{}).
		Where("queue_name = ? AND partition_id = ? AND task_id = ? AND dispatched_at IS NULL", queueName, partitionID, taskID).
		Update("dispatched_at", dispatchedAt)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return queue.ErrTaskNotFound
	}
	return nil
}

func (s *Store) UnDispatchTask(ctx context.Context, queueName string, partitionID int, taskID int64) error {
	result := s.db.WithContext(ctx).Model(&taskRow{}).
		Where("queue_name = ? AND partition_id = ? AND task_id = ?", queueName, partitionID, taskID).
		Update("dispatched_at", nil)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return queue.ErrTaskNotFound
	}
	return nil
}

func (s *Store) ResetTimedOutTasks(ctx context.Context, queueName string, partitionID int, opts queue.VisibilityResetOptions) ([]int64, error) {
	cutoff := time.Now().Add(-opts.VisibilityTimeout)
	acknowledged := make(map[int64]struct{}, len(opts.AcknowledgedIDs))
	for _, taskID := range opts.AcknowledgedIDs {
		acknowledged[taskID] = struct{}{}
	}
	var candidateIDs []int64
	var resetIDs []int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&taskRow{}).
			Where("queue_name = ? AND partition_id = ? AND task_id > ? AND dispatched_at IS NOT NULL AND dispatched_at < ?", queueName, partitionID, opts.AckLevel, cutoff).
			Order("task_id ASC").
			Pluck("task_id", &candidateIDs).Error; err != nil {
			return err
		}
		resetIDs = make([]int64, 0, len(candidateIDs))
		for _, taskID := range candidateIDs {
			if _, ok := acknowledged[taskID]; !ok {
				resetIDs = append(resetIDs, taskID)
			}
		}
		for start := 0; start < len(resetIDs); start += visibilityResetBatch {
			end := min(start+visibilityResetBatch, len(resetIDs))
			if err := tx.Model(&taskRow{}).
				Where("queue_name = ? AND partition_id = ? AND task_id IN ? AND dispatched_at IS NOT NULL AND dispatched_at < ?", queueName, partitionID, resetIDs[start:end], cutoff).
				Update("dispatched_at", nil).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reset timed out tasks: %w", err)
	}
	return resetIDs, nil
}

func (s *Store) nowMS() int64 {
	return time.Now().UnixMilli()
}

func (s *Store) TryAcquire(ctx context.Context, lockName, nodeIP string, ttlSeconds int) (dbleader.AcquireResult, error) {
	nowMS := s.nowMS()
	expireAtMS := nowMS + int64(ttlSeconds)*1000
	var row lockRow
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if s.db.Dialector.Name() == "mysql" {
			if err := tx.Exec(`
				INSERT INTO `+s.lockTable+` (lock_name, leader_ip, expire_at_ms, version)
				VALUES (?, ?, ?, 1)
				ON DUPLICATE KEY UPDATE
					version = IF(expire_at_ms < ?, version + 1, version),
					leader_ip = IF(expire_at_ms < ?, VALUES(leader_ip), leader_ip),
					expire_at_ms = IF(expire_at_ms < ? OR leader_ip = VALUES(leader_ip), VALUES(expire_at_ms), expire_at_ms)
			`, lockName, nodeIP, expireAtMS, nowMS, nowMS, nowMS).Error; err != nil {
				return err
			}
		} else {
			if err := tx.Exec(`
				INSERT INTO `+s.lockTable+` (lock_name, leader_ip, expire_at_ms, version)
				VALUES (?, ?, ?, 1)
				ON CONFLICT(lock_name) DO UPDATE SET
					version = CASE WHEN expire_at_ms < ? THEN version + 1 ELSE version END,
					leader_ip = CASE WHEN expire_at_ms < ? THEN excluded.leader_ip ELSE leader_ip END,
					expire_at_ms = CASE WHEN expire_at_ms < ? OR leader_ip = excluded.leader_ip THEN excluded.expire_at_ms ELSE expire_at_ms END
			`, lockName, nodeIP, expireAtMS, nowMS, nowMS, nowMS).Error; err != nil {
				return err
			}
		}
		return tx.Table(s.lockTable).Where("lock_name = ?", lockName).First(&row).Error
	})
	if err != nil {
		return dbleader.AcquireResult{}, err
	}
	return dbleader.AcquireResult{Acquired: row.LeaderIP == nodeIP, LeaderIP: row.LeaderIP, Version: row.Version}, nil
}

func (s *Store) Renew(ctx context.Context, lockName, nodeIP string, ttlSeconds int) (bool, error) {
	nowMS := s.nowMS()
	expireAtMS := nowMS + int64(ttlSeconds)*1000
	result := s.db.WithContext(ctx).Table(s.lockTable).
		Where("lock_name = ? AND leader_ip = ? AND expire_at_ms >= ?", lockName, nodeIP, nowMS).
		Update("expire_at_ms", expireAtMS)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (s *Store) Release(ctx context.Context, lockName, nodeIP string) error {
	return s.db.WithContext(ctx).Table(s.lockTable).
		Where("lock_name = ? AND leader_ip = ? AND expire_at_ms >= ?", lockName, nodeIP, s.nowMS()).
		Update("expire_at_ms", s.nowMS()-1).Error
}

func (s *Store) GetLeaderAddr(ctx context.Context, lockName string) (string, error) {
	var row lockRow
	err := s.db.WithContext(ctx).Table(s.lockTable).
		Where("lock_name = ? AND expire_at_ms >= ?", lockName, s.nowMS()).
		First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return row.LeaderIP, nil
}

func (s *Store) ForceAssign(ctx context.Context, lockName, newLeaderIP string, ttlSeconds int) error {
	result := s.db.WithContext(ctx).Table(s.lockTable).
		Where("lock_name = ?", lockName).
		Updates(map[string]any{
			"leader_ip":    newLeaderIP,
			"version":      gorm.Expr("version + 1"),
			"expire_at_ms": s.nowMS() + int64(ttlSeconds)*1000,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return dbleader.ErrLockNotFound
	}
	return nil
}

func (s *Store) UpsertMember(ctx context.Context, m dbleader.MemberInfo) error {
	now := time.Now()
	if s.db.Dialector.Name() == "mysql" {
		return s.db.WithContext(ctx).Exec(`
			INSERT INTO `+s.memberTable+`
				(lock_name, instance_id, node_ip, mac_address, hostname, pid, os, go_version, service_version, description, start_time, last_seen)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				last_seen = VALUES(last_seen),
				node_ip = VALUES(node_ip),
				hostname = VALUES(hostname),
				pid = VALUES(pid)
		`, m.LockName, m.InstanceID, m.NodeIP, m.MacAddress, m.Hostname, m.PID, m.OS, m.GoVersion, m.ServiceVersion, m.Description, now, now).Error
	}
	return s.db.WithContext(ctx).Exec(`
		INSERT INTO `+s.memberTable+`
			(lock_name, instance_id, node_ip, mac_address, hostname, pid, os, go_version, service_version, description, start_time, last_seen)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(lock_name, instance_id) DO UPDATE SET
			last_seen = excluded.last_seen,
			node_ip = excluded.node_ip,
			hostname = excluded.hostname,
			pid = excluded.pid
	`, m.LockName, m.InstanceID, m.NodeIP, m.MacAddress, m.Hostname, m.PID, m.OS, m.GoVersion, m.ServiceVersion, m.Description, now, now).Error
}

func (s *Store) ListMembers(ctx context.Context, lockName string, staleAfterSec int) ([]dbleader.MemberInfo, error) {
	cutoff := time.Now().Add(-time.Duration(staleAfterSec) * time.Second)
	var rows []memberRow
	err := s.db.WithContext(ctx).Table(s.memberTable).
		Where("lock_name = ? AND last_seen >= ?", lockName, cutoff).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]dbleader.MemberInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, dbleader.MemberInfo{
			LockName:       row.LockName,
			InstanceID:     row.InstanceID,
			NodeIP:         row.NodeIP,
			MacAddress:     row.MacAddress,
			Hostname:       row.Hostname,
			PID:            row.PID,
			OS:             row.OS,
			GoVersion:      row.GoVersion,
			ServiceVersion: row.ServiceVersion,
			Description:    row.Description,
			StartTime:      row.StartTime,
			LastSeen:       row.LastSeen,
		})
	}
	return out, nil
}

func (s *Store) PurgeStaleMembers(ctx context.Context, lockName string, staleAfterSec int) (int64, error) {
	cutoff := time.Now().Add(-time.Duration(staleAfterSec) * time.Second)
	result := s.db.WithContext(ctx).Table(s.memberTable).
		Where("lock_name = ? AND last_seen < ?", lockName, cutoff).
		Delete(&memberRow{})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}
