package gormstore

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/donutnomad/taskq/membership"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MembershipStore shares its parent's database connection. Store.Close owns
// that connection and must run after the membership Monitor stops.
type MembershipStore struct {
	db    DB
	table string
}

var _ membership.MemberStore = (*MembershipStore)(nil)
var _ membership.ExpiredMemberPruner = (*MembershipStore)(nil)

type membershipMemberRow struct {
	ServiceName  string    `gorm:"column:service_name;primaryKey;size:191;index:idx_membership_active,priority:1"`
	RPCAddress   string    `gorm:"column:rpc_address;primaryKey;size:45"`
	RPCPort      uint16    `gorm:"column:rpc_port;primaryKey"`
	HostID       []byte    `gorm:"column:host_id;not null;size:64"`
	SessionStart time.Time `gorm:"column:session_start;not null"`
	HeartbeatAt  time.Time `gorm:"column:heartbeat_at;not null;index:idx_membership_active,priority:2"`
	ExpiresAt    time.Time `gorm:"column:expires_at;not null;index:idx_membership_expires;index:idx_membership_active,priority:3"`
	Draining     bool      `gorm:"column:draining;not null;default:false"`
	StartAt      int64     `gorm:"column:start_at;not null;default:0"`
	StopAt       int64     `gorm:"column:stop_at;not null;default:0"`
}

func (s *Store) Membership() *MembershipStore {
	return &MembershipStore{db: s.db, table: s.membershipTable}
}

func (s *MembershipStore) UpsertMember(ctx context.Context, req membership.UpsertMemberRequest) error {
	if err := validateMembershipHeartbeat(req); err != nil {
		return err
	}
	now := time.Now().UTC()
	row := membershipMemberRow{
		ServiceName:  req.ServiceName,
		RPCAddress:   req.RPCAddress.String(),
		RPCPort:      req.RPCPort,
		HostID:       append([]byte(nil), req.HostID...),
		SessionStart: req.SessionStart.UTC(),
		HeartbeatAt:  now,
		ExpiresAt:    now.Add(req.RecordExpiry),
		Draining:     req.Draining,
		StartAt:      req.StartAt,
		StopAt:       req.StopAt,
	}
	return s.db.WithContext(ctx).Table(s.table).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "service_name"}, {Name: "rpc_address"}, {Name: "rpc_port"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"host_id", "session_start", "heartbeat_at", "expires_at", "draining", "start_at", "stop_at",
		}),
	}).Create(&row).Error
}

func validateMembershipHeartbeat(req membership.UpsertMemberRequest) error {
	switch {
	case strings.TrimSpace(req.ServiceName) == "":
		return fmt.Errorf("gormstore membership: service name is required")
	case req.RPCAddress == nil || req.RPCAddress.To16() == nil:
		return fmt.Errorf("gormstore membership: valid RPC address is required")
	case req.RPCPort == 0:
		return fmt.Errorf("gormstore membership: RPC port is required")
	case len(req.HostID) == 0:
		return fmt.Errorf("gormstore membership: host ID is required")
	case req.SessionStart.IsZero():
		return fmt.Errorf("gormstore membership: session start is required")
	case req.RecordExpiry <= 0:
		return fmt.Errorf("gormstore membership: record expiry must be positive")
	default:
		return nil
	}
}

func (s *MembershipStore) GetActiveMembers(ctx context.Context, lastHeartbeatWithin time.Duration) ([]membership.MemberRecord, error) {
	now := time.Now().UTC()
	var rows []membershipMemberRow
	err := s.db.WithContext(ctx).Table(s.table).
		Where("heartbeat_at >= ? AND expires_at > ?", now.Add(-lastHeartbeatWithin), now).
		Order("service_name ASC, rpc_address ASC, rpc_port ASC").
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	members := make([]membership.MemberRecord, 0, len(rows))
	for _, row := range rows {
		address := net.ParseIP(row.RPCAddress)
		if address == nil {
			return nil, fmt.Errorf("gormstore membership: invalid persisted RPC address %q", row.RPCAddress)
		}
		members = append(members, membership.MemberRecord{
			RPCAddress:  address,
			RPCPort:     row.RPCPort,
			ServiceName: row.ServiceName,
			Draining:    row.Draining,
			StartAt:     row.StartAt,
			StopAt:      row.StopAt,
		})
	}
	return members, nil
}

func (s *MembershipStore) PruneExpiredMembers(ctx context.Context, maxCount int) error {
	if maxCount <= 0 {
		return nil
	}
	cutoff := time.Now().UTC()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var keys []membershipMemberRow
		if err := tx.Table(s.table).
			Select("service_name, rpc_address, rpc_port").
			Where("expires_at <= ?", cutoff).
			Order("expires_at ASC").
			Limit(maxCount).
			Find(&keys).Error; err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		conditions := make([]string, 0, len(keys))
		args := make([]any, 0, len(keys)*3)
		for _, key := range keys {
			conditions = append(conditions, "(service_name = ? AND rpc_address = ? AND rpc_port = ?)")
			args = append(args, key.ServiceName, key.RPCAddress, key.RPCPort)
		}
		return tx.Table(s.table).
			Where("expires_at <= ?", cutoff).
			Where(strings.Join(conditions, " OR "), args...).
			Delete(&membershipMemberRow{}).Error
	})
}
