package queue

import (
	"errors"
	"fmt"
	"testing"

	"gorm.io/gorm"
)

// isAlreadyExists 必须跨驱动成立：只认 SQLite 措辞会让 MySQL 上并发首次创建同一分区时
// CreateQueue 的唯一键冲突被当成真实故障，newPhysicalQueue 直接失败。
func TestIsAlreadyExistsAcrossDrivers(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"sqlite", errors.New("UNIQUE constraint failed: queue_info.queue_name, queue_info.partition_id"), true},
		{"mysql", errors.New("Error 1062 (23000): Duplicate entry 'wf-tasks-0' for key 'queue_info.PRIMARY'"), true},
		{"postgres", errors.New(`ERROR: duplicate key value violates unique constraint "queue_info_pkey"`), true},
		{"gorm translated", gorm.ErrDuplicatedKey, true},
		{"gorm translated wrapped", fmt.Errorf("create queue: %w", gorm.ErrDuplicatedKey), true},
		{"unrelated", errors.New("connection refused"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAlreadyExists(tc.err); got != tc.want {
				t.Fatalf("isAlreadyExists(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
