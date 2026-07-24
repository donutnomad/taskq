package gormstore

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
)

// DB 抽象本包对数据库句柄的依赖，使调用方可以注入自己持有的 gorm 连接
// （含已包装 tracing / 多租户 scope 的实例），而不必把句柄的构造交给本包。
// *gorm.DB 原样满足该接口，无需适配器。
//
// 接口刻意保持最小：本包所有查询都经 WithContext 派生，Close 需要 DB()
// 取回底层连接池；方言判断走 WithContext(ctx).Dialector（Dialector 是字段，
// 无法出现在接口里）。
type DB interface {
	WithContext(ctx context.Context) *gorm.DB
	DB() (*sql.DB, error)
}

// 编译期断言：调用方可直接传入 *gorm.DB。
var _ DB = (*gorm.DB)(nil)
