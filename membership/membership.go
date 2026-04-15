// Package membership 封装了节点成员发现和一致性哈希路由能力。
// 对外暴露 Monitor 和 ServiceResolver 两个核心接口。
package membership

import (
	"context"
	"errors"
	"net"
	"time"
)

// ---- 错误定义 ----

var ErrUnknownService = errors.New("service not tracked by Monitor")
var ErrInsufficientHosts = errors.New("not enough hosts to serve the request")
var ErrListenerAlreadyExist = errors.New("listener already exist for the service")
var ErrIncorrectAddressFormat = errors.New("incorrect address format")

// ---- 核心数据结构 ----

// ChangedEvent 描述一次成员变化事件（节点加入、离开、信息变化）
type ChangedEvent struct {
	HostsAdded   []HostInfo
	HostsRemoved []HostInfo
	HostsChanged []HostInfo
}

// HostInfo 代表环中的一个节点
type HostInfo interface {
	// Identity 节点唯一标识（通常和 GetAddress 相同）
	Identity() string
	// GetAddress 返回 ip:port，必须是合法的 gRPC 地址
	GetAddress() string
}

// NewHostInfoFromAddress 从地址字符串创建一个 HostInfo
func NewHostInfoFromAddress(address string) HostInfo {
	return hostAddress(address)
}

type hostAddress string

func (a hostAddress) GetAddress() string { return string(a) }
func (a hostAddress) Identity() string   { return string(a) }

// ---- Monitor 接口 ----
//
// Monitor 是整个成员管理的顶层接口。
// 你的每个服务节点启动时调用 Start()，关闭时调用 Stop()。
// 通过 GetResolver(serviceName) 拿到对应服务的哈希环，再用 Lookup(key) 找负责该 key 的节点。
type Monitor interface {
	// Start 启动成员管理，在节点准备好接受请求后调用
	Start()
	// Stop 停止 Monitor
	Stop()
	// EvictSelf 主动离开（用于优雅下线）
	EvictSelf() error
	// EvictSelfAt 在指定时间离开（用于滚动发布，让流量提前切走）
	// 返回值是建议的等待时长，调用方应等待该时长后再退出进程
	EvictSelfAt(asOf time.Time) (time.Duration, error)
	// GetResolver 获取某个服务的哈希环查找器
	GetResolver(service string) (ServiceResolver, error)
	// GetReachableMembers 返回当前所有可达节点地址
	GetReachableMembers() ([]string, error)
	// WaitUntilInitialized 阻塞直到初始化完成
	WaitUntilInitialized(context.Context) error
	// SetDraining 设置本节点是否处于 draining 状态
	// draining=true 时该节点不会被 AvailableMembers 返回
	SetDraining(draining bool) error
	// ApproximateMaxPropagationTime 返回成员变更传播的近似上界时间
	ApproximateMaxPropagationTime() time.Duration
}

// ---- ServiceResolver 接口 ----
//
// ServiceResolver 是某个服务的一致性哈希环视图。
// 核心用法：Lookup(key) → 返回负责该 key 的节点地址。
type ServiceResolver interface {
	// Lookup 用一致性哈希找到负责 key 的节点
	Lookup(key string) (HostInfo, error)
	// LookupN 找 n 个节点（用于副本或 spreading）
	LookupN(key string, n int) []HostInfo
	// AddListener 注册监听器，成员变化时向 notifyChannel 发送事件
	AddListener(name string, notifyChannel chan<- *ChangedEvent) error
	// RemoveListener 移除监听器
	RemoveListener(name string) error
	// MemberCount 当前环中的节点总数
	MemberCount() int
	// AvailableMemberCount 非 draining 状态的节点数
	AvailableMemberCount() int
	// Members 返回所有节点
	Members() []HostInfo
	// AvailableMembers 返回非 draining 节点
	AvailableMembers() []HostInfo
	// RequestRefresh 请求立即刷新环
	RequestRefresh()
}

// ---- 持久化接口 ----

// MemberStore 是 Monitor 依赖的持久化接口。
// 用 MySQL/PostgreSQL/Redis 等实现这个接口即可。
type MemberStore interface {
	// UpsertMember 写入或更新本节点的心跳记录
	UpsertMember(ctx context.Context, req UpsertMemberRequest) error
	// GetActiveMembers 查询最近有心跳的节点列表
	GetActiveMembers(ctx context.Context, lastHeartbeatWithin time.Duration) ([]MemberRecord, error)
	// PruneExpiredMembers 清理过期记录（可选，防止表无限增长）
	PruneExpiredMembers(ctx context.Context, maxCount int) error
}

// UpsertMemberRequest 写入心跳时的请求参数
type UpsertMemberRequest struct {
	ServiceName  string
	RPCAddress   net.IP
	RPCPort      uint16
	HostID       []byte
	SessionStart time.Time
	RecordExpiry time.Duration
	Draining     bool
	StartAt      int64 // unix 秒，0 表示立即生效
	StopAt       int64 // unix 秒，0 表示不设置下线时间
}

// MemberRecord 从 DB 读取的节点记录
type MemberRecord struct {
	RPCAddress  net.IP
	RPCPort     uint16
	ServiceName string
	Draining    bool
	StartAt     int64 // unix 秒，0 表示立即生效
	StopAt      int64 // unix 秒，0 表示永不主动下线
}
