package ringpop

// monitor.go
//
// Monitor 是整个成员管理的顶层实现。
//
// 核心职责：
//   1. 启动时把自己的 ip:port 写入 DB（让别人能发现自己）
//   2. 从 DB 读取当前存活节点，作为 Gossip 的种子节点
//   3. 通过 ringpop.Bootstrap() 加入 Gossip 环
//   4. 定期心跳写 DB（供后续新节点发现种子用）
//   5. 管理多个 ServiceResolver（每个服务角色一个哈希环）
//
// DB 的作用：冷启动种子发现（仅在新节点 Bootstrap 时使用一次）
// Gossip 的作用：运行时成员变化的实时感知（节点间直接通信，不过 DB）

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/ringpop-go/discovery/statichosts"
	"github.com/temporalio/ringpop-go/swim"
	"github.com/temporalio/tchannel-go"
)

const (
	// DB 心跳记录的过期时间（48 小时，主要供运维排查问题用）
	upsertMembershipRecordExpiryDefault = time.Hour * 48

	// Bootstrap 时从 DB 读取的存活判断阈值：20 秒内有心跳才算存活
	// = 10秒基础频率 + 5秒抖动 + 5秒时钟偏差容忍
	healthyHostLastHeartbeatCutoff = time.Second * 20

	// Bootstrap 最多重试次数
	maxBootstrapRetries = 5

	// startAt/stopAt 时间戳的最大允许未来时间（秒）
	// 超过这个值认为时钟异常，直接立即生效
	maxScheduledEventTimeSeconds = 15
)

// MemberStore 是 Monitor 依赖的持久化接口。
// 你只需要实现这个接口（用 MySQL/PostgreSQL/Redis 都可以），不需要依赖 Temporal 的 persistence 包。
type MemberStore interface {
	// UpsertMember 写入或更新本节点的心跳记录
	UpsertMember(ctx context.Context, req UpsertMemberRequest) error
	// GetActiveMembers 查询最近有心跳的节点列表（用于 Bootstrap 种子发现）
	GetActiveMembers(ctx context.Context, lastHeartbeatWithin time.Duration) ([]MemberRecord, error)
	// PruneExpiredMembers 清理过期记录（可选，防止表无限增长）
	PruneExpiredMembers(ctx context.Context, maxCount int) error
}

// UpsertMemberRequest 写入心跳时的请求参数
type UpsertMemberRequest struct {
	ServiceName  string    // 服务角色名称，如 "matching"
	RPCAddress   net.IP    // gRPC 监听 IP
	RPCPort      uint16    // gRPC 监听端口
	MembershipPort uint16  // ringpop/gossip 监听端口（可以和 RPCPort 不同）
	HostID       []byte    // 节点唯一 ID（uuid bytes）
	SessionStart time.Time // 本次启动时间
	RecordExpiry time.Duration // DB 记录的 TTL
}

// MemberRecord 从 DB 读取的节点记录
type MemberRecord struct {
	RPCAddress     net.IP
	MembershipPort uint16 // gossip 端口（用于 Bootstrap）
}

// Monitor 实现
type Monitor struct {
	mu     sync.Mutex
	status int32 // 0=init, 1=started, 2=stopped

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	serviceName string            // 本节点的服务角色
	services    map[string]int    // serviceName → gRPC port（本集群所有服务角色及其端口）
	rp          *ringpop.Ringpop
	store       MemberStore

	maxJoinDuration time.Duration
	propagationTime time.Duration
	joinTime        time.Time    // 计划加入时间（用于滚动发布，0 表示立即加入）

	rings         map[string]*serviceResolver // serviceName → resolver
	replicaPoints int

	hostID              []byte
	broadcastHostPort   string // 本节点对外广播的 gossip 地址（ip:membershipPort）
	broadcastRPCAddress string // 本节点对外广播的 gRPC 地址（ip:rpcPort）

	initialized chan struct{}
}

// NewMonitor 创建一个 Monitor 实例。
//
// 参数说明：
//   - serviceName: 本节点的服务角色名（如 "matching"）
//   - services: 集群中所有服务角色及其 gRPC 端口（用于构建多个 ServiceResolver）
//   - rp: 已创建但未启动的 ringpop 实例
//   - store: DB 操作接口
//   - broadcastHostPort: 本节点 gossip 地址，格式 "ip:membershipPort"
//   - broadcastRPCAddress: 本节点 gRPC 地址，格式 "ip:rpcPort"
//   - replicaPoints: 一致性哈希虚拟节点数，建议 100~300
func NewMonitor(
	serviceName string,
	services map[string]int,
	rp *ringpop.Ringpop,
	store MemberStore,
	broadcastHostPort string,
	broadcastRPCAddress string,
	maxJoinDuration time.Duration,
	propagationTime time.Duration,
	joinTime time.Time,
	replicaPoints int,
) *Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	hostID, _ := uuid.New().MarshalBinary()

	m := &Monitor{
		lifecycleCtx:        ctx,
		lifecycleCancel:     cancel,
		serviceName:         serviceName,
		services:            services,
		rp:                  rp,
		store:               store,
		maxJoinDuration:     maxJoinDuration,
		propagationTime:     propagationTime,
		joinTime:            joinTime,
		rings:               make(map[string]*serviceResolver),
		replicaPoints:       replicaPoints,
		hostID:              hostID,
		broadcastHostPort:   broadcastHostPort,
		broadcastRPCAddress: broadcastRPCAddress,
		initialized:         make(chan struct{}),
	}

	for svc, port := range services {
		m.rings[svc] = newServiceResolver(svc, port, rp, replicaPoints)
	}
	return m
}

// Start 启动 Monitor：写心跳 → Bootstrap Gossip → 设置标签 → 启动各 ServiceResolver
func (m *Monitor) Start() {
	m.mu.Lock()
	if m.status != 0 {
		m.mu.Unlock()
		return
	}
	m.status = 1
	m.mu.Unlock()

	// 1. 写入自己的心跳记录，让别人能发现自己（必须在 Bootstrap 前，否则别人找不到我们）
	if err := m.startHeartbeat(); err != nil {
		panic("unable to initialize membership heartbeats: " + err.Error())
	}

	// 2. 从 DB 获取种子节点，加入 Gossip 环
	if err := m.bootstrapRingPop(); err != nil {
		if strings.Contains(err.Error(), "destroyed while attempting to join") {
			return // Stop() 在 Start() 执行期间被调用，正常情况
		}
		panic("failed to start ringpop: " + err.Error())
	}

	// 3. 设置 Gossip 标签，让其他节点知道我们的角色和 gRPC 端口
	labels, err := m.rp.Labels()
	if err != nil {
		panic("unable to get ringpop labels: " + err.Error())
	}

	// 计划加入时间（滚动发布用，让新节点在指定时间后才对外可见）
	if until := time.Until(m.joinTime); until > 0 && until.Seconds() < maxScheduledEventTimeSeconds {
		_ = labels.Set(startAtKey, strconv.FormatInt(m.joinTime.Unix(), 10))
		time.AfterFunc(until+m.propagationTime, func() {
			if l, err := m.rp.Labels(); err == nil {
				_, _ = l.Remove(startAtKey)
			}
		})
	}

	// 广播 gRPC 端口（其他节点通过这个标签知道该用哪个端口连接我们）
	_, rpcPort, _ := net.SplitHostPort(m.broadcastRPCAddress)
	_ = labels.Set(portKey, rpcPort)

	// 广播服务角色（ServiceResolver 通过这个标签过滤同类节点）
	// 注意：roleKey 必须最后设置，它是 ServiceResolver 过滤成员的条件
	_ = labels.Set(roleKey, m.serviceName)

	// 4. 启动各 ServiceResolver（开始监听 Gossip 事件，刷新哈希环）
	m.mu.Lock()
	for _, ring := range m.rings {
		ring.Start()
	}
	m.mu.Unlock()

	close(m.initialized)
}

// Stop 优雅关闭：停止心跳、离开 Gossip 环、停止各 ServiceResolver
func (m *Monitor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status != 1 {
		return
	}
	m.status = 2
	m.lifecycleCancel()
	for _, ring := range m.rings {
		ring.Stop()
	}
	m.rp.Destroy()
}

// WaitUntilInitialized 阻塞直到 Start() 完成
func (m *Monitor) WaitUntilInitialized(ctx context.Context) error {
	select {
	case <-m.initialized:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EvictSelf 主动离开 Gossip 环（用于优雅下线）
func (m *Monitor) EvictSelf() error {
	return m.rp.SelfEvict()
}

// EvictSelfAt 在指定时间离开（用于滚动发布，提前把流量切走）
// 调用方应等待返回值时长后再退出进程，确保其他节点已感知到变化
func (m *Monitor) EvictSelfAt(asOf time.Time) (time.Duration, error) {
	until := time.Until(asOf)
	if until <= 0 || until.Seconds() >= maxScheduledEventTimeSeconds {
		return 0, m.rp.SelfEvict()
	}
	labels, err := m.rp.Labels()
	if err != nil {
		return 0, err
	}
	if err = labels.Set(stopAtKey, strconv.FormatInt(asOf.Unix(), 10)); err != nil {
		return 0, err
	}
	leaveAfter := until + m.propagationTime
	time.AfterFunc(leaveAfter, func() { _ = m.rp.SelfEvict() })
	return leaveAfter + m.propagationTime, nil
}

// GetResolver 获取某个服务角色的 ServiceResolver（哈希环）
func (m *Monitor) GetResolver(service string) (ServiceResolver, error) {
	ring, found := m.rings[service]
	if !found {
		return nil, ErrUnknownService
	}
	return ring, nil
}

// GetReachableMembers 返回当前所有 Gossip 可达节点的地址
func (m *Monitor) GetReachableMembers() ([]string, error) {
	return m.rp.GetReachableMembers()
}

// SetDraining 设置 draining 状态，通过 Gossip 传播给所有节点
// draining=true 时该节点不会被 AvailableMembers() 返回
func (m *Monitor) SetDraining(draining bool) error {
	labels, err := m.rp.Labels()
	if err != nil {
		return err
	}
	return labels.Set(drainingKey, strconv.FormatBool(draining))
}

func (m *Monitor) ApproximateMaxPropagationTime() time.Duration {
	return m.propagationTime
}

// ---- 内部方法 ----

// bootstrapRingPop 从 DB 获取种子节点，发起 Bootstrap 加入 Gossip 环
func (m *Monitor) bootstrapRingPop() error {
	var lastErr error
	for i := 0; i < maxBootstrapRetries; i++ {
		hostPorts, err := m.fetchCurrentBootstrapHostports()
		if err != nil {
			lastErr = err
			time.Sleep(healthyHostLastHeartbeatCutoff / 2)
			continue
		}

		bootParams := &swim.BootstrapOptions{
			ParallelismFactor: 10,
			JoinSize:          1,            // 只需要 1 个节点响应就算成功
			MaxJoinDuration:   m.maxJoinDuration,
			DiscoverProvider:  statichosts.New(hostPorts...), // 用 DB 读取的地址作为种子
		}

		_, err = m.rp.Bootstrap(bootParams)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(healthyHostLastHeartbeatCutoff / 2)
	}
	return fmt.Errorf("exhausted all retries: %w", lastErr)
}

// fetchCurrentBootstrapHostports 从 DB 读取最近 20 秒内有心跳的所有节点地址
// 注意：这里读取的是 gossip 端口（membershipPort），不是 gRPC 端口
func (m *Monitor) fetchCurrentBootstrapHostports() ([]string, error) {
	members, err := m.store.GetActiveMembers(m.lifecycleCtx, healthyHostLastHeartbeatCutoff)
	if err != nil {
		return nil, err
	}

	set := make(map[string]struct{}, len(members))
	for _, member := range members {
		addr := net.JoinHostPort(member.RPCAddress.String(), strconv.Itoa(int(member.MembershipPort)))
		set[addr] = struct{}{}
	}

	result := make([]string, 0, len(set))
	for addr := range set {
		result = append(result, addr)
	}
	return result, nil
}

// startHeartbeat 初始化心跳：清理过期记录、写入自己、启动心跳循环
func (m *Monitor) startHeartbeat() error {
	// 清理过期记录，防止 DB 无限增长
	_ = m.store.PruneExpiredMembers(m.lifecycleCtx, 10)

	host, portStr, err := net.SplitHostPort(m.broadcastRPCAddress)
	if err != nil {
		return err
	}
	rpcPort, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return err
	}
	_, membershipPortStr, err := net.SplitHostPort(m.broadcastHostPort)
	if err != nil {
		return err
	}
	membershipPort, err := strconv.ParseUint(membershipPortStr, 10, 16)
	if err != nil {
		return err
	}

	req := UpsertMemberRequest{
		ServiceName:    m.serviceName,
		RPCAddress:     net.ParseIP(host),
		RPCPort:        uint16(rpcPort),
		MembershipPort: uint16(membershipPort),
		HostID:         m.hostID,
		SessionStart:   time.Now().UTC(),
		RecordExpiry:   upsertMembershipRecordExpiryDefault,
	}

	// 先写一次，让自己对外可见，然后再 Bootstrap（Temporal 的注释指出这里有一个小的竞态）
	if err = m.store.UpsertMember(m.lifecycleCtx, req); err != nil {
		return err
	}

	// 启动心跳循环（每 10~15 秒写一次，仅供新节点 Bootstrap 时发现种子用）
	go m.heartbeatLoop(req)
	return nil
}

func (m *Monitor) heartbeatLoop(req UpsertMemberRequest) {
	for {
		select {
		case <-m.lifecycleCtx.Done():
			return
		default:
		}
		_ = m.store.UpsertMember(m.lifecycleCtx, req)
		// 10秒基础 + 0~5秒随机抖动，避免所有节点同时写 DB
		jitter := time.Duration(math.Round(rand.Float64() * 5))
		time.Sleep((10 + jitter) * time.Second)
	}
}

// NewRingpop 创建一个 ringpop 实例（对 tchannel + ringpop 初始化的封装）
// membershipHostPort: gossip 监听地址，格式 "ip:port"（必须是具体 IP，不能是 0.0.0.0）
func NewRingpop(appName string, membershipHostPort string) (*ringpop.Ringpop, error) {
	ringpopServiceName := appName + "-ringpop"

	listener, err := net.Listen("tcp", membershipHostPort)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", membershipHostPort, err)
	}

	ch, err := tchannel.NewChannel(ringpopServiceName, &tchannel.ChannelOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create tchannel: %w", err)
	}

	if err = ch.Serve(listener); err != nil {
		return nil, fmt.Errorf("failed to serve tchannel: %w", err)
	}

	rp, err := ringpop.New(appName, ringpop.Channel(ch))
	if err != nil {
		return nil, fmt.Errorf("failed to create ringpop: %w", err)
	}

	return rp, nil
}
