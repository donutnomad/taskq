package ringpop

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	upsertMembershipRecordExpiryDefault = time.Hour * 48
	healthyHostLastHeartbeatCutoff      = time.Second * 20
	maxScheduledEventTimeSeconds        = 15
)

// monitor 实现 Monitor 接口，基于 DB 轮询的成员管理。
type monitor struct {
	mu     sync.Mutex
	status int32 // 0=init, 1=started, 2=stopped
	wg     sync.WaitGroup

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	serviceName string
	services    map[string]int // serviceName → gRPC port
	store       MemberStore

	pollInterval    time.Duration
	propagationTime time.Duration
	joinTime        time.Time

	rings         map[string]*serviceResolver
	replicaPoints int

	hostID              []byte
	broadcastRPCAddress string // "ip:port"
	draining            bool

	initialized chan struct{}
}

var _ Monitor = (*monitor)(nil)

// NewMonitor 创建一个 Monitor 实例。
//
// 参数：
//   - serviceName: 本节点的服务角色名（如 "matching"）
//   - services: 集群中所有服务角色及其 gRPC 端口
//   - store: DB 操作接口
//   - broadcastRPCAddress: 本节点 gRPC 地址，格式 "ip:rpcPort"
//   - pollInterval: DB 轮询间隔（建议 3~10s）
//   - propagationTime: 成员变更传播估计时间（用于 EvictSelfAt）
//   - joinTime: 计划加入时间（零值表示立即加入）
//   - replicaPoints: 一致性哈希虚拟节点数，建议 100~300
func NewMonitor(
	serviceName string,
	services map[string]int,
	store MemberStore,
	broadcastRPCAddress string,
	pollInterval time.Duration,
	propagationTime time.Duration,
	joinTime time.Time,
	replicaPoints int,
) Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	hostID, _ := uuid.New().MarshalBinary()

	m := &monitor{
		lifecycleCtx:        ctx,
		lifecycleCancel:     cancel,
		serviceName:         serviceName,
		services:            services,
		store:               store,
		pollInterval:        pollInterval,
		propagationTime:     propagationTime,
		joinTime:            joinTime,
		rings:               make(map[string]*serviceResolver),
		replicaPoints:       replicaPoints,
		hostID:              hostID,
		broadcastRPCAddress: broadcastRPCAddress,
		initialized:         make(chan struct{}),
	}

	for svc := range services {
		m.rings[svc] = newServiceResolver(svc, replicaPoints)
	}
	return m
}

// Start 启动 Monitor：写心跳 → 初次拉取成员 → 启动各 ServiceResolver → 启动轮询循环
func (m *monitor) Start() {
	m.mu.Lock()
	if m.status != 0 {
		m.mu.Unlock()
		return
	}
	m.status = 1
	m.mu.Unlock()

	if err := m.startHeartbeat(); err != nil {
		panic("unable to initialize membership heartbeats: " + err.Error())
	}

	if err := m.syncMembers(); err != nil {
		panic("failed to sync members on startup: " + err.Error())
	}

	m.mu.Lock()
	for _, ring := range m.rings {
		ring.Start()
	}
	m.mu.Unlock()

	close(m.initialized)

	m.wg.Add(1)
	go m.pollLoop()
}

func (m *monitor) Stop() {
	m.mu.Lock()
	if m.status != 1 {
		m.mu.Unlock()
		return
	}
	m.status = 2
	m.lifecycleCancel()
	for _, ring := range m.rings {
		ring.Stop()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *monitor) WaitUntilInitialized(ctx context.Context) error {
	select {
	case <-m.initialized:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *monitor) EvictSelf() error {
	return m.SetDraining(true)
}

func (m *monitor) EvictSelfAt(asOf time.Time) (time.Duration, error) {
	until := time.Until(asOf)
	if until <= 0 || until.Seconds() >= maxScheduledEventTimeSeconds {
		return 0, m.SetDraining(true)
	}
	leaveAfter := until + m.propagationTime
	time.AfterFunc(until, func() { _ = m.SetDraining(true) })
	return leaveAfter, nil
}

func (m *monitor) GetResolver(service string) (ServiceResolver, error) {
	ring, found := m.rings[service]
	if !found {
		return nil, ErrUnknownService
	}
	return ring, nil
}

func (m *monitor) GetReachableMembers() ([]string, error) {
	members, err := m.store.GetActiveMembers(m.lifecycleCtx, healthyHostLastHeartbeatCutoff)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(members))
	for _, rec := range members {
		result = append(result, net.JoinHostPort(rec.RPCAddress.String(), strconv.Itoa(int(rec.RPCPort))))
	}
	return result, nil
}

func (m *monitor) SetDraining(draining bool) error {
	m.mu.Lock()
	m.draining = draining
	m.mu.Unlock()

	host, portStr, err := net.SplitHostPort(m.broadcastRPCAddress)
	if err != nil {
		return err
	}
	rpcPort, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return err
	}
	req := m.buildUpsertRequest(host, uint16(rpcPort))
	return m.store.UpsertMember(m.lifecycleCtx, req)
}

func (m *monitor) ApproximateMaxPropagationTime() time.Duration {
	return m.propagationTime
}

// ---- 内部方法 ----

func (m *monitor) pollLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.lifecycleCtx.Done():
			return
		case <-ticker.C:
			_ = m.syncMembers()
		}
	}
}

func (m *monitor) syncMembers() error {
	records, err := m.store.GetActiveMembers(m.lifecycleCtx, healthyHostLastHeartbeatCutoff)
	if err != nil {
		return fmt.Errorf("syncMembers: %w", err)
	}

	nowUnix := time.Now().Unix()

	byService := make(map[string][]*hostInfo, len(m.services))
	for svc := range m.services {
		byService[svc] = nil
	}

	for _, rec := range records {
		if _, ok := byService[rec.ServiceName]; !ok {
			continue
		}
		if rec.StartAt > 0 && rec.StartAt > nowUnix {
			continue
		}
		if rec.StopAt > 0 && rec.StopAt <= nowUnix {
			continue
		}
		addr := net.JoinHostPort(rec.RPCAddress.String(), strconv.Itoa(int(rec.RPCPort)))
		host := newHostInfo(addr, rec.Draining, rec.StartAt, rec.StopAt)
		byService[rec.ServiceName] = append(byService[rec.ServiceName], host)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for svc, hosts := range byService {
		if ring, ok := m.rings[svc]; ok {
			ring.setMembers(hosts)
		}
	}
	return nil
}

func (m *monitor) startHeartbeat() error {
	_ = m.store.PruneExpiredMembers(m.lifecycleCtx, 10)

	host, portStr, err := net.SplitHostPort(m.broadcastRPCAddress)
	if err != nil {
		return err
	}
	rpcPort, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return err
	}

	req := m.buildUpsertRequest(host, uint16(rpcPort))

	if err = m.store.UpsertMember(m.lifecycleCtx, req); err != nil {
		return err
	}

	m.wg.Add(1)
	go m.heartbeatLoop(host, uint16(rpcPort))
	return nil
}

func (m *monitor) buildUpsertRequest(host string, rpcPort uint16) UpsertMemberRequest {
	m.mu.Lock()
	draining := m.draining
	m.mu.Unlock()

	var startAt int64
	if until := time.Until(m.joinTime); until > 0 && until.Seconds() < maxScheduledEventTimeSeconds {
		startAt = m.joinTime.Unix()
	}

	return UpsertMemberRequest{
		ServiceName:  m.serviceName,
		RPCAddress:   net.ParseIP(host),
		RPCPort:      rpcPort,
		HostID:       m.hostID,
		SessionStart: time.Now().UTC(),
		RecordExpiry: upsertMembershipRecordExpiryDefault,
		Draining:     draining,
		StartAt:      startAt,
	}
}

func (m *monitor) heartbeatLoop(host string, rpcPort uint16) {
	defer m.wg.Done()
	for {
		req := m.buildUpsertRequest(host, rpcPort)
		_ = m.store.UpsertMember(m.lifecycleCtx, req)
		jitter := time.Duration(rand.Int63n(5000)) * time.Millisecond
		select {
		case <-m.lifecycleCtx.Done():
			return
		case <-time.After(10*time.Second + jitter):
		}
	}
}
