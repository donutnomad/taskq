package membership

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	upsertMembershipRecordExpiryDefault = time.Hour * 48
	healthyHostLastHeartbeatCutoff      = time.Second * 20
	defaultMembershipRetryMaxAttempts   = 5
	defaultMembershipRetryBaseDelay     = 500 * time.Millisecond
	defaultMembershipRetryMaxDelay      = 8 * time.Second
	defaultMembershipHeartbeatInterval  = 10 * time.Second
	defaultMembershipHeartbeatJitterMax = 5 * time.Second
)

var (
	membershipRetryMaxAttempts   = defaultMembershipRetryMaxAttempts
	membershipRetryBaseDelay     = defaultMembershipRetryBaseDelay
	membershipRetryMaxDelay      = defaultMembershipRetryMaxDelay
	membershipHeartbeatInterval  = defaultMembershipHeartbeatInterval
	membershipHeartbeatJitterMax = defaultMembershipHeartbeatJitterMax
)

// monitor 实现 Monitor 接口，基于 DB 轮询的成员管理。
type monitor struct {
	mu     sync.Mutex
	status int32 // 0=init, 1=started, 2=stopped
	wg     sync.WaitGroup

	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	advertisedServices []string
	store              MemberStore

	pollInterval    time.Duration
	propagationTime time.Duration
	joinTime        time.Time

	rings         map[string]*serviceResolver
	replicaPoints int

	hostID        []byte
	broadcastAddr netip.AddrPort
	draining      bool
	stopAt        int64

	initialized chan struct{}
	shutdownErr error
}

var _ Monitor = (*monitor)(nil)

// MonitorConfig separates the services this process publishes from the
// services whose routing views it consumes.
type MonitorConfig struct {
	AdvertisedServices []string
	WatchedServices    []string
	Store              MemberStore
	BroadcastAddr      netip.AddrPort
	PollInterval       time.Duration
	PropagationTime    time.Duration
	JoinTime           time.Time
	ReplicaPoints      int
}

// NewMonitor preserves the legacy behavior where every advertised service is
// also watched by this process.
//
// 参数：
//   - serviceNames: 本节点承担的服务角色列表（如 []string{"matching"} 或 []string{"matching", "history"}）
//   - store: DB 操作接口
//   - broadcastAddr: 本节点 gRPC 广播地址
//   - pollInterval: DB 轮询间隔（建议 3~10s）
//   - propagationTime: 成员变更传播估计时间（用于 EvictSelfAt）
//   - joinTime: 计划加入时间（零值表示立即加入）
//   - replicaPoints: 一致性哈希虚拟节点数，建议 100~300
func NewMonitor(
	serviceNames []string,
	store MemberStore,
	broadcastAddr netip.AddrPort,
	pollInterval time.Duration,
	propagationTime time.Duration,
	joinTime time.Time,
	replicaPoints int,
) Monitor {
	return NewMonitorWithConfig(MonitorConfig{
		AdvertisedServices: serviceNames,
		WatchedServices:    serviceNames,
		Store:              store,
		BroadcastAddr:      broadcastAddr,
		PollInterval:       pollInterval,
		PropagationTime:    propagationTime,
		JoinTime:           joinTime,
		ReplicaPoints:      replicaPoints,
	})
}

// NewMonitorWithConfig creates a monitor with independent publication and
// observation roles. Observer-only processes leave AdvertisedServices empty.
func NewMonitorWithConfig(cfg MonitorConfig) Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	hostID, _ := uuid.New().MarshalBinary()
	advertisedServices := append([]string(nil), cfg.AdvertisedServices...)
	watchedServices := append([]string(nil), cfg.WatchedServices...)

	m := &monitor{
		lifecycleCtx:       ctx,
		lifecycleCancel:    cancel,
		advertisedServices: advertisedServices,
		store:              cfg.Store,
		pollInterval:       cfg.PollInterval,
		propagationTime:    cfg.PropagationTime,
		joinTime:           cfg.JoinTime,
		rings:              make(map[string]*serviceResolver),
		replicaPoints:      cfg.ReplicaPoints,
		hostID:             hostID,
		broadcastAddr:      cfg.BroadcastAddr,
		initialized:        make(chan struct{}),
	}

	for _, svc := range watchedServices {
		m.rings[svc] = newServiceResolver(svc, cfg.ReplicaPoints)
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

	if pruner, ok := m.store.(ExpiredMemberPruner); ok {
		if err := pruner.PruneExpiredMembers(m.lifecycleCtx, 10); err != nil {
			slog.Warn("membership prune expired members failed", "err", err)
		}
	}

	if len(m.advertisedServices) > 0 {
		if err := m.retryMembershipOp("initialize membership heartbeats", m.writeHeartbeats); err != nil {
			m.fail(fmt.Errorf("unable to initialize membership heartbeats: %w", err))
			return
		}
	}

	if err := m.retryMembershipOp("sync members on startup", m.syncMembers); err != nil {
		m.fail(fmt.Errorf("failed to sync members on startup: %w", err))
		return
	}

	close(m.initialized)

	m.wg.Add(1)
	go m.pollLoop()
	if len(m.advertisedServices) > 0 {
		m.wg.Add(1)
		go m.heartbeatLoop()
	}
}

func (m *monitor) Stop() {
	m.stop(nil)
	m.wg.Wait()
}

func (m *monitor) stop(err error) {
	m.mu.Lock()
	if m.status != 1 {
		m.mu.Unlock()
		return
	}
	m.status = 2
	if err != nil && m.shutdownErr == nil {
		m.shutdownErr = err
	}
	m.lifecycleCancel()
	for _, ring := range m.rings {
		ring.Stop()
	}
	m.mu.Unlock()
}

func (m *monitor) fail(err error) {
	slog.Error("membership monitor stopped after retry exhaustion", "err", err)
	m.stop(err)
}

func (m *monitor) WaitUntilInitialized(ctx context.Context) error {
	select {
	case <-m.initialized:
		return nil
	case <-m.lifecycleCtx.Done():
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.shutdownErr != nil {
			return m.shutdownErr
		}
		return m.lifecycleCtx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *monitor) EvictSelf() error {
	return m.SetDraining(true)
}

func (m *monitor) EvictSelfAt(asOf time.Time) (time.Duration, error) {
	until := time.Until(asOf)
	if until <= 0 {
		if err := m.SetDraining(true); err != nil {
			return 0, err
		}
		return m.propagationTime, nil
	}
	m.mu.Lock()
	m.stopAt = asOf.Unix()
	m.mu.Unlock()
	if err := m.writeHeartbeats(); err != nil {
		return 0, err
	}
	return until + m.propagationTime, nil
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
	seen := make(map[string]struct{}, len(members))
	result := make([]string, 0, len(members))
	for _, rec := range members {
		addr := net.JoinHostPort(rec.RPCAddress.String(), strconv.Itoa(int(rec.RPCPort)))
		if _, dup := seen[addr]; !dup {
			seen[addr] = struct{}{}
			result = append(result, addr)
		}
	}
	return result, nil
}

func (m *monitor) SetDraining(draining bool) error {
	m.mu.Lock()
	m.draining = draining
	if !draining {
		m.stopAt = 0
	}
	m.mu.Unlock()

	return m.writeHeartbeats()
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
			if err := m.retryMembershipOp("sync members", m.syncMembers); err != nil {
				m.fail(fmt.Errorf("sync members failed after retries: %w", err))
				return
			}
		}
	}
}

func (m *monitor) syncMembers() error {
	records, err := m.store.GetActiveMembers(m.lifecycleCtx, healthyHostLastHeartbeatCutoff)
	if err != nil {
		return fmt.Errorf("syncMembers: %w", err)
	}

	nowUnix := time.Now().Unix()

	byService := make(map[string][]*hostInfo, len(m.rings))
	for svc := range m.rings {
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

func (m *monitor) writeHeartbeats() error {
	for _, svc := range m.advertisedServices {
		req := m.buildUpsertRequest(svc)
		if err := m.store.UpsertMember(m.lifecycleCtx, req); err != nil {
			return err
		}
	}
	return nil
}

func (m *monitor) buildUpsertRequest(serviceName string) UpsertMemberRequest {
	m.mu.Lock()
	draining := m.draining
	stopAt := m.stopAt
	m.mu.Unlock()

	var startAt int64
	if !m.joinTime.IsZero() && time.Until(m.joinTime) > 0 {
		startAt = m.joinTime.Unix()
	}

	return UpsertMemberRequest{
		ServiceName:  serviceName,
		RPCAddress:   m.broadcastAddr.Addr().AsSlice(),
		RPCPort:      m.broadcastAddr.Port(),
		HostID:       m.hostID,
		SessionStart: time.Now().UTC(),
		RecordExpiry: upsertMembershipRecordExpiryDefault,
		Draining:     draining,
		StartAt:      startAt,
		StopAt:       stopAt,
	}
}

func (m *monitor) heartbeatLoop() {
	defer m.wg.Done()
	for {
		jitter := time.Duration(0)
		if membershipHeartbeatJitterMax > 0 {
			jitter = time.Duration(rand.Int63n(int64(membershipHeartbeatJitterMax)))
		}
		timer := time.NewTimer(membershipHeartbeatInterval + jitter)
		select {
		case <-m.lifecycleCtx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := m.retryMembershipOp("write membership heartbeats", m.writeHeartbeats); err != nil {
			m.fail(fmt.Errorf("write membership heartbeats failed after retries: %w", err))
			return
		}
	}
}

func (m *monitor) retryMembershipOp(name string, op func() error) error {
	delay := membershipRetryBaseDelay
	if delay <= 0 {
		delay = time.Millisecond
	}
	maxDelay := membershipRetryMaxDelay
	if maxDelay < delay {
		maxDelay = delay
	}
	maxAttempts := membershipRetryMaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := op(); err == nil {
			return nil
		} else {
			if errors.Is(err, context.Canceled) && m.lifecycleCtx.Err() != nil {
				return m.lifecycleCtx.Err()
			}
			lastErr = err
		}

		if attempt == maxAttempts {
			break
		}

		slog.Warn(
			"membership operation failed, retrying",
			"operation", name,
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"retry_in", delay,
			"err", lastErr,
		)

		timer := time.NewTimer(delay)
		select {
		case <-m.lifecycleCtx.Done():
			timer.Stop()
			return m.lifecycleCtx.Err()
		case <-timer.C:
		}

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return fmt.Errorf("%s: %w", name, lastErr)
}
