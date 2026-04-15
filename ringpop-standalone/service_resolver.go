package ringpop

// service_resolver.go
//
// ServiceResolver 是某个"服务角色"的一致性哈希环实现。
//
// 核心职责：
//   1. 订阅 ringpop 的 Gossip 事件（HandleEvent）
//   2. 从 ringpop 获取当前所有可达节点，按服务角色过滤
//   3. 将过滤后的节点放入本地 HashRing
//   4. 对外提供 Lookup(key) → 节点地址 的一致性哈希查找
//
// 重要设计：
//   - 每个"服务类型"（如 matching、history）各有一个 ServiceResolver
//   - 节点通过 Gossip 标签 "serviceName=matching" 声明自己的角色
//   - HashRing 存储在 atomic.Value 中，读写无锁（写时全量替换）

import (
	"errors"
	"math"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/ringpop-go/events"
	"github.com/temporalio/ringpop-go/hashring"
	rpmembership "github.com/temporalio/ringpop-go/membership"
	"github.com/temporalio/ringpop-go/swim"
)

const (
	// roleKey: Gossip 标签 key，值为服务名称（如 "matching"）
	// 每个节点启动后通过 rp.Labels().Set(roleKey, serviceName) 广播自己的角色
	roleKey = "serviceName"

	// portKey: Gossip 标签 key，值为该节点 gRPC 监听端口
	// ringpop 自身的监听端口（membership port）与 gRPC 端口可以不同，通过这个标签告知其他节点
	portKey = "servicePort"

	// drainingKey: 节点是否处于 draining 状态（即将下线，不接受新请求）
	drainingKey = "draining"

	// startAtKey / stopAtKey: 计划加入/离开时间（unix 秒）
	// 用于滚动发布：让节点在指定时间后才对外可见，或在指定时间前就从环中消失
	startAtKey = "startAt"
	stopAtKey  = "stopAt"

	// minRefreshInternal: 两次 lazy refresh 之间的最小间隔，防止刷新风暴
	minRefreshInternal = time.Second * 4
	// defaultRefreshInterval: 定时全量刷新间隔（兜底，防止错过事件）
	defaultRefreshInterval = time.Second * 10
)

type refreshMode int

const (
	refreshModeAlways refreshMode = iota // 立即刷新
	refreshModeLazy                      // 距上次刷新超过 minRefreshInternal 才刷新
)

// serviceResolver 实现了 ServiceResolver 接口
type serviceResolver struct {
	service       string         // 服务名称，用于过滤 ringpop 成员
	port          int            // 本服务默认 gRPC 端口（成员没有 portKey 标签时的兜底值）
	rp            *ringpop.Ringpop
	replicaPoints int            // 一致性哈希的虚拟节点数量，越大分布越均匀，建议 100~300

	refreshChan chan struct{}     // 触发刷新的信号通道
	shutdownCh  chan struct{}     // 关闭信号
	shutdownWG  sync.WaitGroup

	// ringAndHosts 原子存储，读操作完全无锁
	// 写时构造新的 ringAndHosts 对象整体替换，保证读写一致性
	ringAndHosts atomic.Value

	refreshLock         sync.Mutex
	lastRefreshTime     time.Time
	scheduledRefreshMap map[int64]*time.Timer // 用于处理 startAt/stopAt 的定时刷新

	listenerLock sync.RWMutex
	listeners    map[string]chan<- *ChangedEvent
}

type ringAndHosts struct {
	ring  *hashring.HashRing
	hosts map[string]*hostInfo // address → hostInfo，HashRing 只返回 address，需要这个 map 反查
}

var _ ServiceResolver = (*serviceResolver)(nil)

var errMissingLabel = errors.New("missing label")

func newServiceResolver(service string, port int, rp *ringpop.Ringpop, replicaPoints int) *serviceResolver {
	r := &serviceResolver{
		service:             service,
		port:                port,
		rp:                  rp,
		replicaPoints:       replicaPoints,
		refreshChan:         make(chan struct{}),
		shutdownCh:          make(chan struct{}),
		scheduledRefreshMap: make(map[int64]*time.Timer),
		listeners:           make(map[string]chan<- *ChangedEvent),
	}
	r.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(replicaPoints),
		hosts: make(map[string]*hostInfo),
	})
	return r
}

func newHashRing(replicaPoints int) *hashring.HashRing {
	// farm.Fingerprint32 是 Google FarmHash 的 32 位版本，速度快、分布均匀
	return hashring.New(farm.Fingerprint32, replicaPoints)
}

// Start 订阅 ringpop 事件，立即做一次全量刷新，并启动后台定时刷新 goroutine
func (r *serviceResolver) Start() {
	r.rp.AddListener(r)
	if err := r.refresh(refreshModeAlways); err != nil {
		panic("unable to start service resolver: " + err.Error())
	}
	r.shutdownWG.Add(1)
	go r.refreshRingWorker()
}

// Stop 取消订阅，清空哈希环，关闭后台 goroutine
func (r *serviceResolver) Stop() {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	r.rp.RemoveListener(r)
	r.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(r.replicaPoints),
		hosts: nil,
	})
	r.listeners = make(map[string]chan<- *ChangedEvent)
	close(r.shutdownCh)
	r.shutdownWG.Wait()
}

func (r *serviceResolver) RequestRefresh() {
	select {
	case r.refreshChan <- struct{}{}:
	default:
	}
}

// Lookup 一致性哈希查找：给定任意字符串 key，返回负责该 key 的节点
// 同一个 key 在节点不变时始终返回同一个节点（确定性）
// 节点加入/离开时，只有部分 key 的映射会改变（一致性哈希的核心特性）
func (r *serviceResolver) Lookup(key string) (HostInfo, error) {
	ring, hosts := r.ring()
	addr, found := ring.Lookup(key)
	if !found {
		r.RequestRefresh()
		return nil, ErrInsufficientHosts
	}
	return hosts[addr], nil
}

// LookupN 返回 n 个节点（按哈希顺序），用于副本分布或 spreading
func (r *serviceResolver) LookupN(key string, n int) []HostInfo {
	if n <= 0 {
		return nil
	}
	ring, hosts := r.ring()
	addrs := ring.LookupN(key, n)
	if len(addrs) == 0 {
		r.RequestRefresh()
		return nil
	}
	result := make([]HostInfo, len(addrs))
	for i, addr := range addrs {
		result[i] = hosts[addr]
	}
	return result
}

func (r *serviceResolver) AddListener(name string, ch chan<- *ChangedEvent) error {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	if _, ok := r.listeners[name]; ok {
		return ErrListenerAlreadyExist
	}
	r.listeners[name] = ch
	return nil
}

func (r *serviceResolver) RemoveListener(name string) error {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	delete(r.listeners, name)
	return nil
}

func (r *serviceResolver) MemberCount() int {
	_, hosts := r.ring()
	return len(hosts)
}

func (r *serviceResolver) AvailableMemberCount() int {
	_, hosts := r.ring()
	n := 0
	for _, host := range hosts {
		if !isDraining(host) {
			n++
		}
	}
	return n
}

func (r *serviceResolver) Members() []HostInfo {
	_, hosts := r.ring()
	result := make([]HostInfo, 0, len(hosts))
	for _, h := range hosts {
		result = append(result, h)
	}
	return result
}

func (r *serviceResolver) AvailableMembers() []HostInfo {
	_, hosts := r.ring()
	var result []HostInfo
	for _, h := range hosts {
		if !isDraining(h) {
			result = append(result, h)
		}
	}
	return result
}

// HandleEvent 实现 ringpop events.EventListener 接口
// ringpop 检测到成员变化时回调此方法（异步，可能乱序，所以我们不用事件内容，而是全量重新拉取）
func (r *serviceResolver) HandleEvent(event events.Event) {
	if _, ok := event.(rpmembership.ChangeEvent); ok {
		_ = r.refresh(refreshModeAlways)
	}
}

// ---- 内部刷新逻辑 ----

func (r *serviceResolver) refresh(mode refreshMode) error {
	var event *ChangedEvent
	defer func() {
		if event != nil {
			r.emitEvent(event)
		}
	}()

	r.refreshLock.Lock()
	defer r.refreshLock.Unlock()

	if mode == refreshModeLazy && r.lastRefreshTime.After(time.Now().UTC().Add(-minRefreshInternal)) {
		return nil
	}

	var err error
	event, err = r.refreshLocked()
	return err
}

func (r *serviceResolver) refreshLocked() (*ChangedEvent, error) {
	hosts, nextEvent, err := r.getReachableMembers()
	if err != nil {
		return nil, err
	}

	r.scheduleRefresh(nextEvent)

	newMembersMap, changedEvent := r.compareMembers(hosts)
	if changedEvent == nil {
		return nil, nil
	}

	// 构造新的 HashRing，整体原子替换
	ring := newHashRing(r.replicaPoints)
	members := make([]rpmembership.Member, len(hosts))
	for i, h := range hosts {
		members[i] = h
	}
	ring.AddMembers(members...)

	r.lastRefreshTime = time.Now().UTC()
	r.ringAndHosts.Store(ringAndHosts{
		ring:  ring,
		hosts: newMembersMap,
	})

	return changedEvent, nil
}

// getReachableMembers 从 ringpop 获取当前所有可达成员，按角色过滤，处理 startAt/stopAt
func (r *serviceResolver) getReachableMembers() ([]*hostInfo, int64, error) {
	// 只获取 serviceName=r.service 的节点
	members, err := r.rp.GetReachableMemberObjects(swim.MemberWithLabelAndValue(roleKey, r.service))
	if err != nil {
		return nil, 0, err
	}

	nowUnix := time.Now().Unix()
	nextEvent := int64(math.MaxInt64)

	// 过滤 startAt：startAt 在未来的节点还不应该出现在环中
	members = slices.DeleteFunc(members, func(m swim.Member) bool {
		startAt, err := parseIntLabel(m, startAtKey)
		if err != nil {
			return false // 没有标签，不过滤
		}
		if startAt <= nowUnix {
			return false // 已经到加入时间了
		}
		nextEvent = min(nextEvent, startAt)
		return true // 还没到时间，过滤掉
	})

	// 过滤 stopAt：stopAt 已过的节点应该从环中移除
	members = slices.DeleteFunc(members, func(m swim.Member) bool {
		stopAt, err := parseIntLabel(m, stopAtKey)
		if err != nil {
			return false
		}
		if stopAt > nowUnix {
			nextEvent = min(nextEvent, stopAt)
			return false // 还没到离开时间
		}
		return true // 已到离开时间，过滤掉
	})

	hosts := make([]*hostInfo, len(members))
	for i, member := range members {
		// gRPC 服务端口通过 portKey 标签传播（ringpop 自身端口可能不同）
		servicePort := r.port
		if portLabel, ok := member.Label(portKey); ok {
			servicePort, err = strconv.Atoi(portLabel)
			if err != nil {
				return nil, 0, err
			}
		}
		hostPort, err := replaceServicePort(member.Address, servicePort)
		if err != nil {
			return nil, 0, err
		}
		hosts[i] = newHostInfo(hostPort, member.Labels)
	}

	if nextEvent == math.MaxInt64 {
		nextEvent = 0
	}
	return hosts, nextEvent, nil
}

// compareMembers 对比新旧成员列表，生成变化事件
func (r *serviceResolver) compareMembers(hosts []*hostInfo) (map[string]*hostInfo, *ChangedEvent) {
	event := &ChangedEvent{}
	changed := false
	_, prevHosts := r.ring()
	newMap := make(map[string]*hostInfo, len(hosts))
	for _, host := range hosts {
		newMap[host.GetAddress()] = host
		if prev, ok := prevHosts[host.GetAddress()]; !ok {
			event.HostsAdded = append(event.HostsAdded, host)
			changed = true
		} else if prev.labelsChecksum != host.labelsChecksum {
			event.HostsChanged = append(event.HostsChanged, host)
			changed = true
		}
	}
	for addr, prev := range prevHosts {
		if _, ok := newMap[addr]; !ok {
			event.HostsRemoved = append(event.HostsRemoved, prev)
			changed = true
		}
	}
	if changed {
		return newMap, event
	}
	return newMap, nil
}

func (r *serviceResolver) scheduleRefresh(nextEvent int64) {
	if nextEvent == 0 {
		return
	}
	if _, ok := r.scheduledRefreshMap[nextEvent]; ok {
		return
	}
	nextEventTime := time.Unix(nextEvent, 0)
	r.scheduledRefreshMap[nextEvent] = time.AfterFunc(time.Until(nextEventTime), func() {
		_ = r.refresh(refreshModeAlways)
		r.refreshLock.Lock()
		defer r.refreshLock.Unlock()
		delete(r.scheduledRefreshMap, nextEvent)
	})
}

func (r *serviceResolver) emitEvent(event *ChangedEvent) {
	r.listenerLock.RLock()
	defer r.listenerLock.RUnlock()
	for _, ch := range r.listeners {
		select {
		case ch <- event:
		default:
			// channel 满了，丢弃（调用方应该保证 channel 有足够缓冲）
		}
	}
}

func (r *serviceResolver) refreshRingWorker() {
	defer r.shutdownWG.Done()
	ticker := time.NewTicker(defaultRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.shutdownCh:
			return
		case <-r.refreshChan:
			_ = r.refresh(refreshModeLazy)
		case <-ticker.C:
			_ = r.refresh(refreshModeLazy)
		}
	}
}

func (r *serviceResolver) ring() (*hashring.HashRing, map[string]*hostInfo) {
	rh := r.ringAndHosts.Load().(ringAndHosts)
	return rh.ring, rh.hosts
}

// ---- 工具函数 ----

func replaceServicePort(address string, servicePort int) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", ErrIncorrectAddressFormat
	}
	return net.JoinHostPort(host, strconv.Itoa(servicePort)), nil
}

func parseIntLabel(member swim.Member, label string) (int64, error) {
	str, ok := member.Label(label)
	if !ok {
		return 0, errMissingLabel
	}
	return strconv.ParseInt(str, 10, 64)
}

func isDraining(host *hostInfo) bool {
	if v, ok := host.Label(drainingKey); ok {
		if draining, err := strconv.ParseBool(v); err == nil {
			return draining
		}
	}
	return false
}
