package ringpop

import (
	"sync"
	"sync/atomic"
)

// serviceResolver 实现 ServiceResolver 接口。
// 成员视图由外部（Monitor）通过 setMembers 注入，不再订阅 gossip 事件。
type serviceResolver struct {
	service       string
	replicaPoints int

	shutdownCh chan struct{}
	shutdownWG sync.WaitGroup

	// ringAndHosts 原子存储，读操作完全无锁
	ringAndHosts atomic.Value

	listenerLock sync.RWMutex
	listeners    map[string]chan<- *ChangedEvent
}

type ringAndHosts struct {
	ring  *hashRing
	hosts map[string]*hostInfo
}

var _ ServiceResolver = (*serviceResolver)(nil)

func newServiceResolver(service string, replicaPoints int) *serviceResolver {
	r := &serviceResolver{
		service:       service,
		replicaPoints: replicaPoints,
		shutdownCh:    make(chan struct{}),
		listeners:     make(map[string]chan<- *ChangedEvent),
	}
	r.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(replicaPoints),
		hosts: make(map[string]*hostInfo),
	})
	return r
}

func (r *serviceResolver) Start() {
	// DB-based resolver 无需后台 goroutine：由 Monitor 驱动 setMembers
}

func (r *serviceResolver) Stop() {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	select {
	case <-r.shutdownCh:
		return // already stopped
	default:
	}
	r.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(r.replicaPoints),
		hosts: nil,
	})
	r.listeners = make(map[string]chan<- *ChangedEvent)
	close(r.shutdownCh)
	r.shutdownWG.Wait()
}

// setMembers 由 Monitor 调用，注入最新成员列表，自动对比差异并发送 ChangedEvent。
func (r *serviceResolver) setMembers(hosts []*hostInfo) {
	newMap, event := r.compareMembers(hosts)
	if event == nil {
		return
	}

	ring := newHashRing(r.replicaPoints)
	for addr := range newMap {
		ring.AddMember(addr)
	}
	r.ringAndHosts.Store(ringAndHosts{ring: ring, hosts: newMap})
	r.emitEvent(event)
}

func (r *serviceResolver) RequestRefresh() {
	// no-op：DB 轮询由 Monitor 驱动
}

func (r *serviceResolver) Lookup(key string) (HostInfo, error) {
	ring, hosts := r.ring()
	addr, found := ring.Lookup(key)
	if !found {
		return nil, ErrInsufficientHosts
	}
	return hosts[addr], nil
}

func (r *serviceResolver) LookupN(key string, n int) []HostInfo {
	if n <= 0 {
		return nil
	}
	ring, hosts := r.ring()
	addrs := ring.LookupN(key, n)
	if len(addrs) == 0 {
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
	for _, h := range hosts {
		if !isDraining(h) {
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
	if !changed {
		return newMap, nil
	}
	return newMap, event
}

func (r *serviceResolver) emitEvent(event *ChangedEvent) {
	r.listenerLock.RLock()
	defer r.listenerLock.RUnlock()
	for _, ch := range r.listeners {
		select {
		case ch <- event:
		default:
		}
	}
}

func (r *serviceResolver) ring() (*hashRing, map[string]*hostInfo) {
	rh := r.ringAndHosts.Load().(ringAndHosts)
	return rh.ring, rh.hosts
}
