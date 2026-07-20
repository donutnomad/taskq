package membership

import (
	"sort"
	"sync"
	"sync/atomic"
)

// serviceResolver 实现 ServiceResolver 接口。
// 成员视图由外部（Monitor）通过 setMembers 注入，不再订阅 gossip 事件。
type serviceResolver struct {
	service       string
	replicaPoints int

	shutdownCh chan struct{}

	// ringAndHosts 原子存储，读操作完全无锁
	ringAndHosts atomic.Value

	listenerLock sync.RWMutex
	listeners    map[string]*resolverListener
}

type resolverListener struct {
	out      chan<- *ChangedEvent
	notify   chan struct{}
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once

	mu      sync.Mutex
	pending *ChangedEvent
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
		listeners:     make(map[string]*resolverListener),
	}
	r.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(replicaPoints),
		hosts: make(map[string]*hostInfo),
	})
	return r
}

func (r *serviceResolver) Stop() {
	r.listenerLock.Lock()
	select {
	case <-r.shutdownCh:
		r.listenerLock.Unlock()
		return // already stopped
	default:
	}
	r.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(r.replicaPoints),
		hosts: nil,
	})
	listeners := r.listeners
	r.listeners = make(map[string]*resolverListener)
	close(r.shutdownCh)
	r.listenerLock.Unlock()

	for _, listener := range listeners {
		listener.stop()
	}
}

// setMembers 由 Monitor 调用，注入最新成员列表，自动对比差异并发送 ChangedEvent。
func (r *serviceResolver) setMembers(hosts []*hostInfo) {
	newMap, event := r.compareMembers(hosts)
	if event == nil {
		return
	}

	ring := newHashRing(r.replicaPoints)
	for addr := range newMap {
		if host := newMap[addr]; host != nil && !host.draining {
			ring.AddMember(addr)
		}
	}
	r.ringAndHosts.Store(ringAndHosts{ring: ring, hosts: newMap})
	r.emitEvent(event)
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
	select {
	case <-r.shutdownCh:
		return ErrResolverStopped
	default:
	}
	if _, ok := r.listeners[name]; ok {
		return ErrListenerAlreadyExist
	}
	listener := newResolverListener(ch)
	r.listeners[name] = listener
	go listener.run()
	return nil
}

func (r *serviceResolver) RemoveListener(name string) error {
	r.listenerLock.Lock()
	listener := r.listeners[name]
	if listener != nil {
		delete(r.listeners, name)
	}
	r.listenerLock.Unlock()
	if listener != nil {
		listener.stop()
	}
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
		if !h.draining {
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
		if !h.draining {
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
	listeners := make([]*resolverListener, 0, len(r.listeners))
	for _, listener := range r.listeners {
		listeners = append(listeners, listener)
	}
	r.listenerLock.RUnlock()

	for _, listener := range listeners {
		listener.publish(event)
	}
}

func newResolverListener(out chan<- *ChangedEvent) *resolverListener {
	return &resolverListener{
		out:    out,
		notify: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func (l *resolverListener) publish(event *ChangedEvent) {
	l.mu.Lock()
	l.pending = mergeChangedEvents(l.pending, event)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

func (l *resolverListener) run() {
	defer close(l.doneCh)
	for {
		select {
		case <-l.stopCh:
			return
		case <-l.notify:
			l.mu.Lock()
			event := l.pending
			l.pending = nil
			l.mu.Unlock()
			if event == nil {
				continue
			}
			select {
			case l.out <- event:
			case <-l.stopCh:
				return
			}
		}
	}
}

func (l *resolverListener) stop() {
	l.stopOnce.Do(func() { close(l.stopCh) })
	<-l.doneCh
}

func mergeChangedEvents(current, next *ChangedEvent) *ChangedEvent {
	changes := make(map[string]pendingHostChange)
	mergeEventChanges(changes, current)
	mergeEventChanges(changes, next)

	addresses := make([]string, 0, len(changes))
	for address := range changes {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)

	merged := &ChangedEvent{}
	for _, address := range addresses {
		change := changes[address]
		switch change.kind {
		case hostAdded:
			merged.HostsAdded = append(merged.HostsAdded, change.host)
		case hostRemoved:
			merged.HostsRemoved = append(merged.HostsRemoved, change.host)
		case hostChanged:
			merged.HostsChanged = append(merged.HostsChanged, change.host)
		}
	}
	return merged
}

type hostChangeKind uint8

const (
	hostAdded hostChangeKind = iota + 1
	hostRemoved
	hostChanged
)

type pendingHostChange struct {
	kind hostChangeKind
	host HostInfo
}

func mergeEventChanges(changes map[string]pendingHostChange, event *ChangedEvent) {
	if event == nil {
		return
	}
	for _, host := range event.HostsAdded {
		mergeHostChange(changes, host, hostAdded)
	}
	for _, host := range event.HostsRemoved {
		mergeHostChange(changes, host, hostRemoved)
	}
	for _, host := range event.HostsChanged {
		mergeHostChange(changes, host, hostChanged)
	}
}

func mergeHostChange(changes map[string]pendingHostChange, host HostInfo, next hostChangeKind) {
	address := host.GetAddress()
	current, ok := changes[address]
	if !ok {
		changes[address] = pendingHostChange{kind: next, host: host}
		return
	}

	switch current.kind {
	case hostAdded:
		if next == hostRemoved {
			delete(changes, address)
			return
		}
		changes[address] = pendingHostChange{kind: hostAdded, host: host}
	case hostRemoved:
		if next == hostRemoved {
			changes[address] = pendingHostChange{kind: hostRemoved, host: host}
			return
		}
		changes[address] = pendingHostChange{kind: hostChanged, host: host}
	case hostChanged:
		if next == hostRemoved {
			changes[address] = pendingHostChange{kind: hostRemoved, host: host}
			return
		}
		changes[address] = pendingHostChange{kind: hostChanged, host: host}
	}
}

func (r *serviceResolver) ring() (*hashRing, map[string]*hostInfo) {
	rh := r.ringAndHosts.Load().(ringAndHosts)
	return rh.ring, rh.hosts
}
