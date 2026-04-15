# Ringpop → DB-Based Membership 迁移实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `ringpop-standalone/` 包从依赖 Ringpop（gossip/TChannel）迁移到纯 DB 轮询的成员管理，保留完全相同的对外接口（`Monitor`、`ServiceResolver`）。

**Architecture:** 删除所有 `ringpop-go` / `tchannel-go` 依赖，用一个后台轮询 goroutine 定期从 `MemberStore` 拉取活跃成员，维护本地一致性哈希环。成员视图变更仍通过 `ChangedEvent` 通知订阅者，语义不变。`startAt`/`stopAt`/`draining` 等字段从 DB 记录中读取，不再依赖 Gossip 标签传播。

**Tech Stack:** Go 1.25、`github.com/dgryski/go-farm`（hashring 哈希函数）、`github.com/google/uuid`（节点 ID）、自实现一致性哈希环（替代 `ringpop-go/hashring`）、`database/sql`（`MemberStore` 实现侧，接口无关）

---

## 文件映射（迁移前 → 迁移后）

| 文件 | 变化 |
|------|------|
| `ringpop-standalone/membership.go` | **保留**：接口和错误定义不变，删除对 ringpop 的语义注释 |
| `ringpop-standalone/hostinfo.go` | **修改**：移除 `rpmembership.Member` 接口实现，保留 `HostInfo` 相关逻辑 |
| `ringpop-standalone/hashring.go` | **新建**：将 ringpop-go/hashring 的一致性哈希环逻辑内化（用 farm hash） |
| `ringpop-standalone/service_resolver.go` | **重写**：移除所有 ringpop 依赖，改为接受成员列表注入 |
| `ringpop-standalone/monitor.go` | **重写**：删除 `NewRingpop`/TChannel，改为 DB 轮询驱动的成员管理 |
| `ringpop-standalone/example_usage.go` | **更新**：演示新 API（去掉 `NewRingpop` 调用） |
| `go.mod` | **修改**：删除 `ringpop-go`、`tchannel-go` 依赖（已不在 go.mod，当前只需保留 farm 和 uuid） |

---

## 关键设计决策

### MemberRecord 扩展（DB 侧存储更多字段）

迁移后，Gossip 标签（`draining`、`startAt`、`stopAt`、`roleKey`、`portKey`）转由 DB 字段承载：

```go
type MemberRecord struct {
    RPCAddress   net.IP
    RPCPort      uint16
    ServiceName  string
    Draining     bool
    StartAt      int64  // unix 秒，0 表示立即生效
    StopAt       int64  // unix 秒，0 表示不下线
}
```

`UpsertMemberRequest` 也增加 `Draining`/`StartAt`/`StopAt` 字段。

### 成员视图刷新机制

```
DB 轮询（每 ~5s）→ getActiveMembers() → 过滤 startAt/stopAt/draining
                                      → compareMembers() → 生成 ChangedEvent
                                      → 原子替换 ringAndHosts
```

`serviceResolver` 不再订阅 ringpop 事件，改为由 `Monitor` 定期调用 `resolver.setMembers(hosts)`。

### 保留不变的对外接口

- `Monitor` 接口（`membership.go`）：**完全不变**
- `ServiceResolver` 接口（`membership.go`）：**完全不变**
- `MemberStore` 接口：**扩展**（`MemberRecord` 增加字段，`UpsertMemberRequest` 增加字段）

---

## Task 1：自实现一致性哈希环（新建 hashring.go）

**Files:**
- Create: `ringpop-standalone/hashring.go`
- Create: `ringpop-standalone/hashring_test.go`

> 目的：替代 `github.com/temporalio/ringpop-go/hashring`，功能完全等价：虚拟节点、farm 哈希、Lookup/LookupN。

- [ ] **Step 1：新建测试文件，写失败测试**

```go
// ringpop-standalone/hashring_test.go
package ringpop

import (
	"fmt"
	"testing"
)

func TestHashRingEmpty(t *testing.T) {
	ring := newHashRing(100)
	_, found := ring.Lookup("any-key")
	if found {
		t.Fatal("expected not found on empty ring")
	}
	if got := ring.LookupN("any-key", 3); len(got) != 0 {
		t.Fatalf("expected empty, got %d", len(got))
	}
}

func TestHashRingLookupDeterministic(t *testing.T) {
	ring := newHashRing(100)
	ring.AddMember("host-a:7233")
	ring.AddMember("host-b:7233")
	ring.AddMember("host-c:7233")

	addr1, _ := ring.Lookup("namespace:queue:1")
	addr2, _ := ring.Lookup("namespace:queue:1")
	if addr1 != addr2 {
		t.Fatalf("non-deterministic: %s vs %s", addr1, addr2)
	}
}

func TestHashRingLookupNFirst(t *testing.T) {
	ring := newHashRing(100)
	ring.AddMember("host-a:7233")
	ring.AddMember("host-b:7233")
	ring.AddMember("host-c:7233")

	single, _ := ring.Lookup("some-key")
	multi := ring.LookupN("some-key", 2)
	if len(multi) == 0 || multi[0] != single {
		t.Fatalf("LookupN[0] must equal Lookup: single=%s multi=%v", single, multi)
	}
}

func TestHashRingRemoveMember(t *testing.T) {
	ring := newHashRing(100)
	ring.AddMember("host-a:7233")
	ring.AddMember("host-b:7233")

	ring.RemoveMember("host-a:7233")
	for range 1000 {
		addr, found := ring.Lookup(fmt.Sprintf("key-%d", found))
		if !found {
			t.Fatal("should find host-b")
		}
		if addr == "host-a:7233" {
			t.Fatal("removed member should not appear")
		}
	}
}

func TestHashRingDistribution(t *testing.T) {
	ring := newHashRing(200)
	members := []string{"host-a:7233", "host-b:7233", "host-c:7233"}
	for _, m := range members {
		ring.AddMember(m)
	}

	counts := map[string]int{}
	for i := range 3000 {
		addr, _ := ring.Lookup(fmt.Sprintf("key-%d", i))
		counts[addr]++
	}
	// 粗略检查分布：每个节点至少拿到 15%（3000/3=1000，允许±85%偏差）
	for _, m := range members {
		if counts[m] < 150 {
			t.Errorf("poor distribution for %s: %d/3000", m, counts[m])
		}
	}
}
```

- [ ] **Step 2：运行测试，确认编译失败**

```bash
go test ./ringpop-standalone/ -run TestHashRing -v
```
期望：编译错误（`newHashRing` 未定义）

- [ ] **Step 3：实现 hashring.go**

```go
// ringpop-standalone/hashring.go
package ringpop

import (
	"slices"
	"sort"

	"github.com/dgryski/go-farm"
)

// hashRing 是一个基于虚拟节点的一致性哈希环。
// 用 farm.Fingerprint32 作为哈希函数，与原 ringpop-go/hashring 行为一致。
// 写操作不是线程安全的；serviceResolver 在持锁时操作 ring，或构造新对象替换。
type hashRing struct {
	replicaPoints int
	vnodes        []vnode // 按 hash 升序排列
	members       map[string]struct{}
}

type vnode struct {
	hash uint32
	addr string
}

func newHashRing(replicaPoints int) *hashRing {
	return &hashRing{
		replicaPoints: replicaPoints,
		members:       make(map[string]struct{}),
	}
}

func (r *hashRing) AddMember(addr string) {
	if _, ok := r.members[addr]; ok {
		return
	}
	r.members[addr] = struct{}{}
	for i := range r.replicaPoints {
		key := fmt.Sprintf("%s-%d", addr, i)
		h := farm.Fingerprint32([]byte(key))
		r.vnodes = append(r.vnodes, vnode{hash: h, addr: addr})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
}

func (r *hashRing) RemoveMember(addr string) {
	if _, ok := r.members[addr]; !ok {
		return
	}
	delete(r.members, addr)
	r.vnodes = slices.DeleteFunc(r.vnodes, func(v vnode) bool {
		return v.addr == addr
	})
}

// Lookup 返回负责 key 的节点地址（顺时针最近虚拟节点）
func (r *hashRing) Lookup(key string) (string, bool) {
	if len(r.vnodes) == 0 {
		return "", false
	}
	h := farm.Fingerprint32([]byte(key))
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].addr, true
}

// LookupN 返回 key 顺时针方向 n 个不重复的真实节点（去虚拟节点重复）
func (r *hashRing) LookupN(key string, n int) []string {
	if len(r.members) == 0 {
		return nil
	}
	if n > len(r.members) {
		n = len(r.members)
	}
	h := farm.Fingerprint32([]byte(key))
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})

	seen := make(map[string]struct{}, n)
	result := make([]string, 0, n)
	for len(result) < n {
		if idx >= len(r.vnodes) {
			idx = 0
		}
		addr := r.vnodes[idx].addr
		if _, ok := seen[addr]; !ok {
			seen[addr] = struct{}{}
			result = append(result, addr)
		}
		idx++
	}
	return result
}
```

注意：`hashring.go` 需要 `import "fmt"` 和 `import "sort"`，补充到 import 中。

- [ ] **Step 4：运行测试，确认全部通过**

```bash
go test ./ringpop-standalone/ -run TestHashRing -v
```
期望：全部 PASS

- [ ] **Step 5：Commit**

```bash
git add ringpop-standalone/hashring.go ringpop-standalone/hashring_test.go
git commit -m "feat: add self-contained consistent hash ring (replaces ringpop-go/hashring)"
```

---

## Task 2：扩展 MemberStore 接口和数据结构

**Files:**
- Modify: `ringpop-standalone/membership.go`

> 目的：`MemberRecord` 增加 DB 侧存储的字段，替代原来靠 Gossip 标签传播的信息。

- [ ] **Step 1：修改 membership.go，扩展 MemberRecord 和 UpsertMemberRequest**

将 `membership.go` 中现有的：

```go
// MemberRecord 从 DB 读取的节点记录
type MemberRecord struct {
	RPCAddress     net.IP
	MembershipPort uint16 // gossip 端口（用于 Bootstrap）
}
```

替换为：

```go
// MemberRecord 从 DB 读取的节点记录
type MemberRecord struct {
	RPCAddress  net.IP
	RPCPort     uint16
	ServiceName string
	Draining    bool
	StartAt     int64 // unix 秒，0 表示立即生效
	StopAt      int64 // unix 秒，0 表示永不主动下线
}
```

将 `UpsertMemberRequest` 中的 `MembershipPort` 字段删除，新增 `Draining`/`StartAt`/`StopAt`：

```go
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
```

同时删除 `membership.go` 中 `MemberStore` 里的 `monitor.go` 注释里对 gossip 的描述，接口签名不变（`UpsertMember`、`GetActiveMembers`、`PruneExpiredMembers`）。

- [ ] **Step 2：确认编译**

```bash
go build ./ringpop-standalone/
```
期望：编译失败（`monitor.go` 里引用了 `MembershipPort`，下一步修复）

- [ ] **Step 3：Commit（先提交接口变更，monitor 将在 Task 4 修复）**

```bash
git add ringpop-standalone/membership.go
git commit -m "feat: extend MemberRecord and UpsertMemberRequest with draining/startAt/stopAt fields"
```

---

## Task 3：重写 hostinfo.go（去掉 rpmembership.Member）

**Files:**
- Modify: `ringpop-standalone/hostinfo.go`
- Create: `ringpop-standalone/hostinfo_test.go`

> 目的：`hostInfo` 不再需要实现 ringpop 的 `rpmembership.Member` 接口（`Label(key string) (string, bool)` 方法），labels 改为直接存储 draining/startAt/stopAt 三个具体字段。

- [ ] **Step 1：写测试**

```go
// ringpop-standalone/hostinfo_test.go
package ringpop

import "testing"

func TestHostInfoBasic(t *testing.T) {
	h := newHostInfo("10.0.0.1:7233", false, 0, 0)
	if h.GetAddress() != "10.0.0.1:7233" {
		t.Fatalf("unexpected address: %s", h.GetAddress())
	}
	if h.Identity() != "10.0.0.1:7233" {
		t.Fatalf("unexpected identity: %s", h.Identity())
	}
}

func TestHostInfoDraining(t *testing.T) {
	h1 := newHostInfo("10.0.0.1:7233", false, 0, 0)
	h2 := newHostInfo("10.0.0.1:7233", true, 0, 0)
	if isDraining(h1) {
		t.Fatal("h1 should not be draining")
	}
	if !isDraining(h2) {
		t.Fatal("h2 should be draining")
	}
}

func TestHostInfoChecksum(t *testing.T) {
	h1 := newHostInfo("10.0.0.1:7233", false, 0, 0)
	h2 := newHostInfo("10.0.0.1:7233", false, 0, 0)
	h3 := newHostInfo("10.0.0.1:7233", true, 0, 0)
	if h1.labelsChecksum != h2.labelsChecksum {
		t.Fatal("same state should produce same checksum")
	}
	if h1.labelsChecksum == h3.labelsChecksum {
		t.Fatal("different draining should produce different checksum")
	}
}
```

- [ ] **Step 2：运行测试，确认失败**

```bash
go test ./ringpop-standalone/ -run TestHostInfo -v
```
期望：编译错误（`newHostInfo` 签名不匹配）

- [ ] **Step 3：重写 hostinfo.go**

```go
// ringpop-standalone/hostinfo.go
package ringpop

import "github.com/dgryski/go-farm"

// hostInfo 代表哈希环中的一个节点实例，实现 HostInfo 接口。
type hostInfo struct {
	addr           string
	draining       bool
	startAt        int64 // unix 秒，0 表示立即生效
	stopAt         int64 // unix 秒，0 表示不主动下线
	labelsChecksum uint64
}

var _ HostInfo = (*hostInfo)(nil)

func newHostInfo(addr string, draining bool, startAt, stopAt int64) *hostInfo {
	h := &hostInfo{
		addr:     addr,
		draining: draining,
		startAt:  startAt,
		stopAt:   stopAt,
	}
	h.labelsChecksum = h.computeChecksum()
	return h
}

func (hi *hostInfo) GetAddress() string { return hi.addr }
func (hi *hostInfo) Identity() string   { return hi.addr }

func (hi *hostInfo) computeChecksum() uint64 {
	var c uint64
	c ^= farm.Fingerprint64([]byte(hi.addr))
	if hi.draining {
		c ^= 0x1
	}
	// startAt/stopAt 变化也会影响 checksum
	var buf [16]byte
	for i, v := range []int64{hi.startAt, hi.stopAt} {
		for j := range 8 {
			buf[i*8+j] = byte(v >> (j * 8))
		}
	}
	c ^= farm.Fingerprint64(buf[:])
	return c
}

func isDraining(host *hostInfo) bool {
	return host.draining
}
```

- [ ] **Step 4：运行测试，确认通过**

```bash
go test ./ringpop-standalone/ -run TestHostInfo -v
```
期望：全部 PASS

- [ ] **Step 5：Commit**

```bash
git add ringpop-standalone/hostinfo.go ringpop-standalone/hostinfo_test.go
git commit -m "refactor: remove ringpop membership.Member dependency from hostInfo"
```

---

## Task 4：重写 service_resolver.go（去掉 ringpop 依赖，改为成员注入）

**Files:**
- Modify: `ringpop-standalone/service_resolver.go`
- Create: `ringpop-standalone/service_resolver_test.go`

> 目的：`serviceResolver` 不再订阅 ringpop 事件，改为由 Monitor 调用 `setMembers(hosts)` 注入成员列表。内部一致性哈希环改用 Task 1 实现的 `hashRing`。

- [ ] **Step 1：写测试**

```go
// ringpop-standalone/service_resolver_test.go
package ringpop

import (
	"testing"
	"time"
)

func makeHost(addr string, draining bool) *hostInfo {
	return newHostInfo(addr, draining, 0, 0)
}

func TestServiceResolverLookup(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
	defer r.Stop()

	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", false),
	})

	host, err := r.Lookup("namespace:queue:1")
	if err != nil {
		t.Fatal(err)
	}
	if host.GetAddress() == "" {
		t.Fatal("expected non-empty address")
	}
}

func TestServiceResolverEmpty(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
	defer r.Stop()

	_, err := r.Lookup("any-key")
	if err != ErrInsufficientHosts {
		t.Fatalf("expected ErrInsufficientHosts, got %v", err)
	}
}

func TestServiceResolverMemberCount(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
	defer r.Stop()

	r.setMembers([]*hostInfo{
		makeHost("10.0.0.1:7235", false),
		makeHost("10.0.0.2:7235", true), // draining
	})

	if r.MemberCount() != 2 {
		t.Fatalf("expected 2, got %d", r.MemberCount())
	}
	if r.AvailableMemberCount() != 1 {
		t.Fatalf("expected 1 available, got %d", r.AvailableMemberCount())
	}
}

func TestServiceResolverChangedEvent(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
	defer r.Stop()

	ch := make(chan *ChangedEvent, 5)
	_ = r.AddListener("test", ch)

	r.setMembers([]*hostInfo{makeHost("10.0.0.1:7235", false)})

	select {
	case ev := <-ch:
		if len(ev.HostsAdded) != 1 {
			t.Fatalf("expected 1 added, got %d", len(ev.HostsAdded))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ChangedEvent")
	}
}

func TestServiceResolverDuplicateListener(t *testing.T) {
	r := newServiceResolver("matching", 200)
	r.Start()
	defer r.Stop()

	ch := make(chan *ChangedEvent, 1)
	if err := r.AddListener("dup", ch); err != nil {
		t.Fatal(err)
	}
	if err := r.AddListener("dup", ch); err != ErrListenerAlreadyExist {
		t.Fatalf("expected ErrListenerAlreadyExist, got %v", err)
	}
}
```

- [ ] **Step 2：运行测试，确认失败**

```bash
go test ./ringpop-standalone/ -run TestServiceResolver -v
```
期望：编译错误

- [ ] **Step 3：重写 service_resolver.go**

```go
// ringpop-standalone/service_resolver.go
package ringpop

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	drainingKey = "draining"
	startAtKey  = "startAt"
	stopAtKey   = "stopAt"

	minRefreshInternal     = time.Second * 4
	defaultRefreshInterval = time.Second * 10
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
	// no-op：DB 轮询由 Monitor 驱动，resolver 本身不需要主动触发
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
```

- [ ] **Step 4：运行测试**

```bash
go test ./ringpop-standalone/ -run TestServiceResolver -v
```
期望：全部 PASS

- [ ] **Step 5：Commit**

```bash
git add ringpop-standalone/service_resolver.go ringpop-standalone/service_resolver_test.go
git commit -m "refactor: rewrite serviceResolver with DB-injected members, remove ringpop-go dependency"
```

---

## Task 5：重写 monitor.go（去掉 Ringpop，改为 DB 轮询）

**Files:**
- Modify: `ringpop-standalone/monitor.go`
- Create: `ringpop-standalone/monitor_test.go`

> 目的：`Monitor` 删除所有 `ringpop.Ringpop`/TChannel 相关代码，改为：启动时写心跳 → 定期从 DB 拉取活跃成员 → 按服务分组 → 调用各 resolver.setMembers()。

- [ ] **Step 1：写测试（用内存 stub MemberStore）**

```go
// ringpop-standalone/monitor_test.go
package ringpop

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// stubStore 是内存版 MemberStore，用于测试
type stubStore struct {
	mu      sync.Mutex
	members []MemberRecord
	upserts []UpsertMemberRequest
}

func (s *stubStore) UpsertMember(_ context.Context, req UpsertMemberRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upserts = append(s.upserts, req)
	// 更新 members 列表（模拟数据库 upsert）
	for i, m := range s.members {
		if m.RPCAddress.Equal(req.RPCAddress) && m.RPCPort == req.RPCPort && m.ServiceName == req.ServiceName {
			s.members[i] = MemberRecord{
				RPCAddress:  req.RPCAddress,
				RPCPort:     req.RPCPort,
				ServiceName: req.ServiceName,
				Draining:    req.Draining,
				StartAt:     req.StartAt,
				StopAt:      req.StopAt,
			}
			return nil
		}
	}
	s.members = append(s.members, MemberRecord{
		RPCAddress:  req.RPCAddress,
		RPCPort:     req.RPCPort,
		ServiceName: req.ServiceName,
		Draining:    req.Draining,
		StartAt:     req.StartAt,
		StopAt:      req.StopAt,
	})
	return nil
}

func (s *stubStore) GetActiveMembers(_ context.Context, _ time.Duration) ([]MemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]MemberRecord, len(s.members))
	copy(result, s.members)
	return result, nil
}

func (s *stubStore) PruneExpiredMembers(_ context.Context, _ int) error { return nil }

func TestMonitorStartStop(t *testing.T) {
	store := &stubStore{}
	// 预填充一个同服务的成员（模拟自身）
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("127.0.0.1"), RPCPort: 7233, ServiceName: "matching"},
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7233},
		store,
		"127.0.0.1:7233",
		5*time.Second,  // pollInterval
		3*time.Second,  // propagationTime
		time.Time{},    // joinTime
		200,            // replicaPoints
	)
	m.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.WaitUntilInitialized(ctx); err != nil {
		t.Fatal(err)
	}

	members, err := m.GetReachableMembers()
	if err != nil {
		t.Fatal(err)
	}
	if len(members) == 0 {
		t.Fatal("expected at least 1 member")
	}

	m.Stop()
}

func TestMonitorGetResolver(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
		{RPCAddress: net.ParseIP("10.0.0.2"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235, "history": 7234},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	resolver, err := m.GetResolver("matching")
	if err != nil {
		t.Fatal(err)
	}
	host, err := resolver.Lookup("some-key")
	if err != nil {
		t.Fatal(err)
	}
	if host.GetAddress() == "" {
		t.Fatal("empty address")
	}

	_, err = m.GetResolver("unknown-service")
	if err != ErrUnknownService {
		t.Fatalf("expected ErrUnknownService, got %v", err)
	}

	m.Stop()
}

func TestMonitorSetDraining(t *testing.T) {
	store := &stubStore{}
	store.members = []MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}

	m := NewMonitor(
		"matching",
		map[string]int{"matching": 7235},
		store,
		"10.0.0.1:7235",
		100*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	if err := m.SetDraining(true); err != nil {
		t.Fatal(err)
	}

	// 等待下次轮询同步
	time.Sleep(300 * time.Millisecond)

	resolver, _ := m.GetResolver("matching")
	if resolver.AvailableMemberCount() != 0 {
		t.Fatalf("expected 0 available after draining, got %d", resolver.AvailableMemberCount())
	}

	m.Stop()
}

func TestMonitorHeartbeat(t *testing.T) {
	store := &stubStore{}

	m := NewMonitor(
		"history",
		map[string]int{"history": 7234},
		store,
		"10.0.0.5:7234",
		50*time.Millisecond,
		time.Second,
		time.Time{},
		200,
	)
	m.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.WaitUntilInitialized(ctx)

	// 等足够长，确保至少有一次心跳写入
	time.Sleep(200 * time.Millisecond)
	store.mu.Lock()
	upsertCount := len(store.upserts)
	store.mu.Unlock()
	if upsertCount == 0 {
		t.Fatal("expected at least 1 heartbeat upsert")
	}

	m.Stop()
	fmt.Printf("heartbeat count: %d\n", upsertCount)
}
```

- [ ] **Step 2：运行测试，确认失败**

```bash
go test ./ringpop-standalone/ -run TestMonitor -v
```
期望：编译错误（`NewMonitor` 签名变化）

- [ ] **Step 3：重写 monitor.go**

```go
// ringpop-standalone/monitor.go
package ringpop

import (
	"context"
	"fmt"
	"math"
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

// MemberStore 是 Monitor 依赖的持久化接口。
type MemberStore interface {
	UpsertMember(ctx context.Context, req UpsertMemberRequest) error
	GetActiveMembers(ctx context.Context, lastHeartbeatWithin time.Duration) ([]MemberRecord, error)
	PruneExpiredMembers(ctx context.Context, maxCount int) error
}

// Monitor 实现
type Monitor struct {
	mu     sync.Mutex
	status int32 // 0=init, 1=started, 2=stopped

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
) *Monitor {
	ctx, cancel := context.WithCancel(context.Background())
	hostID, _ := uuid.New().MarshalBinary()

	m := &Monitor{
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
func (m *Monitor) Start() {
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

	// 初次同步成员列表
	if err := m.syncMembers(); err != nil {
		panic("failed to sync members on startup: " + err.Error())
	}

	m.mu.Lock()
	for _, ring := range m.rings {
		ring.Start()
	}
	m.mu.Unlock()

	close(m.initialized)

	// 启动轮询循环
	go m.pollLoop()
}

// Stop 优雅关闭
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
}

func (m *Monitor) WaitUntilInitialized(ctx context.Context) error {
	select {
	case <-m.initialized:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Monitor) EvictSelf() error {
	return m.SetDraining(true)
}

func (m *Monitor) EvictSelfAt(asOf time.Time) (time.Duration, error) {
	until := time.Until(asOf)
	if until <= 0 || until.Seconds() >= maxScheduledEventTimeSeconds {
		return 0, m.SetDraining(true)
	}
	leaveAfter := until + m.propagationTime
	time.AfterFunc(until, func() { _ = m.SetDraining(true) })
	return leaveAfter + m.propagationTime, nil
}

func (m *Monitor) GetResolver(service string) (ServiceResolver, error) {
	ring, found := m.rings[service]
	if !found {
		return nil, ErrUnknownService
	}
	return ring, nil
}

func (m *Monitor) GetReachableMembers() ([]string, error) {
	members, err := m.store.GetActiveMembers(m.lifecycleCtx, healthyHostLastHeartbeatCutoff)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(members))
	for _, m := range members {
		result = append(result, net.JoinHostPort(m.RPCAddress.String(), strconv.Itoa(int(m.RPCPort))))
	}
	return result, nil
}

func (m *Monitor) SetDraining(draining bool) error {
	m.mu.Lock()
	m.draining = draining
	m.mu.Unlock()

	// 立即更新 DB，让其他节点在下次轮询时感知
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

func (m *Monitor) ApproximateMaxPropagationTime() time.Duration {
	return m.propagationTime
}

// ---- 内部方法 ----

func (m *Monitor) pollLoop() {
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

// syncMembers 从 DB 拉取活跃成员，按服务角色分组，注入各 serviceResolver
func (m *Monitor) syncMembers() error {
	records, err := m.store.GetActiveMembers(m.lifecycleCtx, healthyHostLastHeartbeatCutoff)
	if err != nil {
		return fmt.Errorf("syncMembers: %w", err)
	}

	nowUnix := time.Now().Unix()

	// 按服务角色分组
	byService := make(map[string][]*hostInfo, len(m.services))
	for svc := range m.services {
		byService[svc] = nil
	}

	for _, rec := range records {
		if _, ok := byService[rec.ServiceName]; !ok {
			continue // 不在我们追踪的服务列表里
		}
		// 过滤 startAt
		if rec.StartAt > 0 && rec.StartAt > nowUnix {
			continue
		}
		// 过滤 stopAt
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

func (m *Monitor) startHeartbeat() error {
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

	// 先写一次，再启动循环
	if err = m.store.UpsertMember(m.lifecycleCtx, req); err != nil {
		return err
	}

	go m.heartbeatLoop(host, uint16(rpcPort))
	return nil
}

func (m *Monitor) buildUpsertRequest(host string, rpcPort uint16) UpsertMemberRequest {
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

func (m *Monitor) heartbeatLoop(host string, rpcPort uint16) {
	for {
		select {
		case <-m.lifecycleCtx.Done():
			return
		default:
		}
		req := m.buildUpsertRequest(host, rpcPort)
		_ = m.store.UpsertMember(m.lifecycleCtx, req)
		jitter := time.Duration(math.Round(rand.Float64() * 5))
		time.Sleep((10 + jitter) * time.Second)
	}
}
```

- [ ] **Step 4：运行所有测试**

```bash
go test ./ringpop-standalone/ -v -timeout 30s
```
期望：全部 PASS

- [ ] **Step 5：Commit**

```bash
git add ringpop-standalone/monitor.go ringpop-standalone/monitor_test.go
git commit -m "feat: rewrite Monitor with DB polling, remove Ringpop/TChannel dependency"
```

---

## Task 6：更新 example_usage.go 和 go.mod

**Files:**
- Modify: `ringpop-standalone/example_usage.go`
- Modify: `go.mod`（删除 ringpop-go、tchannel-go 如已存在）

- [ ] **Step 1：更新 example_usage.go**

将 `example_usage.go` 全部替换为：

```go
//go:build ignore
// +build ignore

// 这个文件演示如何使用本包构建自己的分布式节点发现系统（DB 轮询版，无 Gossip 依赖）。
// 运行方式：go run example_usage.go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	ringpop "github.com/donutnomad/taskq/ringpop-standalone"
)

// ---- 第一步：实现 MemberStore 接口 ----

type MySQLMemberStore struct {
	// db *sql.DB
}

func (s *MySQLMemberStore) UpsertMember(ctx context.Context, req ringpop.UpsertMemberRequest) error {
	// INSERT INTO cluster_members
	//   (service_name, rpc_address, rpc_port, host_id, session_start, expires_at, draining, start_at, stop_at)
	// VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	// ON DUPLICATE KEY UPDATE rpc_address=?, rpc_port=?, session_start=?, expires_at=?, draining=?, start_at=?, stop_at=?
	fmt.Printf("[DB] upsert member: %s at %s:%d (draining=%v)\n",
		req.ServiceName, req.RPCAddress, req.RPCPort, req.Draining)
	return nil
}

func (s *MySQLMemberStore) GetActiveMembers(ctx context.Context, lastHeartbeatWithin time.Duration) ([]ringpop.MemberRecord, error) {
	// SELECT service_name, rpc_address, rpc_port, draining, start_at, stop_at
	// FROM cluster_members
	// WHERE updated_at > NOW() - INTERVAL ? SECOND
	return []ringpop.MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}, nil
}

func (s *MySQLMemberStore) PruneExpiredMembers(ctx context.Context, maxCount int) error {
	// DELETE FROM cluster_members WHERE expires_at < NOW() LIMIT ?
	return nil
}

// ---- 第二步：启动 Monitor ----

func main() {
	store := &MySQLMemberStore{}

	myGRPCAddr := "10.0.0.2:7235" // 本节点 gRPC 地址

	services := map[string]int{
		"matching": 7235,
		"history":  7234,
		"frontend": 7233,
	}

	monitor := ringpop.NewMonitor(
		"matching",      // 本节点的服务角色
		services,        // 集群所有角色及其端口
		store,
		myGRPCAddr,     // gRPC 广播地址
		5*time.Second,  // pollInterval：DB 轮询间隔
		3*time.Second,  // propagationTime：变更传播估计时间
		time.Time{},    // joinTime：立即加入（零值表示无延迟）
		200,            // replicaPoints：虚拟节点数
	)

	monitor.Start()
	if err := monitor.WaitUntilInitialized(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Monitor started successfully")

	// ---- 第三步：使用 ServiceResolver 做路由 ----

	resolver, err := monitor.GetResolver("matching")
	if err != nil {
		log.Fatal(err)
	}

	key := "namespace-abc:my-task-queue:1"
	host, err := resolver.Lookup(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Task queue '%s' is served by: %s\n", key, host.GetAddress())

	// 监听成员变化
	changeCh := make(chan *ringpop.ChangedEvent, 10)
	_ = resolver.AddListener("my-listener", changeCh)
	go func() {
		for event := range changeCh {
			fmt.Printf("Membership changed: +%d -%d ~%d\n",
				len(event.HostsAdded), len(event.HostsRemoved), len(event.HostsChanged))
		}
	}()

	// 优雅下线
	time.Sleep(time.Hour)
	leaveAfter, _ := monitor.EvictSelfAt(time.Now().Add(5 * time.Second))
	time.Sleep(leaveAfter)
	monitor.Stop()
}
```

- [ ] **Step 2：确认 go.mod 干净（ringpop-go/tchannel 不在依赖中）**

```bash
go mod tidy
cat go.mod
```
期望：只剩 `github.com/dgryski/go-farm` 和 `github.com/google/uuid`

- [ ] **Step 3：完整编译验证**

```bash
go build ./ringpop-standalone/
```
期望：无错误

- [ ] **Step 4：Commit**

```bash
git add ringpop-standalone/example_usage.go go.mod go.sum
git commit -m "chore: update example and remove ringpop-go/tchannel-go from dependencies"
```

---

## Task 7：全量测试 + 验收

- [ ] **Step 1：运行全量测试**

```bash
go test ./ringpop-standalone/ -v -timeout 60s -count=1
```
期望：全部 PASS，无 ringpop/tchannel 相关 import

- [ ] **Step 2：竞态检测**

```bash
go test ./ringpop-standalone/ -race -timeout 60s
```
期望：no data race detected

- [ ] **Step 3：确认无旧依赖残留**

```bash
grep -r "ringpop-go\|tchannel" ringpop-standalone/
```
期望：无输出

- [ ] **Step 4：最终 Commit**

```bash
git add .
git commit -m "test: add full test coverage for DB-based membership, all tests passing"
```

---

## 验证检查清单

| 验收项 | 验证方式 |
|--------|---------|
| `Monitor`/`ServiceResolver` 接口签名不变 | 编译通过 |
| 不再依赖 `ringpop-go`/`tchannel-go` | `go mod tidy` + `grep` |
| `Lookup`/`LookupN` 一致性哈希语义正确 | `TestHashRing*` |
| `setMembers` 触发 `ChangedEvent` | `TestServiceResolverChangedEvent` |
| `SetDraining` 传播到 DB 并影响 `AvailableMembers` | `TestMonitorSetDraining` |
| 心跳定期写入 DB | `TestMonitorHeartbeat` |
| 无数据竞争 | `-race` flag |
| `startAt`/`stopAt` 过滤逻辑 | `syncMembers` 中的时间过滤 + 单元测试覆盖 |
