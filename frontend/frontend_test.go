package frontend

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---- 测试用 mock ----

type stubResolver struct {
	members   []membership.HostInfo
	listeners map[string]chan<- *membership.ChangedEvent
}

func newStubResolver(addrs ...string) *stubResolver {
	members := make([]membership.HostInfo, len(addrs))
	for i, addr := range addrs {
		members[i] = membership.NewHostInfoFromAddress(addr)
	}
	return &stubResolver{
		members:   members,
		listeners: make(map[string]chan<- *membership.ChangedEvent),
	}
}

func (r *stubResolver) Lookup(key string) (membership.HostInfo, error) {
	if len(r.members) == 0 {
		return nil, membership.ErrInsufficientHosts
	}
	idx := len(key) % len(r.members)
	return r.members[idx], nil
}
func (r *stubResolver) LookupN(_ string, _ int) []membership.HostInfo { return nil }
func (r *stubResolver) AddListener(name string, ch chan<- *membership.ChangedEvent) error {
	r.listeners[name] = ch
	return nil
}
func (r *stubResolver) RemoveListener(name string) error {
	delete(r.listeners, name)
	return nil
}
func (r *stubResolver) MemberCount() int                        { return len(r.members) }
func (r *stubResolver) AvailableMemberCount() int               { return len(r.members) }
func (r *stubResolver) Members() []membership.HostInfo          { return r.members }
func (r *stubResolver) AvailableMembers() []membership.HostInfo { return r.members }
func (r *stubResolver) RequestRefresh()                         {}

type stubMonitor struct {
	resolver *stubResolver
}

func (m *stubMonitor) Start()                                       {}
func (m *stubMonitor) Stop()                                        {}
func (m *stubMonitor) EvictSelf() error                             { return nil }
func (m *stubMonitor) EvictSelfAt(time.Time) (time.Duration, error) { return 0, nil }
func (m *stubMonitor) GetResolver(string) (membership.ServiceResolver, error) {
	return m.resolver, nil
}
func (m *stubMonitor) GetReachableMembers() ([]string, error)       { return nil, nil }
func (m *stubMonitor) WaitUntilInitialized(context.Context) error   { return nil }
func (m *stubMonitor) SetDraining(bool) error                       { return nil }
func (m *stubMonitor) ApproximateMaxPropagationTime() time.Duration { return 0 }

func newTestSQLiteStore(t *testing.T) queue.Store {
	t.Helper()
	store, err := queue.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// startMatchingServer 启动测试用 matching 服务，返回监听地址
func startMatchingServer(t *testing.T, store queue.Store, resolver *stubResolver, queueConfigs map[string]int) string {
	t.Helper()

	monitor := &stubMonitor{resolver: resolver}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv, err := matching.NewServer(matching.Config{
		Store:         store,
		Monitor:       monitor,
		ListenAddr:    addr,
		BroadcastAddr: addr,
		QueueConfigs:  queueConfigs,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 让 resolver 指向本服务
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	time.Sleep(50 * time.Millisecond)
	return addr
}

// ---- 测试 ----

func TestFrontend_EndToEnd(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	addr := startMatchingServer(t, store, resolver, map[string]int{"test-q": 4})

	monitor := &stubMonitor{resolver: resolver}
	_ = addr

	fe, err := New(Config{
		Monitor:       monitor,
		QueueName:     "test-q",
		NumPartitions: 4,
		DialOpts:      []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()

	// Enqueue
	if err := fe.Enqueue(ctx, []byte("hello-frontend")); err != nil {
		t.Fatal(err)
	}

	// Dequeue
	data, token, err := fe.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello-frontend" {
		t.Fatalf("expected hello-frontend, got %s", data)
	}
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	// Ack
	if err := fe.Ack(ctx, token); err != nil {
		t.Fatal(err)
	}
}

func TestFrontend_NackRedelivery(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, store, resolver, map[string]int{"test-q": 4})

	monitor := &stubMonitor{resolver: resolver}
	fe, err := New(Config{
		Monitor:       monitor,
		QueueName:     "test-q",
		NumPartitions: 4,
		DialOpts:      []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()

	fe.Enqueue(ctx, []byte("nack-test"))
	data, token, _ := fe.Dequeue(ctx)

	// Nack
	if err := fe.Nack(ctx, token); err != nil {
		t.Fatal(err)
	}

	// 再次 Dequeue 应拿到同一任务
	data2, _, err := fe.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(data2) != string(data) {
		t.Fatalf("expected %s, got %s", data, data2)
	}
}

func TestFrontend_MultipleEnqueue(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, store, resolver, map[string]int{"test-q": 4})

	monitor := &stubMonitor{resolver: resolver}
	fe, err := New(Config{
		Monitor:       monitor,
		QueueName:     "test-q",
		NumPartitions: 4,
		DialOpts:      []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()

	// 写入 10 个任务
	for i := range 10 {
		if err := fe.Enqueue(ctx, []byte(fmt.Sprintf("task-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// 消费 10 个任务
	seen := make(map[string]bool)
	for range 10 {
		data, token, err := fe.Dequeue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		seen[string(data)] = true
		fe.Ack(ctx, token)
	}

	if len(seen) != 10 {
		t.Fatalf("expected 10 unique tasks, got %d", len(seen))
	}
}

// TestFrontend_WorkerConcurrent 模拟多个 worker 并发消费
func TestFrontend_WorkerConcurrent(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, store, resolver, map[string]int{"work-q": 4})

	monitor := &stubMonitor{resolver: resolver}
	fe, err := New(Config{
		Monitor:       monitor,
		QueueName:     "work-q",
		NumPartitions: 4,
		DialOpts:      []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	const (
		numTasks   = 50
		numWorkers = 4
	)

	ctx := context.Background()

	// 生产：写入 50 个任务
	for i := range numTasks {
		if err := fe.Enqueue(ctx, fmt.Appendf(nil, "job-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// 消费：4 个 worker 并发拉取
	var mu sync.Mutex
	consumed := make(map[string]bool)
	var wg sync.WaitGroup

	for w := range numWorkers {
		wg.Go(func() {
			for {
				mu.Lock()
				done := len(consumed) >= numTasks
				mu.Unlock()
				if done {
					return
				}

				pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				data, token, err := fe.Dequeue(pollCtx)
				cancel()
				if err != nil {
					continue
				}

				if err := fe.Ack(ctx, token); err != nil {
					t.Errorf("worker %d ack error: %v", w, err)
				}

				mu.Lock()
				consumed[string(data)] = true
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	if len(consumed) != numTasks {
		t.Fatalf("expected %d consumed tasks, got %d", numTasks, len(consumed))
	}
}

// TestFrontend_WorkerLongPoll 模拟 worker 先启动长轮询，后到达任务
func TestFrontend_WorkerLongPoll(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, store, resolver, map[string]int{"poll-q": 4})

	monitor := &stubMonitor{resolver: resolver}
	fe, err := New(Config{
		Monitor:       monitor,
		QueueName:     "poll-q",
		NumPartitions: 4,
		DialOpts:      []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout:   5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()

	// worker 先启动 Dequeue（此时队列为空，会进入长轮询）
	type result struct {
		data []byte
		tok  string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		data, tok, err := fe.Dequeue(ctx)
		ch <- result{data, tok, err}
	}()

	// 200ms 后生产者投入任务
	time.Sleep(200 * time.Millisecond)
	if err := fe.Enqueue(ctx, []byte("late-arrival")); err != nil {
		t.Fatal(err)
	}

	// worker 应该在短时间内收到任务（不需要等满 5s）
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		if string(r.data) != "late-arrival" {
			t.Fatalf("expected late-arrival, got %s", r.data)
		}
		fe.Ack(ctx, r.tok)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not receive task within 3s")
	}
}
