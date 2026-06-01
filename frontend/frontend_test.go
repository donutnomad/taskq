package frontend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"github.com/donutnomad/taskq/store/gormstore"
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
func (r *stubResolver) AddListener(name string, ch chan<- *membership.ChangedEvent) error {
	if _, ok := r.listeners[name]; ok {
		return membership.ErrListenerAlreadyExist
	}
	r.listeners[name] = ch
	return nil
}
func (r *stubResolver) RemoveListener(name string) error {
	delete(r.listeners, name)
	return nil
}

type stubMonitor struct {
	resolver *stubResolver
}

func (m *stubMonitor) Start()                                       {}
func (m *stubMonitor) Stop()                                        {}
func (m *stubMonitor) EvictSelfAt(time.Time) (time.Duration, error) { return 0, nil }
func (m *stubMonitor) GetResolver(string) (membership.ServiceResolver, error) {
	return m.resolver, nil
}
func (m *stubMonitor) WaitUntilInitialized(context.Context) error { return nil }
func (m *stubMonitor) SetDraining(bool) error                     { return nil }

type refreshAwareResolver struct {
	mu        sync.Mutex
	current   string
	next      string
	switched  bool
	listeners map[string]chan<- *membership.ChangedEvent
}

func newRefreshAwareResolver(current, next string) *refreshAwareResolver {
	return &refreshAwareResolver{
		current:   current,
		next:      next,
		listeners: make(map[string]chan<- *membership.ChangedEvent),
	}
}

func (r *refreshAwareResolver) Lookup(_ string) (membership.HostInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.switched {
		return membership.NewHostInfoFromAddress(r.next), nil
	}
	return membership.NewHostInfoFromAddress(r.current), nil
}

func (r *refreshAwareResolver) AddListener(name string, ch chan<- *membership.ChangedEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.listeners[name]; ok {
		return membership.ErrListenerAlreadyExist
	}
	r.listeners[name] = ch
	return nil
}
func (r *refreshAwareResolver) RemoveListener(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.listeners, name)
	return nil
}

func (r *refreshAwareResolver) switchToNext() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.switched = true
}

func (r *refreshAwareResolver) isSwitched() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.switched
}

type refreshAwareMonitor struct {
	resolver membership.ServiceResolver
}

func (m *refreshAwareMonitor) Start()                                       {}
func (m *refreshAwareMonitor) Stop()                                        {}
func (m *refreshAwareMonitor) EvictSelfAt(time.Time) (time.Duration, error) { return 0, nil }
func (m *refreshAwareMonitor) GetResolver(string) (membership.ServiceResolver, error) {
	return m.resolver, nil
}
func (m *refreshAwareMonitor) WaitUntilInitialized(context.Context) error { return nil }
func (m *refreshAwareMonitor) SetDraining(bool) error                     { return nil }

func newTestSQLiteStore(t *testing.T) queue.Store {
	t.Helper()
	store, err := gormstore.NewSQLite(":memory:", gormstore.Config{AutoMigrate: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// startMatchingServer 启动测试用 matching 服务，返回监听地址
func startMatchingServer(t *testing.T, store queue.Store, resolver *stubResolver, queueConfigs map[string]int) string {
	t.Helper()
	_, addr := startMatchingServerWithHandle(t, store, resolver, queueConfigs)
	return addr
}

func startMatchingServerWithHandle(t *testing.T, store queue.Store, resolver *stubResolver, queueConfigs map[string]int) (*matching.Server, string) {
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
	return srv, addr
}

// ---- 测试 ----

func TestFrontend_EndToEnd(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	addr := startMatchingServer(t, store, resolver, map[string]int{"test-q": 4})

	monitor := &stubMonitor{resolver: resolver}
	_ = addr

	fe, err := New(Config{
		Monitor:   monitor,
		QueueName: "test-q",

		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 2 * time.Second,
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
		Monitor:   monitor,
		QueueName: "test-q",

		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 2 * time.Second,
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
		Monitor:   monitor,
		QueueName: "test-q",

		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 2 * time.Second,
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
		Monitor:   monitor,
		QueueName: "work-q",

		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 5 * time.Second,
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
		Monitor:   monitor,
		QueueName: "poll-q",

		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 5 * time.Second,
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

func TestFrontend_DequeueEmpty_ReturnsErrNoTask(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, store, resolver, map[string]int{"empty-q": 4})

	monitor := &stubMonitor{resolver: resolver}
	fe, err := New(Config{
		Monitor:     monitor,
		QueueName:   "empty-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 500 * time.Millisecond, // 短超时
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()

	// 空队列 Dequeue 应返回 ErrNoTask，而不是 nil error
	_, _, err = fe.Dequeue(ctx)
	if !errors.Is(err, ErrNoTask) {
		t.Fatalf("expected ErrNoTask on empty queue, got %v", err)
	}
}

func TestFrontend_DequeueReturnsUnderlyingErrorWhenAllPartitionsFail(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	srv, _ := startMatchingServerWithHandle(t, store, resolver, map[string]int{"down-q": 2})

	monitor := &stubMonitor{resolver: resolver}
	fe, err := New(Config{
		Monitor:     monitor,
		QueueName:   "down-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = fe.Dequeue(ctx)
	if err == nil {
		t.Fatal("expected dequeue to surface the transport failure")
	}
	if errors.Is(err, ErrNoTask) {
		t.Fatalf("expected transport failure, got ErrNoTask: %v", err)
	}
}

func TestFrontend_AllowsMultipleInstancesOnSameResolver(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, store, resolver, map[string]int{"multi-q": 2})

	monitor := &stubMonitor{resolver: resolver}
	fe1, err := New(Config{
		Monitor:     monitor,
		QueueName:   "multi-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe1.Close()

	fe2, err := New(Config{
		Monitor:     monitor,
		QueueName:   "multi-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("expected multiple frontend instances on the same resolver to succeed, got %v", err)
	}
	defer fe2.Close()
}

func TestFrontend_EnqueueRetriesAfterResolverRefresh(t *testing.T) {
	store := newTestSQLiteStore(t)

	oldResolver := newStubResolver()
	oldSrv, oldAddr := startMatchingServerWithHandle(t, store, oldResolver, map[string]int{"refresh-q": 1})

	newResolver := newStubResolver()
	_, newAddr := startMatchingServerWithHandle(t, store, newResolver, map[string]int{"refresh-q": 1})

	routing := newRefreshAwareResolver(oldAddr, newAddr)
	fe, err := New(Config{
		Monitor:     &refreshAwareMonitor{resolver: routing},
		QueueName:   "refresh-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	oldSrv.Stop()
	time.AfterFunc(defaultRetryBackoff/2, routing.switchToNext)

	if err := fe.Enqueue(context.Background(), []byte("rerouted")); err != nil {
		t.Fatalf("expected enqueue to succeed after resolver update, got %v", err)
	}

	if !routing.isSwitched() {
		t.Fatal("expected enqueue retry path to wait for resolver update")
	}
}

func TestFrontend_AckRejectsStaleTokenAfterResolverRefresh(t *testing.T) {
	store := newTestSQLiteStore(t)

	oldResolver := newStubResolver()
	oldSrv, oldAddr := startMatchingServerWithHandle(t, store, oldResolver, map[string]int{"ack-refresh-q": 1})

	newResolver := newStubResolver()
	_, newAddr := startMatchingServerWithHandle(t, store, newResolver, map[string]int{"ack-refresh-q": 1})

	routing := newRefreshAwareResolver(oldAddr, newAddr)
	fe, err := New(Config{
		Monitor:     &refreshAwareMonitor{resolver: routing},
		QueueName:   "ack-refresh-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()
	if err := fe.Enqueue(ctx, []byte("ack-me")); err != nil {
		t.Fatal(err)
	}
	_, token, err := fe.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	oldSrv.Stop()
	time.AfterFunc(defaultRetryBackoff/2, routing.switchToNext)

	if err := fe.Ack(ctx, token); err == nil {
		t.Fatal("expected ack with stale ownership token to fail")
	}

	if !routing.isSwitched() {
		t.Fatal("expected ack path to observe resolver update")
	}
}

func TestFrontend_NackRejectsStaleTokenAfterResolverRefresh(t *testing.T) {
	store := newTestSQLiteStore(t)

	oldResolver := newStubResolver()
	oldSrv, oldAddr := startMatchingServerWithHandle(t, store, oldResolver, map[string]int{"nack-refresh-q": 1})

	newResolver := newStubResolver()
	_, newAddr := startMatchingServerWithHandle(t, store, newResolver, map[string]int{"nack-refresh-q": 1})

	routing := newRefreshAwareResolver(oldAddr, newAddr)
	fe, err := New(Config{
		Monitor:     &refreshAwareMonitor{resolver: routing},
		QueueName:   "nack-refresh-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()
	if err := fe.Enqueue(ctx, []byte("nack-me")); err != nil {
		t.Fatal(err)
	}
	_, token, err := fe.Dequeue(ctx)
	if err != nil {
		t.Fatal(err)
	}

	oldSrv.Stop()
	time.AfterFunc(defaultRetryBackoff/2, routing.switchToNext)

	if err := fe.Nack(ctx, token); err == nil {
		t.Fatal("expected nack with stale ownership token to fail")
	}

	if !routing.isSwitched() {
		t.Fatal("expected nack path to observe resolver update")
	}
}

func TestFrontend_DequeueReturnsPromptlyWhenCallerContextCanceled(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver()
	startMatchingServer(t, store, resolver, map[string]int{"cancel-q": 4})

	fe, err := New(Config{
		Monitor:     &stubMonitor{resolver: resolver},
		QueueName:   "cancel-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, _, err = fe.Dequeue(ctx)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("expected dequeue to return promptly after caller cancellation, took %v", elapsed)
	}
}
