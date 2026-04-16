package matching

import (
	"context"
	"net"
	"testing"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---- 测试用 mock ----

// stubResolver 是一个简单的 ServiceResolver mock
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
	// 简单哈希：按 key 长度 mod 成员数
	idx := len(key) % len(r.members)
	return r.members[idx], nil
}
func (r *stubResolver) LookupN(key string, n int) []membership.HostInfo { return nil }
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

// stubMonitor 是一个简单的 Monitor mock
type stubMonitor struct {
	resolver *stubResolver
}

func (m *stubMonitor) Start()                                       {}
func (m *stubMonitor) Stop()                                        {}
func (m *stubMonitor) EvictSelf() error                             { return nil }
func (m *stubMonitor) EvictSelfAt(time.Time) (time.Duration, error) { return 0, nil }
func (m *stubMonitor) GetResolver(service string) (membership.ServiceResolver, error) {
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

// startTestServer 启动一个测试用的 matching gRPC server，返回监听地址
func startTestServer(t *testing.T, store queue.Store, resolver *stubResolver, broadcastAddr string, queueConfigs map[string]int) (*Server, string) {
	t.Helper()

	monitor := &stubMonitor{resolver: resolver}

	// 随机端口
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()

	srv, err := NewServer(Config{
		Store:         store,
		Monitor:       monitor,
		ListenAddr:    addr,
		BroadcastAddr: broadcastAddr,
		QueueConfigs:  queueConfigs,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	// 等 gRPC server 就绪
	time.Sleep(50 * time.Millisecond)

	return srv, addr
}

func newTestClient(t *testing.T, addr string) MatchingServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return NewMatchingServiceClient(conn)
}

// ---- 测试 ----

func TestMatchingServer_AddAndPoll(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 4})
	// 更新 resolver 使其指向真实地址
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)

	// 写入任务
	ctx := context.Background()
	_, err := client.AddTask(ctx, &AddTaskRequest{
		QueueName:   "test-q",
		PartitionId: 0,
		Data:        []byte("hello-world"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// 拉取任务
	resp, err := client.PollTask(ctx, &PollTaskRequest{
		QueueName:   "test-q",
		PartitionId: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Empty {
		t.Fatal("expected task, got empty")
	}
	if string(resp.Data) != "hello-world" {
		t.Fatalf("expected hello-world, got %s", resp.Data)
	}
	if resp.Token == "" {
		t.Fatal("expected non-empty token")
	}
}

func TestMatchingServer_PollEmpty(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 4})
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)

	resp, err := client.PollTask(context.Background(), &PollTaskRequest{
		QueueName:   "test-q",
		PartitionId: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Empty {
		t.Fatal("expected empty")
	}
}

func TestMatchingServer_AckTask(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 4})
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)
	ctx := context.Background()

	client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("ack-me")})
	resp, _ := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 0})

	_, err := client.AckTask(ctx, &AckTaskRequest{
		QueueName:   "test-q",
		PartitionId: 0,
		Token:       resp.Token,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMatchingServer_NackRedelivery(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 4})
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)
	ctx := context.Background()

	client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("nack-me")})
	resp, _ := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 0})

	// nack → 任务应重投递
	_, err := client.NackTask(ctx, &NackTaskRequest{
		QueueName:   "test-q",
		PartitionId: 0,
		Token:       resp.Token,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 再次 poll 应该拿到同一个任务
	resp2, err := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 0})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Empty {
		t.Fatal("expected redelivered task")
	}
	if string(resp2.Data) != "nack-me" {
		t.Fatalf("expected nack-me, got %s", resp2.Data)
	}
}

func TestMatchingServer_LongPoll(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 4})
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)
	ctx := context.Background()

	done := make(chan struct{})
	var resp *PollTaskResponse
	var pollErr error

	// 在后台长轮询
	go func() {
		resp, pollErr = client.PollTask(ctx, &PollTaskRequest{
			QueueName:     "test-q",
			PartitionId:   0,
			PollTimeoutMs: 5000,
		})
		close(done)
	}()

	// 100ms 后写入任务
	time.Sleep(100 * time.Millisecond)
	client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("delayed")})

	select {
	case <-done:
		if pollErr != nil {
			t.Fatal(pollErr)
		}
		if resp.Empty {
			t.Fatal("expected task from long poll")
		}
		if string(resp.Data) != "delayed" {
			t.Fatalf("expected delayed, got %s", resp.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not return in time")
	}
}

func TestMatchingServer_StopUnblocksInFlightLongPoll(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	srv, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 1})
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)

	pollDone := make(chan error, 1)
	go func() {
		_, err := client.PollTask(context.Background(), &PollTaskRequest{
			QueueName:     "test-q",
			PartitionId:   0,
			PollTimeoutMs: 30000,
		})
		pollDone <- err
	}()

	time.Sleep(100 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(1500 * time.Millisecond):
		srv.grpcServer.Stop()
		srv.mgr.close()
		t.Fatal("server stop blocked while a long poll was in flight")
	}

	select {
	case err := <-pollDone:
		if err == nil {
			t.Fatal("expected in-flight long poll to be interrupted when server stops")
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("long poll did not exit after server stop")
	}
}

func TestMatchingServer_AckTaskRejectsTokenPartitionMismatch(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)
	ctx := context.Background()

	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("p0")}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 1, Data: []byte("p1")}); err != nil {
		t.Fatal(err)
	}

	resp0, err := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 1}); err != nil {
		t.Fatal(err)
	}

	_, err = client.AckTask(ctx, &AckTaskRequest{
		QueueName:   "test-q",
		PartitionId: 1,
		Token:       resp0.Token,
	})
	if err == nil {
		t.Fatal("expected partition-mismatched ack token to be rejected")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for partition-mismatched ack token, got %v", err)
	}
}

func TestMatchingServer_NackTaskRejectsTokenPartitionMismatch(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:9999", map[string]int{"test-q": 2})
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress(addr)}

	client := newTestClient(t, addr)
	ctx := context.Background()

	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 0, Data: []byte("p0")}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddTask(ctx, &AddTaskRequest{QueueName: "test-q", PartitionId: 1, Data: []byte("p1")}); err != nil {
		t.Fatal(err)
	}

	resp0, err := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PollTask(ctx, &PollTaskRequest{QueueName: "test-q", PartitionId: 1}); err != nil {
		t.Fatal(err)
	}

	_, err = client.NackTask(ctx, &NackTaskRequest{
		QueueName:   "test-q",
		PartitionId: 1,
		Token:       resp0.Token,
	})
	if err == nil {
		t.Fatal("expected partition-mismatched nack token to be rejected")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for partition-mismatched nack token, got %v", err)
	}
}

func TestPartitionManager_ReloadsClosedPartition(t *testing.T) {
	store := newTestSQLiteStore(t)
	resolver := newStubResolver("127.0.0.1:9999")
	resolver.members = []membership.HostInfo{membership.NewHostInfoFromAddress("127.0.0.1:9999")}

	pm := newPartitionManager(
		context.Background(),
		store,
		store.(dbleader.LockStore),
		"127.0.0.1:9999",
		"lease-owner",
		resolver,
		time.Minute,
		map[string]int{"test-q": 1},
	)
	defer pm.close()

	pq1, err := pm.getOrLoadPartition(context.Background(), "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	pq1.close()

	pq2, err := pm.getOrLoadPartition(context.Background(), "test-q", 0)
	if err != nil {
		t.Fatal(err)
	}
	if pq1 == pq2 {
		t.Fatal("expected closed partition instance to be discarded and reloaded")
	}
}

func TestMatchingServer_NotOwner(t *testing.T) {
	store := newTestSQLiteStore(t)
	// resolver 指向一个不同的地址（不是本节点）
	resolver := newStubResolver("10.0.0.99:9999")

	_, addr := startTestServer(t, store, resolver, "127.0.0.1:7777", map[string]int{"test-q": 4})

	client := newTestClient(t, addr)

	_, err := client.AddTask(context.Background(), &AddTaskRequest{
		QueueName:   "test-q",
		PartitionId: 0,
		Data:        []byte("should-fail"),
	})
	if err == nil {
		t.Fatal("expected error for non-owner")
	}

	s, ok := status.FromError(err)
	if !ok || s.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}
