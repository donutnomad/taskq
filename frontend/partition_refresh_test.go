package frontend

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ---- 可切换 read/write 的 GetQueueInfo mock ----

type swappableInfoServer struct {
	matching.UnimplementedMatchingServiceServer
	read  atomic.Int32
	write atomic.Int32
}

func (s *swappableInfoServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	read := s.read.Load()
	write := s.write.Load()
	return &matching.GetQueueInfoResponse{
		NumPartitions:      read,
		NumReadPartitions:  read,
		NumWritePartitions: write,
	}, nil
}

// TestFrontend_SnapshotZeroGuard 验证快照未就绪/异常时兜底到安全下限，杜绝 `% 0` panic。
func TestFrontend_SnapshotZeroGuard(t *testing.T) {
	f := &Frontend{} // partitions 指针为 nil（未初始化）

	snap := f.snapshot()
	if snap.read != minPartitions || snap.write != minPartitions {
		t.Fatalf("nil-snapshot fallback = %+v, want read/write = %d", snap, minPartitions)
	}
	// 取模不 panic 即证明兜底生效。
	_ = 1 % snap.write
	_ = make([]int, 0, snap.read)

	// 快照显式存 0 也必须被兜底。
	f.partitions.Store(&partitionSnapshot{read: 0, write: 0})
	snap = f.snapshot()
	if snap.read != minPartitions || snap.write != minPartitions {
		t.Fatalf("zero-snapshot fallback = %+v, want read/write = %d", snap, minPartitions)
	}
}

// partitionRoutingRecorder 记录 AddTask 落到的写分区与 PollQueue 枚举的读分区。
type partitionRoutingRecorder struct {
	matching.UnimplementedMatchingServiceServer
	read  int32
	write int32
	mu    sync.Mutex
	added map[int32]bool
	polls map[int32]bool
}

func (s *partitionRoutingRecorder) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	return &matching.GetQueueInfoResponse{
		NumPartitions:      s.read,
		NumReadPartitions:  s.read,
		NumWritePartitions: s.write,
	}, nil
}

func (s *partitionRoutingRecorder) AddTask(_ context.Context, req *matching.AddTaskRequest) (*matching.AddTaskResponse, error) {
	s.mu.Lock()
	s.added[req.PartitionId] = true
	s.mu.Unlock()
	return &matching.AddTaskResponse{}, nil
}

func (s *partitionRoutingRecorder) PollQueue(_ context.Context, req *matching.PollQueueRequest) (*matching.PollQueueResponse, error) {
	s.mu.Lock()
	for _, id := range req.PartitionIds {
		s.polls[id] = true
	}
	s.mu.Unlock()
	return &matching.PollQueueResponse{Empty: true}, nil
}

// TestFrontend_SnapshotSeparatesReadAndWrite 造 read>write 的态（read=4,write=2），
// 断言 Enqueue 只落 [0,write)、Dequeue 枚举覆盖 [0,read)。
func TestFrontend_SnapshotSeparatesReadAndWrite(t *testing.T) {
	server := &partitionRoutingRecorder{
		read:  4,
		write: 2,
		added: make(map[int32]bool),
		polls: make(map[int32]bool),
	}
	addr := startFrontendPollServer(t, server)

	fe, err := New(Config{
		Monitor:     &stubMonitor{resolver: newStubResolver(addr)},
		QueueName:   "rw-split-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	ctx := context.Background()
	for range 20 {
		if err := fe.Enqueue(ctx, []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}

	// 空队列长轮询后应返回 ErrNoTask，期间枚举全部读分区。
	if _, _, err := fe.Dequeue(ctx); !errors.Is(err, ErrNoTask) {
		t.Fatalf("dequeue error = %v, want ErrNoTask", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	// 写只落 [0,2)：绝不出现分区 2、3。
	for id := range server.added {
		if id >= server.write {
			t.Fatalf("enqueue landed on partition %d, want only [0,%d)", id, server.write)
		}
	}
	if !server.added[0] || !server.added[1] {
		t.Fatalf("write partitions covered = %v, want both 0 and 1", server.added)
	}

	// 读覆盖 [0,4)：四个分区都被枚举。
	for id := int32(0); id < server.read; id++ {
		if !server.polls[id] {
			t.Fatalf("dequeue did not poll partition %d; polled = %v", id, server.polls)
		}
	}
}

// TestFrontend_PeriodicRefreshUpdatesSnapshot 改变 matching 上报的 read/write 后，
// frontend 应在一个刷新周期内感知新值。
func TestFrontend_PeriodicRefreshUpdatesSnapshot(t *testing.T) {
	server := &swappableInfoServer{}
	server.read.Store(4)
	server.write.Store(4)
	addr := startFrontendPollServer(t, server)

	fe, err := New(Config{
		Monitor:     &stubMonitor{resolver: newStubResolver(addr)},
		QueueName:   "refresh-tick-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	if snap := fe.snapshot(); snap.read != 4 || snap.write != 4 {
		t.Fatalf("initial snapshot = %+v, want 4/4", snap)
	}

	// 上报扩容后的新值。
	server.read.Store(8)
	server.write.Store(6)

	// 等待周期定时器刷新（<= 一个刷新周期 + 余量）。
	deadline := time.Now().Add(partitionRefreshInterval + 3*time.Second)
	for {
		snap := fe.snapshot()
		if snap.read == 8 && snap.write == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot not refreshed within one cycle: got %+v, want 8/6", snap)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestFrontend_MembershipChangeRefreshesSnapshot 成员变更时应立即刷新快照。
func TestFrontend_MembershipChangeRefreshesSnapshot(t *testing.T) {
	server := &swappableInfoServer{}
	server.read.Store(4)
	server.write.Store(4)
	addr := startFrontendPollServer(t, server)

	resolver := newStubResolver(addr)
	fe, err := New(Config{
		Monitor:     &stubMonitor{resolver: resolver},
		QueueName:   "refresh-event-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	server.read.Store(5)
	server.write.Store(5)

	// 直接向 frontend 的 listener 通道推送成员变更事件，触发即时刷新。
	var ch chan<- *membership.ChangedEvent
	for _, c := range resolver.listeners {
		ch = c
	}
	if ch == nil {
		t.Fatal("frontend did not register a membership listener")
	}
	ch <- &membership.ChangedEvent{}

	deadline := time.Now().Add(2 * time.Second)
	for {
		snap := fe.snapshot()
		if snap.read == 5 && snap.write == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot not refreshed after membership change: got %+v, want 5/5", snap)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// shrinkWriteServer 模拟缩容态：真实 write=realWrite，首次高位写被 FailedPrecondition 拒绝，
// 并将后续 GetQueueInfo 上报的 write 收窄，验证 frontend 重试前刷新快照。
type shrinkWriteServer struct {
	matching.UnimplementedMatchingServiceServer
	realWrite     int32
	reportedRead  int32
	reportedWrite atomic.Int32
	mu            sync.Mutex
	attempts      []int32
}

func (s *shrinkWriteServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	write := s.reportedWrite.Load()
	return &matching.GetQueueInfoResponse{
		NumPartitions:      s.reportedRead,
		NumReadPartitions:  s.reportedRead,
		NumWritePartitions: write,
	}, nil
}

func (s *shrinkWriteServer) AddTask(_ context.Context, req *matching.AddTaskRequest) (*matching.AddTaskResponse, error) {
	s.mu.Lock()
	s.attempts = append(s.attempts, req.PartitionId)
	s.mu.Unlock()
	if req.PartitionId >= s.realWrite {
		// 缩容后真实 write 已收窄：暴露给下一次刷新，并硬拒该高位写。
		s.reportedWrite.Store(s.realWrite)
		return nil, status.Error(codes.FailedPrecondition, "partition not writable")
	}
	return &matching.AddTaskResponse{}, nil
}

// TestFrontend_EnqueueRefreshesSnapshotOnFailedPrecondition 验证：高位写被 matching 拒
// FailedPrecondition 后，frontend 先刷新快照（write 收窄）再重选分区，最终落到合法分区成功。
func TestFrontend_EnqueueRefreshesSnapshotOnFailedPrecondition(t *testing.T) {
	server := &shrinkWriteServer{realWrite: 2, reportedRead: 4}
	server.reportedWrite.Store(4) // 初始快照仍是旧的大 write
	addr := startFrontendPollServer(t, server)

	fe, err := New(Config{
		Monitor:     &stubMonitor{resolver: newStubResolver(addr)},
		QueueName:   "shrink-write-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fe.Close()

	if snap := fe.snapshot(); snap.write != 4 {
		t.Fatalf("initial snapshot write = %d, want 4", snap.write)
	}

	// 播种索引，使首次选到高位分区 2（2 % write(4) = 2 >= realWrite(2)）。
	fe.enqueueIdx.Store(1)

	if err := fe.Enqueue(context.Background(), []byte("late-write")); err != nil {
		t.Fatalf("enqueue after refresh should succeed, got %v", err)
	}

	// 刷新后快照 write 应收窄到 2。
	if snap := fe.snapshot(); snap.write != 2 {
		t.Fatalf("snapshot write after refresh = %d, want 2", snap.write)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.attempts) < 2 {
		t.Fatalf("AddTask attempts = %v, want a rejected high-partition write then a valid retry", server.attempts)
	}
	if server.attempts[0] < server.realWrite {
		t.Fatalf("first attempt partition = %d, want a high partition >= %d", server.attempts[0], server.realWrite)
	}
	last := server.attempts[len(server.attempts)-1]
	if last >= server.realWrite {
		t.Fatalf("final accepted partition = %d, want a valid partition < %d", last, server.realWrite)
	}
}
