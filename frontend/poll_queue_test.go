package frontend

import (
	"context"
	"errors"
	"fmt"
	"net"
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

type countingPollServer struct {
	matching.UnimplementedMatchingServiceServer
	partitionCount int32
	pollQueueFast  atomic.Int64
	pollQueueLong  atomic.Int64
	pollTaskLong   atomic.Int64
}

func (s *countingPollServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	return &matching.GetQueueInfoResponse{NumPartitions: s.partitionCount}, nil
}

func (s *countingPollServer) PollTask(_ context.Context, request *matching.PollTaskRequest) (*matching.PollTaskResponse, error) {
	if request.PollTimeoutMs > 0 {
		s.pollTaskLong.Add(1)
	}
	return &matching.PollTaskResponse{Empty: true}, nil
}

func (s *countingPollServer) PollQueue(_ context.Context, request *matching.PollQueueRequest) (*matching.PollQueueResponse, error) {
	if request.PollTimeoutMs > 0 {
		s.pollQueueLong.Add(1)
	} else {
		s.pollQueueFast.Add(1)
	}
	return &matching.PollQueueResponse{Empty: true}, nil
}

func TestFrontend_DequeuePollsOncePerOwner(t *testing.T) {
	server := &countingPollServer{partitionCount: 64}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	matching.RegisterMatchingServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	resolver := newStubResolver(listener.Addr().String())
	frontend, err := New(Config{
		Monitor:     &stubMonitor{resolver: resolver},
		QueueName:   "owner-grouped-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close()

	_, _, err = frontend.Dequeue(context.Background())
	if !errors.Is(err, ErrNoTask) {
		t.Fatalf("dequeue error = %v, want ErrNoTask", err)
	}
	if got := server.pollQueueFast.Load(); got != 1 {
		t.Fatalf("fast PollQueue RPCs = %d, want 1 owner RPC", got)
	}
	if got := server.pollQueueLong.Load(); got != 1 {
		t.Fatalf("long PollQueue RPCs = %d, want 1 owner RPC", got)
	}
	if got := server.pollTaskLong.Load(); got != 0 {
		t.Fatalf("long PollTask fallback RPCs = %d, want 0", got)
	}
}

type recoveringPollServer struct {
	matching.UnimplementedMatchingServiceServer
	fastCalls atomic.Int64
	longCalls atomic.Int64
}

func (s *recoveringPollServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	return &matching.GetQueueInfoResponse{NumPartitions: 1}, nil
}

func (s *recoveringPollServer) PollQueue(_ context.Context, request *matching.PollQueueRequest) (*matching.PollQueueResponse, error) {
	if request.PollTimeoutMs == 0 {
		s.fastCalls.Add(1)
		return nil, status.Error(codes.Unavailable, "transient fast-scan failure")
	}
	s.longCalls.Add(1)
	return &matching.PollQueueResponse{Empty: true}, nil
}

func TestFrontend_LongPollSuccessClearsFastScanOwnerError(t *testing.T) {
	server := &recoveringPollServer{}
	address := startFrontendPollServer(t, server)
	frontend, err := New(Config{
		Monitor:     &stubMonitor{resolver: newStubResolver(address)},
		QueueName:   "recovering-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close()

	if _, _, err := frontend.Dequeue(context.Background()); !errors.Is(err, ErrNoTask) {
		t.Fatalf("dequeue error = %v, want ErrNoTask after owner recovery", err)
	}
	if server.fastCalls.Load() != 1 || server.longCalls.Load() != 1 {
		t.Fatalf("poll calls: fast=%d long=%d, want 1 each", server.fastCalls.Load(), server.longCalls.Load())
	}
}

type simultaneousLegacyPollServer struct {
	matching.UnimplementedMatchingServiceServer
	started          atomic.Int64
	nacks            atomic.Int64
	undeliveredNacks atomic.Int64
	ready            chan struct{}
	readyOne         sync.Once
}

func (s *simultaneousLegacyPollServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	return &matching.GetQueueInfoResponse{NumPartitions: 2}, nil
}

func (s *simultaneousLegacyPollServer) PollTask(ctx context.Context, request *matching.PollTaskRequest) (*matching.PollTaskResponse, error) {
	if request.PollTimeoutMs == 0 {
		return &matching.PollTaskResponse{Empty: true}, nil
	}
	if s.started.Add(1) == 2 {
		s.readyOne.Do(func() { close(s.ready) })
	}
	select {
	case <-s.ready:
		return &matching.PollTaskResponse{
			Data:  fmt.Appendf(nil, "partition-%d", request.PartitionId),
			Token: fmt.Sprintf("%d:1", request.PartitionId),
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *simultaneousLegacyPollServer) NackTask(_ context.Context, request *matching.NackTaskRequest) (*matching.NackTaskResponse, error) {
	s.nacks.Add(1)
	if request.Undelivered {
		s.undeliveredNacks.Add(1)
	}
	return &matching.NackTaskResponse{}, nil
}

func TestFrontend_LegacyFallbackReturnsSimultaneousLoser(t *testing.T) {
	server := &simultaneousLegacyPollServer{ready: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	matching.RegisterMatchingServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	resolver := newStubResolver(listener.Addr().String())
	frontend, err := New(Config{
		Monitor:     &stubMonitor{resolver: resolver},
		QueueName:   "legacy-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close()

	if _, _, err := frontend.Dequeue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := server.nacks.Load(); got != 1 {
		t.Fatalf("legacy fallback loser Nacks = %d, want 1", got)
	}
	// 败者从未交付给消费者，必须标记 undelivered，避免被计为一次消费失败。
	if got := server.undeliveredNacks.Load(); got != 1 {
		t.Fatalf("legacy fallback loser undelivered Nacks = %d, want 1", got)
	}
}

type simultaneousOwnerPollState struct {
	started          atomic.Int64
	nacks            atomic.Int64
	undeliveredNacks atomic.Int64
	ready            chan struct{}
	readyOne         sync.Once
}

type simultaneousOwnerPollServer struct {
	matching.UnimplementedMatchingServiceServer
	state *simultaneousOwnerPollState
}

func (s *simultaneousOwnerPollServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	return &matching.GetQueueInfoResponse{NumPartitions: 2}, nil
}

func (s *simultaneousOwnerPollServer) PollQueue(ctx context.Context, request *matching.PollQueueRequest) (*matching.PollQueueResponse, error) {
	if request.PollTimeoutMs == 0 {
		return &matching.PollQueueResponse{Empty: true}, nil
	}
	if s.state.started.Add(1) == 2 {
		s.state.readyOne.Do(func() { close(s.state.ready) })
	}
	select {
	case <-s.state.ready:
		partitionID := request.PartitionIds[0]
		return &matching.PollQueueResponse{
			PartitionId: partitionID,
			Data:        fmt.Appendf(nil, "partition-%d", partitionID),
			Token:       fmt.Sprintf("%d:1", partitionID),
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *simultaneousOwnerPollServer) NackTask(_ context.Context, request *matching.NackTaskRequest) (*matching.NackTaskResponse, error) {
	s.state.nacks.Add(1)
	if request.Undelivered {
		s.state.undeliveredNacks.Add(1)
	}
	return &matching.NackTaskResponse{}, nil
}

func TestFrontend_MultipleOwnersReturnSimultaneousLoser(t *testing.T) {
	state := &simultaneousOwnerPollState{ready: make(chan struct{})}
	firstAddress := startFrontendPollServer(t, &simultaneousOwnerPollServer{state: state})
	secondAddress := startFrontendPollServer(t, &simultaneousOwnerPollServer{state: state})
	resolver := &partitionResolver{
		defaultHost: membership.NewHostInfoFromAddress(firstAddress),
		partition1:  membership.NewHostInfoFromAddress(secondAddress),
		listeners:   make(map[string]chan<- *membership.ChangedEvent),
	}
	frontend, err := New(Config{
		Monitor:     &refreshAwareMonitor{resolver: resolver},
		QueueName:   "multi-owner-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close()

	if _, _, err := frontend.Dequeue(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 败者在后台读回并归还。非获胜 owner 的租约从未交付给消费者，必须标记 undelivered。
	awaitFrontendCondition(t, "loser lease returned as undelivered", func() bool {
		return state.undeliveredNacks.Load() == 1
	})
	if got := state.nacks.Load(); got != 1 {
		t.Fatalf("multi-owner loser Nacks = %d, want 1", got)
	}
}

func awaitFrontendCondition(t *testing.T, message string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", message)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func startFrontendPollServer(t *testing.T, server matching.MatchingServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	matching.RegisterMatchingServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}
