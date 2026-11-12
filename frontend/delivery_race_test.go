package frontend

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// lateLeaseServer 在调用方已经离开之后才签发租约：wait 返回后无视自身 ctx 应答一个租约，模拟 owner 在应答前的
// ctx 检查通过、应答还在途中时调用方离开。
type lateLeaseServer struct {
	matching.UnimplementedMatchingServiceServer
	wait        func(ctx context.Context, request *matching.PollQueueRequest)
	canceledIDs chan string
	returned    chan string
}

func newLateLeaseServer(wait func(context.Context, *matching.PollQueueRequest)) *lateLeaseServer {
	return &lateLeaseServer{wait: wait, canceledIDs: make(chan string, 4), returned: make(chan string, 4)}
}

func (s *lateLeaseServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	return &matching.GetQueueInfoResponse{NumPartitions: 1}, nil
}

func (s *lateLeaseServer) PollQueue(ctx context.Context, request *matching.PollQueueRequest) (*matching.PollQueueResponse, error) {
	if request.PollTimeoutMs == 0 {
		return &matching.PollQueueResponse{Empty: true}, nil
	}
	s.wait(ctx, request)
	return &matching.PollQueueResponse{PartitionId: 0, Data: []byte("late"), Token: "0:1"}, nil
}

func (s *lateLeaseServer) CancelPoll(_ context.Context, request *matching.CancelPollRequest) (*matching.CancelPollResponse, error) {
	s.canceledIDs <- request.PollId
	return &matching.CancelPollResponse{}, nil
}

func (s *lateLeaseServer) NackTask(_ context.Context, request *matching.NackTaskRequest) (*matching.NackTaskResponse, error) {
	if request.Undelivered {
		s.returned <- request.Token
	}
	return &matching.NackTaskResponse{}, nil
}

func newLateLeaseFrontend(t *testing.T, server *lateLeaseServer) *Frontend {
	t.Helper()
	address := startFrontendPollServer(t, server)
	frontend, err := New(Config{
		Monitor:     &stubMonitor{resolver: newStubResolver(address)},
		QueueName:   "late-lease-q",
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(frontend.Close)
	return frontend
}

func awaitReturnedLease(t *testing.T, server *lateLeaseServer) {
	t.Helper()
	select {
	case token := <-server.returned:
		if token != "0:1" {
			t.Fatalf("returned token = %q, want 0:1", token)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lease issued after the caller left was never returned; it stays hidden until the visibility timeout")
	}
}

// 调用方的期限先于 owner 的应答到期：owner 已签发的租约必须被读回并立即以 undelivered 归还。
func TestFrontend_LeaseIssuedAfterCallerDeadlineIsReturned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	callerDeadline, _ := ctx.Deadline()
	server := newLateLeaseServer(func(context.Context, *matching.PollQueueRequest) {
		time.Sleep(time.Until(callerDeadline) + 50*time.Millisecond)
	})
	frontend := newLateLeaseFrontend(t, server)

	if _, err := frontend.DequeueWithOptions(ctx, PollOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dequeue error = %v, want deadline exceeded", err)
	}
	awaitReturnedLease(t, server)
}

// 调用方取消：frontend 用 CancelPoll 请 owner 结束轮询，并读回与取消同时签发的租约归还。
func TestFrontend_LeaseIssuedWhileCallerCancelsIsReturned(t *testing.T) {
	var canceled atomic.Bool
	release := make(chan struct{})
	server := newLateLeaseServer(func(ctx context.Context, _ *matching.PollQueueRequest) {
		select {
		case <-ctx.Done():
		case <-release:
		}
	})
	frontend := newLateLeaseFrontend(t, server)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		canceled.Store(true)
		cancel()
	}()
	go func() {
		<-server.canceledIDs
		close(release)
	}()
	if _, err := frontend.DequeueWithOptions(ctx, PollOptions{}); !errors.Is(err, context.Canceled) || !canceled.Load() {
		t.Fatalf("dequeue error = %v, want canceled", err)
	}
	awaitReturnedLease(t, server)
}

// 分区 owner 宕机（成员表尚未剔除）时，写入换到其他 owner 的分区，而不是在同一个分区上耗尽重试。
func TestFrontend_EnqueueFailsOverToAnotherOwner(t *testing.T) {
	live := &partitionRoutingRecorder{read: 2, write: 2, added: make(map[int32]bool), polls: make(map[int32]bool)}
	liveAddress := startFrontendPollServer(t, live)
	resolver := &partitionResolver{
		defaultHost: membership.NewHostInfoFromAddress(liveAddress),
		partition1:  membership.NewHostInfoFromAddress(unusedFrontendAddress(t)),
		listeners:   make(map[string]chan<- *membership.ChangedEvent),
	}
	frontend, err := New(Config{
		Monitor:   &refreshAwareMonitor{resolver: resolver},
		QueueName: "failover-q",
		DialOpts:  []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontend.Close()

	for i := range 4 {
		if err := frontend.Enqueue(context.Background(), []byte("task")); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	live.mu.Lock()
	defer live.mu.Unlock()
	if len(live.added) != 1 || !live.added[0] {
		t.Fatalf("written partitions = %v, want only the live owner's partition 0", live.added)
	}
}
