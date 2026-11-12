package frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/taskq/matching"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// handoffServer 模拟分区换主期间的 owner：每次 Ack、Nack、AddTask 都以可重试的 code 失败。
type handoffServer struct {
	matching.UnimplementedMatchingServiceServer
	code codes.Code
}

func (s *handoffServer) GetQueueInfo(context.Context, *matching.GetQueueInfoRequest) (*matching.GetQueueInfoResponse, error) {
	return &matching.GetQueueInfoResponse{NumPartitions: 1, NumReadPartitions: 1, NumWritePartitions: 1}, nil
}

func (s *handoffServer) AckTask(context.Context, *matching.AckTaskRequest) (*matching.AckTaskResponse, error) {
	return nil, status.Error(s.code, "partition is moving")
}

func (s *handoffServer) NackTask(context.Context, *matching.NackTaskRequest) (*matching.NackTaskResponse, error) {
	return nil, status.Error(s.code, "partition is moving")
}

func (s *handoffServer) AddTask(context.Context, *matching.AddTaskRequest) (*matching.AddTaskResponse, error) {
	return nil, status.Error(s.code, "partition is moving")
}

// 重试次数用完时错误链上有 ErrRetriesExhausted 与最后一次的 gRPC 错误，调用方据此把它当作暂时性失败。
// 此前 Ack 与 Nack 只返回不带原因的 "max retries exceeded" 文本。
func TestFrontend_RetriesExhaustedKeepsLastError(t *testing.T) {
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.Unavailable, codes.Aborted} {
		t.Run(code.String(), func(t *testing.T) {
			addr := startFrontendPollServer(t, &handoffServer{code: code})
			fe, err := New(Config{
				Monitor:     &stubMonitor{resolver: newStubResolver(addr)},
				QueueName:   "handoff-q",
				DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
				PollTimeout: 100 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer fe.Close()

			ctx := context.Background()
			for name, call := range map[string]func() error{
				"ack":     func() error { return fe.Ack(ctx, "0:1") },
				"nack":    func() error { return fe.Nack(ctx, "0:1") },
				"enqueue": func() error { return fe.Enqueue(ctx, []byte("payload")) },
			} {
				err := call()
				if !errors.Is(err, ErrRetriesExhausted) {
					t.Fatalf("%s error = %v, want ErrRetriesExhausted", name, err)
				}
				if status.Code(err) != code {
					t.Fatalf("%s error = %v, want the last %s status in the chain", name, err, code)
				}
			}
		})
	}
}
