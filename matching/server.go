package matching

import (
	"context"
	"errors"
	"time"

	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// matchingHandler 实现 MatchingServiceServer gRPC 接口
type matchingHandler struct {
	UnimplementedMatchingServiceServer
	mgr *partitionManager
}

func (h *matchingHandler) AddTask(ctx context.Context, req *AddTaskRequest) (*AddTaskResponse, error) {
	pq, err := h.mgr.getOrLoadPartition(req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}
	if err := pq.AddTask(ctx, req.Data); err != nil {
		return nil, toGRPCError(err)
	}
	return &AddTaskResponse{}, nil
}

func (h *matchingHandler) PollTask(ctx context.Context, req *PollTaskRequest) (*PollTaskResponse, error) {
	pq, err := h.mgr.getOrLoadPartition(req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}

	// 1. 非阻塞尝试
	if row, token, ok := pq.TryPoll(ctx); ok {
		return &PollTaskResponse{Data: row.Data, Token: token}, nil
	}

	// 2. 无超时，立即返回空
	if req.PollTimeoutMs <= 0 {
		return &PollTaskResponse{Empty: true}, nil
	}

	// 3. 长轮询：注册 waiter，等待任务或超时
	ch := make(chan queue.PollResult, 1)
	elem := pq.RegisterWaiter(ch)
	defer pq.RemoveWaiter(elem)

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(req.PollTimeoutMs)*time.Millisecond)
	defer cancel()

	select {
	case result := <-ch:
		if result.Err != nil {
			return nil, toGRPCError(result.Err)
		}
		return &PollTaskResponse{Data: result.Row.Data, Token: result.Token}, nil
	case <-timeoutCtx.Done():
		// 区分是调用方取消还是轮询超时
		if ctx.Err() != nil {
			return nil, status.Error(codes.Canceled, ctx.Err().Error())
		}
		return &PollTaskResponse{Empty: true}, nil
	}
}

func (h *matchingHandler) AckTask(ctx context.Context, req *AckTaskRequest) (*AckTaskResponse, error) {
	pq, err := h.mgr.getOrLoadPartition(req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}
	if err := pq.Ack(ctx, req.Token); err != nil {
		return nil, toGRPCError(err)
	}
	return &AckTaskResponse{}, nil
}

func (h *matchingHandler) NackTask(ctx context.Context, req *NackTaskRequest) (*NackTaskResponse, error) {
	pq, err := h.mgr.getOrLoadPartition(req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}
	if err := pq.Nack(ctx, req.Token); err != nil {
		return nil, toGRPCError(err)
	}
	return &NackTaskResponse{}, nil
}

func (h *matchingHandler) GetQueueInfo(_ context.Context, req *GetQueueInfoRequest) (*GetQueueInfoResponse, error) {
	n, ok := h.mgr.getQueuePartitionCount(req.QueueName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "queue %q not found", req.QueueName)
	}
	return &GetQueueInfoResponse{NumPartitions: int32(n)}, nil
}

// toGRPCError 将内部错误转换为 gRPC status error
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotOwner) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, queue.ErrOwnershipLost) {
		return status.Error(codes.Aborted, err.Error())
	}
	if errors.Is(err, queue.ErrQueueClosed) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
