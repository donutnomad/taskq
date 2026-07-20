package matching

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// matchingHandler 实现 MatchingServiceServer gRPC 接口
type matchingHandler struct {
	UnimplementedMatchingServiceServer
	mgr        *partitionManager
	pollCursor atomic.Uint64
}

func (h *matchingHandler) AddTask(ctx context.Context, req *AddTaskRequest) (*AddTaskResponse, error) {
	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}
	deadline, err := routingDeadline(req.RoutingDeadline)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := pq.AddTaskWithOptions(ctx, req.Data, queue.EnqueueOptions{
		RoutingKey: req.RoutingKey, RoutingDeadline: deadline,
	}); err != nil {
		return nil, toGRPCError(err)
	}
	return &AddTaskResponse{}, nil
}

func (h *matchingHandler) PollTask(ctx context.Context, req *PollTaskRequest) (*PollTaskResponse, error) {
	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}

	opts := queue.PollOptions{AcceptedRoutingKeys: req.AcceptedRoutingKeys}

	// 1. 非阻塞尝试
	if lease, ok, err := pq.TryPollWithOptions(ctx, opts); err != nil {
		return nil, toGRPCError(err)
	} else if ok {
		return finishPollTask(ctx, pq, lease)
	}

	// 2. 无超时，立即返回空
	if req.PollTimeoutMs <= 0 {
		return &PollTaskResponse{Empty: true}, nil
	}

	// 3. 长轮询：注册 waiter，等待任务或超时
	waiter, err := pq.RegisterWaiterWithOptions(opts)
	if err != nil {
		return nil, toGRPCError(err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(req.PollTimeoutMs)*time.Millisecond)
	defer cancel()

	select {
	case <-waiter.Ready():
		result, claimed := waiter.Claim()
		if !claimed {
			return nil, status.Error(codes.Aborted, "poll delivery claim lost")
		}
		if result.Err != nil {
			return nil, toGRPCError(result.Err)
		}
		return finishPollTask(ctx, pq, result.Lease())
	case <-timeoutCtx.Done():
		if !waiter.Cancel() {
			result, claimed := waiter.Claim()
			if !claimed {
				return nil, status.Error(codes.Aborted, "poll delivery claim lost")
			}
			if result.Err != nil {
				return nil, toGRPCError(result.Err)
			}
			return finishPollTask(ctx, pq, result.Lease())
		}
		// 区分是调用方取消还是轮询超时
		if ctx.Err() != nil {
			return nil, status.Error(codes.Canceled, ctx.Err().Error())
		}
		return &PollTaskResponse{Empty: true}, nil
	}
}

func finishPollTask(ctx context.Context, target *partitionHandle, lease queue.Lease) (*PollTaskResponse, error) {
	if err := ctx.Err(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), pollCleanupTimeout)
		defer cancel()
		if cleanupErr := target.Nack(cleanupCtx, lease.Token); cleanupErr != nil {
			return nil, status.Errorf(codes.Canceled, "%v; return canceled poll lease: %v", err, cleanupErr)
		}
		return nil, status.Error(codes.Canceled, err.Error())
	}
	return pollTaskResponse(lease), nil
}

func pollTaskResponse(lease queue.Lease) *PollTaskResponse {
	response := &PollTaskResponse{
		Data:            lease.Data,
		Token:           lease.Token,
		RoutingKey:      lease.RoutingKey,
		RoutingFallback: lease.RoutingFallback,
	}
	if !lease.RoutingDeadline.IsZero() {
		response.RoutingDeadline = timestamppb.New(lease.RoutingDeadline)
	}
	return response
}

func routingDeadline(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil {
		return time.Time{}, nil
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, err
	}
	return value.AsTime(), nil
}

func (h *matchingHandler) AckTask(ctx context.Context, req *AckTaskRequest) (*AckTaskResponse, error) {
	partID, _, err := queue.ParseToken(req.Token)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if partID != int(req.PartitionId) {
		return nil, status.Errorf(codes.InvalidArgument, "token partition %d does not match request partition %d", partID, req.PartitionId)
	}

	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}
	if err := pq.Ack(ctx, req.Token); err != nil {
		if errors.Is(err, queue.ErrTaskNotOutstanding) {
			return &AckTaskResponse{}, nil
		}
		return nil, toGRPCError(err)
	}
	return &AckTaskResponse{}, nil
}

func (h *matchingHandler) NackTask(ctx context.Context, req *NackTaskRequest) (*NackTaskResponse, error) {
	partID, _, err := queue.ParseToken(req.Token)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if partID != int(req.PartitionId) {
		return nil, status.Errorf(codes.InvalidArgument, "token partition %d does not match request partition %d", partID, req.PartitionId)
	}

	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId))
	if err != nil {
		return nil, toGRPCError(err)
	}
	if err := pq.Nack(ctx, req.Token); err != nil {
		if errors.Is(err, queue.ErrTaskNotOutstanding) {
			return &NackTaskResponse{}, nil
		}
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
	if errors.Is(err, ErrUnknownQueue) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, ErrInvalidPartition) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, queue.ErrOwnershipLost) {
		return status.Error(codes.Aborted, err.Error())
	}
	if errors.Is(err, queue.ErrStaleToken) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, queue.ErrInvalidRoutingKey) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, queue.ErrInvalidRoutingDeadline) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, queue.ErrQueueClosed) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
