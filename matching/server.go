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
	polls      pollRegistry
}

func (h *matchingHandler) AddTask(ctx context.Context, req *AddTaskRequest) (*AddTaskResponse, error) {
	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId), accessWrite)
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
	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId), accessRead)
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
		// 调用方在投递瞬间取消：任务从未交到消费者手里，原地归还、不消耗投递次数。
		if cleanupErr := target.ReturnLease(cleanupCtx, lease.Token); cleanupErr != nil {
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
	// 缩容退役分区的迟到 Ack：该分区在减 read 之前已被确认排空，此 Ack 必为重复请求，
	// 幂等成功，避免消费者收到不可重试的 InvalidArgument（见 isRetiredReadPartition）。
	// 配置不可知时不做幂等判定，返回 Unavailable 让消费者重试。
	retired, err := h.mgr.isRetiredReadPartition(req.QueueName, partID)
	if err != nil {
		return nil, toGRPCError(err)
	}
	if retired {
		return &AckTaskResponse{}, nil
	}

	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId), accessRead)
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
	// 与 AckTask 同理：退役分区已确认排空，迟到的 Nack/归还只可能是重复请求，幂等成功。
	retired, err := h.mgr.isRetiredReadPartition(req.QueueName, partID)
	if err != nil {
		return nil, toGRPCError(err)
	}
	if retired {
		return &NackTaskResponse{}, nil
	}

	pq, err := h.mgr.getOrLoadPartition(ctx, req.QueueName, int(req.PartitionId), accessRead)
	if err != nil {
		return nil, toGRPCError(err)
	}
	// Undelivered 表示任务从未交付给消费者（调用方只是归还一个没用上的租约），
	// 这类归还不算消费失败，必须原地归还而不是消耗一次投递机会。
	release := pq.Nack
	if req.Undelivered {
		release = pq.ReturnLease
	}
	if err := release(ctx, req.Token); err != nil {
		if errors.Is(err, queue.ErrTaskNotOutstanding) {
			return &NackTaskResponse{}, nil
		}
		return nil, toGRPCError(err)
	}
	return &NackTaskResponse{}, nil
}

func (h *matchingHandler) GetQueueInfo(_ context.Context, req *GetQueueInfoRequest) (*GetQueueInfoResponse, error) {
	// 答案来自 DB 配置表（resolve：override ?? 全局默认，不物化），任何节点可读。
	// num_partitions 保留为 read 供未改造的 frontend 继续工作。
	// 配置不可知时返回 Unavailable：frontend 刷新失败会保留旧快照，好过拿到一个
	// 可能被放大的猜测值去路由写入。
	read, write, err := h.mgr.resolvePartitions(req.QueueName)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &GetQueueInfoResponse{
		NumPartitions:      int32(read),
		NumReadPartitions:  int32(read),
		NumWritePartitions: int32(write),
	}, nil
}

// toGRPCError 将内部错误转换为 gRPC status error
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotOwner) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, ErrPartitionNotWritable) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	// 分区配置暂时不可知（DB 抖动且本节点无缓存）：暂时性故障，可重试。
	if errors.Is(err, ErrPartitionConfigUnavailable) {
		return status.Error(codes.Unavailable, err.Error())
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
