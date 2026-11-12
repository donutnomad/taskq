package matching

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	pollCleanupTimeout = time.Second
	// pollCancelTombstoneTTL 是 CancelPoll 先于对应 PollQueue 到达时墓碑的保留时长。两者走同一条连接，
	// 先后颠倒只来自 handler goroutine 的调度，墓碑在对应 PollQueue 到达时即被消费；保留一分钟只为
	// 清理那些对应 PollQueue 已经结束或从未到达的墓碑。
	pollCancelTombstoneTTL = time.Minute
)

// pollRegistry 记录进行中的、带 poll_id 的 PollQueue，供 CancelPoll 协作式地结束它们。
//
// 调用方取消 RPC 时，owner 只能在应答之前检查 ctx：检查通过后、应答到达调用方之前的取消会让调用方丢掉
// 应答，应答里的租约隐身到可见性超时。CancelPoll 让调用方不取消 RPC 也能结束轮询：owner 把已领取的
// 租约原地归还，以 empty 应答；在这之前已发出的租约照常被调用方读到，由调用方归还。
type pollRegistry struct {
	mu        sync.Mutex
	active    map[string]chan struct{}
	canceled  map[string]time.Time
	nextPrune time.Time
}

// begin 登记一次轮询，返回 CancelPoll 时关闭的 channel 与登记的注销函数。CancelPoll 已先到达时
// 返回的 channel 已关闭。
func (r *pollRegistry) begin(pollID string) (<-chan struct{}, func()) {
	stop := make(chan struct{})
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.canceled[pollID]; ok {
		delete(r.canceled, pollID)
		close(stop)
		return stop, func() {}
	}
	if r.active == nil {
		r.active = make(map[string]chan struct{})
	}
	r.active[pollID] = stop
	return stop, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.active[pollID] == stop {
			delete(r.active, pollID)
		}
	}
}

// cancel 结束 pollID 对应的轮询；轮询尚未登记时留下墓碑，让它登记时立即结束。
func (r *pollRegistry) cancel(pollID string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if stop, ok := r.active[pollID]; ok {
		delete(r.active, pollID)
		close(stop)
		return
	}
	if r.canceled == nil {
		r.canceled = make(map[string]time.Time)
	}
	if now.After(r.nextPrune) {
		for id, at := range r.canceled {
			if now.Sub(at) > pollCancelTombstoneTTL {
				delete(r.canceled, id)
			}
		}
		r.nextPrune = now.Add(pollCancelTombstoneTTL)
	}
	r.canceled[pollID] = now
}

func (h *matchingHandler) CancelPoll(_ context.Context, request *CancelPollRequest) (*CancelPollResponse, error) {
	if request.PollId == "" {
		return nil, status.Error(codes.InvalidArgument, "poll_id is required")
	}
	h.polls.cancel(request.PollId, time.Now())
	return &CancelPollResponse{}, nil
}

type queuePollTarget struct {
	partitionID int32
	handle      *partitionHandle
}

type queuePollWaiter struct {
	target queuePollTarget
	waiter *queue.PollWaiter
	active bool
}

type queuePollCandidate struct {
	target queuePollTarget
	lease  queue.Lease
}

func (h *matchingHandler) PollQueue(ctx context.Context, request *PollQueueRequest) (*PollQueueResponse, error) {
	if len(request.PartitionIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "partition_ids is required")
	}
	// PollQueue 是读操作：按 numRead 界定合法分区集合。queue 未知不再报错（fallback 全局默认）。
	// 配置不可知时返回 Unavailable（可重试），不猜测分区数。
	numRead, err := h.mgr.resolveReadPartitions(request.QueueName)
	if err != nil {
		return nil, toGRPCError(err)
	}
	if len(request.PartitionIds) > numRead {
		return nil, status.Errorf(codes.InvalidArgument, "partition_ids has %d entries for a %d-partition queue", len(request.PartitionIds), numRead)
	}
	opts, err := (queue.PollOptions{AcceptedRoutingKeys: request.AcceptedRoutingKeys}).Normalize()
	if err != nil {
		return nil, toGRPCError(err)
	}
	targets, diagnostics := h.loadQueuePollTargets(ctx, request.QueueName, request.PartitionIds)
	if len(targets) == 0 {
		return nil, toGRPCError(errors.Join(diagnostics...))
	}
	targets = rotateQueuePollTargets(targets, h.pollCursor.Add(1)-1)

	var stop <-chan struct{}
	if request.PollId != "" {
		var end func()
		stop, end = h.polls.begin(request.PollId)
		defer end()
		if pollStopped(stop) {
			return &PollQueueResponse{Empty: true}, nil
		}
	}

	candidate, scanErrors := scanQueuePollTargets(ctx, targets, opts)
	diagnostics = append(diagnostics, scanErrors...)
	if candidate != nil {
		return finishQueuePoll(ctx, stop, []queuePollCandidate{*candidate})
	}
	if request.PollTimeoutMs <= 0 {
		if len(diagnostics) > 0 {
			return nil, toGRPCError(errors.Join(diagnostics...))
		}
		return &PollQueueResponse{Empty: true}, nil
	}

	waiters, registerErrors := registerQueuePollWaiters(targets, opts)
	diagnostics = append(diagnostics, registerErrors...)
	if len(waiters) == 0 {
		return nil, toGRPCError(errors.Join(diagnostics...))
	}
	return waitForQueuePoll(ctx, stop, time.Duration(request.PollTimeoutMs)*time.Millisecond, waiters, diagnostics)
}

// pollStopped 报告 CancelPoll 是否已结束这次轮询；没有 poll_id 的轮询 stop 为 nil，永不结束。
func pollStopped(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

func (h *matchingHandler) loadQueuePollTargets(
	ctx context.Context,
	queueName string,
	partitionIDs []int32,
) ([]queuePollTarget, []error) {
	seen := make(map[int32]struct{}, len(partitionIDs))
	targets := make([]queuePollTarget, 0, len(partitionIDs))
	var diagnostics []error
	for _, partitionID := range partitionIDs {
		if _, ok := seen[partitionID]; ok {
			continue
		}
		seen[partitionID] = struct{}{}
		if err := h.mgr.validatePartition(queueName, int(partitionID), accessRead); err != nil {
			diagnostics = append(diagnostics, fmt.Errorf("partition %d: %w", partitionID, err))
			continue
		}
		handle, err := h.mgr.getOrLoadPartition(ctx, queueName, int(partitionID), accessRead)
		if err != nil {
			diagnostics = append(diagnostics, fmt.Errorf("partition %d: %w", partitionID, err))
			continue
		}
		targets = append(targets, queuePollTarget{partitionID: partitionID, handle: handle})
	}
	return targets, diagnostics
}

func rotateQueuePollTargets(targets []queuePollTarget, cursor uint64) []queuePollTarget {
	if len(targets) < 2 {
		return targets
	}
	offset := int(cursor % uint64(len(targets)))
	rotated := make([]queuePollTarget, 0, len(targets))
	rotated = append(rotated, targets[offset:]...)
	return append(rotated, targets[:offset]...)
}

func scanQueuePollTargets(
	ctx context.Context,
	targets []queuePollTarget,
	opts queue.PollOptions,
) (*queuePollCandidate, []error) {
	var diagnostics []error
	for _, target := range targets {
		lease, ok, err := target.handle.TryPollWithOptions(ctx, opts)
		if err != nil {
			diagnostics = append(diagnostics, fmt.Errorf("partition %d: %w", target.partitionID, err))
			continue
		}
		if !ok {
			continue
		}
		candidate, err := newQueuePollCandidate(target, lease)
		if err != nil {
			diagnostics = append(diagnostics, fmt.Errorf("partition %d: %w", target.partitionID, err))
			if cleanupErr := returnQueuePollLeases([]queuePollCandidate{{target: target, lease: lease}}); cleanupErr != nil {
				diagnostics = append(diagnostics, cleanupErr)
			}
			continue
		}
		return &candidate, diagnostics
	}
	return nil, diagnostics
}

func registerQueuePollWaiters(targets []queuePollTarget, opts queue.PollOptions) ([]queuePollWaiter, []error) {
	waiters := make([]queuePollWaiter, 0, len(targets))
	var diagnostics []error
	for _, target := range targets {
		waiter, err := target.handle.RegisterWaiterWithOptions(opts)
		if err != nil {
			diagnostics = append(diagnostics, fmt.Errorf("partition %d: %w", target.partitionID, err))
			continue
		}
		waiters = append(waiters, queuePollWaiter{target: target, waiter: waiter, active: true})
	}
	return waiters, diagnostics
}

func waitForQueuePoll(
	ctx context.Context,
	stop <-chan struct{},
	timeout time.Duration,
	waiters []queuePollWaiter,
	diagnostics []error,
) (*PollQueueResponse, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for activeQueuePollWaiters(waiters) > 0 {
		cases := make([]reflect.SelectCase, len(waiters)+2)
		for i := range waiters {
			var ready <-chan struct{}
			if waiters[i].active {
				ready = waiters[i].waiter.Ready()
			}
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ready)}
		}
		cases[len(waiters)] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timeoutCtx.Done())}
		cases[len(waiters)+1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(stop)}
		selected, _, _ := reflect.Select(cases)
		if selected >= len(waiters) {
			candidates, cleanupErrors := cancelAndCollectQueuePollWaiters(waiters)
			diagnostics = append(diagnostics, cleanupErrors...)
			if len(candidates) > 0 {
				return finishQueuePoll(ctx, stop, candidates)
			}
			if pollStopped(stop) {
				return &PollQueueResponse{Empty: true}, nil
			}
			if ctx.Err() != nil {
				return nil, status.Error(codes.Canceled, ctx.Err().Error())
			}
			if len(diagnostics) > 0 {
				return nil, toGRPCError(errors.Join(diagnostics...))
			}
			return &PollQueueResponse{Empty: true}, nil
		}

		candidates, readyErrors := claimReadyQueuePollWaiters(waiters, selected)
		diagnostics = append(diagnostics, readyErrors...)
		if len(candidates) == 0 {
			continue
		}
		lateCandidates, cleanupErrors := cancelAndCollectQueuePollWaiters(waiters)
		diagnostics = append(diagnostics, cleanupErrors...)
		candidates = append(candidates, lateCandidates...)
		return finishQueuePoll(ctx, stop, candidates)
	}

	if len(diagnostics) > 0 {
		return nil, toGRPCError(errors.Join(diagnostics...))
	}
	return &PollQueueResponse{Empty: true}, nil
}

func claimReadyQueuePollWaiters(waiters []queuePollWaiter, selected int) ([]queuePollCandidate, []error) {
	var candidates []queuePollCandidate
	var diagnostics []error
	for i := range waiters {
		if !waiters[i].active {
			continue
		}
		ready := i == selected
		if !ready {
			select {
			case <-waiters[i].waiter.Ready():
				ready = true
			default:
			}
		}
		if !ready {
			continue
		}
		waiters[i].active = false
		candidate, err := claimQueuePollWaiter(waiters[i])
		if err != nil {
			diagnostics = append(diagnostics, err)
			continue
		}
		if candidate != nil {
			candidates = append(candidates, *candidate)
		}
	}
	return candidates, diagnostics
}

func cancelAndCollectQueuePollWaiters(waiters []queuePollWaiter) ([]queuePollCandidate, []error) {
	var candidates []queuePollCandidate
	var diagnostics []error
	for i := range waiters {
		if !waiters[i].active {
			continue
		}
		waiters[i].active = false
		if waiters[i].waiter.Cancel() {
			continue
		}
		candidate, err := claimQueuePollWaiter(waiters[i])
		if err != nil {
			diagnostics = append(diagnostics, err)
			continue
		}
		if candidate != nil {
			candidates = append(candidates, *candidate)
		}
	}
	return candidates, diagnostics
}

func claimQueuePollWaiter(waiter queuePollWaiter) (*queuePollCandidate, error) {
	result, claimed := waiter.waiter.Claim()
	if !claimed {
		return nil, fmt.Errorf("partition %d: poll delivery claim lost", waiter.target.partitionID)
	}
	if result.Err != nil {
		return nil, fmt.Errorf("partition %d: %w", waiter.target.partitionID, result.Err)
	}
	candidate, err := newQueuePollCandidate(waiter.target, result.Lease())
	if err != nil {
		if cleanupErr := returnQueuePollLeases([]queuePollCandidate{{target: waiter.target, lease: result.Lease()}}); cleanupErr != nil {
			return nil, errors.Join(fmt.Errorf("partition %d: %w", waiter.target.partitionID, err), cleanupErr)
		}
		return nil, fmt.Errorf("partition %d: %w", waiter.target.partitionID, err)
	}
	return &candidate, nil
}

func newQueuePollCandidate(target queuePollTarget, lease queue.Lease) (queuePollCandidate, error) {
	token, err := queue.ParseTaskToken(lease.Token)
	if err != nil {
		return queuePollCandidate{}, err
	}
	if token.PartitionID != int(target.partitionID) {
		return queuePollCandidate{}, fmt.Errorf("token partition %d does not match target %d", token.PartitionID, target.partitionID)
	}
	return queuePollCandidate{target: target, lease: lease}, nil
}

func finishQueuePoll(ctx context.Context, stop <-chan struct{}, candidates []queuePollCandidate) (*PollQueueResponse, error) {
	if pollStopped(stop) {
		// 调用方已用 CancelPoll 放弃这次轮询：租约从未交付，原地归还。
		if err := returnQueuePollLeases(candidates); err != nil {
			return nil, toGRPCError(err)
		}
		return &PollQueueResponse{Empty: true}, nil
	}
	if err := ctx.Err(); err != nil {
		if cleanupErr := returnQueuePollLeases(candidates); cleanupErr != nil {
			return nil, status.Errorf(codes.Canceled, "%v; return canceled poll leases: %v", err, cleanupErr)
		}
		return nil, status.Error(codes.Canceled, err.Error())
	}
	winner := candidates[0]
	if err := returnQueuePollLeases(candidates[1:]); err != nil {
		if winnerErr := returnQueuePollLeases([]queuePollCandidate{winner}); winnerErr != nil {
			err = errors.Join(err, fmt.Errorf("return winner: %w", winnerErr))
		}
		return nil, toGRPCError(err)
	}
	if err := ctx.Err(); err != nil {
		if cleanupErr := returnQueuePollLeases([]queuePollCandidate{winner}); cleanupErr != nil {
			return nil, status.Errorf(codes.Canceled, "%v; return canceled poll lease: %v", err, cleanupErr)
		}
		return nil, status.Error(codes.Canceled, err.Error())
	}
	return pollQueueResponse(winner.target.partitionID, winner.lease), nil
}

// returnQueuePollLeases 归还本次 poll 没有真正交付出去的租约（竞速败者、
// 调用方取消时的胜者）。任务从未到过消费者手里，因此走 ReturnLease 原地归还，
// 不消耗投递次数、不重写新行。
func returnQueuePollLeases(candidates []queuePollCandidate) error {
	if len(candidates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), pollCleanupTimeout)
	defer cancel()
	var errs []error
	for _, candidate := range candidates {
		if err := candidate.target.handle.ReturnLease(ctx, candidate.lease.Token); err != nil {
			slog.Error("matching failed to return queue poll loser", "partition_id", candidate.target.partitionID, "err", err)
			errs = append(errs, fmt.Errorf("return partition %d poll lease: %w", candidate.target.partitionID, err))
		}
	}
	return errors.Join(errs...)
}

func activeQueuePollWaiters(waiters []queuePollWaiter) int {
	count := 0
	for i := range waiters {
		if waiters[i].active {
			count++
		}
	}
	return count
}

func pollQueueResponse(partitionID int32, lease queue.Lease) *PollQueueResponse {
	response := &PollQueueResponse{
		PartitionId:     partitionID,
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
