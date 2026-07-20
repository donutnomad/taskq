// Package frontend 提供分布式任务队列的客户端路由层。
//
// Frontend 是一个库（不是独立服务），worker 通过它透明地访问分布在多个
// matching 节点上的分区队列。内部通过 membership.ServiceResolver 做一致性
// 哈希路由，通过 gRPC 连接池与 matching 节点通信。
package frontend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultPollTimeout   = 30 * time.Second
	defaultMaxRetries    = 3
	defaultRetryBackoff  = 50 * time.Millisecond
	defaultServiceName   = "matching"
	pollWinnerDrainGrace = 2 * time.Millisecond
)

var frontendListenerSeq atomic.Int64

// Config 前端路由配置
type Config struct {
	// ResolverProvider supplies the matching service routing view.
	ResolverProvider membership.ResolverProvider
	// Monitor membership 监控器
	// Deprecated: use ResolverProvider. Existing configurations remain supported.
	Monitor membership.Monitor
	// ServiceName 服务名称，用于 GetResolver，默认 "matching"
	ServiceName string
	// QueueName 队列名称
	QueueName string
	// DialOpts gRPC 拨号选项，默认使用 insecure
	DialOpts []grpc.DialOption
	// PollTimeout Dequeue 长轮询超时，默认 30s
	PollTimeout time.Duration
}

type EnqueueOptions = queue.EnqueueOptions
type PollOptions = queue.PollOptions
type Lease = queue.Lease

// Frontend 是分布式队列的客户端路由器
type Frontend struct {
	cfg           Config
	resolver      membership.ServiceResolver
	pool          *connPool
	changeCh      chan *membership.ChangedEvent
	listenerName  string
	numPartitions int
	enqueueIdx    atomic.Int64
	pollIdx       atomic.Int64
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// New creates a Frontend with an unbounded initialization context.
func New(cfg Config) (*Frontend, error) {
	return NewWithContext(context.Background(), cfg)
}

// NewWithContext uses ctx for initial queue discovery. The returned Frontend
// owns an independent lifecycle that ends when Close is called.
func NewWithContext(ctx context.Context, cfg Config) (*Frontend, error) {
	if ctx == nil {
		return nil, fmt.Errorf("frontend: initialization context is nil")
	}
	resolverProvider := cfg.ResolverProvider
	if resolverProvider == nil {
		resolverProvider = cfg.Monitor
	}
	if resolverProvider == nil {
		return nil, fmt.Errorf("frontend: ResolverProvider or Monitor is required")
	}
	if cfg.QueueName == "" {
		return nil, fmt.Errorf("frontend: QueueName is required")
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaultServiceName
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = defaultPollTimeout
	}

	resolver, err := resolverProvider.GetResolver(cfg.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("frontend: get resolver: %w", err)
	}

	lifecycleCtx, cancel := context.WithCancel(context.Background())
	f := &Frontend{
		cfg:          cfg,
		resolver:     resolver,
		pool:         newConnPool(cfg.DialOpts),
		listenerName: fmt.Sprintf("frontend-%d", frontendListenerSeq.Add(1)),
		ctx:          lifecycleCtx,
		cancel:       cancel,
	}

	// 通过 gRPC 查询分区数
	numPartitions, err := f.fetchNumPartitions(ctx)
	if err != nil {
		cancel()
		f.pool.close()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("frontend: initialize queue %q: %w", cfg.QueueName, ctx.Err())
		}
		return nil, err
	}
	f.numPartitions = numPartitions

	// 监听成员变化，清理已离开节点的连接
	f.changeCh = make(chan *membership.ChangedEvent, 10)
	if err := resolver.AddListener(f.listenerName, f.changeCh); err != nil {
		cancel()
		f.pool.close()
		return nil, fmt.Errorf("frontend: add listener: %w", err)
	}
	f.wg.Go(f.watchMembershipChanges)

	return f, nil
}

// Enqueue 写入任务，round-robin 选分区后路由到对应 matching 节点
func (f *Frontend) Enqueue(ctx context.Context, data []byte) error {
	return f.EnqueueWithOptions(ctx, data, EnqueueOptions{})
}

// EnqueueWithOptions writes a task with delivery routing metadata.
func (f *Frontend) EnqueueWithOptions(ctx context.Context, data []byte, opts EnqueueOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}
	partID := int(f.enqueueIdx.Add(1)) % f.numPartitions

	for attempt := range defaultMaxRetries {
		client, err := f.getPartitionClient(f.cfg.QueueName, partID)
		if err != nil {
			if attempt < defaultMaxRetries-1 {
				if retryErr := f.waitRetry(ctx); retryErr != nil {
					return fmt.Errorf("frontend: enqueue: %w", retryErr)
				}
				continue
			}
			return fmt.Errorf("frontend: enqueue: %w", err)
		}

		request := &matching.AddTaskRequest{
			QueueName:   f.cfg.QueueName,
			PartitionId: int32(partID),
			Data:        data,
			RoutingKey:  opts.RoutingKey,
		}
		if !opts.RoutingDeadline.IsZero() {
			request.RoutingDeadline = timestamppb.New(opts.RoutingDeadline)
		}
		_, err = client.AddTask(ctx, request)
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return fmt.Errorf("frontend: enqueue: %w", err)
		}
		if attempt < defaultMaxRetries-1 {
			if retryErr := f.waitRetry(ctx); retryErr != nil {
				return fmt.Errorf("frontend: enqueue: %w", retryErr)
			}
		}
	}
	return fmt.Errorf("frontend: enqueue: max retries exceeded")
}

// Dequeue 消费任务：快速扫描所有分区 → 长轮询
func (f *Frontend) Dequeue(ctx context.Context) ([]byte, string, error) {
	lease, err := f.DequeueWithOptions(ctx, PollOptions{})
	if err != nil {
		return nil, "", err
	}
	return lease.Data, lease.Token, nil
}

// DequeueWithOptions consumes the first task visible to the accepted routing keys.
func (f *Frontend) DequeueWithOptions(ctx context.Context, opts PollOptions) (Lease, error) {
	normalized, err := opts.Normalize()
	if err != nil {
		return Lease{}, err
	}
	opts = normalized
	startIdx := int(f.pollIdx.Add(1)) % f.numPartitions
	groups, resolutionErrors := f.resolvePollGroups(startIdx)
	if len(groups) == 0 {
		if len(resolutionErrors) > 0 {
			return Lease{}, errors.Join(resolutionErrors...)
		}
		return Lease{}, ErrNoTask
	}
	ownerErrors := make([]error, len(groups))

	for i, group := range groups {
		lease, ok, err := f.pollOwner(ctx, group, 0, opts)
		if err != nil {
			ownerErrors[i] = err
			continue
		}
		ownerErrors[i] = nil
		if ok {
			return lease, nil
		}
	}

	pollCtx, pollCancel := context.WithTimeout(ctx, f.cfg.PollTimeout+5*time.Second)
	defer pollCancel()

	resultCh := make(chan ownerPollResult, len(groups))
	for i, group := range groups {
		go func() {
			lease, ok, err := f.pollOwner(pollCtx, group, f.cfg.PollTimeout, opts)
			resultCh <- ownerPollResult{index: i, group: group, lease: lease, found: ok, err: err}
		}()
	}

	for i := range groups {
		r := <-resultCh
		ownerErrors[r.index] = r.err
		if r.err == nil && r.found {
			losers := make([]ownerPollResult, 0, len(groups)-1)
			for _, other := range drainPollResults(resultCh, len(groups)-i-1, pollCancel) {
				if other.err == nil && other.found {
					losers = append(losers, other)
				}
			}
			if cleanupErr := f.nackOwnerPollResults(losers); cleanupErr != nil {
				if winnerErr := f.nackOwnerPollResults([]ownerPollResult{r}); winnerErr != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("return winner: %w", winnerErr))
				}
				return Lease{}, cleanupErr
			}
			return r.lease, nil
		}
	}
	if ctx.Err() != nil {
		return Lease{}, fmt.Errorf("frontend: dequeue: %w", ctx.Err())
	}
	allErrors := append(append([]error(nil), resolutionErrors...), ownerErrors...)
	if err := errors.Join(allErrors...); err != nil {
		return Lease{}, err
	}
	return Lease{}, ErrNoTask
}

type ownerPollGroup struct {
	address      string
	partitionIDs []int32
}

type ownerPollResult struct {
	index int
	group ownerPollGroup
	lease Lease
	found bool
	err   error
}

func (f *Frontend) resolvePollGroups(startIdx int) ([]ownerPollGroup, []error) {
	groupIndexes := make(map[string]int)
	groups := make([]ownerPollGroup, 0)
	var errs []error
	for i := range f.numPartitions {
		partitionID := (startIdx + i) % f.numPartitions
		host, err := f.resolver.Lookup(fmt.Sprintf("%s/%d", f.cfg.QueueName, partitionID))
		if err != nil {
			errs = append(errs, fmt.Errorf("frontend: dequeue partition %d: %w", partitionID, err))
			continue
		}
		address := host.GetAddress()
		index, ok := groupIndexes[address]
		if !ok {
			index = len(groups)
			groupIndexes[address] = index
			groups = append(groups, ownerPollGroup{address: address})
		}
		groups[index].partitionIDs = append(groups[index].partitionIDs, int32(partitionID))
	}
	return groups, errs
}

func (f *Frontend) pollOwner(ctx context.Context, group ownerPollGroup, timeout time.Duration, opts PollOptions) (Lease, bool, error) {
	client, err := f.pool.getClient(group.address)
	if err != nil {
		return Lease{}, false, fmt.Errorf("frontend: dequeue owner %q partitions %v: %w", group.address, group.partitionIDs, err)
	}
	response, err := client.PollQueue(ctx, &matching.PollQueueRequest{
		QueueName:           f.cfg.QueueName,
		PartitionIds:        group.partitionIDs,
		PollTimeoutMs:       timeout.Milliseconds(),
		AcceptedRoutingKeys: opts.AcceptedRoutingKeys,
	})
	if status.Code(err) == codes.Unimplemented {
		return f.pollPartitions(ctx, client, group.partitionIDs, timeout, opts)
	}
	if err != nil {
		return Lease{}, false, fmt.Errorf("frontend: dequeue owner %q partitions %v: %w", group.address, group.partitionIDs, err)
	}
	if response.Empty {
		return Lease{}, false, nil
	}
	if !containsPartition(group.partitionIDs, response.PartitionId) {
		return Lease{}, false, fmt.Errorf("frontend: owner %q returned unrequested partition %d", group.address, response.PartitionId)
	}
	lease, err := leaseFromPollQueueResponse(response)
	return lease, err == nil, err
}

func (f *Frontend) pollPartitions(
	ctx context.Context,
	client matching.MatchingServiceClient,
	partitionIDs []int32,
	timeout time.Duration,
	opts PollOptions,
) (Lease, bool, error) {
	if timeout <= 0 {
		var errs []error
		for _, partitionID := range partitionIDs {
			lease, found, err := pollPartition(ctx, client, f.cfg.QueueName, partitionID, 0, opts)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if found {
				return lease, true, nil
			}
		}
		if len(errs) > 0 {
			return Lease{}, false, errors.Join(errs...)
		}
		return Lease{}, false, nil
	}

	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan ownerPollResult, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		go func() {
			lease, found, err := pollPartition(pollCtx, client, f.cfg.QueueName, partitionID, timeout, opts)
			results <- ownerPollResult{lease: lease, found: found, err: err}
		}()
	}
	var errs []error
	for i := range partitionIDs {
		result := <-results
		if result.err == nil && result.found {
			losers := make([]Lease, 0, len(partitionIDs)-1)
			for _, other := range drainPollResults(results, len(partitionIDs)-i-1, cancel) {
				if other.err == nil && other.found {
					losers = append(losers, other.lease)
				}
			}
			if cleanupErr := f.nackPollLeases(client, losers); cleanupErr != nil {
				if winnerErr := f.nackPollLeases(client, []Lease{result.lease}); winnerErr != nil {
					cleanupErr = errors.Join(cleanupErr, fmt.Errorf("return winner: %w", winnerErr))
				}
				return Lease{}, false, cleanupErr
			}
			return result.lease, true, nil
		}
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	if ctx.Err() != nil {
		return Lease{}, false, ctx.Err()
	}
	if len(errs) > 0 {
		return Lease{}, false, errors.Join(errs...)
	}
	return Lease{}, false, nil
}

func drainPollResults(results <-chan ownerPollResult, remaining int, cancel context.CancelFunc) []ownerPollResult {
	drained := make([]ownerPollResult, 0, remaining)
	grace := time.NewTimer(pollWinnerDrainGrace)
drainReady:
	for remaining > 0 {
		select {
		case result := <-results:
			drained = append(drained, result)
			remaining--
		case <-grace.C:
			break drainReady
		}
	}
	if !grace.Stop() {
		select {
		case <-grace.C:
		default:
		}
	}
	cancel()
	for range remaining {
		drained = append(drained, <-results)
	}
	return drained
}

func (f *Frontend) nackOwnerPollResults(results []ownerPollResult) error {
	if len(results) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var errs []error
	for _, result := range results {
		client, err := f.pool.getClient(result.group.address)
		if err != nil {
			errs = append(errs, fmt.Errorf("frontend: connect cleanup owner %q: %w", result.group.address, err))
			continue
		}
		if err := nackPollLease(ctx, client, f.cfg.QueueName, result.lease); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (f *Frontend) nackPollLeases(client matching.MatchingServiceClient, leases []Lease) error {
	if len(leases) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var errs []error
	for _, lease := range leases {
		if err := nackPollLease(ctx, client, f.cfg.QueueName, lease); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func nackPollLease(ctx context.Context, client matching.MatchingServiceClient, queueName string, lease Lease) error {
	partitionID, _, err := queue.ParseToken(lease.Token)
	if err != nil {
		return fmt.Errorf("frontend: parse cleanup token: %w", err)
	}
	_, err = client.NackTask(ctx, &matching.NackTaskRequest{
		QueueName: queueName, PartitionId: int32(partitionID), Token: lease.Token,
	})
	if err != nil {
		return fmt.Errorf("frontend: return poll loser partition %d: %w", partitionID, err)
	}
	return nil
}

func pollPartition(
	ctx context.Context,
	client matching.MatchingServiceClient,
	queueName string,
	partitionID int32,
	timeout time.Duration,
	opts PollOptions,
) (Lease, bool, error) {
	response, err := client.PollTask(ctx, &matching.PollTaskRequest{
		QueueName:           queueName,
		PartitionId:         partitionID,
		PollTimeoutMs:       timeout.Milliseconds(),
		AcceptedRoutingKeys: opts.AcceptedRoutingKeys,
	})
	if err != nil {
		return Lease{}, false, fmt.Errorf("frontend: dequeue partition %d: %w", partitionID, err)
	}
	if response.Empty {
		return Lease{}, false, nil
	}
	lease, err := leaseFromPollResponse(response)
	return lease, err == nil, err
}

func containsPartition(partitionIDs []int32, partitionID int32) bool {
	for _, candidate := range partitionIDs {
		if candidate == partitionID {
			return true
		}
	}
	return false
}

func leaseFromPollResponse(response *matching.PollTaskResponse) (Lease, error) {
	lease := Lease{
		Data:            response.Data,
		Token:           response.Token,
		RoutingKey:      response.RoutingKey,
		RoutingFallback: response.RoutingFallback,
	}
	if response.RoutingDeadline == nil {
		return lease, nil
	}
	if err := response.RoutingDeadline.CheckValid(); err != nil {
		return Lease{}, fmt.Errorf("frontend: invalid routing deadline: %w", err)
	}
	lease.RoutingDeadline = response.RoutingDeadline.AsTime()
	return lease, nil
}

func leaseFromPollQueueResponse(response *matching.PollQueueResponse) (Lease, error) {
	lease := Lease{
		Data:            response.Data,
		Token:           response.Token,
		RoutingKey:      response.RoutingKey,
		RoutingFallback: response.RoutingFallback,
	}
	if response.RoutingDeadline == nil {
		return lease, nil
	}
	if err := response.RoutingDeadline.CheckValid(); err != nil {
		return Lease{}, fmt.Errorf("frontend: invalid routing deadline: %w", err)
	}
	lease.RoutingDeadline = response.RoutingDeadline.AsTime()
	return lease, nil
}

// Ack 确认消费完成
func (f *Frontend) Ack(ctx context.Context, token string) error {
	partID, _, err := queue.ParseToken(token)
	if err != nil {
		return err
	}

	for attempt := range defaultMaxRetries {
		client, err := f.getPartitionClient(f.cfg.QueueName, partID)
		if err != nil {
			if attempt < defaultMaxRetries-1 {
				if retryErr := f.waitRetry(ctx); retryErr != nil {
					return fmt.Errorf("frontend: ack: %w", retryErr)
				}
				continue
			}
			return fmt.Errorf("frontend: ack: %w", err)
		}

		_, err = client.AckTask(ctx, &matching.AckTaskRequest{
			QueueName:   f.cfg.QueueName,
			PartitionId: int32(partID),
			Token:       token,
		})
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return fmt.Errorf("frontend: ack: %w", err)
		}
		if attempt < defaultMaxRetries-1 {
			if retryErr := f.waitRetry(ctx); retryErr != nil {
				return fmt.Errorf("frontend: ack: %w", retryErr)
			}
		}
	}
	return fmt.Errorf("frontend: ack: max retries exceeded")
}

// Nack 拒绝消费，触发重投递
func (f *Frontend) Nack(ctx context.Context, token string) error {
	partID, _, err := queue.ParseToken(token)
	if err != nil {
		return err
	}

	for attempt := range defaultMaxRetries {
		client, err := f.getPartitionClient(f.cfg.QueueName, partID)
		if err != nil {
			if attempt < defaultMaxRetries-1 {
				if retryErr := f.waitRetry(ctx); retryErr != nil {
					return fmt.Errorf("frontend: nack: %w", retryErr)
				}
				continue
			}
			return fmt.Errorf("frontend: nack: %w", err)
		}

		_, err = client.NackTask(ctx, &matching.NackTaskRequest{
			QueueName:   f.cfg.QueueName,
			PartitionId: int32(partID),
			Token:       token,
		})
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return fmt.Errorf("frontend: nack: %w", err)
		}
		if attempt < defaultMaxRetries-1 {
			if retryErr := f.waitRetry(ctx); retryErr != nil {
				return fmt.Errorf("frontend: nack: %w", retryErr)
			}
		}
	}
	return fmt.Errorf("frontend: nack: max retries exceeded")
}

// Close 关闭前端路由器
func (f *Frontend) Close() {
	f.cancel()
	_ = f.resolver.RemoveListener(f.listenerName)
	f.pool.close()
	f.wg.Wait()
}

// getPartitionClient 通过一致性哈希找到分区所属节点，返回其 gRPC 客户端
func (f *Frontend) getPartitionClient(queueName string, partID int) (matching.MatchingServiceClient, error) {
	key := fmt.Sprintf("%s/%d", queueName, partID)
	host, err := f.resolver.Lookup(key)
	if err != nil {
		return nil, err
	}
	return f.pool.getClient(host.GetAddress())
}

func (f *Frontend) watchMembershipChanges() {
	for {
		select {
		case <-f.ctx.Done():
			return
		case event, ok := <-f.changeCh:
			if !ok {
				return
			}
			for _, host := range event.HostsRemoved {
				f.pool.removeConn(host.GetAddress())
			}
		}
	}
}

// fetchNumPartitions 从 matching 服务查询队列的分区数
func (f *Frontend) fetchNumPartitions(ctx context.Context) (int, error) {
	host, err := f.resolver.Lookup(f.cfg.QueueName)
	if err != nil {
		return 0, fmt.Errorf("frontend: lookup matching node: %w", err)
	}
	client, err := f.pool.getClient(host.GetAddress())
	if err != nil {
		return 0, fmt.Errorf("frontend: connect matching node: %w", err)
	}
	resp, err := client.GetQueueInfo(ctx, &matching.GetQueueInfoRequest{QueueName: f.cfg.QueueName})
	if err != nil {
		return 0, fmt.Errorf("frontend: get queue info: %w", err)
	}
	if resp.NumPartitions <= 0 {
		return 0, fmt.Errorf("frontend: invalid partition count %d for queue %q", resp.NumPartitions, f.cfg.QueueName)
	}
	return int(resp.NumPartitions), nil
}

// isRetryable 判断 gRPC 错误是否可重试（分区所有权转移）
func isRetryable(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.FailedPrecondition || s.Code() == codes.Unavailable || s.Code() == codes.Aborted
}

func (f *Frontend) waitRetry(ctx context.Context) error {
	timer := time.NewTimer(defaultRetryBackoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.ctx.Done():
		return context.Canceled
	case <-timer.C:
		return nil
	}
}
