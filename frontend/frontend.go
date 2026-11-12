// Package frontend 提供分布式任务队列的客户端路由层。
//
// Frontend 是一个库（不是独立服务），worker 通过它透明地访问分布在多个
// matching 节点上的分区队列。内部通过 membership.ServiceResolver 做一致性
// 哈希路由，通过 gRPC 连接池与 matching 节点通信。
package frontend

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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
	// pollResponseGrace 是 poll RPC 在 owner 端等待时长之外继续等应答的时间。owner 按 PollTimeoutMs 自己结束
	// 轮询并应答，只有 owner 失联时才会用到这个期限。
	pollResponseGrace = 5 * time.Second
	// partitionRefreshInterval 是周期性刷新 read/write 分区快照的间隔，
	// 与 matching 的 partitionConfigTTL 对齐，保证运行时分区变更在一个周期内被 frontend 感知。
	partitionRefreshInterval = 5 * time.Second
	// minPartitions 是快照未就绪/异常时的安全下限，杜绝 `% 0` panic 或 `range 0` 不投递。
	minPartitions = 1
)

var frontendListenerSeq atomic.Int64

// partitionSnapshot 是 read/write 分区数的原子快照。Enqueue 的 `% write` 与
// Dequeue 的 `range read` 必须来自同一快照读取，避免出现 write>read 的撕裂。
type partitionSnapshot struct {
	read  int
	write int
}

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
	cfg          Config
	resolver     membership.ServiceResolver
	pool         *connPool
	changeCh     chan *membership.ChangedEvent
	listenerName string
	partitions   atomic.Pointer[partitionSnapshot]
	enqueueIdx   atomic.Int64
	pollIdx      atomic.Int64
	pollIDPrefix string
	pollSeq      atomic.Uint64
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
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
		pollIDPrefix: newPollIDPrefix(),
		ctx:          lifecycleCtx,
		cancel:       cancel,
	}

	// 通过 gRPC 查询 read/write 分区数，存入原子快照
	snapshot, err := f.fetchNumPartitions(ctx)
	if err != nil {
		cancel()
		f.pool.close()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("frontend: initialize queue %q: %w", cfg.QueueName, ctx.Err())
		}
		return nil, err
	}
	f.partitions.Store(&snapshot)

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
//
// 任务可以写进队列的任一可写分区：RoutingKey 定向由消费侧按 key 过滤，与分区无关。写入失败的 owner 在本次调用
// 内不再选择，重试换到其他 owner 的分区；只剩失败过的 owner 时退避后重新解析路由。owner 宕机后、成员表把它
// 剔除之前，写入由此落到存活的 owner，而不是在同一个分区上耗尽重试。
func (f *Frontend) EnqueueWithOptions(ctx context.Context, data []byte, opts EnqueueOptions) error {
	if err := opts.Validate(); err != nil {
		return err
	}
	request := &matching.AddTaskRequest{
		QueueName:  f.cfg.QueueName,
		Data:       data,
		RoutingKey: opts.RoutingKey,
	}
	if !opts.RoutingDeadline.IsZero() {
		request.RoutingDeadline = timestamppb.New(opts.RoutingDeadline)
	}

	failed := make(map[string]bool)
	var lastErr error
	for attempt := range defaultMaxRetries {
		partID, address, fresh, err := f.pickWritePartition(failed)
		if attempt > 0 && (err != nil || !fresh) {
			if retryErr := f.waitRetry(ctx); retryErr != nil {
				return fmt.Errorf("frontend: enqueue: %w", retryErr)
			}
			partID, address, _, err = f.pickWritePartition(failed)
		}
		if err != nil {
			lastErr = err
			continue
		}
		client, err := f.pool.getClient(address)
		if err != nil {
			failed[address] = true
			lastErr = err
			continue
		}
		request.PartitionId = int32(partID)
		_, err = client.AddTask(ctx, request)
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return fmt.Errorf("frontend: enqueue: %w", err)
		}
		failed[address] = true
		lastErr = err
		// 缩容态下 frontend 快照滞后仍按旧 write 选到高位分区，matching 按 numWrite 硬拒 FailedPrecondition。
		// 重试前刷新快照，按新 write 重选分区。刷新失败保留旧快照（best-effort）。
		if status.Code(err) == codes.FailedPrecondition {
			_ = f.refreshPartitions(ctx)
		}
	}
	return fmt.Errorf("frontend: enqueue: %w: %w", ErrRetriesExhausted, lastErr)
}

// pickWritePartition 从 round-robin 位置起选第一个 owner 不在 failed 里的可写分区，fresh 为 true。所有 owner
// 都失败过时返回 round-robin 位置上的分区，fresh 为 false。
func (f *Frontend) pickWritePartition(failed map[string]bool) (partID int, address string, fresh bool, err error) {
	write := f.snapshot().write
	start := int(f.enqueueIdx.Add(1) % int64(write))
	found := false
	for i := range write {
		candidate := (start + i) % write
		host, lookupErr := f.resolver.Lookup(fmt.Sprintf("%s/%d", f.cfg.QueueName, candidate))
		if lookupErr != nil {
			err = lookupErr
			continue
		}
		if !failed[host.GetAddress()] {
			return candidate, host.GetAddress(), true, nil
		}
		if !found {
			partID, address, found = candidate, host.GetAddress(), true
		}
	}
	if found {
		return partID, address, false, nil
	}
	return 0, "", false, err
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
//
// poll RPC 不随 ctx 取消。owner 在应答前检查调用方是否还在，检查通过之后、应答到达之前的取消会让 gRPC 丢掉
// 应答，应答里的租约在 owner 那里已是已投递，要等可见性超时才重新出现。因此：长轮询的 PollTimeoutMs 不超过
// ctx 的剩余时间，owner 自己按时结束；ctx 被取消或已有 owner 胜出时，用 CancelPoll 请其余 owner 结束轮询并
// 原地归还已领取的租约；应答照常读回，已无人要的租约立即以 undelivered 归还。owner 不支持 CancelPoll 时退回
// 取消 RPC。
func (f *Frontend) DequeueWithOptions(ctx context.Context, opts PollOptions) (Lease, error) {
	normalized, err := opts.Normalize()
	if err != nil {
		return Lease{}, err
	}
	opts = normalized
	// 读枚举 [0, read)，startIdx 与枚举必须用同一快照的 read（兜底下限 1），
	// 保证不会因撕裂漏读或越界。
	numRead := f.snapshot().read
	startIdx := int(f.pollIdx.Add(1)) % numRead
	groups, resolutionErrors := f.resolvePollGroups(startIdx, numRead)
	if len(groups) == 0 {
		if len(resolutionErrors) > 0 {
			return Lease{}, errors.Join(resolutionErrors...)
		}
		return Lease{}, ErrNoTask
	}
	ownerErrors := make([]error, len(groups))

	for i, group := range groups {
		if err := ctx.Err(); err != nil {
			return Lease{}, fmt.Errorf("frontend: dequeue: %w", err)
		}
		rpcCtx, cancel := f.pollRPCContext(ctx, 0)
		lease, ok, err := f.pollOwner(rpcCtx, group, 0, callerTimeoutMs(ctx), opts, "")
		cancel()
		ownerErrors[i] = err
		if err == nil && ok {
			return f.handOverLease(ctx, ownerPollResult{group: group, lease: lease, found: true})
		}
	}

	wait := f.cfg.PollTimeout
	deadline, bounded := ctx.Deadline()
	if bounded {
		bounded = time.Until(deadline) < wait
		if bounded {
			wait = time.Until(deadline)
		}
	}
	if err := ctx.Err(); err != nil {
		return Lease{}, fmt.Errorf("frontend: dequeue: %w", err)
	}
	wait = max(wait, time.Millisecond)

	resultCh := make(chan ownerPollResult, len(groups))
	polls := make([]*ownerPoll, len(groups))
	for i, group := range groups {
		polls[i] = f.startOwnerPoll(ctx, i, group, wait, opts, resultCh)
	}
	for pending := len(groups); pending > 0; {
		select {
		case r := <-resultCh:
			pending--
			polls[r.index] = nil
			ownerErrors[r.index] = r.err
			if r.err == nil && r.found {
				f.abandonOwnerPolls(polls, resultCh, pending, true)
				return f.handOverLease(ctx, r)
			}
		case <-ctx.Done():
			// 到期时 owner 按同一期限自己结束轮询，不必再发 CancelPoll。
			f.abandonOwnerPolls(polls, resultCh, pending, !errors.Is(ctx.Err(), context.DeadlineExceeded))
			return Lease{}, fmt.Errorf("frontend: dequeue: %w", ctx.Err())
		}
	}
	allErrors := errors.Join(append(append([]error(nil), resolutionErrors...), ownerErrors...)...)
	if bounded && allErrors == nil {
		// owner 按截断到毫秒的期限结束，空应答可能早于 ctx 到期到达；等到期限，与不受期限约束的长轮询一样
		// 以 ctx 的错误结束，调用方据此区分「期限到了」与「长轮询到期没有任务」。
		<-ctx.Done()
	}
	if ctx.Err() != nil {
		return Lease{}, fmt.Errorf("frontend: dequeue: %w", ctx.Err())
	}
	if allErrors != nil {
		return Lease{}, allErrors
	}
	return Lease{}, ErrNoTask
}

// handOverLease 把读回的租约交给调用方；调用方已离开时立即以 undelivered 归还。
func (f *Frontend) handOverLease(ctx context.Context, result ownerPollResult) (Lease, error) {
	if err := ctx.Err(); err != nil {
		err = fmt.Errorf("frontend: dequeue: %w", err)
		if cleanupErr := f.nackOwnerPollResults([]ownerPollResult{result}); cleanupErr != nil {
			return Lease{}, errors.Join(err, cleanupErr)
		}
		return Lease{}, err
	}
	return result.lease, nil
}

// ownerPoll 是发往一个 owner 的一次长轮询。
type ownerPoll struct {
	group  ownerPollGroup
	pollID string
	// cancel 掐断 RPC，只在 CancelPoll 不可用时使用。
	cancel context.CancelFunc
}

func (f *Frontend) startOwnerPoll(ctx context.Context, index int, group ownerPollGroup, timeout time.Duration, opts PollOptions, results chan<- ownerPollResult) *ownerPoll {
	rpcCtx, cancel := f.pollRPCContext(ctx, timeout)
	poll := &ownerPoll{group: group, pollID: f.nextPollID(), cancel: cancel}
	go func() {
		defer cancel()
		lease, found, err := f.pollOwner(rpcCtx, group, timeout, callerTimeoutMs(ctx), opts, poll.pollID)
		results <- ownerPollResult{index: index, group: group, lease: lease, found: found, err: err}
	}()
	return poll
}

// pollRPCContext 返回不随 ctx 取消的 poll RPC context：保留 ctx 的值，期限为 owner 端等待时长加
// pollResponseGrace，Frontend 关闭时取消。
func (f *Frontend) pollRPCContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout+pollResponseGrace)
	stop := context.AfterFunc(f.ctx, cancel)
	return rpcCtx, func() {
		stop()
		cancel()
	}
}

// abandonOwnerPolls 在后台结束仍未应答的长轮询：cancel 为 true 时先用 CancelPoll 请 owner 结束，CancelPoll
// 失败时取消 RPC；然后读回全部应答，把其中的租约以 undelivered 归还。polls 里已应答的项为 nil。
func (f *Frontend) abandonOwnerPolls(polls []*ownerPoll, results <-chan ownerPollResult, pending int, cancel bool) {
	if pending == 0 {
		return
	}
	open := make([]*ownerPoll, 0, pending)
	for _, poll := range polls {
		if poll != nil {
			open = append(open, poll)
		}
	}
	go func() {
		if cancel {
			f.cancelOwnerPolls(open)
		}
		var leases []ownerPollResult
		for range pending {
			if r := <-results; r.err == nil && r.found {
				leases = append(leases, r)
			}
		}
		if err := f.nackOwnerPollResults(leases); err != nil {
			slog.Warn("frontend failed to return abandoned poll leases", "queue", f.cfg.QueueName, "err", err)
		}
	}()
}

func (f *Frontend) cancelOwnerPolls(polls []*ownerPoll) {
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	for _, poll := range polls {
		client, err := f.pool.getClient(poll.group.address)
		if err == nil {
			_, err = client.CancelPoll(ctx, &matching.CancelPollRequest{PollId: poll.pollID})
		}
		if err != nil {
			poll.cancel()
		}
	}
}

func (f *Frontend) nextPollID() string {
	return f.pollIDPrefix + "-" + strconv.FormatUint(f.pollSeq.Add(1), 36)
}

func newPollIDPrefix() string {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
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

func (f *Frontend) resolvePollGroups(startIdx, numRead int) ([]ownerPollGroup, []error) {
	groupIndexes := make(map[string]int)
	groups := make([]ownerPollGroup, 0)
	var errs []error
	for i := range numRead {
		partitionID := (startIdx + i) % numRead
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

// callerTimeoutMs 返回 ctx 剩余的毫秒数（不足 1ms 记 1），ctx 没有期限时为 0。poll RPC 不随 ctx 结束，owner 靠它
// 知道调用方何时离开，见 matching.PollQueueRequest.CallerTimeoutMs。
func callerTimeoutMs(ctx context.Context) int64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return max(time.Until(deadline).Milliseconds(), 1)
}

func (f *Frontend) pollOwner(ctx context.Context, group ownerPollGroup, timeout time.Duration, callerTimeoutMs int64, opts PollOptions, pollID string) (Lease, bool, error) {
	client, err := f.pool.getClient(group.address)
	if err != nil {
		return Lease{}, false, fmt.Errorf("frontend: dequeue owner %q partitions %v: %w", group.address, group.partitionIDs, err)
	}
	response, err := client.PollQueue(ctx, &matching.PollQueueRequest{
		QueueName:           f.cfg.QueueName,
		PartitionIds:        group.partitionIDs,
		PollTimeoutMs:       timeout.Milliseconds(),
		AcceptedRoutingKeys: opts.AcceptedRoutingKeys,
		PollId:              pollID,
		CallerTimeoutMs:     callerTimeoutMs,
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
	var errs []error
	for _, result := range results {
		client, err := f.pool.getClient(result.group.address)
		if err != nil {
			errs = append(errs, fmt.Errorf("frontend: connect cleanup owner %q: %w", result.group.address, err))
			continue
		}
		if err := returnPollLease(client, f.cfg.QueueName, result.lease); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (f *Frontend) nackPollLeases(client matching.MatchingServiceClient, leases []Lease) error {
	if len(leases) == 0 {
		return nil
	}
	var errs []error
	for _, lease := range leases {
		if err := returnPollLease(client, f.cfg.QueueName, lease); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// 未交付租约的归还在后台进行，不占调用方的时间：每次尝试给 leaseReturnTimeout，失败按退避重试
// leaseReturnAttempts 次。归还失败的任务要等可见性超时（默认 60s）才能再被领取，所以宁可多等多试；
// 机器繁忙时一次本机 RPC 超过 1s 并不罕见。
const (
	leaseReturnTimeout  = 5 * time.Second
	leaseReturnAttempts = 3
)

func returnPollLease(client matching.MatchingServiceClient, queueName string, lease Lease) error {
	var err error
	for attempt := range leaseReturnAttempts {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}
		ctx, cancel := context.WithTimeout(context.Background(), leaseReturnTimeout)
		err = nackPollLease(ctx, client, queueName, lease)
		cancel()
		if err == nil {
			return nil
		}
	}
	return err
}

func nackPollLease(ctx context.Context, client matching.MatchingServiceClient, queueName string, lease Lease) error {
	partitionID, _, err := queue.ParseToken(lease.Token)
	if err != nil {
		return fmt.Errorf("frontend: parse cleanup token: %w", err)
	}
	// 竞速败者从未交付给消费者，标记 Undelivered 让 matching 原地归还租约，
	// 不消耗任务的投递次数。
	_, err = client.NackTask(ctx, &matching.NackTaskRequest{
		QueueName: queueName, PartitionId: int32(partitionID), Token: lease.Token,
		Undelivered: true,
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

	var lastErr error
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
		lastErr = err
		if attempt < defaultMaxRetries-1 {
			if retryErr := f.waitRetry(ctx); retryErr != nil {
				return fmt.Errorf("frontend: ack: %w", retryErr)
			}
		}
	}
	return fmt.Errorf("frontend: ack: %w: %w", ErrRetriesExhausted, lastErr)
}

// Nack 拒绝消费，触发重投递
func (f *Frontend) Nack(ctx context.Context, token string) error {
	partID, _, err := queue.ParseToken(token)
	if err != nil {
		return err
	}

	var lastErr error
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
		lastErr = err
		if attempt < defaultMaxRetries-1 {
			if retryErr := f.waitRetry(ctx); retryErr != nil {
				return fmt.Errorf("frontend: nack: %w", retryErr)
			}
		}
	}
	return fmt.Errorf("frontend: nack: %w: %w", ErrRetriesExhausted, lastErr)
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
	ticker := time.NewTicker(partitionRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			// 周期刷新 read/write 快照，规避"分区数启动后永不刷新"。
			f.refreshPartitionsBackground()
		case event, ok := <-f.changeCh:
			if !ok {
				return
			}
			for _, host := range event.HostsRemoved {
				f.pool.removeConn(host.GetAddress())
			}
			// 成员变更常伴随分区所有权变化，顺带刷新一次快照。
			f.refreshPartitionsBackground()
		}
	}
}

// refreshPartitionsBackground 用受生命周期约束的超时刷新分区快照；失败保留旧快照。
func (f *Frontend) refreshPartitionsBackground() {
	if f.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(f.ctx, partitionRefreshInterval)
	defer cancel()
	_ = f.refreshPartitions(ctx)
}

// snapshot 读取当前 read/write 分区快照，并对未就绪/异常值兜底到安全下限，
// 杜绝 Enqueue 的 `% 0` panic 与 Dequeue 的 `range 0` 不投递。
func (f *Frontend) snapshot() partitionSnapshot {
	p := f.partitions.Load()
	if p == nil {
		return partitionSnapshot{read: minPartitions, write: minPartitions}
	}
	snap := *p
	if snap.read < minPartitions {
		snap.read = minPartitions
	}
	if snap.write < minPartitions {
		snap.write = minPartitions
	}
	return snap
}

// refreshPartitions 重新拉取 read/write 分区数并原子替换快照。RPC 失败时保留旧快照，不清零。
func (f *Frontend) refreshPartitions(ctx context.Context) error {
	snapshot, err := f.fetchNumPartitions(ctx)
	if err != nil {
		return err
	}
	f.partitions.Store(&snapshot)
	return nil
}

// fetchNumPartitions 从 matching 服务查询队列的 read/write 分区数。
// 老 matching 只填 num_partitions（= read，向后兼容），此时 fallback 到该值。
func (f *Frontend) fetchNumPartitions(ctx context.Context) (partitionSnapshot, error) {
	host, err := f.resolver.Lookup(f.cfg.QueueName)
	if err != nil {
		return partitionSnapshot{}, fmt.Errorf("frontend: lookup matching node: %w", err)
	}
	client, err := f.pool.getClient(host.GetAddress())
	if err != nil {
		return partitionSnapshot{}, fmt.Errorf("frontend: connect matching node: %w", err)
	}
	resp, err := client.GetQueueInfo(ctx, &matching.GetQueueInfoRequest{QueueName: f.cfg.QueueName})
	if err != nil {
		return partitionSnapshot{}, fmt.Errorf("frontend: get queue info: %w", err)
	}
	read := int(resp.NumReadPartitions)
	write := int(resp.NumWritePartitions)
	// 老 matching 不填 read/write，两者为 0 时 fallback 到 num_partitions 保持兼容。
	if read <= 0 {
		read = int(resp.NumPartitions)
	}
	if write <= 0 {
		write = int(resp.NumPartitions)
	}
	if read <= 0 || write <= 0 {
		return partitionSnapshot{}, fmt.Errorf("frontend: invalid partition count read=%d write=%d for queue %q", read, write, f.cfg.QueueName)
	}
	return partitionSnapshot{read: read, write: write}, nil
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
