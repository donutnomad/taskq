// Package frontend 提供分布式任务队列的客户端路由层。
//
// Frontend 是一个库（不是独立服务），worker 通过它透明地访问分布在多个
// matching 节点上的分区队列。内部通过 membership.ServiceResolver 做一致性
// 哈希路由，通过 gRPC 连接池与 matching 节点通信。
package frontend

import (
	"context"
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
)

const (
	defaultPollTimeout = 30 * time.Second
	defaultMaxRetries  = 3
	defaultServiceName = "matching"
)

// Config 前端路由配置
type Config struct {
	// Monitor membership 监控器
	Monitor membership.Monitor
	// ServiceName 服务名称，用于 GetResolver，默认 "matching"
	ServiceName string
	// QueueName 队列名称
	QueueName string
	// NumPartitions 分区数（必须与 matching 服务配置一致）
	NumPartitions int
	// DialOpts gRPC 拨号选项，默认使用 insecure
	DialOpts []grpc.DialOption
	// PollTimeout Dequeue 长轮询超时，默认 30s
	PollTimeout time.Duration
}

// Frontend 是分布式队列的客户端路由器
type Frontend struct {
	cfg        Config
	resolver   membership.ServiceResolver
	pool       *connPool
	changeCh   chan *membership.ChangedEvent
	enqueueIdx atomic.Int64
	pollIdx    atomic.Int64
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// New 创建并启动 Frontend 路由器
func New(cfg Config) (*Frontend, error) {
	if cfg.Monitor == nil {
		return nil, fmt.Errorf("frontend: Monitor is required")
	}
	if cfg.QueueName == "" {
		return nil, fmt.Errorf("frontend: QueueName is required")
	}
	if cfg.NumPartitions <= 0 {
		return nil, fmt.Errorf("frontend: NumPartitions must be > 0")
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaultServiceName
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = defaultPollTimeout
	}

	resolver, err := cfg.Monitor.GetResolver(cfg.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("frontend: get resolver: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	f := &Frontend{
		cfg:      cfg,
		resolver: resolver,
		pool:     newConnPool(cfg.DialOpts),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 监听成员变化，清理已离开节点的连接
	f.changeCh = make(chan *membership.ChangedEvent, 10)
	if err := resolver.AddListener("frontend", f.changeCh); err != nil {
		cancel()
		return nil, fmt.Errorf("frontend: add listener: %w", err)
	}
	f.wg.Go(f.watchMembershipChanges)

	return f, nil
}

// Enqueue 写入任务，round-robin 选分区后路由到对应 matching 节点
func (f *Frontend) Enqueue(ctx context.Context, data []byte) error {
	partID := int(f.enqueueIdx.Add(1)) % f.cfg.NumPartitions

	for attempt := range defaultMaxRetries {
		client, err := f.getPartitionClient(f.cfg.QueueName, partID)
		if err != nil {
			if attempt < defaultMaxRetries-1 {
				continue
			}
			return fmt.Errorf("frontend: enqueue: %w", err)
		}

		_, err = client.AddTask(ctx, &matching.AddTaskRequest{
			QueueName:   f.cfg.QueueName,
			PartitionId: int32(partID),
			Data:        data,
		})
		if err == nil {
			return nil
		}
		if !isRetryable(err) {
			return fmt.Errorf("frontend: enqueue: %w", err)
		}
	}
	return fmt.Errorf("frontend: enqueue: max retries exceeded")
}

// Dequeue 消费任务：快速扫描所有分区 → 长轮询
func (f *Frontend) Dequeue(ctx context.Context) ([]byte, string, error) {
	startIdx := int(f.pollIdx.Add(1)) % f.cfg.NumPartitions

	// 阶段 1：快速扫描，非阻塞尝试所有分区
	for i := range f.cfg.NumPartitions {
		pid := (startIdx + i) % f.cfg.NumPartitions

		client, err := f.getPartitionClient(f.cfg.QueueName, pid)
		if err != nil {
			continue
		}

		resp, err := client.PollTask(ctx, &matching.PollTaskRequest{
			QueueName:     f.cfg.QueueName,
			PartitionId:   int32(pid),
			PollTimeoutMs: 0, // 非阻塞
		})
		if err != nil {
			continue
		}
		if !resp.Empty {
			return resp.Data, resp.Token, nil
		}
	}

	// 阶段 2：长轮询起始分区
	pid := startIdx
	client, err := f.getPartitionClient(f.cfg.QueueName, pid)
	if err != nil {
		return nil, "", fmt.Errorf("frontend: dequeue: %w", err)
	}

	resp, err := client.PollTask(ctx, &matching.PollTaskRequest{
		QueueName:     f.cfg.QueueName,
		PartitionId:   int32(pid),
		PollTimeoutMs: f.cfg.PollTimeout.Milliseconds(),
	})
	if err != nil {
		return nil, "", fmt.Errorf("frontend: dequeue: %w", err)
	}
	if resp.Empty {
		return nil, "", ctx.Err()
	}
	return resp.Data, resp.Token, nil
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
	}
	return fmt.Errorf("frontend: nack: max retries exceeded")
}

// Close 关闭前端路由器
func (f *Frontend) Close() {
	f.cancel()
	_ = f.resolver.RemoveListener("frontend")
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

// isRetryable 判断 gRPC 错误是否可重试（分区所有权转移）
func isRetryable(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.FailedPrecondition || s.Code() == codes.Unavailable
}
