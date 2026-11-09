// Package matching 实现分布式任务队列的分区管理服务。
//
// 每个 matching 节点通过 membership 包的一致性哈希环确定自己负责哪些分区，
// 并通过 gRPC 对外暴露 AddTask/PollTask/AckTask/NackTask 端点。
package matching

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	dbleader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const (
	defaultVisTimeout        = 60 * time.Second
	defaultServiceName       = "matching"
	defaultReconcileInterval = 5 * time.Second
	// defaultPartitionCount 是 bootstrap 全局默认行时的读/写分区数。
	defaultPartitionCount = 4
)

// defaultMaxAttempts 是启用投递上限时的一个参考值。生产默认为不限次数
// （Config.MaxAttempts 零值），仅当调用方显式设为正数时才启用上限。
const defaultMaxAttempts = 10

var matchingListenerSeq atomic.Int64

// Store provides durable queue operations and fenced partition leadership.
// The caller owns the Store connection lifecycle.
type Store interface {
	queue.Store
	dbleader.LockStore
}

// Config 是 matching 服务的配置
type Config struct {
	// Store 任务与分区所有权存储（所有节点共享同一个 DB）
	Store Store
	// Monitor membership 监控器
	Monitor membership.Monitor
	// ServiceName 服务名称，用于 GetResolver，默认 "matching"
	ServiceName string
	// ListenAddr gRPC 监听地址，如 ":7235"
	ListenAddr string
	// BroadcastAddr 广播地址，必须与 Monitor 注册的地址一致
	BroadcastAddr string
	// VisTimeout 可见性超时，默认 60s
	VisTimeout time.Duration
	// MaxAttempts 单条任务的总投递次数上限（含首次投递）。
	// 0（默认）表示不限次数：反复失败的任务会持续以新 taskID 重投，ackLevel 不会被其阻塞。
	// 设为正数则启用上限，超过后任务被丢弃并仅记录一条 error 日志。
	// 消费者遇到无法处理的消息时，推荐自行持久化后回 Ack（而非 Nack），而不是依赖此上限丢弃。
	MaxAttempts int
	// DefaultPartitions 启动时 bootstrap 全局默认行（_default_）的读/写分区数，默认 4。
	// 仅在 _default_ 行不存在时 insert，已存在则不覆盖（幂等）。分区数运行时纯 dynamic，
	// 由 DB partition_config 表决定，不再要求各节点 Config 一致。
	DefaultPartitions int
	// LeaseOwner 节点唯一标识，用于 DB lease 独占分区。为空时自动生成 UUID。
	LeaseOwner string
	// GRPCOptions 额外的 gRPC server 选项
	GRPCOptions []grpc.ServerOption
	// ReconcileInterval 周期校验分区归属，默认 5s
	ReconcileInterval time.Duration
}

// Server 是 matching 服务实例
type Server struct {
	cfg        Config
	grpcServer *grpc.Server
	mgr        *partitionManager
	resolver   membership.ServiceResolver
	changeCh   chan *membership.ChangedEvent
	listener   string
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	stopOnce   sync.Once
	// serveDone 在 Serve 返回时关闭；serveExit 随后收到 Serve 的错误（主动 Stop 时为 nil）。
	// 主动 Stop 也会触发关闭，调用方判断「意外退出」时要结合自身的关闭标志。
	serveDone chan struct{}
	serveExit chan error
}

// NewServer 创建 matching 服务实例
func NewServer(cfg Config) (*Server, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("matching: Store is required")
	}
	if cfg.Monitor == nil {
		return nil, fmt.Errorf("matching: Monitor is required")
	}
	if cfg.BroadcastAddr == "" {
		return nil, fmt.Errorf("matching: BroadcastAddr is required")
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = defaultServiceName
	}
	if cfg.VisTimeout <= 0 {
		cfg.VisTimeout = defaultVisTimeout
	}
	if cfg.LeaseOwner == "" {
		cfg.LeaseOwner = uuid.New().String()
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = defaultReconcileInterval
	}
	if cfg.DefaultPartitions <= 0 {
		cfg.DefaultPartitions = defaultPartitionCount
	}

	resolver, err := cfg.Monitor.GetResolver(cfg.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("matching: get resolver: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	serveExit := make(chan error, 1)

	// bootstrap 全局默认行（幂等，已存在不覆盖）：分区数纯 dynamic，各节点读同一行。
	if err := cfg.Store.EnsureDefaultPartitions(ctx, cfg.DefaultPartitions, cfg.DefaultPartitions); err != nil {
		cancel()
		return nil, fmt.Errorf("matching: bootstrap default partitions: %w", err)
	}

	mgr := newPartitionManager(ctx, cfg.Store, cfg.Store, cfg.BroadcastAddr, cfg.LeaseOwner, resolver, cfg.VisTimeout, cfg.MaxAttempts)

	grpcServer := grpc.NewServer(cfg.GRPCOptions...)
	handler := &matchingHandler{mgr: mgr}
	RegisterMatchingServiceServer(grpcServer, handler)

	return &Server{
		cfg:        cfg,
		grpcServer: grpcServer,
		mgr:        mgr,
		resolver:   resolver,
		listener:   fmt.Sprintf("matching-server-%d", matchingListenerSeq.Add(1)),
		ctx:        ctx,
		cancel:     cancel,
		serveDone:  serveDone,
		serveExit:  serveExit,
	}, nil
}

// Start acquires external resources before starting background work. Any
// failed phase releases resources acquired by earlier phases.
func (s *Server) Start() (err error) {
	started := false
	defer func() {
		if !started {
			s.stop()
		}
	}()

	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("matching: listen %s: %w", s.cfg.ListenAddr, err)
	}

	s.changeCh = make(chan *membership.ChangedEvent, 10)
	if err := s.resolver.AddListener(s.listener, s.changeCh); err != nil {
		_ = lis.Close()
		return fmt.Errorf("matching: add listener: %w", err)
	}

	// Reconcile starts partition leaders only after socket and listener setup.
	s.mgr.reconcile()
	s.wg.Go(s.watchMembershipChanges)
	serveDone, serveExit := s.serveDone, s.serveExit
	s.wg.Go(func() {
		err := s.grpcServer.Serve(lis)
		if err != nil {
			slog.Error("matching gRPC server stopped", "listen_addr", s.cfg.ListenAddr, "err", err)
		}
		serveExit <- err
		close(serveDone)
	})

	started = true
	return nil
}

// Stop 优雅停止 matching 服务
func (s *Server) Stop() {
	s.stop()
}

// Wait 阻塞到 gRPC 服务端退出，返回 Serve 的错误；正常停止（Serve 返回
// grpc.ErrServerStopped）时为 nil。从未成功 Start 的实例立即返回 nil。
func (s *Server) Wait() error {
	select {
	case <-s.serveDone:
		select {
		case err := <-s.serveExit:
			if errors.Is(err, grpc.ErrServerStopped) {
				return nil
			}
			return err
		default:
		}
		return nil
	default:
	}
	return errors.New("matching: Wait called before the gRPC server started")
}

// Exited 返回一个在 gRPC 服务端退出时关闭的 channel，供调用方在 select 中等待，
// 无需占用 goroutine。Start 未成功时它与 Stop 之后同样保持未关闭，调用方应结合
// 自身的启动/关闭状态判断。
func (s *Server) Exited() <-chan struct{} { return s.serveDone }

func (s *Server) stop() {
	s.stopOnce.Do(func() {
		s.cancel()
		_ = s.resolver.RemoveListener(s.listener)
		s.mgr.close()
		s.grpcServer.GracefulStop()
		s.wg.Wait()
	})
}

func (s *Server) watchMembershipChanges() {
	ticker := time.NewTicker(s.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case _, ok := <-s.changeCh:
			if !ok {
				return
			}
			s.mgr.reconcile()
		case <-ticker.C:
			s.mgr.reconcile()
		}
	}
}
