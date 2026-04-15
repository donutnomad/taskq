// Package matching 实现分布式任务队列的分区管理服务。
//
// 每个 matching 节点通过 membership 包的一致性哈希环确定自己负责哪些分区，
// 并通过 gRPC 对外暴露 AddTask/PollTask/AckTask/NackTask 端点。
package matching

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc"
)

const (
	defaultVisTimeout  = 60 * time.Second
	defaultServiceName = "matching"
)

// Config 是 matching 服务的配置
type Config struct {
	// Store 任务存储接口（所有节点共享同一个 DB）
	Store queue.Store
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
	// QueueConfigs 队列名 → 分区数
	QueueConfigs map[string]int
	// GRPCOptions 额外的 gRPC server 选项
	GRPCOptions []grpc.ServerOption
}

// Server 是 matching 服务实例
type Server struct {
	cfg        Config
	grpcServer *grpc.Server
	mgr        *partitionManager
	resolver   membership.ServiceResolver
	changeCh   chan *membership.ChangedEvent
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
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

	resolver, err := cfg.Monitor.GetResolver(cfg.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("matching: get resolver: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	mgr := newPartitionManager(ctx, cfg.Store, cfg.BroadcastAddr, resolver, cfg.VisTimeout, cfg.QueueConfigs)

	grpcServer := grpc.NewServer(cfg.GRPCOptions...)
	handler := &matchingHandler{mgr: mgr}
	RegisterMatchingServiceServer(grpcServer, handler)

	return &Server{
		cfg:        cfg,
		grpcServer: grpcServer,
		mgr:        mgr,
		resolver:   resolver,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// Start 启动 matching 服务：初始 reconcile → 监听成员变化 → 启动 gRPC 监听
func (s *Server) Start() error {
	// 初始对账：加载本节点应拥有的分区
	s.mgr.reconcile()

	// 监听成员变化
	s.changeCh = make(chan *membership.ChangedEvent, 10)
	if err := s.resolver.AddListener("matching-server", s.changeCh); err != nil {
		return fmt.Errorf("matching: add listener: %w", err)
	}

	s.wg.Go(s.watchMembershipChanges)

	// 启动 gRPC 监听
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("matching: listen %s: %w", s.cfg.ListenAddr, err)
	}

	s.wg.Go(func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Printf("matching: gRPC server stopped: %v", err)
		}
	})

	return nil
}

// Stop 优雅停止 matching 服务
func (s *Server) Stop() {
	s.cancel()
	_ = s.resolver.RemoveListener("matching-server")
	s.grpcServer.GracefulStop()
	s.mgr.close()
	s.wg.Wait()
}

func (s *Server) watchMembershipChanges() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case _, ok := <-s.changeCh:
			if !ok {
				return
			}
			s.mgr.reconcile()
		}
	}
}
