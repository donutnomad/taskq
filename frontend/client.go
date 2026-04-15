package frontend

import (
	"sync"

	"github.com/donutnomad/taskq/matching"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connPool 管理到 matching 节点的 gRPC 连接池
type connPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

func newConnPool(opts []grpc.DialOption) *connPool {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return &connPool{
		conns: make(map[string]*grpc.ClientConn),
		opts:  opts,
	}
}

// getClient 获取指定地址的 gRPC 客户端，连接按需创建
func (p *connPool) getClient(addr string) (matching.MatchingServiceClient, error) {
	p.mu.RLock()
	if conn, ok := p.conns[addr]; ok {
		p.mu.RUnlock()
		return matching.NewMatchingServiceClient(conn), nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// double check
	if conn, ok := p.conns[addr]; ok {
		return matching.NewMatchingServiceClient(conn), nil
	}

	conn, err := grpc.NewClient(addr, p.opts...)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return matching.NewMatchingServiceClient(conn), nil
}

// removeConn 关闭并移除指定地址的连接（成员离开时调用）
func (p *connPool) removeConn(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[addr]; ok {
		conn.Close()
		delete(p.conns, addr)
	}
}

// close 关闭所有连接
func (p *connPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, conn := range p.conns {
		conn.Close()
		delete(p.conns, addr)
	}
}
