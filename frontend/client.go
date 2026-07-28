package frontend

import (
	"sync"

	"github.com/donutnomad/taskq/matching"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// connPool 管理到 matching 节点的 gRPC 连接池
type connPool struct {
	mu     sync.RWMutex
	conns  map[string]*grpc.ClientConn
	opts   []grpc.DialOption
	closed bool
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

// getClient 获取指定地址的 gRPC 客户端，连接按需创建。
// 池进入终态（closed）后直接返回 ErrPoolClosed，不再新建连接——杜绝 Close
// 与在途刷新竞态下产生永不被 Close 的泄漏连接。
func (p *connPool) getClient(addr string) (matching.MatchingServiceClient, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	if conn, ok := p.conns[addr]; ok {
		p.mu.RUnlock()
		return matching.NewMatchingServiceClient(conn), nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()

	// 写锁下复检 closed：close() 全程持写锁，故这里若观察到 closed 即为终态，
	// 之后不会再新建连接。
	if p.closed {
		return nil, ErrPoolClosed
	}
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

// close 关闭所有连接并将池置为终态，此后 getClient 不再新建连接。
func (p *connPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for addr, conn := range p.conns {
		conn.Close()
		delete(p.conns, addr)
	}
}
