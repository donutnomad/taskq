package frontend

import (
	"errors"
	"testing"
)

// TestConnPoolClosedGetClient 验证连接池终态：close 后 getClient 返回 ErrPoolClosed
// 且不新建连接——杜绝 Close 与在途刷新竞态下泄漏的 gRPC 连接。
func TestConnPoolClosedGetClient(t *testing.T) {
	p := newConnPool(nil)
	p.close()

	_, err := p.getClient("127.0.0.1:1234")
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("getClient after close = %v, want ErrPoolClosed", err)
	}
	if len(p.conns) != 0 {
		t.Fatalf("closed pool must not create connections, got %d", len(p.conns))
	}
}
