package matching

import (
	"testing"
	"time"
)

// Exited 只在 Serve 退出后才就绪：调用方在 select 中用它区分「服务仍在」与「gRPC 服务端
// 已退出」，提前就绪会让进程把正常服务当成故障。Wait 返回 Serve 的错误，正常停止为 nil。
func TestMatchingServerExitedAfterStop(t *testing.T) {
	store := newTestSQLiteStore(t)
	addr := unusedLocalAddress(t)
	srv, err := NewServer(Config{
		Store:         store,
		Monitor:       &resolverMonitor{resolver: newStubResolver(addr)},
		ListenAddr:    addr,
		BroadcastAddr: addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	exited := srv.Exited()
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
		t.Fatal("Exited() fired while the gRPC server is serving")
	default:
	}

	stopped := make(chan struct{})
	go func() {
		srv.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Exited() did not fire after Stop")
	}
	if err := srv.Wait(); err != nil {
		t.Fatalf("Wait() after a clean Stop = %v, want nil", err)
	}
}

// 从未 Start 的实例：Wait 立即返回错误而不是永久阻塞，Exited 保持未就绪。
func TestMatchingServerExitedWithoutStart(t *testing.T) {
	store := newTestSQLiteStore(t)
	addr := unusedLocalAddress(t)
	srv, err := NewServer(Config{
		Store:         store,
		Monitor:       &resolverMonitor{resolver: newStubResolver(addr)},
		ListenAddr:    addr,
		BroadcastAddr: addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.Stop()
	if err := srv.Wait(); err == nil {
		t.Fatal("Wait() before Start should report that the server never started")
	}
	select {
	case <-srv.Exited():
		t.Fatal("Exited() fired for a server that never started")
	default:
	}
}
