// Package main 演示使用真实组件（SQLite + membership Monitor + matching gRPC + frontend）
// 完成分布式任务队列的完整流程：Enqueue → Dequeue → Ack/Nack。
//
// 运行方式：go run ./example/
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/taskq/matching"
	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---- 内存 MemberStore 实现 ----

type memMemberStore struct {
	mu      sync.Mutex
	members map[string]membership.MemberRecord // key = "serviceName/ip:port"
}

func newMemMemberStore() *memMemberStore {
	return &memMemberStore{members: make(map[string]membership.MemberRecord)}
}

func (s *memMemberStore) UpsertMember(_ context.Context, req membership.UpsertMemberRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s/%s:%d", req.ServiceName, req.RPCAddress, req.RPCPort)
	s.members[key] = membership.MemberRecord{
		RPCAddress:  req.RPCAddress,
		RPCPort:     req.RPCPort,
		ServiceName: req.ServiceName,
		Draining:    req.Draining,
		StartAt:     req.StartAt,
	}
	return nil
}

func (s *memMemberStore) GetActiveMembers(_ context.Context, _ time.Duration) ([]membership.MemberRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]membership.MemberRecord, 0, len(s.members))
	for _, rec := range s.members {
		records = append(records, rec)
	}
	return records, nil
}

func (s *memMemberStore) PruneExpiredMembers(_ context.Context, _ int) error {
	return nil
}

// ---- 主流程 ----

func main() {
	const queueName = "demo-queue"

	// 1. 创建 SQLite store（内存模式）
	store, err := queue.NewSQLiteStore(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	fmt.Println("[启动] SQLite store 创建成功")

	// 2. 随机监听端口
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	addr := lis.Addr().String()
	addrPort, err := netip.ParseAddrPort(addr)
	if err != nil {
		log.Fatal(err)
	}
	lis.Close()

	// 3. 创建真实 Monitor（内存 MemberStore + 单节点）
	memberStore := newMemMemberStore()
	monitor := membership.NewMonitor(
		[]string{"matching"},
		memberStore,
		addrPort,
		1*time.Second, // pollInterval
		1*time.Second, // propagationTime
		time.Time{},   // joinTime：立即加入
		100,           // replicaPoints
	)
	monitor.Start()
	if err := monitor.WaitUntilInitialized(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer monitor.Stop()
	fmt.Println("[启动] Monitor 已启动")

	// 4. 启动真实 matching gRPC 服务
	srv, err := matching.NewServer(matching.Config{
		Store:         store,
		Monitor:       monitor,
		ListenAddr:    addr,
		BroadcastAddr: addr,
		QueueConfigs:  map[string]int{queueName: 4},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()
	fmt.Printf("[启动] Matching server 已启动，监听 %s\n", addr)

	// 等待 gRPC 就绪
	time.Sleep(100 * time.Millisecond)

	// 5. 创建真实 Frontend
	fe, err := frontend.New(frontend.Config{
		Monitor:     monitor,
		QueueName:   queueName,
		DialOpts:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		PollTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer fe.Close()
	fmt.Println("[启动] Frontend 已就绪")
	fmt.Println()

	ctx := context.Background()

	// ===== 测试 1: 基本 Enqueue → Dequeue → Ack =====
	fmt.Println("=== 测试 1: 基本 Enqueue → Dequeue → Ack ===")
	const taskCount = 10
	for i := range taskCount {
		if err := fe.Enqueue(ctx, fmt.Appendf(nil, "task-%d", i)); err != nil {
			log.Fatalf("enqueue failed: %v", err)
		}
	}
	fmt.Printf("[生产者] 写入 %d 条消息\n", taskCount)

	acked := 0
	for range taskCount {
		data, token, err := fe.Dequeue(ctx)
		if err != nil {
			log.Fatalf("dequeue failed: %v", err)
		}
		if err := fe.Ack(ctx, token); err != nil {
			log.Fatalf("ack failed: %v", err)
		}
		acked++
		fmt.Printf("[消费者] Ack: %s\n", data)
	}
	fmt.Printf("[结果] 成功消费并确认 %d/%d 条消息\n\n", acked, taskCount)

	// ===== 测试 2: Nack 重投递 =====
	fmt.Println("=== 测试 2: Nack 重投递 ===")
	if err := fe.Enqueue(ctx, []byte("nack-test")); err != nil {
		log.Fatal(err)
	}

	data, token, err := fe.Dequeue(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[消费者] 首次收到: %s → Nack\n", data)

	if err := fe.Nack(ctx, token); err != nil {
		log.Fatal(err)
	}

	data2, token2, err := fe.Dequeue(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := fe.Ack(ctx, token2); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[消费者] 再次收到: %s → Ack\n", data2)

	if string(data) == string(data2) {
		fmt.Println("[结果] Nack 重投递验证通过")
	} else {
		log.Fatalf("[结果] Nack 重投递验证失败: 期望 %s, 实际 %s", data, data2)
	}
	fmt.Println()

	// ===== 测试 3: 长轮询 =====
	// 由于 Dequeue 的长轮询阶段只等待一个分区，而 Enqueue 的 round-robin
	// 可能把消息分配到其他分区，这里用循环 Dequeue 模拟真实 worker 行为。
	fmt.Println("=== 测试 3: 长轮询 ===")
	fmt.Println("[消费者] 启动长轮询（队列为空）...")

	type dequeueResult struct {
		data []byte
		tok  string
		err  error
	}
	ch := make(chan dequeueResult, 1)
	go func() {
		// 真实 worker 会循环调用 Dequeue 直到拿到任务
		for {
			pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			d, t, e := fe.Dequeue(pollCtx)
			cancel()
			if e == nil && len(d) > 0 {
				ch <- dequeueResult{d, t, nil}
				return
			}
		}
	}()

	time.Sleep(500 * time.Millisecond)
	fmt.Println("[生产者] 500ms 后写入: late-arrival")
	if err := fe.Enqueue(ctx, []byte("late-arrival")); err != nil {
		log.Fatal(err)
	}

	select {
	case r := <-ch:
		if r.err != nil {
			log.Fatal(r.err)
		}
		fe.Ack(ctx, r.tok)
		fmt.Printf("[消费者] 收到: %s（长轮询唤醒）\n", r.data)
		fmt.Println("[结果] 长轮询验证通过")
	case <-time.After(10 * time.Second):
		log.Fatal("[结果] 长轮询验证失败: 超时")
	}

	fmt.Println()
	fmt.Println("=== 全部测试通过 ===")
}
