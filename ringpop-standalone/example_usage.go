//go:build ignore
// +build ignore

// 这个文件演示如何使用本包构建自己的分布式节点发现系统。
// 运行方式：go run example_usage.go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	ringpop "github.com/yourname/ringpop-standalone"
)

// ---- 第一步：实现 MemberStore 接口 ----
// 这是你唯一需要自己实现的部分，把心跳数据存到你的 DB 里。

type MySQLMemberStore struct {
	// db *sql.DB  // 你的数据库连接
}

func (s *MySQLMemberStore) UpsertMember(ctx context.Context, req ringpop.UpsertMemberRequest) error {
	// INSERT INTO cluster_members (service_name, rpc_address, rpc_port, membership_port, host_id, session_start, expires_at)
	// VALUES (?, ?, ?, ?, ?, ?, ?)
	// ON DUPLICATE KEY UPDATE rpc_address=?, rpc_port=?, session_start=?, expires_at=?
	fmt.Printf("[DB] upsert member: %s at %s:%d (gossip:%d)\n",
		req.ServiceName,
		req.RPCAddress,
		req.RPCPort,
		req.MembershipPort,
	)
	return nil
}

func (s *MySQLMemberStore) GetActiveMembers(ctx context.Context, lastHeartbeatWithin time.Duration) ([]ringpop.MemberRecord, error) {
	// SELECT rpc_address, membership_port FROM cluster_members
	// WHERE updated_at > NOW() - INTERVAL ? SECOND
	// 返回所有最近有心跳的节点（用于 Bootstrap 种子发现）
	return []ringpop.MemberRecord{
		// 示例：假设 DB 里已有这个节点
		{RPCAddress: net.ParseIP("10.0.0.1"), MembershipPort: 6933},
	}, nil
}

func (s *MySQLMemberStore) PruneExpiredMembers(ctx context.Context, maxCount int) error {
	// DELETE FROM cluster_members WHERE expires_at < NOW() LIMIT ?
	return nil
}

// ---- 第二步：启动 Monitor ----

func main() {
	store := &MySQLMemberStore{}

	// 本节点的地址配置
	myGossipAddr := "10.0.0.2:6933" // ringpop/gossip 监听地址（必须是具体 IP）
	myGRPCAddr := "10.0.0.2:7233"   // gRPC 服务监听地址

	// 创建 ringpop 实例
	rp, err := ringpop.NewRingpop("my-cluster", myGossipAddr)
	if err != nil {
		log.Fatal(err)
	}

	// 声明集群中所有服务角色及其 gRPC 端口
	// key=服务名称，value=该服务的 gRPC 端口
	// 每个服务角色会有自己独立的 ServiceResolver（哈希环）
	services := map[string]int{
		"matching": 7235,
		"history":  7234,
		"frontend": 7233,
	}

	monitor := ringpop.NewMonitor(
		"matching",      // 本节点的服务角色
		services,        // 集群所有角色及其端口
		rp,
		store,
		myGossipAddr,   // gossip 广播地址
		myGRPCAddr,     // gRPC 广播地址
		10*time.Second, // maxJoinDuration：Bootstrap 超时
		3*time.Second,  // propagationTime：Gossip 传播估计时间
		time.Time{},    // joinTime：立即加入（零值表示无延迟）
		200,            // replicaPoints：虚拟节点数，建议 100~300
	)

	// 启动（阻塞直到加入 Gossip 环）
	monitor.Start()
	if err := monitor.WaitUntilInitialized(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Monitor started successfully")

	// ---- 第三步：使用 ServiceResolver 做路由 ----

	resolver, err := monitor.GetResolver("matching")
	if err != nil {
		log.Fatal(err)
	}

	// Lookup：给定一个 key，找到负责该 key 的 matching 节点
	// key 通常是：namespaceId + ":" + taskQueueName + ":" + taskType
	key := "namespace-abc:my-task-queue:1"
	host, err := resolver.Lookup(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Task queue '%s' is served by: %s\n", key, host.GetAddress())

	// 监听成员变化
	changeCh := make(chan *ringpop.ChangedEvent, 10)
	_ = resolver.AddListener("my-listener", changeCh)
	go func() {
		for event := range changeCh {
			fmt.Printf("Membership changed: +%d -%d ~%d\n",
				len(event.HostsAdded),
				len(event.HostsRemoved),
				len(event.HostsChanged),
			)
		}
	}()

	// 优雅下线
	time.Sleep(time.Hour)
	leaveAfter, _ := monitor.EvictSelfAt(time.Now().Add(5 * time.Second))
	time.Sleep(leaveAfter)
	monitor.Stop()
}
