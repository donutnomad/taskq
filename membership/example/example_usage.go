// 这个文件演示如何使用 membership 包构建自己的分布式节点发现系统（DB 轮询版，无 Gossip 依赖）。
// 运行方式：go run ./membership/example/
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/netip"
	"time"

	"github.com/donutnomad/taskq/membership"
)

// ---- 第一步：实现 MemberStore 接口 ----

type MySQLMemberStore struct {
	// db *sql.DB
}

func (s *MySQLMemberStore) UpsertMember(ctx context.Context, req membership.UpsertMemberRequest) error {
	// INSERT INTO cluster_members
	//   (service_name, rpc_address, rpc_port, host_id, session_start, expires_at, draining, start_at, stop_at)
	// VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	// ON DUPLICATE KEY UPDATE rpc_address=?, rpc_port=?, session_start=?, expires_at=?, draining=?, start_at=?, stop_at=?
	fmt.Printf("[DB] upsert member: %s at %s:%d (draining=%v)\n",
		req.ServiceName, req.RPCAddress, req.RPCPort, req.Draining)
	return nil
}

func (s *MySQLMemberStore) GetActiveMembers(ctx context.Context, lastHeartbeatWithin time.Duration) ([]membership.MemberRecord, error) {
	// SELECT service_name, rpc_address, rpc_port, draining, start_at, stop_at
	// FROM cluster_members
	// WHERE updated_at > NOW() - INTERVAL ? SECOND
	return []membership.MemberRecord{
		{RPCAddress: net.ParseIP("10.0.0.1"), RPCPort: 7235, ServiceName: "matching"},
	}, nil
}

// ---- 第二步：启动 Monitor ----

func main() {
	store := &MySQLMemberStore{}

	myGRPCAddr := netip.MustParseAddrPort("10.0.0.2:7235") // 本节点 gRPC 地址

	monitor := membership.NewMonitor(
		[]string{"matching", "history", "frontend"}, // 本节点承担的服务角色列表
		store,
		myGRPCAddr,    // gRPC 广播地址
		5*time.Second, // pollInterval：DB 轮询间隔
		3*time.Second, // propagationTime：变更传播估计时间
		time.Time{},   // joinTime：立即加入（零值表示无延迟）
		200,           // replicaPoints：虚拟节点数
	)

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

	key := "namespace-abc:my-task-queue:1"
	host, err := resolver.Lookup(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Task queue '%s' is served by: %s\n", key, host.GetAddress())

	// 监听成员变化
	changeCh := make(chan *membership.ChangedEvent, 10)
	_ = resolver.AddListener("my-listener", changeCh)
	go func() {
		for event := range changeCh {
			fmt.Printf("Membership changed: +%d -%d ~%d\n",
				len(event.HostsAdded), len(event.HostsRemoved), len(event.HostsChanged))
		}
	}()

	// 优雅下线
	time.Sleep(time.Hour)
	leaveAfter, _ := monitor.EvictSelfAt(time.Now().Add(5 * time.Second))
	time.Sleep(leaveAfter)
	monitor.Stop()
}
