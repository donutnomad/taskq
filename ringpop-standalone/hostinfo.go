package ringpop

import (
	"fmt"
	"math/bits"

	"github.com/dgryski/go-farm"
	rpmembership "github.com/temporalio/ringpop-go/membership"
)

// hostInfo 代表哈希环中的一个节点实例。
// 实现了两个接口：
//   - rpmembership.Member（ringpop 库要求的接口）
//   - HostInfo（我们自己的接口）
type hostInfo struct {
	addr           string            // ip:port（gRPC 监听地址）
	labels         map[string]string // Gossip 传播的 kv 标签（与 ringpop 共享，不可修改）
	labelsChecksum uint64            // labels 的校验和，用于快速比较成员是否变化
}

var _ rpmembership.Member = (*hostInfo)(nil)
var _ HostInfo = (*hostInfo)(nil)

func newHostInfo(addr string, labels map[string]string) *hostInfo {
	return &hostInfo{
		addr:           addr,
		labels:         labels,
		labelsChecksum: checksumLabels(labels),
	}
}

func (hi *hostInfo) GetAddress() string { return hi.addr }
func (hi *hostInfo) Identity() string   { return hi.addr }

// Label 实现 ringpop Membership 接口，读取 Gossip 标签
func (hi *hostInfo) Label(key string) (string, bool) {
	value, ok := hi.labels[key]
	return value, ok
}

// summary 返回用于日志的简短描述（去掉 role/port 标签，只保留自定义标签）
func (hi *hostInfo) summary() string {
	s := hi.GetAddress()
	for k, v := range hi.labels {
		switch k {
		case roleKey, portKey:
			// 这两个标签可以从上下文推断，日志中跳过
		default:
			s += fmt.Sprintf("[%s=%s]", k, v)
		}
	}
	return s
}

// checksumLabels 对 labels map 计算一个顺序无关的校验和。
// 用 XOR 合并每个 kv 对的哈希，使结果与迭代顺序无关。
func checksumLabels(labels map[string]string) uint64 {
	var c uint64
	for k, v := range labels {
		kfp := farm.Fingerprint64([]byte(k))
		vfp := farm.Fingerprint64([]byte(v))
		c ^= kfp + bits.RotateLeft64(vfp, 3)
	}
	return c
}
