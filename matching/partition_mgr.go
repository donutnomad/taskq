package matching

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
)

// ErrNotOwner 本节点不拥有请求的分区
var ErrNotOwner = errors.New("partition not owned by this node")

// partitionKey 返回用于一致性哈希查找的 key
func partitionKey(queueName string, partitionID int) string {
	return fmt.Sprintf("%s/%d", queueName, partitionID)
}

type partitionManager struct {
	mu           sync.RWMutex
	store        queue.Store
	selfAddr     string // 本节点广播地址
	resolver     membership.ServiceResolver
	visTimeout   time.Duration
	partitions   map[string]*queue.PhysicalQueue // key = partitionKey(...)
	queueConfigs map[string]int                  // queueName → numPartitions
	ctx          context.Context
	cancel       context.CancelFunc
}

func newPartitionManager(
	ctx context.Context,
	store queue.Store,
	selfAddr string,
	resolver membership.ServiceResolver,
	visTimeout time.Duration,
	queueConfigs map[string]int,
) *partitionManager {
	ctx, cancel := context.WithCancel(ctx)
	return &partitionManager{
		store:        store,
		selfAddr:     selfAddr,
		resolver:     resolver,
		visTimeout:   visTimeout,
		partitions:   make(map[string]*queue.PhysicalQueue),
		queueConfigs: queueConfigs,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// isOwner 检查本节点是否拥有指定分区
func (pm *partitionManager) isOwner(queueName string, partitionID int) bool {
	host, err := pm.resolver.Lookup(partitionKey(queueName, partitionID))
	if err != nil {
		return false
	}
	return host.GetAddress() == pm.selfAddr
}

// getOrLoadPartition 获取分区实例，如果本节点拥有但尚未加载则延迟加载
func (pm *partitionManager) getOrLoadPartition(queueName string, partitionID int) (*queue.PhysicalQueue, error) {
	key := partitionKey(queueName, partitionID)

	// 快路径：读锁查找
	pm.mu.RLock()
	if pq, ok := pm.partitions[key]; ok {
		pm.mu.RUnlock()
		return pq, nil
	}
	pm.mu.RUnlock()

	// 检查所有权
	if !pm.isOwner(queueName, partitionID) {
		return nil, ErrNotOwner
	}

	// 慢路径：写锁加载
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// double check
	if pq, ok := pm.partitions[key]; ok {
		return pq, nil
	}

	pq, err := queue.NewPhysicalQueue(pm.ctx, pm.store, queueName, partitionID, pm.visTimeout)
	if err != nil {
		return nil, fmt.Errorf("load partition %s: %w", key, err)
	}
	pm.partitions[key] = pq
	return pq, nil
}

type partitionID struct {
	queueName   string
	partitionID int
}

// reconcile 对比哈希环分配结果，加载新分区、卸载失去的分区
func (pm *partitionManager) reconcile() {
	// 计算本节点应拥有的分区
	owned := make(map[string]partitionID)
	for queueName, numParts := range pm.queueConfigs {
		for i := range numParts {
			if pm.isOwner(queueName, i) {
				owned[partitionKey(queueName, i)] = partitionID{queueName, i}
			}
		}
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 卸载不再属于本节点的分区
	for key, pq := range pm.partitions {
		if _, ok := owned[key]; !ok {
			log.Printf("matching: unloading partition %s", key)
			pq.Close()
			delete(pm.partitions, key)
		}
	}

	// 加载新分配给本节点的分区
	for key, pid := range owned {
		if _, ok := pm.partitions[key]; ok {
			continue
		}
		pq, err := queue.NewPhysicalQueue(pm.ctx, pm.store, pid.queueName, pid.partitionID, pm.visTimeout)
		if err != nil {
			log.Printf("matching: failed to load partition %s: %v", key, err)
			continue
		}
		log.Printf("matching: loaded partition %s", key)
		pm.partitions[key] = pq
	}
}

// close 关闭所有分区
func (pm *partitionManager) close() {
	pm.cancel()
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for key, pq := range pm.partitions {
		pq.Close()
		delete(pm.partitions, key)
	}
}
