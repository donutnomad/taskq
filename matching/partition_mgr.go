package matching

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/donutnomad/taskq/membership"
	"github.com/donutnomad/taskq/queue"
)

// partitionKey 返回用于一致性哈希查找的 key
func partitionKey(queueName string, partitionID int) string {
	return fmt.Sprintf("%s/%d", queueName, partitionID)
}

type partitionManager struct {
	mu           sync.RWMutex
	store        queue.Store
	selfAddr     string // 本节点广播地址
	leaseOwner   string // DB lease 标识
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
	leaseOwner string,
	resolver membership.ServiceResolver,
	visTimeout time.Duration,
	queueConfigs map[string]int,
) *partitionManager {
	ctx, cancel := context.WithCancel(ctx)
	return &partitionManager{
		store:        store,
		selfAddr:     selfAddr,
		leaseOwner:   leaseOwner,
		resolver:     resolver,
		visTimeout:   visTimeout,
		partitions:   make(map[string]*queue.PhysicalQueue),
		queueConfigs: queueConfigs,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// isOwner 检查本节点是否拥有指定分区
func (pm *partitionManager) isOwner(queueName string, partitionID int) (bool, error) {
	host, err := pm.resolver.Lookup(partitionKey(queueName, partitionID))
	if err != nil {
		return false, err
	}
	return host.GetAddress() == pm.selfAddr, nil
}

func (pm *partitionManager) validatePartition(queueName string, partitionID int) error {
	numPartitions, ok := pm.queueConfigs[queueName]
	if !ok {
		return ErrUnknownQueue
	}
	if partitionID < 0 || partitionID >= numPartitions {
		return ErrInvalidPartition
	}
	return nil
}

// getOrLoadPartition 获取分区实例，如果本节点拥有但尚未加载则延迟加载
func (pm *partitionManager) getOrLoadPartition(queueName string, partitionID int) (*queue.PhysicalQueue, error) {
	if err := pm.validatePartition(queueName, partitionID); err != nil {
		return nil, err
	}

	key := partitionKey(queueName, partitionID)

	// 快路径：读锁查找
	pm.mu.RLock()
	pq, ok := pm.partitions[key]
	pm.mu.RUnlock()
	if ok {
		if pq.IsClosed() {
			pm.mu.Lock()
			if cur, stillOK := pm.partitions[key]; stillOK && cur == pq {
				delete(pm.partitions, key)
			}
			pm.mu.Unlock()
		} else {
			return pq, nil
		}
	}

	// 检查所有权
	isOwner, err := pm.isOwner(queueName, partitionID)
	if err != nil {
		return nil, fmt.Errorf("lookup owner for %s: %w", key, err)
	}
	if !isOwner {
		return nil, ErrNotOwner
	}

	// 慢路径：写锁加载
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// double check
	if pq, ok := pm.partitions[key]; ok {
		if pq.IsClosed() {
			delete(pm.partitions, key)
		} else {
			return pq, nil
		}
	}

	pq, err = queue.NewPhysicalQueue(pm.ctx, pm.store, queueName, partitionID, pm.visTimeout, pm.leaseOwner)
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
			isOwner, err := pm.isOwner(queueName, i)
			if err != nil {
				slog.Error("matching reconcile aborted: ownership lookup failed", "queue", queueName, "partition_id", i, "err", err)
				return
			}
			if isOwner {
				owned[partitionKey(queueName, i)] = partitionID{queueName, i}
			}
		}
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 卸载不再属于本节点的分区
	for key, pq := range pm.partitions {
		if _, ok := owned[key]; !ok {
			slog.Info("matching unloading partition", "partition", key)
			pq.Close()
			delete(pm.partitions, key)
		}
	}

	// 加载新分配给本节点的分区
	for key, pid := range owned {
		if _, ok := pm.partitions[key]; ok {
			continue
		}
		pq, err := queue.NewPhysicalQueue(pm.ctx, pm.store, pid.queueName, pid.partitionID, pm.visTimeout, pm.leaseOwner)
		if errors.Is(err, queue.ErrLeaseHeld) {
			continue // 正常竞争，其他节点持有 lease
		}
		if err != nil {
			slog.Error("matching failed to load partition", "partition", key, "err", err)
			continue
		}
		slog.Info("matching loaded partition", "partition", key)
		pm.partitions[key] = pq
	}
}

// getQueuePartitionCount 返回队列的分区数
func (pm *partitionManager) getQueuePartitionCount(queueName string) (int, bool) {
	n, ok := pm.queueConfigs[queueName]
	return n, ok
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
