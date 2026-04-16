package queue

import (
	"errors"
	"fmt"
)

// ErrConditionFailed 乐观锁冲突错误
var ErrConditionFailed = errors.New("condition failed: range_id mismatch")

// ErrLeaseHeld 分区 lease 被其他节点持有
var ErrLeaseHeld = errors.New("partition lease held by another owner")

// ErrOwnershipLost 表示当前进程已失去分区所有权（另一个进程抢占了 rangeID）。
var ErrOwnershipLost = fmt.Errorf("ownership lost: %w", ErrConditionFailed)

// ErrQueueClosed 队列已关闭
var ErrQueueClosed = errors.New("queue closed")
