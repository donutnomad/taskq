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

// ErrInvalidToken 表示 ack/nack token 无效。
var ErrInvalidToken = errors.New("invalid ack token")

// ErrStaleToken 表示 token 来自旧的分区所有权任期。
var ErrStaleToken = errors.New("stale ack token")

// ErrTaskNotOutstanding 表示任务当前不在处理中，通常是重复 ack/nack 或过期 token。
var ErrTaskNotOutstanding = errors.New("task not outstanding")

// ErrQueueClosed 队列已关闭
var ErrQueueClosed = errors.New("queue closed")

var ErrTaskNotFound = errors.New("task not found")
