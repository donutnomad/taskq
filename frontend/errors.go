package frontend

import "errors"

// ErrNoTask 队列为空，长轮询超时后无可用任务
var ErrNoTask = errors.New("no task available")

// ErrPoolClosed 连接池已进入终态（Frontend 已 Close），不再新建连接
var ErrPoolClosed = errors.New("frontend: connection pool closed")

// ErrRetriesExhausted 表示 Enqueue、Ack 或 Nack 用完了重试次数，每次都遇到可重试的错误（分区换主中的
// FailedPrecondition、Aborted，或 Unavailable）。错误链上同时带着最后一次的 gRPC 错误。这是暂时性失败：
// 调用方稍后重试可能成功。
var ErrRetriesExhausted = errors.New("max retries exceeded")
