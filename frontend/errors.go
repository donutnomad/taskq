package frontend

import "errors"

// ErrNoTask 队列为空，长轮询超时后无可用任务
var ErrNoTask = errors.New("no task available")

// ErrPoolClosed 连接池已进入终态（Frontend 已 Close），不再新建连接
var ErrPoolClosed = errors.New("frontend: connection pool closed")
