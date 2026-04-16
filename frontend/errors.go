package frontend

import "errors"

// ErrNoTask 队列为空，长轮询超时后无可用任务
var ErrNoTask = errors.New("no task available")
