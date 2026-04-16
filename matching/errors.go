package matching

import "errors"

// ErrNotOwner 本节点不拥有请求的分区
var ErrNotOwner = errors.New("partition not owned by this node")

// ErrUnknownQueue 请求了未配置的队列
var ErrUnknownQueue = errors.New("queue not found")

// ErrInvalidPartition 请求了越界的分区
var ErrInvalidPartition = errors.New("invalid partition")
