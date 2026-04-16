package matching

import "errors"

// ErrNotOwner 本节点不拥有请求的分区
var ErrNotOwner = errors.New("partition not owned by this node")
