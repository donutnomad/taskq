package membership

import (
	"fmt"
	"slices"
	"sort"

	"github.com/dgryski/go-farm"
)

// hashRing 是一个基于虚拟节点的一致性哈希环。
// 用 farm.Fingerprint32 作为哈希函数，与原 ringpop-go/hashring 行为一致。
// 写操作不是线程安全的；serviceResolver 构造新对象后整体原子替换。
type hashRing struct {
	replicaPoints int
	vnodes        []vnode
	members       map[string]struct{}
	hash          func([]byte) uint32
}

type vnode struct {
	hash uint32
	addr string
}

func newHashRing(replicaPoints int) *hashRing {
	return newHashRingWithHash(replicaPoints, farm.Fingerprint32)
}

func newHashRingWithHash(replicaPoints int, hash func([]byte) uint32) *hashRing {
	return &hashRing{
		replicaPoints: replicaPoints,
		members:       make(map[string]struct{}),
		hash:          hash,
	}
}

func (r *hashRing) AddMember(addr string) {
	if _, ok := r.members[addr]; ok {
		return
	}
	r.members[addr] = struct{}{}
	for i := range r.replicaPoints {
		key := fmt.Sprintf("%s-%d", addr, i)
		h := r.hash([]byte(key))
		r.vnodes = append(r.vnodes, vnode{hash: h, addr: addr})
	}
	sort.Slice(r.vnodes, func(i, j int) bool {
		if r.vnodes[i].hash == r.vnodes[j].hash {
			return r.vnodes[i].addr < r.vnodes[j].addr
		}
		return r.vnodes[i].hash < r.vnodes[j].hash
	})
}

func (r *hashRing) RemoveMember(addr string) {
	if _, ok := r.members[addr]; !ok {
		return
	}
	delete(r.members, addr)
	r.vnodes = slices.DeleteFunc(r.vnodes, func(v vnode) bool {
		return v.addr == addr
	})
}

// Lookup 返回负责 key 的节点地址（顺时针最近虚拟节点）
func (r *hashRing) Lookup(key string) (string, bool) {
	if len(r.vnodes) == 0 {
		return "", false
	}
	h := r.hash([]byte(key))
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].addr, true
}

// LookupN 返回 key 顺时针方向 n 个不重复的真实节点
func (r *hashRing) LookupN(key string, n int) []string {
	if len(r.members) == 0 {
		return nil
	}
	if n > len(r.members) {
		n = len(r.members)
	}
	h := r.hash([]byte(key))
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})

	seen := make(map[string]struct{}, n)
	result := make([]string, 0, n)
	for len(result) < n {
		if idx >= len(r.vnodes) {
			idx = 0
		}
		addr := r.vnodes[idx].addr
		if _, ok := seen[addr]; !ok {
			seen[addr] = struct{}{}
			result = append(result, addr)
		}
		idx++
	}
	return result
}
