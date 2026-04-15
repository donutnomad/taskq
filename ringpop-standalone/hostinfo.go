package ringpop

import "github.com/dgryski/go-farm"

// hostInfo 代表哈希环中的一个节点实例，实现 HostInfo 接口。
type hostInfo struct {
	addr           string
	draining       bool
	startAt        int64
	stopAt         int64
	labelsChecksum uint64
}

var _ HostInfo = (*hostInfo)(nil)

func newHostInfo(addr string, draining bool, startAt, stopAt int64) *hostInfo {
	h := &hostInfo{
		addr:     addr,
		draining: draining,
		startAt:  startAt,
		stopAt:   stopAt,
	}
	h.labelsChecksum = h.computeChecksum()
	return h
}

func (hi *hostInfo) GetAddress() string { return hi.addr }
func (hi *hostInfo) Identity() string   { return hi.addr }

func (hi *hostInfo) computeChecksum() uint64 {
	var c uint64
	c ^= farm.Fingerprint64([]byte(hi.addr))
	if hi.draining {
		c ^= 0x1
	}
	var buf [16]byte
	for i, v := range []int64{hi.startAt, hi.stopAt} {
		for j := range 8 {
			buf[i*8+j] = byte(v >> (j * 8))
		}
	}
	c ^= farm.Fingerprint64(buf[:])
	return c
}

func isDraining(host *hostInfo) bool {
	return host.draining
}
