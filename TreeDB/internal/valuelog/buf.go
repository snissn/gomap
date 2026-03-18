package valuelog

import "sync/atomic"

type GrowBufferStats struct {
	CallsTotal              uint64
	ReallocCallsTotal       uint64
	RequestedBytesTotal     uint64
	AllocatedBytesTotal     uint64
	CopiedBytesTotal        uint64
	CapacityWasteBytesTotal uint64
}

var growCallsTotal atomic.Uint64
var growReallocCallsTotal atomic.Uint64
var growRequestedBytesTotal atomic.Uint64
var growAllocatedBytesTotal atomic.Uint64
var growCopiedBytesTotal atomic.Uint64
var growCapacityWasteBytesTotal atomic.Uint64

func GrowBufferStatsSnapshot() GrowBufferStats {
	return GrowBufferStats{
		CallsTotal:              growCallsTotal.Load(),
		ReallocCallsTotal:       growReallocCallsTotal.Load(),
		RequestedBytesTotal:     growRequestedBytesTotal.Load(),
		AllocatedBytesTotal:     growAllocatedBytesTotal.Load(),
		CopiedBytesTotal:        growCopiedBytesTotal.Load(),
		CapacityWasteBytesTotal: growCapacityWasteBytesTotal.Load(),
	}
}

func grow(dst []byte, n int) []byte {
	if n <= 0 {
		return dst
	}
	growCallsTotal.Add(1)
	growRequestedBytesTotal.Add(uint64(n))
	oldLen := len(dst)
	newLen := oldLen + n
	if newLen < 0 {
		return dst[:0]
	}
	if cap(dst) < newLen {
		newCap := cap(dst) * 2
		if newCap < newLen {
			newCap = newLen
		}
		growReallocCallsTotal.Add(1)
		growAllocatedBytesTotal.Add(uint64(newCap))
		growCopiedBytesTotal.Add(uint64(oldLen))
		growCapacityWasteBytesTotal.Add(uint64(newCap - newLen))
		tmp := make([]byte, oldLen, newCap)
		copy(tmp, dst)
		dst = tmp
	}
	return dst[:newLen]
}
