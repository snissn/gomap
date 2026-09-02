package valuelog

import "sync/atomic"

type GrowBufferStats struct {
	CallsTotal                                            uint64
	ReallocCallsTotal                                     uint64
	RequestedBytesTotal                                   uint64
	AllocatedBytesTotal                                   uint64
	CopiedBytesTotal                                      uint64
	CapacityWasteBytesTotal                               uint64
	ReadAppendCompressedFallbackCallsTotal                uint64
	ReadAppendCompressedFallbackRequestedBytesTotal       uint64
	ReadAppendCompressedFallbackDstPresentCallsTotal      uint64
	ReadAppendCompressedFallbackDstFitCallsTotal          uint64
	ReadAppendCompressedFallbackDstFitRequestedBytesTotal uint64
	ReadAppendPayloadCallsTotal                           uint64
	ReadAppendPayloadRequestedBytesTotal                  uint64
	ReadAppendCurrentMmapDirectDecodeCallsTotal           uint64
	ReadAppendCurrentMmapDirectDecodeRequestedBytesTotal  uint64
	ReadAppendDecodedPayloadCallsTotal                    uint64
	ReadAppendDecodedPayloadRequestedBytesTotal           uint64
	ReadAppendDecodedPayloadDstPresentCallsTotal          uint64
	ReadAppendDecodedPayloadDstFitCallsTotal              uint64
	ReadAppendDecodedPayloadDstFitRequestedBytesTotal     uint64
	ReadAppendTemplateEncodedPayloadCallsTotal            uint64
	ReadAppendTemplateEncodedPayloadRequestedBytesTotal   uint64
}

var growCallsTotal atomic.Uint64
var growReallocCallsTotal atomic.Uint64
var growRequestedBytesTotal atomic.Uint64
var growAllocatedBytesTotal atomic.Uint64
var growCopiedBytesTotal atomic.Uint64
var growCapacityWasteBytesTotal atomic.Uint64
var growReadAppendCompressedFallbackCallsTotal atomic.Uint64
var growReadAppendCompressedFallbackRequestedBytesTotal atomic.Uint64
var growReadAppendCompressedFallbackDstPresentCallsTotal atomic.Uint64
var growReadAppendCompressedFallbackDstFitCallsTotal atomic.Uint64
var growReadAppendCompressedFallbackDstFitRequestedBytesTotal atomic.Uint64
var growReadAppendPayloadCallsTotal atomic.Uint64
var growReadAppendPayloadRequestedBytesTotal atomic.Uint64
var growReadAppendCurrentMmapDirectDecodeCallsTotal atomic.Uint64
var growReadAppendCurrentMmapDirectDecodeRequestedBytesTotal atomic.Uint64
var growReadAppendDecodedPayloadCallsTotal atomic.Uint64
var growReadAppendDecodedPayloadRequestedBytesTotal atomic.Uint64
var growReadAppendDecodedPayloadDstPresentCallsTotal atomic.Uint64
var growReadAppendDecodedPayloadDstFitCallsTotal atomic.Uint64
var growReadAppendDecodedPayloadDstFitRequestedBytesTotal atomic.Uint64
var growReadAppendTemplateEncodedPayloadCallsTotal atomic.Uint64
var growReadAppendTemplateEncodedPayloadRequestedBytesTotal atomic.Uint64

func GrowBufferStatsSnapshot() GrowBufferStats {
	return GrowBufferStats{
		CallsTotal:                                            growCallsTotal.Load(),
		ReallocCallsTotal:                                     growReallocCallsTotal.Load(),
		RequestedBytesTotal:                                   growRequestedBytesTotal.Load(),
		AllocatedBytesTotal:                                   growAllocatedBytesTotal.Load(),
		CopiedBytesTotal:                                      growCopiedBytesTotal.Load(),
		CapacityWasteBytesTotal:                               growCapacityWasteBytesTotal.Load(),
		ReadAppendCompressedFallbackCallsTotal:                growReadAppendCompressedFallbackCallsTotal.Load(),
		ReadAppendCompressedFallbackRequestedBytesTotal:       growReadAppendCompressedFallbackRequestedBytesTotal.Load(),
		ReadAppendCompressedFallbackDstPresentCallsTotal:      growReadAppendCompressedFallbackDstPresentCallsTotal.Load(),
		ReadAppendCompressedFallbackDstFitCallsTotal:          growReadAppendCompressedFallbackDstFitCallsTotal.Load(),
		ReadAppendCompressedFallbackDstFitRequestedBytesTotal: growReadAppendCompressedFallbackDstFitRequestedBytesTotal.Load(),
		ReadAppendPayloadCallsTotal:                           growReadAppendPayloadCallsTotal.Load(),
		ReadAppendPayloadRequestedBytesTotal:                  growReadAppendPayloadRequestedBytesTotal.Load(),
		ReadAppendCurrentMmapDirectDecodeCallsTotal:           growReadAppendCurrentMmapDirectDecodeCallsTotal.Load(),
		ReadAppendCurrentMmapDirectDecodeRequestedBytesTotal:  growReadAppendCurrentMmapDirectDecodeRequestedBytesTotal.Load(),
		ReadAppendDecodedPayloadCallsTotal:                    growReadAppendDecodedPayloadCallsTotal.Load(),
		ReadAppendDecodedPayloadRequestedBytesTotal:           growReadAppendDecodedPayloadRequestedBytesTotal.Load(),
		ReadAppendDecodedPayloadDstPresentCallsTotal:          growReadAppendDecodedPayloadDstPresentCallsTotal.Load(),
		ReadAppendDecodedPayloadDstFitCallsTotal:              growReadAppendDecodedPayloadDstFitCallsTotal.Load(),
		ReadAppendDecodedPayloadDstFitRequestedBytesTotal:     growReadAppendDecodedPayloadDstFitRequestedBytesTotal.Load(),
		ReadAppendTemplateEncodedPayloadCallsTotal:            growReadAppendTemplateEncodedPayloadCallsTotal.Load(),
		ReadAppendTemplateEncodedPayloadRequestedBytesTotal:   growReadAppendTemplateEncodedPayloadRequestedBytesTotal.Load(),
	}
}

func noteGrowReadAppendCompressedFallback(n int) {
	if n <= 0 {
		return
	}
	growReadAppendCompressedFallbackCallsTotal.Add(1)
	growReadAppendCompressedFallbackRequestedBytesTotal.Add(uint64(n))
}

func noteGrowReadAppendCompressedFallbackDst(dst []byte, n int) {
	if n <= 0 {
		return
	}
	if dst != nil {
		growReadAppendCompressedFallbackDstPresentCallsTotal.Add(1)
	}
	if cap(dst)-len(dst) >= n {
		growReadAppendCompressedFallbackDstFitCallsTotal.Add(1)
		growReadAppendCompressedFallbackDstFitRequestedBytesTotal.Add(uint64(n))
	}
}

func noteGrowReadAppendPayload(n int) {
	if n <= 0 {
		return
	}
	growReadAppendPayloadCallsTotal.Add(1)
	growReadAppendPayloadRequestedBytesTotal.Add(uint64(n))
}

func noteGrowReadAppendCurrentMmapDirectDecode(n int) {
	if n <= 0 {
		return
	}
	growReadAppendCurrentMmapDirectDecodeCallsTotal.Add(1)
	growReadAppendCurrentMmapDirectDecodeRequestedBytesTotal.Add(uint64(n))
}

func noteGrowReadAppendDecodedPayload(dst []byte, n int) {
	if n <= 0 {
		return
	}
	growReadAppendDecodedPayloadCallsTotal.Add(1)
	growReadAppendDecodedPayloadRequestedBytesTotal.Add(uint64(n))
	if dst != nil {
		growReadAppendDecodedPayloadDstPresentCallsTotal.Add(1)
	}
	if cap(dst)-len(dst) >= n {
		growReadAppendDecodedPayloadDstFitCallsTotal.Add(1)
		growReadAppendDecodedPayloadDstFitRequestedBytesTotal.Add(uint64(n))
	}
}

func noteGrowReadAppendTemplateEncodedPayload(n int) {
	if n <= 0 {
		return
	}
	growReadAppendTemplateEncodedPayloadCallsTotal.Add(1)
	growReadAppendTemplateEncodedPayloadRequestedBytesTotal.Add(uint64(n))
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
		dst = growRealloc(dst, newLen, func() int {
			newCap := cap(dst) * 2
			if newCap < newLen {
				newCap = newLen
			}
			return newCap
		}())
	}
	return dst[:newLen]
}

func growRealloc(dst []byte, newLen, newCap int) []byte {
	oldLen := len(dst)
	growReallocCallsTotal.Add(1)
	growAllocatedBytesTotal.Add(uint64(newCap))
	growCopiedBytesTotal.Add(uint64(oldLen))
	growCapacityWasteBytesTotal.Add(uint64(newCap - newLen))
	tmp := make([]byte, oldLen, newCap)
	copy(tmp, dst)
	return tmp
}
