package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// The SAME candidate continuation owns any accounting-only old-backing delta.
// Each installed replacement advances its original subphase before a yield;
// the caller drains its RemovalWork outside runtime.mu before reentering.
func (runtime *rootPublicationRuntimeV1) reserveNativeVisibleCapacityV6(phase *uint8, refund *primaryBackingRefundV6, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*runtime))+16) {
		return false, nil
	}
	if refund.owner != nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if *phase == 0 {
		if runtime.visibleHead != 0 && len(runtime.visibleMembers) == cap(runtime.visibleMembers) {
			length := uint64(len(runtime.visibleMembers))
			if !w.Reserve(1+2*length, 3*length*uint64(unsafe.Sizeof(uintptr(0)))+48) {
				return false, nil
			}
			remaining := copy(runtime.visibleMembers, runtime.visibleMembers[runtime.visibleHead:])
			clear(runtime.visibleMembers[remaining:])
			runtime.visibleMembers = runtime.visibleMembers[:remaining]
			runtime.visibleHead = 0
		}
		*phase = 1
	}
	if *phase == 1 {
		ready, err := growPrimaryRuntimeSliceWithWorkV6(runtime, &runtime.visibleMembers, len(runtime.visibleMembers)+1, refund, w)
		if !ready || err != nil {
			return false, err
		}
		*phase = 2
		if refund.owner != nil {
			return false, nil
		}
	}
	if *phase == 2 {
		ready, err := growPrimaryRuntimeBackingWithWorkV6(runtime, &runtime.debt, len(runtime.debt)+1, &runtime.debtBackingCapacity, refund, w)
		if !ready || err != nil {
			return false, err
		}
		*phase = 3
		if refund.owner != nil {
			return false, nil
		}
	}
	return true, nil
}
