package db

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"unsafe"
)

// growRootPublicationSlice admits full replacement backing while the old array
// remains live. The caller holds runtime.mu; copied immutable elements retain
// their own owners. Releasing the old array precedes its refund.
func growRootPublicationSlice[T any](runtime *rootPublicationRuntimeV1, values *[]T, need int) error {
	if need < 0 {
		return retainedalloc.ErrCapacity
	}
	if cap(*values) >= need {
		return nil
	}
	capacity := need
	if cap(*values) <= math.MaxInt/2 {
		capacity = cap(*values) * 2
	}
	if capacity < 4 {
		capacity = 4
	}
	if capacity < need {
		capacity = need
	}
	size := uint64(unsafe.Sizeof(*new(T)))
	if size != 0 && uint64(capacity) > math.MaxUint64/size {
		return retainedalloc.ErrCapacity
	}
	nextCharge := retainedalloc.AllocationCharge(uint64(capacity) * size)
	oldCharge := retainedalloc.AllocationCharge(uint64(cap(*values)) * size)
	if runtime.metadata != nil {
		if err := runtime.metadata.AddPending(nextCharge); err != nil {
			return err
		}
	}
	old := *values
	next := make([]T, len(old), capacity)
	copy(next, old)
	*values = next
	clear(old[:cap(old)])
	old = nil
	if runtime.metadata != nil {
		runtime.metadata.RemovePending(oldCharge)
		runtime.metadataCharge += nextCharge - oldCharge
	}
	return nil
}
func (runtime *rootPublicationRuntimeV1) reserveVisibleCapacity(withDebt bool) error {
	if runtime.visibleHead != 0 && len(runtime.visibleMembers) == cap(runtime.visibleMembers) {
		remaining := copy(runtime.visibleMembers, runtime.visibleMembers[runtime.visibleHead:])
		clear(runtime.visibleMembers[remaining:])
		runtime.visibleMembers = runtime.visibleMembers[:remaining]
		runtime.visibleHead = 0
	}
	if err := growRootPublicationSlice(runtime, &runtime.visibleMembers, len(runtime.visibleMembers)+1); err != nil {
		return err
	}
	if withDebt {
		return growRootPublicationSlice(runtime, &runtime.debt, len(runtime.debt)+1)
	}
	return nil
}

// Five actual visibility callback environments capture these immutable
// pointers; the nested allocator-activation callback has its own environment.
// The conservative rounded capacities cover each separately allocated closure.
type visibilityCallbackEnvironmentV6 struct {
	code    uintptr
	runtime *rootPublicationRuntimeV1
	member  *rootPublicationVisibleMemberV1
	cow     *freelist.PreparedCOWCandidateV1
	limits  *PreparedRootPublicationLimits
}

func visibleRootMetadataV6() uint64 {
	return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootPublicationVisibleMemberV1{}))) +
		5*retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(visibilityCallbackEnvironmentV6{}))) +
		retainedalloc.AllocationCharge(3*uint64(unsafe.Sizeof(uintptr(0))))
}
