package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"math"
	"unsafe"
)

// growPrimaryRuntimeSlice admits full replacement backing while the old array
// remains live. The caller holds runtime.mu; copied immutable elements retain
// their own owners. Releasing the old array precedes its refund.
func growPrimaryRuntimeSlice[T any](runtime *rootPublicationRuntimeV1, values *[]T, need int) error {
	_, err := growPrimaryRuntimeSliceWithWorkV6(runtime, values, need, nil)
	return err
}

// The finite form uses this SAME runtime backing/owner. It prices capacity,
// replacement, copying and clearing before allocating; no publication operand
// or new retained owner is introduced. Ordinary nil-work behavior is unchanged.
func growPrimaryRuntimeSliceWithWorkV6[T any](runtime *rootPublicationRuntimeV1, values *[]T, need int, w *iterator.OrdinalScanWork) (bool, error) {
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(*values))+uint64(unsafe.Sizeof(need))) {
		return false, nil
	}
	if need < 0 {
		return false, retainedalloc.ErrCapacity
	}
	if cap(*values) >= need {
		return true, nil
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
		return false, retainedalloc.ErrCapacity
	}
	if w != nil {
		// All operands are nonnegative ints, but the byte sum can still
		// overflow even when the replacement allocation itself fits.
		length, oldCapacity := uint64(len(*values)), uint64(cap(*values))
		if length > (math.MaxUint64-uint64(capacity)-oldCapacity)/2 {
			return false, retainedalloc.ErrCapacity
		}
		units := uint64(capacity) + 2*length + oldCapacity
		headers := 2 * uint64(unsafe.Sizeof(*values))
		if size != 0 && units > (math.MaxUint64-headers)/size {
			return false, retainedalloc.ErrCapacity
		}
		if !w.Reserve(1+length+oldCapacity, size*units+headers) {
			return false, nil
		}
	}
	nextCharge := retainedalloc.AllocationCharge(uint64(capacity) * size)
	oldCharge := retainedalloc.AllocationCharge(uint64(cap(*values)) * size)
	if runtime.metadata != nil {
		if err := runtime.metadata.AddPending(nextCharge); err != nil {
			return false, err
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
	return true, nil
}
func (runtime *rootPublicationRuntimeV1) reserveVisibleCapacity(withDebt bool) error {
	if runtime.visibleHead != 0 && len(runtime.visibleMembers) == cap(runtime.visibleMembers) {
		remaining := copy(runtime.visibleMembers, runtime.visibleMembers[runtime.visibleHead:])
		clear(runtime.visibleMembers[remaining:])
		runtime.visibleMembers = runtime.visibleMembers[:remaining]
		runtime.visibleHead = 0
	}
	if err := growPrimaryRuntimeSlice(runtime, &runtime.visibleMembers, len(runtime.visibleMembers)+1); err != nil {
		return err
	}
	if withDebt {
		return growPrimaryRuntimeSlice(runtime, &runtime.debt, len(runtime.debt)+1)
	}
	return nil
}

// Pending descriptor and prefix charge survives physical arena shutdown until
// the actual publisher release clears every projection alias.
func primarySealMetadataV6(prefixCount int) uint64 {
	return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootPublicationSealV1{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryPublicationSealV5{}))) +
		retainedalloc.AllocationCharge(uint64(prefixCount)*uint64(unsafe.Sizeof(uintptr(0))))
}

// Five actual visibility callback environments capture these immutable
// pointers; the nested allocator-activation callback has its own environment.
// The conservative rounded capacities cover each separately allocated closure.
type primaryVisibilityCallbackEnvironmentV6 struct {
	code    uintptr
	runtime *rootPublicationRuntimeV1
	member  *rootPublicationVisibleMemberV1
	cow     *freelist.PreparedCOWCandidateV1
	limits  *PreparedRootPublicationLimits
}

func primaryVisibleMetadataV6() uint64 {
	return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootPublicationVisibleMemberV1{}))) +
		5*retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryVisibilityCallbackEnvironmentV6{}))) +
		retainedalloc.AllocationCharge(3*uint64(unsafe.Sizeof(uintptr(0))))
}

// The compatibility record uses the shared encoder and actual bank seal; the
// temporary image cannot escape into a caller or retained publication.
func sealPrimaryRecordV5(a *primaryarena.Arena, ref primaryarena.Ref, value rootpublication.DurablePrimaryRootRecordV5) ([32]byte, error) {
	charge := retainedalloc.AllocationCharge(page.PageSize)
	if err := a.MetadataOwner().AddPending(charge); err != nil {
		return [32]byte{}, err
	}
	image := make([]byte, page.PageSize)
	defer func() { clear(image); a.MetadataOwner().RemovePending(charge) }()
	digest, err := value.EncodePageInto(image, ref.PageID)
	if err != nil {
		return [32]byte{}, err
	}
	ready, err := a.SealMetadata(ref, image, nil)
	if !ready || err != nil {
		return [32]byte{}, errors.Join(err, rootpublication.ErrResourceOwnership)
	}
	return digest, nil
}
