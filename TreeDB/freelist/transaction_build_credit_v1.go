package freelist

import (
	"sync/atomic"
	"unsafe"
)

// bindBuildCreatorV1 lends one future-birth edge to an already managed builder.
// Call only under its allocator/transaction synchronization and after complete
// operation admission. It does not adopt resident objects or open finite gates.
func (t *FreelistTxn) bindBuildCreatorV1(creator *allocationCreditLeaseV1) error {
	if err := t.valid(); err != nil {
		return err
	}
	if t.buildCreator == creator {
		return nil
	}
	if t.buildCreator != nil {
		return ErrCandidateConsumed
	}
	if creator == nil {
		return nil
	}
	t.ledger.mu.Lock()
	escaped := t.ledger.rawEscaped
	t.ledger.mu.Unlock()
	if escaped || atomic.LoadUint32(&t.base.escaped) != 0 {
		return ErrFiniteAllocationExportV1
	}
	if err := creator.retainReferencesV1(1); err != nil {
		return err
	}
	// New birth authority does not prove existing dirty aliases are private.
	t.privatePreparation = false
	t.buildCreator = creator
	t.allocationRequired = true
	return nil
}

// endBuildCreatorV1 requires the exact active creator and revokes private edit
// permission before releasing the mutable edge. Ambiguous owners must not call
// this seam; intrinsic backing keeps its original creating owner.
func (t *FreelistTxn) endBuildCreatorV1(creator *allocationCreditLeaseV1) error {
	if t == nil {
		return ErrGenerationFormat
	}
	if t.buildCreator == nil {
		return nil
	}
	if t.buildCreator != creator {
		return ErrCandidateConsumed
	}
	t.privatePreparation = false
	t.buildCreator = nil
	creator.release()
	return nil
}

// requireBirthCreditV1 distinguishes ordinary transactions from credited origin
// after the mutable epoch edge has ended. This is an O(1) authority check; old
// intrinsic backing retains its creator and confers no future birth authority.
func (t *FreelistTxn) requireBirthCreditV1(request AllocationRequestCreditV1) error {
	if t == nil {
		return ErrGenerationFormat
	}
	if t.allocationRequired && (request == nil || t.buildCreator == nil) {
		return ErrAllocationCertificateIncompleteV1
	}
	return nil
}

// growAppendVectorsV1 reserves both new buffers and their intrinsic creating
// edges as one operation before high-water/extent edits. Ordinary and finite
// transactions use the same explicit capacity growth.
func (t *FreelistTxn) growAppendVectorsV1(request AllocationRequestCreditV1, allocatedAdditional, abandonedAdditional int) error {
	if err := t.requireBirthCreditV1(request); err != nil {
		return err
	}
	maximum := int(^uint(0) >> 1)
	if allocatedAdditional < 0 || abandonedAdditional < 0 || allocatedAdditional > maximum-len(t.allocated) || abandonedAdditional > maximum-len(t.abandonedAppends) {
		return ErrNoAllocatablePage
	}
	allocatedCapacity := grownCapacityV1(cap(t.allocated), len(t.allocated)+allocatedAdditional)
	abandonedCapacity := grownCapacityV1(cap(t.abandonedAppends), len(t.abandonedAppends)+abandonedAdditional)
	if allocatedCapacity > maximum/int(unsafe.Sizeof(allocatedPage{})) || abandonedCapacity > maximum/int(unsafe.Sizeof(ReservationExtentV1{})) {
		return ErrNoAllocatablePage
	}
	bytes, refs := uint64(0), uint64(0)
	if allocatedCapacity > cap(t.allocated) {
		bytes += allocationClassV1(uint64(allocatedCapacity)*uint64(unsafe.Sizeof(allocatedPage{})), false)
		refs++
	}
	if abandonedCapacity > cap(t.abandonedAppends) {
		bytes += allocationClassV1(uint64(abandonedCapacity)*uint64(unsafe.Sizeof(ReservationExtentV1{})), false)
		refs++
	}
	if refs == 0 {
		return nil
	}
	operation, err := admitAllocationOperationV1(request, t.buildCreator, bytes, refs)
	if err != nil {
		return err
	}
	defer operation.close()
	if allocatedCapacity > cap(t.allocated) {
		size := allocationClassV1(uint64(allocatedCapacity)*uint64(unsafe.Sizeof(allocatedPage{})), false)
		if err := operation.take(size, 1); err != nil {
			return err
		}
		next := make([]allocatedPage, len(t.allocated), allocatedCapacity)
		copy(next, t.allocated)
		oldCreator := t.allocatedCreator
		clear(t.allocated[:cap(t.allocated)])
		t.allocated, t.allocatedCreator = next, t.buildCreator
		oldCreator.release()
	}
	if abandonedCapacity > cap(t.abandonedAppends) {
		size := allocationClassV1(uint64(abandonedCapacity)*uint64(unsafe.Sizeof(ReservationExtentV1{})), false)
		if err := operation.take(size, 1); err != nil {
			return err
		}
		next := make([]ReservationExtentV1, len(t.abandonedAppends), abandonedCapacity)
		copy(next, t.abandonedAppends)
		oldCreator := t.abandonedCreator
		clear(t.abandonedAppends[:cap(t.abandonedAppends)])
		t.abandonedAppends, t.abandonedCreator = next, t.buildCreator
		oldCreator.release()
	}
	return nil
}
