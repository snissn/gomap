package freelist

import (
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

// Materialization backing has the candidate/generation creator's lifetime.
// Scratch uses a distinct short resident scope, never the candidate creator.
// Cumulative request debit is never refunded at publication.
// Public finite preparation remains refused until the whole caller fits.
func reserveMaterializationBackingV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1, count, width uint64, scan bool) error {
	if creator == nil || count == 0 {
		return nil
	}
	if width != 0 && count > math.MaxUint64/width {
		return ErrNoAllocatablePage
	}
	raw := count * width
	if raw > uint64(^uint(0)>>1) {
		return ErrNoAllocatablePage
	}
	return creator.reserve(request, allocationClassV1(raw, scan), 0)
}

func reserveMaterializationObjectsV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1, count, raw uint64, scan bool) error {
	if creator == nil || count == 0 {
		return nil
	}
	if raw > uint64(^uint(0)>>1) {
		return ErrNoAllocatablePage
	}
	class := allocationClassV1(raw, scan)
	if class > uint64(^uint(0)>>1) {
		return ErrNoAllocatablePage
	}
	if class != 0 && count > math.MaxUint64/class {
		return ErrNoAllocatablePage
	}
	return creator.reserve(request, count*class, 0)
}

func countUnmaterializedStateKindsV1(root stateRefV1) (branches, chunks uint64) {
	if root.zero() || root.pageID() != 0 {
		return 0, 0
	}
	if root.chunk != nil {
		return 0, 1
	}
	branches = 1
	for _, child := range root.branch.child {
		b, c := countUnmaterializedStateKindsV1(child)
		branches += b
		chunks += c
	}
	return
}

// Charge every materializer-owned output and transient before the first sink
// write. Full capacity is paid, including unused vector cells and scratch that
// could escape to the heap. Header/tree births have their own intrinsic refs.
func (t *FreelistTxn) reserveMaterializationOutputsV1(request AllocationRequestCreditV1, scratch *AllocationCreatorV1, metadataPages, recordPages uint64) error {
	if err := t.requireBirthCreditV1(request); err != nil {
		return err
	}
	if t.buildCreator == nil {
		return nil
	}
	branches, chunks := countUnmaterializedStateKindsV1(t.root)
	if t.root.zero() {
		chunks = 1
	}
	creator := t.buildCreator
	charges := [...]struct {
		count, width uint64
		scan         bool
	}{
		{metadataPages, uint64(unsafe.Sizeof(candidatePageV1{})), true},
		{metadataPages, 8, false}, // generation metadata IDs
		{metadataPages, 8, false}, // candidate dirty IDs
		{recordPages, 8, false},   // reservation IDs
	}
	for _, charge := range charges {
		if err := reserveMaterializationBackingV1(request, creator, charge.count, charge.width, charge.scan); err != nil {
			return err
		}
	}
	scratchCharges := [...]struct {
		count, width uint64
		scan         bool
	}{
		{recordPages, uint64(unsafe.Sizeof([]byte{})), true},

		{1, uint64(unsafe.Sizeof(recordingSink{})), true},
		{1, 120, false},                  // pinned non-boring Go SHA-256 reservation digest control
		{1, 120, false},                  // generation digest control, conservatively heap charged
		{1, page.PageSize, false},        // canonical reservation digest scratch
		{1, generationHeaderSize, false}, // generation digest scratch
	}
	for _, charge := range scratchCharges {
		if err := reserveAllocationScratchV1(request, scratch, creator, charge.count, charge.width, charge.scan); err != nil {
			return err
		}
	}
	if err := reserveMaterializationObjectsV1(request, creator, cowSaturatingAddV1(cowSaturatingAddV1(chunks, recordPages), 1), page.PageSize, false); err != nil {
		return err
	}
	if err := reserveMaterializationObjectsV1(request, creator, branches, uint64(unsafe.Sizeof(indexPagePlanV1{})), false); err != nil {
		return err
	}
	if branches != 0 {
		if err := reserveAllocationScratchV1(request, scratch, creator, 1, page.PageSize, false); err != nil {
			return err
		}
	}
	return nil
}

// The caller lends a separately constructed short resident scope from the same
// installed owner. Pointer inequality rejects the permanent creator itself;
// interface satisfaction alone never certifies installed-owner provenance.
// The caller releases scratch only after every full-capacity alias is scrubbed.
func reserveAllocationScratchV1(request AllocationRequestCreditV1, scratch, permanent *AllocationCreatorV1, count, width uint64, scan bool) error {
	if permanent == nil {
		return nil
	}
	if scratch == nil || scratch == permanent {
		return ErrAllocationCertificateIncompleteV1
	}
	return reserveMaterializationBackingV1(request, scratch, count, width, scan)
}
