package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// This adapter lends the current request only to the synchronous single Apply.
// It delegates to the installed allocator's existing managed writer/epoch and
// transaction. It never grows a Pager, stores a callback or creates an ID engine.
// Both fields are cleared before the call returns, including error and panic.
type ownedPreAppendPageAllocator struct {
	allocator *freelist.Allocator
	request   freelist.AllocationRequestCreditV1
}

func (a *ownedPreAppendPageAllocator) Alloc(hint uint64) (uint64, error) {
	if a == nil || a.allocator == nil || a.request == nil {
		return 0, freelist.ErrAllocationCertificateIncompleteV1
	}
	return a.allocator.ReserveCOWPageBeforePublicationV1(a.request, hint, false)
}

// stageScopedOwnedPointRoot uses the exact captured owner and the SAME C13
// Prepare/Apply. It creates only private images; the existing COW allocator
// reserves every actual ID without growing the installed Pager. A caller must
// already have admitted the full managed resident epoch and a concrete staged
// leaf producer. This helper grants no public finite admission, Pager capacity
// certificate, storage permission or permission to replay a partial Apply.
func (db *DB) stageScopedOwnedPointRoot(o *PreparedOwnedPointRoot, s *Snapshot, policy OrderedRootStoragePolicy, delta *batch.Batch, request freelist.AllocationRequestCreditV1, writer zipper.LeafPageLog) (root uint64, retired []uint64, metrics adaptive.Metrics, retErr error) {
	if request == nil {
		return 0, nil, metrics, freelist.ErrAllocationCertificateIncompleteV1
	}
	if err := o.checkedScopedContext(db, s, false); err != nil {
		return 0, nil, metrics, err
	}
	if s.idx.allocator == nil || !o.scopedPrepared || o.scopedApplied {
		return 0, nil, metrics, ErrPreparedRootPointProfileLimit
	}
	// The independently escaping interface receiver is a real allocator-tranche
	// control birth; it is admitted before its literal, never rounded together
	// with a workspace or output-image allocation.
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(ownedPreAppendPageAllocator{})), true)
	if err != nil {
		return 0, nil, metrics, err
	}
	if err := request.ReserveAllocation(control); err != nil {
		return 0, nil, metrics, err
	}
	a := &ownedPreAppendPageAllocator{allocator: s.idx.allocator, request: request}
	defer func() { a.allocator, a.request = nil, nil }()
	return db.applyScopedOwnedPointRootMode(o, s, policy, delta, a, writer, true)
}
