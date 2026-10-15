package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// CloneAdmittedStableResourceSetWithOrdinalWorkV6 clones the existing admitted
// immutable descriptor, including its complete logical proof. It shares the
// SAME backing instead of enumerating physical files or historical obligations.
// Generic legacy closures need their ordinary derivation and explicitly refuse
// this capability. Work is reserved under the source guard before any new edge.
func CloneAdmittedStableResourceSetWithOrdinalWorkV6(source *StableResourceSet, w *iterator.OrdinalScanWork) (*StableResourceSet, bool, error) {
	if w == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceSet{}))+2*uint64(unsafe.Sizeof(resourceAllocation{}))) {
		return nil, false, nil
	}
	if source == nil {
		return nil, true, nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	owner := source.Owner()
	if source.physicalOnly || owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return nil, false, ErrResourceOwnership
	}
	if source.metadata == nil || !source.selected {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	// Empty selected directories own a real directory edge; sharing the same
	// admitted wrapper constructor preserves its existing independent lifetime.
	if directory := source.emptyDependencyDirectoryLocked(); directory != nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceSet{}))+uint64(unsafe.Sizeof(resourceAllocation{}))+uint64(unsafe.Sizeof(stableResourceSetExtras{}))+64) {
			return nil, false, nil
		}
		// Reserve the possible nonlast edge rollback BEFORE retaining. source.mu
		// keeps the original sibling edge alive through Add refusal/capacity error.
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(directory.refs))) {
			return nil, false, nil
		}
		for {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(directory.refs))) {
				return nil, false, nil
			}
			n := directory.refs.Load()
			if n <= 0 || n == math.MaxInt64 {
				return nil, false, ErrResourceOwnership
			}
			if directory.refs.CompareAndSwap(n, n+1) {
				break
			}
		}
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceSet{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableResourceSetExtras{})))
		allocation, err := newResourceAllocationWithWorkV6(source.metadata.owner, charge, w)
		if err != nil {
			directory.refs.Add(-1)
			return nil, false, err
		}
		set := &StableResourceSet{metadata: allocation, selected: true, extras: &stableResourceSetExtras{empty: directory}}
		set.owner.Store(uint32(ResourceOwnerBuilder))
		return set, true, nil
	}
	views := source.kindViews
	if views == nil || views.allocation == nil || views.allocation.owner != source.metadata.owner {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceSet{}))+uint64(unsafe.Sizeof(resourceAllocation{}))+uint64(unsafe.Sizeof(resourceKindBacking{}))+64) {
		return nil, false, nil
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(views.allocation.refs))) {
		return nil, false, nil
	}
	if ready, err := views.allocation.retainWithWorkV6(w); !ready || err != nil {
		return nil, false, err
	}
	set, err := newStableResourceSetFromKindViewsWithWorkV6(views, w)
	if err != nil {
		// The source still owns its edge under source.mu; this cannot be last.
		views.allocation.refs.Add(-1)
	}
	return set, err == nil, err
}
