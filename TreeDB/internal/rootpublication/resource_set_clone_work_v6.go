package rootpublication

import (
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
		set, err := newStableResourceSetWithEmptyDirectory(source.metadata.owner, directory)
		return set, err == nil, err
	}
	views := source.kindViews
	if views == nil || views.allocation == nil || views.allocation.owner != source.metadata.owner {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceSet{}))+uint64(unsafe.Sizeof(resourceAllocation{}))+uint64(unsafe.Sizeof(resourceKindBacking{}))+64) {
		return nil, false, nil
	}
	if !views.retain() {
		return nil, false, ErrResourceOwnership
	}
	set, err := newStableResourceSetFromKindViews(views)
	if err != nil {
		views.release()
	}
	return set, err == nil, err
}
