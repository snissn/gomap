package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// The wrapper is an independently admitted allocation. Descriptor clones share
// actual immutable kind backing, but each public owner has its own disposal
// boundary. Construction consumes views only after full wrapper admission.
func newStableResourceSetFromKindViews(views *resourceKindBacking) (*StableResourceSet, error) {
	var allocation *resourceAllocation
	if views != nil && views.allocation != nil {
		var err error
		allocation, err = newResourceAllocation(views.allocation.owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceSet{}))))
		if err != nil {
			return nil, err
		}
	}
	result := &StableResourceSet{metadata: allocation, selected: allocation != nil, kindViews: views}
	if allocation == nil {
		result.pinHighWater = stableResourcePinCountsFromViews(views)
	}
	result.owner.Store(uint32(ResourceOwnerBuilder))
	return result, nil
}

// Requires the existing set mutex and no remaining public ownership. Actual
// descriptor borrowers independently retain the kind allocation through use.
func (set *StableResourceSet) disposeSelectedWrapperLocked() {
	if set.metadata == nil {
		return
	}
	allocation := set.metadata
	set.metadata = nil
	if allocation.drop() {
		allocation.refund()
	}
}

func (set *StableResourceSet) rangePinHighWaterLocked(visit func(ResourceKind, uint64)) {
	if set.selected {
		if !set.selectedKindViewsAccessibleLocked() {
			return
		}
		for kind, view := range set.kindViews.all() {
			visit(kind, uint64(view.count))
		}
		return
	}
	for kind, highWater := range set.pinHighWater {
		visit(kind, highWater)
	}
}

// Empty selected directories still carry real recovery custody. This wrapper
// owns its exact directory edge and its admitted extras allocation; no field
// or physical-token surrogate substitutes for that edge.
func newStableResourceSetWithEmptyDirectory(owner *retainedalloc.Owner, directory *DependencyDirectoryV2) (*StableResourceSet, error) {
	if directory == nil {
		return nil, ErrResourceOwnership
	}
	var allocation *resourceAllocation
	if owner != nil {
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableResourceSet{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableResourceSetExtras{})))
		var err error
		allocation, err = newResourceAllocation(owner, charge)
		if err != nil {
			return nil, err
		}
	}
	if err := directory.Retain(); err != nil {
		if allocation != nil && allocation.drop() {
			allocation.refund()
		}
		return nil, err
	}
	set := &StableResourceSet{metadata: allocation, selected: allocation != nil, extras: &stableResourceSetExtras{empty: directory}}
	set.owner.Store(uint32(ResourceOwnerBuilder))
	return set, nil
}

// The unchanged same-owner import is a descriptor view, not a second import
// lifecycle. Acquire its wrapper and exact immutable root while source.mu
// excludes release. No directory decode, handle acquisition or callbacks occur.
func cloneAdmittedResourceImport(owner *retainedalloc.Owner, source *StableResourceSet) (*StableResourceSet, bool, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.Owner() == ResourceOwnerReleased || source.Owner() == ResourceOwnerTransferred || source.physicalOnly {
		return nil, true, ErrResourceOwnership
	}
	if source.metadata == nil || source.metadata.owner != owner {
		return nil, false, nil
	}
	if source.emptyDependencyDirectoryLocked() != nil {
		set, err := newStableResourceSetWithEmptyDirectory(owner, source.emptyDependencyDirectoryLocked())
		return set, true, err
	}
	views := source.kindViews
	if views == nil || views.allocation == nil || views.allocation.owner != owner {
		return nil, true, ErrResourceOwnership
	}
	if !views.retain() {
		return nil, true, ErrResourceOwnership
	}
	set, err := newStableResourceSetFromKindViews(views)
	if err != nil {
		views.release()
	}
	return set, true, err
}

// MetadataOwner identifies the existing governing allocation owner while this
// set is accessible. It conveys metadata admission context only; physical,
// operation and deletion custody still belongs to each original producer.
func (set *StableResourceSet) MetadataOwner() *retainedalloc.Owner {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if !set.selected || set.metadata == nil || !set.selectedKindViewsAccessibleLocked() {
		return nil
	}
	return set.metadata.owner
}
