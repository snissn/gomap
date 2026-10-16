package rootpublication

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// These are the original Set flush/sync operations' genuinely empty selected
// branch. It inspects the actual original descriptor under its owner mutex;
// there is no callback to price or skip. Nonempty/inaccessible backing refuses
// before effects and needs the real retained traversal/callback engine.
func (set *StableResourceSet) FlushThroughWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	return set.emptyOperationWithWorkV6(w)
}

func (set *StableResourceSet) SyncThroughWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	return set.emptyOperationWithWorkV6(w)
}

func NativeEmptyResourceOperationWorkV6() (uint64, uint64) {
	return 3, uint64(unsafe.Sizeof(StableResourceSet{})) + uint64(unsafe.Sizeof(resourceKindBacking{})) + uint64(unsafe.Sizeof(resourceAllocation{})) + uint64(unsafe.Sizeof(stableResourceSetExtras{})) + uint64(unsafe.Sizeof(DependencyDirectoryV2{}))
}

func (set *StableResourceSet) emptyOperationWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	if set == nil {
		return true, nil
	}
	records, bytes := NativeEmptyResourceOperationWorkV6()
	if !w.Reserve(records, bytes) {
		return false, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if !set.selected || set.metadata == nil || !set.selectedKindViewsAccessibleLocked() || set.Owner() == ResourceOwnerReleased || set.Owner() == ResourceOwnerTransferred {
		return false, ErrResourceOwnership
	}
	if len(set.entries) != 0 || set.kindViews != nil && len(set.kindViews.cells) != 0 {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	if set.extras != nil {
		if len(set.extras.physical) != 0 || len(set.extras.logicalMembershipEvidence) != 0 {
			return false, ErrNativeMVCCRootWorkRequiredV6
		}
		if d := set.extras.empty; d != nil && (d.refs.Load() <= 0 || d.ref.PhysicalCount != 0 || d.ref.LogicalCount != 0) {
			return false, ErrNativeMVCCRootWorkRequiredV6
		}
	}
	// rangeEntriesLocked's canonical selected empty branch performs no token
	// operation and returns success. No iterator/vector/error slice is built.
	return true, nil
}
