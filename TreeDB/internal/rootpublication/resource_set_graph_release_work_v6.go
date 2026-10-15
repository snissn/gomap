package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// ReleaseFromWithWorkV6 consumes the SAME selected Set and retains its original
// metadata as the last-edge continuation. The parent must clear its Set alias
// before completing the returned accounting-only receipt.
func (set *StableResourceSet) ReleaseFromWithWorkV6(expected ResourceOwnerState, w *iterator.OrdinalScanWork) (StableResourceTokenReleaseCompletionV6, bool, error) {
	if w == nil {
		return StableResourceTokenReleaseCompletionV6{}, false, retainedalloc.ErrWorkRequired
	}
	return set.releaseSelectedGraphV6(expected, nil, w)
}

func (set *StableResourceSet) releaseSelectedGraphV6(expected ResourceOwnerState, counts map[ResourceKind]uint64, w *iterator.OrdinalScanWork) (StableResourceTokenReleaseCompletionV6, bool, error) {
	var empty StableResourceTokenReleaseCompletionV6
	if set == nil {
		return empty, true, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.cleanupDone.Load() {
		return empty, true, set.releaseResult()
	}
	a := set.metadata
	if !set.selected || a == nil {
		return empty, false, retainedalloc.ErrWorkRequired
	}
	root := &a.release
	if root.current == nil {
		if set.Owner() != expected {
			return empty, false, ErrResourceOwnership
		}
		// Native coordinator pin-accounting uses the protected visible-edge
		// consumption path. Last-Set retirement here cannot hide that full traversal.
		if w != nil && (counts != nil || len(set.entries) != 0 || len(set.pinHighWater) != 0 ||
			(set.extras != nil && len(set.extras.logicalMembershipEvidence) != 0)) {
			return empty, false, retainedalloc.ErrWorkRequired
		}
		if a.refs.Load() != 1 {
			return empty, false, ErrResourceOwnership
		}
		if err := a.cleanupResult(); err != nil {
			return empty, false, err
		}
		if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*set))+2*uint64(unsafe.Sizeof(*a))) {
			return empty, false, nil
		}
		if !set.owner.CompareAndSwap(uint32(expected), uint32(ResourceOwnerReleased)) {
			return empty, false, ErrResourceOwnership
		}
		set.adjustActivePinsByKindLocked(counts, false)
		if !a.drop() {
			panic("selected Set wrapper shared after exclusive claim")
		}
		*root = resourceReleaseFrameV6{value: set, allocation: a}
		root.current = root
	}
	completion, ready, err := root.continueWork(w)
	if err != nil {
		return empty, false, err
	}
	if !ready {
		return empty, false, nil
	}
	set.metadata = nil
	set.cleanupDone.Store(true)
	return StableResourceTokenReleaseCompletionV6{owner: completion.owner, charge: completion.charge}, true, nil
}

func (root *resourceReleaseFrameV6) stepSet(f *resourceReleaseFrameV6, set *StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
	switch f.phase {
	case 0:
		if set.extras != nil && f.index < len(set.extras.physical) {
			d := set.extras.physical[f.index]
			if d != nil {
				ready, e := root.releaseDirectory(d, w)
				if !ready || e != nil {
					return false, e
				}
				set.extras.physical[f.index] = nil
				set.releasedDirectoryLease = true
			}
			f.index++
		} else {
			f.index = 0
			f.phase = 1
		}
	case 1:
		if set.extras != nil && set.extras.empty != nil {
			ready, e := root.releaseDirectory(set.extras.empty, w)
			if !ready || e != nil {
				return false, e
			}
			set.extras.empty = nil
			set.releasedDirectoryLease = true
		}
		f.phase = 2
	case 2:
		// These evidence views borrow the kind roots. End their aliases before any
		// corresponding last kind/root allocation can be physically retired.
		if set.extras != nil {
			set.extras.logicalMembershipEvidence = nil
		}
		set.pinHighWater = nil
		f.phase = 3
	case 3:
		if set.kindViews != nil {
			n := set.kindViews
			if ready, e := root.child(n.allocation, n, w); !ready || e != nil {
				return false, e
			}
			set.kindViews = nil
		}
		f.phase = 4
	default:
		if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*set))) {
			return false, nil
		}
		set.entries, set.kindViews, set.extras, set.pinHighWater = nil, nil, nil, nil
		f.value = nil
	}
	return true, nil
}
