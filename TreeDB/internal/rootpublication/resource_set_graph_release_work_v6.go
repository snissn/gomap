package rootpublication

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

func (set *StableResourceSet) releaseSelectedGraphV6(expected ResourceOwnerState, counts map[ResourceKind]uint64) (StableResourceTokenReleaseCompletionV6, bool, error) {
	var empty StableResourceTokenReleaseCompletionV6
	if set == nil {
		return empty, true, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.cleanupDone.Load() {
		err := set.releaseResult()
		return empty, err == nil, err
	}
	a := set.metadata
	if !set.selected || a == nil {
		return empty, false, retainedalloc.ErrWorkRequired
	}
	root := &a.release
	if root.running {
		// A duplicate release cannot enter the same physical release actor.
		return empty, false, ErrResourceOwnership
	}
	if root.current == nil {
		if set.Owner() != expected {
			return empty, false, ErrResourceOwnership
		}
		if a.refs.Load() != 1 {
			return empty, false, ErrResourceOwnership
		}
		if err := a.cleanupResult(); err != nil {
			return empty, false, err
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
	// The released owner makes all selected public views inaccessible before
	// any graph field is detached. Keep only the original actor claim under
	// set.mu: physical providers and their callbacks may inspect this Set.
	root.running = true
	completion, ready, err := func() (resourceGraphReleaseCompletionV6, bool, error) {
		set.mu.Unlock()
		defer func() {
			set.mu.Lock()
			root.running = false
		}()
		return root.releaseGraph()
	}()
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

func (root *resourceReleaseFrameV6) stepSet(f *resourceReleaseFrameV6, set *StableResourceSet) (bool, error) {
	switch f.phase {
	case 0:
		if set.extras != nil && f.index < len(set.extras.physical) {
			d := set.extras.physical[f.index]
			if d != nil {
				ready, e := root.releaseDirectory(d)
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
			ready, e := root.releaseDirectory(set.extras.empty)
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
			if ready, e := root.child(n.allocation, n); !ready || e != nil {
				return false, e
			}
			set.kindViews = nil
		}
		f.phase = 4
	default:
		set.entries, set.kindViews, set.extras, set.pinHighWater = nil, nil, nil, nil
		f.value = nil
	}
	return true, nil
}
