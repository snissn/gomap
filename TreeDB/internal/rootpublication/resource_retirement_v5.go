package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// StableResourceRetirementV5 retains the exact immutable rope and its finite
// release cursor. Preparation checks callback eligibility without dropping any
// edge. A caller must finish preparation before activating this owned member.
// Ordinary release remains available only to close/recovery owners.
type StableResourceRetirementV5 struct {
	set                     *StableResourceSet
	roots                   []*stableResourceEntryNode
	stack                   []*stableResourceEntryNode
	current                 *stableResourceEntryNode
	entry                   int
	token                   *StableResourceToken
	tokenPhase              uint8
	prepared, started, done bool
}

func PrepareStableResourceRetirementV5(set *StableResourceSet, w *iterator.OrdinalScanWork) (*StableResourceRetirementV5, bool, error) {
	if set == nil || w == nil {
		return nil, false, ErrResourceOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceRetirementV5{}))+2*uint64(unsafe.Sizeof(StableResourceSet{}))+16*2*uint64(unsafe.Sizeof(stableResourceKindView{}))) {
		return nil, false, nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.ownedRetirementV5 != nil || set.kindViews == nil || len(set.kindViews) > 16 || set.physicalOnly || set.emptyDependencyDirectoryLocked() != nil || len(set.physicalDependencyDirectoriesLocked()) != 0 {
		return nil, false, ErrResourceOwnership
	}
	r := &StableResourceRetirementV5{set: set, roots: make([]*stableResourceEntryNode, 0, len(set.kindViews))}
	if !w.Reserve(uint64(len(set.kindViews)), uint64(len(set.kindViews))*2*uint64(unsafe.Sizeof(stableResourceKindView{}))) {
		return nil, false, nil
	}
	for _, view := range set.kindViews {
		r.roots = append(r.roots, view.root)
	}
	r.stack = append(r.stack, r.roots...)
	set.ownedRetirementV5 = r
	return r, true, nil
}

// PrepareStep visits the actual retained rope, not its current logical count.
// Thus shared old chunks and each physical token are part of the charged proof.
func (r *StableResourceRetirementV5) PrepareStep(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if r == nil || w == nil {
		return false, false, ErrResourceOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*r))+2*uint64(unsafe.Sizeof(StableResourceSet{}))) {
		return false, false, nil
	}
	if r.prepared {
		return true, false, nil
	}
	progress := false
	for {
		if r.current != nil && r.entry < len(r.current.entries) {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(stableResourceEntry{}))+2*uint64(unsafe.Sizeof(StableResourceToken{}))) {
				return false, progress, nil
			}
			t := r.current.entries[r.entry].token
			if t == nil || t.directory != nil || (t.onRelease != nil && t.ownedOnRelease == nil) {
				return false, progress, ErrResourceOwnership
			}
			r.entry++
			progress = true
			continue
		}
		r.current = nil
		r.entry = 0
		if len(r.stack) == 0 {
			r.prepared = true
			return true, progress, nil
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(stableResourceEntryNode{}))+4*uint64(unsafe.Sizeof((*stableResourceEntryNode)(nil)))) {
			return false, progress, nil
		}
		last := len(r.stack) - 1
		n := r.stack[last]
		r.stack = r.stack[:last]
		progress = true
		if n == nil {
			continue
		}
		if n.left != nil || n.right != nil {
			r.stack = append(r.stack, n.left, n.right)
		} else {
			r.current = n
		}
	}
}

// Step consumes only this set's Finish ownership. Shared node decrements use
// one reservation per CAS attempt. Last-node tokens retain a separate finite
// phase cursor, so no ordinary recursive release or nil-work callback runs.
func (r *StableResourceRetirementV5) Step(w *iterator.OrdinalScanWork) (bool, bool, error) {
	if r == nil || w == nil {
		return false, false, ErrResourceOwnership
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*r))+2*uint64(unsafe.Sizeof(StableResourceSet{}))) {
		return false, false, nil
	}
	if !r.prepared {
		return false, false, ErrResourceOwnership
	}
	if r.done {
		return true, false, nil
	}
	progress := false
	if !r.started {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*r))+2*uint64(unsafe.Sizeof(StableResourceSet{}))) {
			return false, false, nil
		}
		r.set.mu.Lock()
		if r.set.Owner() != ResourceOwnerFinish {
			r.set.mu.Unlock()
			return false, false, ErrResourceOwnership
		}
		// Keep Finish ownership until every physical operand/callback is retired.
		r.stack = append(r.stack[:0], r.roots...)
		r.started = true
		r.set.mu.Unlock()
		progress = true
	}
	for {
		if r.token != nil {
			ready, moved, e := r.releaseToken(w)
			progress = progress || moved
			if e != nil || !ready {
				return false, progress, e
			}
			r.token = nil
			r.tokenPhase = 0
			r.entry++
			continue
		}
		if r.current != nil && r.entry < len(r.current.entries) {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(stableResourceEntry{}))) {
				return false, progress, nil
			}
			r.token = r.current.entries[r.entry].token
			progress = true
			continue
		}
		r.current = nil
		r.entry = 0
		if len(r.stack) == 0 {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceSet{}))) {
				return false, progress, nil
			}
			r.set.mu.Lock()
			if !r.set.owner.CompareAndSwap(uint32(ResourceOwnerFinish), uint32(ResourceOwnerReleased)) {
				r.set.mu.Unlock()
				return false, progress, ErrResourceOwnership
			}
			r.done = true
			r.set.mu.Unlock()
			return true, true, nil
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(stableResourceEntryNode{}))+4*uint64(unsafe.Sizeof((*stableResourceEntryNode)(nil)))) {
			return false, progress, nil
		}
		last := len(r.stack) - 1
		n := r.stack[last]
		progress = true
		if n == nil {
			r.stack = r.stack[:last]
			continue
		}
		refs := n.refs.Load()
		if refs <= 0 {
			return false, progress, ErrResourceOwnership
		}
		if !n.refs.CompareAndSwap(refs, refs-1) {
			continue
		}
		r.stack = r.stack[:last]
		if refs != 1 {
			continue
		}
		if n.left != nil || n.right != nil {
			r.stack = append(r.stack, n.left, n.right)
		} else {
			r.current = n
		}
	}
}

func (r *StableResourceRetirementV5) releaseToken(w *iterator.OrdinalScanWork) (bool, bool, error) {
	t := r.token
	progress := false
	for {
		switch r.tokenPhase {
		case 0:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceToken{}))) {
				return false, progress, nil
			}
			if t == nil || !t.owner.CompareAndSwap(uint32(ResourceOwnerShared), uint32(ResourceOwnerFinish)) || t.released.Load() {
				return false, progress, ErrResourceOwnership
			}
		case 1:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceToken{}))+32) {
				return false, progress, nil
			}
			if t.pinnedRefs == nil || t.pinnedRefs.Add(-1) == 0 {
				r.tokenPhase = 2
				progress = true
				continue
			}
			r.tokenPhase = 3
			progress = true
			continue
		case 2:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceToken{}))+128) {
				return false, progress, nil
			}
			e := t.pinned.Close()
			r.tokenPhase++
			progress = true
			if e != nil {
				return false, progress, e
			}
			continue
		case 3:
			if t.namespace != nil {
				// This fixed primitive has one namespace edge and at most two handles.
				if !w.Reserve(3, 2*uint64(unsafe.Sizeof(StableNamespaceToken{}))+256) {
					return false, progress, nil
				}
				t.namespace.release()
			}
		case 4:
			if t.identityPin != nil {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(IdentityPin{}))+2*uint64(unsafe.Sizeof(IdentityPinRegistry{}))+256) {
					return false, progress, nil
				}
				t.identityPin.Release()
			}
		case 5:
			if t.directory != nil {
				return false, progress, ErrResourceOwnership
			}
			if t.ownedOnRelease != nil {
				ready, e := t.ownedOnRelease(w)
				if e != nil || !ready {
					return false, progress, e
				}
			} else if t.onRelease != nil {
				return false, progress, ErrResourceOwnership
			}
		case 6:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(StableResourceToken{}))) {
				return false, progress, nil
			}
			if !t.owner.CompareAndSwap(uint32(ResourceOwnerFinish), uint32(ResourceOwnerReleased)) {
				return false, progress, ErrResourceOwnership
			}
			t.released.Store(true)
		default:
			return true, progress, nil
		}
		r.tokenPhase++
		progress = true
	}
}

// A partially decremented rope is no longer a sufficient recovery authority.
// Refuse ordinary handoff while exact stack/token/callback debt remains here.
func (set *StableResourceSet) hasOwnedRetirementDebtV5() bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	r := set.ownedRetirementV5
	return r != nil && r.started && !r.done
}
