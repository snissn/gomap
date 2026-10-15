package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// These claims consume existing physical edges. Their continuation is embedded
// in the exact original last-owner object, never a replacement release owner.
func (root *resourceReleaseFrameV6) rope(n *stableResourceEntryNode, w *iterator.OrdinalScanWork) (bool, error) {
	if n == nil {
		return true, nil
	}
	if n.cleanupDone.Load() {
		if n.cleanupFailure.causes[0] != nil {
			return false, &n.cleanupFailure
		}
		return true, nil
	}
	if n.metadata == nil {
		if w != nil {
			return false, retainedalloc.ErrWorkRequired
		}
		return true, n.release()
	}
	if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*n))+2*uint64(unsafe.Sizeof(*n.metadata))) {
		return false, nil
	}
	for {
		refs := n.refs.Load()
		if refs <= 0 {
			return false, ErrResourceOwnership
		}
		if !n.refs.CompareAndSwap(refs, refs-1) {
			if !resourceGraphReserveV6(w, 1, 16) {
				return false, nil
			}
			continue
		}
		if refs != 1 {
			return true, nil
		}
		f := &n.metadata.release
		*f = resourceReleaseFrameV6{value: n, owner: n.metadata.owner, charge: n.metadata.charge, parent: root.current}
		root.current = f
		return true, nil
	}
}

func (root *resourceReleaseFrameV6) backing(b *resourcePinnedBacking, w *iterator.OrdinalScanWork) (bool, error) {
	if b == nil {
		return true, nil
	}
	if b.failure != nil {
		return false, b.failure
	}
	if w != nil && b.owner == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*b))) {
		return false, nil
	}
	refs := b.refs.Add(-1)
	if refs < 0 {
		panic("unbalanced resource pinned backing")
	}
	if refs != 0 {
		return true, nil
	}
	f := &b.releaseWork
	*f = resourceReleaseFrameV6{value: b, owner: b.owner, charge: b.charge, parent: root.current}
	root.current = f
	return true, nil
}

func (root *resourceReleaseFrameV6) token(t *StableResourceToken, expected ResourceOwnerState, w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil {
		return true, nil
	}
	if t.cleanupDone.Load() {
		return true, t.releaseResult()
	}
	if w != nil {
		if t.metadata == nil || t.physicalBacking == nil || t.onRelease != nil || t.observationCleanup != nil || t.releaseObservation != nil {
			return false, retainedalloc.ErrWorkRequired
		}
		if ns := t.namespace; ns != nil && t.importedSource == nil {
			ns.mu.Lock()
			supported := ns.metadata != nil && ns.batch == nil && ns.originalCleanup == nil && !ns.released.Load()
			ns.mu.Unlock()
			if !supported {
				return false, retainedalloc.ErrWorkRequired
			}
		}
	}
	if !resourceGraphReserveV6(w, 1, 2*uint64(unsafe.Sizeof(*t))) {
		return false, nil
	}
	if !t.owner.CompareAndSwap(uint32(expected), uint32(ResourceOwnerReleased)) {
		return false, t.releaseResult()
	}
	if t.released.Swap(true) {
		return false, t.releaseResult()
	}
	f := &t.releaseWork.graph
	*f = resourceReleaseFrameV6{value: t, parent: root.current}
	if t.metadata != nil {
		f.owner = t.metadata.owner
	}
	root.current = f
	return true, nil
}

func (root *resourceReleaseFrameV6) stepToken(f *resourceReleaseFrameV6, t *StableResourceToken, w *iterator.OrdinalScanWork) (bool, error) {
	switch f.phase {
	case 0:
		if t.physicalBacking != nil {
			if ready, e := root.backing(t.physicalBacking, w); !ready || e != nil {
				return false, e
			}
		} else {
			if w != nil {
				return false, retainedalloc.ErrWorkRequired
			}
			if e := t.releasePinnedReference(); e != nil {
				return false, e
			}
		}
		t.pinned, t.pinnedRefs, t.physicalBacking = nil, nil, nil
		f.phase = 1
	case 1:
		if t.namespace != nil && t.importedSource == nil {
			ready, e := root.tokenNamespace(f, t, w)
			if !ready || e != nil {
				return false, e
			}
		}
		f.phase = 2
	case 2:
		if w == nil {
			t.identityPin.Release()
		} else {
			ready, e := t.identityPin.ReleaseWithWorkV6(w)
			if !ready || e != nil {
				return false, e
			}
		}
		t.identityPin = nil
		f.phase = 3
	case 3:
		if t.directory != nil {
			ready, e := root.releaseDirectory(t.directory, w)
			if !ready || e != nil {
				return false, e
			}
			t.directory = nil
		}
		f.phase = 4
	case 4:
		if w != nil && (t.onRelease != nil || t.observationCleanup != nil || t.releaseObservation != nil) {
			return false, retainedalloc.ErrWorkRequired
		}
		if t.onRelease != nil {
			t.onRelease()
			t.onRelease = nil
		}
		if t.observationCleanup != nil {
			cleanup := t.observationCleanup
			if e := cleanup.finishOriginalObservation(t.releaseObservation, t.identity, t.namespace); e != nil {
				return false, e
			}
			t.observationCleanup = nil
		} else if t.releaseObservation != nil {
			if e := t.releaseObservation.Unobserve(t.identity); e != nil {
				return false, e
			}
		}
		t.releaseObservation = nil
		t.namespace = nil
		f.phase = 5
	case 5:
		if ready, e := root.tokenMetadata(t, w); !ready || e != nil {
			return false, e
		}
		f.phase = 6
	default:
		t.cleanupDone.Store(true)
		f.value = nil
	}
	return true, nil
}

func (root *resourceReleaseFrameV6) tokenNamespace(f *resourceReleaseFrameV6, t *StableResourceToken, w *iterator.OrdinalScanWork) (bool, error) {
	ns := t.namespace
	if w == nil {
		return true, ns.release()
	}
	completion, ready, err := ns.ReleaseWithWorkV6(&t.releaseWork.namespacePhase, w)
	if !ready || err != nil {
		return false, err
	}
	t.namespace = nil
	completion.Complete()
	return true, nil
}

func releaseOwnedTokenPhysicalV6(t *StableResourceToken) error {
	if t.released.Swap(true) {
		return t.releaseResult()
	}
	root := &t.releaseWork.graph
	*root = resourceReleaseFrameV6{value: t, owner: t.metadata.owner}
	root.current = root
	completion, _, err := root.continueWork(nil)
	if err != nil {
		return t.releaseResult()
	}
	t = nil
	completion.complete()
	return nil
}
