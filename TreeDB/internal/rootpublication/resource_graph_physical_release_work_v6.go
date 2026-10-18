package rootpublication

// These claims consume existing physical edges. Their continuation is embedded
// in the exact original last-owner object, never a replacement release owner.
func (root *resourceReleaseFrameV6) rope(n *stableResourceEntryNode) (bool, error) {
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
		return true, n.release()
	}
	for {
		refs := n.refs.Load()
		if refs <= 0 {
			return false, ErrResourceOwnership
		}
		if !n.refs.CompareAndSwap(refs, refs-1) {
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

func (root *resourceReleaseFrameV6) backing(b *resourcePinnedBacking) (bool, error) {
	if b == nil {
		return true, nil
	}
	if b.failure != nil {
		return false, b.failure
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

func (root *resourceReleaseFrameV6) token(t *StableResourceToken, expected ResourceOwnerState) (bool, error) {
	if t == nil {
		return true, nil
	}
	if t.cleanupDone.Load() {
		return true, t.releaseResult()
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

func (root *resourceReleaseFrameV6) stepToken(f *resourceReleaseFrameV6, t *StableResourceToken) (bool, error) {
	switch f.phase {
	case 0:
		if t.physicalBacking != nil {
			if ready, e := root.backing(t.physicalBacking); !ready || e != nil {
				return false, e
			}
		} else {
			if e := t.releasePinnedReference(); e != nil {
				return false, e
			}
		}
		t.pinned, t.pinnedRefs, t.physicalBacking = nil, nil, nil
		f.phase = 1
	case 1:
		if t.namespace != nil && t.importedSource == nil {
			ready, e := root.tokenNamespace(t)
			if !ready || e != nil {
				return false, e
			}
		}
		f.phase = 2
	case 2:
		t.identityPin.Release()
		t.identityPin = nil
		f.phase = 3
	case 3:
		if t.directory != nil {
			ready, e := root.releaseDirectory(t.directory)
			if !ready || e != nil {
				return false, e
			}
			t.directory = nil
		}
		f.phase = 4
	case 4:
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
		if ready, e := root.tokenMetadata(t); !ready || e != nil {
			return false, e
		}
		f.phase = 6
	default:
		t.cleanupDone.Store(true)
		f.value = nil
	}
	return true, nil
}

func (root *resourceReleaseFrameV6) tokenNamespace(t *StableResourceToken) (bool, error) {
	return true, t.namespace.release()
}

func releaseOwnedTokenPhysicalV6(t *StableResourceToken) error {
	if t.released.Swap(true) {
		return t.releaseResult()
	}
	root := &t.releaseWork.graph
	*root = resourceReleaseFrameV6{value: t, owner: t.metadata.owner}
	root.current = root
	completion, _, err := root.releaseGraph()
	if err != nil {
		return t.releaseResult()
	}
	t = nil
	completion.complete()
	return nil
}
