package rootpublication

import "unsafe"

// CloneUnchangedRawLeafCapture reuses only the physical capture. The caller
// holds the original manager File lease and has flushed candidate output before
// reading its size. Logical obligations/directory authority are deliberately
// absent: the candidate must rebuild those through its normal closure path.
func CloneUnchangedRawLeafCapture(source *StableResourceSet, identity, parent StableIdentity, lane, id, path string, generation, bytes uint64) (*StableResourceToken, error) {
	if source == nil {
		return nil, nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.physicalOnly || !source.selectedKindViewsAccessibleLocked() || source.Owner() == ResourceOwnerReleased || source.Owner() == ResourceOwnerTransferred {
		return nil, ErrResourceOwnership
	}
	// This fast path shares the existing admitted owner, never creates generic
	// observation custody or converts producer metadata to another owner.
	if source.metadata == nil {
		return nil, nil
	}
	found := rawLeafEntryLocked(source, stableLogicalResourceKey{kind: ResourceOuterLeafLog, lane: lane, resourceID: id, generation: generation})
	if found == nil {
		return nil, nil
	}
	t := activeEntryToken(*found)
	if t.released.Load() || !sameStableObject(t.identity, identity) || t.stability != ResourceMutableAppend || found.diagnosticPath != path || found.frontier.Bytes != bytes || found.frontier.MaxLSN != 0 || found.frontier.exactRIDs != nil || t.digest != ([32]byte{}) {
		return nil, nil
	}
	if t.namespace != nil && (!sameStableObject(t.namespace.parentIdentity, parent) || t.namespace.validateStable() != nil) {
		return nil, nil
	}
	if t.identityPin == nil || t.identityPin.registry.IsRetired(t.identity) {
		return nil, nil
	}
	return t.cloneSharedPinnedDirectory(lane, id, path, found.frontier, ReachabilityOuterLeafRawPointer, nil, nil, nil)
}

// RawLeafDependenciesCoveredBy compares immutable physical prefixes, not logical
// publication authority. Only the coordinator, under its preparation gate, may
// supply its exact successfully selected durable parent. Visible roots alone
// grant no credit. Mixed kinds and mutable pager banks always decline.
func (set *StableResourceSet) RawLeafDependenciesCoveredBy(parent *StableResourceSet) (bool, error) {
	if set == nil || parent == nil {
		return false, nil
	}
	first, second := set, parent
	if uintptr(unsafe.Pointer(first)) > uintptr(unsafe.Pointer(second)) {
		first, second = second, first
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	if second != first {
		second.mu.Lock()
		defer second.mu.Unlock()
	}
	for _, s := range []*StableResourceSet{set, parent} {
		if s.physicalOnly || !s.selectedKindViewsAccessibleLocked() || s.Owner() == ResourceOwnerReleased || s.Owner() == ResourceOwnerTransferred {
			return false, ErrResourceOwnership
		}
	}
	seen, covered := false, true
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		t := activeEntryToken(*entry)
		if t == nil || t.released.Load() || t.kind != ResourceOuterLeafLog || t.stability != ResourceMutableAppend {
			covered = false
			return false
		}
		seen = true
		match := false
		prior := rawLeafEntryLocked(parent, t.logicalKey())
		if prior != nil {
			p := activeEntryToken(*prior)
			if p != nil && !p.released.Load() {
				coalesce, err := stableResourcesCoalesce(p, t)
				match = err == nil && coalesce && (t.namespace == nil || p.namespace != nil) && p.namespaceCompatible(t) && durableFrontierCovers(prior.frontier, entry.frontier)
			}
		}
		covered = match
		return covered
	})
	return seen && covered, nil
}

// Frozen selected closures already carry an indexed logical-resource lookup.
func rawLeafEntryLocked(set *StableResourceSet, key stableLogicalResourceKey) *stableResourceEntry {
	if set.kindViews != nil {
		return findStableResourceLogical(set.kindViews.get(ResourceOuterLeafLog).logical, key)
	}
	var found *stableResourceEntry
	set.rangeEntriesLocked(func(e *stableResourceEntry) bool {
		t := activeEntryToken(*e)
		if t != nil && t.logicalKey() == key {
			found = e
			return false
		}
		return true
	})
	return found
}
