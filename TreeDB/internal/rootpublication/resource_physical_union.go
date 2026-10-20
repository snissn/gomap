package rootpublication

import (
	"cmp"
	"errors"
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// physicalDurabilityUnion retains members' exact physical tokens for the
// publisher callback. Admitted inputs use the original owned builder; the
// caller releases this private view before original members finish. Generic
// diagnostic inputs retain their borrowed view. Neither has logical authority.
func physicalDurabilityUnion(candidates []*PreparedRootCandidate) (*StableResourceSet, error) {
	if len(candidates) == 0 {
		return nil, ErrDurableRootLineage
	}
	var previous *DurableRootTransaction
	var metadata *retainedalloc.Owner
	hasResources, hasGenericResources := false, false
	for _, candidate := range candidates {
		var err error
		previous, err = validatePhysicalDurabilityMember(previous, candidate)
		if err != nil {
			return nil, err
		}
		if set := candidate.resourceSet(); set != nil {
			hasResources = true
			if owner := set.MetadataOwner(); owner != nil {
				if metadata != nil && metadata != owner {
					return nil, ErrResourceOwnership
				}
				metadata = owner
			} else {
				hasGenericResources = true
			}
		}
	}
	// Every member still participates in lineage validation. With no external
	// resources there are no pins or directory aliases to union or release.
	if !hasResources {
		return nil, nil
	}
	if metadata != nil {
		if hasGenericResources {
			return nil, ErrResourceOwnership
		}
		return buildOwnedPhysicalUnion(metadata, func(visit func(*StableResourceSet) error) error {
			for _, candidate := range candidates {
				if err := visit(candidate.resourceSet()); err != nil {
					return err
				}
			}
			return nil
		})
	}
	sets, err := physicalDurabilityMemberSets(candidates)
	if err != nil {
		return nil, err
	}
	return unionStableResourceSets(stableResourceViewPhysicalPinned, sets...)
}

// physicalDurabilityCount is admission bookkeeping only. The frozen members
// retain their own leases; no logical authority, pins or durable frontier union
// escapes this call. Every member must first pass validatePhysicalDurabilityMember.
// Compatibility checks share the publication merge primitive.
func physicalDurabilityCount(sets []*StableResourceSet) (int, error) {
	if len(sets) != 0 {
		if owner := sets[0].MetadataOwner(); owner != nil {
			return countOwnedPhysicalDurability(owner, sets)
		}
	}
	count, err := unionStableResourceSets(stableResourceViewPhysicalCount, sets...)
	if err != nil {
		return 0, err
	}
	return len(count.entries), nil
}

// Count under the original candidate leases, without constructing another owned
// physical graph merely to inspect its length. Both sorts use only admitted,
// shallow metadata borrows; no pins, directory reads or frontier unions escape.
func countOwnedPhysicalDurability(owner *retainedalloc.Owner, sets []*StableResourceSet) (int, error) {
	capacity := 0
	for _, set := range sets {
		if set == nil {
			continue
		}
		set.mu.Lock()
		if set.metadata == nil || set.metadata.owner != owner || !set.selectedKindViewsAccessibleLocked() || set.Owner() == ResourceOwnerTransferred || set.Owner() == ResourceOwnerReleased {
			set.mu.Unlock()
			return 0, ErrResourceOwnership
		}
		n := len(set.entries)
		if set.kindViews != nil {
			n = stableResourceKindViewCount(set.kindViews)
		}
		set.mu.Unlock()
		if n < 0 || n > int(^uint(0)>>1)-capacity {
			return 0, retainedalloc.ErrCapacity
		}
		capacity += n
	}
	width := uint64(unsafe.Sizeof(stableResourceEntry{}))
	if uint64(capacity) > uint64(^uint(0)>>1)/width {
		return 0, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(capacity) * width)
	if err := owner.AddPending(charge); err != nil {
		return 0, err
	}
	entries := make([]stableResourceEntry, 0, capacity)
	defer func() {
		clear(entries)
		entries = nil
		owner.RemovePending(charge)
	}()
	for _, set := range sets {
		if set == nil {
			continue
		}
		set.mu.Lock()
		if set.metadata == nil || set.metadata.owner != owner || !set.selectedKindViewsAccessibleLocked() || set.Owner() == ResourceOwnerTransferred || set.Owner() == ResourceOwnerReleased {
			set.mu.Unlock()
			return 0, ErrResourceOwnership
		}
		valid := set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
			if len(entries) == capacity {
				return false // Frozen candidate inventory cannot grow under its lease.
			}
			token := activeEntryToken(*entry)
			if token == nil || token.released.Load() || token.metadata == nil || token.metadata.owner != owner {
				return false
			}
			entries = append(entries, stableResourceEntry{token: token, logicalLane: entry.logicalLane, resourceID: entry.resourceID, frontier: entry.frontier})
			return true
		})
		set.mu.Unlock()
		if !valid {
			return 0, ErrResourceOwnership
		}
	}
	slices.SortFunc(entries, func(a, b stableResourceEntry) int {
		left := stableLogicalResourceKey{kind: a.token.kind, lane: a.logicalLane, resourceID: a.resourceID, generation: a.token.generation}
		right := stableLogicalResourceKey{kind: b.token.kind, lane: b.logicalLane, resourceID: b.resourceID, generation: b.token.generation}
		if stableLogicalResourceKeyLess(left, right) {
			return -1
		}
		if stableLogicalResourceKeyLess(right, left) {
			return 1
		}
		return 0
	})
	for i := 1; i < len(entries); i++ {
		left, right := &entries[i-1], &entries[i]
		if left.token.kind == right.token.kind && left.logicalLane == right.logicalLane && left.resourceID == right.resourceID && left.token.generation == right.token.generation && !left.token.samePhysicalIdentity(right.token) {
			return 0, ErrResourceConflict
		}
	}
	slices.SortFunc(entries, func(a, b stableResourceEntry) int {
		left, right := a.token.physicalIdentityKey(), b.token.physicalIdentityKey()
		if stablePhysicalIdentityKeyLess(left, right) {
			return -1
		}
		if stablePhysicalIdentityKeyLess(right, left) {
			return 1
		}
		lk, rk := a.token.physicalCoalescingKey(), b.token.physicalCoalescingKey()
		if c := cmp.Compare(lk.kind, rk.kind); c != 0 {
			return c
		}
		return cmp.Compare(lk.generation, rk.generation)
	})
	count := 0
	for start := 0; start < len(entries); {
		identity := entries[start].token
		end := start + 1
		for end < len(entries) && identity.samePhysicalIdentity(entries[end].token) {
			// Even distinct coalescing generations share stability and immutable
			// digest constraints for the same physical identity.
			if _, err := stableResourcesCoalesce(identity, entries[end].token); err != nil {
				return 0, err
			}
			end++
		}
		for group := start; group < end; {
			representative := entries[group].token
			key := representative.physicalCoalescingKey()
			next := group
			for next < end && entries[next].token.physicalCoalescingKey() == key {
				incoming := entries[next].token
				coalesce, err := stableResourcesCoalesce(representative, incoming)
				if err != nil {
					return 0, err
				}
				if !coalesce || !representative.namespaceCompatible(incoming) || !frontierCompatible(entries[group].frontier, entries[next].frontier) {
					return 0, ErrResourceConflict
				}
				if !representative.hasDiagnosticNamespace() && incoming.hasDiagnosticNamespace() {
					representative = incoming
				}
				next++
			}
			count++
			group = next
		}
		start = end
	}
	return count, nil
}

func physicalDurabilityMemberSets(candidates []*PreparedRootCandidate) ([]*StableResourceSet, error) {
	if len(candidates) == 0 {
		return nil, ErrDurableRootLineage
	}
	var previous *DurableRootTransaction
	sets := make([]*StableResourceSet, 0, len(candidates))
	for _, candidate := range candidates {
		var err error
		previous, err = validatePhysicalDurabilityMember(previous, candidate)
		if err != nil {
			return nil, err
		}
		sets = append(sets, candidate.resourceSet())
	}
	return sets, nil
}

// validatePhysicalDurabilityMember extends one exact consecutive lineage. The
// caller retains all candidate leases and must validate every member before
// using their sets for physical admission counting or publication.
func validatePhysicalDurabilityMember(previous *DurableRootTransaction, candidate *PreparedRootCandidate) (*DurableRootTransaction, error) {
	if candidate == nil {
		return nil, ErrDurableRootLineage
	}
	group := candidate.durableRootGroup()
	if len(group.members) == 0 {
		return nil, ErrDurableRootLineage
	}
	for _, transaction := range group.members {
		if transaction == nil || transaction.Lineage() == (DurableRootLineageID{}) {
			return nil, ErrDurableRootLineage
		}
		if previous != nil {
			if err := validateConsecutiveDurableRootTransactions(previous, transaction); err != nil {
				return nil, err
			}
		}
		previous = transaction
	}
	if previous.Sequence() != candidate.frontier.commitSeq {
		return nil, ErrDurableRootLineage
	}
	return previous, nil
}

// ClonePhysicalReachabilityUnion independently pins physical storage and every
// contributing directory root for maintenance. Its inputs may be unrelated
// recoverable slots; it grants no group publication or logical proof authority.
func ClonePhysicalReachabilityUnion(sources ...*StableResourceSet) (*StableResourceSet, error) {
	view, err := unionStableResourceSets(stableResourceViewPhysicalPinned, sources...)
	if err != nil {
		return nil, err
	}
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	for i := range view.entries {
		if err := cloneStableResourceEntryIntoBuilder(builder, &view.entries[i]); err != nil {
			return nil, err
		}
	}
	directories := make(map[*DependencyDirectoryV2]struct{})
	for _, source := range sources {
		if source == nil {
			continue
		}
		source.mu.Lock()
		if source.emptyDependencyDirectoryLocked() != nil {
			directories[source.emptyDependencyDirectoryLocked()] = struct{}{}
		}
		for _, directory := range source.physicalDependencyDirectoriesLocked() {
			directories[directory] = struct{}{}
		}
		source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
			if directory := entry.logicalObligations.directory; directory != nil {
				directories[directory] = struct{}{}
			}
			return true
		})
		source.mu.Unlock()
	}
	owned, err := builder.Freeze()
	if err != nil {
		return nil, err
	}
	owned.physicalOnly = true
	for directory := range directories {
		if err := directory.Retain(); err != nil {
			owned.Release()
			return nil, err
		}
		if owned.extras == nil {
			owned.extras = &stableResourceSetExtras{}
		}
		owned.extras.physical = append(owned.extras.physical, directory)
	}
	return owned, nil
}

// Use the original admitted builder, rather than a second flat descriptor/cache
// union. The clones retain each exact physical producer while the callback is
// active; logical directory obligations never enter this physical capability.
func buildOwnedPhysicalUnion(owner *retainedalloc.Owner, sources func(func(*StableResourceSet) error) error) (_ *StableResourceSet, retErr error) {
	builder, err := NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cleanup := builder.AbandonWithError(); cleanup != nil {
			receipt := &builder.metadata.failure
			if !errors.Is(retErr, receipt) {
				receipt.LinkPriorCleanupError(retErr)
			}
			retErr = receipt
		}
	}()
	err = sources(func(source *StableResourceSet) error {
		if source == nil {
			return nil
		}
		if source.MetadataOwner() != owner {
			return ErrResourceOwnership
		}
		var failure error
		source.rangeEntries(func(entry *stableResourceEntry) bool {
			token := activeEntryToken(*entry)
			clone, err := token.cloneOwnedPinnedDirectory(entry.logicalLane, entry.resourceID, entry.diagnosticPath, entry.frontier, token.reachability, nil, nil, nil)
			if err != nil {
				failure = err
				return false
			}
			clone.importedFields = entry.reachability
			failure = builder.Add(clone)
			clone.importedFields = nil
			if failure != nil {
				if cleanup := clone.ReleaseWithError(); cleanup != nil {
					receipt := &clone.cleanupFailure
					if !errors.Is(failure, receipt) {
						receipt.LinkPriorCleanupError(failure)
					}
					failure = receipt
				}
			}
			return failure == nil
		})
		return failure
	})
	if err != nil {
		return nil, err
	}
	result, err := builder.Freeze()
	if err == nil {
		result.physicalOnly = true
	}
	return result, err
}
