package rootpublication

import "fmt"

// cloneDirectoryRemovingV2 checks declared removals against the pinned root and
// carries only those exact keys until the next directory is built. Physical
// entries and summaries are bounded by physical resources, not logical rows.
func cloneDirectoryRemovingV2(source *StableResourceSet, mutation StableLogicalObligationMutation, work StableResourceClosureWork, excluded ...ResourceKind) (*StableResourceSet, StableResourceClosureWork, bool, error) {
	if source == nil {
		return nil, work, false, nil
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if owner := ResourceOwnerState(source.owner.Load()); owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return nil, work, true, ErrResourceOwnership
	}
	directory := source.emptyDirectory
	source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		if entry.logicalObligations.directory != nil {
			directory = entry.logicalObligations.directory
			return false
		}
		return true
	})
	if directory == nil {
		return nil, work, false, nil
	}
	excludedKinds := make(map[ResourceKind]struct{}, len(excluded))
	for _, kind := range excluded {
		excludedKinds[kind] = struct{}{}
	}
	scoped := make(map[ReachabilityField]struct{}, len(mutation.ScopedFields))
	for _, field := range mutation.ScopedFields {
		scoped[field] = struct{}{}
	}
	entries := make(map[string]*stableResourceEntry)
	var validationErr error
	source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		work.SourceEntriesInspected++
		if _, skip := excludedKinds[entry.token.kind]; skip {
			work.DroppedEntries++
			work.DroppedObligations += uint64(entry.logicalObligations.count)
			return true
		}
		view := entry.logicalObligations
		if view.directory != directory || view.tail != nil || view.index != nil || len(view.removed) != 0 {
			validationErr = fmt.Errorf("%w: removal source is not a fully bound directory", ErrResourceConflict)
			return false
		}
		entries[string(view.owner)] = entry
		return true
	})
	if validationErr != nil {
		return nil, work, true, validationErr
	}
	byOwner := make(map[string][]StableLogicalObligation)
	for _, obligation := range mutation.Removed {
		work.AggregateMembershipProbes++
		owner, actual, found, err := directory.LookupLogical(obligation)
		if err != nil {
			return nil, work, true, err
		}
		entry := entries[string(owner)]
		if !found || actual != obligation || entry == nil {
			return nil, work, true, fmt.Errorf("%w: removal does not match inherited logical directory record", ErrUnresolvedResource)
		}
		byOwner[string(owner)] = append(byOwner[string(owner)], obligation)
	}
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	work.CloneOperations++
	for owner, entry := range entries {
		view := entry.logicalObligations
		removed := byOwner[owner]
		fields := cloneReachabilityUnion(nil, entry.reachability)
		if len(removed) != 0 {
			view.commitments = cloneStableLogicalObligationCommitments(view.commitments)
			view.removed = make(map[stableLogicalObligationIndex]StableLogicalObligation, len(removed))
			for _, obligation := range removed {
				key := stableLogicalObligationKey(obligation)
				if _, duplicate := view.removed[key]; duplicate || view.count == 0 {
					return nil, work, true, ErrResourceConflict
				}
				commitment := view.commitments[obligation.Reachability]
				if !commitment.removeObligation(obligation) {
					return nil, work, true, ErrResourceConflict
				}
				view.commitments[obligation.Reachability] = commitment
				view.removed[key] = obligation
				view.count--
			}
		}
		for field := range fields {
			if _, inScope := scoped[field]; inScope && view.commitments[field].count == 0 {
				delete(fields, field)
			}
		}
		if len(fields) == 0 {
			work.DroppedEntries++
			work.DroppedObligations += uint64(entry.logicalObligations.count)
			continue
		}
		cloned, err := cloneStableResourceEntryDirectoryV2(entry, directory)
		if err != nil {
			return nil, work, true, err
		}
		cloned.logicalObligations, cloned.reachability = view, fields
		builder.entries = append(builder.entries, cloned)
		work.CopiedEntries++
		work.PhysicalHandleShares++
		work.RetainedEntries++
		work.RetainedObligations += uint64(view.count)
		work.DroppedObligations += uint64(len(removed))
	}
	result, err := builder.Freeze()
	work.FreezeOperations++
	return result, work, true, err
}
