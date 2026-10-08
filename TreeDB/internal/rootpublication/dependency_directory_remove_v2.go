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
	directory := source.emptyDependencyDirectoryLocked()
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
		for field := range fields.reachableFields() {
			if _, inScope := scoped[field]; inScope && view.commitments[field].count == 0 {
				fields.removeReachable(field)
			}
		}
		if fields.reachableCount() == 0 {
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

func (set *StableResourceSet) hasDependencyDirectoryV2() bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	found := set.emptyDependencyDirectoryLocked() != nil
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		found = found || entry.logicalObligations.directory != nil
		return !found
	})
	return found
}

func cloneDirectoryForRequirementsV2(source *StableResourceSet, requirements StableLogicalObligationRequirements, index stableLogicalObligationRequirementIndex, work StableResourceClosureWork, excluded ...ResourceKind) (*StableResourceSet, StableResourceClosureWork, error) {
	mutation := StableLogicalObligationMutation{}
	for field := range index.scoped {
		mutation.ScopedFields = append(mutation.ScopedFields, field)
	}
	excludedKinds := make(map[ResourceKind]bool, len(excluded))
	for _, kind := range excluded {
		excludedKinds[kind] = true
	}
	if err := source.WalkLogicalObligations(func(physical StableResourcePhysicalDescriptor, obligation StableLogicalObligation) error {
		work.SourceObligationsInspected++
		if excludedKinds[physical.Kind] || !index.containsScope(obligation) {
			return nil
		}
		if _, desired := index.desired[obligation.Reachability][obligation]; !desired {
			mutation.Removed = append(mutation.Removed, obligation)
		}
		return nil
	}); err != nil {
		return nil, work, err
	}
	result, work, _, err := cloneDirectoryRemovingV2(source, mutation, work, excluded...)
	return result, work, err
}

func validateDirectoryRequirementsV2(resources *StableResourceSet, index stableLogicalObligationRequirementIndex, work StableResourceClosureWork) (StableResourceClosureWork, error) {
	type fieldKey struct {
		resource stableLogicalResourceKey
		field    ReachabilityField
	}
	fields := make(map[fieldKey]bool)
	diagnostics, captureErr := resources.AcquirePhysicalDiagnostics()
	if captureErr != nil {
		return work, captureErr
	}
	defer diagnostics.Close()
	for _, physical := range diagnostics.Physical() {
		work.SourceEntriesInspected++
		key := stableLogicalResourceKey{kind: physical.Kind, lane: physical.logicalLane, resourceID: physical.resourceID, generation: physical.Generation}
		for _, field := range physical.reachability {
			if _, scoped := index.scoped[field]; scoped {
				fields[fieldKey{key, field}] = false
			}
		}
	}
	seen := make(map[StableLogicalObligation]bool)
	if err := resources.WalkLogicalObligations(func(physical StableResourcePhysicalDescriptor, obligation StableLogicalObligation) error {
		work.SourceObligationsInspected++
		key := stableLogicalResourceKey{kind: physical.Kind, lane: physical.logicalLane, resourceID: physical.resourceID, generation: physical.Generation}
		if _, scoped := index.scoped[obligation.Reachability]; scoped {
			fields[fieldKey{key, obligation.Reachability}] = true
		}
		if !index.containsScope(obligation) {
			return nil
		}
		if _, desired := index.desired[obligation.Reachability][obligation]; !desired {
			return fmt.Errorf("%w: stale logical obligation %+v", ErrResourceConflict, obligation)
		}
		if seen[obligation] {
			return fmt.Errorf("%w: duplicate logical obligation %+v", ErrResourceConflict, obligation)
		}
		seen[obligation] = true
		return nil
	}); err != nil {
		return work, err
	}
	for key, found := range fields {
		if !found {
			return work, fmt.Errorf("%w: scoped reachability field %q has no logical obligations", ErrUnresolvedResource, key.field)
		}
	}
	for _, desired := range index.desired {
		for obligation := range desired {
			if !seen[obligation] {
				return work, fmt.Errorf("%w: missing logical obligation %+v", ErrUnresolvedResource, obligation)
			}
		}
	}
	return work, nil
}
