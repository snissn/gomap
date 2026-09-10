package rootpublication

// StableResourceSelector identifies one physical resource and one exact
// logical obligation that the selected resource must own.
type StableResourceSelector struct {
	Kind               ResourceKind
	LogicalLane        string
	ResourceID         string
	PhysicalGeneration uint64
	Obligation         StableLogicalObligation
}

// CloneStableResourceForSelector returns an independently pinned, one-entry
// resource set for an exact logical obligation. Indexed frozen sets use their
// per-kind logical-resource index; flat sets retain their bounded linear form.
func CloneStableResourceForSelector(source *StableResourceSet, selector StableResourceSelector) (*StableResourceSet, error) {
	if source == nil || selector.Kind == "" || selector.LogicalLane == "" || selector.ResourceID == "" || selector.PhysicalGeneration == 0 {
		return nil, ErrUnresolvedResource
	}
	key := stableLogicalResourceKey{
		kind: selector.Kind, lane: selector.LogicalLane,
		resourceID: selector.ResourceID, generation: selector.PhysicalGeneration,
	}

	source.mu.Lock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		source.mu.Unlock()
		return nil, ErrResourceOwnership
	}
	var entry *stableResourceEntry
	if source.kindViews != nil {
		view, ok := source.kindViews[selector.Kind]
		if ok {
			entry = findStableResourceLogical(view.logical, key)
		}
	} else {
		for i := range source.entries {
			if token := activeEntryToken(source.entries[i]); token != nil && token.logicalKey() == key {
				entry = &source.entries[i]
				break
			}
		}
	}
	if entry == nil {
		source.mu.Unlock()
		return nil, ErrUnresolvedResource
	}
	obligation, found := findStableLogicalObligationIndex(entry.logicalObligations.index, selector.Obligation, nil)
	if !found {
		source.mu.Unlock()
		return nil, ErrUnresolvedResource
	}
	if obligation != selector.Obligation {
		source.mu.Unlock()
		return nil, ErrResourceConflict
	}
	selected := *entry
	selected.logicalObligations = newStableLogicalObligationView([]StableLogicalObligation{selector.Obligation})
	selected.reachability = map[ReachabilityField]struct{}{selector.Obligation.Reachability: {}}
	selected.dependencyManifestV1 = nil

	builder := NewStableResourceSetBuilder()
	err := cloneStableResourceEntryIntoBuilder(builder, &selected)
	source.mu.Unlock()
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	result, err := builder.Freeze()
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	return result, nil
}
