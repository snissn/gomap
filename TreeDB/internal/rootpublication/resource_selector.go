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
	if err := source.RequireMetadataExport(); err != nil {
		return nil, err
	}
	if source != nil && source.physicalOnly {
		return nil, ErrResourceOwnership
	}
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
		view, ok := source.kindViews.lookup(selector.Kind)
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
	obligation, found, lookupErr := entry.logicalObligations.lookup(selector.Obligation, nil)
	if lookupErr != nil {
		source.mu.Unlock()
		return nil, lookupErr
	}
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
	selected.reachability = newStableReachabilitySet(1, selector.Obligation.Reachability)
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

// CloneStableResourceSetSelectingPhysicalKind independently retains selected
// exact identities of one kind and every non-excluded entry of other kinds.
// Selection is a maintenance proof supplied by the caller; this operation
// never opens diagnostic paths or creates producer authority from metadata.
// Missing identities, physical-only views, and released sources fail closed.
func CloneStableResourceSetSelectingPhysicalKind(source *StableResourceSet, kind ResourceKind, selected []StableIdentity, excluded ...ResourceKind) (*StableResourceSet, error) {
	if err := source.RequireMetadataExport(); err != nil {
		return nil, err
	}
	if kind == "" || source == nil || source.physicalOnly {
		return nil, ErrResourceOwnership
	}
	wanted := make(map[StableIdentity]bool, len(selected))
	for _, identity := range selected {
		if !identity.valid() || identity.Generation == 0 {
			return nil, ErrUnresolvedResource
		}
		if _, duplicate := wanted[identity]; duplicate {
			return nil, ErrResourceConflict
		}
		wanted[identity] = false
	}
	omitted := make(map[ResourceKind]bool, len(excluded))
	for _, excludedKind := range excluded {
		if excludedKind == kind {
			return nil, ErrResourceConflict
		}
		omitted[excludedKind] = true
	}
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	source.mu.Lock()
	owner := ResourceOwnerState(source.owner.Load())
	if owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		source.mu.Unlock()
		return nil, ErrResourceOwnership
	}
	var cloneErr error
	source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		token := activeEntryToken(*entry)
		if token == nil || token.released.Load() {
			cloneErr = ErrResourceOwnership
			return false
		}
		if token.kind == kind {
			if err := token.namespace.validateStable(); err != nil {
				cloneErr = err
				return false
			}
			if _, keep := wanted[token.identity]; !keep {
				return true
			}
			wanted[token.identity] = true
		} else if omitted[token.kind] {
			return true
		}
		cloneErr = cloneStableResourceEntryIntoBuilder(builder, entry)
		return cloneErr == nil
	})
	source.mu.Unlock()
	if cloneErr != nil {
		return nil, cloneErr
	}
	for _, found := range wanted {
		if !found {
			return nil, ErrUnresolvedResource
		}
	}
	return builder.Freeze()
}
