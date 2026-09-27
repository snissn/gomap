package rootpublication

// physicalDurabilityUnion borrows members' exact tokens for the publisher
// callback. Original candidate sets retain ownership through success, retry,
// or recovery retention. This capability cannot answer logical metadata APIs.
func physicalDurabilityUnion(candidates []*PreparedRootCandidate) (*StableResourceSet, error) {
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
	count, err := unionStableResourceSets(stableResourceViewPhysicalCount, sets...)
	if err != nil {
		return 0, err
	}
	return len(count.entries), nil
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
		if source.emptyDirectory != nil {
			directories[source.emptyDirectory] = struct{}{}
		}
		for _, directory := range source.physicalDirectories {
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
		owned.physicalDirectories = append(owned.physicalDirectories, directory)
	}
	return owned, nil
}
