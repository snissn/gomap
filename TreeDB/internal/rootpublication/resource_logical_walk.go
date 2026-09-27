package rootpublication

// WalkLogicalObligations streams complete logical metadata, scanning each
// distinct pinned directory once. Scratch space is bounded by physical entries
// and pending producer deltas. The callback must not re-enter this set.
func (set *StableResourceSet) WalkLogicalObligations(visit func(StableResourcePhysicalDescriptor, StableLogicalObligation) error) error {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	return set.walkLogicalObligationsLocked(visit)
}

func (set *StableResourceSet) walkLogicalObligationsLocked(visit func(StableResourcePhysicalDescriptor, StableLogicalObligation) error) error {
	if visit == nil {
		return ErrResourceOwnership
	}
	if owner := ResourceOwnerState(set.owner.Load()); owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return ErrResourceOwnership
	}
	type streamEntry struct {
		entry    *stableResourceEntry
		physical StableResourcePhysicalDescriptor
		seen     uint64
	}
	entries := make([]*streamEntry, 0)
	directories := make(map[*DependencyDirectoryV2]map[string]*streamEntry)
	set.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		item := &streamEntry{entry: entry, physical: physicalDescriptorFromEntry(entry)}
		entries = append(entries, item)
		view := entry.logicalObligations
		if view.directory != nil {
			if directories[view.directory] == nil {
				directories[view.directory] = make(map[string]*streamEntry)
			}
			directories[view.directory][string(view.owner)] = item
		}
		return true
	})
	if set.emptyDirectory != nil {
		directories[set.emptyDirectory] = nil
	}
	for directory, owners := range directories {
		if err := directory.Walk(func(key, value []byte) error {
			if key[0] != dependencyLogicalKeyV2 {
				return nil
			}
			owner, obligation, err := DecodeDependencyLogicalV2(key, value)
			if err != nil {
				return err
			}
			item := owners[string(owner)]
			if item == nil {
				return nil
			}
			if _, removed := item.entry.logicalObligations.removed[stableLogicalObligationKey(obligation)]; removed {
				return nil
			}
			item.seen++
			return visit(item.physical, obligation)
		}); err != nil {
			return err
		}
	}
	for _, item := range entries {
		var walkErr error
		item.entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
			if _, removed := item.entry.logicalObligations.removed[stableLogicalObligationKey(obligation)]; removed {
				return true
			}
			item.seen++
			walkErr = visit(item.physical, obligation)
			return walkErr == nil
		})
		if walkErr != nil {
			return walkErr
		}
		if item.seen != item.physical.LogicalObligationCount {
			return ErrDependencyManifestFormat
		}
	}
	return nil
}
