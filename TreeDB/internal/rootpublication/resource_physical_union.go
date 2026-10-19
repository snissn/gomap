package rootpublication

import (
	"errors"
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
	if len(sets) != 0 && sets[0].MetadataOwner() != nil {
		view, err := buildOwnedPhysicalUnion(sets[0].MetadataOwner(), func(visit func(*StableResourceSet) error) error {
			for _, set := range sets {
				if err := visit(set); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
		count := view.Len()
		return count, view.ReleaseWithError()
	}
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
