package rootpublication

import (
	"bytes"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Lookups borrow the caller's original admitted codec and immutable entry
// edges. The entry, rather than its representative token, owns membership.
func lookupOwnedEntryObligationV2(entry *stableResourceEntry, o StableLogicalObligation, s *ownedDirectoryCodecV2) (StableLogicalObligation, bool, error) {
	return lookupOwnedEntryObligationWithScopeV2(entry, o, s, nil)
}

func lookupOwnedEntryObligationWithScopeV2(entry *stableResourceEntry, o StableLogicalObligation, s *ownedDirectoryCodecV2, scope *ownedDirectoryCodecScopeV2) (StableLogicalObligation, bool, error) {
	v := entry.logicalObligations
	if actual, found := findStableLogicalObligationIndex(v.index, o, nil); found {
		return actual, true, nil
	}
	if v.directory == nil {
		return StableLogicalObligation{}, false, nil
	}
	if scope != nil {
		if scope.owner != v.directory.metadata {
			return StableLogicalObligation{}, false, ErrResourceOwnership
		}
		var err error
		s, err = scope.get()
		if err != nil {
			return StableLogicalObligation{}, false, err
		}
	}
	if s == nil {
		return StableLogicalObligation{}, false, ErrResourceOwnership
	}
	var actual StableLogicalObligation
	present := false
	_, err := s.lookupLogical(v.directory, o, func(key []byte, value StableLogicalObligation) error {
		if _, removed := v.removedValue(value); !removed && bytes.Equal(key, v.owner) {
			actual, present = value, true
		}
		return nil
	})
	return actual, present, err
}

func lookupOwnedViewObligationV2(view stableResourceKindView, kind ResourceKind, o StableLogicalObligation, s *ownedDirectoryCodecV2, work *StableResourceClosureWork) (StableLogicalObligation, bool, error) {
	if actual, found := findStableLogicalObligationIndex(view.logicalMembership, o, work); found {
		return actual, true, nil
	}
	if view.directory == nil {
		return StableLogicalObligation{}, false, nil
	}
	if s == nil {
		return StableLogicalObligation{}, false, ErrResourceOwnership
	}
	if work != nil {
		work.AggregateMembershipProbes++
	}
	var actual StableLogicalObligation
	present := false
	_, err := s.lookupLogical(view.directory, o, func(key []byte, value StableLogicalObligation) error {
		k, err := decodeBorrowedPhysicalKey(key)
		if err != nil {
			return err
		}
		if k.kind != kind {
			return nil
		}
		entry := findStableResourceLogical(view.logical, k)
		if entry != nil && entry.logicalObligations.directory == view.directory && bytes.Equal(entry.logicalObligations.owner, key) {
			if _, removed := entry.logicalObligations.removedValue(value); !removed {
				actual, present = value, true
			}
		}
		return nil
	})
	return actual, present, err
}

// Extend only the incoming physical/delta entries. Existing directory history,
// physical pins and indexes stay immutable. New physical entries use the same
// original-owner clone/claim/leaf protocol as the maintained owned builder.
func mergeOwnedDirectoryKindView(next *stableResourceKindView, child stableResourceKindView, owner *retainedalloc.Owner) (retErr error) {
	// One admitted backing is reused only after each nested probe has returned.
	scope := ownedDirectoryCodecScopeV2{owner: owner}
	defer scope.close()
	rangeStableResourceLogicalIndex(child.logical, func(entry *stableResourceEntry) bool {
		if entry.outgoing == nil || entry.logicalObligations.directory != nil {
			retErr = ErrResourceOwnership
			return false
		}
		old := findStableResourceLogical(next.logical, entry.token.logicalKey())
		if old != nil {
			coalesce, err := stableResourcesCoalesce(old.token, entry.token)
			if err != nil {
				retErr = err
				return false
			}
			if !coalesce || !old.token.namespaceCompatible(entry.token) || !frontierCompatible(old.frontier, entry.frontier) || (old.token.namespace == nil && entry.token.namespace != nil) {
				retErr = ErrResourceConflict
				return false
			}
			retErr = appendOwnedPhysicalHistoryToViewWithScope(next, old, entry, owner, &scope)
			return retErr == nil
		}
		// A different logical representative of a coalescible physical owner
		// needs the established canonical representative construction, rather
		// than silently adding a second directory owner for its obligations.
		for _, physical := range findStableResourcePhysical(next.physical, entry.token.physicalIdentityKey()) {
			coalesce, err := stableResourcesCoalesce(physical.token, entry.token)
			if err != nil {
				retErr = err
				return false
			}
			if coalesce {
				retErr = ErrResourceConflict
				return false
			}
		}
		retErr = appendOwnedDirectoryPhysicalEntry(next, entry, owner)
		return retErr == nil
	})
	return retErr
}

func appendOwnedDirectoryPhysicalEntry(view *stableResourceKindView, entry *stableResourceEntry, owner *retainedalloc.Owner) (retErr error) {
	backing, err := cloneOwnedResourceEntryUnion(entry, nil, owner)
	if err != nil {
		return err
	}
	token := backing.entries[0].token
	defer func() {
		if ResourceOwnerState(token.owner.Load()) == ResourceOwnerToken {
			if cleanup := token.ReleaseWithError(); cleanup != nil {
				token.cleanupFailure.LinkPriorCleanupError(retErr)
				retErr = &token.cleanupFailure
			}
		}
		if cleanup := backing.release(); cleanup != nil {
			if binding := SelectOriginalCleanupPrior(cleanup, retErr); binding.Valid() && binding.TryLink(retErr) {
				retErr = cleanup
			} else if !errors.Is(retErr, cleanup) {
				retErr = errors.Join(retErr, cleanup)
			}
		}
	}()
	leaf, err := appendOwnedResourceEntryToView(view, backing, owner)
	if err != nil {
		return err
	}
	if err := token.claim(ResourceOwnerShared); err != nil {
		return err
	}
	leaf.entries = backing.entries[:1:1]
	return nil
}
