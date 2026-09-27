package rootpublication

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/tree"
)

// WalkDependencyDirectoryChangesV2 emits only changed physical descriptors and
// admitted logical additions. Removing a physical owner streams its records
// from the old root. The DB applies these updates with its existing COW zipper
// before freezing the allocator; visit must copy data it retains.
//
// base must be the visible predecessor, including queued publications. A nil
// base is for a new directory, never an implicit migration of a populated V1 DB.
func WalkDependencyDirectoryChangesV2(source *StableResourceSet, base *DependencyDirectoryV2, visit func(key, value []byte, deleted bool) error) (physicalCount, logicalCount uint64, err error) {
	if source == nil || visit == nil {
		return 0, 0, ErrResourceOwnership
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if owner := ResourceOwnerState(source.owner.Load()); owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred {
		return 0, 0, ErrResourceOwnership
	}
	owners := make(map[string]struct{})
	// The map contains only this publication's additions, not retained keys.
	additions := make(map[string]struct{})
	var added, removed uint64
	source.rangeEntriesLocked(func(entry *stableResourceEntry) bool {
		physicalEntry := *entry
		physicalEntry.logicalObligations = stableLogicalObligationView{}
		descriptor := dependencyManifestEntryV1FromStableResourceEntry(physicalEntry)
		owner := DependencyPhysicalKeyV2(descriptor)
		if _, duplicate := owners[string(owner)]; duplicate {
			err = fmt.Errorf("%w: duplicate physical directory owner", ErrResourceConflict)
			return false
		}
		owners[string(owner)] = struct{}{}
		physicalCount++
		if entry.logicalObligations.count < 0 || uint64(entry.logicalObligations.count) > ^uint64(0)-logicalCount {
			err = ErrDependencyManifestFormat
			return false
		}
		logicalCount += uint64(entry.logicalObligations.count)
		var encoded []byte
		encoded, err = EncodeDependencyPhysicalV2(descriptor)
		if err != nil {
			return false
		}
		changed := true
		if base != nil {
			previous, lookupErr := base.LookupPhysical(owner)
			if lookupErr == nil {
				var previousBytes []byte
				previousBytes, err = EncodeDependencyPhysicalV2(previous)
				changed = !bytes.Equal(previousBytes, encoded)
			} else if !errors.Is(lookupErr, tree.ErrKeyNotFound) {
				err = lookupErr
			}
			if err != nil {
				return false
			}
		}
		if changed {
			err = visit(owner, encoded, false)
			if err != nil {
				return false
			}
		}
		if inherited := entry.logicalObligations.directory; inherited != nil && inherited != base {
			err = fmt.Errorf("%w: logical directory is not the visible predecessor", ErrResourceConflict)
			return false
		}
		for _, obligation := range entry.logicalObligations.removed {
			if base == nil {
				err = ErrResourceConflict
				return false
			}
			priorOwner, previous, found, lookupErr := base.LookupLogical(obligation)
			if lookupErr != nil {
				err = lookupErr
				return false
			}
			if !found || previous != obligation || !bytes.Equal(priorOwner, owner) {
				err = fmt.Errorf("%w: directory removal changes exact identity or owner", ErrResourceConflict)
				return false
			}
			if err = visit(DependencyLogicalKeyV2(obligation), nil, true); err != nil {
				return false
			}
			removed++
		}
		// rangeValues visits only the in-memory delta for a directory view.
		entry.logicalObligations.rangeValues(func(obligation StableLogicalObligation) bool {
			key := DependencyLogicalKeyV2(obligation)
			if _, duplicate := additions[string(key)]; duplicate {
				err = fmt.Errorf("%w: duplicate logical directory addition", ErrResourceConflict)
				return false
			}
			additions[string(key)] = struct{}{}
			if base != nil {
				priorOwner, previous, found, lookupErr := base.LookupLogical(obligation)
				if lookupErr != nil {
					err = lookupErr
					return false
				}
				if found {
					if previous != obligation || !bytes.Equal(priorOwner, owner) {
						err = fmt.Errorf("%w: logical directory addition changes exact identity or owner", ErrResourceConflict)
						return false
					}
					return true
				}
			}
			if err = dependencyLogicalOwnerV2(descriptor, obligation); err != nil {
				return false
			}
			var value []byte
			value, err = EncodeDependencyLogicalV2(owner, obligation)
			if err == nil {
				err = visit(key, value, false)
			}
			if err == nil {
				added++
			}
			return err == nil
		})
		return err == nil
	})
	if err != nil {
		return 0, 0, err
	}
	if base == nil {
		if logicalCount != added {
			return 0, 0, ErrDependencyManifestFormat
		}
		return physicalCount, logicalCount, nil
	}
	// No scan is needed when every predecessor physical owner remains. This
	// is the ordinary source-import append path.
	retainedPhysical := uint64(0)
	for owner := range owners {
		if _, lookupErr := base.LookupPhysical([]byte(owner)); lookupErr == nil {
			retainedPhysical++
		} else if !errors.Is(lookupErr, tree.ErrKeyNotFound) {
			return 0, 0, lookupErr
		}
	}
	if retainedPhysical != base.ref.PhysicalCount {
		err = base.Walk(func(key, value []byte) error {
			owner := key
			if key[0] == dependencyLogicalKeyV2 {
				var decodeErr error
				owner, _, decodeErr = DecodeDependencyLogicalV2(key, value)
				if decodeErr != nil {
					return decodeErr
				}
			}
			if _, retained := owners[string(owner)]; retained {
				return nil
			}
			if key[0] == dependencyLogicalKeyV2 {
				removed++
			}
			return visit(key, nil, true)
		})
		if err != nil {
			return 0, 0, err
		}
	}
	if removed > base.ref.LogicalCount || added > ^uint64(0)-(base.ref.LogicalCount-removed) || logicalCount != base.ref.LogicalCount-removed+added {
		return 0, 0, fmt.Errorf("%w: directory logical delta count mismatch", ErrDependencyManifestFormat)
	}
	return physicalCount, logicalCount, nil
}
