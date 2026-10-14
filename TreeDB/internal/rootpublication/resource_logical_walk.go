package rootpublication

import (
	"errors"
	"slices"
	"unsafe"
)

// WalkLogicalObligations streams complete logical metadata, scanning each
// distinct pinned directory once. Scratch space is bounded by physical entries
// and pending producer deltas. The callback must not re-enter this set. Selected values are scoped borrows
// and must not escape the synchronous callback.
func (set *StableResourceSet) WalkLogicalObligations(visit func(StableResourcePhysicalDescriptor, StableLogicalObligation) error) error {
	if set == nil {
		return nil
	}
	// Selected diagnostics acquire the original kind borrow under set.mu.
	// Dispatch before taking that lock; callbacks run on the immutable borrow.
	if set.selected {
		if visit == nil || set.physicalOnly {
			return ErrResourceOwnership
		}
		diagnostics, err := set.AcquirePhysicalDiagnostics()
		if err != nil {
			return err
		}
		walkErr := diagnostics.WalkLogicalObligations(visit)
		return errors.Join(walkErr, diagnostics.CloseWithError())
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	return set.walkLogicalObligationsLocked(visit)
}

func (set *StableResourceSet) walkLogicalObligationsLocked(visit func(StableResourcePhysicalDescriptor, StableLogicalObligation) error) error {
	if visit == nil || set.physicalOnly {
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
	if set.emptyDependencyDirectoryLocked() != nil {
		directories[set.emptyDependencyDirectoryLocked()] = nil
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

// WalkLogicalObligations uses this already-admitted diagnostic's original kind
// authority. Physical and logical values are read-only synchronous borrows;
// callers must finish their use before Close and must not close during a walk.
func (d *ResourceDiagnostics) WalkLogicalObligations(visit func(StableResourcePhysicalDescriptor, StableLogicalObligation) error) error {
	if d == nil || d.closed.Load() || d.allocation == nil || visit == nil {
		return ErrResourceOwnership
	}
	for _, physical := range d.physical {
		if !physical.LogicalObligationCountAvailable {
			return ErrResourceOwnership
		}
	}
	owner := d.allocation.owner
	if d.emptyDirectory != nil {
		return d.emptyDirectory.walkBorrowed(owner, nil)
	}
	if d.borrow.backing == nil {
		return ErrResourceOwnership
	}
	type record struct {
		entry         *stableResourceEntry
		key           stableLogicalResourceKey
		physicalIndex int
		seen          uint64
	}
	var charge uint64
	if err := diagnosticChargeAdd(&charge, uint64(len(d.physical)), uint64(unsafe.Sizeof(record{}))); err != nil {
		return err
	}
	if err := owner.AddPending(charge); err != nil {
		return err
	}
	records := make([]record, 0, len(d.physical))
	defer func() { clear(records); records = nil; owner.RemovePending(charge) }()
	for _, view := range d.borrow.backing.all() {
		capacityMismatch := false
		rangeStableResourceLogicalIndex(view.logical, func(entry *stableResourceEntry) bool {
			if len(records) == cap(records) {
				capacityMismatch = true
				return false
			}
			records = append(records, record{entry: entry, key: stableLogicalResourceKey{kind: entry.token.kind, lane: entry.logicalLane, resourceID: entry.resourceID, generation: entry.token.generation}, physicalIndex: len(records)})
			return true
		})
		if capacityMismatch {
			return ErrResourceOwnership
		}
	}
	if len(records) != len(d.physical) {
		return ErrResourceOwnership
	}
	directoryID := func(r record) uintptr { return uintptr(unsafe.Pointer(r.entry.logicalObligations.directory)) }
	// A single fixed array serves both directory deduplication and exact owner
	// lookup. No decoded strings, map keys, inherited corpus or second index.
	slices.SortFunc(records, func(a, b record) int {
		x, y := directoryID(a), directoryID(b)
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
		if stableLogicalResourceKeyLess(a.key, b.key) {
			return -1
		}
		if stableLogicalResourceKeyLess(b.key, a.key) {
			return 1
		}
		return 0
	})
	for start := 0; start < len(records); {
		directory := records[start].entry.logicalObligations.directory
		end := start + 1
		for end < len(records) && records[end].entry.logicalObligations.directory == directory {
			end++
		}
		if directory != nil {
			group := records[start:end]
			if err := directory.walkBorrowed(owner, func(key []byte, obligation StableLogicalObligation) error {
				logicalKey, err := decodeBorrowedPhysicalKey(key)
				if err != nil {
					return err
				}
				index, found := slices.BinarySearchFunc(group, logicalKey, func(r record, k stableLogicalResourceKey) int {
					if stableLogicalResourceKeyLess(r.key, k) {
						return -1
					}
					if stableLogicalResourceKeyLess(k, r.key) {
						return 1
					}
					return 0
				})
				if !found {
					return nil
				}
				item := &group[index]
				if _, removed := item.entry.logicalObligations.removed[stableLogicalObligationKey(obligation)]; removed {
					return nil
				}
				item.seen++
				return visit(d.physical[item.physicalIndex], obligation)
			}); err != nil {
				return err
			}
		}
		start = end
	}
	for i := range records {
		item := &records[i]
		var walkErr error
		item.entry.logicalObligations.rangeDeltaValues(func(obligation StableLogicalObligation) bool {
			if _, removed := item.entry.logicalObligations.removed[stableLogicalObligationKey(obligation)]; removed {
				return true
			}
			item.seen++
			walkErr = visit(d.physical[item.physicalIndex], obligation)
			return walkErr == nil
		})
		if walkErr != nil {
			return walkErr
		}
		if !d.physical[item.physicalIndex].LogicalObligationCountAvailable || item.entry.logicalObligations.count < 0 || item.seen != d.physical[item.physicalIndex].LogicalObligationCount {
			return ErrDependencyManifestFormat
		}
	}
	return nil
}
