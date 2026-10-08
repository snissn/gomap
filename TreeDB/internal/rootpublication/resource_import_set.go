package rootpublication

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"slices"
	"strings"
	"unsafe"
)

// Capture the immutable kind cells and retain their actual custody under the
// existing set lock. All subsequent construction and disposal is outside that
// lock. Generic indexes remain original producer storage; selected indexes keep
// their original admitted backing until this operation finishes.
type resourceImportSource struct {
	allocation  *resourceAllocation
	cells       []resourceKindCell
	backing     *resourceKindBacking
	empty       *DependencyDirectoryV2
	directories *resourceImportDirectory
}

func borrowResourceImportSource(owner *retainedalloc.Owner, source *StableResourceSet) (*resourceImportSource, error) {
	if owner == nil || source == nil {
		return nil, ErrResourceOwnership
	}
	source.mu.Lock()
	if (source.kindViews == nil && len(source.entries) != 0) || source.physicalOnly || source.Owner() == ResourceOwnerReleased || source.Owner() == ResourceOwnerTransferred {
		source.mu.Unlock()
		return nil, ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceImportSource{})))
	if err := diagnosticChargeAdd(&charge, uint64(source.kindViews.len()), uint64(unsafe.Sizeof(resourceKindCell{}))); err != nil {
		source.mu.Unlock()
		return nil, err
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		source.mu.Unlock()
		return nil, err
	}
	snapshot := &resourceImportSource{allocation: allocation, cells: make([]resourceKindCell, 0, source.kindViews.len())}
	if source.kindViews != nil && source.kindViews.allocation != nil {
		if !source.kindViews.retain() {
			source.mu.Unlock()
			snapshot.close()
			return nil, ErrResourceOwnership
		}
		snapshot.backing = source.kindViews
	}
	for kind, view := range source.kindViews.all() {
		if snapshot.backing == nil && !retainStableResourceKindView(view) {
			source.mu.Unlock()
			snapshot.close()
			return nil, ErrResourceOwnership
		}
		snapshot.cells = append(snapshot.cells, resourceKindCell{kind: kind, view: view})
	}
	if empty := source.emptyDependencyDirectoryLocked(); empty != nil {
		if err := empty.Retain(); err != nil {
			source.mu.Unlock()
			snapshot.close()
			return nil, err
		}
		snapshot.empty = empty
	}
	source.mu.Unlock()
	return snapshot, nil
}
func (s *resourceImportSource) close() {
	if s == nil {
		return
	}
	if s.backing != nil {
		s.backing.release()
		s.backing = nil
	} else {
		for _, cell := range s.cells {
			releaseStableResourceKindView(cell.view)
		}
	}
	clear(s.cells)
	s.cells = nil
	if s.empty != nil {
		s.empty.Release()
		s.empty = nil
	}
	for s.directories != nil {
		d := s.directories
		s.directories = d.next
		d.close()
	}
	allocation := s.allocation
	s.allocation = nil
	if allocation != nil && allocation.drop() {
		allocation.refund()
	}
}

// Directory bytes, removed identities and immutable deltas are all original
// source operands. No maps/streams/descriptors are created by traversal. A
// temporary operation may only borrow these strings during its callback.
func walkImportedEntry(directory *resourceImportDirectory, entry *stableResourceEntry, visit func(StableLogicalObligation) error) error {
	if entry == nil || visit == nil || entry.logicalObligations.count < 0 {
		return ErrResourceOwnership
	}
	view := entry.logicalObligations
	seen := 0
	emit := func(o StableLogicalObligation) error {
		if _, removed := view.removed[stableLogicalObligationKey(o)]; removed {
			return nil
		}
		if err := validateStableLogicalObligation(o, o.Reachability); err != nil {
			return err
		}
		seen++
		return visit(o)
	}
	if view.directory != nil {
		if directory == nil || directory.directory != view.directory {
			return ErrResourceOwnership
		}
		start, _ := slices.BinarySearchFunc(directory.records, view.owner, func(record resourceImportRecord, key []byte) int { return bytes.Compare([]byte(record.ownerKey), key) })
		for i := start; i < len(directory.records) && bytes.Equal([]byte(directory.records[i].ownerKey), view.owner); i++ {
			if err := emit(directory.records[i].obligation); err != nil {
				return err
			}
		}
	}
	if view.index != nil && view.index.allocation != nil {
		var scanErr error
		rangeOwnedResourceObligations(view.index, func(o StableLogicalObligation) bool {
			scanErr = emit(o)
			return scanErr == nil
		})
		if scanErr != nil {
			return scanErr
		}
	} else if view.ownedValues != nil {
		for _, o := range view.ownedValues.values {
			if err := emit(o); err != nil {
				return err
			}
		}
	} else {
		// Delta order is not authority: final storage is canonicalized by exact key.
		// Direct parent traversal avoids the generic walker allocating a stack.
		for tail := view.tail; tail != nil; tail = tail.parent {
			for _, o := range tail.values {
				if err := emit(o); err != nil {
					return err
				}
			}
		}
	}
	if seen != view.count {
		return ErrDependencyManifestFormat
	}
	return nil
}
func importEntryObligations(owner *retainedalloc.Owner, directory *resourceImportDirectory, entry *stableResourceEntry) (*resourceObligationBacking, error) {
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceObligationBacking{})))
	if err := diagnosticChargeAdd(&charge, uint64(entry.logicalObligations.count), uint64(unsafe.Sizeof(StableLogicalObligation{}))); err != nil {
		return nil, err
	}
	err := walkImportedEntry(directory, entry, func(o StableLogicalObligation) error {
		for _, text := range [...]string{o.Class, o.Kind, o.Namespace, string(o.Reachability)} {
			n := retainedalloc.AllocationCharge(uint64(len(text)))
			if n > math.MaxUint64-charge {
				return retainedalloc.ErrCapacity
			}
			charge += n
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	out := &resourceObligationBacking{allocation: allocation, values: make([]StableLogicalObligation, 0, entry.logicalObligations.count)}
	err = walkImportedEntry(directory, entry, func(o StableLogicalObligation) error {
		if len(out.values) == cap(out.values) {
			return ErrDependencyManifestFormat
		}
		o.Class = strings.Clone(o.Class)
		o.Kind = strings.Clone(o.Kind)
		o.Namespace = strings.Clone(o.Namespace)
		o.Reachability = ReachabilityField(strings.Clone(string(o.Reachability)))
		out.values = append(out.values, o)
		return nil
	})
	if err != nil {
		out.release()
		return nil, err
	}
	slices.SortFunc(out.values, compareResourceObligation)
	for i := 1; i < len(out.values); i++ {
		if stableLogicalObligationKey(out.values[i-1]) == stableLogicalObligationKey(out.values[i]) {
			out.release()
			return nil, ErrResourceConflict
		}
	}
	return out, nil
}

// ImportStableResourceSetMetadata is the selected boundary before first visible
// Clone. New descriptor/proof/index/rope storage is admitted to owner, while
// exact original kind cohorts retain foreign physical operations and deletion
// authority. It does not change the foreign producer's metadata ownership.
// Cohort retention can exceed the projected files and must be included in the
// selected producer's physical-resource/storage qualification.
func ImportStableResourceSetMetadata(owner *retainedalloc.Owner, source *StableResourceSet) (*StableResourceSet, error) {
	return importStableResourceSetMetadata(owner, source, false)
}

func importStableResourceSetMetadata(owner *retainedalloc.Owner, source *StableResourceSet, flatten bool) (*StableResourceSet, error) {
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	if source == nil {
		b, err := NewStableResourceSetBuilderWithMetadata(owner)
		if err != nil {
			return nil, err
		}
		defer b.Abandon()
		return b.Freeze()
	}
	if !flatten {
		if result, handled, err := cloneAdmittedResourceImport(owner, source); handled {
			return result, err
		}
	}
	original, err := borrowResourceImportSource(owner, source)
	if err != nil {
		return nil, err
	}
	defer original.close()
	builder, err := NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		return nil, err
	}
	defer builder.Abandon()
	for _, cell := range original.cells {
		var importErr error
		rangeStableResourceLogicalIndex(cell.view.logical, func(entry *stableResourceEntry) bool {
			directory, e := original.borrowDirectory(owner, entry.logicalObligations.directory)
			if e != nil {
				importErr = e
				return false
			}
			backing, e := importEntryObligations(owner, directory, entry)
			if e != nil {
				importErr = e
				return false
			}
			defer backing.release()
			// Preserve every actual field, including zero-obligation fields, in the
			// imported entry. The private token carries the original primary field.
			token, e := newImportedResourceToken(owner, activeEntryToken(*entry), cell.view.root, entry.logicalLane, entry.resourceID, entry.diagnosticPath, entry.frontier, entry.token.reachability, backing.values)
			if e != nil {
				importErr = e
				return false
			}
			token.importedFields = entry.reachability
			e = builder.Add(token)
			token.importedFields = nil
			if e != nil {
				token.Release()
				importErr = e
				return false
			}
			return true
		})
		if importErr != nil {
			return nil, importErr
		}
	}
	if original.empty != nil {
		if err := original.empty.walkBorrowed(owner, nil); err != nil {
			return nil, err
		}
		if len(original.cells) != 0 {
			return nil, ErrDependencyManifestFormat
		}
		return newStableResourceSetWithEmptyDirectory(owner, original.empty)
	}
	return builder.Freeze()
}
