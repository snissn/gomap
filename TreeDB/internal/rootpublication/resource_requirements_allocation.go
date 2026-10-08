package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// Selected requirements borrow their caller-owned immutable input. Linear
// exact predicates avoid constructing opaque map backing in the selected owner.
// The input is synchronous, and no borrowed slice or string escapes the call.
func requirementFieldScoped(r StableLogicalObligationRequirements, f ReachabilityField) bool {
	for _, field := range r.ScopedFields {
		if field == f {
			return true
		}
	}
	for _, scope := range r.ScopedNamespaces {
		if scope.Field == f {
			return true
		}
	}
	return false
}
func requirementContainsScope(r StableLogicalObligationRequirements, o StableLogicalObligation) bool {
	for _, field := range r.ScopedFields {
		if field == o.Reachability {
			return true
		}
	}
	for _, scope := range r.ScopedNamespaces {
		if scope.Field == o.Reachability && scope.Namespace == o.Namespace {
			return true
		}
	}
	return false
}
func requirementDesired(r StableLogicalObligationRequirements, o StableLogicalObligation) int {
	for i, value := range r.Obligations {
		if value == o {
			return i
		}
	}
	return -1
}
func validateBorrowedRequirements(r StableLogicalObligationRequirements) error {
	if len(r.ScopedFields)+len(r.ScopedNamespaces) == 0 && len(r.Obligations) != 0 {
		return ErrUnresolvedResource
	}
	for _, field := range r.ScopedFields {
		if field == "" {
			return ErrUnresolvedResource
		}
	}
	for _, scope := range r.ScopedNamespaces {
		if scope.Field == "" || scope.Namespace == "" {
			return ErrUnresolvedResource
		}
		for _, field := range r.ScopedFields {
			if field == scope.Field {
				return ErrResourceConflict
			}
		}
	}
	for i, o := range r.Obligations {
		if !requirementContainsScope(r, o) {
			return ErrResourceConflict
		}
		if err := validateStableLogicalObligation(o, o.Reachability); err != nil {
			return err
		}
		for j := 0; j < i; j++ {
			if stableLogicalObligationKey(o) == stableLogicalObligationKey(r.Obligations[j]) && o != r.Obligations[j] {
				return ErrResourceConflict
			}
		}
	}
	return nil
}
func resourceKindExcluded(kind ResourceKind, excluded []ResourceKind) bool {
	for _, candidate := range excluded {
		if kind == candidate {
			return true
		}
	}
	return false
}
func (set *StableResourceSet) hasSelectedMetadata() bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	return set.selected
}

// Each filtered entry has independently admitted metadata while retaining its
// actual original physical/operation edges. Its field and obligation temporary
// backing remains charged alongside the newly constructed output until Add has
// consumed the complete immutable entry. No callback runs under source.mu.
func cloneSelectedResourceRequirements(source *StableResourceSet, r StableLogicalObligationRequirements, excluded []ResourceKind, work StableResourceClosureWork) (*StableResourceSet, StableResourceClosureWork, error) {
	return cloneSelectedResourceFiltered(source, r, nil, excluded, work)
}

func cloneSelectedResourceFiltered(source *StableResourceSet, r StableLogicalObligationRequirements, removed []StableLogicalObligation, excluded []ResourceKind, work StableResourceClosureWork) (*StableResourceSet, StableResourceClosureWork, error) {
	if removed == nil {
		if err := validateBorrowedRequirements(r); err != nil {
			return nil, work, err
		}
	}
	source.mu.Lock()
	if directory := source.emptyDependencyDirectoryLocked(); directory != nil && source.metadata != nil && source.selectedKindViewsAccessibleLocked() {
		result, err := newStableResourceSetWithEmptyDirectory(source.metadata.owner, directory)
		source.mu.Unlock()
		if len(removed) != 0 {
			result.Release()
			return nil, work, ErrUnresolvedResource
		}
		return result, work, err
	}
	source.mu.Unlock()
	borrow, err := source.borrowSelectedKindViews()
	if err != nil {
		return nil, work, err
	}
	defer borrow.release()
	owner := borrow.backing.allocation.owner

	removalScratch, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(len(removed))))
	if err != nil {
		return nil, work, err
	}
	found := make([]bool, len(removed))
	defer func() { clear(found); found = nil; removalScratch.drop(); removalScratch.refund() }()
	builder, err := NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		return nil, work, err
	}
	defer builder.Abandon()
	var buildErr error
	for _, cell := range borrow.backing.cells {
		rangeStableResourceLogicalIndex(cell.view.logical, func(entry *stableResourceEntry) bool {
			work.SourceEntriesInspected++
			if resourceKindExcluded(cell.kind, excluded) {
				work.DroppedEntries++
				work.DroppedObligations += uint64(entry.logicalObligations.count)
				return true
			}
			if entry.outgoing == nil || entry.logicalObligations.directory != nil {
				buildErr = ErrResourceOwnership
				return false
			}
			valueCount := entry.logicalObligations.count
			if uint64(valueCount) > ^uint64(0)/uint64(unsafe.Sizeof(StableLogicalObligation{})) {
				buildErr = retainedalloc.ErrCapacity
				return false
			}
			charge := retainedalloc.AllocationCharge(uint64(valueCount) * uint64(unsafe.Sizeof(StableLogicalObligation{})))
			temporary, e := newResourceAllocation(owner, charge)
			if e != nil {
				buildErr = e
				return false
			}
			filtered := make([]StableLogicalObligation, 0, valueCount)
			defer func() { clear(filtered[:cap(filtered)]); filtered = nil; temporary.drop(); temporary.refund() }()
			for o := range entry.logicalObligations.selectedValues() {
				work.SourceObligationsInspected++
				drop := false
				if removed != nil {
					for i, value := range removed {
						if value == o {
							found[i] = true
							drop = true
						}
					}
				} else {
					drop = requirementContainsScope(r, o) && requirementDesired(r, o) < 0
				}
				if drop {
					work.DroppedObligations++
					continue
				}
				filtered = append(filtered, o)
			}
			fields, e := newResourceFieldBacking(owner, entry.reachability.cells)
			if e != nil {
				buildErr = e
				return false
			}
			defer fields.release()
			kept := 0
			for _, field := range fields.cells {
				keep := !requirementFieldScoped(r, field.field)
				if removed != nil {
					keep = true
					for _, o := range removed {
						if o.Reachability == field.field {
							keep = false
							break
						}
					}
				}
				for _, o := range filtered {
					if o.Reachability == field.field {
						keep = true
						break
					}
				}
				if keep {
					fields.cells[kept] = field
					kept++
				}
			}
			clear(fields.cells[kept:])
			fields.cells = fields.cells[:kept]
			if kept == 0 {
				work.DroppedEntries++
				return true
			}
			token := activeEntryToken(*entry)
			cloned, e := token.cloneOwnedPinnedDirectory(entry.logicalLane, entry.resourceID, entry.diagnosticPath, entry.frontier, fields.cells[0].field, filtered, nil, nil)
			if e != nil {
				buildErr = e
				return false
			}
			cloned.importedFields = fields
			e = builder.Add(cloned)
			cloned.importedFields = nil
			if e != nil {
				cloned.Release()
				buildErr = e
				return false
			}
			work.RetainedEntries++
			work.CopiedEntries++
			work.PhysicalHandleShares++
			work.RetainedObligations += uint64(len(filtered))
			work.CopiedObligations += uint64(len(filtered))
			return true
		})
		if buildErr != nil {
			return nil, work, buildErr
		}
	}
	for _, seen := range found {
		if !seen {
			return nil, work, ErrUnresolvedResource
		}
	}
	result, err := builder.Freeze()
	if err != nil {
		return nil, work, err
	}
	work.Add(builder.ClosureWorkSnapshot())
	work.FreezeOperations++
	return result, work, nil
}

// Validation admits exact desired-membership scratch once. Every actual input
// obligation is inspected; duplicate actual references are rejected exactly,
// including references owned by different physical resources. No maps/index
// walk scratch or detached diagnostics are created.
func validateSelectedResourceRequirements(source *StableResourceSet, r StableLogicalObligationRequirements, work StableResourceClosureWork) (StableResourceClosureWork, error) {
	if err := validateBorrowedRequirements(r); err != nil {
		return work, err
	}
	if len(r.ScopedFields)+len(r.ScopedNamespaces) == 0 {
		return work, nil
	}
	source.mu.Lock()
	empty := source.emptyDependencyDirectoryLocked() != nil && source.metadata != nil && source.selectedKindViewsAccessibleLocked()
	source.mu.Unlock()
	if empty {
		if len(r.Obligations) != 0 {
			return work, ErrUnresolvedResource
		}
		return work, nil
	}
	borrow, err := source.borrowSelectedKindViews()
	if err != nil {
		return work, err
	}
	defer borrow.release()
	allocation, err := newResourceAllocation(borrow.backing.allocation.owner, retainedalloc.AllocationCharge(uint64(len(r.Obligations))))
	if err != nil {
		return work, err
	}
	seen := make([]bool, len(r.Obligations))
	defer func() { clear(seen); seen = nil; allocation.drop(); allocation.refund() }()
	var validationErr error
	for _, cell := range borrow.backing.cells {
		rangeStableResourceLogicalIndex(cell.view.logical, func(entry *stableResourceEntry) bool {
			work.SourceEntriesInspected++
			if entry.outgoing == nil || entry.logicalObligations.directory != nil {
				validationErr = ErrResourceOwnership
				return false
			}
			for _, field := range entry.reachability.cells {
				if !field.reachable || !requirementFieldScoped(r, field.field) {
					continue
				}
				found := false
				for o := range entry.logicalObligations.selectedValues() {
					work.SourceObligationsInspected++
					if o.Reachability != field.field {
						continue
					}
					found = true
					if !requirementContainsScope(r, o) {
						continue
					}
					index := requirementDesired(r, o)
					if index < 0 || seen[index] {
						validationErr = ErrResourceConflict
						return false
					}
					seen[index] = true
				}
				if !found {
					validationErr = ErrUnresolvedResource
					return false
				}
			}
			return true
		})
		if validationErr != nil {
			return work, validationErr
		}
	}
	for _, o := range r.Obligations {
		if !seen[requirementDesired(r, o)] {
			return work, ErrUnresolvedResource
		}
	}
	return work, nil
}
