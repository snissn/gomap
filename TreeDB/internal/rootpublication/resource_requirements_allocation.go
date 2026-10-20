package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"slices"
	"strings"
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
	return cloneSelectedResourceFilteredWithPhysicalSelection(source, r, removed, excluded, work, "", nil)
}

// Physical selection is a synchronous caller-owned proof. The only new scratch
// is exact found membership, admitted alongside the source borrow and output.
func cloneSelectedResourceFilteredWithPhysicalSelection(source *StableResourceSet, r StableLogicalObligationRequirements, removed []StableLogicalObligation, excluded []ResourceKind, work StableResourceClosureWork, selectedKind ResourceKind, selected []StableIdentity) (output *StableResourceSet, outputWork StableResourceClosureWork, retErr error) {
	if removed == nil {
		if err := validateBorrowedRequirements(r); err != nil {
			return nil, work, err
		}
	}
	source.mu.Lock()
	if directory := source.emptyDependencyDirectoryLocked(); directory != nil && source.metadata != nil && source.selectedKindViewsAccessibleLocked() {
		result, err := newStableResourceSetWithEmptyDirectory(source.metadata.owner, directory)
		source.mu.Unlock()
		if len(removed) != 0 || len(selected) != 0 {
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
	defer func() {
		if err := borrow.release(); err != nil {
			if output != nil {
				err = errors.Join(err, output.ReleaseWithError())
				output = nil
			}
			retErr = errors.Join(retErr, err)
		}
	}()
	owner := borrow.backing.allocation.owner

	var physicalFound []bool
	if selectedKind != "" {
		physicalScratch, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(len(selected))))
		if err != nil {
			return nil, work, err
		}
		physicalFound = make([]bool, len(selected))
		defer func() { clear(physicalFound); physicalFound = nil; physicalScratch.drop(); physicalScratch.refund() }()
	}

	removalScratch, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(len(removed))))
	if err != nil {
		return nil, work, err
	}
	found := make([]bool, len(removed))
	defer func() { clear(found); found = nil; removalScratch.drop(); removalScratch.refund() }()
	var removalPlan ownedDirectoryRemovalPlanV2
	var directoryRequirements *StableLogicalObligationRequirements
	if removed != nil {
		removalPlan, err = prepareOwnedDirectoryRemovalPlanV2(borrow.backing, removed, excluded, found, &work)
		if err != nil {
			return nil, work, err
		}
		defer removalPlan.close()
	} else {
		var hasDirectory bool
		rangeStableResourceKindViews(borrow.backing, func(entry *stableResourceEntry) bool {
			hasDirectory = entry.logicalObligations.directory != nil
			return !hasDirectory
		})
		if hasDirectory {
			removalPlan, err = prepareOwnedDirectoryRequirementsV2(source, borrow.backing, r, excluded, selectedKind, selected, &work)
			if err != nil {
				return nil, work, err
			}
			defer removalPlan.close()
			directoryRequirements = &r
		}
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		return nil, work, err
	}
	defer func() {
		if builder != nil {
			if cleanup := builder.AbandonWithError(); cleanup != nil {
				if binding := SelectOriginalCleanupPrior(cleanup, retErr); binding.Valid() && binding.TryLink(retErr) {
					retErr = cleanup
				} else {
					retErr = errors.Join(retErr, cleanup)
				}
			}
		}
	}()
	var buildErr error
	for _, cell := range borrow.backing.cells {
		rangeStableResourceLogicalIndex(cell.view.logical, func(entry *stableResourceEntry) bool {
			work.SourceEntriesInspected++
			if resourceKindExcluded(cell.kind, excluded) {
				work.DroppedEntries++
				work.DroppedObligations += uint64(entry.logicalObligations.count)
				return true
			}

			if cell.kind == selectedKind {
				token := activeEntryToken(*entry)
				if token == nil || token.released.Load() {
					buildErr = ErrResourceOwnership
					return false
				}
				if err := token.namespace.validateStable(); err != nil {
					buildErr = err
					return false
				}
				selectedIndex := -1
				for i, identity := range selected {
					if identity == token.identity {
						selectedIndex = i
						break
					}
				}
				if selectedIndex < 0 {
					work.DroppedEntries++
					work.DroppedObligations += uint64(entry.logicalObligations.count)
					return true
				}
				physicalFound[selectedIndex] = true
			}
			if (removed != nil || directoryRequirements != nil) && entry.outgoing != nil && entry.logicalObligations.directory != nil {
				records := removalPlan.forEntry(entry)
				backing, e := cloneOwnedDirectoryRemovingEntryV2(entry, records, directoryRequirements, &work)
				if e != nil {
					buildErr = e
					return false
				}
				if backing == nil {
					return true
				}
				token := backing.entries[0].token
				if e = builder.addPreparedAdmittedEntry(backing); e != nil {
					buildErr = errors.Join(e, token.ReleaseWithError())
					return false
				}
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
	for _, seen := range physicalFound {
		if !seen {
			return nil, work, ErrUnresolvedResource
		}
	}
	for _, seen := range found {
		if !seen {
			return nil, work, ErrUnresolvedResource
		}
	}
	if builder.kindViews == nil && removalPlan.directory != nil {
		result, err := newStableResourceSetWithEmptyDirectory(owner, removalPlan.directory)
		return result, work, err
	}
	builderWork := builder.ClosureWorkSnapshot()
	result, err := builder.Freeze()
	if err != nil {
		return nil, work, err
	}
	builder = nil // Freeze refunds the original builder header.
	work.Add(builderWork)
	work.FreezeOperations++
	return result, work, nil
}

// Validation admits exact desired-membership scratch once. Every actual input
// obligation is inspected; duplicate actual references are rejected exactly,
// including references owned by different physical resources. The maintained
// admitted diagnostic streams complete directory history with only physical
// and desired-membership scratch, never detached or flattened logical values.
func validateSelectedResourceRequirements(source *StableResourceSet, r StableLogicalObligationRequirements, work StableResourceClosureWork) (_ StableResourceClosureWork, retErr error) {
	if err := validateBorrowedRequirements(r); err != nil {
		return work, err
	}
	if len(r.ScopedFields)+len(r.ScopedNamespaces) == 0 {
		return work, nil
	}
	diagnostics, err := source.AcquirePhysicalDiagnostics()
	if err != nil {
		return work, err
	}
	defer func() { retErr = errors.Join(retErr, diagnostics.CloseWithError()) }()
	if diagnostics.allocation == nil {
		return work, ErrResourceOwnership
	}
	type fieldCheck struct {
		key   stableLogicalResourceKey
		field ReachabilityField
		seen  bool
	}
	physicalKey := func(p StableResourcePhysicalDescriptor) stableLogicalResourceKey {
		return stableLogicalResourceKey{kind: p.Kind, lane: p.logicalLane, resourceID: p.resourceID, generation: p.Generation}
	}
	compare := func(a, b fieldCheck) int {
		if stableLogicalResourceKeyLess(a.key, b.key) {
			return -1
		}
		if stableLogicalResourceKeyLess(b.key, a.key) {
			return 1
		}
		return strings.Compare(string(a.field), string(b.field))
	}
	count := 0
	for _, physical := range diagnostics.Physical() {
		work.SourceEntriesInspected++
		for _, field := range physical.reachability {
			if requirementFieldScoped(r, field) {
				if count == int(^uint(0)>>1)/int(unsafe.Sizeof(fieldCheck{})) {
					return work, retainedalloc.ErrCapacity
				}
				count++
			}
		}
	}
	var charge uint64
	if err := diagnosticChargeAdd(&charge, uint64(count), uint64(unsafe.Sizeof(fieldCheck{}))); err != nil {
		return work, err
	}
	if err := diagnosticChargeAdd(&charge, uint64(len(r.Obligations)), 1); err != nil {
		return work, err
	}
	allocation, err := newResourceAllocation(diagnostics.allocation.owner, charge)
	if err != nil {
		return work, err
	}
	fields := make([]fieldCheck, 0, count)
	seen := make([]bool, len(r.Obligations))
	defer func() {
		clear(fields)
		fields = nil
		clear(seen)
		seen = nil
		allocation.drop()
		allocation.refund()
	}()
	for _, physical := range diagnostics.Physical() {
		for _, field := range physical.reachability {
			if requirementFieldScoped(r, field) {
				fields = append(fields, fieldCheck{key: physicalKey(physical), field: field})
			}
		}
	}
	slices.SortFunc(fields, compare)
	if err := diagnostics.WalkLogicalObligations(func(physical StableResourcePhysicalDescriptor, o StableLogicalObligation) error {
		work.SourceObligationsInspected++
		if !requirementFieldScoped(r, o.Reachability) {
			return nil
		}
		i, found := slices.BinarySearchFunc(fields, fieldCheck{key: physicalKey(physical), field: o.Reachability}, compare)
		if !found {
			return ErrResourceOwnership
		}
		fields[i].seen = true
		if !requirementContainsScope(r, o) {
			return nil
		}
		index := requirementDesired(r, o)
		if index < 0 || seen[index] {
			return ErrResourceConflict
		}
		seen[index] = true
		return nil
	}); err != nil {
		return work, err
	}
	for _, field := range fields {
		if !field.seen {
			return work, ErrUnresolvedResource
		}
	}
	for _, o := range r.Obligations {
		if !seen[requirementDesired(r, o)] {
			return work, ErrUnresolvedResource
		}
	}
	return work, nil
}
