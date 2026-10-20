package rootpublication

import (
	"bytes"
	"errors"
	"iter"
	"slices"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Generic diagnostics keep their legacy map; selected entries borrow the exact
// normalized removal allocation held by their original outgoing edge.
func (v stableLogicalObligationView) removedValue(o StableLogicalObligation) (StableLogicalObligation, bool) {
	if v.ownedRemoved != nil {
		i, ok := slices.BinarySearchFunc(v.ownedRemoved.values, o, compareResourceObligation)
		if ok && v.ownedRemoved.values[i] == o {
			// The proof masks this exact base value, not a live pending
			// replacement with the same stable key and different integrity.
			return v.ownedRemoved.values[i], true
		}
	}
	x, ok := v.removed[stableLogicalObligationKey(o)]
	return x, ok
}
func (v stableLogicalObligationView) removalCount() int {
	n := len(v.removed)
	if v.ownedRemoved != nil {
		n += len(v.ownedRemoved.values)
	}
	return n
}
func (v stableLogicalObligationView) removalValues() iter.Seq[StableLogicalObligation] {
	return func(yield func(StableLogicalObligation) bool) {
		if v.ownedRemoved != nil {
			for _, o := range v.ownedRemoved.values {
				if !yield(o) {
					return
				}
			}
		}
		for _, o := range v.removed {
			if !yield(o) {
				return
			}
		}
	}
}

type ownedDirectoryRemovalPlanV2 struct {
	allocation *resourceAllocation
	values     []ownedDirectoryDeltaIdentityV2
	directory  *DependencyDirectoryV2 // borrowed under the original kind borrow
}

func (p *ownedDirectoryRemovalPlanV2) close() {
	a := p.allocation
	clear(p.values)
	*p = ownedDirectoryRemovalPlanV2{}
	if a != nil {
		a.drop()
		a.refund()
	}
}
func compareRemovalEntryV2(a, b *stableResourceEntry) int {
	x, y := a.token.logicalKey(), b.token.logicalKey()
	if stableLogicalResourceKeyLess(x, y) {
		return -1
	}
	if stableLogicalResourceKeyLess(y, x) {
		return 1
	}
	return 0
}
func (p ownedDirectoryRemovalPlanV2) forEntry(e *stableResourceEntry) []ownedDirectoryDeltaIdentityV2 {
	i, ok := slices.BinarySearchFunc(p.values, e, func(a ownedDirectoryDeltaIdentityV2, b *stableResourceEntry) int {
		return compareRemovalEntryV2(a.entry, b)
	})
	if !ok {
		return nil
	}
	j := i
	for j < len(p.values) && p.values[j].entry == e {
		j++
	}
	return p.values[i:j]
}

// Resolve each declared key once. The original logical entry index identifies
// its physical owner. Only pending producer deltas need a borrowed owner census;
// inherited directory history is never visited or flattened.
func prepareOwnedDirectoryRemovalPlanV2(views *resourceKindBacking, removed []StableLogicalObligation, excluded []ResourceKind, seen []bool, work *StableResourceClosureWork) (plan ownedDirectoryRemovalPlanV2, retErr error) {
	if views == nil || views.allocation == nil {
		return plan, ErrResourceOwnership
	}
	owner := views.allocation.owner
	var pendingCount uint64
	rangeStableResourceKindViews(views, func(e *stableResourceEntry) bool {
		if d := e.logicalObligations.directory; d != nil {
			if plan.directory != nil && plan.directory != d {
				retErr = ErrResourceConflict
				return false
			}
			plan.directory = d
		}
		if resourceKindExcluded(e.token.kind, excluded) {
			return true
		}
		e.logicalObligations.rangeDeltaValues(func(StableLogicalObligation) bool {
			if pendingCount == ^uint64(0) {
				retErr = retainedalloc.ErrCapacity
				return false
			}
			pendingCount++
			return true
		})
		return retErr == nil
	})
	if retErr != nil || plan.directory == nil {
		return plan, retErr
	}
	width := uint64(unsafe.Sizeof(ownedDirectoryDeltaIdentityV2{}))
	max := uint64(int(^uint(0)>>1)) / width
	if pendingCount > max || uint64(len(removed)) > max {
		return plan, retainedalloc.ErrCapacity
	}
	scratch, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(pendingCount*width))
	if err != nil {
		return plan, err
	}
	pending := make([]ownedDirectoryDeltaIdentityV2, 0, int(pendingCount))
	defer func() { clear(pending); pending = nil; scratch.drop(); scratch.refund() }()
	rangeStableResourceKindViews(views, func(e *stableResourceEntry) bool {
		if resourceKindExcluded(e.token.kind, excluded) {
			return true
		}
		e.logicalObligations.rangeDeltaValues(func(o StableLogicalObligation) bool {
			pending = append(pending, ownedDirectoryDeltaIdentityV2{obligation: o, entry: e})
			work.SourceObligationsInspected++
			return true
		})
		return true
	})
	slices.SortFunc(pending, func(a, b ownedDirectoryDeltaIdentityV2) int {
		return compareResourceObligation(a.obligation, b.obligation)
	})
	for i := 1; i < len(pending); i++ {
		if stableLogicalObligationKey(pending[i-1].obligation) == stableLogicalObligationKey(pending[i].obligation) {
			return plan, ErrResourceConflict
		}
	}
	plan.allocation, err = newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(len(removed))*width))
	if err != nil {
		return plan, err
	}
	plan.values = make([]ownedDirectoryDeltaIdentityV2, 0, len(removed))
	defer func() {
		if retErr != nil {
			plan.close()
		}
	}()
	retErr = withOwnedDirectoryCodecV2(owner, func(s *ownedDirectoryCodecV2) error {
		for i, o := range removed {
			if !firstMutationValue(removed, i) {
				seen[i] = true
				continue
			}
			var entry *stableResourceEntry
			if n, ok := slices.BinarySearchFunc(pending, o, func(a ownedDirectoryDeltaIdentityV2, b StableLogicalObligation) int {
				return compareResourceObligation(a.obligation, b)
			}); ok {
				if pending[n].obligation != o {
					return ErrResourceConflict
				}
				entry = pending[n].entry
			} else {
				work.AggregateMembershipProbes++
				found, err := s.lookupLogical(plan.directory, o, func(key []byte, actual StableLogicalObligation) error {
					if actual != o {
						return ErrResourceConflict
					}
					k, err := decodeBorrowedPhysicalKey(key)
					if err != nil {
						return err
					}
					cell, ok := views.lookup(k.kind)
					if !ok || resourceKindExcluded(k.kind, excluded) {
						return ErrUnresolvedResource
					}
					entry = findStableResourceLogical(cell.logical, k)
					if entry == nil || entry.logicalObligations.directory != plan.directory || !bytes.Equal(entry.logicalObligations.owner, key) {
						return ErrUnresolvedResource
					}
					if _, gone := entry.logicalObligations.removedValue(o); gone {
						return ErrUnresolvedResource
					}
					return nil
				})
				if err != nil {
					return err
				}
				if !found {
					// A mixed source can own this declaration in a flat entry.
					// Its original filtering path resolves it and marks seen;
					// global validation still refuses genuinely absent keys.
					continue
				}
			}
			plan.values = append(plan.values, ownedDirectoryDeltaIdentityV2{obligation: o, entry: entry})
			seen[i] = true
		}
		return nil
	})
	if retErr != nil {
		return plan, retErr
	}
	slices.SortFunc(plan.values, func(a, b ownedDirectoryDeltaIdentityV2) int {
		if c := compareRemovalEntryV2(a.entry, b.entry); c != 0 {
			return c
		}
		return compareResourceObligation(a.obligation, b.obligation)
	})
	return plan, nil
}

// Private successor retains the original physical/directory and pending delta
// edges. Only the declared removals and bounded field cells are copied.
func cloneOwnedDirectoryRemovingEntryV2(entry *stableResourceEntry, records []ownedDirectoryDeltaIdentityV2, requirements *StableLogicalObligationRequirements, work *StableResourceClosureWork) (backing *resourceEntryBacking, retErr error) {
	owner := entry.outgoing.allocation.owner
	fields, err := newResourceFieldBacking(owner, entry.reachability.cells)
	if err != nil {
		return nil, err
	}
	defer func() {
		if fields != nil {
			fields.release()
		}
	}()
	count := entry.logicalObligations.count
	for _, r := range records {
		if count == 0 {
			return nil, ErrResourceConflict
		}
		count--
		ok := false
		for i := range fields.cells {
			cell := &fields.cells[i]
			if cell.field == r.obligation.Reachability {
				ok = cell.commitment.removeObligation(r.obligation)
				break
			}
		}
		if !ok {
			return nil, ErrResourceConflict
		}
	}
	kept := 0
	for _, cell := range fields.cells {
		touched := false
		for _, r := range records {
			if r.obligation.Reachability == cell.field {
				touched = true
				break
			}
		}
		// Complete requirements also remove scoped empty reachability markers.
		// Unscoped empty fields remain independently reachable as before.
		if (touched || requirements != nil && requirementFieldScoped(*requirements, cell.field)) && cell.commitment.count == 0 {
			continue
		}
		fields.cells[kept] = cell
		kept++
	}
	clear(fields.cells[kept:])
	fields.cells = fields.cells[:kept]
	if kept == 0 {
		work.DroppedEntries++
		work.DroppedObligations += uint64(entry.logicalObligations.count)
		return nil, nil
	}
	var values []StableLogicalObligation
	a, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(len(records))*uint64(unsafe.Sizeof(StableLogicalObligation{}))))
	if err != nil {
		return nil, err
	}
	values = make([]StableLogicalObligation, len(records))
	defer func() { clear(values); values = nil; a.drop(); a.refund() }()
	for i, r := range records {
		values[i] = r.obligation
	}
	var prior []StableLogicalObligation
	if entry.logicalObligations.ownedRemoved != nil {
		prior = entry.logicalObligations.ownedRemoved.values
	}
	// Removing a pending replacement preserves the original base-removal
	// proof. Two different payloads under one key must not be normalized as
	// conflicting removal declarations.
	proofsKept := 0
	for _, value := range values {
		if _, found := slices.BinarySearchFunc(prior, value, compareResourceObligation); !found {
			values[proofsKept] = value
			proofsKept++
		}
	}
	removed, err := combineResourceObligationBackings(owner, prior, values[:proofsKept], "")
	if err != nil {
		return nil, err
	}
	defer func() {
		if removed != nil {
			removed.release()
		}
	}()
	retErr = withOwnedDirectoryCodecV2(owner, func(s *ownedDirectoryCodecV2) error {
		physical, key, _, err := s.physical(entry)
		if err != nil {
			return err
		}
		backing, err = cloneOwnedDirectoryEntryV2(entry, key, physical, entry.logicalObligations.directory, true)
		return err
	})
	if retErr != nil {
		return nil, retErr
	}
	owned := backing
	token := owned.entries[0].token
	defer func() {
		if retErr != nil {
			backing = nil
			if cleanup := owned.release(); cleanup != nil {
				if binding := SelectOriginalCleanupPrior(cleanup, retErr); binding.Valid() && binding.TryLink(retErr) {
					retErr = cleanup
				} else if !errors.Is(retErr, cleanup) {
					retErr = errors.Join(retErr, cleanup)
				}
			}
			if cleanup := token.ReleaseWithError(); cleanup != nil {
				if binding := SelectOriginalCleanupPrior(cleanup, retErr); binding.Valid() && binding.TryLink(retErr) {
					retErr = cleanup
				} else if !errors.Is(retErr, cleanup) {
					retErr = errors.Join(retErr, cleanup)
				}
			}
		}
	}()
	next := &backing.entries[0]
	out := next.outgoing
	filterPending := false
	for _, r := range records {
		if _, found := findStableLogicalObligationIndex(entry.outgoing.index, r.obligation, nil); found {
			filterPending = true
			break
		}
	}
	if filterPending {
		// Only producer deltas are in this index. Reuse their original
		// admitted arrays; persisted directory history is never rebuilt.
		var copyPending func(*stableLogicalObligationIndexNode) error
		copyPending = func(n *stableLogicalObligationIndexNode) error {
			if n == nil {
				return nil
			}
			if err := copyPending(n.left); err != nil {
				return err
			}
			if _, gone := slices.BinarySearchFunc(removed.values, n.obligation, compareResourceObligation); !gone {
				if n.backing == nil || n.backing.allocation == nil || n.backing.allocation.owner != owner {
					return ErrResourceOwnership
				}
				i, ok := slices.BinarySearchFunc(n.backing.values, n.obligation, compareResourceObligation)
				if !ok {
					return ErrUnresolvedResource
				}
				index, err := insertOwnedResourceObligationIndex(out.index, n.backing, i, owner)
				if err != nil {
					return err
				}
				if index != out.index {
					releaseOwnedResourceObligationIndex(out.index)
					out.index = index
				}
			}
			return copyPending(n.right)
		}
		if err := copyPending(entry.outgoing.index); err != nil {
			return nil, err
		}
		// No ownedValues fallback: if every pending key was removed the
		// empty index must stay empty in entry and kind membership alike.
	} else {
		if !entry.outgoing.index.retainAllocation() {
			return nil, ErrResourceOwnership
		}
		out.index = entry.outgoing.index
		if !entry.outgoing.obligations.retain() {
			return nil, ErrResourceOwnership
		}
		out.obligations = entry.outgoing.obligations
	}
	out.fields.release()
	out.fields = fields
	fields = nil
	out.removed = removed
	removed = nil
	next.reachability = out.fields
	next.logicalObligations = stableLogicalObligationView{fieldSummary: out.fields, index: out.index, ownedValues: out.obligations, count: count, directory: out.directory, owner: out.directoryKey, ownedRemoved: out.removed}
	work.CopiedEntries++
	work.PhysicalHandleShares++
	work.RetainedEntries++
	work.RetainedObligations += uint64(count)
	work.DroppedObligations += uint64(len(records))
	return backing, nil
}
