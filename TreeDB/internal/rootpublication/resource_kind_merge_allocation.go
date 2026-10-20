package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"slices"
	"strings"
	"unsafe"
)

// The selected merge prepares a genuinely independent immutable descriptor.
// Input ownership changes only after the existing builder's admission and
// child-owner CAS succeed. It never sends admitted storage to a generic map or
// path-copy fallback. Generic callers keep the established exact fallback.
func mergeOwnedResourceKindBackings(target, incoming *resourceKindBacking) (*resourceKindBacking, error) {
	if target.len() == 0 {
		return cloneResourceKindBacking(incoming, nil)
	}
	if incoming.len() == 0 {
		return cloneResourceKindBacking(target, nil)
	}
	if target.allocation == nil || incoming.allocation == nil || target.allocation.owner != incoming.allocation.owner {
		return nil, ErrResourceOwnership
	}
	owner := target.allocation.owner
	for _, source := range [2]*resourceKindBacking{target, incoming} {
		for _, cell := range source.cells {
			view := cell.view
			if !resourceKindViewAdmittedBy(view, owner) {
				return nil, ErrResourceOwnership
			}
		}
	}
	compatible := true
	for kind, child := range incoming.all() {
		if current, ok := target.lookup(kind); ok && current.directory != nil && child.directory != nil && current.directory != child.directory {
			return nil, ErrResourceConflict
		}
	}
	rangeStableResourceKindViews(incoming, func(entry *stableResourceEntry) bool {
		compatible = !stableResourceViewsConflict(target, entry)
		return compatible
	})
	directory := false
	for _, cell := range target.cells {
		directory = directory || cell.view.directory != nil
	}
	var incrementalHistory bool
	if !compatible && !directory {
		var err error
		incrementalHistory, err = canCoalesceOwnedPhysicalHistory(target, incoming)
		if err != nil {
			return nil, err
		}
		if !incrementalHistory {
			return rebuildOwnedResourceKindCollision(target, incoming, owner)
		}
	}
	maxInt := int(^uint(0) >> 1)
	if incoming.len() > maxInt-target.len() {
		return nil, retainedalloc.ErrCapacity
	}
	capacity := target.len() + incoming.len()
	if incrementalHistory {
		// Existing collision-only merges allocated only the retained target's
		// cells. Reserve additional cells solely for genuinely new kinds.
		capacity = target.len()
		for _, cell := range incoming.cells {
			if _, exists := target.lookup(cell.kind); !exists {
				capacity++
			}
		}
	}
	width := uint64(unsafe.Sizeof(resourceKindCell{}))
	if uint64(capacity) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceKindBacking{})))
	array := retainedalloc.AllocationCharge(uint64(capacity) * width)
	if array > ^uint64(0)-charge {
		return nil, retainedalloc.ErrCapacity
	}
	charge += array
	for i, source := range [2]*resourceKindBacking{target, incoming} {
		for _, cell := range source.cells {
			if _, duplicate := target.lookup(cell.kind); i == 1 && duplicate {
				continue
			}
			size := retainedalloc.AllocationCharge(uint64(len(cell.kind)))
			if size > ^uint64(0)-charge {
				return nil, retainedalloc.ErrCapacity
			}
			charge += size
		}
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	next := &resourceKindBacking{allocation: allocation, cells: make([]resourceKindCell, 0, capacity)}
	for _, cell := range target.cells {
		if !retainStableResourceKindView(cell.view) {
			next.release()
			return nil, ErrResourceOwnership
		}
		cell.directoryOwned = false
		if cell.view.directory != nil {
			if err := cell.view.directory.Retain(); err != nil {
				releaseStableResourceKindView(cell.view)
				next.release()
				return nil, err
			}
			cell.directoryOwned = true
		}
		cell.kind = ResourceKind(strings.Clone(string(cell.kind)))
		next.cells = append(next.cells, cell)
	}
	for _, cell := range incoming.cells {
		index := -1
		for i := range next.cells {
			if next.cells[i].kind == cell.kind {
				index = i
				break
			}
		}
		if index < 0 {
			if !retainStableResourceKindView(cell.view) {
				next.release()
				return nil, ErrResourceOwnership
			}
			cell.directoryOwned = false
			if cell.view.directory != nil {
				if err := cell.view.directory.Retain(); err != nil {
					releaseStableResourceKindView(cell.view)
					next.release()
					return nil, err
				}
				cell.directoryOwned = true
			}
			cell.kind = ResourceKind(strings.Clone(string(cell.kind)))
			next.cells = append(next.cells, cell)
			continue
		}
		if incrementalHistory {
			// A mixed kind retains the target's complete membership index. Only
			// incoming entries extend it; untouched historical entries are never
			// reconstructed merely because another incoming resource is new.
			var updateErr error
			rangeStableResourceLogicalIndex(cell.view.logical, func(child *stableResourceEntry) bool {
				view := &next.cells[index].view
				previous := findStableResourceLogical(view.logical, child.token.logicalKey())
				if previous != nil {
					updateErr = appendOwnedPhysicalHistoryToView(view, previous, child, owner)
				} else {
					updateErr = appendOwnedNewPhysicalEntryToView(view, child, owner)
				}
				return updateErr == nil
			})
			if updateErr != nil {
				if cleanup := next.release(); cleanup != nil {
					if binding := SelectOriginalCleanupPrior(cleanup, updateErr); binding.Valid() && binding.TryLink(updateErr) {
						return nil, cleanup
					}
					return nil, errors.Join(updateErr, cleanup)
				}
				return nil, updateErr
			}
			continue
		}
		old := next.cells[index]
		// Bind adds disjoint entries from the same retained directory. Their
		// immutable indexes and summaries already have independent custody;
		// append-only merging is reserved for incoming unbound deltas.
		sameDirectory := compatible && old.view.directory != nil && old.view.directory == cell.view.directory
		if directory && !sameDirectory {
			// The admitted next descriptor owns every partial edge, including
			// the SAME failed physical cleanup receipt on refusal.
			if err := mergeOwnedDirectoryKindView(&next.cells[index].view, cell.view, owner); err != nil {
				if cleanup := next.release(); cleanup != nil {
					if binding := SelectOriginalCleanupPrior(cleanup, err); binding.Valid() && binding.TryLink(err) {
						return nil, cleanup
					}
					return nil, errors.Join(err, cleanup)
				}
				return nil, err
			}
			continue
		}
		view, err := mergeOwnedResourceKindView(old.view, cell.view, owner)
		if err != nil {
			next.release()
			return nil, err
		}
		directoryOwned := false
		if view.directory != nil {
			if err := view.directory.Retain(); err != nil {
				releaseStableResourceKindView(view)
				next.release()
				return nil, err
			}
			directoryOwned = true
		}
		next.cells[index].view, next.cells[index].directoryOwned = view, directoryOwned
		releaseStableResourceKindView(old.view)
		if old.directoryOwned {
			old.view.directory.Release()
		}
	}
	slices.SortFunc(next.cells, compareResourceKindCells)
	return next, nil
}

func mergeOwnedResourceKindView(current, child stableResourceKindView, owner *retainedalloc.Owner) (stableResourceKindView, error) {
	maxInt := int(^uint(0) >> 1)
	if current.count < 0 || child.count < 0 || child.count > maxInt-current.count || current.logicalObligationCount < 0 || child.logicalObligationCount < 0 || child.logicalObligationCount > maxInt-current.logicalObligationCount {
		return stableResourceKindView{}, retainedalloc.ErrCapacity
	}
	if !retainStableResourceKindView(current) {
		return stableResourceKindView{}, ErrResourceOwnership
	}
	next := current
	fail := func(err error) (stableResourceKindView, error) {
		releaseStableResourceKindView(next)
		return stableResourceKindView{}, err
	}
	if !child.root.retain() {
		return fail(ErrResourceOwnership)
	}
	root, err := concatOwnedAdmittedStableResourceEntryNodes(next.root, child.root, owner)
	if err != nil {
		child.root.release()
		return fail(err)
	}
	next.root = root
	var mergeErr error
	rangeStableResourceLogicalIndex(child.logical, func(entry *stableResourceEntry) bool {
		logical, e := insertOwnedResourceLogical(next.logical, entry, owner)
		if e != nil {
			mergeErr = e
			return false
		}
		releaseOwnedResourceLogical(next.logical)
		next.logical = logical
		physical, e := insertOwnedResourcePhysical(next.physical, entry, owner)
		if e != nil {
			mergeErr = e
			return false
		}
		releaseOwnedResourcePhysical(next.physical)
		next.physical = physical
		return true
	})
	if mergeErr != nil {
		return fail(mergeErr)
	}
	visited, added, err := mergeOwnedResourceMembership(&next.logicalMembership, child.logicalMembership, owner)
	if err != nil {
		return fail(err)
	}
	if child.logicalMembershipCount < visited || added > maxInt-next.logicalMembershipCount || child.logicalMembershipCount-visited > maxInt-next.logicalMembershipCount-added {
		return fail(ErrUnresolvedResource)
	}
	next.logicalMembershipCount += added + child.logicalMembershipCount - visited
	fields, err := mergeResourceFieldBackings(owner, current.fields, child.fields)
	if err != nil {
		return fail(err)
	}
	next.fields.release()
	next.fields = fields
	if next.directory == nil {
		next.directory = child.directory
	}
	next.count += child.count
	next.logicalObligationCount += child.logicalObligationCount
	return next, nil
}

// Every index insertion selects its operand from the exact retained constructor
// array. No borrowed obligation value is paired with unrelated backing.
func mergeOwnedResourceMembership(target **stableLogicalObligationIndexNode, source *stableLogicalObligationIndexNode, owner *retainedalloc.Owner) (int, int, error) {
	if source == nil {
		return 0, 0, nil
	}
	visited, added, err := mergeOwnedResourceMembership(target, source.left, owner)
	if err != nil {
		return visited, added, err
	}
	if source.backing == nil || source.backing.allocation == nil || source.backing.allocation.owner != owner {
		return visited, added, ErrResourceOwnership
	}
	ordinal, found := slices.BinarySearchFunc(source.backing.values, source.obligation, compareResourceObligation)
	if !found || source.backing.values[ordinal] != source.obligation {
		return visited, added, ErrResourceOwnership
	}
	next, err := insertOwnedResourceObligationIndex(*target, source.backing, ordinal, owner)
	if err != nil {
		return visited, added, err
	}
	visited++
	if next != *target {
		releaseOwnedResourceObligationIndex(*target)
		*target = next
		added++
	}
	v, a, err := mergeOwnedResourceMembership(target, source.right, owner)
	return visited + v, added + a, err
}

func completeOwnedResourceKindMerge(target, incoming, merged *resourceKindBacking) {
	if merged != nil && merged.allocation != nil {
		releaseStableResourceKindViews(target)
		releaseStableResourceKindViews(incoming)
	}
}
func abandonOwnedResourceKindMerge(merged *resourceKindBacking) {
	if merged != nil && merged.allocation != nil {
		merged.release()
	}
}
