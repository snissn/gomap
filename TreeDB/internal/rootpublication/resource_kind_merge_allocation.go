package rootpublication

import (
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
	if !compatible {
		if next, handled, err := coalesceOwnedPhysicalHistory(target, incoming, owner); handled {
			return next, err
		}
		return rebuildOwnedResourceKindCollision(target, incoming, owner)
	}
	maxInt := int(^uint(0) >> 1)
	if incoming.len() > maxInt-target.len() {
		return nil, retainedalloc.ErrCapacity
	}
	capacity := target.len() + incoming.len()
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
		old := next.cells[index]
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
