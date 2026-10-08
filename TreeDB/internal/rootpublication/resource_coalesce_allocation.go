package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"slices"
	"unsafe"
)

// This scratch owns only temporary entry edges. Each output token has its own
// physical pin and each output summary has its own retained allocation edges.
// Inputs remain immutable and independently releasable throughout construction.
func rebuildOwnedResourceKindCollision(left, right *resourceKindBacking, owner *retainedalloc.Owner) (*resourceKindBacking, error) {
	count := 0
	for _, source := range [2]*resourceKindBacking{left, right} {
		for _, cell := range source.cells {
			if cell.view.count < 0 || cell.view.count > int(^uint(0)>>1)-count {
				return nil, retainedalloc.ErrCapacity
			}
			count += cell.view.count
		}
	}
	width := uint64(unsafe.Sizeof((*resourceEntryBacking)(nil)))
	if uint64(count) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	scratch, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(count)*width))
	if err != nil {
		return nil, err
	}
	entries := make([]*resourceEntryBacking, 0, count)
	defer func() {
		for _, entry := range entries {
			if entry != nil {
				entry.entries[0].token.Release()
				entry.release()
			}
		}
		clear(entries[:cap(entries)])
		scratch.drop()
		scratch.refund()
	}()
	var buildErr error
	for _, source := range [2]*resourceKindBacking{left, right} {
		rangeStableResourceKindViews(source, func(incoming *stableResourceEntry) bool {
			for i, backing := range entries {
				existing := &backing.entries[0]
				if existing.token.kind == incoming.token.kind && existing.token.logicalKey() == incoming.token.logicalKey() && !existing.token.samePhysicalIdentity(incoming.token) {
					buildErr = ErrResourceConflict
					return false
				}
				coalesce, e := stableResourcesCoalesce(existing.token, incoming.token)
				if e != nil {
					buildErr = e
					return false
				}
				if !coalesce {
					continue
				}
				if !existing.token.namespaceCompatible(incoming.token) || !frontierCompatible(existing.frontier, incoming.frontier) {
					buildErr = ErrResourceConflict
					return false
				}
				replacement, e := cloneOwnedResourceEntryUnion(existing, incoming, owner)
				if e != nil {
					buildErr = e
					return false
				}
				entries[i] = replacement
				existing.token.Release()
				backing.release()
				return true
			}
			backing, e := cloneOwnedResourceEntryUnion(incoming, nil, owner)
			if e != nil {
				buildErr = e
				return false
			}
			entries = append(entries, backing)
			return true
		})
		if buildErr != nil {
			return nil, buildErr
		}
	}
	// Fixed kind count comes from the retained input descriptors. The array is
	// admitted during overlap, and each final kind clones its own string.
	kindCapacity := left.len() + right.len()
	cellsCharge := retainedalloc.AllocationCharge(uint64(kindCapacity) * uint64(unsafe.Sizeof(resourceKindCell{})))
	cellsAllocation, err := newResourceAllocation(owner, cellsCharge)
	if err != nil {
		return nil, err
	}
	cells := make([]resourceKindCell, 0, kindCapacity)
	defer func() {
		for _, cell := range cells {
			releaseStableResourceKindView(cell.view)
		}
		clear(cells[:cap(cells)])
		cellsAllocation.drop()
		cellsAllocation.refund()
	}()
	for _, backing := range entries {
		entry := &backing.entries[0]
		index := -1
		for i := range cells {
			if cells[i].kind == entry.token.kind {
				index = i
				break
			}
		}
		if index < 0 {
			cells = append(cells, resourceKindCell{kind: entry.token.kind})
			index = len(cells) - 1
		}
		view := &cells[index].view
		if err = appendOwnedResourceEntryToView(view, backing, owner); err != nil {
			return nil, err
		}
	}
	result, err := newResourceKindBacking(owner, cells)
	if err != nil {
		return nil, err
	}
	// The descriptor owns the view edges; temporary entry ownership transfers to
	// each real Shared rope pin only after the complete descriptor is admitted.
	for i := range cells {
		cells[i].view = stableResourceKindView{}
	}
	for _, backing := range entries {
		if err = backing.entries[0].token.claim(ResourceOwnerShared); err != nil {
			result.release()
			return nil, err
		}
	}
	for i, backing := range entries {
		backing.release()
		entries[i] = nil
	}
	return result, nil
}

func cloneOwnedResourceEntryUnion(left, right *stableResourceEntry, owner *retainedalloc.Owner) (*resourceEntryBacking, error) {
	if left == nil || left.outgoing == nil || left.token.metadata == nil || left.token.metadata.owner != owner || left.logicalObligations.directory != nil {
		return nil, ErrResourceOwnership
	}
	selected := left.token
	lane, id, path := left.logicalLane, left.resourceID, left.diagnosticPath
	frontier := left.frontier
	var scratch *resourceAllocation
	if right != nil {
		if right.outgoing == nil || right.token.metadata == nil || right.token.metadata.owner != owner || right.logicalObligations.directory != nil {
			return nil, ErrResourceOwnership
		}
		if selected.namespace == nil && right.token.namespace != nil {
			selected = right.token
		}
		var err error
		frontier, scratch, err = ownedFrontierUnion(owner, left.frontier, right.frontier)
		if err != nil {
			return nil, err
		}
		if scratch != nil {
			defer func() {
				clear(frontier.exactRIDs.values[:cap(frontier.exactRIDs.values)])
				frontier.exactRIDs.values = nil
				scratch.drop()
				scratch.refund()
			}()
		}
		if right.logicalLane < lane || right.logicalLane == lane && (right.resourceID < id || right.resourceID == id && right.diagnosticPath < path) {
			lane, id, path = right.logicalLane, right.resourceID, right.diagnosticPath
		}
	}
	// Full physical reconstruction still clones the genuine operation/pin owner;
	// immutable obligation storage comes from the exact persistent index.
	cloned, err := selected.cloneOwnedPinnedDirectory(lane, id, path, frontier, selected.reachability, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	backing, err := newOwnedResourceEntryHistory(left, right, cloned, owner)
	if err != nil {
		cloned.Release()
		return nil, err
	}
	return backing, nil
}

// RID operands are borrowed from actual immutable frontier allocations. The
// union's temporary capacity is reserved before make; the cloned token copies
// it into its own preadmitted frontier backing before this scratch is refunded.
func ownedFrontierUnion(owner *retainedalloc.Owner, a, b DurableFrontier) (DurableFrontier, *resourceAllocation, error) {
	var left, right []uint64
	if a.exactRIDs != nil {
		left = a.exactRIDs.values
	}
	if b.exactRIDs != nil {
		right = b.exactRIDs.values
	}
	out := a
	if b.Bytes > out.Bytes {
		out.Bytes = b.Bytes
	}
	if b.MaxLSN > out.MaxLSN {
		out.MaxLSN = b.MaxLSN
	}
	if len(left) == 0 && len(right) == 0 {
		return out, nil, nil
	}
	if len(right) > int(^uint(0)>>1)-len(left) {
		return DurableFrontier{}, nil, retainedalloc.ErrCapacity
	}
	count := len(left) + len(right)
	if uint64(count) > ^uint64(0)/8 {
		return DurableFrontier{}, nil, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(exactRIDMembership{}))) + retainedalloc.AllocationCharge(uint64(count)*8)
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return DurableFrontier{}, nil, err
	}
	values := make([]uint64, 0, count)
	i, j := 0, 0
	for i < len(left) || j < len(right) {
		var value uint64
		switch {
		case j == len(right) || (i < len(left) && left[i] < right[j]):
			value = left[i]
			i++
		case i == len(left) || right[j] < left[i]:
			value = right[j]
			j++
		default:
			value = left[i]
			i++
			j++
		}
		values = append(values, value)
	}
	summary := exactRIDSummary(values)
	out.MaxRID, out.RIDSetDigest, out.RIDCount, out.RIDMin, out.RIDMax = summary.MaxRID, summary.RIDSetDigest, summary.RIDCount, summary.RIDMin, summary.RIDMax
	out.exactRIDs = &exactRIDMembership{values: values}
	return out, allocation, nil
}

func appendOwnedResourceEntryToView(view *stableResourceKindView, backing *resourceEntryBacking, owner *retainedalloc.Owner) error {
	entry := &backing.entries[0]
	logical, err := insertOwnedResourceLogical(view.logical, entry, owner)
	if err != nil {
		return err
	}
	releaseOwnedResourceLogical(view.logical)
	view.logical = logical
	physical, err := insertOwnedResourcePhysical(view.physical, entry, owner)
	if err != nil {
		return err
	}
	releaseOwnedResourcePhysical(view.physical)
	view.physical = physical
	leaf, err := newOwnedStableResourceEntryLeaf(backing, 0, 1, owner)
	if err != nil {
		return err
	}
	root, err := concatOwnedAdmittedStableResourceEntryNodes(view.root, leaf, owner)
	if err != nil {
		leaf.release()
		return err
	}
	view.root = root
	fields, err := mergeResourceFieldBackings(owner, view.fields, entry.reachability)
	if err != nil {
		return err
	}
	view.fields.release()
	view.fields = fields
	_, added, err := mergeOwnedResourceMembership(&view.logicalMembership, entry.outgoing.index, owner)
	if err != nil {
		return err
	}
	view.logicalMembershipCount += added
	view.count++
	view.logicalObligationCount += entry.logicalObligations.count
	if view.directory == nil {
		view.directory = entry.outgoing.directory
	}
	return nil
}

func subtractOwnedFieldCommitments(target, removed *resourceFieldBacking) error {
	if removed == nil {
		return nil
	}
	for _, old := range removed.cells {
		found := false
		for i := range target.cells {
			cell := &target.cells[i]
			if cell.field != old.field {
				continue
			}
			found = true
			if cell.commitment.count < old.commitment.count {
				return ErrUnresolvedResource
			}
			cell.commitment.count -= old.commitment.count
			subtractStableLogicalObligationDigest(&cell.commitment.sum, old.commitment.sum)
			break
		}
		if !found {
			return ErrUnresolvedResource
		}
	}
	return nil
}

// The canonical index retains each original exact obligation allocation.
// Only incoming operands are inspected and inserted; the old physical rope
// and immutable history remain genuinely shared, rather than cloned.
func newOwnedResourceEntryHistory(left, right *stableResourceEntry, token *StableResourceToken, owner *retainedalloc.Owner) (*resourceEntryBacking, error) {
	backing, err := newResourceEntryBacking(owner, 1)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*resourceEntryBacking, error) { backing.release(); return nil, e }
	entry := &backing.entries[0]
	if err = entry.bindOwnedToken(token); err != nil {
		return fail(err)
	}
	a, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceEntryOutgoing{}))))
	if err != nil {
		return fail(err)
	}
	out := &resourceEntryOutgoing{allocation: a}
	entry.outgoing = out
	if !left.outgoing.index.retainAllocation() {
		return fail(ErrResourceOwnership)
	}
	out.index = left.outgoing.index
	var rightFields *resourceFieldBacking
	var rightIndex *stableLogicalObligationIndexNode
	if right != nil {
		rightFields = right.reachability
		rightIndex = right.outgoing.index
	}
	out.fields, err = mergeResourceFieldBackings(owner, left.reachability, rightFields)
	if err != nil {
		return fail(err)
	}
	if err = subtractOwnedFieldCommitments(out.fields, rightFields); err != nil {
		return fail(err)
	}
	count := left.logicalObligations.count
	var insertErr error
	var insert func(*stableLogicalObligationIndexNode) bool
	insert = func(n *stableLogicalObligationIndexNode) bool {
		if n == nil {
			return true
		}
		if !insert(n.left) {
			return false
		}
		if n.backing == nil || n.backing.allocation == nil || n.backing.allocation.owner != owner {
			insertErr = ErrResourceOwnership
			return false
		}
		ordinal, found := slices.BinarySearchFunc(n.backing.values, n.obligation, compareResourceObligation)
		if !found {
			insertErr = ErrUnresolvedResource
			return false
		}
		next, e := insertOwnedResourceObligationIndex(out.index, n.backing, ordinal, owner)
		if e != nil {
			insertErr = e
			return false
		}
		if next != out.index {
			if count == int(^uint(0)>>1) {
				releaseOwnedResourceObligationIndex(next)
				insertErr = retainedalloc.ErrCapacity
				return false
			}
			releaseOwnedResourceObligationIndex(out.index)
			out.index = next
			count++
			foundField := false
			for i := range out.fields.cells {
				if out.fields.cells[i].field == n.obligation.Reachability && out.fields.cells[i].reachable {
					out.fields.cells[i].commitment.addObligation(n.obligation)
					foundField = true
					break
				}
			}
			if !foundField {
				insertErr = ErrUnresolvedResource
				return false
			}
		}
		return insert(n.right)
	}
	if !insert(rightIndex) {
		return fail(insertErr)
	}
	if token == left.token && right != nil {
		entry.frontier, out.frontier, err = ownedFrontierUnion(owner, left.frontier, right.frontier)
		if err != nil {
			return fail(err)
		}
		out.frontierValue = entry.frontier
	} else {
		entry.frontier = token.frontier
	}
	entry.logicalLane, entry.resourceID, entry.diagnosticPath = token.logicalLane, token.resourceID, token.diagnosticPath
	entry.reachability = out.fields
	entry.logicalObligations = stableLogicalObligationView{fieldSummary: out.fields, index: out.index, count: count}
	entry.dependencyManifestV1 = &out.cache
	return backing, nil
}

// Collision-only batches extend the existing complete index while retaining
// the same physical root. Namespace representative changes, cross-kind aliases
// and newly introduced physical resources retain the admitted general route.
func coalesceOwnedPhysicalHistory(target, incoming *resourceKindBacking, owner *retainedalloc.Owner) (*resourceKindBacking, bool, error) {
	handled := true
	var validationErr error
	rangeStableResourceKindViews(incoming, func(child *stableResourceEntry) bool {
		current, ok := target.lookup(child.token.kind)
		if !ok || current.directory != nil || child.logicalObligations.directory != nil {
			handled = false
			return false
		}
		old := findStableResourceLogical(current.logical, child.token.logicalKey())
		if old == nil || old.outgoing == nil || child.outgoing == nil || !old.token.samePhysicalIdentity(child.token) {
			handled = false
			return false
		}
		physical := findStableResourcePhysical(current.physical, old.token.physicalIdentityKey())
		if len(physical) != 1 || physical[0] != old {
			handled = false
			return false
		}
		for _, c := range target.cells {
			if c.kind != child.token.kind && len(findStableResourcePhysical(c.view.physical, old.token.physicalIdentityKey())) != 0 {
				handled = false
				return false
			}
		}
		if old.token.namespace == nil && child.token.namespace != nil {
			handled = false
			return false
		}
		coalesce, e := stableResourcesCoalesce(old.token, child.token)
		if e != nil {
			validationErr = e
			return false
		}
		if !coalesce {
			handled = false
			return false
		}
		if !old.token.namespaceCompatible(child.token) || !frontierCompatible(old.frontier, child.frontier) {
			validationErr = ErrResourceConflict
			return false
		}
		return true
	})
	if !handled || validationErr != nil {
		return nil, handled, validationErr
	}
	next, e := copyResourceKindBacking(owner, target.cells, nil, nil, true)
	if e != nil {
		return nil, true, e
	}
	var updateErr error
	rangeStableResourceKindViews(incoming, func(child *stableResourceEntry) bool {
		for i := range next.cells {
			if next.cells[i].kind != child.token.kind {
				continue
			}
			view := &next.cells[i].view
			old := findStableResourceLogical(view.logical, child.token.logicalKey())
			updateErr = appendOwnedPhysicalHistoryToView(view, old, child, owner)
			return updateErr == nil
		}
		updateErr = ErrResourceConflict
		return false
	})
	if updateErr != nil {
		next.release()
		return nil, true, updateErr
	}
	return next, true, nil
}

func appendOwnedPhysicalHistoryToView(view *stableResourceKindView, old, child *stableResourceEntry, owner *retainedalloc.Owner) error {
	replacement, e := newOwnedResourceEntryHistory(old, child, old.token, owner)
	if e != nil {
		return e
	}
	defer replacement.release()
	entry := &replacement.entries[0]
	logical, e := insertOwnedResourceLogical(view.logical, entry, owner)
	if e != nil {
		return e
	}
	releaseOwnedResourceLogical(view.logical)
	view.logical = logical
	physical, e := replaceOwnedResourcePhysical(view.physical, old, entry, owner)
	if e != nil {
		return e
	}
	releaseOwnedResourcePhysical(view.physical)
	view.physical = physical
	_, added, e := mergeOwnedResourceMembership(&view.logicalMembership, child.outgoing.index, owner)
	if e != nil {
		return e
	}
	if added > int(^uint(0)>>1)-view.logicalMembershipCount {
		return retainedalloc.ErrCapacity
	}
	view.logicalMembershipCount += added
	fields, e := mergeResourceFieldBackings(owner, view.fields, entry.reachability)
	if e != nil {
		return e
	}
	if e = subtractOwnedFieldCommitments(fields, old.reachability); e != nil {
		fields.release()
		return e
	}
	view.fields.release()
	view.fields = fields
	delta := entry.logicalObligations.count - old.logicalObligations.count
	if delta < 0 || delta > int(^uint(0)>>1)-view.logicalObligationCount {
		return retainedalloc.ErrCapacity
	}
	view.logicalObligationCount += delta
	return nil
}
