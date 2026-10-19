package rootpublication

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"slices"
	"unsafe"
)

// This scratch owns only temporary entry edges. Each output token has its own
// physical pin and each output summary has its own retained allocation edges.
// Inputs remain immutable and independently releasable throughout construction.
func rebuildOwnedResourceKindCollision(left, right *resourceKindBacking, owner *retainedalloc.Owner) (_ *resourceKindBacking, retErr error) {
	count := 0
	for _, source := range [2]*resourceKindBacking{left, right} {
		for _, cell := range source.cells {
			if cell.view.count < 0 || cell.view.count > int(^uint(0)>>1)-count {
				return nil, retainedalloc.ErrCapacity
			}
			count += cell.view.count
		}
	}
	// The existing admitted scratch retains the exact private leaf alongside
	// its entry until the cloned Token's physical edge can transfer to it.
	type privateEntry struct {
		backing *resourceEntryBacking
		leaf    *stableResourceEntryNode
	}
	width := uint64(unsafe.Sizeof(privateEntry{}))
	if uint64(count) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	scratch, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(count)*width))
	if err != nil {
		return nil, err
	}
	entries := make([]privateEntry, 0, count)
	// Each refused release already retains its SAME original allocation. Link
	// that receipt before the scratch drops its alias; never replace custody
	// with a wrapper or insert an already reachable receipt into its own chain.
	keepFailure := func(receipt *originalReleaseFailure) {
		if !errors.Is(retErr, receipt) {
			receipt.LinkPriorCleanupError(retErr)
			retErr = receipt
		}
	}
	releaseEntry := func(backing *resourceEntryBacking) {
		token := backing.entries[0].token
		if ResourceOwnerState(token.owner.Load()) == ResourceOwnerToken {
			if err := token.ReleaseWithError(); err != nil {
				keepFailure(&token.cleanupFailure)
			}
		}
		if err := backing.release(); err != nil {
			keepFailure(&backing.allocation.failure)
		}
	}
	defer func() {
		for _, entry := range entries {
			if entry.backing != nil {
				releaseEntry(entry.backing)
			}
		}
		clear(entries[:cap(entries)])
		scratch.drop()
		scratch.refund()
	}()
	var buildErr error
	for _, source := range [2]*resourceKindBacking{left, right} {
		rangeStableResourceKindViews(source, func(incoming *stableResourceEntry) bool {
			for i, private := range entries {
				backing := private.backing
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
				entries[i] = privateEntry{backing: replacement}
				releaseEntry(backing)
				if retErr != nil {
					buildErr = retErr
					return false
				}
				return true
			}
			backing, e := cloneOwnedResourceEntryUnion(incoming, nil, owner)
			if e != nil {
				buildErr = e
				return false
			}
			entries = append(entries, privateEntry{backing: backing})
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
		// These are construction borrows of leaves owned by the views. End
		// them before a failed build can dispose and refund those views.
		for i := range entries {
			entries[i].leaf = nil
		}
		for _, cell := range cells {
			releaseStableResourceKindView(cell.view)
		}
		clear(cells[:cap(cells)])
		cellsAllocation.drop()
		cellsAllocation.refund()
	}()
	for i := range entries {
		backing := entries[i].backing
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
		entries[i].leaf, err = appendOwnedResourceEntryToView(view, backing, owner)
		if err != nil {
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
	for _, private := range entries {
		backing := private.backing
		if err = backing.entries[0].token.claim(ResourceOwnerShared); err != nil {
			for i := range entries {
				entries[i].leaf = nil
			}
			if cleanupErr := result.release(); cleanupErr != nil {
				// Failed graph disposal retains this original descriptor and
				// marks its admitted receipt before returning the child error.
				result.allocation.failure.LinkPriorCleanupError(err)
				return nil, &result.allocation.failure
			}
			return nil, err
		}
		private.leaf.entries = backing.entries[:1:1]
	}
	for i := range entries {
		backing := entries[i].backing
		entries[i] = privateEntry{}
		backing.release()
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
		if cleanupErr := cloned.ReleaseWithError(); cleanupErr != nil {
			cloned.cleanupFailure.LinkPriorCleanupError(err)
			return nil, &cloned.cleanupFailure
		}
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

// Append retains metadata and a detached private leaf. Its caller must attach
// the physical entry slice only after the Token's Shared claim succeeds.
func appendOwnedResourceEntryToView(view *stableResourceKindView, backing *resourceEntryBacking, owner *retainedalloc.Owner) (*stableResourceEntryNode, error) {
	entry := &backing.entries[0]
	logical, err := insertOwnedResourceLogical(view.logical, entry, owner)
	if err != nil {
		return nil, err
	}
	releaseOwnedResourceLogical(view.logical)
	view.logical = logical
	physical, err := insertOwnedResourcePhysical(view.physical, entry, owner)
	if err != nil {
		return nil, err
	}
	releaseOwnedResourcePhysical(view.physical)
	view.physical = physical
	leaf, err := newOwnedStableResourceEntryLeaf(backing, 0, 1, owner)
	if err != nil {
		return nil, err
	}
	leaf.entries = nil
	root, err := concatOwnedAdmittedStableResourceEntryNodes(view.root, leaf, owner)
	if err != nil {
		leaf.release()
		return nil, err
	}
	view.root = root
	fields, err := mergeResourceFieldBackings(owner, view.fields, entry.reachability)
	if err != nil {
		return nil, err
	}
	view.fields.release()
	view.fields = fields
	_, added, err := mergeOwnedResourceMembership(&view.logicalMembership, entry.outgoing.index, owner)
	if err != nil {
		return nil, err
	}
	view.logicalMembershipCount += added
	view.count++
	view.logicalObligationCount += entry.logicalObligations.count
	if view.directory == nil {
		view.directory = entry.outgoing.directory
	}
	return leaf, nil
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
	fail := func(e error) (*resourceEntryBacking, error) {
		if cleanup := backing.release(); cleanup != nil {
			if binding := SelectOriginalCleanupPrior(cleanup, e); binding.Valid() && binding.TryLink(e) {
				return nil, cleanup
			}
			return nil, errors.Join(e, cleanup)
		}
		return nil, e
	}
	entry := &backing.entries[0]
	if err = entry.bindOwnedToken(token); err != nil {
		return fail(err)
	}
	a, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceEntryOutgoing{})))+retainedalloc.AllocationCharge(uint64(len(left.logicalObligations.owner))))
	if err != nil {
		return fail(err)
	}
	out := &resourceEntryOutgoing{allocation: a}
	entry.outgoing = out
	if left.logicalObligations.directory != nil {
		if left.logicalObligations.directory.metadata != owner {
			return fail(ErrResourceOwnership)
		}
		if err := left.logicalObligations.directory.Retain(); err != nil {
			return fail(err)
		}
		out.directory = left.logicalObligations.directory
		out.directoryKey = bytes.Clone(left.logicalObligations.owner)
	}
	if left.outgoing.removed != nil {
		// Only prior delta removals are inspected; directory history is untouched.
		unmask := false
		if right != nil {
			for _, o := range left.outgoing.removed.values {
				if actual, found := findStableLogicalObligationIndex(right.outgoing.index, o, nil); found && actual == o {
					unmask = true
					break
				}
			}
		}
		if unmask {
			out.removed, err = newResourceObligationBacking(owner, left.outgoing.removed.values, "")
			if err != nil {
				return fail(err)
			}
			kept := 0
			for _, o := range out.removed.values {
				if actual, found := findStableLogicalObligationIndex(right.outgoing.index, o, nil); !found || actual != o {
					out.removed.values[kept] = o
					kept++
				}
			}
			clear(out.removed.values[kept:])
			out.removed.values = out.removed.values[:kept]
		} else {
			if !left.outgoing.removed.retain() {
				return fail(ErrResourceOwnership)
			}
			out.removed = left.outgoing.removed
		}
	}
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
	var codec *ownedDirectoryCodecV2
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
		actual, present, e := lookupOwnedEntryObligationV2(left, n.obligation, codec)
		if e != nil {
			insertErr = e
			return false
		}
		if present {
			if actual != n.obligation {
				insertErr = ErrResourceConflict
				return false
			}
			return insert(n.right)
		}
		// An identical re-add may unmask a persisted value without adding a
		// second pending emission. A removed unsealed value has no base record
		// and still needs its original live pending index edge.
		restoredBase := false
		if left.logicalObligations.directory != nil {
			if _, removed := left.logicalObligations.removedValue(n.obligation); removed {
				_, e := codec.lookupLogical(left.logicalObligations.directory, n.obligation, func(key []byte, actual StableLogicalObligation) error {
					restoredBase = bytes.Equal(key, left.logicalObligations.owner) && actual == n.obligation
					return nil
				})
				if e != nil {
					insertErr = e
					return false
				}
			}
		}
		next := out.index
		if !restoredBase {
			var e error
			next, e = insertOwnedResourceObligationIndex(out.index, n.backing, ordinal, owner)
			if e != nil {
				insertErr = e
				return false
			}
		}
		if next != out.index || restoredBase {
			if count == int(^uint(0)>>1) {
				if next != out.index {
					releaseOwnedResourceObligationIndex(next)
				}
				insertErr = retainedalloc.ErrCapacity
				return false
			}
			if next != out.index {
				releaseOwnedResourceObligationIndex(out.index)
				out.index = next
			}
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
	apply := func(s *ownedDirectoryCodecV2) error {
		codec = s
		if !insert(rightIndex) {
			return insertErr
		}
		return nil
	}
	if out.directory != nil {
		err = withOwnedDirectoryCodecV2(owner, apply)
	} else {
		err = apply(nil)
	}
	if err != nil {
		return fail(err)
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
	if out.directory != nil {
		entry.logicalLane, entry.resourceID, entry.diagnosticPath = left.logicalLane, left.resourceID, left.diagnosticPath
	}
	entry.reachability = out.fields
	entry.logicalObligations = stableLogicalObligationView{fieldSummary: out.fields, index: out.index, count: count, directory: out.directory, owner: out.directoryKey, ownedRemoved: out.removed}
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

func appendOwnedPhysicalHistoryToView(view *stableResourceKindView, old, child *stableResourceEntry, owner *retainedalloc.Owner) (retErr error) {
	replacement, e := newOwnedResourceEntryHistory(old, child, old.token, owner)
	if e != nil {
		return e
	}
	defer func() {
		if cleanup := replacement.release(); cleanup != nil {
			if binding := SelectOriginalCleanupPrior(cleanup, retErr); binding.Valid() && binding.TryLink(retErr) {
				retErr = cleanup
			} else {
				retErr = errors.Join(retErr, cleanup)
			}
		}
	}()
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
	// Persisted duplicates are absent from the pending index. Do not count or
	// emit them again merely because a producer repeats an existing member.
	merge := func(s *ownedDirectoryCodecV2) error {
		var merge func(*stableLogicalObligationIndexNode) error
		merge = func(n *stableLogicalObligationIndexNode) error {
			if n == nil {
				return nil
			}
			if err := merge(n.left); err != nil {
				return err
			}
			_, present, err := lookupOwnedEntryObligationV2(old, n.obligation, s)
			if err != nil {
				return err
			}
			if !present {
				i, found := slices.BinarySearchFunc(n.backing.values, n.obligation, compareResourceObligation)
				if !found {
					return ErrResourceOwnership
				}
				next, err := insertOwnedResourceObligationIndex(view.logicalMembership, n.backing, i, owner)
				if err != nil {
					return err
				}
				if next != view.logicalMembership {
					releaseOwnedResourceObligationIndex(view.logicalMembership)
					view.logicalMembership = next
				}
			}
			return merge(n.right)
		}
		return merge(child.outgoing.index)
	}
	if old.logicalObligations.directory != nil {
		e = withOwnedDirectoryCodecV2(owner, merge)
	} else {
		e = merge(nil)
	}
	if e != nil {
		return e
	}
	delta := entry.logicalObligations.count - old.logicalObligations.count
	if delta < 0 || delta > int(^uint(0)>>1)-view.logicalMembershipCount {
		return retainedalloc.ErrCapacity
	}
	view.logicalMembershipCount += delta
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
	if delta < 0 || delta > int(^uint(0)>>1)-view.logicalObligationCount {
		return retainedalloc.ErrCapacity
	}
	view.logicalObligationCount += delta
	return nil
}
