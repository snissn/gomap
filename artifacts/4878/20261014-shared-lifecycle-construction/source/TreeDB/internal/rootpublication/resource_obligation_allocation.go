package rootpublication

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"iter"
	"slices"
	"strings"
	"unsafe"
)

// The normalized sequence owns its exact array and cloned string allocations.
// Treap nodes independently retain this backing, so neither a token's physical
// handle nor an older sequence node can silently authorize its refund.
type resourceObligationBacking struct {
	allocation *resourceAllocation
	values     []StableLogicalObligation
}

func newResourceObligationBacking(owner *retainedalloc.Owner, source []StableLogicalObligation, field ReachabilityField) (*resourceObligationBacking, error) {
	return combineResourceObligationBackings(owner, source, nil, field)
}

// The union is constructed in its admitted final backing, never a temporary
// ungoverned list. Empty field validates each exact operand's own field.
func combineResourceObligationBackings(owner *retainedalloc.Owner, left, right []StableLogicalObligation, field ReachabilityField) (*resourceObligationBacking, error) {
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	maxInt := int(^uint(0) >> 1)
	if len(right) > maxInt-len(left) {
		return nil, retainedalloc.ErrCapacity
	}
	for _, source := range [2][]StableLogicalObligation{left, right} {
		for _, value := range source {
			actualField := field
			if actualField == "" {
				actualField = value.Reachability
			}
			if err := validateStableLogicalObligation(value, actualField); err != nil {
				return nil, err
			}
		}
	}
	count := len(left) + len(right)
	width := uint64(unsafe.Sizeof(StableLogicalObligation{}))
	if uint64(count) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceObligationBacking{})))
	array := retainedalloc.AllocationCharge(uint64(count) * width)
	if array > ^uint64(0)-charge {
		return nil, retainedalloc.ErrCapacity
	}
	charge += array
	for _, source := range [2][]StableLogicalObligation{left, right} {
		for _, value := range source {
			for _, text := range [...]string{value.Class, value.Kind, value.Namespace, string(value.Reachability)} {
				size := retainedalloc.AllocationCharge(uint64(len(text)))
				if size > ^uint64(0)-charge {
					return nil, retainedalloc.ErrCapacity
				}
				charge += size
			}
		}
	}
	allocation, err := newResourceAllocation(owner, charge)
	if err != nil {
		return nil, err
	}
	backing := &resourceObligationBacking{allocation: allocation, values: make([]StableLogicalObligation, count)}
	offset := 0
	for _, source := range [2][]StableLogicalObligation{left, right} {
		for _, value := range source {
			value.Class = strings.Clone(value.Class)
			value.Kind = strings.Clone(value.Kind)
			value.Namespace = strings.Clone(value.Namespace)
			value.Reachability = ReachabilityField(strings.Clone(string(value.Reachability)))
			backing.values[offset] = value
			offset++
		}
	}
	slices.SortFunc(backing.values, compareResourceObligation)
	kept := 0
	for _, value := range backing.values {
		if kept != 0 && stableLogicalObligationKey(backing.values[kept-1]) == stableLogicalObligationKey(value) {
			if backing.values[kept-1] != value {
				backing.release()
				return nil, fmt.Errorf("%w: duplicate logical obligation integrity mismatch", ErrResourceConflict)
			}
			continue
		}
		backing.values[kept] = value
		kept++
	}
	// Capacity and string storage remain conservatively admitted for the exact
	// original allocation until its last alias ends; no count-based refund.
	clear(backing.values[kept:])
	backing.values = backing.values[:kept:len(backing.values)]
	return backing, nil
}
func compareResourceObligation(a, b StableLogicalObligation) int {
	if stableLogicalObligationLess(a, b) {
		return -1
	}
	if stableLogicalObligationLess(b, a) {
		return 1
	}
	return 0
}
func (backing *resourceObligationBacking) retain() bool {
	return backing == nil || backing.allocation.retain()
}
func (backing *resourceObligationBacking) release() {
	if backing == nil || !backing.allocation.drop() {
		return
	}
	clear(backing.values[:cap(backing.values)])
	backing.values = nil
	backing.allocation.refund()
}

func (node *stableLogicalObligationIndexNode) retainAllocation() bool {
	return node == nil || node.allocation.retain()
}
func releaseOwnedResourceObligationIndex(node *stableLogicalObligationIndexNode) {
	if node == nil || !node.allocation.drop() {
		return
	}
	left, right, backing := node.left, node.right, node.backing
	node.left, node.right, node.backing = nil, nil, nil
	node.key = stableLogicalObligationIndex{}
	node.obligation = StableLogicalObligation{}
	releaseOwnedResourceObligationIndex(left)
	releaseOwnedResourceObligationIndex(right)
	backing.release()
	node.allocation.refund()
}
func cloneOwnedResourceObligationIndex(root *stableLogicalObligationIndexNode, owner *retainedalloc.Owner) (*stableLogicalObligationIndexNode, error) {
	if owner == nil || (root != nil && (root.allocation == nil || root.allocation.owner != owner || root.backing == nil || root.backing.allocation == nil || root.backing.allocation.owner != owner)) {
		return nil, ErrResourceOwnership
	}
	allocation, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableLogicalObligationIndexNode{}))))
	if err != nil {
		return nil, err
	}
	next := &stableLogicalObligationIndexNode{allocation: allocation}
	if root == nil {
		return next, nil
	}
	if !root.backing.retain() {
		releaseOwnedResourceObligationIndex(next)
		return nil, ErrResourceOwnership
	}
	next.backing = root.backing
	next.key = root.key
	next.obligation = root.obligation
	next.priority = root.priority
	if !root.left.retainAllocation() {
		releaseOwnedResourceObligationIndex(next)
		return nil, ErrResourceOwnership
	}
	next.left = root.left
	if !root.right.retainAllocation() {
		releaseOwnedResourceObligationIndex(next)
		return nil, ErrResourceOwnership
	}
	next.right = root.right
	return next, nil
}

// An unchanged duplicate returns the original root without a new ownership
// edge. A distinct insertion returns one new root; callers retire the replaced
// root only after the new path has been completely admitted.
func insertOwnedResourceObligationIndex(root *stableLogicalObligationIndexNode, backing *resourceObligationBacking, ordinal int, owner *retainedalloc.Owner) (*stableLogicalObligationIndexNode, error) {
	if owner == nil || (root != nil && (root.allocation == nil || root.allocation.owner != owner)) || backing == nil || backing.allocation == nil || backing.allocation.owner != owner || ordinal < 0 || ordinal >= len(backing.values) {
		return nil, ErrResourceOwnership
	}
	// Select the value from the exact retained constructor allocation. A caller
	// cannot pair borrowed strings or a forged value with an unrelated backing.
	value := backing.values[ordinal]
	key := stableLogicalObligationKey(value)
	if root != nil && root.key == key {
		if root.obligation != value {
			return nil, ErrResourceConflict
		}
		return root, nil
	}
	next, err := cloneOwnedResourceObligationIndex(root, owner)
	if err != nil {
		return nil, err
	}
	if root == nil {
		if !backing.retain() {
			releaseOwnedResourceObligationIndex(next)
			return nil, ErrResourceOwnership
		}
		next.key, next.obligation, next.priority, next.backing = key, value, stableLogicalObligationPriority(key), backing
		return next, nil
	}
	if stableLogicalObligationIndexLess(key, root.key) {
		child, e := insertOwnedResourceObligationIndex(root.left, backing, ordinal, owner)
		if e != nil {
			releaseOwnedResourceObligationIndex(next)
			return nil, e
		}
		if child == root.left {
			releaseOwnedResourceObligationIndex(next)
			return root, nil
		}
		releaseOwnedResourceObligationIndex(next.left)
		next.left = child
		if child.priority < next.priority {
			next.left = child.right
			child.right = next
			return child, nil
		}
	} else {
		child, e := insertOwnedResourceObligationIndex(root.right, backing, ordinal, owner)
		if e != nil {
			releaseOwnedResourceObligationIndex(next)
			return nil, e
		}
		if child == root.right {
			releaseOwnedResourceObligationIndex(next)
			return root, nil
		}
		releaseOwnedResourceObligationIndex(next.right)
		next.right = child
		if child.priority < next.priority {
			next.right = child.left
			child.left = next
			return child, nil
		}
	}
	return next, nil
}

// Canonical selected history is the constructor-owned persistent index. Its
// nodes retain exact source arrays; traversal never materializes a second list.
func rangeOwnedResourceObligations(root *stableLogicalObligationIndexNode, visit func(StableLogicalObligation) bool) bool {
	if root == nil {
		return true
	}
	if !rangeOwnedResourceObligations(root.left, visit) {
		return false
	}
	if !visit(root.obligation) {
		return false
	}
	return rangeOwnedResourceObligations(root.right, visit)
}
func (view stableLogicalObligationView) selectedValues() iter.Seq[StableLogicalObligation] {
	return func(yield func(StableLogicalObligation) bool) {
		if view.index != nil && view.index.allocation != nil {
			rangeOwnedResourceObligations(view.index, yield)
		} else if view.ownedValues != nil {
			for _, v := range view.ownedValues.values {
				if !yield(v) {
					return
				}
			}
		}
	}
}

// Full materialization is reserved for exact filtering/import/fallback work.
// The retained index authorizes these borrowed strings only until scratch ends.
func acquireOwnedEntryValues(entry *stableResourceEntry, owner *retainedalloc.Owner) ([]StableLogicalObligation, *resourceAllocation, error) {
	if entry == nil || entry.outgoing == nil || entry.outgoing.allocation.owner != owner || entry.logicalObligations.directory != nil || entry.logicalObligations.count < 0 {
		return nil, nil, ErrResourceOwnership
	}
	n := entry.logicalObligations.count
	if uint64(n) > ^uint64(0)/uint64(unsafe.Sizeof(StableLogicalObligation{})) {
		return nil, nil, retainedalloc.ErrCapacity
	}
	a, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(n)*uint64(unsafe.Sizeof(StableLogicalObligation{}))))
	if err != nil {
		return nil, nil, err
	}
	values := make([]StableLogicalObligation, 0, n)
	for v := range entry.logicalObligations.selectedValues() {
		if len(values) == cap(values) {
			clear(values)
			a.drop()
			a.refund()
			return nil, nil, ErrResourceOwnership
		}
		values = append(values, v)
	}
	if len(values) != n {
		clear(values)
		a.drop()
		a.refund()
		return nil, nil, ErrResourceOwnership
	}
	return values, a, nil
}
