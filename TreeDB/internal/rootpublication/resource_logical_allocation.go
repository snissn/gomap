package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// cloneOwnedResourceLogical constructs one actual path-copy node. Child
// allocations have independent refs; no physical rope/token edge substitutes
// for their ownership. The caller retains an original root during this call.
func cloneOwnedResourceLogical(root *stableResourceLogicalIndexNode, owner *retainedalloc.Owner) (*stableResourceLogicalIndexNode, error) {
	if owner == nil || (root != nil && (root.allocation == nil || root.allocation.owner != owner)) {
		return nil, ErrResourceOwnership
	}
	allocation, err := newResourceAllocation(owner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableResourceLogicalIndexNode{}))))
	if err != nil {
		return nil, err
	}
	next := &stableResourceLogicalIndexNode{allocation: allocation}
	if root == nil {
		return next, nil
	}
	if !root.entry.retainBacking() {
		releaseOwnedResourceLogical(next)
		return nil, ErrResourceOwnership
	}
	next.key, next.entry, next.priority = root.key, root.entry, root.priority
	if !root.left.retainAllocation() {
		releaseOwnedResourceLogical(next)
		return nil, ErrResourceOwnership
	}
	next.left = root.left
	if !root.right.retainAllocation() {
		releaseOwnedResourceLogical(next)
		return nil, ErrResourceOwnership
	}
	next.right = root.right
	return next, nil
}

func (node *stableResourceLogicalIndexNode) retainAllocation() bool {
	return node == nil || node.allocation.retain()
}

// releaseOwnedResourceLogical releases metadata only. Physical resources are
// separately owned by the existing rope. Generic diagnostic nodes are untouched.
func releaseOwnedResourceLogical(node *stableResourceLogicalIndexNode) {
	if node == nil || !node.allocation.drop() {
		return
	}
	left, right, entry := node.left, node.right, node.entry
	node.left, node.right, node.entry = nil, nil, nil
	node.key = stableLogicalResourceKey{}
	releaseOwnedResourceLogical(left)
	releaseOwnedResourceLogical(right)
	entry.releaseBacking()
	node.allocation.refund()
}

// insertOwnedResourceLogical returns a new owned root without changing any
// original node. Refusal releases every private path copy; original aliases
// remain valid. Rotation moves existing private child edges rather than making
// another redundant copy of the just-constructed child.
func insertOwnedResourceLogical(root *stableResourceLogicalIndexNode, entry *stableResourceEntry, owner *retainedalloc.Owner) (*stableResourceLogicalIndexNode, error) {
	if owner == nil || entry == nil || entry.token == nil || entry.backing == nil || entry.backing.allocation.owner != owner {
		return nil, ErrResourceOwnership
	}
	next, err := cloneOwnedResourceLogical(root, owner)
	if err != nil {
		return nil, err
	}
	key := entry.token.logicalKey()
	if root == nil {
		if !entry.retainBacking() {
			releaseOwnedResourceLogical(next)
			return nil, ErrResourceOwnership
		}
		next.key, next.entry, next.priority = key, entry, stableResourceLogicalPriority(key)
		return next, nil
	}
	if key == root.key {
		if !entry.retainBacking() {
			releaseOwnedResourceLogical(next)
			return nil, ErrResourceOwnership
		}
		next.entry.releaseBacking()
		next.entry = entry
		return next, nil
	}
	if stableLogicalResourceKeyLess(key, root.key) {
		child, e := insertOwnedResourceLogical(root.left, entry, owner)
		if e != nil {
			releaseOwnedResourceLogical(next)
			return nil, e
		}
		releaseOwnedResourceLogical(next.left)
		next.left = child
		if child.priority < next.priority {
			next.left = child.right
			child.right = next
			return child, nil
		}
	} else {
		child, e := insertOwnedResourceLogical(root.right, entry, owner)
		if e != nil {
			releaseOwnedResourceLogical(next)
			return nil, e
		}
		releaseOwnedResourceLogical(next.right)
		next.right = child
		if child.priority < next.priority {
			next.right = child.left
			child.left = next
			return child, nil
		}
	}
	return next, nil
}

// Ordinary immutable traversal reuses the same tree-recursive shape as owned
// path construction and metadata retirement; it allocates no growable heap
// scratch slice. Native ordinal traversal must still charge every actual node
// and use its separately admitted finite cursor; this is not a work certificate.
func rangeOwnedResourceLogicalIndex(root *stableResourceLogicalIndexNode, visit func(*stableResourceEntry) bool) bool {
	if root == nil {
		return true
	}
	if !rangeOwnedResourceLogicalIndex(root.left, visit) {
		return false
	}
	if !visit(root.entry) {
		return false
	}
	return rangeOwnedResourceLogicalIndex(root.right, visit)
}
