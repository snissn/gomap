package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

// An owned physical index owns its pointer-vector allocation and each vector
// entry's immutable array backing. Those are independent of content pins.
func cloneOwnedResourcePhysical(root *stableResourcePhysicalIndexNode, count int, owner *retainedalloc.Owner) (*stableResourcePhysicalIndexNode, error) {
	if owner == nil || count < 0 || (root != nil && (root.allocation == nil || root.allocation.owner != owner)) {
		return nil, ErrResourceOwnership
	}
	width := uint64(unsafe.Sizeof((*stableResourceEntry)(nil)))
	if uint64(count) > ^uint64(0)/width {
		return nil, retainedalloc.ErrCapacity
	}
	wrapper := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableResourcePhysicalIndexNode{})))
	vector := retainedalloc.AllocationCharge(uint64(count) * width)
	if vector > ^uint64(0)-wrapper {
		return nil, retainedalloc.ErrCapacity
	}
	allocation, err := newResourceAllocation(owner, wrapper+vector)
	if err != nil {
		return nil, err
	}
	next := &stableResourcePhysicalIndexNode{allocation: allocation, entries: make([]*stableResourceEntry, count)}
	if root == nil {
		return next, nil
	}
	next.key, next.priority = root.key, root.priority
	if !root.left.retainAllocation() {
		releaseOwnedResourcePhysical(next)
		return nil, ErrResourceOwnership
	}
	next.left = root.left
	if !root.right.retainAllocation() {
		releaseOwnedResourcePhysical(next)
		return nil, ErrResourceOwnership
	}
	next.right = root.right
	for i, entry := range root.entries {
		if i == count {
			break
		}
		if !entry.retainBacking() {
			releaseOwnedResourcePhysical(next)
			return nil, ErrResourceOwnership
		}
		next.entries[i] = entry
	}
	return next, nil
}

func (node *stableResourcePhysicalIndexNode) retainAllocation() bool {
	return node == nil || node.allocation.retain()
}
func releaseOwnedResourcePhysical(node *stableResourcePhysicalIndexNode) {
	if node == nil || !node.allocation.drop() {
		return
	}
	left, right := node.left, node.right
	node.left, node.right = nil, nil
	for i, entry := range node.entries {
		node.entries[i] = nil
		entry.releaseBacking()
	}
	node.entries = nil
	node.key = stablePhysicalIdentityKey{}
	releaseOwnedResourcePhysical(left)
	releaseOwnedResourcePhysical(right)
	node.allocation.refund()
}

// insertOwnedResourcePhysical owns the returned root; input remains retained
// by its caller. Private rotations move ownership edges without copying nodes.
func insertOwnedResourcePhysical(root *stableResourcePhysicalIndexNode, entry *stableResourceEntry, owner *retainedalloc.Owner) (*stableResourcePhysicalIndexNode, error) {
	if entry == nil || entry.token == nil || owner == nil || entry.backing == nil || entry.backing.allocation.owner != owner {
		return nil, ErrResourceOwnership
	}
	key := entry.token.physicalIdentityKey()
	count := 1
	if root != nil {
		count = len(root.entries)
		if key == root.key {
			count++
		}
	}
	next, err := cloneOwnedResourcePhysical(root, count, owner)
	if err != nil {
		return nil, err
	}
	if root == nil || key == root.key {
		if !entry.retainBacking() {
			releaseOwnedResourcePhysical(next)
			return nil, ErrResourceOwnership
		}
		next.entries[count-1] = entry
		next.key, next.priority = key, stableResourcePhysicalPriority(key)
		return next, nil
	}
	if stablePhysicalIdentityKeyLess(key, root.key) {
		child, e := insertOwnedResourcePhysical(root.left, entry, owner)
		if e != nil {
			releaseOwnedResourcePhysical(next)
			return nil, e
		}
		releaseOwnedResourcePhysical(next.left)
		next.left = child
		if child.priority < next.priority {
			next.left = child.right
			child.right = next
			return child, nil
		}
	} else {
		child, e := insertOwnedResourcePhysical(root.right, entry, owner)
		if e != nil {
			releaseOwnedResourcePhysical(next)
			return nil, e
		}
		releaseOwnedResourcePhysical(next.right)
		next.right = child
		if child.priority < next.priority {
			next.right = child.left
			child.left = next
			return child, nil
		}
	}
	return next, nil
}

// Replace one exact logical representative under an unchanged physical key.
// Both original and replacement entry arrays remain independently retained.
func replaceOwnedResourcePhysical(root *stableResourcePhysicalIndexNode, original, entry *stableResourceEntry, owner *retainedalloc.Owner) (*stableResourcePhysicalIndexNode, error) {
	if root == nil || original == nil || entry == nil || !original.token.samePhysicalIdentity(entry.token) {
		return nil, ErrResourceConflict
	}
	next, err := cloneOwnedResourcePhysical(root, len(root.entries), owner)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*stableResourcePhysicalIndexNode, error) {
		releaseOwnedResourcePhysical(next)
		return nil, e
	}
	key := original.token.physicalIdentityKey()
	if key == root.key {
		for i, old := range next.entries {
			if old == original {
				if !entry.retainBacking() {
					return fail(ErrResourceOwnership)
				}
				next.entries[i] = entry
				old.releaseBacking()
				return next, nil
			}
		}
		return fail(ErrResourceConflict)
	}
	if stablePhysicalIdentityKeyLess(key, root.key) {
		child, e := replaceOwnedResourcePhysical(root.left, original, entry, owner)
		if e != nil {
			return fail(e)
		}
		releaseOwnedResourcePhysical(next.left)
		next.left = child
	} else {
		child, e := replaceOwnedResourcePhysical(root.right, original, entry, owner)
		if e != nil {
			return fail(e)
		}
		releaseOwnedResourcePhysical(next.right)
		next.right = child
	}
	return next, nil
}
