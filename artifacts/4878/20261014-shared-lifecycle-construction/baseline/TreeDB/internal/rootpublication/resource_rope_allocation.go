package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

type resourceRopeMetadata struct {
	owner   *retainedalloc.Owner
	charge  uint64
	backing *resourceEntryBacking
	// Used only after the existing physical refs reach zero. This admitted
	// node storage holds the synchronous ordinary release worklist.
	releaseNext  *stableResourceEntryNode
	releaseStage uint8
	releaseEntry int
}

// The existing rope refs remain the sole physical/rope allocation lifecycle.
// Each owned leaf also retains its exact immutable entry-array backing so
// metadata indexes and physical callbacks may retire in either order.
func newOwnedStableResourceEntryLeaf(backing *resourceEntryBacking, start, end int, owner *retainedalloc.Owner) (*stableResourceEntryNode, error) {
	if owner == nil || backing == nil || backing.allocation == nil || backing.allocation.owner != owner || start < 0 || end <= start || end > len(backing.entries) {
		return nil, ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableResourceEntryNode{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceRopeMetadata{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	if !backing.allocation.retain() {
		owner.RemovePending(charge)
		return nil, ErrResourceOwnership
	}
	node := &stableResourceEntryNode{metadata: &resourceRopeMetadata{owner: owner, charge: charge, backing: backing}, entries: backing.entries[start:end:end]}
	node.refs.Store(1)
	return node, nil
}

// Concatenation consumes the same two real physical root edges only after its
// allocation is admitted. A refusal leaves both input roots wholly untouched.
func concatOwnedAdmittedStableResourceEntryNodes(left, right *stableResourceEntryNode, owner *retainedalloc.Owner) (*stableResourceEntryNode, error) {
	if owner == nil || (left != nil && (left.metadata == nil || left.metadata.owner != owner)) || (right != nil && (right.metadata == nil || right.metadata.owner != owner)) {
		return nil, ErrResourceOwnership
	}
	if left == nil {
		return right, nil
	}
	if right == nil {
		return left, nil
	}
	if owner == nil {
		return nil, ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stableResourceEntryNode{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceRopeMetadata{})))
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	node := &stableResourceEntryNode{metadata: &resourceRopeMetadata{owner: owner, charge: charge}, left: left, right: right}
	node.refs.Store(1)
	return node, nil
}

// The existing physical refs are the only ownership authority. A successful
// last-reference decrement makes the admitted node exclusive, so its embedded
// link can replace the formerly growable release slice. No release-time
// allocation, second registry, or background owner is introduced.
func releaseOwnedResourceRope(root *stableResourceEntryNode) error {
	if root == nil {
		return nil
	}
	if root.cleanupDone.Load() {
		if root.cleanupFailure.causes[0] != nil {
			return &root.cleanupFailure
		}
		return nil
	}
	claim := func(node *stableResourceEntryNode) bool {
		if node == nil {
			return false
		}
		for refs := node.refs.Load(); refs > 0; refs = node.refs.Load() {
			if node.refs.CompareAndSwap(refs, refs-1) {
				return refs == 1
			}
		}
		return false
	}
	if !claim(root) {
		return nil
	}
	current := root
	fail := func(err error) error {
		// Keep every unresolved original ancestor and first refusal. Successful
		// child edges already cleared their aliases; no action may be replayed.
		for node := current; node != nil; node = node.metadata.releaseNext {
			node.cleanupFailure.original, node.cleanupFailure.causes[0] = node, err
			node.cleanupDone.Store(true)
			node.metadata.owner.CleanupFailed()
		}
		return &root.cleanupFailure
	}
	for current != nil {
		metadata := current.metadata
		if metadata.releaseStage < 2 {
			stage := metadata.releaseStage
			metadata.releaseStage++
			child := current.left
			if stage == 1 {
				child = current.right
			}
			if child != nil && child.cleanupDone.Load() && child.cleanupFailure.causes[0] != nil {
				return fail(&child.cleanupFailure)
			}
			if claim(child) {
				child.metadata.releaseNext = current
				current = child
				continue
			}
			if stage == 0 {
				current.left = nil
			} else {
				current.right = nil
			}
			continue
		}
		// Internal nodes have no entries; leaves release the original physical
		// token edge before releasing their independent immutable entry backing.
		for metadata.releaseEntry < len(current.entries) {
			if err := current.entries[metadata.releaseEntry].token.releaseFrom(ResourceOwnerShared); err != nil {
				return fail(err)
			}
			metadata.releaseEntry++
		}
		if err := metadata.backing.release(); err != nil {
			return fail(err)
		}
		metadata.backing = nil
		parent := metadata.releaseNext
		current.left, current.right, current.entries = nil, nil, nil
		owner, charge := metadata.owner, metadata.charge
		metadata.owner, metadata.charge, metadata.releaseNext = nil, 0, nil
		current.metadata = nil
		current.cleanupDone.Store(true)
		owner.RemovePending(charge)
		if parent != nil {
			if parent.metadata.releaseStage == 1 {
				parent.left = nil
			} else {
				parent.right = nil
			}
		}
		current = parent
	}
	return nil
}
