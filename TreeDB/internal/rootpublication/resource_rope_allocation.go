package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"unsafe"
)

type resourceRopeMetadata struct {
	release resourceReleaseFrameV6
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
func releaseOwnedResourceRope(node *stableResourceEntryNode) error {
	if node == nil {
		return nil
	}
	var root resourceReleaseFrameV6
	if ready, err := root.rope(node, nil); !ready || err != nil {
		return err
	}
	if root.current == nil {
		return nil
	}
	driver := root.current
	driver.parent = nil
	driver.current = driver
	completion, _, err := driver.continueWork(nil)
	if err != nil {
		return err
	}
	node = nil
	completion.complete()
	return nil
}
