package tree

import (
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var ErrOwnedIteratorLeafReader = errors.New("tree: bounded external-leaf reader required")

// ownedProjectionStorage has a single allocation and never enters iteratorPool.
// Root depth and reconstructed keys are checked before exceeding these arrays.
type ownedProjectionStorage struct {
	iterator    Iterator
	primary     primaryIterator
	deltaLeaf   [page.PageSize]byte
	stack       [maxTraversalDepth]CursorItem
	nodeKey     [page.PageSize]byte
	combinedKey [page.PageSize]byte
	leaf        [page.PageSize]byte
}

// OwnedPointerProjectionAllocationSize reports the raw single-allocation size.
// The caller must reserve it before constructing an iterator, and hold the
// reservation and exact tree/root owner until Close. Domain bytes are borrowed.
func OwnedPointerProjectionAllocationSize() uint64 {
	return uint64(unsafe.Sizeof(ownedProjectionStorage{}))
}

// OwnedPointerProjectionIterator exposes entry metadata without value decoding,
// mutable scratch growth, pooled retention, or an independent root registry.
// readLeaf must read into dst using its own pre-admitted bounded scratch. It may
// be nil when the captured root has no external leaves; an encountered external
// leaf then fails before invoking an allocating reader.
func (t *Tree) OwnedPointerProjectionIterator(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) iterator.UnsafeIterator {
	storage := new(ownedProjectionStorage)
	it := &storage.iterator
	*it = Iterator{
		tree: t, start: start, end: end, owned: true,
		mode: IteratorModePointerProjection, includeTombstones: true,
		emptyDomain:    start != nil && end != nil && compareTreeKey(start, end) >= 0,
		verifyAlways:   t.pager != nil && t.pager.VerifyOnRead(),
		nodeKeyScratch: storage.nodeKey[:], ownedStack: storage.stack[:],
		leafRefScratch: storage.leaf[:0], boundedLeafReader: readLeaf,
	}
	it.leafState.keyScratch = storage.combinedKey[:0]
	it.leafState.fixedScratch = true
	it.resetStack()
	if p, err := t.initPrimaryIterator(&storage.primary, it, start, end, IteratorOptions{Mode: IteratorModePointerProjection, IncludeTombstones: true}, false, readLeaf, storage.deltaLeaf[:], true); p != nil || err != nil {
		if err != nil {
			it.err = err
			return it
		}
		return p
	}
	it.Seek(start)
	return it
}

// OwnedUserRootEmpty proves emptiness from this exact captured tree root
// without iterator or key scratch allocation. A PRIMARY directory is empty only
// when it has no components and its digest-validated base is an empty leaf.
// External roots and nonempty/internal bases conservatively return false.
func (t *Tree) OwnedUserRootEmpty() (bool, error) {
	var n node.Node
	verify := t.pager != nil && t.pager.VerifyOnRead()
	if err := t.loadNodeViewWithLoadKindInto(&n, t.rootPageID, verify, false); err != nil {
		return false, err
	}
	if n.Type() != page.PageTypePrimaryDirectory {
		return n.Type() == page.PageTypeLeaf && n.Count() == 0, nil
	}
	directory, err := node.DecodePrimaryDirectory(n.Data())
	if err != nil {
		return false, err
	}
	if directory.Count() != 0 {
		return false, nil
	}
	base, _ := directory.Base()
	if err := t.loadChildRefViewInto(&n, base.Ref, true, false); err != nil {
		return false, err
	}
	if !node.VerifyPrimaryOperand(base, n.Data()) || (n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal) {
		return false, node.ErrPrimaryDirectory
	}
	return n.Type() == page.PageTypeLeaf && n.Count() == 0, nil
}
