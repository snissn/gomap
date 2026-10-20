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
	return t.ownedPointerProjectionIterator(start, end, readLeaf, false)
}

// OwnedReversePointerProjectionIterator is the reverse traversal of the same
// exact tree owner and fixed scratch; it creates no independent snapshot owner.
func (t *Tree) OwnedReversePointerProjectionIterator(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) iterator.UnsafeIterator {
	return t.ownedPointerProjectionIterator(start, end, readLeaf, true)
}
func (t *Tree) ownedPointerProjectionIterator(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), reverse bool) iterator.UnsafeIterator {
	storage := new(ownedProjectionStorage)
	it := &storage.iterator
	*it = Iterator{
		tree: t, start: start, end: end, owned: true, reverse: reverse,
		mode: IteratorModePointerProjection, includeTombstones: true,
		emptyDomain:    start != nil && end != nil && compareTreeKey(start, end) >= 0,
		verifyAlways:   t.pager != nil && t.pager.VerifyOnRead(),
		nodeKeyScratch: storage.nodeKey[:], ownedStack: storage.stack[:],
		leafRefScratch: storage.leaf[:0], boundedLeafReader: readLeaf,
	}
	it.leafState.keyScratch = storage.combinedKey[:0]
	it.leafState.fixedScratch = true
	it.resetStack()

	if reverse {
		it.Seek(nil)
	} else {
		it.Seek(start)
	}
	return it
}

// OwnedUserRootEmpty proves emptiness from this exact captured tree root
// without iterator or key scratch allocation. External roots and nonempty or
// internal roots conservatively return false.
func (t *Tree) OwnedUserRootEmpty() (bool, error) {
	var n node.Node
	verify := t.pager != nil && t.pager.VerifyOnRead()
	if err := t.loadNodeViewWithLoadKindInto(&n, t.rootPageID, verify, false); err != nil {
		return false, err
	}
	return n.Type() == page.PageTypeLeaf && n.Count() == 0, nil
}
