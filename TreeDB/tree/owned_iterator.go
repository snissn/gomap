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

// PRIMARY roots need a second traversal for their directory components. Plain
// roots never allocate or zero this state. Either shape has one storage owner.
type ownedPrimaryProjectionStorage struct {
	ownedProjectionStorage
	primary   primaryIterator
	deltaLeaf [page.PageSize]byte
}

// OwnedPointerProjectionWorkspace belongs to one preadmitted operation. It
// retains only layouts actually encountered; ordinary roots keep compact backing.
// It is not concurrency safe: the caller serializes lending and Reset, and must
// close the previous iterator before Reset or releasing its original allowance.
// No borrowed entry remains valid after Close/Reset.
type OwnedPointerProjectionWorkspace struct {
	ordinary *ownedProjectionStorage
	primary  *ownedPrimaryProjectionStorage
	inUse    bool
}

var ErrOwnedProjectionWorkspaceBusy = errors.New("tree: owned projection workspace is in use")
var ErrOwnedProjectionWorkspaceAdmission = errors.New("tree: owned projection storage admission required")

func (s *OwnedPointerProjectionWorkspace) InUse() bool { return s.inUse }

// Reset detaches root/page/key/directory/callback aliases, including Node views
// in fixed stack backing, while retaining exclusively owned arrays for reuse.
func (s *OwnedPointerProjectionWorkspace) Reset() {
	if s.ordinary != nil {
		s.ordinary.iterator = Iterator{}
		clear(s.ordinary.stack[:])
	}
	if s.primary != nil {
		s.primary.iterator = Iterator{}
		s.primary.primary = primaryIterator{}
		clear(s.primary.stack[:])
	}
	s.inUse = false
}

// OwnedPointerProjectionIteratorWithWorkspace uses the original authenticated
// constructor and fixed layouts. admitStorage receives RAW maximum constructor
// bytes, once BEFORE allocating each missing layout. The caller applies its
// allocator's rounding and retains every layout's allowance through workspace
// disposal. Overlapping users must choose independently admitted storage.
func (t *Tree) OwnedPointerProjectionIteratorWithWorkspace(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), storage *OwnedPointerProjectionWorkspace, admitStorage func(uint64) error) (iterator.UnsafeIterator, error) {
	if storage == nil || storage.inUse {
		return nil, ErrOwnedProjectionWorkspaceBusy
	}
	return t.ownedPointerProjectionIteratorStorage(start, end, readLeaf, false, storage, admitStorage)
}

// OwnedPointerProjectionAllocationSize reports the largest raw single-allocation size.
// The caller must reserve it before constructing an iterator, and hold the
// reservation and exact tree/root owner until Close. Domain bytes are borrowed.
// Ordinary roots retain that same conservative maximum admission promise.
func OwnedPointerProjectionAllocationSize() uint64 {
	return uint64(unsafe.Sizeof(ownedPrimaryProjectionStorage{}))
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
	it, _ := t.ownedPointerProjectionIteratorStorage(start, end, readLeaf, reverse, nil, nil)
	return it
}

func (t *Tree) ownedPointerProjectionIteratorStorage(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), reverse bool, workspace *OwnedPointerProjectionWorkspace, admitStorage func(uint64) error) (iterator.UnsafeIterator, error) {
	// Use the same checked root probe for both constructors and reuse its view
	// for PRIMARY initialization. Shape selection adds no root read or owner.
	var root node.Node
	rootErr := t.loadNodeViewWithLoadKindInto(&root, t.rootPageID, true, true)
	isPrimary := rootErr == nil && root.Type() == page.PageTypePrimaryDirectory
	if workspace != nil && ((isPrimary && workspace.primary == nil) || (!isPrimary && workspace.ordinary == nil)) {
		if admitStorage == nil {
			return nil, ErrOwnedProjectionWorkspaceAdmission
		}
		if err := admitStorage(OwnedPointerProjectionAllocationSize()); err != nil {
			return nil, err
		}
	}
	var storage *ownedProjectionStorage
	var primary *primaryIterator
	var deltaLeaf []byte
	if isPrimary {
		var owner *ownedPrimaryProjectionStorage
		if workspace != nil {
			owner = workspace.primary
		}
		if owner == nil {
			owner = new(ownedPrimaryProjectionStorage)
			if workspace != nil {
				workspace.primary = owner
			}
		}
		storage, primary, deltaLeaf = &owner.ownedProjectionStorage, &owner.primary, owner.deltaLeaf[:]
	} else {
		if workspace != nil {
			storage = workspace.ordinary
		}
		if storage == nil {
			storage = new(ownedProjectionStorage)
			if workspace != nil {
				workspace.ordinary = storage
			}
		}
	}
	if workspace != nil {
		workspace.inUse = true
	}
	it := &storage.iterator
	t.initOwnedProjectionIterator(it, start, end, readLeaf, reverse, storage.nodeKey[:], storage.stack[:], storage.combinedKey[:], storage.leaf[:])
	if rootErr != nil {
		it.err = rootErr
		return it, nil
	}
	if p, err := t.initPrimaryIteratorFromRoot(&root, primary, it, start, end, IteratorOptions{Mode: IteratorModePointerProjection, IncludeTombstones: true}, reverse, readLeaf, deltaLeaf, true); p != nil || err != nil {
		if err != nil {
			it.err = err
			return it, nil
		}
		return p, nil
	}
	if reverse {
		it.Seek(nil)
	} else {
		it.Seek(start)
	}
	return it, nil
}

func (t *Tree) initOwnedProjectionIterator(it *Iterator, start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), reverse bool, nodeKey []byte, stack []CursorItem, combinedKey, leaf []byte) {
	*it = Iterator{
		tree: t, start: start, end: end, owned: true, reverse: reverse,
		mode: IteratorModePointerProjection, includeTombstones: true,
		emptyDomain:    start != nil && end != nil && compareTreeKey(start, end) >= 0,
		verifyAlways:   t.pager != nil && t.pager.VerifyOnRead(),
		nodeKeyScratch: nodeKey, ownedStack: stack,
		leafRefScratch: leaf[:0], boundedLeafReader: readLeaf,
	}
	it.leafState.keyScratch = combinedKey[:0]
	it.leafState.fixedScratch = true
	it.resetStack()
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
