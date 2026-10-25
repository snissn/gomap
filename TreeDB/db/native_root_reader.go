package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// OwnedPointerProjectionIterator borrows this reader's exact tree and snapshot.
// The caller admits tree.OwnedPointerProjectionAllocationSize before calling,
// serializes use with its enclosing read scope, and keeps this reader at a
// stable address until the iterator is closed. It adds no snapshot/root owner.
func (r *SnapshotRootReader) OwnedPointerProjectionIterator(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (iterator.UnsafeIterator, error) {
	if err := r.beginRead(); err != nil {
		return nil, err
	}
	defer r.owner.endRead()
	it := r.tree.OwnedPointerProjectionIterator(start, end, readLeaf)
	if err := it.Error(); err != nil {
		_ = it.Close()
		return nil, err
	}
	return it, nil
}

// NativeReaderAtRoot constructs a by-value reader without adding to the
// snapshot's root cache. The caller admits its containing storage before this
// call and retains this exact snapshot until all reader operations and borrowed
// entry uses finish. The root must belong to that captured snapshot's closure;
// this method does not discover or validate a current catalog root.
func (s *Snapshot) NativeReaderAtRoot(rootID uint64) (SnapshotRootReader, error) {
	if err := s.beginRead(); err != nil {
		return SnapshotRootReader{}, err
	}
	defer s.endRead()
	if rootID == 0 {
		return SnapshotRootReader{}, tree.ErrKeyNotFound
	}
	r := SnapshotRootReader{owner: s, ok: true}
	if s.state != nil && rootID == s.state.RootPageID {
		// Preserve PRIMARY page namespace and captured bounds as well as root ID.
		r.tree = s.tree
	} else {
		r.tree.Reset(s.idx.pager, &s.reader, rootID)
	}
	return r, nil
}

// GetEntryExactWithFixedScratch returns metadata borrowed from this reader's
// captured snapshot and caller-admitted fixed scratch. Those owners must remain
// live until the entry is consumed; scratch cannot be reused concurrently.
func (r *SnapshotRootReader) GetEntryExactWithFixedScratch(key, keyScratch, leafScratch []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, error) {
	if err := r.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer r.owner.endRead()
	return r.tree.GetEntryWithFixedScratch(key, keyScratch, leafScratch, readLeaf)
}

// SeekGEEntryWithFixedScratch includes physical tombstones, leaving suppression
// to the captured native merge. Returned bytes borrow the snapshot and scratch.
func (r *SnapshotRootReader) SeekGEEntryWithFixedScratch(start, end, keyScratch, leafScratch []byte, scratch *tree.OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	if err := r.beginRead(); err != nil {
		return node.LeafEntry{}, false, err
	}
	defer r.owner.endRead()
	return r.tree.SeekGEWithFixedScratch(start, end, keyScratch, leafScratch, scratch, readLeaf)
}

// OwnedRootEmpty checks only this captured root, without constructing an iterator.
func (r *SnapshotRootReader) OwnedRootEmpty() (bool, error) {
	if err := r.beginRead(); err != nil {
		return false, err
	}
	defer r.owner.endRead()
	return r.tree.OwnedUserRootEmpty()
}

// SeekLTEntryWithFixedScratch is the DATA predecessor counterpart of SeekGE.
// Results borrow this exact snapshot and the same exclusively admitted scratch.
func (r *SnapshotRootReader) SeekLTEntryWithFixedScratch(start, end, keyScratch, leafScratch []byte, scratch *tree.OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	if err := r.beginRead(); err != nil {
		return node.LeafEntry{}, false, err
	}
	defer r.owner.endRead()
	return r.tree.SeekLTWithFixedScratch(start, end, keyScratch, leafScratch, scratch, readLeaf)
}

// GetEntryExactWithFixedScratchView is the scoped state/view counterpart of the
// original fixed-destination point read. The caller retains this snapshot and
// consumes all borrowed entry bytes before releasing the returned cache lease.
func (r *SnapshotRootReader) GetEntryExactWithFixedScratchView(key, keyScratch, leafScratch []byte, readLeaf tree.LeafLogPointViewReader) (node.LeafEntry, tree.LeafLogPageViewLease, error) {
	if err := r.beginRead(); err != nil {
		return node.LeafEntry{}, nil, err
	}
	defer r.owner.endRead()
	return r.tree.GetEntryWithFixedScratchView(key, keyScratch, leafScratch, readLeaf)
}
