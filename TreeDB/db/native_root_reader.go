package db

import (
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

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
