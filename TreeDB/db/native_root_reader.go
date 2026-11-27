package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
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

// OwnedPointerProjectionIteratorWithWorkspace retains the same exact snapshot
// authority and error cleanup as the ordinary constructor. The workspace owner
// admits its header and raw-size callback before this call; the callback admits
// each missing layout before allocation. Successful users Close then Reset.
func (r *SnapshotRootReader) OwnedPointerProjectionIteratorWithWorkspace(start, end []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), storage *tree.OwnedPointerProjectionWorkspace, admitStorage func(uint64) error) (iterator.UnsafeIterator, error) {
	if err := r.beginRead(); err != nil {
		return nil, err
	}
	defer r.owner.endRead()
	it, err := r.tree.OwnedPointerProjectionIteratorWithWorkspace(start, end, readLeaf, storage, admitStorage)
	if err != nil {
		return nil, err
	}
	if err := it.Error(); err != nil {
		_ = it.Close()
		storage.Reset()
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

// GetEntryExactWithPinnedCOWLeafCached performs a fixed-scratch point read on
// this captured root with optional stack-local leaf-page caching under one
// snapshot read pin, carrying verified leaf-page checksum proofs back to the
// owner's leaf page cache.
func (r *SnapshotRootReader) GetEntryExactWithPinnedCOWLeafCached(
	key, keyScratch, leafScratch []byte,
	cache *tree.FixedScratchLeafCache,
	limits valuelog.COWReadLimits,
	readLeaf func(page.LeafLogPtr, []byte, PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error),
) (node.LeafEntry, error) {
	if err := r.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer r.owner.endRead()
	if readLeaf == nil {
		return r.tree.GetEntryWithFixedScratchLeafCache(key, keyScratch, leafScratch, cache, nil)
	}
	return r.tree.GetEntryWithFixedScratchLeafProofCached(key, keyScratch, leafScratch, cache, func(ptr page.LeafLogPtr, dst []byte) ([]byte, tree.LeafLogPageReadState, error) {
		record, err := r.owner.inspectPinnedCOWLeafRecord(ptr, limits)
		if err != nil {
			return nil, tree.LeafLogPageReadState{}, err
		}
		return readLeaf(ptr, dst, record)
	}, r.owner.reader.leafPageCache.markCopiedPageChecksumVerified)
}

func markPinnedCOWLeafViewChecksumVerified(lease tree.LeafLogPageViewLease) {
	if slot, ok := lease.(*leafPageReadCacheSlot); ok {
		slot.markCopiedPageChecksumVerifiedLocked()
	}
}

// GetEntryExactAtRootWithPinnedCOWLeafView combines NativeReaderAtRoot and
// GetEntryExactWithPinnedCOWLeafView under a single snapshot beginRead/endRead
// pin, halving readState atomic contention on concurrent point reads. When
// readScratch is non-empty, unmapped outer-leaf misses read the full record
// into readScratch in one positional ReadAt syscall without heap allocation.
func (s *Snapshot) GetEntryExactAtRootWithPinnedCOWLeafView(
	rootID uint64,
	key, keyScratch, leafScratch, readScratch []byte,
	limits valuelog.COWReadLimits,
	checkShape func(valuelog.COWRecordShape) bool,
	readLeaf func(page.LeafLogPtr, []byte, PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error),
	consume func(node.LeafEntry) error,
) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	if rootID == 0 {
		return tree.ErrKeyNotFound
	}
	var tr tree.Tree
	if s.state != nil && rootID == s.state.RootPageID {
		tr = s.tree
	} else {
		tr.Reset(s.idx.pager, &s.reader, rootID)
	}
	return s.getEntryExactWithPinnedCOWLeafViewLocked(&tr, key, keyScratch, leafScratch, readScratch, limits, checkShape, readLeaf, consume)
}

// GetEntryExactWithPinnedCOWLeafView performs a bounded fixed-scratch point read
// on this captured root that borrows verified leaf pages directly from the
// owner's leaf page cache under a read lock (avoiding metadata reinspection and
// 4 KiB page copies) and passes the found entry to consume before releasing the
// borrowed page view. Any policy/limit/identity mismatch falls back to full
// inspection and readLeaf for canonical error reporting.
func (r *SnapshotRootReader) GetEntryExactWithPinnedCOWLeafView(
	key, keyScratch, leafScratch []byte,
	limits valuelog.COWReadLimits,
	checkShape func(valuelog.COWRecordShape) bool,
	readLeaf func(page.LeafLogPtr, []byte, PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error),
	consume func(node.LeafEntry) error,
) error {
	if err := r.beginRead(); err != nil {
		return err
	}
	defer r.owner.endRead()
	return r.owner.getEntryExactWithPinnedCOWLeafViewLocked(&r.tree, key, keyScratch, leafScratch, nil, limits, checkShape, readLeaf, consume)
}

func (s *Snapshot) getEntryExactWithPinnedCOWLeafViewLocked(
	tr *tree.Tree,
	key, keyScratch, leafScratch, readScratch []byte,
	limits valuelog.COWReadLimits,
	checkShape func(valuelog.COWRecordShape) bool,
	readLeaf func(page.LeafLogPtr, []byte, PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error),
	consume func(node.LeafEntry) error,
) error {
	if readLeaf == nil {
		entry, err := tr.GetEntryWithFixedScratchLeafCache(key, keyScratch, leafScratch, nil, nil)
		if err != nil {
			return err
		}
		if consume != nil {
			return consume(entry)
		}
		return nil
	}
	cache := s.reader.leafPageCache
	var readView func(page.LeafLogPtr) ([]byte, tree.LeafLogPageViewLease, tree.LeafLogPageReadState, bool)
	var markView func(tree.LeafLogPageViewLease)
	if cache != nil {
		readView = func(ptr page.LeafLogPtr) ([]byte, tree.LeafLogPageViewLease, tree.LeafLogPageReadState, bool) {
			f, identity, err := s.pinnedValueLogSegment(ptr.ValueLogFileID())
			if err != nil {
				return nil, nil, tree.LeafLogPageReadState{}, false
			}
			data, slot, state, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, limits, checkShape)
			if !ok {
				return nil, nil, tree.LeafLogPageReadState{}, false
			}
			return data, slot, tree.LeafLogPageReadState(state), true
		}
		markView = markPinnedCOWLeafViewChecksumVerified
	}
	return tr.GetEntryWithFixedScratchLeafView(
		key, keyScratch, leafScratch,
		readView, markView,
		func(ptr page.LeafLogPtr, dst []byte) ([]byte, tree.LeafLogPageReadState, error) {
			record, err := s.inspectPinnedCOWLeafRecordInto(ptr, limits, readScratch)
			if err != nil {
				return nil, tree.LeafLogPageReadState{}, err
			}
			return readLeaf(ptr, dst, record)
		},
		cache.markCopiedPageChecksumVerified,
		consume,
	)
}

// WithPinnedCOWPointRead holds one snapshot read pin across a batch of
// fixed-scratch point reads on this captured root, carrying verified leaf-page
// checksum proofs back to the owner's leaf page cache.
func (r *SnapshotRootReader) WithPinnedCOWPointRead(
	limits valuelog.COWReadLimits,
	readLeaf func(page.LeafLogPtr, []byte, PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error),
	fn func(read func(key, keyScratch, leafScratch []byte, cache *tree.FixedScratchLeafCache) (node.LeafEntry, error)) error,
) error {
	if err := r.beginRead(); err != nil {
		return err
	}
	defer r.owner.endRead()
	if fn == nil {
		return nil
	}
	if readLeaf == nil {
		return fn(func(key, keyScratch, leafScratch []byte, cache *tree.FixedScratchLeafCache) (node.LeafEntry, error) {
			return r.tree.GetEntryWithFixedScratchLeafCache(key, keyScratch, leafScratch, cache, nil)
		})
	}
	return fn(func(key, keyScratch, leafScratch []byte, cache *tree.FixedScratchLeafCache) (node.LeafEntry, error) {
		return r.tree.GetEntryWithFixedScratchLeafProofCached(key, keyScratch, leafScratch, cache, func(ptr page.LeafLogPtr, dst []byte) ([]byte, tree.LeafLogPageReadState, error) {
			record, err := r.owner.inspectPinnedCOWLeafRecord(ptr, limits)
			if err != nil {
				return nil, tree.LeafLogPageReadState{}, err
			}
			return readLeaf(ptr, dst, record)
		}, r.owner.reader.leafPageCache.markCopiedPageChecksumVerified)
	})
}

// WithPinnedCOWPointReadView holds one snapshot read pin across a batch of
// fixed-scratch point reads on this captured root, borrowing verified cached
// leaf pages directly under a read lock (avoiding metadata reinspection and
// 4 KiB page copies) and falling back to full inspection and readLeaf on misses
// or policy mismatches.
func (r *SnapshotRootReader) WithPinnedCOWPointReadView(
	limits valuelog.COWReadLimits,
	checkShape func(valuelog.COWRecordShape) bool,
	readLeaf func(page.LeafLogPtr, []byte, PinnedCOWLeafRead) ([]byte, tree.LeafLogPageReadState, error),
	fn func(read func(key, keyScratch, leafScratch []byte, consume func(node.LeafEntry) error) error) error,
) error {
	if err := r.beginRead(); err != nil {
		return err
	}
	defer r.owner.endRead()
	if fn == nil {
		return nil
	}
	if readLeaf == nil {
		return fn(func(key, keyScratch, leafScratch []byte, consume func(node.LeafEntry) error) error {
			entry, err := r.tree.GetEntryWithFixedScratchLeafCache(key, keyScratch, leafScratch, nil, nil)
			if err != nil {
				return err
			}
			if consume != nil {
				return consume(entry)
			}
			return nil
		})
	}
	cache := r.owner.reader.leafPageCache
	var readView func(page.LeafLogPtr) ([]byte, tree.LeafLogPageViewLease, tree.LeafLogPageReadState, bool)
	var markView func(tree.LeafLogPageViewLease)
	if cache != nil {
		readView = func(ptr page.LeafLogPtr) ([]byte, tree.LeafLogPageViewLease, tree.LeafLogPageReadState, bool) {
			f, identity, err := r.owner.pinnedValueLogSegment(ptr.ValueLogFileID())
			if err != nil {
				return nil, nil, tree.LeafLogPageReadState{}, false
			}
			data, slot, state, ok := cache.getVerifiedCOWViewLocked(ptr, identity, f, limits, checkShape)
			if !ok {
				return nil, nil, tree.LeafLogPageReadState{}, false
			}
			return data, slot, tree.LeafLogPageReadState(state), true
		}
		markView = markPinnedCOWLeafViewChecksumVerified
	}
	fallbackRead := func(ptr page.LeafLogPtr, dst []byte) ([]byte, tree.LeafLogPageReadState, error) {
		record, err := r.owner.inspectPinnedCOWLeafRecord(ptr, limits)
		if err != nil {
			return nil, tree.LeafLogPageReadState{}, err
		}
		return readLeaf(ptr, dst, record)
	}
	markCopy := cache.markCopiedPageChecksumVerified
	return fn(func(key, keyScratch, leafScratch []byte, consume func(node.LeafEntry) error) error {
		return r.tree.GetEntryWithFixedScratchLeafView(
			key, keyScratch, leafScratch,
			readView, markView,
			fallbackRead, markCopy,
			consume,
		)
	})
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
