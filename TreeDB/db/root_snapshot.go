package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// SnapshotRootReader is a root-bound read view owned by a Snapshot.
// It is valid only while the parent Snapshot remains open.
type SnapshotRootReader struct {
	owner *Snapshot
	tree  tree.Tree
	ok    bool
}

func (s *Snapshot) treeAtRoot(rootID uint64) (*tree.Tree, error) {
	if s == nil || s.closed.Load() {
		return nil, ErrClosed
	}
	if rootID == 0 {
		return nil, tree.ErrKeyNotFound
	}
	if s.state != nil && rootID == s.state.RootPageID {
		return &s.tree, nil
	}
	s.rootTreesMu.Lock()
	defer s.rootTreesMu.Unlock()
	for i := range s.rootTrees {
		if s.rootTrees[i].root == rootID {
			return &s.rootTrees[i].tree, nil
		}
	}
	s.rootTrees = append(s.rootTrees, snapshotRootTree{root: rootID})
	cached := &s.rootTrees[len(s.rootTrees)-1]
	cached.tree.Reset(s.idx.pager, &s.reader, rootID)
	return &cached.tree, nil
}

func (s *Snapshot) ReaderAtRoot(rootID uint64) (SnapshotRootReader, error) {
	if err := s.beginRead(); err != nil {
		return SnapshotRootReader{}, err
	}
	defer s.endRead()
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return SnapshotRootReader{}, err
	}
	return SnapshotRootReader{owner: s, tree: *tr, ok: true}, nil
}

func (r *SnapshotRootReader) beginRead() error {
	if r == nil || !r.ok || r.owner == nil {
		return ErrClosed
	}
	if err := r.owner.beginRead(); err != nil {
		return err
	}
	return nil
}

func (r *SnapshotRootReader) GetAppend(key, dst []byte) ([]byte, error) {
	if err := r.beginRead(); err != nil {
		return dst, err
	}
	defer r.owner.endRead()
	return r.tree.GetAppend(key, dst)
}

func (r *SnapshotRootReader) GetManyView(keys [][]byte, fn GetManyViewFunc) error {
	if err := r.beginRead(); err != nil {
		return err
	}
	defer r.owner.endRead()
	return r.tree.GetManyView(keys, fn)
}

func (s *Snapshot) GetEntryAtRoot(rootID uint64, key []byte) (node.LeafEntry, error) {
	if err := s.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer s.endRead()
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return node.LeafEntry{}, err
	}
	return tr.GetEntry(key)
}

func (s *Snapshot) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return nil, err
	}
	return tr.Get(key)
}

func (s *Snapshot) GetAppendAtRoot(rootID uint64, key, dst []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return dst, err
	}
	defer s.endRead()
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return dst, err
	}
	return tr.GetAppend(key, dst)
}

func (s *Snapshot) GetManyViewAtRoot(rootID uint64, keys [][]byte, fn GetManyViewFunc) error {
	if err := s.beginRead(); err != nil {
		return err
	}
	defer s.endRead()
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return err
	}
	return tr.GetManyView(keys, fn)
}

func (s *Snapshot) GetUnsafeAtRoot(rootID uint64, key []byte) ([]byte, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return nil, err
	}
	return tr.GetUnsafe(key)
}

func (s *Snapshot) IteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	return s.IteratorAtRootWithOptions(rootID, start, end, IteratorOptions{})
}

func (s *Snapshot) IteratorAtRootWithOptions(rootID uint64, start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	return s.bindRootIterator(rootID, start, end, opts, false)
}

func (s *Snapshot) bindRootIterator(rootID uint64, start, end []byte, opts IteratorOptions, reverse bool) (iterator.UnsafeIterator, error) {
	if s == nil {
		return nil, ErrClosed
	}
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if s.closed.Load() {
		return nil, ErrClosed
	}
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return nil, err
	}
	if reverse {
		return s.bindIteratorLocked(tr.ReverseIteratorWithOptions(start, end, opts))
	}
	return s.bindIteratorLocked(tr.IteratorWithOptions(start, end, opts))
}

func (s *Snapshot) ReverseIteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	return s.ReverseIteratorAtRootWithOptions(rootID, start, end, IteratorOptions{})
}

func (s *Snapshot) ReverseIteratorAtRootWithOptions(rootID uint64, start, end []byte, opts IteratorOptions) (iterator.UnsafeIterator, error) {
	return s.bindRootIterator(rootID, start, end, opts, true)
}

func (s *Snapshot) HasManyAtRoot(rootID uint64, keys [][]byte) ([]bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	out := make([]bool, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	tr, err := s.treeAtRoot(rootID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	return tr.HasMany(keys)
}

func (s *Snapshot) HasAnySortedAtRoot(rootID uint64, keys [][]byte) (bool, error) {
	if err := s.beginRead(); err != nil {
		return false, err
	}
	defer s.endRead()
	if len(keys) == 0 {
		return false, nil
	}
	tr, err := s.treeAtRoot(rootID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	ok, stats, err := tr.HasAnySortedWithStats(keys)
	if stats.FallbackCalls != 0 {
		s.db.noteRootProbeKeyFallback(stats)
	}
	return ok, err
}

func (s *Snapshot) HasPrefixesAtRoot(rootID uint64, prefixes [][]byte) ([]bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	out := make([]bool, len(prefixes))
	if len(prefixes) == 0 {
		return out, nil
	}
	tr, err := s.treeAtRoot(rootID)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	out, stats, err := tr.HasPrefixesWithStats(prefixes)
	if stats.FallbackCalls != 0 {
		s.db.noteRootProbePrefixFallback(stats)
	}
	return out, err
}
