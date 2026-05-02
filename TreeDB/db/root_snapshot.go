package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

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

func (s *Snapshot) GetEntryAtRoot(rootID uint64, key []byte) (node.LeafEntry, error) {
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return node.LeafEntry{}, err
	}
	return tr.GetEntry(key)
}

func (s *Snapshot) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return nil, err
	}
	return tr.Get(key)
}

func (s *Snapshot) GetAppendAtRoot(rootID uint64, key, dst []byte) ([]byte, error) {
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return dst, err
	}
	return tr.GetAppend(key, dst)
}

func (s *Snapshot) GetUnsafeAtRoot(rootID uint64, key []byte) ([]byte, error) {
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
	tr, err := s.treeAtRoot(rootID)
	if err != nil {
		return nil, err
	}
	return tr.IteratorWithOptions(start, end, opts), nil
}

func (s *Snapshot) HasManyAtRoot(rootID uint64, keys [][]byte) ([]bool, error) {
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
