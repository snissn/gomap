package db

import (
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
	return tree.New(s.idx.pager, &s.reader, rootID), nil
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
