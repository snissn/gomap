package tree

import (
	"reflect"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// OwnedPointScratch retains only an authenticated external-leaf image in the
// caller's private leafScratch. It adds no root/file pin or independent cache.
// Admit the containing storage (including this header and target array) before
// constructing it. The exact immutable tree owner and all scratch must stay live
// and exclusively borrowed until returned entry views have been consumed.
//
// Invalidate before any other writer uses either scratch backing, even if that
// writer later restores the same bytes. Do not copy a live OwnedPointScratch.
// Its zero value is ready for use.
type OwnedPointScratch struct {
	pager                     *pager.Pager
	reader                    SlabReader
	root, limit, namespace    uint64
	local, primary            bool
	keyBase, leafBase         *byte
	keyCapacity, leafCapacity int
	low, high                 []byte
	leaf                      node.Node
	target                    [page.PageSize]byte
}

// Invalidate detaches every borrowed view. It deliberately leaves target bytes
// untouched: an in-flight lookup's copied target may still borrow this array.
func (s *OwnedPointScratch) Invalidate() {
	if s == nil {
		return
	}
	s.pager, s.reader = nil, nil
	s.root, s.limit, s.namespace = 0, 0, 0
	s.local, s.primary = false, false
	s.keyBase, s.leafBase = nil, nil
	s.keyCapacity, s.leafCapacity = 0, 0
	s.low, s.high = nil, nil
	s.leaf = node.Node{}
}

func (s *OwnedPointScratch) exactHit(key []byte) (node.LeafEntry, bool, error) {
	if s.primary || s.leaf.Data() == nil {
		return node.LeafEntry{}, false, nil
	}
	// These are the original root's fences, not an inferred leaf interval.
	if len(s.low) > 0 && compareTreeKey(key, s.low) < 0 || len(s.high) > 0 && compareTreeKey(key, s.high) >= 0 {
		return node.LeafEntry{}, true, ErrKeyNotFound
	}
	index, found, err := s.leaf.SearchLeaf(key)
	if err != nil || !found {
		return node.LeafEntry{}, false, err
	}
	k, v, p, f, r, err := s.leaf.GetLeafEntryViewWithRevision(index)
	return node.LeafEntry{Key: k, Value: v, ValuePtr: p, Flags: f, Revision: r}, true, err
}

// GetEntryWithOwnedScratch is the opt-in immutable-private-leaf counterpart of
// GetEntryWithFixedScratch. The old API continues to traverse and validate on
// every call, including corruption checks. This API can reuse exact successful
// hits only; all misses traverse normally. PRIMARY and VerifyOnRead never reuse.
//
// keyScratch, leafScratch and scratch's embedded target must have disjoint
// backing. readLeaf must return the authenticated page in leafScratch exactly,
// under the same immutable owner; it must use separately admitted decode storage.
// Returned entry bytes borrow that owner and these scratch backings, and expire
// on the next operation using them. Invalidate before a foreign scratch writer.
func (t *Tree) GetEntryWithOwnedScratch(key, keyScratch, leafScratch []byte, scratch *OwnedPointScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (entry node.LeafEntry, err error) {
	if scratch == nil {
		return t.GetEntryWithFixedScratch(key, keyScratch, leafScratch, readLeaf)
	}
	// Preserve a borrowed reconstructed key before SearchLeaf, resetting a node,
	// or the ordinary fallback can overwrite its reconstruction backing.
	if len(key) <= len(scratch.target) {
		copy(scratch.target[:], key)
		key = scratch.target[:len(key)]
	} else {
		scratch.Invalidate()
		return t.GetEntryWithFixedScratch(key, keyScratch, leafScratch, readLeaf)
	}
	if cap(keyScratch) < page.PageSize || cap(leafScratch) < page.PageSize {
		scratch.Invalidate()
		return node.LeafEntry{}, node.ErrCorruptedNode
	}
	// An unusual non-comparable reader cannot establish a reusable identity.
	// It still takes the maintained fixed traversal, without retaining a view.
	comparable := t.slabReader == nil || reflect.TypeOf(t.slabReader).Comparable()
	verify := t.pager != nil && t.pager.VerifyOnRead()
	if !comparable || verify {
		scratch.Invalidate()
		return t.GetEntryWithFixedScratch(key, keyScratch, leafScratch, readLeaf)
	}
	keyBase, leafBase := &keyScratch[:cap(keyScratch)][0], &leafScratch[:cap(leafScratch)][0]
	if scratch.pager != t.pager || scratch.reader != t.slabReader ||
		scratch.root != t.rootPageID || scratch.limit != t.pageLimit || scratch.namespace != t.pageNamespace || scratch.local != t.primaryLocal ||
		scratch.keyBase != keyBase || scratch.leafBase != leafBase || scratch.keyCapacity != cap(keyScratch) || scratch.leafCapacity != cap(leafScratch) {
		scratch.Invalidate()
	}
	entry, err = t.getEntryOwned(key, keyScratch, leafScratch, readLeaf, true, scratch)
	if err != nil {
		scratch.Invalidate()
	} else {
		// readLeaf may invalidate this header before overwriting its buffer.
		scratch.pager, scratch.reader = t.pager, t.slabReader
		scratch.root, scratch.limit, scratch.namespace, scratch.local = t.rootPageID, t.pageLimit, t.pageNamespace, t.primaryLocal
		scratch.keyBase, scratch.leafBase = keyBase, leafBase
		scratch.keyCapacity, scratch.leafCapacity = cap(keyScratch), cap(leafScratch)
	}
	return entry, err
}
