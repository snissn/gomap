//go:build treedb_safe

package memtable

import btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"

// The safe build uses existing owned strings as map pivots. Rank searches avoid
// allocating a temporary string for arbitrarily long external byte bounds.
// This matches the raw tree's safe lookup bound: O(log N * height).
func cowOrderedPrimaryLowerBound(tree *btree.Map[string, *cowOrderedHistory], key []byte) (string, *cowOrderedHistory, bool) {
	lo, hi := 0, tree.Len()
	for lo < hi {
		mid := lo + (hi-lo)/2
		owned, _, _ := tree.GetAt(mid)
		if cowKeyCompare(owned, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return tree.GetAt(lo)
}

func cowOrderedPrimaryGet(tree *btree.Map[string, *cowOrderedHistory], key []byte) (*cowOrderedHistory, bool) {
	owned, history, ok := cowOrderedPrimaryLowerBound(tree, key)
	if !ok || cowKeyCompare(owned, key) != 0 {
		return nil, false
	}
	return history, true
}

func cowOrderedPrimaryAscend(tree *btree.Map[string, *cowOrderedHistory], key []byte, iter func(string, *cowOrderedHistory) bool) {
	owned, _, ok := cowOrderedPrimaryLowerBound(tree, key)
	if ok {
		tree.Ascend(owned, iter)
	}
}

func cowOrderedPrimaryDescend(tree *btree.Map[string, *cowOrderedHistory], key []byte, iter func(string, *cowOrderedHistory) bool) {
	lo, hi := 0, tree.Len()
	for lo < hi {
		mid := lo + (hi-lo)/2
		owned, _, _ := tree.GetAt(mid)
		if cowKeyCompare(owned, key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if owned, _, ok := tree.GetAt(lo - 1); ok {
		tree.Descend(owned, iter)
	}
}

func cowOrderedPrimaryDelete(tree *btree.Map[string, *cowOrderedHistory], key []byte) {
	owned, _, ok := cowOrderedPrimaryLowerBound(tree, key)
	if ok && cowKeyCompare(owned, key) == 0 {
		tree.Delete(owned)
	}
}
