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

// The rank lookup supplies an existing owned pivot, avoiding temporary string
// allocation for arbitrary long byte bounds. The resulting iterator keeps the
// original admitted stack and is positioned once for subsequent movement.
func cowOrderedPrimaryCursorFloor(cursor *btree.MapIter[string, *cowOrderedHistory], tree *btree.Map[string, *cowOrderedHistory], key []byte) bool {
	cursor.ResetTree(tree)
	owned, _, found := cowOrderedPrimaryLowerBound(tree, key)
	if !found {
		return cursor.Last()
	}
	if !cursor.Seek(owned) {
		return false
	}
	if cowKeyCompare(owned, key) > 0 {
		return cursor.Prev()
	}
	return true
}
