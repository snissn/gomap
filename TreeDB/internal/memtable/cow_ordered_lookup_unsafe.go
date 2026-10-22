//go:build !treedb_safe

package memtable

import btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"

// These pivots are borrowed only for the duration of the map operation. The
// map owns every stored prefix and never retains a lookup or deletion pivot.
func cowOrderedPrimaryGet(tree *btree.Map[string, *cowOrderedHistory], key []byte) (*cowOrderedHistory, bool) {
	return tree.Get(bytesToStringNoCopy(key))
}

func cowOrderedPrimaryAscend(tree *btree.Map[string, *cowOrderedHistory], key []byte, iter func(string, *cowOrderedHistory) bool) {
	tree.Ascend(bytesToStringNoCopy(key), iter)
}

func cowOrderedPrimaryDescend(tree *btree.Map[string, *cowOrderedHistory], key []byte, iter func(string, *cowOrderedHistory) bool) {
	tree.Descend(bytesToStringNoCopy(key), iter)
}

func cowOrderedPrimaryDelete(tree *btree.Map[string, *cowOrderedHistory], key []byte) {
	tree.Delete(bytesToStringNoCopy(key))
}

// Seek does not retain its pivot. Rebinding clears its old endpoint flags while
// preserving the stack backing already admitted for this immutable cursor.
func cowOrderedPrimaryCursorFloor(cursor *btree.MapIter[string, *cowOrderedHistory], tree *btree.Map[string, *cowOrderedHistory], key []byte) bool {
	cursor.ResetTree(tree)
	if !cursor.Seek(bytesToStringNoCopy(key)) {
		return cursor.Last()
	}
	if cowKeyCompare(cursor.Key(), key) > 0 {
		return cursor.Prev()
	}
	return true
}
