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
