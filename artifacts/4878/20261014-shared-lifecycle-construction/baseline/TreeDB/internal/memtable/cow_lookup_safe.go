//go:build treedb_safe

package memtable

import "github.com/tidwall/btree"

// Tidwall has no heterogeneous byte/string lookup. Rank binary search uses
// read-only GetAt and existing owned strings without allocating a conversion.
// Its cost is O(log N * height), with the dependency's fixed node degree.
func cowLookupGE(tree *btree.Map[string, cowValue], key []byte) (string, cowValue, bool) {
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
func cowKeyCompare(owned string, key []byte) int {
	n := len(owned)
	if len(key) < n {
		n = len(key)
	}
	for i := 0; i < n; i++ {
		if owned[i] < key[i] {
			return -1
		}
		if owned[i] > key[i] {
			return 1
		}
	}
	if len(owned) < len(key) {
		return -1
	}
	if len(owned) > len(key) {
		return 1
	}
	return 0
}
func cowHasKey(tree *btree.Map[string, cowValue], key []byte) bool {
	owned, _, ok := cowLookupGE(tree, key)
	return ok && cowKeyCompare(owned, key) == 0
}
func cowIteratorSeek(tree *btree.Map[string, cowValue], iter *btree.MapIter[string, cowValue], key []byte) bool {
	owned, _, ok := cowLookupGE(tree, key)
	return ok && iter.Seek(owned)
}

func cowDeleteKey(tree *btree.Map[string, cowValue], key []byte) {
	owned, _, ok := cowLookupGE(tree, key)
	if ok && cowKeyCompare(owned, key) == 0 {
		tree.Delete(owned)
	}
}
