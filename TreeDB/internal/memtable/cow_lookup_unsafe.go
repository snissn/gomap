//go:build !treedb_safe

package memtable

import btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"

func cowLookupGE(tree *btree.Map[string, cowValue], key []byte) (string, cowValue, bool) {
	var owned string
	var value cowValue
	found := false
	tree.Ascend(bytesToStringNoCopy(key), func(k string, v cowValue) bool {
		owned, value, found = k, v, true
		return false
	})
	return owned, value, found
}
func cowKeyCompare(owned string, key []byte) int {
	pivot := bytesToStringNoCopy(key)
	if owned < pivot {
		return -1
	}
	if owned > pivot {
		return 1
	}
	return 0
}
func cowHasKey(tree *btree.Map[string, cowValue], key []byte) bool {
	_, ok := tree.Get(bytesToStringNoCopy(key))
	return ok
}
func cowIteratorSeek(_ *btree.Map[string, cowValue], iter *btree.MapIter[string, cowValue], key []byte) bool {
	return iter.Seek(bytesToStringNoCopy(key))
}

func cowDeleteKey(tree *btree.Map[string, cowValue], key []byte) {
	tree.Delete(bytesToStringNoCopy(key))
}

// cowLookupLT borrows the immutable map and returns its strict predecessor.
func cowLookupLT(tree *btree.Map[string, cowValue], end []byte) (key string, value cowValue, found bool) {
	visit := func(k string, v cowValue) bool {
		if end != nil && cowKeyCompare(k, end) >= 0 {
			return true
		}
		key, value, found = k, v, true
		return false
	}
	if end == nil {
		tree.Reverse(visit)
	} else {
		tree.Descend(bytesToStringNoCopy(end), visit)
	}
	return
}
