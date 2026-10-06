//go:build !treedb_safe

package memtable

import "github.com/tidwall/btree"

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
