//go:build !treedb_safe

package cowbtree

import (
	"cmp"
	"unsafe"
)

func compareStringBytes(s string, b []byte) int {
	return cmp.Compare(s, unsafe.String(unsafe.SliceData(b), len(b)))
}
