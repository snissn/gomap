//go:build !treedb_safe
// +build !treedb_safe

package caching

import "unsafe"

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
