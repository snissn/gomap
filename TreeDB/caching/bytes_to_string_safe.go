//go:build treedb_safe
// +build treedb_safe

package caching

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b)
}
