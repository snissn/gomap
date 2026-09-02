//go:build treedb_safe
// +build treedb_safe

package memtable

func bytesToStringNoCopy(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return string(b)
}

func stringToBytesNoCopy(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}
	return []byte(s)
}
