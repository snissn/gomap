//go:build treedb_safe

package cowbtree

func compareStringBytes(s string, b []byte) int {
	n := len(s)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if s[i] != b[i] {
			if s[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if len(s) < len(b) {
		return -1
	}
	if len(s) > len(b) {
		return 1
	}
	return 0
}
