//go:build linux

package pager

// Linux fsync flushes all modified in-core file data, including dirty pages in
// the page cache backing a writable mapping.
func mappedRangeSyncRequired() bool {
	return false
}
