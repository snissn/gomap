//go:build !linux

package pager

// Windows requires FlushViewOfFile before FlushFileBuffers for mapped writes.
// Keep the explicit mapped-range fence on non-Linux Unix platforms as well.
func mappedRangeSyncRequired() bool {
	return true
}
