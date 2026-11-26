package pager

import "os"

const (
	maxDurableRangeWrites = 32
	maxDurableRangeBytes  = 256 << 10
)

// syncPageRangesFn is a test hook that overrides the default range durability
// implementation when non-nil.
var syncPageRangesFn func(file *os.File, chunks [][]byte, ranges []syncPageRange, chunkSize int64) (bool, error)

func syncPageRangesWithinDurableFileSize(ranges []syncPageRange, chunkSize, durableFileSize int64) bool {
	if chunkSize <= 0 || durableFileSize <= 0 || len(ranges) == 0 {
		return false
	}
	for _, r := range ranges {
		if r.chunk < 0 || r.start < 0 || r.end < r.start {
			return false
		}
		end := int64(r.chunk)*chunkSize + int64(r.end)
		if end > durableFileSize {
			return false
		}
	}
	return true
}

func useDurableRangeWrites(ranges []syncPageRange) bool {
	if len(ranges) == 0 || len(ranges) > maxDurableRangeWrites {
		return false
	}
	totalBytes := 0
	for _, r := range ranges {
		if r.end < r.start {
			return false
		}
		totalBytes += r.end - r.start
		if totalBytes > maxDurableRangeBytes {
			return false
		}
	}
	return true
}
