package pager

const (
	maxDurableRangeWrites = 32
	maxDurableRangeBytes  = 256 << 10
)

var syncPageRangesFn = syncPageRangesData

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
