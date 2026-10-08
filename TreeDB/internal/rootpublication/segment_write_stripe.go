package rootpublication

import (
	"path/filepath"
	"sync"
)

// These are the existing column segment write stripes, shared by append,
// synchronous GC and DB Close. They retain no path, owner, callback or quota.
const SegmentWriteStripeCount = 64

var segmentWriteStripes [SegmentWriteStripeCount]sync.Mutex

func SegmentWriteStripeIndex(path string) uint8 {
	path = filepath.Clean(path)
	hash := uint64(14695981039346656037)
	for i := 0; i < len(path); i++ {
		hash ^= uint64(path[i])
		hash *= 1099511628211
	}
	return uint8(hash % SegmentWriteStripeCount)
}
func SegmentWriteStripe(index uint8) *sync.Mutex {
	if int(index) >= SegmentWriteStripeCount {
		return nil
	}
	return &segmentWriteStripes[index]
}
