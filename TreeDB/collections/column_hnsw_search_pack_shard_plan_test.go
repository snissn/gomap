package collections

import (
	"testing"

	vectorpartition "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// TestSearchPackVectorStrideMatchesShardPlanChargeV1 pins the invariant the
// byte-bounded planner depends on. vectorpartition cannot import collections
// (collections imports vectorpartition), so it encodes the vector-section
// alignment itself; this test is the cross-check that the encoded rule still
// matches what the writer actually materializes.
func TestSearchPackVectorStrideMatchesShardPlanChargeV1(t *testing.T) {
	for _, dimensions := range []int{1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 127, 128, 129, 512, 4096} {
		stride, err := columnHNSWSearchPackVectorStrideForDimensions(dimensions)
		if err != nil {
			t.Fatalf("dimensions=%d stride: %v", dimensions, err)
		}
		want := stride * 4
		got, ok := vectorpartition.AlignedTraversalRowBytesV1(dimensions)
		if !ok {
			t.Fatalf("dimensions=%d planner charge rejected", dimensions)
		}
		if got != want {
			t.Fatalf("dimensions=%d planner charges %d bytes, pack materializes %d", dimensions, got, want)
		}
		if got < dimensions*4 {
			t.Fatalf("dimensions=%d planner charge %d understates the unpadded width %d", dimensions, got, dimensions*4)
		}
	}
}
