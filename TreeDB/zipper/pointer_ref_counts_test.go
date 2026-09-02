package zipper

import "testing"

func TestPointerRefCountsInlinePromotionAndMerge(t *testing.T) {
	var left PointerRefCounts
	for i := uint32(1); i <= pointerRefCountsInlineCapacity+2; i++ {
		left.add(i, uint64(i))
	}
	if left.counts == nil {
		t.Fatal("counts did not promote after inline capacity")
	}

	var right PointerRefCounts
	right.add(1, 10)
	right.add(100, 20)
	left.merge(&right)
	if got, want := left.Count(1), uint64(11); got != want {
		t.Fatalf("merged count for file 1=%d want %d", got, want)
	}
	if got, want := left.Count(100), uint64(20); got != want {
		t.Fatalf("merged count for file 100=%d want %d", got, want)
	}
}
