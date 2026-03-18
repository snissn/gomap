package caching

import "testing"

func TestAppendOnlyEntryHint_AdaptiveDecayAndCapacityClamp(t *testing.T) {
	var cache DB

	baseCapacity := 128 << 20
	if got := cache.appendOnlyMemtableCapacityHint(baseCapacity, appendOnlyEstimatedBytesPerEntryDefault); got != baseCapacity {
		t.Fatalf("initial capacity hint=%d want %d", got, baseCapacity)
	}

	cache.observeAppendOnlyMutableEntries(appendOnlyEntryHintMaxEntries)
	high := int(cache.appendOnlyEntryHint.Load())
	if high != appendOnlyEntryHintMaxEntries {
		t.Fatalf("high hint=%d want %d", high, appendOnlyEntryHintMaxEntries)
	}

	for i := 0; i < 24; i++ {
		cache.observeAppendOnlyMutableEntries(appendOnlyEntryHintMinEntries)
	}
	low := int(cache.appendOnlyEntryHint.Load())
	if low >= high {
		t.Fatalf("hint did not decay: high=%d low=%d", high, low)
	}
	if low < appendOnlyEntryHintMinEntries {
		t.Fatalf("hint dropped below min: got=%d min=%d", low, appendOnlyEntryHintMinEntries)
	}

	hinted := cache.appendOnlyMemtableCapacityHint(baseCapacity, appendOnlyEstimatedBytesPerEntryDefault)
	if hinted >= baseCapacity {
		t.Fatalf("hinted capacity did not shrink: got=%d base=%d", hinted, baseCapacity)
	}
	if hinted < minMemtablePrealloc {
		t.Fatalf("hinted capacity=%d want >=%d", hinted, minMemtablePrealloc)
	}

	cache.appendOnlyEntryHint.Store(appendOnlyEntryHintMaxEntries)
	if got := cache.appendOnlyMemtableCapacityHint(4<<20, appendOnlyEstimatedBytesPerEntryDefault); got != 4<<20 {
		t.Fatalf("hinted capacity exceeded caller cap: got=%d want=%d", got, 4<<20)
	}
}

func TestAppendOnlyEntriesToCapacity_OverflowSafe(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	if got := appendOnlyEntriesToCapacity(maxInt, 2); got != maxInt {
		t.Fatalf("overflow clamp=%d want=%d", got, maxInt)
	}
	if got := appendOnlyEntriesToCapacity(0, appendOnlyEstimatedBytesPerEntryDefault); got != 0 {
		t.Fatalf("zero entries capacity=%d want 0", got)
	}
}
