package caching

import "testing"

func TestAppendOnlyMutableSeedCapacityUsesObservedHint(t *testing.T) {
	db := &DB{}
	db.observeAppendOnlyMutableEntries(1024)

	got := db.appendOnlyMutableSeedCapacity(64 << 20)
	wantEntries := clampAppendOnlyEntryHint((1024*appendOnlyEntryHintHeadroomMul + (appendOnlyEntryHintHeadroomDiv - 1)) / appendOnlyEntryHintHeadroomDiv)
	want := appendOnlyCapacityForEntries(wantEntries)
	if got != want {
		t.Fatalf("seed capacity=%d want=%d", got, want)
	}
}

func TestAppendOnlyMutableSeedCapacityRespectsConfiguredCapacity(t *testing.T) {
	db := &DB{}
	db.observeAppendOnlyMutableEntries(appendOnlyEntryHintMax)

	capacity := appendOnlyCapacityForEntries(256)
	got := db.appendOnlyMutableSeedCapacity(capacity)
	if got != capacity {
		t.Fatalf("seed capacity=%d want configured capacity=%d", got, capacity)
	}
}

func TestObserveAppendOnlyMutableEntriesDecaysAfterSmallRuns(t *testing.T) {
	db := &DB{}
	db.observeAppendOnlyMutableEntries(16384)
	for i := 0; i < 16; i++ {
		db.observeAppendOnlyMutableEntries(512)
	}
	got := int(db.appendOnlyEntryHint.Load())
	if got >= 4096 {
		t.Fatalf("appendOnlyEntryHint=%d want < 4096 after decay", got)
	}
	if got < appendOnlyEntryHintMin {
		t.Fatalf("appendOnlyEntryHint=%d want >= %d", got, appendOnlyEntryHintMin)
	}
}
