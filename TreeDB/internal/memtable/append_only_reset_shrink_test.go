package memtable

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestAppendOnlyResetWithCapacity_ShrinksEntriesAfterSpike(t *testing.T) {
	const (
		capacityBytes           = 4 << 10
		estimatedBytesPerEntry  = 96
		desiredEntriesMultiple  = 10
		shrinkThresholdMultiple = 4
	)

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	if base <= 0 {
		t.Fatalf("expected base entry count > 0, got %d", base)
	}

	for i := 0; i < base*desiredEntriesMultiple; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	if got := cap(mt.entries); got <= base*shrinkThresholdMultiple {
		t.Fatalf("test setup did not grow entries enough: cap(entries)=%d want>%d", got, base*shrinkThresholdMultiple)
	}

	mt.ResetWithCapacity(capacityBytes, estimatedBytesPerEntry)
	if got := cap(mt.entries); got > base*shrinkThresholdMultiple {
		t.Fatalf("entries not shrunk after reset: cap(entries)=%d want<=%d", got, base*shrinkThresholdMultiple)
	}
	if mt.count != 0 {
		t.Fatalf("expected count=0 after reset, got %d", mt.count)
	}
}

func TestAppendOnlyResetWithCapacity_RetainsObservedEntriesWithinBound(t *testing.T) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
		steadyEntriesMultiple  = 2
	)

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	if base <= 0 {
		t.Fatalf("expected base entry count > 0, got %d", base)
	}
	steadyEntries := base * steadyEntriesMultiple

	for i := 0; i < steadyEntries; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	capBeforeReset := cap(mt.entries)
	if capBeforeReset < steadyEntries {
		t.Fatalf("test setup did not grow entries enough: cap(entries)=%d want>=%d", capBeforeReset, steadyEntries)
	}

	mt.ResetWithCapacity(capacityBytes, estimatedBytesPerEntry)
	if got := len(mt.entries); got < steadyEntries {
		t.Fatalf("retained len(entries)=%d want>=%d", got, steadyEntries)
	}
	if got := cap(mt.entries); got != capBeforeReset {
		t.Fatalf("retained cap(entries)=%d want=%d", got, capBeforeReset)
	}

	for i := 0; i < steadyEntries; i++ {
		key := []byte(fmt.Sprintf("n%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	if got := cap(mt.entries); got != capBeforeReset {
		t.Fatalf("entries regrew after warm reset: cap(entries)=%d want=%d", got, capBeforeReset)
	}
}

func TestAppendOnlyNewWithCapacityAndEntryHint_DefersHintGrowthUntilAppend(t *testing.T) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	entryHint := base * 16
	mt := NewAppendOnlyWithCapacityEstimatedEntryBytesAndHint(capacityBytes, estimatedBytesPerEntry, entryHint)
	if got := len(mt.entries); got != base {
		t.Fatalf("initial len(entries)=%d want=%d", got, base)
	}
	if got := cap(mt.entries); got >= entryHint {
		t.Fatalf("initial cap(entries)=%d want <%d before append-driven growth", got, entryHint)
	}
	if got := mt.growEntriesLen; got < entryHint {
		t.Fatalf("growEntriesLen=%d want >=%d", got, entryHint)
	}

	for i := 0; i <= base; i++ {
		key := []byte(fmt.Sprintf("h%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	if got := cap(mt.entries); got < entryHint {
		t.Fatalf("cap(entries) after first growth=%d want >=%d", got, entryHint)
	}
}

func TestAppendOnlyResetWithCapacityAndEntryHint_DefersHintGrowthUntilAppend(t *testing.T) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	entryHint := base * 16
	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	if got := len(mt.entries); got != base {
		t.Fatalf("initial len(entries)=%d want=%d", got, base)
	}
	if got := cap(mt.entries); got >= entryHint {
		t.Fatalf("initial cap(entries)=%d want <%d before append-driven growth", got, entryHint)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		mt.ResetWithCapacityAndEntryHint(capacityBytes, estimatedBytesPerEntry, entryHint)
	})
	if allocs > 0 {
		t.Fatalf("reset allocs/run=%f want 0", allocs)
	}
	if got := len(mt.entries); got != base {
		t.Fatalf("reset len(entries)=%d want=%d", got, base)
	}
	if got := cap(mt.entries); got >= entryHint {
		t.Fatalf("reset cap(entries)=%d want <%d before append-driven growth", got, entryHint)
	}
	if got := mt.growEntriesLen; got < entryHint {
		t.Fatalf("growEntriesLen=%d want >=%d", got, entryHint)
	}

	for i := 0; i < entryHint; i++ {
		key := []byte(fmt.Sprintf("r%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	capAfterGrowth := cap(mt.entries)
	if capAfterGrowth < entryHint {
		t.Fatalf("grown cap(entries)=%d want >=%d", capAfterGrowth, entryHint)
	}

	mt.ResetWithCapacityAndEntryHint(capacityBytes, estimatedBytesPerEntry, entryHint)
	if got := cap(mt.entries); got != capAfterGrowth {
		t.Fatalf("warm hinted cap(entries)=%d want=%d", got, capAfterGrowth)
	}
}
