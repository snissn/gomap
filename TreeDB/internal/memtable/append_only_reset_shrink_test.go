package memtable

import (
	"fmt"
	"sync/atomic"
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

func TestAppendOnlyNewWithCapacityAndEntryHint_PreallocatesHintedEntries(t *testing.T) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	entryHint := base * 16
	mt := NewAppendOnlyWithCapacityEstimatedEntryBytesAndHint(capacityBytes, estimatedBytesPerEntry, entryHint)
	if got := len(mt.entries); got < entryHint {
		t.Fatalf("initial len(entries)=%d want>=%d", got, entryHint)
	}
	if got := cap(mt.entries); got < entryHint {
		t.Fatalf("initial cap(entries)=%d want>=%d", got, entryHint)
	}

	capBefore := cap(mt.entries)
	for i := 0; i < entryHint; i++ {
		key := []byte(fmt.Sprintf("h%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	if got := cap(mt.entries); got != capBefore {
		t.Fatalf("hinted constructor regrew entries: cap(entries)=%d want=%d", got, capBefore)
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
	if got := cap(mt.entries); got != base {
		t.Fatalf("initial cap(entries)=%d want=%d", got, base)
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
	if got := cap(mt.entries); got != base {
		t.Fatalf("reset cap(entries)=%d want=%d", got, base)
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

func TestAppendOnlyPredictiveGrowthHintRaisesFutureGrowEntriesLen(t *testing.T) {
	const (
		capacityBytes          = 128 << 10
		estimatedBytesPerEntry = 96
	)

	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	if base <= appendOnlyPredictHintMinEntries {
		t.Fatalf("base=%d want > %d", base, appendOnlyPredictHintMinEntries)
	}

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	var observed int
	mt.SetPredictiveGrowthHint(capacityBytes, nil, func(entries int) {
		if entries > observed {
			observed = entries
		}
	})

	for i := 0; i < appendOnlyPredictHintMinEntries; i++ {
		key := []byte(fmt.Sprintf("p%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}

	if got, wantMin := mt.growEntriesLen, base*8; got < wantMin {
		t.Fatalf("growEntriesLen=%d want >= %d", got, wantMin)
	}
	if observed != mt.growEntriesLen {
		t.Fatalf("observed predicted entries=%d want %d", observed, mt.growEntriesLen)
	}
	if got := cap(mt.entries); got != base {
		t.Fatalf("predictive hint should not allocate immediately: cap(entries)=%d want=%d", got, base)
	}
}

func TestAppendOnlySharedPredictiveHintRaisesGrowthFloorBeforeLocalPrediction(t *testing.T) {
	const (
		capacityBytes          = 128 << 10
		estimatedBytesPerEntry = 96
	)

	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	sharedHint := atomic.Int32{}
	sharedHint.Store(int32(base * 8))

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	mt.SetPredictiveGrowthHint(capacityBytes, &sharedHint, nil)

	for i := 0; i < base+1; i++ {
		key := []byte(fmt.Sprintf("s%08d", i))
		mt.SetEntrySteal(key, nil, page.ValuePtr{}, node.FlagTombstone)
	}
	if got := cap(mt.entries); got < int(sharedHint.Load()) {
		t.Fatalf("cap(entries)=%d want >= %d", got, sharedHint.Load())
	}
}
