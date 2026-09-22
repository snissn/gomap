package memtable

import (
	"fmt"
	"testing"
)

func TestAppendOnlyResetWithCapacityHard_DropsObservedSpike(t *testing.T) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)

	for i := 0; i < base*24; i++ {
		key := []byte(fmt.Sprintf("k-%06d", i))
		mt.Set(key, []byte("value"))
	}
	if got := cap(mt.entries); got <= base*appendOnlyReuseOversizeFactor {
		t.Fatalf("test setup did not grow enough: cap(entries)=%d want>%d", got, base*appendOnlyReuseOversizeFactor)
	}

	mt.ResetWithCapacityHard(capacityBytes, estimatedBytesPerEntry)

	if got := cap(mt.entries); got != base {
		t.Fatalf("hard reset cap(entries)=%d want=%d", got, base)
	}
	if got := len(mt.valueArena.retained); got != 0 {
		t.Fatalf("hard reset retained chunks=%d want=0", got)
	}
	if got := mt.valueArena.retainedB; got != 0 {
		t.Fatalf("hard reset retained bytes=%d want=0", got)
	}
}

func TestAppendOnlyResetWithCapacityHard_ClampsWarmReuseCapacity(t *testing.T) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)

	for i := 0; i < base*2; i++ {
		key := []byte(fmt.Sprintf("k-%06d", i))
		mt.Set(key, []byte("value"))
	}
	mt.ResetWithCapacity(capacityBytes, estimatedBytesPerEntry)
	warmCap := cap(mt.entries)
	if warmCap <= base {
		t.Fatalf("test setup did not retain warm capacity: cap(entries)=%d want>%d", warmCap, base)
	}
	if warmCap > base*appendOnlyReuseOversizeFactor {
		t.Fatalf("test setup grew beyond reuse ceiling: cap(entries)=%d want<=%d", warmCap, base*appendOnlyReuseOversizeFactor)
	}

	mt.ResetWithCapacityHard(capacityBytes, estimatedBytesPerEntry)

	if got := cap(mt.entries); got != base {
		t.Fatalf("hard reset cap(entries)=%d want=%d", got, base)
	}
}

func TestAppendOnlyResetWithCapacityHard_BoundsNonClassCapacity(t *testing.T) {
	const (
		capacityBytes          = 512 << 10
		estimatedBytesPerEntry = 96
	)

	retainedEntries := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	_, classCap, ok := appendOnlyEntryPoolClassForLength(retainedEntries)
	if !ok {
		t.Fatalf("retained entry count %d should map to a pool class", retainedEntries)
	}
	if classCap <= retainedEntries {
		t.Fatalf("test setup needs rounded class cap: retained=%d class=%d", retainedEntries, classCap)
	}

	mt := NewAppendOnlyWithEntryCapacity(classCap)
	if got := cap(mt.entries); got <= retainedEntries {
		t.Fatalf("test setup cap(entries)=%d want > %d", got, retainedEntries)
	}

	mt.ResetWithCapacityHard(capacityBytes, estimatedBytesPerEntry)

	if got := cap(mt.entries); got != retainedEntries {
		t.Fatalf("hard reset cap(entries)=%d want=%d", got, retainedEntries)
	}
	if got := mt.EntryBackingBytes(); got > int64(capacityBytes) {
		t.Fatalf("hard reset backing bytes=%d want <= capacity %d", got, capacityBytes)
	}
}

func TestAppendOnlyResetWithCapacityHardPoolsDisplacedBacking(t *testing.T) {
	DropAppendOnlyEntryPools()
	t.Cleanup(DropAppendOnlyEntryPools)

	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)

	for i := 0; i < base*2; i++ {
		key := []byte(fmt.Sprintf("k-%06d", i))
		mt.Set(key, []byte("value"))
	}
	oldCap := cap(mt.entries)
	if oldCap <= base {
		t.Fatalf("test setup did not grow enough: cap(entries)=%d want>%d", oldCap, base)
	}
	if oldCap > appendOnlyEntryPoolRetainMaxCap {
		t.Fatalf("test setup cap(entries)=%d exceeds retention max=%d", oldCap, appendOnlyEntryPoolRetainMaxCap)
	}

	mt.ResetWithCapacityHard(capacityBytes, estimatedBytesPerEntry)

	if got := cap(mt.entries); got != base {
		t.Fatalf("hard reset cap(entries)=%d want=%d", got, base)
	}
	wantRetained := appendOnlyEntryPoolBytes(oldCap)
	if got := AppendOnlyEntryPoolStatsSnapshot().RetainedBytesEstimate; got != wantRetained {
		t.Fatalf("retained entry pool bytes=%d want=%d", got, wantRetained)
	}
}

func TestAppendOnlyReleaseDropEntries_DropsEntryBacking(t *testing.T) {
	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, 96)
	if got := mt.EntryCapacity(); got == 0 {
		t.Fatal("test setup produced zero entry capacity")
	}
	for i := 0; i < 1024; i++ {
		mt.Set([]byte(fmt.Sprintf("k-%06d", i)), []byte("value"))
	}

	mt.ReleaseDropEntries()

	if got := mt.EntryCapacity(); got != 0 {
		t.Fatalf("entry capacity after cold release=%d want 0", got)
	}
	if got := mt.EntryBackingBytes(); got != 0 {
		t.Fatalf("entry backing bytes after cold release=%d want 0", got)
	}
}

func TestAppendOnlyResetDropEntries_AllowsReuseOnNextWrite(t *testing.T) {
	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, 96)
	if got := mt.EntryBackingBytes(); got == 0 {
		t.Fatal("test setup produced zero entry backing")
	}

	mt.ResetDropEntries()

	if got := mt.EntryCapacity(); got != 0 {
		t.Fatalf("entry capacity after reset/drop=%d want 0", got)
	}
	if got := mt.EntryBackingBytes(); got != 0 {
		t.Fatalf("entry backing after reset/drop=%d want 0", got)
	}

	mt.Set([]byte("k"), []byte("v"))
	if got := mt.Len(); got != 1 {
		t.Fatalf("len after reuse write=%d want 1", got)
	}
	value, deleted, found := mt.Get([]byte("k"))
	if !found || deleted || string(value) != "v" {
		t.Fatalf("Get after reuse=(%q,%t,%t), want value", value, deleted, found)
	}
	if got := mt.EntryBackingBytes(); got == 0 {
		t.Fatal("entry backing after reuse write=0 want >0")
	}
}
