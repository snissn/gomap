package memtable

import (
	"fmt"
	"testing"
	"time"
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

func TestAppendOnlyResetWithCapacityLimitDefersEntryAllocation(t *testing.T) {
	const estimate = 96
	for _, entries := range []int{128, 4096} {
		t.Run(fmt.Sprint(entries), func(t *testing.T) {
			mt := NewAppendOnlyWithEntryCapacity(entries)
			mt.Set([]byte("old-long-key"), []byte("old-value"))
			mt.Set([]byte("earlier-long-key"), []byte("other-value"))
			mt.Freeze()
			before := mt.EntryCapacity()

			mt.ResetWithCapacityLimit(128*estimate, estimate)
			if got := mt.EntryCapacity(); got > 128 {
				t.Fatalf("recycled entry capacity=%d want <=128", got)
			}
			if before <= 128 && mt.EntryCapacity() != before {
				t.Fatalf("small backing lost: before=%d after=%d", before, mt.EntryCapacity())
			}
			if before > 128 && mt.EntryCapacity() != 0 {
				t.Fatalf("oversize backing replaced: capacity=%d want 0", mt.EntryCapacity())
			}
			if mt.Len() != 0 || mt.Size() != 0 || mt.frozenFast.Load() {
				t.Fatalf("reset did not clear table: len=%d size=%d frozen=%t", mt.Len(), mt.Size(), mt.frozenFast.Load())
			}
			if _, _, found := mt.Get([]byte("old-long-key")); found {
				t.Fatal("recycled table retained old key")
			}
			if got := mt.ValueArenaBackingStats(); got.ActiveBytes != 0 || got.RetainedBytes != 0 {
				t.Fatalf("recycled table retained values: %+v", got)
			}

			// Re-admission is the point that prepares the requested capacity.
			mt.ResetWithCapacity(256*estimate, estimate)
			if mt.EntryCapacity() < 256 {
				t.Fatalf("mutable backing not prepared: capacity=%d", mt.EntryCapacity())
			}
			mt.Set([]byte("new-key"), []byte("new-value"))
			if value, deleted, found := mt.Get([]byte("new-key")); !found || deleted || string(value) != "new-value" {
				t.Fatalf("reused table Get=(%q,%t,%t)", value, deleted, found)
			}
		})
	}

	mt := NewAppendOnlyWithEntryCapacity(128)
	mt.ResetDropEntries()
	mt.ResetWithCapacityLimit(4096*estimate, estimate)
	if got := mt.EntryCapacity(); got != 0 {
		t.Fatalf("cold recycle allocated entry backing: capacity=%d", got)
	}
}

func TestAppendOnlyCapacityHintAllocatesBaselineOnFirstUse(t *testing.T) {
	const estimate, baseline = 96, 512
	for _, reserve := range []int{0, 1, baseline * 3} {
		t.Run(fmt.Sprint(reserve), func(t *testing.T) {
			DropAppendOnlyEntryPools()
			t.Cleanup(DropAppendOnlyEntryPools)
			mt := NewAppendOnlyWithCapacityHint(baseline*estimate, estimate)
			if mt.EntryBackingBytes() != 0 || mt.baseEntriesLen != baseline {
				t.Fatalf("deferred table: backing=%d baseline=%d", mt.EntryBackingBytes(), mt.baseEntriesLen)
			}
			want := baseline
			if reserve > 0 {
				want, _, _ = appendOnlyReserveTargetCapacity(baseline, reserve)
				mt.ReserveAdditionalEntries(reserve)
			} else {
				mt.Set([]byte("first-key"), []byte("first-value"))
			}
			if got := len(mt.entries); got != want {
				t.Fatalf("first-use backing length=%d want=%d", got, want)
			}
			backing := &mt.entries[0]
			mt.Set([]byte("second-key"), []byte("second-value"))
			if &mt.entries[0] != backing {
				t.Fatal("write after first use grew prepared backing")
			}
			if value, deleted, found := mt.Get([]byte("second-key")); !found || deleted || string(value) != "second-value" {
				t.Fatalf("Get after first use=(%q,%t,%t)", value, deleted, found)
			}
		})
	}
	// Existing constructors retain their eager allocation contract.
	for _, mt := range []*AppendOnly{
		NewAppendOnlyWithCapacity(baseline * estimate),
		NewAppendOnlyWithCapacityEstimatedEntryBytes(baseline*estimate, estimate),
		NewAppendOnlyWithEntryCapacity(baseline),
	} {
		if mt.EntryBackingBytes() == 0 {
			t.Fatal("eager constructor deferred its backing")
		}
	}
}

func TestAppendOnlyCapacityHintEmptyAndHeldIterators(t *testing.T) {
	const estimate, baseline = 96, 512
	mt := NewAppendOnlyWithCapacityHint(baseline*estimate, estimate)
	mt.Freeze()
	for _, it := range []interface {
		Valid() bool
		Close() error
	}{mt.NewIterator(nil, nil), mt.NewReverseIterator(nil, nil)} {
		if it.Valid() {
			t.Fatal("empty deferred table has a valid iterator")
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if mt.EntryBackingBytes() != 0 {
		t.Fatal("reading or freezing an empty table allocated entry backing")
	}
	mt.ResetWithCapacityLimit(baseline*estimate, estimate)
	mt.Set([]byte("k2"), []byte("v2"))
	mt.Set([]byte("k1"), []byte("v1"))
	mt.Freeze()
	it := mt.NewIterator(nil, nil)
	defer it.Close()
	resetDone := make(chan struct{})
	go func() {
		mt.ResetWithCapacityLimit(128*estimate, estimate)
		close(resetDone)
	}()
	select {
	case <-resetDone:
		t.Fatal("reset completed while frozen iterator was held")
	case <-time.After(50 * time.Millisecond):
	}
	if !it.Valid() || string(it.Key()) != "k1" || string(it.Value()) != "v1" {
		t.Fatal("held iterator lost its frozen contents")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-resetDone:
	case <-time.After(2 * time.Second):
		t.Fatal("reset did not resume after iterator close")
	}
	if mt.EntryBackingBytes() != 0 || mt.Len() != 0 {
		t.Fatal("reset retained oversized backing or old entries")
	}
}
