package memtable

import (
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
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

func TestAppendOnlyResetWithCapacityHardForReuse_KeepsBoundedWarmReuseCapacity(t *testing.T) {
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

	mt.ResetWithCapacityHardForReuse(capacityBytes, estimatedBytesPerEntry)

	if got := cap(mt.entries); got != warmCap {
		t.Fatalf("reuse hard reset cap(entries)=%d want retained warm cap %d", got, warmCap)
	}
	if got, max := cap(mt.entries), base*appendOnlyReuseOversizeFactor; got > max {
		t.Fatalf("reuse hard reset retained cap(entries)=%d want<=%d", got, max)
	}
}

func BenchmarkAppendOnlyResetWithCapacityHard_WarmReuseCapacity(b *testing.B) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	base := appendOnlyInitialEntriesForCapacity(capacityBytes, estimatedBytesPerEntry)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
		for j := 0; j < base*2; j++ {
			mt.SetEntrySteal([]byte(fmt.Sprintf("k-%06d", j)), nil, page.ValuePtr{}, 0)
		}
		mt.ResetWithCapacity(capacityBytes, estimatedBytesPerEntry)
		DropAppendOnlyEntryPools()
		b.StartTimer()
		mt.ResetWithCapacityHardForReuse(capacityBytes, estimatedBytesPerEntry)
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
