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

func TestAppendOnlyResetWithCapacityHard_DropsSideBuffers(t *testing.T) {
	const (
		capacityBytes          = 4 << 10
		estimatedBytesPerEntry = 96
	)

	mt := NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, estimatedBytesPerEntry)
	for i := 0; i < appendOnlyResetDropThresholdEntries; i++ {
		key := []byte(fmt.Sprintf("long-key-%08d", i))
		mt.Set(key, []byte("value"))
	}
	if cap(mt.keys) == 0 {
		t.Fatalf("test did not populate key side buffer")
	}
	if cap(mt.payloads) == 0 {
		t.Fatalf("test did not populate payload side buffer")
	}

	mt.ResetWithCapacityHard(capacityBytes, estimatedBytesPerEntry)
	if got := cap(mt.keys); got != 0 {
		t.Fatalf("hard reset cap(keys)=%d want=0", got)
	}
	if got := cap(mt.payloads); got != 0 {
		t.Fatalf("hard reset cap(payloads)=%d want=0", got)
	}
}
