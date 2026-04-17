package memtable

import (
	"fmt"
	"testing"
)

func TestHashSortedResetWithCapacity_ShrinksEntriesAfterSpike(t *testing.T) {
	const capacityBytes = 4 << 10

	mt := NewHashSortedWithCapacityAndIndexer(capacityBytes, nil)
	base := hashSortedInitialEntries(capacityBytes)
	if base <= 0 {
		t.Fatalf("expected base entry count > 0, got %d", base)
	}

	for i := 0; i < base*12; i++ {
		mt.Set([]byte(fmt.Sprintf("k%08d", i)), []byte("value"))
	}
	if got := cap(mt.entries); got <= hashSortedMaxReuseEntries(base) {
		t.Fatalf("test setup did not grow enough: cap(entries)=%d want>%d", got, hashSortedMaxReuseEntries(base))
	}

	mt.ResetWithCapacity(capacityBytes)

	if got := cap(mt.entries); got != base {
		t.Fatalf("cap(entries)=%d want=%d", got, base)
	}
	if got := len(mt.entries); got != 0 {
		t.Fatalf("len(entries)=%d want=0", got)
	}
	if got := len(mt.items); got != 0 {
		t.Fatalf("len(items)=%d want=0", got)
	}
}

func TestHashSortedResetWithCapacity_RetainsEntriesWithinBound(t *testing.T) {
	const capacityBytes = 4 << 10

	mt := NewHashSortedWithCapacityAndIndexer(capacityBytes, nil)
	base := hashSortedInitialEntries(capacityBytes)
	if base <= 0 {
		t.Fatalf("expected base entry count > 0, got %d", base)
	}

	steadyEntries := base * 2
	for i := 0; i < steadyEntries; i++ {
		mt.Set([]byte(fmt.Sprintf("k%08d", i)), []byte("value"))
	}
	capBeforeReset := cap(mt.entries)
	if capBeforeReset < steadyEntries {
		t.Fatalf("test setup did not grow enough: cap(entries)=%d want>=%d", capBeforeReset, steadyEntries)
	}
	if capBeforeReset > hashSortedMaxReuseEntries(base) {
		t.Fatalf("test setup grew beyond reuse bound: cap(entries)=%d want<=%d", capBeforeReset, hashSortedMaxReuseEntries(base))
	}

	mt.ResetWithCapacity(capacityBytes)

	if got := cap(mt.entries); got != capBeforeReset {
		t.Fatalf("cap(entries)=%d want=%d", got, capBeforeReset)
	}

	for i := 0; i < steadyEntries; i++ {
		mt.Set([]byte(fmt.Sprintf("n%08d", i)), []byte("value"))
	}
	if got := cap(mt.entries); got != capBeforeReset {
		t.Fatalf("entries regrew after warm reset: cap(entries)=%d want=%d", got, capBeforeReset)
	}
}

func TestHashSortedResetWithCapacity_DropsOversizedFirstArenaChunk(t *testing.T) {
	const capacityBytes = 4 << 10

	mt := NewHashSortedWithCapacityAndIndexer(capacityBytes, nil)
	buf := mt.arena.alloc(hashSortedArenaRetainMaxCap * 2)
	if cap(buf) <= hashSortedArenaRetainCap(capacityBytes) {
		t.Fatalf("test setup did not allocate oversized first chunk: cap=%d want>%d", cap(buf), hashSortedArenaRetainCap(capacityBytes))
	}
	if len(mt.arena.chunks) != 1 {
		t.Fatalf("expected one arena chunk, got %d", len(mt.arena.chunks))
	}

	mt.ResetWithCapacity(capacityBytes)

	if len(mt.arena.chunks) != 0 {
		t.Fatalf("retained oversized first chunk after reset: chunks=%d", len(mt.arena.chunks))
	}
	if mt.arena.cur != nil || mt.arena.off != 0 || mt.arena.nextCap != 0 {
		t.Fatalf("expected arena fully dropped after oversized reset: cur=%v off=%d nextCap=%d", mt.arena.cur != nil, mt.arena.off, mt.arena.nextCap)
	}
}
