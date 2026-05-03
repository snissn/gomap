package batch

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestBatchReleaseClearsPointers(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Set(k2): %v", err)
	}

	entries := b.entries
	if len(entries) == 0 {
		t.Fatalf("expected entries to be populated")
	}

	Release(b)

	if b.reader != nil {
		t.Fatalf("expected reader to be cleared")
	}
	if b.inlineThreshold != 0 {
		t.Fatalf("expected inline threshold to be reset")
	}
	if !b.closed {
		t.Fatalf("expected batch to be marked closed")
	}
	for i, entry := range entries {
		if entry.Key != nil || entry.Value != nil || entry.IsPtr || entry.ValuePtr != (page.ValuePtr{}) {
			t.Fatalf("entry %d not cleared: %+v", i, entry)
		}
	}
}

func TestBatchReleaseDropsOversizedEntriesWithoutClearing(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	key := []byte("large-key")
	value := []byte("large-value")
	b.entries = make([]Entry, 1, maxBatchPoolCap+1)
	b.entries[0] = Entry{Type: OpPut, Key: key, Value: value}
	entries := b.entries

	Release(b)

	if b.entries != nil {
		t.Fatalf("oversized entries retained with cap=%d", cap(b.entries))
	}
	if len(entries) != 1 || string(entries[0].Key) != string(key) || string(entries[0].Value) != string(value) {
		t.Fatalf("oversized discarded entries were cleared before drop: %+v", entries)
	}
}

func TestBatchReleaseRetainsDefaultMaxSizedEntries(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	key := []byte("flush-sized-key")
	value := []byte("flush-sized-value")
	b.entries = make([]Entry, 1, maxBatchPoolCap)
	b.entries[0] = Entry{Type: OpPut, Key: key, Value: value}
	entries := b.entries

	Release(b)

	if b.entries == nil {
		t.Fatalf("max-sized entries were dropped")
	}
	if got := cap(b.entries); got != maxBatchPoolCap {
		t.Fatalf("retained entries cap=%d want %d", got, maxBatchPoolCap)
	}
	if len(entries) != 1 {
		t.Fatalf("captured entries len=%d want 1", len(entries))
	}
	if entries[0].Key != nil || entries[0].Value != nil || entries[0].IsPtr || entries[0].ValuePtr != (page.ValuePtr{}) {
		t.Fatalf("retained entries were not cleared: %+v", entries[0])
	}
}

func TestBatchReleaseDropsLargeEntryBatchFromDefaultPool(t *testing.T) {
	b := New(newMapValueReader(), page.DefaultInlineThreshold)
	b.entries = make([]Entry, 1, maxLargeEntryBatchPoolCap)

	Release(b)

	if b.entries != nil {
		t.Fatalf("default pool retained large entries with cap=%d", cap(b.entries))
	}
}

func TestLargeEntryBatchReleaseRetainsMaxSizedEntries(t *testing.T) {
	b := NewRetainingLargeEntries(newMapValueReader(), page.DefaultInlineThreshold)
	key := []byte("large-flush-sized-key")
	value := []byte("large-flush-sized-value")
	b.entries = make([]Entry, 1, maxLargeEntryBatchPoolCap)
	b.entries[0] = Entry{Type: OpPut, Key: key, Value: value}
	entries := b.entries

	Release(b)

	if b.entries == nil {
		t.Fatalf("large-entry pool dropped max-sized entries")
	}
	if got := cap(b.entries); got != maxLargeEntryBatchPoolCap {
		t.Fatalf("retained large entries cap=%d want %d", got, maxLargeEntryBatchPoolCap)
	}
	if len(entries) != 1 {
		t.Fatalf("captured entries len=%d want 1", len(entries))
	}
	if entries[0].Key != nil || entries[0].Value != nil || entries[0].IsPtr || entries[0].ValuePtr != (page.ValuePtr{}) {
		t.Fatalf("retained large entries were not cleared: %+v", entries[0])
	}
}
