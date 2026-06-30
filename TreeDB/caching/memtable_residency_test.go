package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestSummarizeMemtableResidency_ByMode(t *testing.T) {
	makeTable := func(t *testing.T, mode memtable.Mode, key string) memtable.Table {
		t.Helper()
		mt, err := memtable.NewWithCapacityMode(1<<20, mode)
		if err != nil {
			t.Fatalf("NewWithCapacityMode(%s): %v", mode, err)
		}
		mt.Set([]byte(key), []byte("value-"+key))
		return mt
	}

	tables := []memtable.Table{
		makeTable(t, memtable.ModeSkiplist, "skip"),
		makeTable(t, memtable.ModeHashSorted, "hash"),
		makeTable(t, memtable.ModeBTree, "btree"),
		makeTable(t, memtable.ModeAppendOnly, "append"),
	}

	got := summarizeMemtableResidency(tables)

	if got.total.count != 4 {
		t.Fatalf("total.count=%d want 4", got.total.count)
	}
	if got.total.len != 4 {
		t.Fatalf("total.entries=%d want 4", got.total.len)
	}
	if got.total.size <= 0 {
		t.Fatalf("total.size=%d want >0", got.total.size)
	}
	if got.total.entryCapacity <= 0 {
		t.Fatalf("total.entryCapacity=%d want >0 from append-only table", got.total.entryCapacity)
	}
	if got.total.entryBackingBytes <= 0 {
		t.Fatalf("total.entryBackingBytes=%d want >0 from append-only table", got.total.entryBackingBytes)
	}
	if got.total.valueArenaActiveChunks <= 0 {
		t.Fatalf("total.valueArenaActiveChunks=%d want >0 from append-only table", got.total.valueArenaActiveChunks)
	}
	if got.total.valueArenaActiveBytes <= 0 {
		t.Fatalf("total.valueArenaActiveBytes=%d want >0 from append-only table", got.total.valueArenaActiveBytes)
	}

	checkMode := func(name string, s memtableResidencySummary) {
		t.Helper()
		if s.count != 1 {
			t.Fatalf("%s.count=%d want 1", name, s.count)
		}
		if s.len != 1 {
			t.Fatalf("%s.entries=%d want 1", name, s.len)
		}
		if s.size <= 0 {
			t.Fatalf("%s.size=%d want >0", name, s.size)
		}
	}

	checkMode("skiplist", got.skiplist)
	checkMode("hash_sorted", got.hashSorted)
	checkMode("btree", got.btree)
	checkMode("append_only", got.appendOnly)
	if got.skiplist.entryCapacity != 0 || got.hashSorted.entryCapacity != 0 || got.btree.entryCapacity != 0 {
		t.Fatalf("non-append-only entry capacities: skip=%d hash=%d btree=%d want all zero", got.skiplist.entryCapacity, got.hashSorted.entryCapacity, got.btree.entryCapacity)
	}
	if got.skiplist.valueArenaActiveBytes != 0 || got.hashSorted.valueArenaActiveBytes != 0 || got.btree.valueArenaActiveBytes != 0 {
		t.Fatalf("non-append-only value arena active bytes: skip=%d hash=%d btree=%d want all zero", got.skiplist.valueArenaActiveBytes, got.hashSorted.valueArenaActiveBytes, got.btree.valueArenaActiveBytes)
	}
	if got.appendOnly.entryCapacity <= 0 {
		t.Fatalf("append_only.entryCapacity=%d want >0", got.appendOnly.entryCapacity)
	}
	if got.appendOnly.entryBackingBytes <= 0 {
		t.Fatalf("append_only.entryBackingBytes=%d want >0", got.appendOnly.entryBackingBytes)
	}
	if got.appendOnly.valueArenaActiveChunks <= 0 {
		t.Fatalf("append_only.valueArenaActiveChunks=%d want >0", got.appendOnly.valueArenaActiveChunks)
	}
	if got.appendOnly.valueArenaActiveBytes <= 0 {
		t.Fatalf("append_only.valueArenaActiveBytes=%d want >0", got.appendOnly.valueArenaActiveBytes)
	}
	if got.total.entryCapacity != got.appendOnly.entryCapacity {
		t.Fatalf("total.entryCapacity=%d want append-only %d", got.total.entryCapacity, got.appendOnly.entryCapacity)
	}
	if got.total.entryBackingBytes != got.appendOnly.entryBackingBytes {
		t.Fatalf("total.entryBackingBytes=%d want append-only %d", got.total.entryBackingBytes, got.appendOnly.entryBackingBytes)
	}
	if got.total.valueArenaActiveBytes != got.appendOnly.valueArenaActiveBytes {
		t.Fatalf("total.valueArenaActiveBytes=%d want append-only %d", got.total.valueArenaActiveBytes, got.appendOnly.valueArenaActiveBytes)
	}
}
