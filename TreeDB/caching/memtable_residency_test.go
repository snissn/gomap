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
}
