package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestAppendUniqueMemtableDeduplicates(t *testing.T) {
	mt1 := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(1024, 64)
	mt2 := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(1024, 64)

	var mems []memtable.Table
	mems = appendUniqueMemtable(mems, nil)
	mems = appendUniqueMemtable(mems, mt1)
	mems = appendUniqueMemtable(mems, mt1)
	mems = appendUniqueMemtable(mems, mt2)
	mems = appendUniqueMemtable(mems, mt1)

	if len(mems) != 2 {
		t.Fatalf("len(mems)=%d want 2", len(mems))
	}
	if mems[0] != mt1 || mems[1] != mt2 {
		t.Fatalf("memtable order/dedup mismatch: got %#v", mems)
	}
}

func TestBatchCloseReleasesAuxiliaryIndexSlices(t *testing.T) {
	db := &DB{}
	b := &Batch{
		db:           db,
		entries:      db.getBatchEntries(4),
		shardIdxs:    make([]int, 2, 4),
		eligibleIdxs: make([]int, 1, 4),
		shardAdds:    make([]int64, 2, 4),
		shardCnts:    make([]int, 2, 4),
		shardEntries: [][]batch.Entry{make([]batch.Entry, 0, 2)},
		shardIdxSets: [][]int{make([]int, 0, 2)},
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if b.entries != nil || b.shardIdxs != nil || b.eligibleIdxs != nil || b.shardAdds != nil || b.shardCnts != nil || b.shardEntries != nil || b.shardIdxSets != nil {
		t.Fatalf("Close retained auxiliary slices: entries=%v shardIdxs=%v eligible=%v shardAdds=%v shardCnts=%v shardEntries=%v shardIdxSets=%v",
			b.entries, b.shardIdxs, b.eligibleIdxs, b.shardAdds, b.shardCnts, b.shardEntries, b.shardIdxSets)
	}

	// The returned pools must still hand out correctly sized, owned scratch slices.
	idxs := db.getBatchIntSlice(4)
	if cap(idxs) < 4 {
		t.Fatalf("getBatchIntSlice cap=%d want >= 4", cap(idxs))
	}
	db.putBatchIntSlice(idxs)
	adds := db.getBatchInt64Slice(4)
	if cap(adds) < 4 {
		t.Fatalf("getBatchInt64Slice cap=%d want >= 4", cap(adds))
	}
	db.putBatchInt64Slice(adds)
}
