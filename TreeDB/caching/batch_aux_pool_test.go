package caching

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func forceNormalPoolPressureForTest(tb testing.TB) {
	tb.Helper()
	saved, _ := poolPressureState.Load().(poolPressureSnapshot)
	poolPressureState.Store(poolPressureSnapshot{
		sampledUnixNano: poolPressureNow().UnixNano(),
		level:           poolPressureNormal,
	})
	tb.Cleanup(func() {
		poolPressureState.Store(saved)
	})
}

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

func TestBatchAuxScratchLeasesReuseOneShotSlices(t *testing.T) {
	forceNormalPoolPressureForTest(t)
	db := &DB{}

	entries := make([]batch.Entry, 1, 4)
	entriesPtr := unsafe.SliceData(entries)
	db.putBatchEntries(entries)
	gotEntries := db.getBatchEntries(4)
	gotEntries = append(gotEntries, batch.Entry{})
	if gotPtr := unsafe.SliceData(gotEntries); gotPtr != entriesPtr {
		t.Fatalf("getBatchEntries did not reuse leased backing")
	}

	shardEntries := make([]batch.Entry, 1, 4)
	shardEntriesPtr := unsafe.SliceData(shardEntries)
	db.putBatchShardEntries(shardEntries)
	gotShardEntries := db.getBatchShardEntries(4)
	gotShardEntries = append(gotShardEntries, batch.Entry{})
	if gotPtr := unsafe.SliceData(gotShardEntries); gotPtr != shardEntriesPtr {
		t.Fatalf("getBatchShardEntries did not reuse leased backing")
	}

	idxs := make([]int, 1, 4)
	idxsPtr := unsafe.SliceData(idxs)
	db.putBatchIntSlice(idxs)
	gotIdxs := db.getBatchIntSlice(4)
	gotIdxs = append(gotIdxs, 0)
	if gotPtr := unsafe.SliceData(gotIdxs); gotPtr != idxsPtr {
		t.Fatalf("getBatchIntSlice did not reuse leased backing")
	}

	adds := make([]int64, 1, 4)
	addsPtr := unsafe.SliceData(adds)
	db.putBatchInt64Slice(adds)
	gotAdds := db.getBatchInt64Slice(4)
	gotAdds = append(gotAdds, 0)
	if gotPtr := unsafe.SliceData(gotAdds); gotPtr != addsPtr {
		t.Fatalf("getBatchInt64Slice did not reuse leased backing")
	}
}

func TestBatchAuxScratchLeasesBoundRetention(t *testing.T) {
	forceNormalPoolPressureForTest(t)
	db := &DB{}

	for i := 0; i < batchAuxEntryLeaseMaxCount+16; i++ {
		db.putBatchShardEntries(make([]batch.Entry, 0, 1))
	}
	for i := 0; i < batchAuxEntryGroupLeaseMaxCount+16; i++ {
		db.putBatchShardEntryGroups(make([][]batch.Entry, 0, 1))
	}
	for i := 0; i < batchAuxIntLeaseMaxCount+16; i++ {
		db.putBatchIntSlice(make([]int, 0, 1))
		db.putBatchInt64Slice(make([]int64, 0, 1))
	}

	db.batchAuxMu.Lock()
	shardEntriesCount := len(db.batchShardEntriesLeases.slices)
	shardEntriesBytes := db.batchShardEntriesLeases.bytes
	shardEntryGroupsCount := len(db.batchShardEntryGroupsLeases.slices)
	shardEntryGroupsBytes := db.batchShardEntryGroupsLeases.bytes
	intCount := len(db.batchIntLeases.slices)
	intBytes := db.batchIntLeases.bytes
	int64Count := len(db.batchInt64Leases.slices)
	int64Bytes := db.batchInt64Leases.bytes
	db.batchAuxMu.Unlock()

	if shardEntriesCount > batchAuxEntryLeaseMaxCount || shardEntriesBytes > batchAuxEntryLeaseMaxBytes {
		t.Fatalf("batch shard entry leases count=%d bytes=%d exceed limits count=%d bytes=%d",
			shardEntriesCount, shardEntriesBytes, batchAuxEntryLeaseMaxCount, batchAuxEntryLeaseMaxBytes)
	}
	if shardEntryGroupsCount > batchAuxEntryGroupLeaseMaxCount || shardEntryGroupsBytes > batchAuxEntryGroupLeaseMaxBytes {
		t.Fatalf("batch shard entry group leases count=%d bytes=%d exceed limits count=%d bytes=%d",
			shardEntryGroupsCount, shardEntryGroupsBytes, batchAuxEntryGroupLeaseMaxCount, batchAuxEntryGroupLeaseMaxBytes)
	}
	if intCount > batchAuxIntLeaseMaxCount || intBytes > batchAuxIntLeaseMaxBytes {
		t.Fatalf("batch int leases count=%d bytes=%d exceed limits count=%d bytes=%d",
			intCount, intBytes, batchAuxIntLeaseMaxCount, batchAuxIntLeaseMaxBytes)
	}
	if int64Count > batchAuxIntLeaseMaxCount || int64Bytes > batchAuxIntLeaseMaxBytes {
		t.Fatalf("batch int64 leases count=%d bytes=%d exceed limits count=%d bytes=%d",
			int64Count, int64Bytes, batchAuxIntLeaseMaxCount, batchAuxIntLeaseMaxBytes)
	}
}

func TestBatchShardEntryGroupLeaseReuseAndClear(t *testing.T) {
	db := &DB{}

	groups := make([][]batch.Entry, 1, 4)
	groups[0] = make([]batch.Entry, 1, 2)
	groupsPtr := unsafe.SliceData(groups)

	db.putBatchShardEntryGroups(groups)
	got := db.getBatchShardEntryGroups(2)
	if cap(got) < 2 {
		t.Fatalf("getBatchShardEntryGroups cap=%d want >= 2", cap(got))
	}
	got = append(got, nil)
	if gotPtr := unsafe.SliceData(got); gotPtr != groupsPtr {
		t.Fatalf("getBatchShardEntryGroups did not reuse backing")
	}
	for i, entries := range got[:cap(got)] {
		if entries != nil {
			t.Fatalf("leased shard entry group retained entries at %d: cap=%d", i, cap(entries))
		}
	}
}

func TestBatchReleaseShardEntryGroupsReturnsOuterAndInnerLeases(t *testing.T) {
	forceNormalPoolPressureForTest(t)
	db := &DB{}
	b := &Batch{db: db}
	entries := make([]batch.Entry, 1, 4)
	entriesPtr := unsafe.SliceData(entries)
	groups := make([][]batch.Entry, 1, 4)
	groups[0] = entries
	groupsPtr := unsafe.SliceData(groups)

	b.releaseShardEntryGroups(groups)

	gotGroups := db.getBatchShardEntryGroups(1)
	gotGroups = append(gotGroups, nil)
	if gotPtr := unsafe.SliceData(gotGroups); gotPtr != groupsPtr {
		t.Fatalf("releaseShardEntryGroups did not return outer group backing")
	}
	for i, gotEntries := range gotGroups[:cap(gotGroups)] {
		if gotEntries != nil {
			t.Fatalf("outer group retained inner entries at %d: cap=%d", i, cap(gotEntries))
		}
	}

	gotEntries := db.getBatchShardEntries(4)
	gotEntries = append(gotEntries, batch.Entry{})
	if gotPtr := unsafe.SliceData(gotEntries); gotPtr != entriesPtr {
		t.Fatalf("releaseShardEntryGroups did not return inner entry backing")
	}
}

func TestBatchArenaChunkListLeaseReuseAndClear(t *testing.T) {
	db := &DB{}

	chunks := make([][]byte, 1, 4)
	chunks[0] = make([]byte, 0, 8)
	chunksPtr := unsafe.SliceData(chunks)

	if !db.putBatchArenaChunkListLease(chunks) {
		t.Fatalf("putBatchArenaChunkListLease returned false")
	}
	got := db.takeBatchArenaChunkListLease(2)
	if cap(got) < 2 {
		t.Fatalf("takeBatchArenaChunkListLease cap=%d want >= 2", cap(got))
	}
	got = append(got, nil)
	if gotPtr := unsafe.SliceData(got); gotPtr != chunksPtr {
		t.Fatalf("takeBatchArenaChunkListLease did not reuse backing")
	}
	for i, chunk := range got[:cap(got)] {
		if chunk != nil {
			t.Fatalf("leased chunk list retained chunk at %d: cap=%d", i, cap(chunk))
		}
	}
}

func TestBatchArenaChunkListLeaseReturnedAfterMemtableRelease(t *testing.T) {
	db := &DB{}
	mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(1024, 64)
	chunks := make([][]byte, 1, 4)
	chunks[0] = make([]byte, 0, batchCopyArenaMinChunk)
	chunksPtr := unsafe.SliceData(chunks)

	db.retainBatchArenaChunksForMemtables(chunks, []memtable.Table{mt})
	db.releaseBatchArenaLeasesForMemtable(mt)

	got := db.takeBatchArenaChunkListLease(1)
	if cap(got) < 1 {
		t.Fatalf("takeBatchArenaChunkListLease cap=%d want >= 1", cap(got))
	}
	got = append(got, nil)
	if gotPtr := unsafe.SliceData(got); gotPtr != chunksPtr {
		t.Fatalf("memtable release did not return chunk-list backing")
	}
	for i, chunk := range got[:cap(got)] {
		if chunk != nil {
			t.Fatalf("leased chunk list retained chunk at %d: cap=%d", i, cap(chunk))
		}
	}
}

func TestBatchArenaChunkListEnsureAppendCapGrowsThroughLease(t *testing.T) {
	db := &DB{}
	oldChunk := make([]byte, 0, 8)
	chunks := make([][]byte, 1, 1)
	chunks[0] = oldChunk
	chunksPtr := unsafe.SliceData(chunks)
	grown := make([][]byte, 0, batchArenaChunkListInitialCap)
	grownPtr := unsafe.SliceData(grown)
	db.putBatchArenaChunkListLease(grown)

	got := db.ensureBatchArenaChunkListAppendCap(chunks)
	if cap(got) < batchArenaChunkListInitialCap {
		t.Fatalf("ensureBatchArenaChunkListAppendCap cap=%d want >= %d", cap(got), batchArenaChunkListInitialCap)
	}
	if gotPtr := unsafe.SliceData(got); gotPtr != grownPtr {
		t.Fatalf("ensureBatchArenaChunkListAppendCap did not reuse grown lease")
	}
	if len(got) != 1 || cap(got[0]) != cap(oldChunk) {
		t.Fatalf("ensureBatchArenaChunkListAppendCap lost existing chunk: len=%d cap0=%d", len(got), cap(got[0]))
	}

	returned := db.takeBatchArenaChunkListLease(1)
	if cap(returned) < 1 {
		t.Fatalf("takeBatchArenaChunkListLease cap=%d want >= 1", cap(returned))
	}
	returned = append(returned, nil)
	if returnedPtr := unsafe.SliceData(returned); returnedPtr != chunksPtr {
		t.Fatalf("ensureBatchArenaChunkListAppendCap did not return old backing")
	}
	for i, chunk := range returned[:cap(returned)] {
		if chunk != nil {
			t.Fatalf("returned chunk list retained chunk at %d: cap=%d", i, cap(chunk))
		}
	}
}

func BenchmarkBatchAuxScratchLeaseRoundTrip(b *testing.B) {
	forceNormalPoolPressureForTest(b)
	db := &DB{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := db.getBatchEntries(100)
		entries = entries[:100]
		shardEntries := db.getBatchShardEntries(16)
		shardEntries = shardEntries[:16]
		idxs := db.getBatchIntSlice(100)
		idxs = idxs[:100]
		adds := db.getBatchInt64Slice(16)
		adds = adds[:16]

		db.putBatchEntries(entries)
		db.putBatchShardEntries(shardEntries)
		db.putBatchIntSlice(idxs)
		db.putBatchInt64Slice(adds)
	}
}

func BenchmarkBatchArenaChunkListLeaseRoundTrip(b *testing.B) {
	db := &DB{}
	chunks := make([][]byte, 0, 8)
	db.putBatchArenaChunkListLease(chunks)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunks = db.takeBatchArenaChunkListLease(4)
		chunks = append(chunks, nil, nil, nil, nil)
		db.putBatchArenaChunkListLease(chunks)
	}
}

func BenchmarkBatchArenaChunkListEnsureAppendCapRoundTrip(b *testing.B) {
	db := &DB{}
	chunks := make([][]byte, 0, batchArenaChunkListInitialCap)
	db.putBatchArenaChunkListLease(chunks)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunks = db.ensureBatchArenaChunkListAppendCap(nil)
		chunks = append(chunks, nil)
		db.putBatchArenaChunkListLease(chunks)
	}
}

func BenchmarkZeroInlineKeyProviderRoundTrip(b *testing.B) {
	entries := []batch.Entry{{Key: []byte("key")}}
	var provider zeroInlineBatchKeyProvider
	provider.keyAt = provider.keyAtEntry

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keyAt := provider.keyAtEntries(entries)
		if len(keyAt(0)) == 0 {
			b.Fatal("empty key")
		}
		provider.entries = nil
	}
}

func BenchmarkBatchShardEntryGroupLeaseRoundTrip(b *testing.B) {
	db := &DB{}
	groups := make([][]batch.Entry, 0, 8)
	db.putBatchShardEntryGroups(groups)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		groups = db.getBatchShardEntryGroups(8)
		groups = groups[:8]
		db.putBatchShardEntryGroups(groups)
	}
}
