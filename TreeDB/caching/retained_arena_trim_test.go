package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestTrimRetainedArenasAfterFlush_CheckpointPathTrimsAppendOnlyCaches(t *testing.T) {
	lockEntrySlicePoolStateForTest(t)
	resetEntrySlicePoolStateForTest(t)
	batchArenaPoolTestMu.Lock()
	t.Cleanup(batchArenaPoolTestMu.Unlock)
	resetBatchArenaPoolsForTest()

	db := &DB{
		mutableShards: make([]memShard, 1),
	}
	db.storeMemtableMode(memtable.ModeAppendOnly)

	leaseCount := postCheckpointAppendOnlyMemLeaseKeep + 6
	for i := 0; i < leaseCount; i++ {
		mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, appendOnlyEstimatedBytesPerEntryDefault)
		db.appendOnlyMemLeases = append(db.appendOnlyMemLeases, mt)
	}

	shard := &db.mutableShards[0]
	for i := 0; i < 64; i++ {
		chunk := make([]byte, 0, appendOnlyDirectValueArenaDefaultChunk)
		shard.appendOnlyDirectValueArena.retained = append(shard.appendOnlyDirectValueArena.retained, chunk)
		shard.appendOnlyDirectValueArena.retainedBytes += int64(cap(chunk))
	}

	db.trimRetainedArenasAfterFlush(true)

	if got := len(db.appendOnlyMemLeases); got > postCheckpointAppendOnlyMemLeaseKeep {
		t.Fatalf("append-only mem leases=%d want <= %d", got, postCheckpointAppendOnlyMemLeaseKeep)
	}
	maxDirectRetained := int64(appendOnlyDirectValueArenaRetainMaxBytes / 4)
	if got := shard.appendOnlyDirectValueArena.retainedBytes; got > maxDirectRetained {
		t.Fatalf("direct arena retained bytes=%d want <= %d", got, maxDirectRetained)
	}
}

func TestTrimAppendOnlyMemLeases_DroppedLeasesReturnToPool(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeAppendOnly)

	keep := 2
	leaseCount := keep + 6
	for i := 0; i < leaseCount; i++ {
		mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, appendOnlyEstimatedBytesPerEntryDefault)
		db.appendOnlyMemLeases = append(db.appendOnlyMemLeases, mt)
	}

	returned := db.trimAppendOnlyMemLeases(keep, 4<<20)

	if got := len(db.appendOnlyMemLeases); got != keep {
		t.Fatalf("append-only mem leases=%d want %d", got, keep)
	}
	if want := leaseCount - keep; returned != want {
		t.Fatalf("returned append-only leases=%d want %d", returned, want)
	}
}

func TestTrimAppendOnlyMemLeases_DropsColdWhenModeNotAppendOnly(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeBTree)

	const leaseCount = 6
	leases := make([]*memtable.AppendOnly, 0, leaseCount)
	for i := 0; i < leaseCount; i++ {
		mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, appendOnlyEstimatedBytesPerEntryDefault)
		if mt.EntryCapacity() == 0 {
			t.Fatal("test setup produced zero append-only entry capacity")
		}
		leases = append(leases, mt)
		db.appendOnlyMemLeases = append(db.appendOnlyMemLeases, mt)
	}

	returned := db.trimAppendOnlyMemLeases(2, 4<<20)

	if got := len(db.appendOnlyMemLeases); got != 0 {
		t.Fatalf("append-only mem leases=%d want 0 after non-append-only trim", got)
	}
	if returned != leaseCount {
		t.Fatalf("returned append-only leases=%d want %d", returned, leaseCount)
	}
	for i, mt := range leases {
		if got := mt.EntryCapacity(); got != 0 {
			t.Fatalf("lease %d retained entry capacity=%d want 0", i, got)
		}
	}
}

func TestRecycleAppendOnlyMemtables_DropsColdWhenModeNotAppendOnly(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeBTree)

	mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(4<<20, appendOnlyEstimatedBytesPerEntryDefault)
	if mt.EntryCapacity() == 0 || mt.EntryBackingBytes() == 0 {
		t.Fatal("test setup produced zero append-only entry backing")
	}

	db.recycleMemtables([]memtable.Table{mt})

	if got := len(db.appendOnlyMemLeases); got != 0 {
		t.Fatalf("append-only mem leases=%d want 0", got)
	}
	if got := mt.EntryCapacity(); got != 0 {
		t.Fatalf("released append-only entry capacity=%d want 0", got)
	}
	count, entryCapacity, entryBackingBytes, valueArena := db.appendOnlyMemLeaseStats()
	if count != 0 || entryCapacity != 0 || entryBackingBytes != 0 {
		t.Fatalf("append-only lease stats count=%d capacity=%d bytes=%d want all zero", count, entryCapacity, entryBackingBytes)
	}
	if valueArena.ActiveBytes != 0 || valueArena.RetainedBytes != 0 {
		t.Fatalf("append-only lease value arena stats=%+v want zero bytes", valueArena)
	}
}

func TestRecycleAppendOnlyMemtables_DropsOverflowEntryBackingBeforePool(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeAppendOnly)

	const capacityBytes = 4 << 20
	mems := make([]memtable.Table, 0, maxAppendOnlyMemLeases+1)
	for i := 0; i < cap(mems); i++ {
		mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, appendOnlyEstimatedBytesPerEntryDefault)
		if mt.EntryBackingBytes() == 0 {
			t.Fatal("test setup produced zero append-only entry backing")
		}
		mems = append(mems, mt)
	}

	db.recycleMemtables(mems)

	if got := len(db.appendOnlyMemLeases); got != maxAppendOnlyMemLeases {
		t.Fatalf("append-only mem leases=%d want %d", got, maxAppendOnlyMemLeases)
	}
	if got := db.appendOnlyMemPoolPutTotal.Load(); got != 1 {
		t.Fatalf("append-only mem pool puts=%d want 1", got)
	}
	if got := db.appendOnlyMemPoolEntryDropBytes.Load(); got == 0 {
		t.Fatal("append-only mem pool entry backing dropped bytes=0 want >0")
	}

	overflow, ok := mems[maxAppendOnlyMemLeases].(*memtable.AppendOnly)
	if !ok || overflow == nil {
		t.Fatalf("overflow memtable is %T, want *AppendOnly", mems[maxAppendOnlyMemLeases])
	}
	if got := overflow.EntryBackingBytes(); got != 0 {
		t.Fatalf("overflow append-only entry backing bytes=%d want 0", got)
	}
}

func TestStoreMemtableMode_DropsAppendOnlyPoolsOnColdTransition(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeAppendOnly)
	beforePool := db.appendOnlyMemtablePool()
	if beforePool == nil {
		t.Fatal("expected append-only memtable pool")
	}
	beforeEntryPoolDrops := memtable.AppendOnlyEntryPoolDropTotal()
	beforeValueArenaPoolDrops := memtable.AppendOnlyValueArenaPoolDropTotal()

	db.storeMemtableMode(memtable.ModeBTree)

	afterPool := db.appendOnlyMemtablePool()
	if afterPool == nil {
		t.Fatal("expected replacement append-only memtable pool")
	}
	if afterPool == beforePool {
		t.Fatal("append-only memtable pool was not replaced on cold transition")
	}
	if got := db.appendOnlyMemPoolDropTotal.Load(); got != 1 {
		t.Fatalf("append-only memtable pool drops=%d want 1", got)
	}
	if got := memtable.AppendOnlyEntryPoolDropTotal(); got != beforeEntryPoolDrops+1 {
		t.Fatalf("append-only entry pool drops=%d want %d", got, beforeEntryPoolDrops+1)
	}
	if got := memtable.AppendOnlyValueArenaPoolDropTotal(); got != beforeValueArenaPoolDrops+1 {
		t.Fatalf("append-only value arena pool drops=%d want %d", got, beforeValueArenaPoolDrops+1)
	}
}

func TestTrimEmptyAppendOnlyMutableShards_DropsWarmEntryBacking(t *testing.T) {
	const capacityBytes = 4 << 10
	var db DB
	db.storeMemtableMode(memtable.ModeAppendOnly)
	db.mutableShards = make([]memShard, 1)
	db.memtableCap = capacityBytes

	mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(capacityBytes, appendOnlyEstimatedBytesPerEntryDefault)
	for i := 0; i < 1024; i++ {
		mt.Set([]byte{byte(i), byte(i >> 8)}, []byte("value"))
	}
	mt.ResetWithCapacity(capacityBytes, appendOnlyEstimatedBytesPerEntryDefault)
	warmCap := mt.EntryCapacity()
	if warmCap <= 0 {
		t.Fatal("test setup produced zero warm entry capacity")
	}
	db.mutableShards[0].mem = mt

	trimmed := db.trimEmptyAppendOnlyMutableShards(capacityBytes)

	if trimmed != 1 {
		t.Fatalf("trimmed mutable shards=%d want 1", trimmed)
	}
	if got := mt.EntryCapacity(); got >= warmCap {
		t.Fatalf("entry capacity after trim=%d want below warm capacity=%d", got, warmCap)
	}
	if got := db.appendOnlyMutableTrimTotal.Load(); got != 1 {
		t.Fatalf("mutable trim total=%d want 1", got)
	}
	if got := db.appendOnlyMutableTrimDropped.Load(); got == 0 {
		t.Fatal("mutable trim dropped bytes=0 want >0")
	}
}

func TestAppendOnlyIdleMutableRetainCapacity_CapsEmptyShardBacking(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeAppendOnly)
	db.memtableCap = 64 << 20
	db.mutableShards = make([]memShard, 1)

	activeCap := db.checkpointRotateCapacity()
	if activeCap <= appendOnlyIdleMutableRetainCapacity {
		t.Fatalf("checkpoint rotate capacity=%d want above idle cap=%d", activeCap, appendOnlyIdleMutableRetainCapacity)
	}
	idleCap := db.appendOnlyIdleMutableRetainCapacity()
	if idleCap != appendOnlyIdleMutableRetainCapacity {
		t.Fatalf("idle mutable retain capacity=%d want %d", idleCap, appendOnlyIdleMutableRetainCapacity)
	}

	mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(activeCap, appendOnlyEstimatedBytesPerEntryDefault)
	for i := 0; i < 4096; i++ {
		mt.Set([]byte{byte(i), byte(i >> 8)}, []byte("value"))
	}
	mt.ResetWithCapacity(activeCap, appendOnlyEstimatedBytesPerEntryDefault)
	before := mt.EntryBackingBytes()
	if before <= 0 {
		t.Fatal("test setup produced zero append-only entry backing")
	}
	db.mutableShards[0].mem = mt

	if trimmed := db.trimEmptyAppendOnlyMutableShards(idleCap); trimmed != 1 {
		t.Fatalf("trimmed mutable shards=%d want 1", trimmed)
	}
	after := mt.EntryBackingBytes()
	if after >= before {
		t.Fatalf("entry backing after trim=%d want below before=%d", after, before)
	}
	if after > int64(appendOnlyIdleMutableRetainCapacity) {
		t.Fatalf("entry backing after trim=%d want <= idle cap=%d", after, appendOnlyIdleMutableRetainCapacity)
	}
}

func TestTrimRetainedArenasAfterFlush_TrimsSparseAppendOnlyMutableBacking(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeAppendOnly)
	db.memtableCap = 64 << 20
	db.mutableShards = make([]memShard, 1)

	mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(db.checkpointRotateCapacity(), appendOnlyEstimatedBytesPerEntryDefault)
	for i := 0; i < 1024; i++ {
		mt.Set([]byte{byte(i >> 8), byte(i)}, []byte("value"))
	}
	before := mt.EntryBackingBytes()
	if before <= int64(appendOnlyIdleMutableRetainCapacity) {
		t.Fatalf("test setup entry backing=%d want above idle cap=%d", before, appendOnlyIdleMutableRetainCapacity)
	}
	db.mutableShards[0].mem = mt

	db.trimRetainedArenasAfterFlush(false)

	after := mt.EntryBackingBytes()
	if after >= before {
		t.Fatalf("entry backing after sparse trim=%d want below before=%d", after, before)
	}
	if after > int64(appendOnlyIdleMutableRetainCapacity) {
		t.Fatalf("entry backing after sparse trim=%d want <= idle cap=%d", after, appendOnlyIdleMutableRetainCapacity)
	}
	if got := mt.Len(); got != 1024 {
		t.Fatalf("len after sparse trim=%d want 1024", got)
	}
	value, deleted, found := mt.Get([]byte{0, 42})
	if !found || deleted || string(value) != "value" {
		t.Fatalf("Get after sparse trim=(%q,%t,%t), want value", value, deleted, found)
	}
	if got := db.appendOnlyMutableTrimTotal.Load(); got != 1 {
		t.Fatalf("mutable trim total=%d want 1", got)
	}
	if got := db.appendOnlyMutableSparseTrimTotal.Load(); got != 1 {
		t.Fatalf("mutable sparse trim total=%d want 1", got)
	}
	if got := db.appendOnlyMutableTrimDropped.Load(); got == 0 {
		t.Fatal("mutable trim dropped bytes=0 want >0")
	}
	if got := db.appendOnlyMutableSparseTrimDropped.Load(); got == 0 {
		t.Fatal("mutable sparse trim dropped bytes=0 want >0")
	}
}

func TestTrimRetainedArenasAfterFlush_CheckpointDropsEmptyMutableBacking(t *testing.T) {
	var db DB
	db.storeMemtableMode(memtable.ModeAppendOnly)
	db.memtableCap = 64 << 20
	db.mutableShards = make([]memShard, 1)

	mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(db.checkpointRotateCapacity(), appendOnlyEstimatedBytesPerEntryDefault)
	if got := mt.EntryBackingBytes(); got == 0 {
		t.Fatal("test setup produced zero append-only entry backing")
	}
	db.mutableShards[0].mem = mt

	db.trimRetainedArenasAfterFlush(true)

	if got := mt.EntryBackingBytes(); got != 0 {
		t.Fatalf("entry backing after checkpoint trim=%d want 0", got)
	}
	if got := db.appendOnlyMutableTrimTotal.Load(); got != 1 {
		t.Fatalf("mutable trim total=%d want 1", got)
	}
	if got := db.appendOnlyMutableTrimDropped.Load(); got == 0 {
		t.Fatal("mutable trim dropped bytes=0 want >0")
	}

	mt.Set([]byte("k"), []byte("v"))
	if got := mt.Len(); got != 1 {
		t.Fatalf("len after post-trim write=%d want 1", got)
	}
	value, deleted, found := mt.Get([]byte("k"))
	if !found || deleted || string(value) != "v" {
		t.Fatalf("Get after post-trim write=(%q,%t,%t), want value", value, deleted, found)
	}
}

func TestCheckpointDropsEmptyAppendOnlyMutableBackingStats(t *testing.T) {
	db, err := Open(t.TempDir(), NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("before"), []byte("value")); err != nil {
		t.Fatalf("set before checkpoint: %v", err)
	}
	before := db.Stats()
	beforeBacking := parseUintStat(before, "treedb.cache.append_only.mutable_entry_backing_bytes")
	if beforeBacking == 0 {
		t.Fatalf("mutable entry backing before checkpoint=0, stats=%v", before)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	after := db.Stats()
	if got := parseUintStat(after, "treedb.cache.append_only.mutable_entry_backing_bytes"); got != 0 {
		t.Fatalf("mutable entry backing after checkpoint=%d want 0", got)
	}
	if got := parseUintStat(after, "treedb.cache.memtable_residency.mutable.append_only.entry_backing_bytes"); got != 0 {
		t.Fatalf("mutable residency append-only entry backing after checkpoint=%d want 0", got)
	}
	if got := parseUintStat(after, "treedb.cache.append_only.mutable_trim_total"); got == 0 {
		t.Fatalf("mutable trim total after checkpoint=0, stats=%v", after)
	}
	if got := parseUintStat(after, "treedb.cache.append_only.mutable_trim_dropped_bytes_total"); got == 0 {
		t.Fatalf("mutable trim dropped bytes after checkpoint=0, stats=%v", after)
	}

	gotBefore, err := db.Get([]byte("before"))
	if err != nil {
		t.Fatalf("get before after checkpoint: %v", err)
	}
	if string(gotBefore) != "value" {
		t.Fatalf("get before after checkpoint=%q want value", gotBefore)
	}
	if err := db.Set([]byte("after"), []byte("reuse")); err != nil {
		t.Fatalf("set after checkpoint: %v", err)
	}
	gotAfter, err := db.Get([]byte("after"))
	if err != nil {
		t.Fatalf("get after post-checkpoint write: %v", err)
	}
	if string(gotAfter) != "reuse" {
		t.Fatalf("get after post-checkpoint write=%q want reuse", gotAfter)
	}
	postWrite := db.Stats()
	if got := parseUintStat(postWrite, "treedb.cache.append_only.mutable_entry_backing_bytes"); got == 0 {
		t.Fatalf("mutable entry backing after post-checkpoint write=0, stats=%v", postWrite)
	}
}
