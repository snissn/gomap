package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestBatchReset_DoesNotCorruptPriorBorrowedWrites(t *testing.T) {
	for _, mode := range []string{"append_only", "hash_sorted"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, NewMockBackend(), Options{
				AllowUnsafe:    true,
				DisableWAL:     true,
				MemtableMode:   mode,
				MemtableShards: 1,
				FlushThreshold: 1 << 30,
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			firstKey := []byte("k1")
			firstVal := bytes.Repeat([]byte{0x11}, 64)
			secondKey := []byte("k2")
			secondVal := bytes.Repeat([]byte{0x22}, 64)

			b := db.NewBatchWithSize(1)
			defer func() { _ = b.Close() }()
			if err := b.Set(firstKey, firstVal); err != nil {
				t.Fatalf("set first: %v", err)
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write first: %v", err)
			}

			// Reuse the same batch object; this historically reused copyArena
			// storage still referenced by memtables that retain batch-owned bytes.
			b.Reset()
			if err := b.Set(secondKey, secondVal); err != nil {
				t.Fatalf("set second: %v", err)
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write second: %v", err)
			}

			got, err := db.Get(firstKey)
			if err != nil {
				t.Fatalf("get first: %v", err)
			}
			if !bytes.Equal(got, firstVal) {
				t.Fatalf("first value corrupted: got=%x want=%x", got, firstVal)
			}
		})
	}
}

func TestBatchArenaLeases_CopiedMemtablesMatchOwnershipMode(t *testing.T) {
	cases := []struct {
		mode       string
		wantLeases bool
	}{
		{mode: "append_only", wantLeases: true},
		{mode: "hash_sorted", wantLeases: true},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, NewMockBackend(), Options{
				AllowUnsafe:    true,
				DisableWAL:     true,
				MemtableMode:   tc.mode,
				MemtableShards: 1,
				FlushThreshold: 1 << 30,
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			b := db.NewBatchWithSize(128)
			defer func() { _ = b.Close() }()
			value := bytes.Repeat([]byte{0xAB}, 128)
			for i := 0; i < 128; i++ {
				key := []byte{byte(i), byte(i >> 1), byte(i >> 2), byte(i >> 3), byte(i >> 4), byte(i >> 5), byte(i >> 6), byte(i >> 7)}
				if err := b.Set(key, value); err != nil {
					t.Fatalf("set %d: %v", i, err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write: %v", err)
			}

			db.batchArenaLeaseMu.Lock()
			leased := len(db.batchArenaLeasesByMem)
			db.batchArenaLeaseMu.Unlock()
			if tc.wantLeases {
				if leased == 0 {
					t.Fatalf("expected active batch arena leases after write")
				}
				if got := db.batchArenaLeaseBytes.Load(); got == 0 {
					t.Fatalf("expected non-zero leased batch arena bytes")
				}
			} else {
				if leased != 0 {
					t.Fatalf("expected no active batch arena leases after write, got=%d", leased)
				}
				if got := db.batchArenaLeaseBytes.Load(); got != 0 {
					t.Fatalf("expected zero leased batch arena bytes, got=%d", got)
				}
			}
		})
	}
}

func TestBatchArenaLeases_ReleasedAfterCheckpoint(t *testing.T) {
	for _, mode := range []string{"btree", "append_only", "hash_sorted"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, NewMockBackend(), Options{
				AllowUnsafe:    true,
				DisableWAL:     true,
				MemtableMode:   mode,
				MemtableShards: 1,
				FlushThreshold: 1 << 30,
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			b := db.NewBatchWithSize(128)
			defer func() { _ = b.Close() }()
			value := bytes.Repeat([]byte{0xAB}, 128)
			for i := 0; i < 128; i++ {
				key := []byte{byte(i), byte(i >> 1), byte(i >> 2), byte(i >> 3), byte(i >> 4), byte(i >> 5), byte(i >> 6), byte(i >> 7)}
				if err := b.Set(key, value); err != nil {
					t.Fatalf("set %d: %v", i, err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write: %v", err)
			}

			db.batchArenaLeaseMu.Lock()
			leasedBefore := len(db.batchArenaLeasesByMem)
			db.batchArenaLeaseMu.Unlock()
			if leasedBefore == 0 {
				t.Fatalf("expected active batch arena leases after write")
			}

			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint: %v", err)
			}

			db.batchArenaLeaseMu.Lock()
			leasedAfter := len(db.batchArenaLeasesByMem)
			db.batchArenaLeaseMu.Unlock()
			if leasedAfter != 0 {
				t.Fatalf("expected leases to be released after checkpoint, got=%d", leasedAfter)
			}
		})
	}
}

func TestBatchArenaLeases_RetiredMemtableReleaseWaitsForRecycle(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()
	forceNormalPoolPressureForTest(t)

	db := &DB{}
	mt := memtable.NewAppendOnlyWithCapacityEstimatedEntryBytes(1024, 64)
	chunk := make([]byte, 0, batchCopyArenaMinChunk)
	chunkBytes := int64(cap(chunk))

	db.retainBatchArenaChunksForMemtables([][]byte{chunk}, []memtable.Table{mt})
	if got := db.batchArenaLeaseBytes.Load(); got != chunkBytes {
		t.Fatalf("leased bytes after retain=%d want %d", got, chunkBytes)
	}

	db.queueRetiredMemtableLocked(mt)
	if got := batchArenaPoolBytes.Load(); got != 0 {
		t.Fatalf("retired queue returned live arena bytes=%d want 0", got)
	}
	if got := db.batchArenaLeaseBytes.Load(); got != chunkBytes {
		t.Fatalf("leased bytes after retired queue=%d want %d", got, chunkBytes)
	}

	db.recycleMemtables([]memtable.Table{mt})
	if got := db.batchArenaLeaseBytes.Load(); got != 0 {
		t.Fatalf("leased bytes after recycle=%d want 0", got)
	}
	if got := batchArenaPoolBytes.Load(); got == 0 {
		t.Fatalf("expected recycled arena chunk to return to pool")
	}
}

func TestBatchArenaRetainedHardCap_BoundsLeaseGrowthAcrossSustainedWrites(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	const hardCapBytes = int64(32 << 10)
	batchArenaRetainedHardCapOverride.Store(hardCapBytes)
	t.Cleanup(func() {
		batchArenaRetainedHardCapOverride.Store(0)
		batchArenaLeasedBytesGlobal.Store(0)
	})

	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "btree",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	writeBatch := func(prefix byte) int64 {
		b := db.NewBatchWithSize(512)
		defer func() { _ = b.Close() }()
		value := bytes.Repeat([]byte{prefix}, 1024)
		for i := 0; i < 512; i++ {
			key := []byte{prefix, byte(i >> 8), byte(i)}
			if err := b.Set(key, value); err != nil {
				t.Fatalf("set %d: %v", i, err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatalf("write: %v", err)
		}
		return db.batchArenaLeaseBytes.Load()
	}

	leasedAfterFirst := writeBatch(0x11)
	if leasedAfterFirst < 0 || leasedAfterFirst > hardCapBytes {
		t.Fatalf("leased bytes after first write=%d want in [0,%d]", leasedAfterFirst, hardCapBytes)
	}

	blockedBefore := batchArenaBorrowBlockedTotal.Load()
	leasedAfterSecond := writeBatch(0x22)
	if leasedAfterSecond > leasedAfterFirst {
		t.Fatalf("leased bytes grew under hard cap gating: first=%d second=%d", leasedAfterFirst, leasedAfterSecond)
	}
	if leasedAfterSecond < 0 || leasedAfterSecond > hardCapBytes {
		t.Fatalf("leased bytes after second write=%d want in [0,%d]", leasedAfterSecond, hardCapBytes)
	}
	if got := batchArenaBorrowBlockedTotal.Load(); got <= blockedBefore {
		t.Fatalf("borrow_blocked_total did not increase: before=%d after=%d", blockedBefore, got)
	}
	if got := batchArenaLeasedBytesGlobal.Load(); got != leasedAfterSecond {
		t.Fatalf("global leased bytes=%d want %d", got, leasedAfterSecond)
	}

	got, err := db.Get([]byte{0x11, 0x00, 0x00})
	if err != nil {
		t.Fatalf("get first key: %v", err)
	}
	if len(got) != 1024 || got[0] != 0x11 {
		t.Fatalf("unexpected first key value len=%d head=%x", len(got), got[:1])
	}
}

func TestBatchArenaRetainedHardCap_PreflightBlocksLargeFirstBatchLease(t *testing.T) {
	batchArenaPoolTestMu.Lock()
	defer batchArenaPoolTestMu.Unlock()
	resetBatchArenaPoolsForTest()

	const hardCapBytes = int64(32 << 10)
	batchArenaRetainedHardCapOverride.Store(hardCapBytes)
	t.Cleanup(func() {
		batchArenaRetainedHardCapOverride.Store(0)
		batchArenaLeasedBytesGlobal.Store(0)
	})

	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "btree",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	preflightBefore := batchArenaBorrowPreflightBlockedTotal.Load()
	b := db.NewBatchWithSize(1024)
	defer func() { _ = b.Close() }()
	value := bytes.Repeat([]byte{0x44}, 1024)
	for i := 0; i < 1024; i++ {
		key := []byte{0x44, byte(i >> 8), byte(i)}
		if err := b.Set(key, value); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := db.batchArenaLeaseBytes.Load(); got != 0 {
		t.Fatalf("expected no batch arena leases after preflight-blocked write, got=%d", got)
	}
	if got := batchArenaLeasedBytesGlobal.Load(); got != 0 {
		t.Fatalf("expected no global batch arena leases after preflight-blocked write, got=%d", got)
	}
	if got := batchArenaBorrowPreflightBlockedTotal.Load(); got <= preflightBefore {
		t.Fatalf("borrow_preflight_blocked_total did not increase: before=%d after=%d", preflightBefore, got)
	}
	if got := batchArenaBorrowPreflightBlockedBytesTotal.Load(); got == 0 {
		t.Fatalf("borrow_preflight_blocked_bytes_total=%d want >0", got)
	}

	got, err := db.Get([]byte{0x44, 0x00, 0x00})
	if err != nil {
		t.Fatalf("get first key: %v", err)
	}
	if len(got) != 1024 || got[0] != 0x44 {
		t.Fatalf("unexpected first key value len=%d head=%x", len(got), got[:1])
	}
}

func TestCachedBatchWriteUsesSteal_ExplicitAllowlist(t *testing.T) {
	cases := []struct {
		name        string
		newMemtable func() memtable.Table
		want        bool
	}{
		{name: "skiplist", newMemtable: func() memtable.Table { return memtable.NewWithCapacity(0) }, want: true},
		{name: "btree", newMemtable: func() memtable.Table { return memtable.NewBTree() }, want: true},
		{name: "hash_sorted", newMemtable: func() memtable.Table {
			return memtable.NewHashSortedWithCapacityAndIndexer(0, memtable.NewHashSortedIndexer())
		}, want: false},
		{name: "append_only", newMemtable: func() memtable.Table { return memtable.NewAppendOnlyWithCapacity(0) }, want: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mt := tc.newMemtable()
			if got := cachedBatchWriteUsesSteal(mt); got != tc.want {
				t.Fatalf("memtable=%s useSteal=%v want=%v", tc.name, got, tc.want)
			}
		})
	}
}

func TestCachedBatchWriteUseSteal_DeferredPressureBTreeFallback(t *testing.T) {
	btreeMem := memtable.NewBTree()
	if useSteal, suppressed := cachedBatchWriteUseSteal(nil, btreeMem); !useSteal || suppressed {
		t.Fatalf("nil db btree useSteal=%v suppressed=%v want true,false", useSteal, suppressed)
	}

	db := &DB{}
	if useSteal, suppressed := cachedBatchWriteUseSteal(db, btreeMem); !useSteal || suppressed {
		t.Fatalf("no pressure btree useSteal=%v suppressed=%v want true,false", useSteal, suppressed)
	}

	db.memtableViewTelemetry.deferredBytesCurrent.Store(batchArenaDeferredPressureThresholdBytes)
	if useSteal, suppressed := cachedBatchWriteUseSteal(db, btreeMem); useSteal || !suppressed {
		t.Fatalf("deferred pressure btree useSteal=%v suppressed=%v want false,true", useSteal, suppressed)
	}

	skiplistMem := memtable.NewWithCapacity(0)
	if useSteal, suppressed := cachedBatchWriteUseSteal(db, skiplistMem); !useSteal || suppressed {
		t.Fatalf("deferred pressure skiplist useSteal=%v suppressed=%v want true,false", useSteal, suppressed)
	}
}

func TestBatchWrite_BTreeStealSuppressedUnderDeferredPressure(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "btree",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	suppressedBefore := batchArenaStealSuppressedDeferredTotal.Load()
	suppressedEntriesBefore := batchArenaStealSuppressedDeferredEntriesTotal.Load()
	db.memtableViewTelemetry.deferredBytesCurrent.Store(batchArenaDeferredPressureThresholdBytes)

	b := db.NewBatchWithSize(2)
	defer func() { _ = b.Close() }()
	if err := b.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := batchArenaStealSuppressedDeferredTotal.Load(); got <= suppressedBefore {
		t.Fatalf("steal_suppressed_deferred_total=%d before=%d want increased", got, suppressedBefore)
	}
	if got := batchArenaStealSuppressedDeferredEntriesTotal.Load(); got < suppressedEntriesBefore+2 {
		t.Fatalf("steal_suppressed_deferred_entries_total=%d before=%d want >= %d", got, suppressedEntriesBefore, suppressedEntriesBefore+2)
	}
	if got := db.batchArenaLeaseBytes.Load(); got != 0 {
		t.Fatalf("batchArenaLeaseBytes=%d want 0", got)
	}
	db.batchArenaLeaseMu.Lock()
	leasedMemtables := len(db.batchArenaLeasesByMem)
	db.batchArenaLeaseMu.Unlock()
	if leasedMemtables != 0 {
		t.Fatalf("batchArenaLeasesByMem=%d want 0", leasedMemtables)
	}
}

func TestBatchArenaLeases_NotNeededForPointerWritesOnCopiedMemtables(t *testing.T) {
	for _, mode := range []string{"append_only", "hash_sorted"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, NewMockBackend(), Options{
				AllowUnsafe:              true,
				DisableWAL:               true,
				MemtableMode:             mode,
				MemtableShards:           1,
				FlushThreshold:           1 << 30,
				ValueLogPointerThreshold: 1,
			})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()

			b := db.NewBatchWithSize(1)
			defer func() { _ = b.Close() }()
			key := []byte("ptr-key")
			value := bytes.Repeat([]byte("p"), 256)
			if err := b.Set(key, value); err != nil {
				t.Fatalf("set: %v", err)
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write: %v", err)
			}

			shard := &db.mutableShards[0]
			shard.mu.Lock()
			memVal, ptr, flags, ok := shard.mem.GetEntry(key)
			shard.mu.Unlock()
			if !ok {
				t.Fatalf("expected memtable entry for pointer write")
			}
			if ptr == (page.ValuePtr{}) {
				t.Fatalf("expected non-zero value pointer")
			}
			if flags != node.FlagPointer {
				t.Fatalf("expected pointer flag, got=%d", flags)
			}
			if memVal != nil {
				t.Fatalf("expected pointer-backed memtable entry to drop inline value bytes")
			}

			db.batchArenaLeaseMu.Lock()
			leased := len(db.batchArenaLeasesByMem)
			db.batchArenaLeaseMu.Unlock()
			if leased != 0 {
				t.Fatalf("expected no active batch arena leases after pointer write, got=%d", leased)
			}
			if got := db.batchArenaLeaseBytes.Load(); got != 0 {
				t.Fatalf("expected zero leased batch arena bytes after pointer write, got=%d", got)
			}

			got, err := db.Get(key)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !bytes.Equal(got, value) {
				t.Fatalf("value mismatch: got=%q want=%q", got, value)
			}
		})
	}
}

func TestBatchSetView_MutationAfterWriteDoesNotCorruptStoredValue(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("setview-key")
	buf := bytes.Repeat([]byte("1"), 32)
	want := bytes.Clone(buf)
	blockedBefore := batchArenaBorrowViewOpsBlockedTotal.Load()

	b := db.NewBatchWithSize(1)
	defer func() { _ = b.Close() }()
	if err := b.SetView(key, buf); err != nil {
		t.Fatalf("set view: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	for i := range buf {
		buf[i] = '2'
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value mutated after write: got=%q want=%q", got, want)
	}
	if got := batchArenaBorrowViewOpsBlockedTotal.Load(); got <= blockedBefore {
		t.Fatalf("borrow_view_ops_blocked_total=%d before=%d want increased", got, blockedBefore)
	}
	stats := db.Stats()
	if got := stats["treedb.cache.batch_arena.borrow_view_ops_blocked_total"]; got == "0" {
		t.Fatalf("cache borrow_view_ops_blocked_total=%q want >0", got)
	}
}

func TestBatchStableViewValueLease_AppendOnlyBorrowsWithoutDirectArena(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("stable-view-key")
	value := bytes.Repeat([]byte{0x42}, 128)
	want := bytes.Clone(value)

	b := db.NewBatchWithSize(1)
	defer func() { _ = b.Close() }()
	if err := b.SetView(key, value); err != nil {
		t.Fatalf("set view: %v", err)
	}
	b.AttachStableViewValueLease([][]byte{value[:0]})
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !b.StableViewValueLeaseConsumed() {
		t.Fatal("stable view value lease was not consumed")
	}

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.append_only_direct_arena.active_used_bytes"); got != 0 {
		t.Fatalf("direct arena active used bytes=%d want 0", got)
	}
	if got := mustStatInt64(t, stats, "treedb.cache.batch_arena.leased_bytes"); got == 0 {
		t.Fatal("expected stable view value bytes to be retained as an external lease")
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch: got=%x want=%x", got, want)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if got := mustStatInt64(t, db.Stats(), "treedb.cache.batch_arena.leased_bytes"); got != 0 {
		t.Fatalf("stable external lease bytes after checkpoint=%d want 0", got)
	}
	got, err = db.Get(key)
	if err != nil {
		t.Fatalf("get after checkpoint: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch after checkpoint: got=%x want=%x", got, want)
	}
}

func TestBatchStableViewValueLease_ZeroByteSignalBorrowsWithoutDirectArena(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	key := []byte("stable-zero-signal-key")
	value := bytes.Repeat([]byte{0x24}, 128)
	want := bytes.Clone(value)
	stableSignal := make([]byte, 0)

	b := db.NewBatchWithSize(1)
	defer func() { _ = b.Close() }()
	if err := b.SetView(key, value); err != nil {
		t.Fatalf("set view: %v", err)
	}
	b.AttachStableViewValueLease([][]byte{stableSignal[:0:0]})
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !b.StableViewValueLeaseConsumed() {
		t.Fatal("stable view value lease was not consumed")
	}

	stats := db.Stats()
	if got := mustStatInt64(t, stats, "treedb.cache.append_only_direct_arena.active_used_bytes"); got != 0 {
		t.Fatalf("direct arena active used bytes=%d want 0", got)
	}
	if got := mustStatInt64(t, stats, "treedb.cache.batch_arena.leased_bytes"); got != 0 {
		t.Fatalf("stable zero-signal lease bytes=%d want 0", got)
	}
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value mismatch: got=%x want=%x", got, want)
	}
}

func TestBatchDeleteViewStats(t *testing.T) {
	oldHotPathStatsEnabled := hotPathStatsEnabled
	hotPathStatsEnabled = true
	t.Cleanup(func() {
		hotPathStatsEnabled = oldHotPathStatsEnabled
	})

	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		DisableWAL:     true,
		MemtableMode:   "append_only",
		MemtableShards: 1,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	callsBefore := batchDeleteViewCallsTotal.Load()
	bytesBefore := batchDeleteViewBytesTotal.Load()
	key := []byte("delete-view-key")

	b := db.NewBatchWithSize(1)
	defer func() { _ = b.Close() }()
	if err := b.DeleteView(key); err != nil {
		t.Fatalf("delete view: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	stats := db.Stats()
	if got := batchDeleteViewCallsTotal.Load(); got != callsBefore+1 {
		t.Fatalf("batchDeleteViewCallsTotal=%d before=%d want +1", got, callsBefore)
	}
	if got := batchDeleteViewBytesTotal.Load(); got != bytesBefore+uint64(len(key)) {
		t.Fatalf("batchDeleteViewBytesTotal=%d before=%d want +%d", got, bytesBefore, len(key))
	}
	if got := stats["treedb.cache.batch.delete_view.calls_total"]; got == "" || got == "0" {
		t.Fatalf("cache batch.delete_view.calls_total=%q want >0", got)
	}
	if got := stats["treedb.process.batch.delete_view.calls_total"]; got == "" || got == "0" {
		t.Fatalf("process batch.delete_view.calls_total=%q want >0", got)
	}
}
