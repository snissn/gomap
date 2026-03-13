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
		{mode: "hash_sorted", wantLeases: false},
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
	for _, mode := range []string{"btree", "append_only"} {
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
