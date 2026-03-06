package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestBatchReset_DoesNotCorruptPriorStealWrites(t *testing.T) {
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
			// storage still referenced by borrowed-steal memtables.
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

func TestBatchArenaLeases_NotNeededForCopiedMemtables(t *testing.T) {
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
			if leased != 0 {
				t.Fatalf("expected no active batch arena leases after write, got=%d", leased)
			}
			if got := db.batchArenaLeaseBytes.Load(); got != 0 {
				t.Fatalf("expected zero leased batch arena bytes, got=%d", got)
			}
		})
	}
}

func TestBatchArenaLeases_ReleasedAfterCheckpoint_BTree(t *testing.T) {
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
}

func TestMemtableBatchWriteUsesSteal_ExplicitAllowlist(t *testing.T) {
	cases := []struct {
		mode memtable.Mode
		want bool
	}{
		{mode: memtable.ModeSkiplist, want: true},
		{mode: memtable.ModeBTree, want: true},
		{mode: memtable.ModeHashSorted, want: false},
		{mode: memtable.ModeAppendOnly, want: false},
	}
	for _, tc := range cases {
		mt, err := memtable.NewWithCapacityMode(0, tc.mode)
		if err != nil {
			t.Fatalf("NewWithCapacityMode(%v): %v", tc.mode, err)
		}
		if got := memtableBatchWriteUsesSteal(mt); got != tc.want {
			t.Fatalf("mode=%v useSteal=%v want=%v", tc.mode, got, tc.want)
		}
	}
}
