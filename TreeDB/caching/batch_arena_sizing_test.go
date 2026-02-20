package caching

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestBatchArenaCopy_IgnoresEntrySliceCapacityForFirstChunk(t *testing.T) {
	b := &Batch{
		// Simulate a large pooled entry backing array from prior workload phase.
		entries:      make([]batch.Entry, 0, 8192),
		copyArenaCap: batchCopyArenaUnsizedInit,
	}

	_ = b.arenaCopy(136)

	if got, want := cap(b.copyArena), batchCopyArenaUnsizedInit; got != want {
		t.Fatalf("copyArena first chunk cap=%d want=%d", got, want)
	}
}

func TestBatchCopyArenaInitCapForEntries(t *testing.T) {
	t.Run("unsized", func(t *testing.T) {
		if got, want := batchCopyArenaInitCapForEntries(0), batchCopyArenaUnsizedInit; got != want {
			t.Fatalf("unsized init cap=%d want=%d", got, want)
		}
	})

	t.Run("sized", func(t *testing.T) {
		entries := 100
		got := batchCopyArenaInitCapForEntries(entries)
		want := entries * batchCopyArenaBytesPerEntry
		if got != want {
			t.Fatalf("sized init cap=%d want=%d", got, want)
		}
	})

	t.Run("clamped", func(t *testing.T) {
		got := batchCopyArenaInitCapForEntries(1 << 30)
		if got != batchCopyArenaInitMax {
			t.Fatalf("clamped init cap=%d want=%d", got, batchCopyArenaInitMax)
		}
	})
}

func TestBatchCopyArenaHint_DecaysAfterLargeToSmall(t *testing.T) {
	var db DB

	db.observeBatchCopyBytes(1 << 20)
	initial := db.batchCopyArenaInitCap(0)
	if initial < (1 << 19) {
		t.Fatalf("initial cap too small after large observe: %d", initial)
	}

	for i := 0; i < 8; i++ {
		db.observeBatchCopyBytes(16 << 10)
	}
	decayed := db.batchCopyArenaInitCap(0)
	if decayed >= initial {
		t.Fatalf("expected decayed cap < initial, initial=%d decayed=%d", initial, decayed)
	}
	if decayed > (128 << 10) {
		t.Fatalf("expected decayed cap to drop near small workload, got=%d", decayed)
	}
}

func TestBatchCopyArenaHint_NewBatchWithSizeCanRaiseInit(t *testing.T) {
	var db DB

	db.observeBatchCopyBytes(16 << 10)
	unsized := db.batchCopyArenaInitCap(0)
	if unsized != 16<<10 {
		t.Fatalf("unsized init cap=%d want=%d", unsized, 16<<10)
	}

	const entries = 8000
	sized := db.batchCopyArenaInitCap(entries)
	wantMin := batchCopyArenaInitCapForEntries(entries)
	if sized < wantMin {
		t.Fatalf("sized init cap=%d want >= %d", sized, wantMin)
	}
}

func TestBatchCopyArenaHint_UsesTotalCopiedBytes(t *testing.T) {
	var db DB
	b := &Batch{
		db:           &db,
		copyArenaCap: batchCopyArenaMinChunk,
	}

	// Force multiple chunk switches so len(copyArena) at the end is much smaller
	// than total copied bytes.
	for i := 0; i < 1024; i++ {
		_ = b.arenaCopy(1024)
	}
	b.updateBatchCopyHint()

	got := db.batchCopyArenaInitCap(0)
	if got < (512 << 10) {
		t.Fatalf("expected init hint to reflect total copied bytes, got=%d", got)
	}
}
