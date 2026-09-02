package db

import (
	"bytes"
	"errors"
	"runtime"
	"testing"
)

func TestCompactIndexPrepareFailureRollsBackOldTreeRetirements(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, KeepRecent: 1, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const keyCount = 512
	initial := d.NewBatch().(*Batch)
	for i := 0; i < keyCount; i++ {
		key := []byte{byte(i >> 8), byte(i)}
		if err := initial.Set(key, bytes.Repeat([]byte{byte(i)}, 32)); err != nil {
			t.Fatalf("initial Set %d: %v", i, err)
		}
	}
	if err := initial.WriteSync(); err != nil {
		t.Fatalf("initial WriteSync: %v", err)
	}
	_ = initial.Close()

	beforeFreePages := d.idx.Load().allocator.Counters().FreePages
	beforeSeq := d.currentCommitSeq()
	d.testFailDurableRootAfterCOWPrepare.Store(true)
	err = d.CompactIndex()
	d.testFailDurableRootAfterCOWPrepare.Store(false)
	if !errors.Is(err, errTestDurableRootAfterCOWPrepareFailpoint) {
		t.Fatalf("CompactIndex error=%v want post-COW-prepare failpoint", err)
	}
	if errors.Is(err, ErrRecoveryRequired) || d.publicationPoisoned.Load() {
		t.Fatalf("rollback-safe prepare poisoned writable handle: %v", err)
	}
	if got := d.currentCommitSeq(); got != beforeSeq {
		t.Fatalf("commit seq after failed compaction=%d want %d", got, beforeSeq)
	}
	if got := d.idx.Load().allocator.Counters().FreePages; got != beforeFreePages {
		t.Fatalf("free pages after failed compaction=%d want %d", got, beforeFreePages)
	}

	// Cross both durable slots, then allocate enough fresh pages that stale
	// compaction retirements would become eligible for reuse and corrupt the
	// still-visible original tree.
	for round := 0; round < 4; round++ {
		batch := d.NewBatch().(*Batch)
		for i := 0; i < 256; i++ {
			key := []byte{0x80 + byte(round), byte(i >> 8), byte(i)}
			if err := batch.Set(key, bytes.Repeat([]byte{byte(round + 1)}, 64)); err != nil {
				t.Fatalf("round %d Set %d: %v", round, i, err)
			}
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatalf("round %d WriteSync: %v", round, err)
		}
		_ = batch.Close()
	}
	assertOriginal := func(database *DB) {
		t.Helper()
		for i := 0; i < keyCount; i++ {
			key := []byte{byte(i >> 8), byte(i)}
			want := bytes.Repeat([]byte{byte(i)}, 32)
			got, err := database.Get(key)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("Get original %d=(%x, %v) want %x", i, got, err, want)
			}
		}
	}
	assertOriginal(d)
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(Options{Dir: dir, KeepRecent: 1, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	assertOriginal(reopened)
}

func TestCompactIndexRetiresOldPages(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 10)
	keys := 2000
	if runtime.GOOS == "windows" {
		keys = 1000
	}
	for i := 0; i < keys; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		if err := d.SetSync(k, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	idx := d.idx.Load()
	if idx == nil {
		t.Fatalf("missing index")
	}
	oldHead := idx.allocator.Head()

	if err := d.CompactIndex(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Advance commit seq enough for KeepRecent=1 pruning to kick in for the
	// compaction-retired pages.
	if err := d.SetSync([]byte{0xFF, 0xFE}, val); err != nil {
		t.Fatalf("set1: %v", err)
	}
	if err := d.SetSync([]byte{0xFF, 0xFD}, val); err != nil {
		t.Fatalf("set2: %v", err)
	}

	newHead := d.idx.Load().allocator.Head()
	if oldHead == 0 && newHead == 0 {
		t.Fatalf("expected freelist to become non-empty after compact + pruning")
	}
}
