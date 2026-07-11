package db

import (
	"fmt"
	"sync/atomic"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogRefTrackerApplyPathAvoidsPointLookups(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	const count = 64
	seed := d.NewBatch().(*Batch)
	for i := 0; i < count; i++ {
		ptr := page.ValuePtr{FileID: page.ValueLogFileID(1), Offset: uint64(i + 1), Length: 64}
		if err := seed.SetPointer([]byte(fmt.Sprintf("key-%06d", i)), ptr); err != nil {
			t.Fatalf("seed SetPointer %d: %v", i, err)
		}
	}
	if err := seed.Write(); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	_ = seed.Close()

	var lookups atomic.Uint64
	restore := registerLookupValueLogRefAtKeyHook(func() { lookups.Add(1) })
	defer restore()

	update := d.NewBatch().(*Batch)
	for i := 0; i < count; i++ {
		ptr := page.ValuePtr{FileID: page.ValueLogFileID(2), Offset: uint64(i + 1000), Length: 96}
		if err := update.SetPointer([]byte(fmt.Sprintf("key-%06d", i)), ptr); err != nil {
			t.Fatalf("update SetPointer %d: %v", i, err)
		}
	}
	if err := update.Write(); err != nil {
		t.Fatalf("update Write: %v", err)
	}
	_ = update.Close()

	if got := lookups.Load(); got != 0 {
		t.Fatalf("lookupValueLogRefAtKey calls=%d want 0", got)
	}
}

func TestBuildValueLogRefDeltaFallsBackWhenApplyEvidenceMissing(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	seed := d.NewBatch().(*Batch)
	for i := 0; i < 4; i++ {
		ptr := page.ValuePtr{FileID: page.ValueLogFileID(31), Offset: uint64(i + 1), Length: 64}
		if err := seed.SetPointer([]byte(fmt.Sprintf("key-%06d", i)), ptr); err != nil {
			t.Fatalf("seed SetPointer: %v", err)
		}
	}
	if err := seed.Write(); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	_ = seed.Close()

	idx := d.idx.Load()
	state := d.state.Load()
	if state == nil {
		t.Fatal("missing visible state")
	}
	rootID := state.RootPageID
	baseSeq := state.CommitSeq
	entries := make([]batchpkg.Entry, 4)
	for i := range entries {
		entries[i] = batchpkg.Entry{Key: []byte(fmt.Sprintf("key-%06d", i)), Type: batchpkg.OpDelete}
	}

	var lookups atomic.Uint64
	restore := registerLookupValueLogRefAtKeyHook(func() { lookups.Add(1) })
	defer restore()
	delta, err := d.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries, nil, nil, false)
	if err != nil {
		t.Fatalf("buildValueLogRefDelta: %v", err)
	}
	defer releaseValueLogRefDelta(delta)
	if got, want := lookups.Load(), uint64(len(entries)); got != want {
		t.Fatalf("fallback lookup calls=%d want %d", got, want)
	}
}

func TestValueLogRefTrackerPreservesSharedPointerMultiplicity(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	shared := page.ValuePtr{FileID: page.ValueLogFileID(7), Offset: 100, Length: 256}
	seed := d.NewBatch().(*Batch)
	for _, key := range []string{"a", "b"} {
		if err := seed.SetPointer([]byte(key), shared); err != nil {
			t.Fatalf("seed SetPointer %q: %v", key, err)
		}
	}
	if err := seed.Write(); err != nil {
		t.Fatalf("seed Write: %v", err)
	}
	_ = seed.Close()

	removeOne := d.NewBatch().(*Batch)
	if err := removeOne.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := removeOne.Write(); err != nil {
		t.Fatalf("delete Write: %v", err)
	}
	_ = removeOne.Close()

	seq, counts, ok := d.valueLogRefTracker.dirtySnapshot()
	if !ok || seq != d.currentCommitSeq() {
		t.Fatalf("tracker snapshot ok=%v seq=%d current=%d", ok, seq, d.currentCommitSeq())
	}
	if got, want := counts[shared.FileID], uint64(1); got != want {
		t.Fatalf("shared pointer count=%d want %d", got, want)
	}
}
