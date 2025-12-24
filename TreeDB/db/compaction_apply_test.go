package db

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestApplyCompactionMicroBatches_PartialResumeIsSafe(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	// Populate slab 0 with pointer values.
	val := bytes.Repeat([]byte("V"), 300)
	keys := make([][]byte, 0, 80)
	for i := 0; i < 80; i++ {
		k := []byte{0x01, byte(i)}
		keys = append(keys, k)
		if err := d.Set(k, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	// Rotate so new records are appended to a different slab.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected active slab != 0 after rotation")
	}

	// Build compaction ops by copying each live pointer value to the active slab.
	snap := d.AcquireSnapshot()
	defer snap.Close()

	ops := make([]CompactionOp, 0, len(keys))
	for _, k := range keys {
		entry, err := snap.GetEntry(k)
		if err != nil {
			t.Fatalf("GetEntry: %v", err)
		}
		if entry.Flags&node.FlagPointer == 0 {
			t.Fatalf("expected pointer entry for key %q", k)
		}

		// Re-append the same key/value to the active slab to get a new pointer.
		newPtr, err := d.SlabManager().Append(k, val)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		// Sanity: new ptr must not be in the original slab.
		if newPtr.FileID == entry.ValuePtr.FileID {
			t.Fatalf("expected new ptr in a different slab (got %d)", newPtr.FileID)
		}

		ops = append(ops, CompactionOp{
			Key:    append([]byte(nil), k...),
			OldPtr: entry.ValuePtr,
			NewPtr: newPtr,
		})
	}

	// Apply only a subset of the ops (simulating an interrupted compaction).
	if err := d.ApplyCompactionMicroBatches(ops[:40], 10); err != nil {
		t.Fatalf("ApplyCompactionMicroBatches(partial): %v", err)
	}

	// Verify the DB remains readable after partial apply.
	for _, k := range keys[:40] {
		got, err := d.Get(k)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got, val) {
			t.Fatalf("value mismatch after partial apply")
		}
	}

	// Resume by applying the full op list. Already-applied keys should be skipped.
	if err := d.ApplyCompactionMicroBatches(ops, 10); err != nil {
		t.Fatalf("ApplyCompactionMicroBatches(resume): %v", err)
	}

	// Verify all keys still read correctly.
	for _, k := range keys {
		got, err := d.Get(k)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !bytes.Equal(got, val) {
			t.Fatalf("value mismatch after resume")
		}
	}

	// Avoid unused import if slab changes; keep at least one reference.
	_ = slab.MaxSlabSize
}

func TestApplyCompactionMicroBatches_SkipsStalePointer(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("k")
	val1 := bytes.Repeat([]byte("A"), 300)
	val2 := bytes.Repeat([]byte("B"), 300)

	if err := d.Set(key, val1); err != nil {
		t.Fatalf("set val1: %v", err)
	}

	// Rotate so compaction targets a non-active slab.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected active slab != 0 after rotation")
	}

	snap := d.AcquireSnapshot()
	entry1, err := snap.GetEntry(key)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("GetEntry val1: %v", err)
	}
	if entry1.Flags&node.FlagPointer == 0 {
		t.Fatalf("expected pointer entry for key %q", key)
	}

	if err := d.Set(key, val2); err != nil {
		t.Fatalf("set val2: %v", err)
	}

	snap2 := d.AcquireSnapshot()
	entry2, err := snap2.GetEntry(key)
	_ = snap2.Close()
	if err != nil {
		t.Fatalf("GetEntry val2: %v", err)
	}
	if entry2.Flags&node.FlagPointer == 0 {
		t.Fatalf("expected pointer entry for key %q", key)
	}

	newPtr, err := d.SlabManager().Append(key, val1)
	if err != nil {
		t.Fatalf("append compaction ptr: %v", err)
	}
	if newPtr == entry2.ValuePtr {
		t.Fatalf("expected compaction ptr to differ from latest pointer")
	}

	op := CompactionOp{
		Key:    append([]byte(nil), key...),
		OldPtr: entry1.ValuePtr,
		NewPtr: newPtr,
	}

	if err := d.ApplyCompactionMicroBatches([]CompactionOp{op}, 1); err != nil {
		t.Fatalf("ApplyCompactionMicroBatches: %v", err)
	}

	snap3 := d.AcquireSnapshot()
	entry3, err := snap3.GetEntry(key)
	_ = snap3.Close()
	if err != nil {
		t.Fatalf("GetEntry after compaction: %v", err)
	}
	if entry3.ValuePtr != entry2.ValuePtr {
		t.Fatalf("expected pointer to remain at latest value")
	}

	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val2) {
		t.Fatalf("expected latest value to survive compaction")
	}
}
