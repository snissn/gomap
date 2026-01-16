package db

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestApplyCompaction_ValueID_InPlace(t *testing.T) {
	// This test verifies that Slab Compaction correctly updates ValueID-backed
	// pointers when using the in-place fast path (no pinned readers).
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:              dir,
		EnableValueIndex: true,
		// Lower threshold to ensure we use ValueIDs even for relatively small values
		ValueLogPointerThreshold: 10,
		ForceValuePointers:       true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	key := []byte("key-using-vid")
	val := bytes.Repeat([]byte("A"), 100)
	if err := d.SetSync(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}

	// Verify it's a ValueID
	snap := d.AcquireSnapshot()
	entry, err := snap.GetEntry(key)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Flags&node.FlagValueID == 0 {
		t.Fatalf("expected FlagValueID to be set")
	}
	vid := ValueID(binary.BigEndian.Uint64(entry.Value))
	oldPtr, err := snap.ResolveValueIDToPtr(entry.Value)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	snap.Close()

	// Rotate slab
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Build compaction op: move value to new slab
	newPtr, err := d.SlabManager().Append(key, val)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	op := CompactionOp{
		Key:    append([]byte(nil), key...),
		OldPtr: oldPtr,
		NewPtr: newPtr,
	}

	// Ensure no pinned readers (already true, but let's be explicit)
	// ApplyCompactionMicroBatches should use the in-place path.
	if err := d.ApplyCompactionMicroBatches([]CompactionOp{op}, 1); err != nil {
		t.Fatalf("ApplyCompactionMicroBatches: %v", err)
	}

	// Verify pointer was updated
	snap2 := d.AcquireSnapshot()
	defer snap2.Close()
	entry2, err := snap2.GetEntry(key)
	if err != nil {
		t.Fatalf("GetEntry after compaction: %v", err)
	}

	// ValueID in User Tree should NOT change
	if !bytes.Equal(entry2.Value, entry.Value) {
		t.Fatalf("ValueID in user tree changed unexpectedly")
	}

	gotPtr, err := snap2.ResolveValueIDToPtr(entry2.Value)
	if err != nil {
		t.Fatalf("Resolve after compaction: %v", err)
	}

	if gotPtr != newPtr {
		t.Fatalf("Pointer not updated in system tree! got %v, want %v (vid=%d)", gotPtr, newPtr, vid)
	}

	// Verify data is still correct
	gotVal, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(gotVal, val) {
		t.Fatalf("data corrupted after compaction")
	}
}
