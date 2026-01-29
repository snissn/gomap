package treedb

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestForceValueLogPointers_UsesPointersForSmallValues(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:            dir,
		FlushThreshold: 1,
		ValueLog: ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1 << 20, // Large enough that small values would be inline without ForcePointers.
		},
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	key := []byte("k1")
	val := []byte("v1")
	if err := db.Set(key, val); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	backend := db.backend
	snap := backend.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("snapshot nil")
	}
	entry, err := snap.GetEntry(key)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Flags&node.FlagPointer == 0 || !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		_ = snap.Close()
		t.Fatalf("expected value-log pointer for small value, got flags=%#x file_id=%#x", entry.Flags, entry.ValuePtr.FileID)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}

	got, err := backend.Get(key)
	if err != nil {
		t.Fatalf("backend Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("backend Get mismatch")
	}
}
