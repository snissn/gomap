package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

// TestValueIndexCompaction verifies that compacting a ValueID-backed key
// updates the System Tree but NOT the User Tree.
func TestValueIndexCompaction(t *testing.T) {
	opts := Options{
		Dir:              t.TempDir(),
		EnableValueIndex: true,
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 1. Write a large value
	key := []byte("test-key")
	val := make([]byte, 1024) // > 256 default threshold
	val[0] = 0xAA
	if err := db.Set(key, val); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// 2. Commit (implicit in Set)

	// 3. Verify it used ValueID
	snap := db.AcquireSnapshot()
	defer snap.Close()

	entry, err := snap.GetEntry(key)
	if err != nil {
		t.Fatalf("GetEntry failed: %v", err)
	}
	if entry.Flags&node.FlagValueID == 0 {
		t.Logf("Flags: %x", entry.Flags)
		t.Fatalf("Expected FlagValueID")
	}

	// Decode ValueID
	if len(entry.Value) != 8 {
		t.Fatalf("Expected 8 byte ValueID, got %d", len(entry.Value))
	}

	// 4. Trigger Compaction (Manual)
	oldPtr, err := snap.ResolveValueIDToPtr(entry.Value)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	// Create a NewPtr (fake)
	newPtr := oldPtr
	newPtr.Offset += 100 // Fake move

	ops := []CompactionOp{
		{
			Key:    key,
			OldPtr: oldPtr,
			NewPtr: newPtr,
		},
	}

	// Capture Root ID before
	userRootBefore := db.meta.UserRootPageID
	sysRootBefore := db.meta.SystemRootPageID

	// Apply Compaction
	if err := db.ApplyCompaction(ops); err != nil {
		t.Fatalf("ApplyCompaction failed: %v", err)
	}

	// 5. Verify User Root did NOT change
	userRootAfter := db.meta.UserRootPageID
	sysRootAfter := db.meta.SystemRootPageID

	if userRootAfter != userRootBefore {
		t.Errorf("User Root changed! Value Index failed to isolate churn. Before: %d, After: %d", userRootBefore, userRootAfter)
	}

	if sysRootAfter == sysRootBefore {
		t.Errorf("System Root did NOT change! Compaction update was lost.")
	}

	// 6. Verify Read works with new Ptr
	snap2 := db.AcquireSnapshot()
	defer snap2.Close()

	resolvedPtr, err := snap2.ResolveValueIDToPtr(entry.Value)
	if err != nil {
		t.Fatalf("Resolve after compaction failed: %v", err)
	}

	if resolvedPtr != newPtr {
		t.Errorf("Value Index did not update to new pointer. Got: %+v, Want: %+v", resolvedPtr, newPtr)
	}
}
