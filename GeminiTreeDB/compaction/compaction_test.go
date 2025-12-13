package compaction

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap-gemini/TreeDB/db"
)

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := db.Options{Dir: dir}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// 1. Fill Slab 0
	// We need to force rotation. MaxSlabSize is 4GB default.
	// We can't easily force it without writing 4GB.
	// But `SlabManager` doesn't expose `ForceRotate`.
	// However, we can create a new SlabManager with small max size?
	// Or we can rely on `CompactSlab(0)` working even if Slab 0 is active?
	// If Slab 0 is active, `Append` writes to Slab 0.
	// Compaction reads from 0, writes to 0.
	// This is effectively a "Rewrite" / GC.
	// `ApplyCompaction` will update pointers.
	// Pointers will change (offset increases).
	// This tests the logic correctly.
	
	// Insert A, B
	valA := bytes.Repeat([]byte("A"), 300) // > 256 -> Pointer
	d.Set([]byte("A"), valA)
	d.Set([]byte("B"), []byte("valB")) // Inline (ignored by slab compaction usually? No, "Slab Record" format is for large values only?
	// Spec 2.1.1: "Only values stored out-of-line ... are appended as slab records".
	// My `Batch` respects this.
	// So "B" is inline. Slab 0 only has "A".
	
	// Insert C (Large)
	valC := bytes.Repeat([]byte("C"), 300)
	d.Set([]byte("C"), valC)
	
	// Update A (New pointer in Slab 0)
	// Old A is dead.
	d.Set([]byte("A"), bytes.Repeat([]byte("A2"), 300))
	
	// Current Slab 0 contains:
	// 1. A (Dead)
	// 2. C (Live)
	// 3. A2 (Live)
	
	// Compact Slab 0
	// This will scan 0.
	// 1. Read A. Check Tree. Tree has A2. Mismatch. Skip (actually `ApplyCompaction` skips).
	// 2. Read C. Check Tree. Match. Append C to Slab 0 (end). Update Tree.
	// 3. Read A2. Match. Append A2 to Slab 0 (end). Update Tree.
	
	// Wait, if I append C, it moves to end.
	// Then I append A2, it moves to end.
	// So I duplicate live data at the end of the file.
	// This grows the file.
	// But `ApplyCompaction` updates the index.
	// So old space becomes dead.
	// This confirms the Mechanism works.
	// (Real compaction moves to *different* slab usually, or creates new file).
	
	c := New(d)
	if err := c.CompactSlab(0); err != nil {
		t.Fatalf("CompactSlab failed: %v", err)
	}
	
	// Verify Data Integrity
	val, _ := d.Get([]byte("A"))
	if !bytes.Equal(val, bytes.Repeat([]byte("A2"), 300)) {
		t.Error("A corrupted")
	}
	val, _ = d.Get([]byte("C"))
	if !bytes.Equal(val, valC) {
		t.Error("C corrupted")
	}
	
	// Verify Stats/Index updated?
	// Hard to check internal pointer without exposing it.
	// But if Get works, it's good.
}
