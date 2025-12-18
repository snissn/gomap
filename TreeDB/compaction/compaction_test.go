package compaction

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	opts := db.Options{Dir: dir}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Force slab rotation so we can compact a non-active slab.
	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = 1000
	t.Cleanup(func() { slab.MaxSlabSize = oldMax })

	// Insert A, B
	valA := bytes.Repeat([]byte("A"), 300) // > 256 -> Pointer
	d.Set([]byte("A"), valA)
	d.Set([]byte("B"), []byte("valB")) // Inline
	// Spec 2.1.1: "Only values stored out-of-line ... are appended as slab records".
	// My `Batch` respects this.
	// So "B" is inline. Slab 0 only has "A".

	// Insert C (Large)
	valC := bytes.Repeat([]byte("C"), 300)
	d.Set([]byte("C"), valC)

	// Update A (New pointer in Slab 0)
	// Old A is dead.
	d.Set([]byte("A"), bytes.Repeat([]byte("A2"), 300))

	// Trigger rotation: the next large write should move active to slab 1.
	d.Set([]byte("D"), bytes.Repeat([]byte("D"), 300))
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected slab rotation, active slab still %d", got)
	}

	// Compact Slab 0
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
