package compaction

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestCompactCandidates_IdempotentWhenSlabDeleted(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = 700
	t.Cleanup(func() { slab.MaxSlabSize = oldMax })

	// Write a pointer value into slab 0.
	valA := bytes.Repeat([]byte("A"), 300)
	if err := d.Set([]byte("A"), valA); err != nil {
		t.Fatalf("Set(A): %v", err)
	}

	// Rotate to slab 1 so slab 0 is eligible for compaction.
	for i := 0; i < 10; i++ {
		key := []byte{byte('D' + i)}
		if err := d.Set(key, bytes.Repeat(key, 300)); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
		if d.SlabManager().ActiveSlabID() != 0 {
			break
		}
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected slab rotation, active slab still %d", got)
	}

	// Overwrite A so the slab0 record becomes dead (dead ratio ~= 1.0).
	if err := d.Set([]byte("A"), bytes.Repeat([]byte("B"), 300)); err != nil {
		t.Fatalf("overwrite A: %v", err)
	}

	slab0Path := filepath.Join(dir, "data-0000.slab")
	if _, err := os.Stat(slab0Path); err != nil {
		t.Fatalf("expected slab0 to exist: %v", err)
	}

	c := New(d)
	opts := Options{DeadRatioThreshold: 0.50, MinTotalBytes: 1, MaxSlabs: 0}

	if err := c.CompactCandidates(opts); err != nil {
		t.Fatalf("CompactCandidates(1): %v", err)
	}

	// With sprint10 RefreshSlabSet, the compacted slab can be deleted immediately.
	// Candidate selection must skip it on subsequent runs rather than erroring.
	if err := c.CompactCandidates(opts); err != nil {
		t.Fatalf("CompactCandidates(2): %v", err)
	}
}
