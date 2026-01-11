package compaction

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/slab"
)

func TestCandidatesSelectsDeadNonActiveSlabs(t *testing.T) {
	dir := t.TempDir()

	// Force rotation so we have a non-active slab to consider.
	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = slab.SlabV2DataStart + 1000
	t.Cleanup(func() { slab.MaxSlabSize = oldMax })

	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	valA1 := bytes.Repeat([]byte("A"), 300)
	valA2 := bytes.Repeat([]byte("B"), 300)
	valB := bytes.Repeat([]byte("C"), 300)

	// Fill slab 0 with a dead record and two live ones.
	if err := d.SetSync([]byte("A"), valA1); err != nil {
		t.Fatalf("set A1: %v", err)
	}
	if err := d.SetSync([]byte("A"), valA2); err != nil {
		t.Fatalf("set A2: %v", err)
	}
	if err := d.SetSync([]byte("B"), valB); err != nil {
		t.Fatalf("set B: %v", err)
	}

	// Trigger rotation; now active slab should not be 0.
	if err := d.SetSync([]byte("C"), bytes.Repeat([]byte("D"), 300)); err != nil {
		t.Fatalf("set C: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected rotation, active still %d", got)
	}

	c := New(d)
	cands, err := c.Candidates(Options{
		DeadRatioThreshold: 0.30,
		MinTotalBytes:      1,
	})
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}

	active := d.SlabManager().ActiveSlabID()
	for _, cand := range cands {
		if cand.FileID == active {
			t.Fatalf("expected active slab %d to be excluded", active)
		}
	}

	found0 := false
	for _, cand := range cands {
		if cand.FileID == 0 {
			found0 = true
			if cand.DeadRatio < 0.30 {
				t.Fatalf("expected dead ratio >= 0.30, got %f", cand.DeadRatio)
			}
		}
	}
	if !found0 {
		t.Fatalf("expected slab 0 to be selected, got %+v", cands)
	}
}
