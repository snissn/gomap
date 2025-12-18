package compaction

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestCompaction_SnapshotKeepsOldSlabPinnedUntilClosed(t *testing.T) {
	dir := t.TempDir()
	opts := db.Options{Dir: dir}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = 700
	t.Cleanup(func() { slab.MaxSlabSize = oldMax })

	valA := bytes.Repeat([]byte("A"), 300) // pointer
	valC := bytes.Repeat([]byte("C"), 300) // pointer
	if err := d.Set([]byte("A"), valA); err != nil {
		t.Fatalf("Set(A): %v", err)
	}
	if err := d.Set([]byte("C"), valC); err != nil {
		t.Fatalf("Set(C): %v", err)
	}

	// Trigger rotation: ensure slab 1 becomes active.
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

	slab0Path := filepath.Join(dir, "data-0000.slab")
	if _, err := os.Stat(slab0Path); err != nil {
		t.Fatalf("expected slab0 to exist: %v", err)
	}

	// Pin slab0 via an external snapshot.
	snap := d.AcquireSnapshot()

	c := New(d)
	if err := c.CompactSlab(0); err != nil {
		_ = snap.Close()
		t.Fatalf("CompactSlab(0): %v", err)
	}

	// While the snapshot is open, the old slab must remain available.
	if _, err := os.Stat(slab0Path); err != nil {
		_ = snap.Close()
		t.Fatalf("expected slab0 to remain until snapshot closes: %v", err)
	}
	val, err := snap.Get([]byte("A"))
	if err != nil {
		_ = snap.Close()
		t.Fatalf("snapshot Get(A): %v", err)
	}
	if !bytes.Equal(val, valA) {
		_ = snap.Close()
		t.Fatalf("snapshot A mismatch")
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("snapshot Close: %v", err)
	}

	// With no snapshots pinned, slab0 should be deleted after being marked zombie
	// and refreshing the DB's slab set.
	if _, err := os.Stat(slab0Path); err == nil {
		t.Fatalf("expected slab0 to be deleted after snapshots close")
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected stat error: %v", err)
	}

	// Current DB state should still be readable.
	val, err = d.Get([]byte("A"))
	if err != nil {
		t.Fatalf("Get(A): %v", err)
	}
	if !bytes.Equal(val, valA) {
		t.Fatalf("A mismatch after compaction")
	}
}

func TestCompaction_AllowsConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	opts := db.Options{Dir: dir}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Populate slab 0 with many live pointer values.
	val := bytes.Repeat([]byte("V"), 300)
	for i := 0; i < 200; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		if err := d.Set(k, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// Make slab 1 active so slab 0 is eligible for compaction.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected active slab != 0 after rotation")
	}

	writeDone := make(chan error, 1)
	go func() {
		otherVal := bytes.Repeat([]byte("W"), 300)
		for i := 0; i < 200; i++ {
			k := []byte{0xFF, byte(i)}
			if err := d.Set(k, otherVal); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- nil
	}()

	compactDone := make(chan error, 1)
	go func() {
		c := New(d)
		compactDone <- c.CompactSlabWithOptions(0, Options{MicroBatchSize: 1})
	}()

	timeout := 5 * time.Second

	select {
	case err := <-compactDone:
		if err != nil {
			t.Fatalf("CompactSlab: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("compaction timed out (possible deadlock)")
	}

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("writer failed: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("writer timed out (possible starvation/deadlock)")
	}
}
