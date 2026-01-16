package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

	valA := bytes.Repeat([]byte("A"), 300) // pointer
	valC := bytes.Repeat([]byte("C"), 300) // pointer
	if err := d.Set([]byte("A"), valA); err != nil {
		t.Fatalf("Set(A): %v", err)
	}
	if err := d.Set([]byte("C"), valC); err != nil {
		t.Fatalf("Set(C): %v", err)
	}

	// Rotate to make slab 0 eligible for compaction.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected active slab != 0 after rotation")
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

func TestCompaction_LiveSetSkipsTreeLookups(t *testing.T) {
	dir := t.TempDir()
	opts := db.Options{Dir: dir}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("V"), 300)
	for i := 0; i < 200; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		if err := d.Set(k, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected active slab != 0 after rotation")
	}

	stats := Stats{}
	c := New(d)
	if err := c.CompactSlabWithOptions(0, Options{
		MicroBatchSize:    16,
		LiveSetMaxEntries: 1000,
		Stats:             &stats,
	}); err != nil {
		t.Fatalf("CompactSlab: %v", err)
	}

	if stats.LiveSetAborted {
		t.Fatalf("expected live set to complete (aborted)")
	}
	if stats.LiveSetEntries == 0 {
		t.Fatalf("expected live set entries")
	}
	if stats.TreeLookups != 0 {
		t.Fatalf("expected tree lookups to be skipped, got %d", stats.TreeLookups)
	}
	if stats.TreeLookupsSkipped == 0 {
		t.Fatalf("expected skipped lookups")
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

func TestCompaction_WriterLatencyBudget(t *testing.T) {
	dir := t.TempDir()
	opts := db.Options{Dir: dir}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Populate slab 0 with enough pointer values that compaction takes multiple
	// micro-batches to apply.
	val := bytes.Repeat([]byte("V"), 300)
	for i := 0; i < 2_000; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		if err := d.Set(k, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// Make slab 1 active so slab 0 is eligible for compaction.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	compactDone := make(chan error, 1)
	go func() {
		c := New(d)
		compactDone <- c.CompactSlabWithOptions(0, Options{MicroBatchSize: 16})
	}()

	writeDone := make(chan error, 1)
	go func() {
		otherVal := bytes.Repeat([]byte("W"), 300)
		const perOpMax = 1 * time.Second
		for i := 0; i < 1_000; i++ {
			start := time.Now()
			k := []byte{0xFF, byte(i >> 8), byte(i)}
			if err := d.Set(k, otherVal); err != nil {
				writeDone <- err
				return
			}
			if dur := time.Since(start); dur > perOpMax {
				writeDone <- fmt.Errorf("write latency budget exceeded: dur=%s cap=%s", dur, perOpMax)
				return
			}
		}
		writeDone <- nil
	}()

	timeout := 15 * time.Second

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

func TestCompaction_IndexSwap_PreservesData(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("index-swap compaction not supported on windows")
	}
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

	valA := bytes.Repeat([]byte("A"), 300) // pointer
	valC := bytes.Repeat([]byte("C"), 300) // pointer
	if err := d.Set([]byte("A"), valA); err != nil {
		t.Fatalf("Set(A): %v", err)
	}
	if err := d.Set([]byte("C"), valC); err != nil {
		t.Fatalf("Set(C): %v", err)
	}

	// Rotate to make slab 0 eligible.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected slab rotation, active slab still %d", got)
	}

	c := New(d)
	if err := c.CompactSlabWithOptions(0, Options{IndexSwap: true}); err != nil {
		t.Fatalf("CompactSlab(IndexSwap) failed: %v", err)
	}

	got, err := d.Get([]byte("A"))
	if err != nil {
		t.Fatalf("Get(A): %v", err)
	}
	if !bytes.Equal(got, valA) {
		t.Fatalf("A mismatch after compaction")
	}
	got, err = d.Get([]byte("C"))
	if err != nil {
		t.Fatalf("Get(C): %v", err)
	}
	if !bytes.Equal(got, valC) {
		t.Fatalf("C mismatch after compaction")
	}
}

func TestCompaction_IndexSwap_SnapshotKeepsOldSlabPinnedUntilClosed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("index-swap compaction not supported on windows")
	}
	dir := t.TempDir()
	opts := db.Options{Dir: dir}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = 1 << 20
	t.Cleanup(func() { slab.MaxSlabSize = oldMax })

	valA := bytes.Repeat([]byte("A"), 300) // pointer
	valC := bytes.Repeat([]byte("C"), 300) // pointer
	if err := d.Set([]byte("A"), valA); err != nil {
		t.Fatalf("Set(A): %v", err)
	}
	if err := d.Set([]byte("C"), valC); err != nil {
		t.Fatalf("Set(C): %v", err)
	}

	// Rotate to make slab 0 eligible.
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got == 0 {
		t.Fatalf("expected active slab != 0 after rotation")
	}

	slab0Path := filepath.Join(dir, "data-0000.slab")
	if _, err := os.Stat(slab0Path); err != nil {
		t.Fatalf("expected slab0 to exist: %v", err)
	}

	// Pin slab0 via an external snapshot.
	snap := d.AcquireSnapshot()

	c := New(d)
	if err := c.CompactSlabWithOptions(0, Options{IndexSwap: true}); err != nil {
		_ = snap.Close()
		t.Fatalf("CompactSlab(IndexSwap): %v", err)
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

	// With no snapshots pinned, slab0 should be deleted.
	if _, err := os.Stat(slab0Path); err == nil {
		t.Fatalf("expected slab0 to be deleted after snapshots close")
	} else if !os.IsNotExist(err) {
		t.Fatalf("unexpected stat error: %v", err)
	}
}

func TestCompaction_IndexSwap_AllowsConcurrentWrites(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("index-swap compaction not supported on windows")
	}
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
		compactDone <- c.CompactSlabWithOptions(0, Options{IndexSwap: true})
	}()

	timeout := 15 * time.Second

	select {
	case err := <-compactDone:
		if err != nil {
			t.Fatalf("CompactSlab(IndexSwap): %v", err)
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

	// Sanity: concurrent writer keys should remain readable.
	got, err := d.Get([]byte{0xFF, 0x10})
	if err != nil {
		t.Fatalf("Get(writer key): %v", err)
	}
	if len(got) == 0 {
		t.Fatalf("expected non-empty writer value")
	}
}

func TestCompaction_IndexSwap_CompactsMultipleCandidatesWithSingleIndexSwapCommit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("index-swap compaction not supported on windows")
	}
	dir := t.TempDir()
	d, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	oldMax := slab.MaxSlabSize
	slab.MaxSlabSize = 1 << 20
	t.Cleanup(func() { slab.MaxSlabSize = oldMax })

	val := bytes.Repeat([]byte("V"), 300)

	// Populate slab 0.
	for i := 0; i < 50; i++ {
		k := []byte{0x00, byte(i)}
		if err := d.Set(k, val); err != nil {
			t.Fatalf("Set slab0: %v", err)
		}
	}
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate to slab1: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got != 1 {
		t.Fatalf("expected active slab 1, got %d", got)
	}

	// Populate slab 1.
	for i := 0; i < 50; i++ {
		k := []byte{0x01, byte(i)}
		if err := d.Set(k, val); err != nil {
			t.Fatalf("Set slab1: %v", err)
		}
	}
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate to slab2: %v", err)
	}
	if got := d.SlabManager().ActiveSlabID(); got != 2 {
		t.Fatalf("expected active slab 2, got %d", got)
	}

	// Overwrite both key sets to create dead bytes in slabs 0 and 1.
	other := bytes.Repeat([]byte("W"), 300)
	for i := 0; i < 50; i++ {
		k := []byte{0x00, byte(i)}
		if err := d.Set(k, other); err != nil {
			t.Fatalf("Overwrite slab0 key: %v", err)
		}
	}
	for i := 0; i < 50; i++ {
		k := []byte{0x01, byte(i)}
		if err := d.Set(k, other); err != nil {
			t.Fatalf("Overwrite slab1 key: %v", err)
		}
	}

	c := New(d)
	cands, err := c.Candidates(Options{
		DeadRatioThreshold: 0,
		MinTotalBytes:      1,
		MaxSlabs:           2,
	})
	if err != nil {
		t.Fatalf("Candidates: %v", err)
	}
	if len(cands) < 2 {
		t.Fatalf("expected at least 2 candidates, got %d", len(cands))
	}

	beforeSeq := d.State().CommitSeq
	if err := c.CompactCandidatesWithContext(context.Background(), Options{
		DeadRatioThreshold: 0,
		MinTotalBytes:      1,
		MaxSlabs:           2,
		IndexSwap:          true,
	}); err != nil {
		t.Fatalf("CompactCandidates(IndexSwap): %v", err)
	}
	afterSeq := d.State().CommitSeq
	if afterSeq != beforeSeq+1 {
		t.Fatalf("expected a single index-swap commit: before=%d after=%d", beforeSeq, afterSeq)
	}

	// Best-effort: compacted slabs should be removed once no snapshots pin them.
	for _, name := range []string{"data-0000.slab", "data-0001.slab"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			t.Fatalf("expected %s to be deleted", name)
		} else if !os.IsNotExist(err) {
			t.Fatalf("unexpected stat error for %s: %v", name, err)
		}
	}
}

func TestCompaction_FullCompression(t *testing.T) {
	dir := t.TempDir()

	opts := db.Options{
		Dir: dir,
		SlabCompression: slab.CompressionOptions{
			Kind:            slab.CompressionZSTD,
			MinBytes:        1,
			MinSavingsBytes: 0,
		},
		AllowUnsafe: true,
	}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer d.Close()

	// Write highly compressible data to force full compression
	key := []byte("key_with_significant_length_for_compression_testing_abc_def_ghi")
	val := bytes.Repeat([]byte("redundant_data_redundant_data_redundant_data_"), 20)

	slabID := d.SlabManager().ActiveSlabID()

	batch := d.NewBatch().(*db.Batch)
	batch.Set(key, val)
	if err := batch.Write(); err != nil {
		t.Fatalf("batch.Write: %v", err)
	}

	// Rotate slab to allow compaction
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Run compaction
	c := New(d)
	if err := c.CompactSlab(slabID); err != nil {
		t.Fatalf("CompactSlab: %v", err)
	}

	// Verify data still readable
	snap := d.AcquireSnapshot()
	got, err := snap.Get(key)
	snap.Close()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("data mismatch")
	}
}

func TestCompaction_OmitKeys(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("CompactSlabsIndexSwap unsupported on windows")
	}
	dir := t.TempDir()

	opts := db.Options{
		Dir:                dir,
		OmitSlabKeys:       true,
		ForceValuePointers: true,
		AllowUnsafe:        true,
	}
	d, err := db.Open(opts)
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer d.Close()

	key := []byte("k")
	val := []byte("v")

	slabID := d.SlabManager().ActiveSlabID()

	batch := d.NewBatch().(*db.Batch)
	batch.Set(key, val)
	if err := batch.Write(); err != nil {
		t.Fatalf("batch.Write: %v", err)
	}

	// Rotate slab to allow compaction
	if _, err := d.SlabManager().Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	// Run IndexSwap compaction (should MIGRATE the record even if key is missing in slab)
	if err := d.CompactSlabsIndexSwap(nil, []uint32{slabID}, db.IndexSwapCompactionOptions{}); err != nil {
		t.Fatalf("CompactSlabsIndexSwap: %v", err)
	}

	// Verify data still readable (now from new slab)
	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("data mismatch")
	}
}
