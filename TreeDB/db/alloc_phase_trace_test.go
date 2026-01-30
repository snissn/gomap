package db

import (
	"bytes"
	"strconv"
	"testing"
)

func TestBackendPhaseAllocTimeline(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	const keys = 20000
	valA := bytes.Repeat([]byte("a"), 128)
	valB := bytes.Repeat([]byte("b"), 128)

	record := func(phase string) {
		stats, err := d.FragmentationReport()
		if err != nil {
			t.Fatalf("FragmentationReport: %v", err)
		}
		reclaimable := parseReportUintReuseDB(t, stats, "treedb.freelist.reclaimable_pages")
		statMap := d.Stats()
		freelist := parseReportUintReuseDB(t, statMap, "treedb.alloc.freelist")
		appendAlloc := parseReportUintReuseDB(t, statMap, "treedb.alloc.append")
		pages := d.Pager().PageCount()
		t.Logf("phase=%s pages=%d reclaimable=%d alloc.freelist=%d alloc.append=%d", phase, pages, reclaimable, freelist, appendAlloc)
	}

	// Phase 1: batch write
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
	record("batch_write")

	// Phase 2: random write
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valB); err != nil {
				t.Fatalf("overwrite: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("overwrite write: %v", err)
		}
		_ = b.Close()
	}
	record("random_write")

	// Phase 3: batch delete
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Delete(k); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("delete write: %v", err)
		}
		_ = b.Close()
		d.Prune()
	}
	record("batch_delete")

	// Phase 4: rewrite
	{
		b := d.NewBatch().(*Batch)
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, valA); err != nil {
				t.Fatalf("rewrite: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("rewrite write: %v", err)
		}
		_ = b.Close()
		d.Prune()
	}
	record("rewrite")
}

func parseReportUintReuseDB(t *testing.T, rep map[string]string, key string) uint64 {
	t.Helper()
	val, ok := rep[key]
	if !ok {
		t.Fatalf("missing report key %q", key)
	}
	out, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		t.Fatalf("parse report %q: %v", key, err)
	}
	return out
}
