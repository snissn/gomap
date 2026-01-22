package db

import (
	"bytes"
	"runtime"
	"strconv"
	"testing"
)

func TestCompactIndexImprovesSpanLocality(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir, KeepRecent: 1, PreferAppendAlloc: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d.Close()

	val := bytes.Repeat([]byte("x"), 16)
	keys := 4000
	if runtime.GOOS == "windows" {
		keys = 1200
	}
	for i := 0; i < keys; i++ {
		k := []byte{byte(i >> 8), byte(i)}
		if err := d.SetSync(k, val); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	if err := d.CompactIndex(); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Advance commit seq enough for KeepRecent=1 pruning to kick in for the
	// compaction-retired pages.
	if err := d.SetSync([]byte{0xFF, 0xFE}, val); err != nil {
		t.Fatalf("set1: %v", err)
	}
	if err := d.SetSync([]byte{0xFF, 0xFD}, val); err != nil {
		t.Fatalf("set2: %v", err)
	}

	rep, err := d.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	ratioStr := rep["treedb.user.pages.span_ratio_ppm"]
	if ratioStr == "" {
		t.Fatalf("missing span_ratio_ppm in report")
	}
	ratio, err := strconv.ParseUint(ratioStr, 10, 64)
	if err != nil {
		t.Fatalf("parse span_ratio_ppm: %v", err)
	}

	// A vacuum rebuild should allocate pages essentially contiguously (span ~= pages).
	// Allow some tolerance for minor bookkeeping differences.
	if ratio > 1_200_000 {
		t.Fatalf("expected good locality after vacuum, span_ratio_ppm=%d", ratio)
	}
}
