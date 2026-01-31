package caching

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCachedSeed_EmptyishCheckpointBuildsDenseTree(t *testing.T) {
	const keys = 20000
	val := bytes.Repeat([]byte("a"), 128)

	dir := t.TempDir()
	backend, err := db.Open(db.Options{
		Dir:               dir,
		PreferAppendAlloc: false,
		KeepRecent:        1,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	cached, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cached open: %v", err)
	}
	defer func() {
		_ = cached.Close()
		_ = backend.Close()
	}()

	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	rep, err := backend.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}

	leafAvg, err := strconv.Atoi(rep["treedb.user.leaf_fill_ppm_avg"])
	if err != nil {
		t.Fatalf("parse leaf_fill_ppm_avg: %v", err)
	}
	userPages, err := strconv.Atoi(rep["treedb.user.pages"])
	if err != nil {
		t.Fatalf("parse user.pages: %v", err)
	}

	if leafAvg < 800000 {
		t.Fatalf("leaf fill avg too low: got=%d want>=%d", leafAvg, 800000)
	}
	// Absolute bound: dense build should stay close to vacuum-like packing.
	// This is intentionally strict to catch regressions.
	if userPages > 900 {
		t.Fatalf("too many reachable user pages after seed+checkpoint: got=%d want<=%d", userPages, 900)
	}
}
