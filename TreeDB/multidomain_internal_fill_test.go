package treedb

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestProfileFast_MultiDomainWritesAvoidPageExplosionAtCheckpointBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("stress-style page explosion regression")
	}
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                           dir,
		Durability:                    DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog:    true,
		PreferAppendAlloc:             true,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: -1,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	val := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)
	const (
		stores   = 8
		versions = 64
		keys     = 32
	)

	for version := 1; version <= versions; version++ {
		for store := 0; store < stores; store++ {
			b := db.NewBatch()
			for i := 0; i < keys; i++ {
				key := []byte(fmt.Sprintf("s/k:store%02d/n/%08d/%08d", store, version, i))
				val[0] = byte(version)
				val[1] = byte(store)
				if err := b.Set(key, val); err != nil {
					t.Fatalf("set version=%d store=%d key=%d: %v", version, store, i, err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("write version=%d store=%d: %v", version, store, err)
			}
			_ = b.Close()
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatalf("checkpoint version=%d: %v", version, err)
		}
	}

	rep, err := db.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}

	parse := func(key string) uint64 {
		t.Helper()
		v, err := strconv.ParseUint(rep[key], 10, 64)
		if err != nil {
			t.Fatalf("parse %s=%q: %v", key, rep[key], err)
		}
		return v
	}

	p50 := parse("treedb.user.internal_fill_ppm_p50")
	avg := parse("treedb.user.internal_fill_ppm_avg")
	pages := parse("treedb.pages.total")
	autoVacuumRuns, err := strconv.ParseUint(db.Stats()["treedb.cache.checkpoint.auto_vacuum_runs"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto vacuum runs: %v", err)
	}

	if pages > 5_000 {
		t.Fatalf("expected pages.total <= 5000, got %d (p50=%d avg=%d report=%v)", pages, p50, avg, rep)
	}
	if autoVacuumRuns != 0 {
		t.Fatalf("expected checkpoint auto vacuum to stay disabled for outer-leaf-in-vlog fast path; stats=%v report=%v", db.Stats(), rep)
	}
}
