package treedb

import (
	"fmt"
	"strconv"
	"testing"
)

func TestProfileFast_MultiDomainSyncWritesCheckpointVacuumKeepsInternalPagesPacked(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                           dir,
		Durability:                    DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog:    true,
		PreferAppendAlloc:             true,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: -1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	val := make([]byte, 64)
	const (
		stores   = 12
		versions = 120
		keys     = 48
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
			if err := b.WriteSync(); err != nil {
				t.Fatalf("writesync version=%d store=%d: %v", version, store, err)
			}
			_ = b.Close()
			if err := db.Checkpoint(); err != nil {
				t.Fatalf("checkpoint version=%d store=%d: %v", version, store, err)
			}
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
	autoVacuumRuns, err := strconv.ParseUint(db.Stats()["treedb.cache.checkpoint.auto_vacuum_runs"], 10, 64)
	if err != nil {
		t.Fatalf("parse auto vacuum runs: %v", err)
	}

	if p50 < 200_000 {
		t.Fatalf("expected internal fill p50 >= 200000 ppm, got %d (avg=%d report=%v)", p50, avg, rep)
	}
	if avg < 350_000 {
		t.Fatalf("expected internal fill avg >= 350000 ppm, got %d (p50=%d report=%v)", avg, p50, rep)
	}
	if autoVacuumRuns == 0 {
		t.Fatalf("expected checkpoint auto vacuum to run; stats=%v report=%v", db.Stats(), rep)
	}
}
