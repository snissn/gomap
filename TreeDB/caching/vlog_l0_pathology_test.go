package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func countValueLogLaneSegments(t *testing.T, dir string, lanePrefix string) int {
	t.Helper()

	walDir := filepath.Join(dir, "wal")
	ents, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("readdir wal: %v", err)
	}
	count := 0
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !bytes.HasPrefix([]byte(name), []byte("value-"+lanePrefix+"-")) || filepath.Ext(name) != ".log" {
			continue
		}
		info, err := ent.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if info.Size() <= 0 {
			continue
		}
		count++
	}
	return count
}

func touchValueLogSegmentsOld(t *testing.T, dir string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "wal", "value-l*.log"))
	if err != nil {
		t.Fatalf("glob wal value logs: %v", err)
	}
	old := time.Now().Add(-10 * time.Minute)
	for _, path := range paths {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatalf("chtimes %s: %v", path, err)
		}
	}
}

func TestVlogGenerationMaintenance_ReclaimsL0Segments(t *testing.T) {
	disableVlogGenerationLoop(t)
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	db, err := Open(dir, backend, Options{
		AllowUnsafe:                             true,
		DisableWAL:                              true,
		JournalLanes:                            1,
		MemtableShards:                          1,
		ForceValueLogPointers:                   true,
		ValueLogPointerThreshold:                1,
		ValueLogGenerationPolicy:                uint8(backenddb.ValueLogGenerationHotWarmCold),
		ValueLogCompression:                     1, // off; keep sizes deterministic
		ValueLogMaxSegmentBytes:                 1 << 20,
		ValueLogGenerationHotSegmentTargetBytes: 64 << 10,
		// Trigger rewrite by retained bytes; the test workload creates stale bytes
		// in many closed l0 segments and we assert the maintenance cycle reclaims at
		// least some of them on disk.
		ValueLogRewriteTriggerTotalBytes:   1,
		ValueLogRewriteTriggerChurnPerSec:  1 << 62,
		ValueLogRewriteBudgetBytesPerSec:   1 << 62,
		ValueLogRewriteBudgetRecordsPerSec: 256,
	})
	if err != nil {
		t.Fatalf("open cachingdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.testSkipVlogCheckpointKick = true
	skipRetainedPrune(db)

	// Build many closed l0 segments with some stale bytes each, but not enough to
	// keep rotation cheap while still creating real stale bytes. We rotate
	// explicitly to keep overwrites co-located with the original writes, ensuring
	// each closed segment has both live and stale bytes.
	const (
		segments   = 8
		liveKeys   = 24
		staleKeys  = 8
		valueBytes = 2 << 10
	)

	for seg := 0; seg < segments; seg++ {
		for i := 0; i < liveKeys; i++ {
			k := []byte("k")
			k = append(k, byte(seg>>8), byte(seg), byte(i))
			v := bytes.Repeat([]byte("v"), valueBytes)
			v[0] = byte(seg)
			v[1] = byte(i)
			if err := db.Set(k, v); err != nil {
				t.Fatalf("set seg=%d i=%d: %v", seg, i, err)
			}
		}

		for i := 0; i < staleKeys; i++ {
			k := []byte("k")
			k = append(k, byte(seg>>8), byte(seg), byte(i))
			v := bytes.Repeat([]byte("w"), valueBytes)
			v[0] = 0xFF
			v[1] = byte(i)
			if err := db.Set(k, v); err != nil {
				t.Fatalf("overwrite seg=%d i=%d: %v", seg, i, err)
			}
		}

		if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
			t.Fatalf("rotate vlog seg=%d: %v", seg, err)
		}
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	touchValueLogSegmentsOld(t, dir)

	beforeL0 := countValueLogLaneSegments(t, dir, "l0")
	if beforeL0 < 4 {
		t.Fatalf("expected multiple l0 segments before maintenance, got=%d", beforeL0)
	}

	db.vlogGenerationRewriteBudgetTokensBytes.Store(1 << 20)
	forceVlogMaintenanceIdle(db)

	// Run a few maintenance cycles. Each cycle is bounded (queue chunking), so we
	// expect at least one closed l0 segment to be reclaimed on disk.
	for i := 0; i < 2; i++ {
		forceVlogMaintenanceIdle(db)
		db.maybeRunVlogGenerationMaintenance(false)
	}

	if db.vlogGenerationRewriteRuns.Load() == 0 {
		t.Fatalf("expected maintenance to run at least one rewrite cycle; state=%d reason=%d", db.vlogGenerationSchedulerState.Load(), db.vlogGenerationLastReason.Load())
	}

	afterL0 := countValueLogLaneSegments(t, dir, "l0")
	if afterL0 >= beforeL0 {
		t.Fatalf("expected maintenance to reclaim l0 segments; before=%d after=%d", beforeL0, afterL0)
	}
}
