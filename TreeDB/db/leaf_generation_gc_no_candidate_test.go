package db

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Exercise the public collector: a currently open producer is sufficient to
// rule out segment retirement, but never sufficient to skip revision GC.
func TestLeafGenerationGC_CurrentWritersSkipSegmentScan(t *testing.T) {
	for _, dryRun := range []bool{false, true} {
		name := "apply"
		if dryRun {
			name = "inspect"
		}
		t.Run(name, func(t *testing.T) {
			db, leafLog := openLeafGenerationGCTestDB(t)
			writeLeafGenerationKeys(t, db, "current", 64, 'a')
			path, _ := currentLeafSegmentOrFatal(t, leafLog)
			var held *LeafGenerationManifestStablePreparedClosure
			if rootpublication.StableRelativeNamespaceSupported() {
				var err error
				held, err = db.PrepareLeafGenerationManifestStableClosure()
				if err != nil {
					t.Fatal(err)
				}
				defer held.Release()
				for range 4 {
					closure, err := db.PrepareLeafGenerationManifestStableClosure()
					if err != nil {
						t.Fatal(err)
					}
					closure.Release()
				}
			}
			var scans atomic.Int64
			restore := registerLeafGenerationLiveScanHook(func() { scans.Add(1) })
			defer restore()
			stats, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: dryRun})
			if err != nil {
				t.Fatal(err)
			}
			if scans.Load() != 0 {
				t.Fatalf("all-current segment phase performed %d reachability scans", scans.Load())
			}
			if stats.GenerationsTotal == 0 || stats.GenerationsWritable+stats.GenerationsLive != stats.GenerationsTotal || stats.GenerationsEligible != 0 || stats.GenerationsDeleted != 0 || stats.FilesDeleted != 0 {
				t.Fatalf("all-current segment census: %+v", stats)
			}
			if held != nil {
				if stats.ManifestRevisionsEligible == 0 || (!dryRun && stats.ManifestRevisionsDeleted == 0) || (dryRun && stats.ManifestRevisionsDeleted != 0) {
					t.Fatalf("segment shortcut skipped independent revision collection: %+v", stats)
				}
				if _, err := os.Stat(filepath.Join(LeafLogDirPath(db.dir), leafGenerationDurableManifestFileName(held.Revision()))); err != nil {
					t.Fatalf("revision held across segment shortcut lost: %v", err)
				}
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("current producer removed: %v", err)
			}
			expectLeafGenerationValue(t, db, leafGenerationKey("current", 0), 'a')

			// A real rotation makes the old producer noncurrent. Its retained
			// DATA must now be checked by the original fresh reachability path.
			if err := leafLog.rotateLeaf(); err != nil {
				t.Fatal(err)
			}
			writeLeafGenerationKeys(t, db, "next", 1, 'b')
			if _, err := db.LeafGenerationGC(context.Background(), LeafGenerationGCOptions{DryRun: true}); err != nil {
				t.Fatal(err)
			}
			if scans.Load() == 0 {
				t.Fatal("rotated sealed generation did not take full scan path")
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("live rotated producer removed: %v", err)
			}
			expectLeafGenerationValue(t, db, leafGenerationKey("current", 63), 'a')
		})
	}
}
