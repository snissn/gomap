package caching

import (
	"context"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// runVlogGenerationLeafGC runs only inside the existing backend maintenance
// fence. The maintained collector owns all reachability, recoverable-root and
// physical-identity deletion authority, including immutable manifest revisions.
func (db *DB) runVlogGenerationLeafGC(ctx context.Context) (backenddb.LeafGenerationGCStats, bool, error) {
	if !db.indexOuterLeavesInValueLog {
		return backenddb.LeafGenerationGCStats{}, false, nil
	}
	runner, ok := db.backend.(backendLeafGenerationGCRunner)
	if !ok {
		return backenddb.LeafGenerationGCStats{}, false, nil
	}
	start := time.Now()
	stats, err := runner.LeafGenerationGC(ctx, db.leafGenerationGCOptions())
	db.vlogGenerationLeafGCExecNanos.Add(uint64(time.Since(start)))
	db.vlogGenerationLeafGCRuns.Add(1)
	if stats.GenerationsDeleted > 0 {
		db.vlogGenerationLeafGCDeletedGenerations.Add(uint64(stats.GenerationsDeleted))
	}
	if stats.FilesDeleted > 0 {
		db.vlogGenerationLeafGCDeletedFiles.Add(uint64(stats.FilesDeleted))
	}
	if stats.BytesDeleted > 0 {
		db.vlogGenerationLeafGCDeletedBytes.Add(uint64(stats.BytesDeleted))
	}
	if stats.ManifestRevisionsDeleted > 0 {
		db.vlogGenerationLeafGCDeletedManifestRevisions.Add(uint64(stats.ManifestRevisionsDeleted))
	}
	if stats.ManifestRevisionBytesDeleted > 0 {
		db.vlogGenerationLeafGCDeletedManifestBytes.Add(uint64(stats.ManifestRevisionBytesDeleted))
	}
	if err != nil {
		db.vlogGenerationLeafGCErrors.Add(1)
	}
	return stats, true, err
}
