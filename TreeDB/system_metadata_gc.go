package treedb

import (
	"context"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

// MaxKeepRecentPublishedSystemMetaIDs is the maximum session-recent grace
// window supported by SystemMetadataAuditOptions.
const MaxKeepRecentPublishedSystemMetaIDs = treedbdb.MaxKeepRecentPublishedSystemMetaIDs

// SystemMetadataAuditOptions configures reachability auditing for dictionaries
// (dict/*) and templates (template/*) stored in SystemRootPageID.
type SystemMetadataAuditOptions = treedbdb.SystemMetadataAuditOptions

// SystemMetadataAuditReport summarizes live versus reclaimable dictionary and
// template metadata inside SystemRootPageID.
type SystemMetadataAuditReport = treedbdb.SystemMetadataAuditReport

// SystemMetadataGCOptions configures garbage collection for unreachable
// dictionaries (dict/*) and templates (template/*) inside SystemRootPageID.
type SystemMetadataGCOptions = treedbdb.SystemMetadataGCOptions

// SystemMetadataGCStats summarizes a SystemMetadataGC run.
type SystemMetadataGCStats = treedbdb.SystemMetadataGCStats

// SystemMetadataAudit reports live versus reclaimable dictionary and template
// metadata inside SystemRootPageID without mutating the database.
func (db *DB) SystemMetadataAudit(ctx context.Context, opts SystemMetadataAuditOptions) (SystemMetadataAuditReport, error) {
	var out SystemMetadataAuditReport
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if db.cached != nil {
		opts.ProtectedDictIDs = appendUniqueUint64s(opts.ProtectedDictIDs, db.cached.ActiveDictIDs())
		opts.ProtectedTemplateIDs = appendUniqueUint64s(opts.ProtectedTemplateIDs, db.cached.RecentTemplateIDs())
	}
	return db.backend.SystemMetadataAudit(ctx, opts)
}

// SystemMetadataGC deletes unreachable dictionary (dict/*) and template
// (template/*) definitions from SystemRootPageID, prunes dead template IDs
// from template/fp/* candidate lists, and evicts deleted IDs from in-memory
// caches.
func (db *DB) SystemMetadataGC(ctx context.Context, opts SystemMetadataGCOptions) (SystemMetadataGCStats, error) {
	var out SystemMetadataGCStats
	if err := db.ensureOpen(); err != nil {
		return out, err
	}
	if db.backend == nil {
		return out, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lockFullScanMaintenanceContext(ctx, &db.bgVac.runMu); err != nil {
		return out, err
	}
	defer db.bgVac.runMu.Unlock()
	if db.bgVac.deferredVectorBuildClosed.Load() || db.backend == nil {
		return out, ErrClosed
	}
	_, finishMaintenance, err := db.beginFullScanMaintenanceContext(ctx, "system-metadata-gc")
	if err != nil {
		return out, err
	}
	db.endDeferredVectorBuild()
	success := false
	defer func() { finishMaintenance(success) }()

	if db.cached != nil && !opts.DryRun {
		if err := db.Checkpoint(); err != nil {
			return out, err
		}
	}

	var stats treedbdb.SystemMetadataGCStats
	err = db.runCachedBackendMaintenance(func() error {
		runOpts := opts
		if db.cached != nil {
			runOpts.Audit.ProtectedDictIDs = appendUniqueUint64s(runOpts.Audit.ProtectedDictIDs, db.cached.ActiveDictIDs())
			runOpts.Audit.ProtectedTemplateIDs = appendUniqueUint64s(runOpts.Audit.ProtectedTemplateIDs, db.cached.RecentTemplateIDs())
		}
		var runErr error
		stats, runErr = db.backend.SystemMetadataGC(ctx, runOpts)
		return runErr
	})
	if err != nil {
		return out, err
	}
	if db.cached != nil && stats.TemplatesDeleted > 0 && len(stats.Audit.ReclaimableTemplateIDs) > 0 {
		db.cached.EvictTemplateIDs(stats.Audit.ReclaimableTemplateIDs)
	}
	success = true
	return stats, nil
}

func appendUniqueUint64s(dst, src []uint64) []uint64 {
	if len(src) == 0 {
		return dst
	}
	out := append([]uint64(nil), dst...)
	for _, id := range src {
		if id == 0 {
			continue
		}
		seen := false
		for _, existing := range out {
			if existing == id {
				seen = true
				break
			}
		}
		if !seen {
			out = append(out, id)
		}
	}
	return out
}
