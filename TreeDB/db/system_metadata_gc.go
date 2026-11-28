package db

import (
	"context"
	"encoding/binary"
	"errors"
	"sort"
)

// SystemMetadataGCOptions configures destructive garbage collection for
// unreachable dictionaries (dict/*) and templates (template/*) stored in
// SystemRootPageID.
type SystemMetadataGCOptions struct {
	// DryRun computes the full audit plan without mutating SystemRootPageID.
	DryRun bool
	// Audit configures reachability grace windows and explicit caller pins.
	Audit SystemMetadataAuditOptions
	// SkipSync commits the SystemRootPageID delete batch without forcing an
	// immediate pager fsync (false = durable sync commit, the default).
	SkipSync bool
}

// SystemMetadataGCStats summarizes a SystemMetadataGC execution.
type SystemMetadataGCStats struct {
	Audit                 SystemMetadataAuditReport
	DryRun                bool
	DictsDeleted          int
	DictKeysDeleted       int
	TemplatesDeleted      int
	TemplateKeysDeleted   int
	TemplateFPKeysUpdated int
	TemplateFPKeysDeleted int
	PointerEntriesDeleted int
	OpsCommitted          int
	AbortedStale          bool
}

// SystemMetadataGC deletes unreachable dictionary (dict/*) and template
// (template/*) metadata from SystemRootPageID, prunes dead template IDs from
// template/fp/* candidate lists, and evicts deleted IDs from in-memory caches.
//
// Safety invariants:
//   - Any dictID or templateID referenced by any on-disk ordinary value_vlog or
//     leaf_vlog segment is preserved.
//   - Any dictID targeted by dict/current or dict/current/*, queued in
//     stagedSystemOps, listed in ProtectedDictIDs, or within the session-recent
//     grace window is preserved.
//   - The SystemRootPageID delete batch commits atomically only if CommitSeq
//     has not advanced since the audit scan and no candidate ID became staged;
//     otherwise it retries once and fails closed (AbortedStale=true) with zero
//     deletions.
//   - Deleting pointer-backed entries updates valueLogRefTracker so a
//     subsequent ValueLogGC pass can reclaim now-unreferenced lane-254
//     value-log segments.
func (db *DB) SystemMetadataGC(ctx context.Context, opts SystemMetadataGCOptions) (SystemMetadataGCStats, error) {
	return db.systemMetadataGC(ctx, opts, true, true)
}

func (db *DB) systemMetadataGC(ctx context.Context, opts SystemMetadataGCOptions, lockMaintenance bool, useMutationFence bool) (SystemMetadataGCStats, error) {
	var stats SystemMetadataGCStats
	stats.DryRun = opts.DryRun
	if db == nil {
		return stats, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if db.readOnly && !opts.DryRun {
		return stats, ErrReadOnly
	}
	if !opts.DryRun {
		if err := db.commandWALPoisonedError(); err != nil {
			return stats, err
		}
	}

	const maxStaleRetries = 1
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		var (
			plan *systemMetadataAuditPlan
			err  error
		)
		if lockMaintenance {
			db.maintenanceMu.Lock()
		}
		if !opts.DryRun {
			err = db.CheckStorageMaintenanceReady()
		}
		if err == nil {
			plan, err = db.buildSystemMetadataAuditPlanWithMaintenanceLockHeld(ctx, opts.Audit, !opts.DryRun)
		}
		if lockMaintenance {
			db.maintenanceMu.Unlock()
		}
		if err != nil {
			if errors.Is(err, ErrRecoverableRootSetStale) || errors.Is(err, errDurableRootCandidateStale) {
				if attempt == maxStaleRetries {
					stats.AbortedStale = true
					return stats, nil
				}
				continue
			}
			return stats, err
		}

		stats.Audit = plan.report
		if opts.DryRun || (len(plan.deadDicts) == 0 && len(plan.deadTpls) == 0) {
			return stats, nil
		}

		ops, built := buildSystemMetadataGCDeleteOps(plan)
		if len(ops) == 0 {
			return stats, nil
		}

		// Re-verify inside db.writeMu that no candidate ID became staged between
		// the audit scan and commit publication.
		preCommitCheck := func() bool {
			stagedDicts, stagedTpls := db.snapshotStagedSystemMetadataIDs()
			for id := range stagedDicts {
				if _, dead := plan.deadDicts[id]; dead {
					return false
				}
			}
			for id := range stagedTpls {
				if _, dead := plan.deadTpls[id]; dead {
					return false
				}
			}
			return true
		}

		var finish func() error
		if useMutationFence {
			if begin := db.systemRootMutationFenceFn(); begin != nil {
				var fenceErr error
				finish, fenceErr = begin()
				if fenceErr != nil {
					return stats, fenceErr
				}
			}
		}
		// Hold systemMetaPublishMu exclusively across staged-op re-verification,
		// in-memory SystemRootPageID activation, and cache eviction so
		// StageSystemOps cannot queue an activation marker referencing a definition
		// being deleted here. Eviction and unlock happen inside onAccepted right
		// after activation and before waiting on physical root publication, so
		// synchronous durability callbacks dispatched by the publication coordinator
		// or postWork cannot deadlock if they call StageSystemOps.
		db.systemMetaPublishMu.Lock()
		publishLocked := true
		onAccepted := func() {
			db.EvictSystemDictCache(plan.report.ReclaimableDictIDs)
			db.EvictSystemTemplateCache(plan.report.ReclaimableTemplateIDs)
			db.forgetRecentSystemMetadataIDs(plan.report.ReclaimableDictIDs, plan.report.ReclaimableTemplateIDs)
			if publishLocked {
				db.systemMetaPublishMu.Unlock()
				publishLocked = false
			}
		}
		stale, postWork, commitErr := db.applySystemOpsDirectIfCommitSeq(ops, plan.scannedSeq, plan.scannedSystemRoot, !opts.SkipSync, preCommitCheck, onAccepted)
		if publishLocked {
			db.systemMetaPublishMu.Unlock()
			publishLocked = false
		}
		if finish != nil {
			commitErr = errors.Join(commitErr, finish())
		}
		if postWork != nil {
			postWork()
		}
		if commitErr != nil {
			if errors.Is(commitErr, ErrRecoverableRootSetStale) || errors.Is(commitErr, errDurableRootCandidateStale) {
				stale = true
				commitErr = nil
			} else {
				return stats, commitErr
			}
		}
		if stale {
			if attempt == maxStaleRetries {
				stats.AbortedStale = true
				return stats, nil
			}
			continue
		}

		stats.DictsDeleted = len(plan.report.ReclaimableDictIDs)
		stats.DictKeysDeleted = built.dictKeysDeleted
		stats.TemplatesDeleted = len(plan.report.ReclaimableTemplateIDs)
		stats.TemplateKeysDeleted = built.templateKeysDeleted
		stats.TemplateFPKeysUpdated = built.fpKeysUpdated
		stats.TemplateFPKeysDeleted = built.fpKeysDeleted
		stats.PointerEntriesDeleted = built.pointerEntriesDeleted
		stats.OpsCommitted = len(ops)
		return stats, nil
	}
}

type systemMetadataGCBuiltOps struct {
	dictKeysDeleted       int
	templateKeysDeleted   int
	fpKeysUpdated         int
	fpKeysDeleted         int
	pointerEntriesDeleted int
}

func buildSystemMetadataGCDeleteOps(plan *systemMetadataAuditPlan) ([]SystemKVOp, systemMetadataGCBuiltOps) {
	var built systemMetadataGCBuiltOps
	if plan == nil || plan.inv == nil {
		return nil, built
	}
	ops := make([]SystemKVOp, 0, plan.report.DictKeysReclaimable+plan.report.TemplateKeysReclaimable+plan.report.TemplateFPKeysPrunable)

	for _, id := range plan.report.ReclaimableDictIDs {
		entry := plan.inv.dicts[id]
		if entry == nil {
			continue
		}
		if entry.hasBytesKey {
			ops = append(ops, SystemKVOp{Key: SystemDictBytesKey(id), Delete: true})
			built.dictKeysDeleted++
			if entry.pointerSegID != 0 {
				built.pointerEntriesDeleted++
			}
		}
		for _, ck := range entry.chunkKeys {
			ops = append(ops, SystemKVOp{Key: ck, Delete: true})
			built.dictKeysDeleted++
		}
		if entry.hasK {
			ops = append(ops, SystemKVOp{Key: systemDictKKey(id), Delete: true})
			built.dictKeysDeleted++
		}
		if entry.hasLeafMode {
			ops = append(ops, SystemKVOp{Key: systemDictLeafModeKey(id), Delete: true})
			built.dictKeysDeleted++
		}
		for _, hk := range entry.hashKeys {
			ops = append(ops, SystemKVOp{Key: hk, Delete: true})
			built.dictKeysDeleted++
		}
	}

	for _, id := range plan.report.ReclaimableTemplateIDs {
		entry := plan.inv.templates[id]
		if entry == nil {
			continue
		}
		if entry.hasDefKey {
			ops = append(ops, SystemKVOp{Key: SystemTemplateDefKey(id), Delete: true})
			built.templateKeysDeleted++
			if entry.pointerSegID != 0 {
				built.pointerEntriesDeleted++
			}
		}
		for _, ck := range entry.chunkKeys {
			ops = append(ops, SystemKVOp{Key: ck, Delete: true})
			built.templateKeysDeleted++
		}
	}

	if len(plan.deadTpls) > 0 {
		for _, fp := range plan.inv.templateFPs {
			if len(fp.candidates) == 0 {
				continue
			}
			kept := make([]systemTemplateFPCandidate, 0, len(fp.candidates))
			dropped := 0
			for _, c := range fp.candidates {
				if _, dead := plan.deadTpls[c.id]; dead {
					dropped++
					continue
				}
				kept = append(kept, c)
			}
			if dropped == 0 {
				continue
			}
			if len(kept) == 0 {
				ops = append(ops, SystemKVOp{Key: fp.key, Delete: true})
				built.fpKeysDeleted++
				built.templateKeysDeleted++
			} else {
				ops = append(ops, SystemKVOp{Key: fp.key, Value: encodeSystemTemplateFPCandidates(kept)})
				built.fpKeysUpdated++
			}
		}
	}

	return ops, built
}

func systemDictKKey(dictID uint64) []byte {
	buf := make([]byte, len(systemDictKPrefix)+8)
	copy(buf, systemDictKPrefix)
	binary.BigEndian.PutUint64(buf[len(systemDictKPrefix):], dictID)
	return buf
}

func systemDictLeafModeKey(dictID uint64) []byte {
	buf := make([]byte, len(systemDictLeafModePrefix)+8)
	copy(buf, systemDictLeafModePrefix)
	binary.BigEndian.PutUint64(buf[len(systemDictLeafModePrefix):], dictID)
	return buf
}

func encodeSystemTemplateFPCandidates(cands []systemTemplateFPCandidate) []byte {
	if len(cands) == 0 {
		return nil
	}
	sorted := append([]systemTemplateFPCandidate(nil), cands...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].size != sorted[j].size {
			return sorted[i].size < sorted[j].size
		}
		return sorted[i].id < sorted[j].id
	})
	var tmp [binary.MaxVarintLen64]byte
	out := make([]byte, 0, len(sorted)*8)
	for _, c := range sorted {
		n := binary.PutUvarint(tmp[:], c.id)
		out = append(out, tmp[:n]...)
		n = binary.PutUvarint(tmp[:], c.size)
		out = append(out, tmp[:n]...)
	}
	return out
}
