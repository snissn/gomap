package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type LeafGenerationGCOptions struct {
	DryRun bool

	// ProtectedRootIDs are additional ordinary root page IDs whose leaf-log
	// children must be treated as live even when they are not reachable from the
	// backend meta roots. Cached named roots use this during online maintenance.
	ProtectedRootIDs []uint64
	// ProtectedSystemRootIDs are system-root page IDs whose collection root
	// descriptors should be expanded into additional protected roots.
	ProtectedSystemRootIDs []uint64
}

type LeafGenerationGCStats struct {
	GenerationsTotal    int
	GenerationsWritable int
	GenerationsLive     int
	GenerationsRetiring int
	GenerationsEligible int
	GenerationsDeleted  int
	FilesDeleted        int
	BytesEligible       int64
	BytesDeleted        int64
}

func (db *DB) LeafGenerationGC(ctx context.Context, opts LeafGenerationGCOptions) (LeafGenerationGCStats, error) {
	return db.leafGenerationGC(ctx, opts, true)
}

func (db *DB) leafGenerationGC(ctx context.Context, opts LeafGenerationGCOptions, lockMaintenance bool) (LeafGenerationGCStats, error) {
	var stats LeafGenerationGCStats
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly && !opts.DryRun {
		return stats, ErrReadOnly
	}
	if !db.indexOuterLeavesInValueLog {
		return stats, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if lockMaintenance {
		db.maintenanceMu.Lock()
		defer db.maintenanceMu.Unlock()
	}
	if !opts.DryRun {
		if err := db.CheckStorageMaintenanceReady(); err != nil {
			return stats, err
		}
	}

	attempts := 1
	if opts.DryRun {
		attempts++
	}
	for attempt := 0; attempt < attempts; attempt++ {
		stats, stale, err := db.leafGenerationGCAttempt(ctx, opts)
		if err != nil {
			return stats, err
		}
		if !stale {
			return stats, nil
		}
		if err := ctx.Err(); err != nil {
			return LeafGenerationGCStats{}, err
		}
	}
	return LeafGenerationGCStats{}, ErrLeafGenerationGCStaleScan
}

func (db *DB) leafGenerationGCAttempt(ctx context.Context, opts LeafGenerationGCOptions) (LeafGenerationGCStats, bool, error) {
	var stats LeafGenerationGCStats

	// Refresh, snapshot acquisition, and the live scan intentionally run outside
	// writeMu. Pin their backing resources against Close without joining the lock
	// used by checkpoints and foreground publishes.
	db.teardownMu.RLock()
	teardownHeld := true
	releaseTeardown := func() {
		if teardownHeld {
			db.teardownMu.RUnlock()
			teardownHeld = false
		}
	}
	defer releaseTeardown()
	if db.closing.Load() {
		return stats, false, nil
	}

	prepared, err := db.prepareLeafGenerationGCScan()
	if err != nil || !prepared {
		return stats, false, err
	}
	var recoverableRoots *RecoverableRootSet
	if opts.DryRun {
		recoverableRoots, err = db.captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(ctx)
	} else {
		recoverableRoots, err = db.captureRecoverableRootSetWithMaintenanceLockHeld(ctx)
	}
	if err != nil {
		return stats, false, err
	}
	defer recoverableRoots.Release()

	snap := db.AcquireSnapshot()
	if snap == nil {
		return stats, false, ErrClosed
	}
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	if snap.state == nil || snap.state.LeafGenerations == nil {
		_ = snap.Close()
		return stats, false, nil
	}

	basis, ok := db.captureLeafGenerationGCScanBasis(snap)
	if !ok {
		_ = snap.Close()
		return stats, opts.DryRun, nil
	}

	liveGenerations, err := collectLiveLeafGenerationIDs(ctx, snap, opts.ProtectedRootIDs, opts.ProtectedSystemRootIDs)
	if err != nil {
		_ = snap.Close()
		return stats, false, err
	}
	var recoverableLive map[uint64]struct{}
	if recoverableRoots != nil {
		recoverableLive, err = db.collectRecoverableLeafGenerationIDs(ctx, recoverableRoots, snap.state.LeafGenerations)
		if err != nil {
			_ = snap.Close()
			if opts.DryRun && errors.Is(err, ErrRecoverableRootSetStale) {
				return stats, true, nil
			}
			return stats, false, err
		}
	}
	if err := snap.Close(); err != nil {
		return stats, false, err
	}
	snap = nil
	releaseTeardown()

	if opts.DryRun {
		return db.leafGenerationGCDryRunFromScan(basis, liveGenerations, recoverableLive, recoverableRoots)
	}
	stats, err = db.leafGenerationGCApplyScan(basis, liveGenerations, recoverableLive, recoverableRoots)
	return stats, false, err
}

type leafGenerationGCScanBasis struct {
	commitSeq                  uint64
	leafGenerationStateVersion uint64
	manifest                   *leafGenerationManifest
}

type leafGenerationGCDecision struct {
	stats               LeafGenerationGCStats
	intermediateChanged bool
	zombieFileIDs       map[uint32]struct{}
}

func (db *DB) prepareLeafGenerationGCScan() (bool, error) {
	if db.closing.Load() {
		return false, nil
	}
	db.mu.RLock()
	if db.leafGenerationManifest == nil || db.valueLogManager == nil {
		db.mu.RUnlock()
		return false, nil
	}
	valueLogManager := db.valueLogManager
	db.mu.RUnlock()

	// The caller's teardown read lock keeps Close from clearing or closing this
	// manager. Manager synchronization handles concurrent normal DB activity.
	if err := valueLogManager.Refresh(); err != nil {
		return false, err
	}

	db.writeMu.Lock()
	runLeafGenerationGCExclusivePhaseHook(true)
	defer func() {
		runLeafGenerationGCExclusivePhaseHook(false)
		db.writeMu.Unlock()
	}()

	if db.closing.Load() || db.leafGenerationManifest == nil || db.valueLogManager == nil {
		return false, nil
	}
	commitSeq := uint64(1)
	if state, ok := db.StateToken(); ok && state.CommitSeq != 0 {
		commitSeq = state.CommitSeq
	}
	if _, err := db.reconcileLeafGenerationManifestWithDirLocked(commitSeq); err != nil {
		return false, err
	}
	return true, nil
}

func (db *DB) captureLeafGenerationGCScanBasis(snap *Snapshot) (leafGenerationGCScanBasis, bool) {
	var basis leafGenerationGCScanBasis
	if snap == nil || snap.state == nil || snap.state.LeafGenerations == nil {
		return basis, false
	}

	// Optimistic publishers swap the state and manifest under db.mu, so capture
	// the pair under that lock. The teardown read lock protects backing resources
	// from Close; snapshot references handle normal index-generation cutovers.
	db.mu.RLock()
	current := db.state.Load()
	var manifest *leafGenerationManifest
	if db.leafGenerationManifest != nil {
		manifest = db.leafGenerationManifest.clone()
	}
	db.mu.RUnlock()

	if current == nil || current.CommitSeq != snap.state.CommitSeq || current.LeafGenerationStateVersion != snap.state.LeafGenerationStateVersion {
		return basis, false
	}
	if manifest == nil {
		return basis, false
	}
	basis.commitSeq = snap.state.CommitSeq
	basis.leafGenerationStateVersion = snap.state.LeafGenerationStateVersion
	basis.manifest = manifest
	return basis, true
}

func (db *DB) leafGenerationGCValidatedManifestClone(basis leafGenerationGCScanBasis) (*leafGenerationManifest, bool) {
	// See captureLeafGenerationGCScanBasis: db.mu serializes manifest swaps from
	// optimistic publishers that share writeMu.RLock with this maintenance path.
	db.mu.RLock()
	defer db.mu.RUnlock()

	current := db.state.Load()
	if current == nil || current.CommitSeq != basis.commitSeq || current.LeafGenerationStateVersion != basis.leafGenerationStateVersion {
		return nil, false
	}
	if !leafGenerationManifestsEqualForGC(basis.manifest, db.leafGenerationManifest) {
		return nil, false
	}
	manifest := db.leafGenerationManifest.clone()
	if manifest == nil {
		return nil, false
	}
	return manifest, true
}

func (db *DB) leafGenerationGCDryRunFromScan(basis leafGenerationGCScanBasis, liveGenerations, recoverableGenerations map[uint64]struct{}, recoverableRoots *RecoverableRootSet) (LeafGenerationGCStats, bool, error) {
	var stats LeafGenerationGCStats

	db.writeMu.Lock()
	runLeafGenerationGCExclusivePhaseHook(true)
	if db.closing.Load() {
		runLeafGenerationGCExclusivePhaseHook(false)
		db.writeMu.Unlock()
		return stats, false, nil
	}
	manifest, ok := db.leafGenerationGCValidatedManifestClone(basis)
	if !ok {
		runLeafGenerationGCExclusivePhaseHook(false)
		db.writeMu.Unlock()
		return stats, true, nil
	}
	currentLeafLogRawFileIDs, err := db.currentLeafPageLogRawFileIDSet()
	if err != nil {
		runLeafGenerationGCExclusivePhaseHook(false)
		db.writeMu.Unlock()
		return stats, false, err
	}
	filePaths := db.leafGenerationFilePaths(manifest)
	if err := recoverableRoots.Revalidate(); err != nil {
		runLeafGenerationGCExclusivePhaseHook(false)
		db.writeMu.Unlock()
		if errors.Is(err, ErrRecoverableRootSetStale) {
			return stats, true, nil
		}
		return stats, false, err
	}
	recoverableRoots.Release()
	runLeafGenerationGCExclusivePhaseHook(false)
	db.writeMu.Unlock()

	decision := db.buildLeafGenerationGCDecision(manifest, filePaths, currentLeafLogRawFileIDs, liveGenerations, recoverableGenerations, basis.commitSeq, true)
	return decision.stats, false, nil
}

func (db *DB) leafGenerationGCApplyScan(basis leafGenerationGCScanBasis, liveGenerations, recoverableGenerations map[uint64]struct{}, recoverableRoots *RecoverableRootSet) (LeafGenerationGCStats, error) {
	var stats LeafGenerationGCStats

	db.writeMu.Lock()
	runLeafGenerationGCExclusivePhaseHook(true)
	defer func() {
		runLeafGenerationGCExclusivePhaseHook(false)
		db.writeMu.Unlock()
	}()
	if db.closing.Load() {
		return stats, nil
	}

	manifest, ok := db.leafGenerationGCValidatedManifestClone(basis)
	if !ok {
		return stats, nil
	}
	currentLeafLogRawFileIDs, err := db.currentLeafPageLogRawFileIDSet()
	if err != nil {
		return stats, err
	}
	filePaths := db.leafGenerationFilePaths(manifest)
	decision := db.buildLeafGenerationGCDecision(manifest, filePaths, currentLeafLogRawFileIDs, liveGenerations, recoverableGenerations, basis.commitSeq, false)
	stats = decision.stats
	if err := recoverableRoots.Revalidate(); err != nil {
		return stats, err
	}
	recoverableRoots.Release()

	if len(decision.zombieFileIDs) > 0 {
		for fileID := range decision.zombieFileIDs {
			if err := db.valueLogManager.MarkZombie(fileID); err != nil && !errors.Is(err, valuelog.ErrFileNotFound) {
				return stats, err
			}
			// A maintenance refresh can discover a previously deleted segment
			// that was never part of a published ValueLogSet. Such a file has no
			// Set release to trigger zombie deletion, so attempt the unpinned
			// removal directly. Snapshot- or identity-pinned files remain marked
			// zombie and are retried by the manager when their final pin releases.
			if _, err := db.valueLogManager.RemoveSegmentIfUnpinned(fileID); err != nil && !errors.Is(err, valuelog.ErrFileNotFound) {
				return stats, err
			}
		}
	}

	if decision.intermediateChanged {
		if err := db.replaceLeafGenerationManifest(manifest); err != nil {
			return stats, err
		}
		db.leafGenerationManifest = manifest
	}
	if decision.intermediateChanged || len(decision.zombieFileIDs) > 0 {
		if err := db.publishLeafGenerationState(len(decision.zombieFileIDs) > 0); err != nil {
			return stats, err
		}
	}

	prePrune := manifest.clone()
	manifest, pruned, filesDeleted, err := db.pruneDeletedLeafGenerationRecords(manifest, filePaths)
	if err != nil {
		return stats, fmt.Errorf("leaf generation gc: prune deleted generation records: %w", err)
	}
	if !pruned {
		return stats, nil
	}
	stats.GenerationsDeleted = countPrunedLeafGenerations(prePrune, manifest)
	stats.FilesDeleted = filesDeleted
	if err := db.replaceLeafGenerationManifest(manifest); err != nil {
		return stats, err
	}
	db.leafGenerationManifest = manifest
	return stats, nil
}

func (db *DB) collectRecoverableLeafGenerationIDs(ctx context.Context, roots *RecoverableRootSet, current *leafGenerationView) (map[uint64]struct{}, error) {
	captured := roots.Roots()
	if len(captured) == 0 {
		return nil, ErrRecoverableRootSetStale
	}
	snap := roots.AcquireSnapshotForRoot(captured[0])
	if snap == nil {
		return nil, ErrRecoverableRootSetStale
	}
	protectedRootIDs := make([]uint64, 0, len(captured))
	protectedSystemRootIDs := make([]uint64, 0, len(captured))
	for _, root := range captured {
		protectedRootIDs = append(protectedRootIDs, root.UserRootPageID)
		protectedSystemRootIDs = append(protectedSystemRootIDs, root.SystemRootPageID)
	}
	scan, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{
		Collectors:             maintenanceReachabilityLeafFileIDs,
		ProtectedRootIDs:       protectedRootIDs,
		ProtectedSystemRootIDs: protectedSystemRootIDs,
	})
	closeErr := snap.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return roots.leafGenerationIDsForFiles(scan.leafFileIDs, current)
}

func (db *DB) buildLeafGenerationGCDecision(manifest *leafGenerationManifest, filePaths map[uint32]string, currentLeafLogRawFileIDs map[uint32]struct{}, liveGenerations, recoverableGenerations map[uint64]struct{}, currentCommitSeq uint64, dryRun bool) leafGenerationGCDecision {
	decision := leafGenerationGCDecision{zombieFileIDs: make(map[uint32]struct{})}
	activeFileRefs := leafGenerationActiveFileRefCounts(manifest)
	for i := range manifest.Generations {
		gen := &manifest.Generations[i]
		genBytes := int64(0)
		genBytesKnown := false
		loadGenBytes := func() int64 {
			if !genBytesKnown {
				genBytes = leafGenerationRecordBytesTotal(*gen, filePaths, db.reportError)
				genBytesKnown = true
			}
			return genBytes
		}
		decision.stats.GenerationsTotal++
		if gen.State == leafGenerationStateDeleted {
			// Zombie state is in-memory only. After a reopen, deleted manifest records
			// whose files are still present must be marked zombie again so deletion is
			// retried, unless another active generation has legitimately reused the ID.
			seenFiles := make(map[uint32]struct{}, len(gen.FileIDs))
			for _, fileID := range gen.FileIDs {
				if fileID == 0 {
					continue
				}
				if _, seen := seenFiles[fileID]; seen {
					continue
				}
				seenFiles[fileID] = struct{}{}
				if activeFileRefs[fileID] > 0 {
					continue
				}
				if _, current := currentLeafLogRawFileIDs[fileID]; current {
					continue
				}
				decision.zombieFileIDs[page.ValueLogFileID(fileID)] = struct{}{}
			}
			continue
		}
		if gen.State == leafGenerationStateWritable {
			decision.stats.GenerationsWritable++
			continue
		}
		// Multiple physical leaf-log append writers can be current at once while
		// the generation manifest still records one file per generation. A sealed
		// generation that owns an active current writer must not be zombied just
		// because the current root no longer references its older pages; future
		// appends may still land in that file until the lane rotates or closes.
		if leafGenerationRecordIntersectsFileIDSet(*gen, currentLeafLogRawFileIDs) {
			decision.stats.GenerationsLive++
			if gen.State != leafGenerationStateSealed {
				gen.State = leafGenerationStateSealed
				decision.intermediateChanged = true
			}
			continue
		}
		if _, ok := liveGenerations[gen.GenerationID]; ok {
			decision.stats.GenerationsLive++
			if gen.State != leafGenerationStateSealed {
				gen.State = leafGenerationStateSealed
				decision.intermediateChanged = true
			}
			continue
		}
		// A generation referenced only by an independently recoverable root is
		// retained debt, not part of the currently visible tree. Keep it retiring
		// so a later pass can reclaim it after every durable fallback advances.
		if _, ok := recoverableGenerations[gen.GenerationID]; ok {
			decision.stats.GenerationsRetiring++
			if gen.State != leafGenerationStateRetiring {
				gen.State = leafGenerationStateRetiring
				if currentCommitSeq > gen.RetiredCommitSeq {
					gen.RetiredCommitSeq = currentCommitSeq
				}
				decision.intermediateChanged = true
			}
			continue
		}
		if db.leafGenerationPins.count(gen.GenerationID) > 0 {
			decision.stats.GenerationsRetiring++
			if gen.State != leafGenerationStateRetiring {
				gen.State = leafGenerationStateRetiring
				if currentCommitSeq > gen.RetiredCommitSeq {
					gen.RetiredCommitSeq = currentCommitSeq
				}
				decision.intermediateChanged = true
			}
			continue
		}
		decision.stats.GenerationsEligible++
		if bytes := loadGenBytes(); bytes > 0 {
			decision.stats.BytesEligible += bytes
		}
		if dryRun {
			continue
		}
		if gen.State != leafGenerationStateDeleted {
			gen.State = leafGenerationStateDeleted
			if currentCommitSeq > gen.DeletedCommitSeq {
				gen.DeletedCommitSeq = currentCommitSeq
			}
			if bytes := loadGenBytes(); bytes > 0 {
				decision.stats.BytesDeleted += bytes
			}
			seenFiles := make(map[uint32]struct{}, len(gen.FileIDs))
			for _, fileID := range gen.FileIDs {
				if fileID == 0 {
					continue
				}
				if _, seen := seenFiles[fileID]; seen {
					continue
				}
				seenFiles[fileID] = struct{}{}
				// Malformed legacy manifests may mention a physical leaf segment in
				// more than one active generation. Deleting this generation must not
				// unlink a segment another active generation still references, but it
				// should still reclaim the file when this pass deletes the final active
				// reference.
				if activeFileRefs[fileID] > 0 {
					activeFileRefs[fileID]--
				}
				if activeFileRefs[fileID] > 0 {
					continue
				}
				decision.zombieFileIDs[page.ValueLogFileID(fileID)] = struct{}{}
			}
			decision.intermediateChanged = true
		}
	}
	return decision
}

func leafGenerationManifestsEqualForGC(a, b *leafGenerationManifest) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Version != b.Version || a.CurrentGenerationID != b.CurrentGenerationID || a.NextGenerationID != b.NextGenerationID || len(a.Generations) != len(b.Generations) {
		return false
	}
	for i := range a.Generations {
		ag, bg := a.Generations[i], b.Generations[i]
		if ag.GenerationID != bg.GenerationID || ag.State != bg.State || ag.CreatedCommitSeq != bg.CreatedCommitSeq || ag.SealedCommitSeq != bg.SealedCommitSeq || ag.RetiredCommitSeq != bg.RetiredCommitSeq || ag.DeletedCommitSeq != bg.DeletedCommitSeq || ag.PublishedCommitSeq != bg.PublishedCommitSeq {
			return false
		}
		if !leafGenerationFileIDsEqualForGC(ag.FileIDs, bg.FileIDs) {
			return false
		}
	}
	return true
}

func leafGenerationFileIDsEqualForGC(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (db *DB) currentLeafPageLogRawFileIDSet() (map[uint32]struct{}, error) {
	if db == nil || db.leafPageLog == nil {
		return nil, nil
	}
	segments, err := leafPageLogCurrentSegments(db.leafPageLog)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		return nil, nil
	}
	out := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		rawFileID, ok := rawLeafGenerationFileID(seg.FileID)
		if !ok {
			continue
		}
		out[rawFileID] = struct{}{}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func leafGenerationRecordIntersectsFileIDSet(gen leafGenerationRecord, fileIDs map[uint32]struct{}) bool {
	if len(fileIDs) == 0 || len(gen.FileIDs) == 0 {
		return false
	}
	for _, fileID := range gen.FileIDs {
		if _, ok := fileIDs[fileID]; ok {
			return true
		}
	}
	return false
}

func leafGenerationActiveFileRefCounts(manifest *leafGenerationManifest) map[uint32]int {
	counts := make(map[uint32]int)
	if manifest == nil {
		return counts
	}
	for _, gen := range manifest.Generations {
		if gen.State == leafGenerationStateDeleted {
			continue
		}
		seen := make(map[uint32]struct{}, len(gen.FileIDs))
		for _, fileID := range gen.FileIDs {
			if fileID == 0 {
				continue
			}
			if _, ok := seen[fileID]; ok {
				continue
			}
			seen[fileID] = struct{}{}
			counts[fileID]++
		}
	}
	return counts
}

func collectLiveLeafGenerationIDs(ctx context.Context, snap *Snapshot, protectedRootIDs, protectedSystemRootIDs []uint64) (map[uint64]struct{}, error) {
	live := make(map[uint64]struct{})
	if snap == nil || snap.db == nil {
		return live, nil
	}
	// GC deletion must not trust cached subtree/live reachability. A stale cache
	// can turn a live generation into a deletion candidate, while a fresh scan is
	// maintenance-only work outside the write/read happy path.
	scan, err := snap.db.leafGenerationLiveStatsForSnapshotUncachedWithProtectedRoots(ctx, snap, protectedRootIDs, protectedSystemRootIDs)
	if err != nil {
		return nil, err
	}
	for generationID, totals := range scan.Generations {
		if generationID == 0 || (totals.LiveBytes <= 0 && totals.LivePages <= 0) {
			continue
		}
		live[generationID] = struct{}{}
	}
	return live, nil
}

func (db *DB) leafGenerationFilePaths(manifest *leafGenerationManifest) map[uint32]string {
	paths := make(map[uint32]string)
	if db == nil || manifest == nil || db.valueLogManager == nil {
		return paths
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil {
		return paths
	}
	defer func() { _ = db.valueLogManager.Release(set) }()
	for _, gen := range manifest.Generations {
		for _, fileID := range gen.FileIDs {
			if fileID == 0 {
				continue
			}
			if f := set.Files[page.ValueLogFileID(fileID)]; f != nil && f.Path != "" {
				paths[fileID] = f.Path
				continue
			}
			paths[fileID] = leafGenerationFallbackPath(db.dir, fileID)
		}
	}
	return paths
}

func leafGenerationFallbackPath(rootDir string, fileID uint32) string {
	if rootDir == "" || fileID == 0 {
		return ""
	}
	lane, seq := valuelog.DecodeFileID(fileID)
	return filepath.Join(LeafLogDirPath(rootDir), fmt.Sprintf("value-l%d-%06d.log", lane, seq))
}

func leafGenerationRecordBytesTotal(gen leafGenerationRecord, filePaths map[uint32]string, report func(error)) int64 {
	var total int64
	for _, fileID := range gen.FileIDs {
		if fileID == 0 {
			continue
		}
		path := filePaths[fileID]
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			if report != nil && !os.IsNotExist(err) {
				report(fmt.Errorf("treedb: stat leaf generation file %d (%s): %w", fileID, path, err))
			}
			continue
		}
		total += info.Size()
	}
	return total
}

func (db *DB) pruneDeletedLeafGenerationRecords(manifest *leafGenerationManifest, filePaths map[uint32]string) (*leafGenerationManifest, bool, int, error) {
	if manifest == nil {
		return nil, false, 0, nil
	}
	pruned := false
	filesDeleted := 0
	var prunedFileIDs []uint32
	for _, gen := range manifest.Generations {
		if gen.State != leafGenerationStateDeleted || !leafGenerationFilesMissing(gen.FileIDs, filePaths) {
			continue
		}
		pruned = true
		filesDeleted += len(gen.FileIDs)
		prunedFileIDs = append(prunedFileIDs, gen.FileIDs...)
	}
	if !pruned {
		return manifest, false, 0, nil
	}
	if err := db.removeLeafGenerationRecordLengthIndexes(prunedFileIDs); err != nil {
		return manifest, false, 0, err
	}
	next := *manifest
	kept := next.Generations[:0]
	for _, gen := range next.Generations {
		if gen.State == leafGenerationStateDeleted && leafGenerationFilesMissing(gen.FileIDs, filePaths) {
			continue
		}
		kept = append(kept, gen)
	}
	next.Generations = kept
	return &next, true, filesDeleted, nil
}

func (db *DB) removeLeafGenerationRecordLengthIndexes(fileIDs []uint32) error {
	if db == nil || len(fileIDs) == 0 {
		return nil
	}
	var errs []error
	for _, fileID := range fileIDs {
		if fileID == 0 {
			continue
		}
		path := leafGenerationRecordLengthIndexPath(db.dir, fileID)
		_, err := removePersistentFile(filepath.Dir(path), path, durabilitycut.ResourceAuxiliary)
		if err != nil {
			errs = append(errs, fmt.Errorf("remove leaf generation record-length index %d (%s): %w", fileID, path, err))
			continue
		}
		db.leafGenerationRecordLengthMu.Lock()
		delete(db.leafGenerationRecordLengthByFile, fileID)
		db.leafGenerationRecordLengthMu.Unlock()
	}
	return errors.Join(errs...)
}

func leafGenerationFilesMissing(fileIDs []uint32, filePaths map[uint32]string) bool {
	if len(fileIDs) == 0 {
		return true
	}
	for _, fileID := range fileIDs {
		path := filePaths[fileID]
		if path == "" {
			return false
		}
		if _, err := os.Stat(path); err == nil {
			return false
		} else if !os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func countPrunedLeafGenerations(before, after *leafGenerationManifest) int {
	if before == nil {
		return 0
	}
	afterSet := make(map[uint64]struct{})
	if after != nil {
		for _, gen := range after.Generations {
			afterSet[gen.GenerationID] = struct{}{}
		}
	}
	count := 0
	for _, gen := range before.Generations {
		if gen.State != leafGenerationStateDeleted {
			continue
		}
		if _, ok := afterSet[gen.GenerationID]; ok {
			continue
		}
		count++
	}
	return count
}

func (db *DB) publishLeafGenerationState(refreshValueLogSet bool) error {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	oldState := db.state.Load()
	if oldState == nil {
		return nil
	}

	var valueLogSet = oldState.ValueLogSet
	if refreshValueLogSet {
		if db.valueLogManager != nil {
			if err := db.valueLogManager.Refresh(); err != nil {
				return err
			}
			valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
		}
	} else if valueLogSet != nil && db.valueLogManager != nil {
		db.valueLogManager.Acquire(valueLogSet)
	}

	newState := &DBState{
		CommitSeq:         oldState.CommitSeq,
		RootPageID:        oldState.RootPageID,
		SystemRootPageID:  oldState.SystemRootPageID,
		AppliedCommandLSN: oldState.AppliedCommandLSN,
		MaxEntryRevision:  oldState.MaxEntryRevision,
		ValueLogSet:       valueLogSet,
		LeafGenerations:   db.currentLeafGenerationView(),
	}
	if newState.LeafGenerations != nil {
		db.leafGenerationStateVersion++
		newState.LeafGenerationStateVersion = db.leafGenerationStateVersion
	} else {
		newState.LeafGenerationStateVersion = oldState.LeafGenerationStateVersion
	}
	db.state.Store(newState)
	db.publishSnapshotView(db.idx.Load(), newState, db.valueLogManager)
	if oldState.ValueLogSet != nil && db.valueLogManager != nil {
		return db.valueLogManager.Release(oldState.ValueLogSet)
	}
	return nil
}
