package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type LeafGenerationGCOptions struct {
	DryRun bool
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
	var stats LeafGenerationGCStats
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if !db.indexOuterLeavesInValueLog {
		return stats, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.leafGenerationManifest == nil || db.valueLogManager == nil {
		return stats, nil
	}
	if err := db.valueLogManager.Refresh(); err != nil {
		return stats, err
	}

	manifest := db.leafGenerationManifest.clone()
	if manifest == nil {
		return stats, nil
	}
	filePaths := db.leafGenerationFilePaths(manifest)

	snap := db.AcquireSnapshot()
	if snap == nil {
		return stats, ErrClosed
	}
	if len(snap.leafGenerationIDs) > 0 {
		snap.releaseLeafGenerationPins()
	}
	if snap.state == nil || snap.state.LeafGenerations == nil {
		_ = snap.Close()
		return stats, nil
	}

	liveGenerations, err := collectLiveLeafGenerationIDs(ctx, snap)
	if err != nil {
		_ = snap.Close()
		return stats, err
	}

	currentCommitSeq := snap.state.CommitSeq
	if err := snap.Close(); err != nil {
		return stats, err
	}
	snap = nil
	intermediateChanged := false
	zombieFileIDs := make(map[uint32]struct{})
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
		stats.GenerationsTotal++
		if gen.State == leafGenerationStateDeleted {
			continue
		}
		if gen.State == leafGenerationStateWritable {
			stats.GenerationsWritable++
			continue
		}
		if _, ok := liveGenerations[gen.GenerationID]; ok {
			stats.GenerationsLive++
			if gen.State != leafGenerationStateSealed {
				gen.State = leafGenerationStateSealed
				intermediateChanged = true
			}
			continue
		}
		if db.leafGenerationPins.count(gen.GenerationID) > 0 {
			stats.GenerationsRetiring++
			if gen.State != leafGenerationStateRetiring {
				gen.State = leafGenerationStateRetiring
				if currentCommitSeq > gen.RetiredCommitSeq {
					gen.RetiredCommitSeq = currentCommitSeq
				}
				intermediateChanged = true
			}
			continue
		}
		stats.GenerationsEligible++
		if bytes := loadGenBytes(); bytes > 0 {
			stats.BytesEligible += bytes
		}
		if opts.DryRun {
			continue
		}
		if gen.State != leafGenerationStateDeleted {
			gen.State = leafGenerationStateDeleted
			if currentCommitSeq > gen.DeletedCommitSeq {
				gen.DeletedCommitSeq = currentCommitSeq
			}
			if bytes := loadGenBytes(); bytes > 0 {
				stats.BytesDeleted += bytes
			}
			for _, fileID := range gen.FileIDs {
				if fileID == 0 {
					continue
				}
				zombieFileIDs[page.ValueLogFileID(fileID)] = struct{}{}
			}
			intermediateChanged = true
		}
	}

	if opts.DryRun {
		return stats, nil
	}

	if len(zombieFileIDs) > 0 {
		for fileID := range zombieFileIDs {
			if err := db.valueLogManager.MarkZombie(fileID); err != nil && !errors.Is(err, valuelog.ErrFileNotFound) {
				return stats, err
			}
		}
	}

	if intermediateChanged {
		if err := saveLeafGenerationManifest(LeafLogDirPath(db.dir), manifest); err != nil {
			return stats, err
		}
		db.leafGenerationManifest = manifest
		if err := db.publishLeafGenerationState(len(zombieFileIDs) > 0); err != nil {
			return stats, err
		}
	}

	prePrune := manifest.clone()
	manifest, pruned, filesDeleted, err := db.pruneDeletedLeafGenerationRecords(manifest, filePaths)
	if err != nil {
		return stats, err
	}
	if !pruned {
		return stats, nil
	}
	stats.GenerationsDeleted = countPrunedLeafGenerations(prePrune, manifest)
	stats.FilesDeleted = filesDeleted
	if err := saveLeafGenerationManifest(LeafLogDirPath(db.dir), manifest); err != nil {
		return stats, err
	}
	db.leafGenerationManifest = manifest
	return stats, nil
}

func collectLiveLeafGenerationIDs(ctx context.Context, snap *Snapshot) (map[uint64]struct{}, error) {
	live := make(map[uint64]struct{})
	if snap == nil || snap.db == nil {
		return live, nil
	}
	// GC deletion must not trust cached subtree/live reachability. A stale cache
	// can turn a live generation into a deletion candidate, while a fresh scan is
	// maintenance-only work outside the write/read happy path.
	scan, err := snap.db.leafGenerationLiveStatsForSnapshotUncached(ctx, snap)
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
	prune := make([]bool, len(manifest.Generations))
	pruned := false
	prunedRecords := 0
	filesDeleted := 0
	for i, gen := range manifest.Generations {
		if gen.State != leafGenerationStateDeleted || !leafGenerationFilesMissing(gen.FileIDs, filePaths) {
			continue
		}
		if err := db.removeLeafGenerationRecordLengthIndexes(gen.FileIDs); err != nil {
			return manifest, false, 0, err
		}
		prune[i] = true
		pruned = true
		prunedRecords++
		filesDeleted += len(gen.FileIDs)
	}
	if !pruned {
		return manifest, false, 0, nil
	}
	kept := make([]leafGenerationRecord, 0, len(manifest.Generations)-prunedRecords)
	for i, gen := range manifest.Generations {
		if !prune[i] {
			kept = append(kept, gen)
		}
	}
	next := *manifest
	next.Generations = kept
	return &next, true, filesDeleted, nil
}

func (db *DB) removeLeafGenerationRecordLengthIndexes(fileIDs []uint32) error {
	if db == nil || len(fileIDs) == 0 {
		return nil
	}
	for _, fileID := range fileIDs {
		if fileID == 0 {
			continue
		}
		path := leafGenerationRecordLengthIndexPath(db.dir, fileID)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove leaf generation record-length index %d (%s): %w", fileID, path, err)
		}
		db.leafGenerationRecordLengthMu.Lock()
		delete(db.leafGenerationRecordLengthByFile, fileID)
		db.leafGenerationRecordLengthMu.Unlock()
	}
	return nil
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
		CommitSeq:        oldState.CommitSeq,
		RootPageID:       oldState.RootPageID,
		SystemRootPageID: oldState.SystemRootPageID,
		ValueLogSet:      valueLogSet,
		LeafGenerations:  db.currentLeafGenerationView(),
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
