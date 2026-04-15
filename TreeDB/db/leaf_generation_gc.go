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
		db.unpinLeafGenerationIDs(snap.leafGenerationIDs)
		snap.leafGenerationIDs = snap.leafGenerationIDs[:0]
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
		if opts.DryRun {
			continue
		}
		if gen.State != leafGenerationStateDeleted {
			gen.State = leafGenerationStateDeleted
			if currentCommitSeq > gen.DeletedCommitSeq {
				gen.DeletedCommitSeq = currentCommitSeq
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

	if intermediateChanged {
		if err := saveLeafGenerationManifest(LeafLogDirPath(db.dir), manifest); err != nil {
			return stats, err
		}
	}
	if !opts.DryRun && len(zombieFileIDs) > 0 {
		for fileID := range zombieFileIDs {
			if err := db.valueLogManager.MarkZombie(fileID); err != nil && !errors.Is(err, valuelog.ErrFileNotFound) {
				return stats, err
			}
		}
	}

	if intermediateChanged {
		db.leafGenerationManifest = manifest
		if err := db.publishLeafGenerationState(len(zombieFileIDs) > 0); err != nil {
			return stats, err
		}
	}

	prePrune := manifest.clone()
	manifest, pruned, filesDeleted := pruneDeletedLeafGenerationRecords(manifest, filePaths)
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
	scan, err := snap.db.leafGenerationLiveStatsForSnapshot(ctx, snap)
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

func pruneDeletedLeafGenerationRecords(manifest *leafGenerationManifest, filePaths map[uint32]string) (*leafGenerationManifest, bool, int) {
	if manifest == nil {
		return nil, false, 0
	}
	kept := manifest.Generations[:0]
	pruned := false
	filesDeleted := 0
	for _, gen := range manifest.Generations {
		if gen.State != leafGenerationStateDeleted || !leafGenerationFilesMissing(gen.FileIDs, filePaths) {
			kept = append(kept, gen)
			continue
		}
		pruned = true
		filesDeleted += len(gen.FileIDs)
	}
	if !pruned {
		return manifest, false, 0
	}
	manifest.Generations = kept
	return manifest, true, filesDeleted
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
	db.state.Store(newState)
	db.publishSnapshotView(db.idx.Load(), newState, db.valueLogManager)
	if oldState.ValueLogSet != nil && db.valueLogManager != nil {
		return db.valueLogManager.Release(oldState.ValueLogSet)
	}
	return nil
}
