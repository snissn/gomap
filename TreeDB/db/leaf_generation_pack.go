package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

const leafGenerationPackDefaultMinReclaimPerByteCopiedPPM = 10000
const leafGenerationPackDefaultLeafFrameK = 16

var leafGenerationPackRIDStartScanner = nextRewriteRIDStartFromSet
var removeLeafGenerationPackStagingDirFn = func(path string) error {
	return removePersistentTree(filepath.Dir(path), path, durabilitycut.ResourceOuterLeaf)
}

func cleanupLeafGenerationPackStagingDirs(leafDir string) error {
	leafParent, err := os.Open(leafDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer leafParent.Close()
	entries, err := leafParent.ReadDir(-1)
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), rootpublication.StableChildFileInstallProbePrefix) {
			if entry.IsDir() {
				return fmt.Errorf("leaf generation pack: reserved install probe name %q is a directory", entry.Name())
			}
			if err := rootpublication.RemoveStableChildFile(leafParent, entry.Name()); err != nil {
				return fmt.Errorf("leaf generation pack: remove orphan install probe %q: %w", entry.Name(), err)
			}
			removed = true
			continue
		}
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), ".leaf-pack-copy-") {
			continue
		}
		if err := removeLeafGenerationPackStagingDirFn(filepath.Join(leafDir, entry.Name())); err != nil {
			return fmt.Errorf("leaf generation pack: remove orphan staging directory %q: %w", entry.Name(), err)
		}
		removed = true
	}
	if removed {
		return syncDeletionNamespaceDirectory(leafDir, durabilitycut.ResourceOuterLeaf)
	}
	return nil
}

type LeafGenerationPackOptions struct {
	GenerationIDs []uint64
	// Sync is retained for API compatibility. Leaf-generation pack publication
	// is always durable: copied records/pages, the promoted-directory entry, and
	// the alternate meta page are synchronized before source generations can
	// become reclaimable. Setting Sync to false does not weaken that contract.
	Sync                       bool
	MinPublishedAgeCommits     uint64
	MinExpectedReclaimBytes    int64
	MinExpectedReclaimRatioPPM int
	MinReclaimPerByteCopiedPPM int
	ReserveRIDs                func(count int) (start uint64, err error)
	Force                      bool
	LeafFrameK                 int
	ProtectedRootIDs           []uint64
	ProtectedSystemRootIDs     []uint64
}

// LeafGenerationPackApplyStageStats attributes copy and publication wall time.
// DirectorySyncTimeNanos is diagnostic operation wall and may overlap page
// relocation/sync; DirectorySyncWaitTimeNanos is the non-overlapping portion
// charged to the exclusive publication path.
type LeafGenerationPackApplyStageStats struct {
	SetupTimeNanos             int64
	TreeRewriteTimeNanos       int64
	LeafSyncTimeNanos          int64
	CopyCloseTimeNanos         int64
	RevalidateTimeNanos        int64
	PromotionTimeNanos         int64
	RelocationTimeNanos        int64
	PageSyncTimeNanos          int64
	DirectorySyncTimeNanos     int64
	DirectorySyncWaitTimeNanos int64
	RegistrationTimeNanos      int64
	CollectionPublishTimeNanos int64
	FinalizeTimeNanos          int64
	PostWorkTimeNanos          int64
	CleanupTimeNanos           int64
}

func (s *LeafGenerationPackApplyStageStats) add(other LeafGenerationPackApplyStageStats) {
	if s == nil {
		return
	}
	s.SetupTimeNanos += other.SetupTimeNanos
	s.TreeRewriteTimeNanos += other.TreeRewriteTimeNanos
	s.LeafSyncTimeNanos += other.LeafSyncTimeNanos
	s.CopyCloseTimeNanos += other.CopyCloseTimeNanos
	s.RevalidateTimeNanos += other.RevalidateTimeNanos
	s.PromotionTimeNanos += other.PromotionTimeNanos
	s.RelocationTimeNanos += other.RelocationTimeNanos
	s.PageSyncTimeNanos += other.PageSyncTimeNanos
	s.DirectorySyncTimeNanos += other.DirectorySyncTimeNanos
	s.DirectorySyncWaitTimeNanos += other.DirectorySyncWaitTimeNanos
	s.RegistrationTimeNanos += other.RegistrationTimeNanos
	s.CollectionPublishTimeNanos += other.CollectionPublishTimeNanos
	s.FinalizeTimeNanos += other.FinalizeTimeNanos
	s.PostWorkTimeNanos += other.PostWorkTimeNanos
	s.CleanupTimeNanos += other.CleanupTimeNanos
}

type LeafGenerationPackStats struct {
	GenerationsRequested            int
	GenerationsMatched              int
	SourceGenerationIDs             []uint64
	SourceFilesRequested            int
	SourceFileIDs                   []uint32
	SourceBytesTotal                int64
	SourceBytesLive                 int64
	SourceBytesDead                 int64
	SourceBytesToCopy               int64
	ExpectedReclaimBytes            int64
	ExpectedReclaimRatioPPM         int
	ExpectedReclaimPerByteCopiedPPM int
	LeafPagesCopied                 int
	BytesCopied                     int64
	LeafFramesWritten               int
	MaxLeafFrameK                   int
	InternalPagesVisited            int
	SubtreesPruned                  int
	CreatedFileIDs                  []uint32
	CopyAttempts                    int
	CopyAborts                      int
	RetryCopyTimeNanos              int64
	CopyTimeNanos                   int64
	PublishWaitNanos                int64
	PublishHoldNanos                int64
	PrivatePagesAllocated           int
	PrivatePagesDiscarded           int
	ApplyStages                     LeafGenerationPackApplyStageStats
	RetryApplyStages                LeafGenerationPackApplyStageStats
	WallTimeNanos                   int64
}

type leafGenerationPackCarryResult struct {
	sourceStateKey                         treeReachabilityCacheKey
	publishedState                         *DBState
	trackSourceLiveMoved                   bool
	protectedRootsOverlapSourceMaintenance bool
	sourceLiveMovedByGeneration            map[uint64]leafGenerationLiveTotals
}

// LeafGenerationPack rewrites live leaf-log pages from sealed source generations
// into a fresh leaf-log output so the old generations can later be reclaimed by
// ordinary generation GC.
func (db *DB) LeafGenerationPack(ctx context.Context, opts LeafGenerationPackOptions) (stats LeafGenerationPackStats, err error) {
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	if !db.indexOuterLeavesInValueLog {
		return stats, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	started := time.Now()
	defer func() {
		stats.WallTimeNanos = time.Since(started).Nanoseconds()
	}()
	stats.GenerationsRequested = len(opts.GenerationIDs)
	opts = normalizeLeafGenerationPackOptions(opts)

	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	selectedPlan, err := db.validateLeafGenerationPackSelection(ctx, opts)
	stats.SourceGenerationIDs = append(stats.SourceGenerationIDs, selectedPlan.CandidateGenerationIDs...)
	stats.SourceBytesTotal = selectedPlan.CandidateBytesTotal
	stats.SourceBytesLive = selectedPlan.CandidateBytesLive
	stats.SourceBytesDead = selectedPlan.CandidateBytesDead
	stats.SourceBytesToCopy = selectedPlan.CandidateBytesToCopy
	stats.ExpectedReclaimBytes = selectedPlan.ExpectedReclaimBytes
	stats.ExpectedReclaimRatioPPM = selectedPlan.ExpectedReclaimRatioPPM
	stats.ExpectedReclaimPerByteCopiedPPM = selectedPlan.ExpectedReclaimPerByteCopiedPPM
	if err != nil {
		return stats, err
	}
	return db.leafGenerationPackLocked(ctx, opts, selectedPlan, stats, nil)
}

func (db *DB) leafGenerationPackSelected(ctx context.Context, opts LeafGenerationPackOptions, selectedPlan LeafGenerationPlan, lockMaintenance bool) (stats LeafGenerationPackStats, err error) {
	return db.leafGenerationPackSelectedWithCarry(ctx, opts, selectedPlan, lockMaintenance, nil)
}

func (db *DB) leafGenerationPackSelectedWithCarry(ctx context.Context, opts LeafGenerationPackOptions, selectedPlan LeafGenerationPlan, lockMaintenance bool, carry *leafGenerationPackCarryResult) (stats LeafGenerationPackStats, err error) {
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	if !db.indexOuterLeavesInValueLog {
		return stats, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	started := time.Now()
	defer func() {
		stats.WallTimeNanos = time.Since(started).Nanoseconds()
	}()
	stats.GenerationsRequested = len(opts.GenerationIDs)
	opts = normalizeLeafGenerationPackOptions(opts)

	if lockMaintenance {
		db.maintenanceMu.Lock()
		defer db.maintenanceMu.Unlock()
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return stats, err
	}
	stats.SourceGenerationIDs = append(stats.SourceGenerationIDs, selectedPlan.CandidateGenerationIDs...)
	stats.SourceBytesTotal = selectedPlan.CandidateBytesTotal
	stats.SourceBytesLive = selectedPlan.CandidateBytesLive
	stats.SourceBytesDead = selectedPlan.CandidateBytesDead
	stats.SourceBytesToCopy = selectedPlan.CandidateBytesToCopy
	stats.ExpectedReclaimBytes = selectedPlan.ExpectedReclaimBytes
	stats.ExpectedReclaimRatioPPM = selectedPlan.ExpectedReclaimRatioPPM
	stats.ExpectedReclaimPerByteCopiedPPM = selectedPlan.ExpectedReclaimPerByteCopiedPPM
	return db.leafGenerationPackLocked(ctx, opts, selectedPlan, stats, carry)
}

func (db *DB) leafGenerationPackLocked(ctx context.Context, opts LeafGenerationPackOptions, selectedPlan LeafGenerationPlan, stats LeafGenerationPackStats, carry *leafGenerationPackCarryResult) (LeafGenerationPackStats, error) {
	rawSourceIDs, matchedGenerations, err := db.resolveLeafGenerationPackSourceFileIDs(opts.GenerationIDs)
	if err != nil {
		return stats, err
	}
	stats.GenerationsMatched = matchedGenerations
	stats.SourceFileIDs = append(stats.SourceFileIDs, rawSourceIDs...)
	stats.SourceFilesRequested = len(rawSourceIDs)
	if len(rawSourceIDs) == 0 {
		return stats, nil
	}
	layout := resolveStorageLayout(db.dir)
	// Stable packed publication requires exact relative namespace primitives.
	// Exercise the real target filesystem install before publishing a value-log
	// set or creating a staging directory, writer, or private pager. A true
	// no-op needs no namespace capability.
	if err := leafGenerationPackPromotionTargetPreflight(layout.leafVLogDir); err != nil {
		return stats, err
	}
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return stats, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set == nil {
		return stats, fmt.Errorf("leaf generation pack: value-log set unavailable")
	}
	leafStartSeq := maxRewriteLaneSeqFromSet(set, rewriteLeafLogLaneID)
	nextRID := uint64(0)
	if opts.ReserveRIDs == nil {
		nextRID, err = leafGenerationPackRIDStartScanner(set)
	}
	_ = db.valueLogManager.Release(set)
	if err != nil {
		return stats, err
	}

	blockCompression := db.valueLogCompression != ValueLogCompressionOff
	blockCodec := valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	leafBlockCodec := leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	var leafDictID uint64
	var leafDictBytes []byte
	var leafDictUseRawPages bool
	if blockCompression {
		if state := db.State(); state != nil {
			leafDictID, leafDictBytes, leafDictUseRawPages, err = prepareRewriteLeafDict(db, state, db.valueLogDictCurrentForClass, db.valueLogDictLeafPayloadMode, db.valueLogDictLookup, db.valueLogDictPut, db.valueLogDictSetCurrentForClass, db.valueLogDictSetLeafPayloadMode, compression.TrainConfig{})
			if err != nil {
				return stats, err
			}
		}
	}

	sourceValueIDs := make(map[uint32]struct{}, len(rawSourceIDs))
	for _, rawID := range rawSourceIDs {
		sourceValueIDs[page.ValueLogFileID(rawID)] = struct{}{}
	}

	seqAlloc, ridAlloc := db.leafGenerationPackAllocators(leafStartSeq, nextRID, opts.ReserveRIDs)
	const maxCopyAttempts = 2
	for attempt := 1; attempt <= maxCopyAttempts; attempt++ {
		stats.CopyAttempts++
		attemptStarted := time.Now()
		stagingDir, err := os.MkdirTemp(layout.leafVLogDir, ".leaf-pack-copy-")
		if err != nil {
			return stats, err
		}
		privateLeafDir := filepath.Join(stagingDir, filepath.Base(layout.leafVLogDir))
		if err := os.MkdirAll(privateLeafDir, 0o700); err != nil {
			_ = removeLeafGenerationPackStagingDirFn(stagingDir)
			return stats, err
		}
		writer := newRewriteWriter(layout.valueVLogDir, 0, 0, 0)
		writer.ConfigureLeafLog(privateLeafDir, rewriteLeafLogLaneID, leafStartSeq)
		writer.configureLeafStaging(stagingDir)
		writer.setLeafPageLogSeqAllocator(seqAlloc)
		writer.setLeafPageLogRIDAllocator(ridAlloc)
		writer.blockCompression = blockCompression
		writer.blockCodec = blockCodec
		writer.leafBlockCodec = leafBlockCodec
		if leafDictID != 0 && len(leafDictBytes) > 0 {
			writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
		}

		rewriteStats := leafRefRewriteRunStats{
			trackCarry:             carry != nil,
			trackSourceLiveMoved:   carry != nil && (len(opts.ProtectedRootIDs) > 0 || len(opts.ProtectedSystemRootIDs) > 0),
			protectedRootIDs:       opts.ProtectedRootIDs,
			protectedSystemRootIDs: opts.ProtectedSystemRootIDs,
		}
		rewriteStarted := time.Now()
		copied, copiedBytes, rewriteErr := db.rewriteLeafRefsOnline(ctx, writer, ridAlloc, sourceValueIDs, nil, 0, 0, false, 0, opts.Sync, normalizeLeafGenerationPackLeafFrameK(opts.LeafFrameK), attempt, &rewriteStats)
		rewriteStats.ApplyStages.SetupTimeNanos += rewriteStarted.Sub(attemptStarted).Nanoseconds()
		cleanupStarted := time.Now()
		closeErr := writer.Close()
		var removeErr error
		if !errors.Is(rewriteErr, ErrRecoveryRequired) {
			removeErr = removeLeafGenerationPackStagingDirFn(stagingDir)
		}
		rewriteStats.ApplyStages.CleanupTimeNanos += time.Since(cleanupStarted).Nanoseconds()
		stats.CopyTimeNanos += rewriteStats.CopyTimeNanos
		stats.PublishWaitNanos += rewriteStats.PublishWaitNanos
		stats.PublishHoldNanos += rewriteStats.PublishHoldNanos
		stats.PrivatePagesAllocated += rewriteStats.PrivatePages
		stats.ApplyStages.add(rewriteStats.ApplyStages)
		if carry != nil {
			carry.sourceStateKey = rewriteStats.sourceStateKey
			carry.publishedState = rewriteStats.publishedState
			carry.trackSourceLiveMoved = rewriteStats.trackSourceLiveMoved
			carry.protectedRootsOverlapSourceMaintenance = rewriteStats.protectedRootsOverlapSourceMaintenance
			carry.sourceLiveMovedByGeneration = cloneLeafGenerationLiveTotalsMap(rewriteStats.sourceLiveMovedByGeneration)
		}
		if errors.Is(rewriteErr, errLeafGenerationPackPublishConflict) {
			stats.CopyAborts++
			stats.RetryCopyTimeNanos += rewriteStats.CopyTimeNanos
			stats.PrivatePagesDiscarded += rewriteStats.PrivatePages
			stats.RetryApplyStages.add(rewriteStats.ApplyStages)
			if closeErr != nil || removeErr != nil {
				return stats, errors.Join(rewriteErr, closeErr, removeErr)
			}
			if err := ctx.Err(); err != nil {
				return stats, err
			}
			continue
		}
		if rewriteErr != nil {
			return stats, fmt.Errorf("leaf generation pack: rewrite leaf refs: %w", errors.Join(rewriteErr, closeErr, removeErr))
		}
		if cleanupErr := errors.Join(closeErr, removeErr); cleanupErr != nil && db.notifyError != nil {
			db.notifyError(fmt.Errorf("leaf generation pack: post-publication cleanup: %w", cleanupErr))
		}

		stats.LeafPagesCopied = copied
		stats.BytesCopied = copiedBytes
		stats.InternalPagesVisited = rewriteStats.InternalPagesVisited
		stats.SubtreesPruned = rewriteStats.SubtreesPruned
		stats.LeafFramesWritten = rewriteStats.LeafFramesWritten
		stats.MaxLeafFrameK = rewriteStats.MaxLeafFrameK
		createdIDs, err := writer.createdFileIDs()
		if err != nil {
			return stats, err
		}
		stats.CreatedFileIDs = filterLeafGenerationRawFileIDs(createdIDs)
		return stats, nil
	}
	return stats, errLeafGenerationPackPublishConflict
}

func (db *DB) leafGenerationPackAllocators(leafStartSeq uint32, nextRID uint64, reserveRIDs func(int) (uint64, error)) (*leafLogSeqAllocator, *rewriteRIDAllocator) {
	seqAlloc := newLeafLogSeqAllocator(leafStartSeq)
	ridAlloc := newRewriteRIDAllocator(nextRID, reserveRIDs)
	if db == nil {
		return seqAlloc, ridAlloc
	}
	db.writeMu.RLock()
	group, ok := db.leafPageLog.(*leafPageLogLaneGroup)
	if ok && group != nil {
		if group.seqAlloc != nil {
			seqAlloc = group.seqAlloc
		}
		if reserveRIDs == nil && group.ridAlloc != nil {
			ridAlloc = group.ridAlloc
		}
	}
	db.writeMu.RUnlock()
	return seqAlloc, ridAlloc
}

func selectedLeafGenerationPackPlan(selection LeafGenerationPackSelection) LeafGenerationPlan {
	return LeafGenerationPlan{
		Admission:                       leafGenerationPlanAdmissionEligible,
		Generations:                     append([]LeafGenerationPlanGeneration(nil), selection.Generations...),
		Candidates:                      append([]LeafGenerationPlanGeneration(nil), selection.Generations...),
		CandidateGenerationIDs:          append([]uint64(nil), selection.GenerationIDs...),
		CandidateBytesTotal:             selection.BytesTotal,
		CandidateBytesLive:              selection.BytesLive,
		CandidateBytesDead:              selection.BytesDead,
		CandidateBytesToCopy:            selection.BytesToCopy,
		CandidateLivePages:              selection.LivePages,
		ExpectedReclaimBytes:            selection.ExpectedReclaimBytes,
		ExpectedReclaimRatioPPM:         selection.ExpectedReclaimRatioPPM,
		ExpectedReclaimPerByteCopiedPPM: selection.ExpectedReclaimPerByteCopiedPPM,
	}
}

func normalizeLeafGenerationPackOptions(opts LeafGenerationPackOptions) LeafGenerationPackOptions {
	opts.LeafFrameK = normalizeLeafGenerationPackLeafFrameK(opts.LeafFrameK)
	if opts.Force {
		return opts
	}
	if opts.MinExpectedReclaimBytes == 0 && opts.MinExpectedReclaimRatioPPM == 0 && opts.MinReclaimPerByteCopiedPPM == 0 {
		opts.MinReclaimPerByteCopiedPPM = leafGenerationPackDefaultMinReclaimPerByteCopiedPPM
	}
	return opts
}

func normalizeLeafGenerationPackLeafFrameK(k int) int {
	if k <= 0 {
		return leafGenerationPackDefaultLeafFrameK
	}
	if k > valuelog.MaxFrameK {
		return valuelog.MaxFrameK
	}
	return k
}

func leafGenerationPackPlanOptions(opts LeafGenerationPackOptions) LeafGenerationPlanOptions {
	return LeafGenerationPlanOptions{
		MinPublishedAgeCommits:     opts.MinPublishedAgeCommits,
		MinExpectedReclaimBytes:    opts.MinExpectedReclaimBytes,
		MinExpectedReclaimRatioPPM: opts.MinExpectedReclaimRatioPPM,
		MinReclaimPerByteCopiedPPM: opts.MinReclaimPerByteCopiedPPM,
		Force:                      opts.Force,
		ProtectedRootIDs:           opts.ProtectedRootIDs,
		ProtectedSystemRootIDs:     opts.ProtectedSystemRootIDs,
	}
}

func (db *DB) validateLeafGenerationPackSelection(ctx context.Context, opts LeafGenerationPackOptions) (LeafGenerationPlan, error) {
	var selected LeafGenerationPlan
	if len(opts.GenerationIDs) == 0 {
		return selected, nil
	}
	plan, err := db.LeafGenerationPlan(ctx, leafGenerationPackPlanOptions(opts))
	if err != nil {
		return selected, err
	}
	byGenerationID := make(map[uint64]LeafGenerationPlanGeneration, len(plan.Generations))
	for _, gen := range plan.Generations {
		byGenerationID[gen.GenerationID] = gen
	}
	seen := make(map[uint64]struct{}, len(opts.GenerationIDs))
	selected = LeafGenerationPlan{Candidates: make([]LeafGenerationPlanGeneration, 0, len(opts.GenerationIDs))}
	for _, generationID := range opts.GenerationIDs {
		if generationID == 0 {
			continue
		}
		if _, ok := seen[generationID]; ok {
			continue
		}
		seen[generationID] = struct{}{}
		gen, ok := byGenerationID[generationID]
		if !ok {
			return selected, fmt.Errorf("leaf generation pack: generation %d not found", generationID)
		}
		if gen.WholeGenerationGCEligible {
			return selected, fmt.Errorf("leaf generation pack: generation %d is a whole-generation GC candidate; use leafgen-gc", generationID)
		}
		if !gen.Eligible {
			return selected, fmt.Errorf("leaf generation pack: generation %d ineligible: %s", generationID, gen.SkipReason)
		}
		selected.Candidates = append(selected.Candidates, gen)
		selected.CandidateGenerationIDs = append(selected.CandidateGenerationIDs, generationID)
		selected.CandidateBytesTotal += gen.BytesTotal
		selected.CandidateBytesLive += gen.BytesLive
		selected.CandidateBytesDead += gen.BytesDead
		selected.CandidateBytesToCopy += gen.BytesToCopy
		selected.CandidateLivePages += gen.LivePages
	}
	selected.ExpectedReclaimBytes = selected.CandidateBytesDead
	selected.ExpectedReclaimRatioPPM = ratioPPM(selected.CandidateBytesDead, selected.CandidateBytesTotal)
	selected.ExpectedReclaimPerByteCopiedPPM = ratioPPM(selected.CandidateBytesDead, selected.CandidateBytesToCopy)
	selected.Admission = leafGenerationPlanAdmission(leafGenerationPackPlanOptions(opts), selected)
	if selected.Admission != leafGenerationPlanAdmissionEligible {
		return selected, fmt.Errorf("leaf generation pack: selection admission=%s", selected.Admission)
	}
	return selected, nil
}

func (db *DB) resolveLeafGenerationPackSourceFileIDs(generationIDs []uint64) ([]uint32, int, error) {
	if len(generationIDs) == 0 {
		return nil, 0, nil
	}
	db.writeMu.RLock()
	manifest := db.leafGenerationManifest.clone()
	db.writeMu.RUnlock()
	if manifest == nil {
		return nil, 0, fmt.Errorf("leaf generation pack: manifest unavailable")
	}

	generationByID := make(map[uint64]leafGenerationRecord, len(manifest.Generations))
	for _, gen := range manifest.Generations {
		generationByID[gen.GenerationID] = gen
	}
	seenGenerations := make(map[uint64]struct{}, len(generationIDs))
	seenFiles := make(map[uint32]struct{}, len(generationIDs))
	out := make([]uint32, 0, len(generationIDs))
	matched := 0
	for _, generationID := range generationIDs {
		if generationID == 0 {
			continue
		}
		if _, seen := seenGenerations[generationID]; seen {
			continue
		}
		seenGenerations[generationID] = struct{}{}
		gen, ok := generationByID[generationID]
		if !ok {
			return nil, 0, fmt.Errorf("leaf generation pack: generation %d not found", generationID)
		}
		if gen.State != leafGenerationStateSealed {
			return nil, 0, fmt.Errorf("leaf generation pack: generation %d state=%q, want %q", generationID, gen.State, leafGenerationStateSealed)
		}
		matched++
		for _, rawID := range gen.FileIDs {
			if rawID == 0 {
				continue
			}
			if _, seen := seenFiles[rawID]; seen {
				continue
			}
			seenFiles[rawID] = struct{}{}
			out = append(out, rawID)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out, matched, nil
}

func noteCreatedLeafGenerationFileIDsInManifest(manifest *leafGenerationManifest, commitSeq uint64, fileIDs []uint32) ([]uint32, bool, error) {
	if manifest == nil || commitSeq == 0 || len(fileIDs) == 0 {
		return nil, false, nil
	}
	changed := false
	rawFileIDs := make([]uint32, 0, len(fileIDs))
	for _, fileID := range fileIDs {
		if !page.IsValueLogFileID(fileID) {
			continue
		}
		lane, _ := valuelog.DecodeFileID(fileID)
		if lane != rewriteLeafLogLaneID {
			continue
		}
		rawFileID := page.ValueLogSegmentID(fileID)
		registered, err := manifest.registerCurrentGenerationFileID(rawFileID, commitSeq)
		if err != nil {
			return nil, false, err
		}
		if registered {
			changed = true
			rawFileIDs = append(rawFileIDs, rawFileID)
		}
	}
	if !changed {
		return nil, false, nil
	}
	return rawFileIDs, true, nil
}

func (db *DB) persistLeafGenerationManifestAndRecordLengthIndexes(manifest *leafGenerationManifest, rawFileIDs []uint32) error {
	if db == nil || manifest == nil {
		return nil
	}
	rawFileIDs = dedupeLeafGenerationRawFileIDs(rawFileIDs)
	if err := db.replaceLeafGenerationManifest(manifest); err != nil {
		return err
	}
	db.persistLeafGenerationRecordLengthIndexes(rawFileIDs)
	return nil
}

func (db *DB) persistLeafGenerationRecordLengthIndexes(rawFileIDs []uint32) {
	if db == nil {
		return
	}
	rawFileIDs = dedupeLeafGenerationRawFileIDs(rawFileIDs)
	var firstErr error
	var firstRawFileID uint32
	failedCount := 0
	for _, rawFileID := range rawFileIDs {
		if err := db.persistLeafGenerationRecordLengthIndex(rawFileID); err != nil {
			if firstErr == nil {
				firstErr = err
				firstRawFileID = rawFileID
			}
			failedCount++
			continue
		}
	}
	if firstErr != nil {
		var reportErr error
		if failedCount == 1 {
			reportErr = fmt.Errorf("persist leaf generation record-length index for raw file %d: %w", firstRawFileID, firstErr)
		} else {
			reportErr = fmt.Errorf("persist leaf generation record-length indexes failed for %d files (first raw file %d): %w", failedCount, firstRawFileID, firstErr)
		}
		// Record-length sidecars are rebuildable optimization metadata. Keep the
		// manifest authoritative and surface the failure out-of-band.
		db.reportError(reportErr)
	}
}

func (db *DB) noteCreatedLeafGenerationFileIDs(commitSeq uint64, fileIDs []uint32) error {
	if db == nil || db.leafGenerationManifest == nil || commitSeq == 0 || len(fileIDs) == 0 {
		return nil
	}
	db.mu.RLock()
	baseManifest := db.leafGenerationManifest
	db.mu.RUnlock()
	if baseManifest == nil {
		return nil
	}
	nextManifest := baseManifest.clone()
	rawFileIDs, changed, err := noteCreatedLeafGenerationFileIDsInManifest(nextManifest, commitSeq, fileIDs)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if err := db.persistLeafGenerationManifestAndRecordLengthIndexes(nextManifest, rawFileIDs); err != nil {
		return err
	}
	db.mu.Lock()
	db.leafGenerationManifest = nextManifest
	db.mu.Unlock()
	return db.publishLeafGenerationState(false)
}

func dedupeLeafGenerationRawFileIDs(rawFileIDs []uint32) []uint32 {
	if len(rawFileIDs) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(rawFileIDs))
	seen := make(map[uint32]struct{}, len(rawFileIDs))
	for _, rawID := range rawFileIDs {
		if rawID == 0 {
			continue
		}
		if _, ok := seen[rawID]; ok {
			continue
		}
		seen[rawID] = struct{}{}
		out = append(out, rawID)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func filterLeafGenerationRawFileIDs(fileIDs []uint32) []uint32 {
	if len(fileIDs) == 0 {
		return nil
	}
	out := make([]uint32, 0, len(fileIDs))
	seen := make(map[uint32]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if !page.IsValueLogFileID(fileID) {
			continue
		}
		lane, _ := valuelog.DecodeFileID(fileID)
		if lane != rewriteLeafLogLaneID {
			continue
		}
		rawID := page.ValueLogSegmentID(fileID)
		if rawID == 0 {
			continue
		}
		if _, ok := seen[rawID]; ok {
			continue
		}
		seen[rawID] = struct{}{}
		out = append(out, rawID)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}
