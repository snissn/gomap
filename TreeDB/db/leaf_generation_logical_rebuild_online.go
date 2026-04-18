package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

var ErrLeafGenerationLogicalRebuildNoCandidate = errors.New("leaf logical rebuild: no eligible sealed incremental candidate")

type LeafGenerationLogicalRebuildRunOnceOptions struct {
	RawFileID             uint32
	MaxPublishedCommitSeq uint64
	ClusterFilesMax       int
	CandidateTryMax       int
	PilotSamplePages      int
	Sync                  bool
	CatchupPassesMax      int
	CutoverMaxKeys        int
	CutoverMaxDefers      int
}

type LeafGenerationLogicalRebuildRunOnceStats struct {
	CommitSeqBefore uint64
	CommitSeqAfter  uint64

	SelectedGenerationID  uint64
	SelectedRawFileID     uint32
	SelectedFileID        uint32
	SelectedGenerationIDs []uint64
	SelectedRawFileIDs    []uint32
	SelectedFileIDs       []uint32
	SelectedSourceFiles   int
	SelectedRunCount      int
	SourceLeafPages       int
	SourceLeafBytes       int64

	ReplacementLeafPages int
	RecordsCopied        int
	BytesCopied          int64

	CreatedFileIDs       []uint32
	RetiredGenerationIDs []uint64

	CandidateAttempts int
	CatchupPasses     int
	CatchupKeys       int
	FinalCutoverKeys  int
	CutoverDefers     int

	WallTimeNanos int64
}

type leafLogicalRebuildSource struct {
	generationID uint64
	rawFileID    uint32
	fileID       uint32
	firstIndex   int
	lastIndex    int
	ranges       [][2]int
	sourcePages  int
	sourceBytes  int64
	retireable   bool
}

type leafLogicalRebuildCandidate struct {
	generationIDs []uint64
	rawFileIDs    []uint32
	fileIDs       []uint32
	runRanges     [][2]int
	windowPages   int
	sourcePages   int
	sourceBytes   int64
}

type leafLogicalRebuildRunBuild struct {
	startIndex  int
	endIndex    int
	startKey    []byte
	endKey      []byte
	replacement []vacuumLeafChild
}

type leafLogicalRebuildPilotEstimate struct {
	sourceTotalBytes    int64
	samplePages         int
	sampleSourceBytes   int64
	sampleCreatedBytes  int64
	estimatedCreatedBytes int64
	minSavingsBytes     int64
}

func normalizeLeafGenerationLogicalRebuildRunOnceOptions(opts LeafGenerationLogicalRebuildRunOnceOptions) LeafGenerationLogicalRebuildRunOnceOptions {
	if opts.ClusterFilesMax <= 0 {
		opts.ClusterFilesMax = 4
	}
	if opts.CandidateTryMax <= 0 {
		opts.CandidateTryMax = 8
	}
	if opts.PilotSamplePages <= 0 {
		opts.PilotSamplePages = 192
	}
	if opts.CatchupPassesMax <= 0 {
		opts.CatchupPassesMax = vacuumCatchupPassesMax
	}
	if opts.CutoverMaxKeys <= 0 {
		opts.CutoverMaxKeys = vacuumCutoverMaxKeys
	}
	if opts.CutoverMaxDefers <= 0 {
		opts.CutoverMaxDefers = vacuumCutoverMaxDefers
	}
	return opts
}

func (db *DB) LeafGenerationLogicalRebuildRunOnce(ctx context.Context, opts LeafGenerationLogicalRebuildRunOnceOptions) (stats LeafGenerationLogicalRebuildRunOnceStats, err error) {
	if db == nil {
		return stats, fmt.Errorf("missing db")
	}
	if db.readOnly {
		return stats, ErrReadOnly
	}
	if !db.indexOuterLeavesInValueLog {
		return stats, nil
	}
	if db.valueLogManager == nil {
		return stats, fmt.Errorf("leaf logical rebuild: value log manager unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime.GOOS == "windows" {
		return stats, ErrVacuumUnsupported
	}
	opts = normalizeLeafGenerationLogicalRebuildRunOnceOptions(opts)

	started := time.Now()
	defer func() {
		stats.WallTimeNanos = time.Since(started).Nanoseconds()
	}()

	db.maintenanceMu.Lock()
	defer db.maintenanceMu.Unlock()
	if !db.vacuumInProgress.CompareAndSwap(false, true) {
		return stats, ErrVacuumInProgress
	}
	defer db.vacuumInProgress.Store(false)

	if err := ctx.Err(); err != nil {
		return stats, err
	}
	if err := db.publishValueLogSetNoRefresh(); err != nil {
		return stats, err
	}

	indexPath := filepath.Join(db.dir, indexFileName)
	newPath := filepath.Join(db.dir, indexNewFileName)
	bakPath := filepath.Join(db.dir, indexBakFileName)
	readyPath := filepath.Join(db.dir, indexReadyFileName)

	baseSnap := db.AcquireSnapshot()
	if baseSnap == nil || baseSnap.idx == nil || baseSnap.state == nil || baseSnap.state.LeafGenerations == nil {
		return stats, fmt.Errorf("leaf logical rebuild: missing snapshot state")
	}
	defer func() { _ = baseSnap.Close() }()
	stats.CommitSeqBefore = baseSnap.state.CommitSeq

	candidates, baseChildren, err := db.selectLeafGenerationLogicalRebuildCandidates(baseSnap, opts.RawFileID, opts.MaxPublishedCommitSeq, opts.ClusterFilesMax)
	if err != nil {
		return stats, err
	}
	if len(candidates) == 0 {
		return stats, ErrLeafGenerationLogicalRebuildNoCandidate
	}
	grouped := make(map[int][]leafLogicalRebuildCandidate)
	widths := make([]int, 0, 8)
	for _, candidate := range candidates {
		width := len(candidate.rawFileIDs)
		if _, ok := grouped[width]; !ok {
			widths = append(widths, width)
		}
		grouped[width] = append(grouped[width], candidate)
	}
	sort.Ints(widths)

	attempts := 0
	for _, width := range widths {
		group := grouped[width]
		limit := len(group)
		if opts.CandidateTryMax > 0 && limit > opts.CandidateTryMax {
			limit = opts.CandidateTryMax
		}
		for i := 0; i < limit; i++ {
			attempts++
			estimate, err := db.estimateLeafGenerationLogicalRebuildCandidate(baseSnap, baseChildren, group[i], opts.PilotSamplePages)
			if err != nil {
				return stats, err
			}
			if !estimate.admit() {
				continue
			}
			stats, err = db.tryLeafGenerationLogicalRebuildCandidate(
				ctx,
				opts,
				baseSnap,
				baseChildren,
				group[i],
				indexPath,
				newPath,
				bakPath,
				readyPath,
			)
			if err == nil {
				stats.CandidateAttempts = attempts
				return stats, nil
			}
			if !errors.Is(err, ErrLeafGenerationLogicalRebuildNoCandidate) {
				return stats, err
			}
		}
	}
	return stats, ErrLeafGenerationLogicalRebuildNoCandidate
}

func (e leafLogicalRebuildPilotEstimate) admit() bool {
	if e.sourceTotalBytes == 0 || e.sampleSourceBytes == 0 || e.sampleCreatedBytes == 0 {
		return false
	}
	return e.estimatedCreatedBytes <= e.sourceTotalBytes-e.minSavingsBytes
}

func (db *DB) estimateLeafGenerationLogicalRebuildCandidate(baseSnap *Snapshot, baseChildren []vacuumLeafChild, candidate leafLogicalRebuildCandidate, pilotSamplePages int) (leafLogicalRebuildPilotEstimate, error) {
	var estimate leafLogicalRebuildPilotEstimate
	if baseSnap == nil || baseSnap.state == nil {
		return estimate, fmt.Errorf("leaf logical rebuild: missing base snapshot")
	}
	if len(candidate.runRanges) == 0 || candidate.windowPages == 0 {
		return estimate, nil
	}
	sourceTotalBytes, err := leafGenerationLogicalRebuildSourceTotalBytes(db.dir, candidate.rawFileIDs)
	if err != nil {
		return estimate, err
	}
	estimate.sourceTotalBytes = sourceTotalBytes
	estimate.minSavingsBytes = max(int64(1<<20), sourceTotalBytes/100)
	sampleRanges := leafGenerationLogicalRebuildSampleRanges(candidate.runRanges, pilotSamplePages)
	if len(sampleRanges) == 0 {
		return estimate, nil
	}
	sampleSourceBytes, err := db.leafGenerationLogicalRebuildSampleSourceBytes(baseSnap, baseChildren, sampleRanges)
	if err != nil {
		return estimate, err
	}
	estimate.sampleSourceBytes = sampleSourceBytes
	for _, rr := range sampleRanges {
		estimate.samplePages += rr[1] - rr[0] + 1
	}
	if sampleSourceBytes == 0 {
		return estimate, nil
	}
	sampleCreatedBytes, err := db.pilotLeafGenerationLogicalRebuildCandidate(baseSnap, baseChildren, sampleRanges)
	if err != nil {
		return estimate, err
	}
	estimate.sampleCreatedBytes = sampleCreatedBytes
	if sampleCreatedBytes == 0 {
		return estimate, nil
	}
	if estimate.samplePages > 0 {
		estimate.estimatedCreatedBytes = int64(float64(candidate.windowPages) * (float64(sampleCreatedBytes) / float64(estimate.samplePages)))
	}
	return estimate, nil
}

func (db *DB) tryLeafGenerationLogicalRebuildCandidate(
	ctx context.Context,
	opts LeafGenerationLogicalRebuildRunOnceOptions,
	baseSnap *Snapshot,
	baseChildren []vacuumLeafChild,
	candidate leafLogicalRebuildCandidate,
	indexPath, newPath, bakPath, readyPath string,
) (stats LeafGenerationLogicalRebuildRunOnceStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}

	stats.CommitSeqBefore = baseSnap.state.CommitSeq
	if len(candidate.generationIDs) > 0 {
		stats.SelectedGenerationID = candidate.generationIDs[0]
		stats.SelectedGenerationIDs = append([]uint64(nil), candidate.generationIDs...)
	}
	if len(candidate.rawFileIDs) > 0 {
		stats.SelectedRawFileID = candidate.rawFileIDs[0]
		stats.SelectedRawFileIDs = append([]uint32(nil), candidate.rawFileIDs...)
	}
	if len(candidate.fileIDs) > 0 {
		stats.SelectedFileID = candidate.fileIDs[0]
		stats.SelectedFileIDs = append([]uint32(nil), candidate.fileIDs...)
	}
	stats.SelectedSourceFiles = len(candidate.rawFileIDs)
	stats.SelectedRunCount = len(candidate.runRanges)
	stats.SourceLeafPages = candidate.sourcePages
	stats.SourceLeafBytes = candidate.sourceBytes
	stats.BytesCopied = candidate.sourceBytes

	_ = os.Remove(newPath)
	_ = os.Remove(readyPath)
	newPager, err := pager.Open(newPath, db.chunkSize)
	if err != nil {
		return stats, err
	}
	pagerOpen := true
	cleanupNewPager := func() {
		if pagerOpen {
			_ = newPager.Close()
		}
		_ = os.Remove(newPath)
		_ = os.Remove(readyPath)
	}
	defer func() {
		if err != nil {
			cleanupNewPager()
		}
	}()
	if _, err = newPager.Alloc(2); err != nil {
		return stats, err
	}
	newAlloc := freelist.New(newPager, 0)
	newAlloc.SetPreferAppend(db.preferAppendAlloc)
	newAlloc.SetFreelistRegion(db.freelistRegionPages, db.freelistRegionRadius)

	newZ := zipper.New(newPager, newAlloc)
	newZ.SetFillTargets(db.leafFillTargetPPM, db.internalFillTargetPPM)
	newZ.SetPiggybackCompaction(db.piggybackCompaction)
	newZ.SetLeafPrefixCompression(db.leafPrefixCompression)
	newZ.SetIndexColumnarLeaves(db.indexColumnarLeaves)
	newZ.SetIndexPackedValuePtr(db.indexPackedValuePtr)
	newZ.SetIndexInternalBaseDelta(db.indexInternalBaseDelta)
	newZ.SetAdaptiveLeafEncoding(db.indexAdaptiveLeafEncoding)
	newZ.SetMaintenanceOpsPerCoalesce(db.maintenanceOpsPerCoalesce)
	newZ.SetLeafPageReader(db.valueLogManager)
	newZ.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
	db.idxMu.Lock()
	parallelMergePressureSource := db.zipperParallelMergeSource
	db.idxMu.Unlock()
	newZ.SetParallelMergePressureSource(parallelMergePressureSource)

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
		return stats, fmt.Errorf("leaf logical rebuild: value-log set unavailable")
	}
	leafStartSeq := maxRewriteLaneSeqFromSet(set, rewriteLeafLogLaneID)
	nextRID, err := nextRewriteRIDStartFromSet(set)
	_ = db.valueLogManager.Release(set)
	if err != nil {
		return stats, err
	}

	layout := resolveStorageLayout(db.dir)
	writer := newRewriteWriter(layout.valueVLogDir, 0, 0, 0)
	writer.ConfigureLeafLog(layout.leafVLogDir, rewriteLeafLogLaneID, leafStartSeq)
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	writer.nextRID = nextRID
	writerOpen := true
	cleanupCreated := func(baseErr error) error {
		closeErr := error(nil)
		if writerOpen {
			closeErr = writer.Close()
			writerOpen = false
		}
		createdSegments, segErr := writer.createdSegmentsSnapshot()
		if segErr != nil {
			return errors.Join(baseErr, closeErr, segErr)
		}
		cleanupErr := db.cleanupRewriteCreatedSegments(createdSegments)
		return errors.Join(baseErr, closeErr, cleanupErr)
	}
	defer func() {
		if err != nil {
			err = cleanupCreated(err)
		} else if writerOpen {
			_ = writer.Close()
		}
	}()

	if writer.blockCompression {
		state := db.state.Load()
		if state != nil {
			leafDictID, leafDictBytes, leafDictUseRawPages, dictErr := prepareRewriteLeafDict(db, state, db.valueLogDictCurrentForClass, db.valueLogDictLeafPayloadMode, db.valueLogDictLookup, db.valueLogDictPut, db.valueLogDictSetCurrentForClass, db.valueLogDictSetLeafPayloadMode, compression.TrainConfig{})
			if dictErr != nil {
				return stats, dictErr
			}
			if leafDictID != 0 && len(leafDictBytes) > 0 {
				writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
			}
		}
	}
	newZ.SetLeafPageLog(writer)

	db.vacuum.Start()
	defer db.vacuum.Stop()

	runBuilds, recordsCopied, err := db.buildLogicalRebuildRuns(baseSnap, baseChildren, candidate, writer)
	if err != nil {
		return stats, err
	}
	stats.RecordsCopied = recordsCopied

	modifiedChildren := append([]vacuumLeafChild(nil), baseChildren...)
	shift := 0
	for _, runBuild := range runBuilds {
		startIndex := runBuild.startIndex + shift
		endIndex := runBuild.endIndex + shift
		beforeLen := endIndex - startIndex + 1
		modifiedChildren = slices.Replace(modifiedChildren, startIndex, endIndex+1, runBuild.replacement...)
		shift += len(runBuild.replacement) - beforeLen
		stats.ReplacementLeafPages += len(runBuild.replacement)
	}

	newRoot, err := leafGenerationLogicalRebuildRootFromChildren(newPager, newAlloc, modifiedChildren, db.indexInternalBaseDelta)
	if err != nil {
		return stats, err
	}

	applyCatchup := func(opsMap map[string]batch.Entry) error {
		var retired []uint64
		newRoot, retired, err = db.applyVacuumDelta(newRoot, opsMap, newZ, nil)
		if err != nil {
			return err
		}
		for _, id := range retired {
			_ = newAlloc.Free(id)
		}
		return nil
	}

	for pass := 0; pass < opts.CatchupPassesMax; pass++ {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		opsMap := db.vacuum.Drain()
		if len(opsMap) == 0 {
			break
		}
		stats.CatchupPasses++
		stats.CatchupKeys += len(opsMap)
		if err := applyCatchup(opsMap); err != nil {
			return stats, err
		}
		if err := writer.Flush(); err != nil {
			return stats, err
		}
		if len(opsMap) <= vacuumCatchupKeyTarget {
			break
		}
	}

	defers := 0
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		db.writeMu.Lock()
		db.vacuum.Stop()
		finalOps := db.vacuum.Drain()
		stats.FinalCutoverKeys = len(finalOps)
		if len(finalOps) > opts.CutoverMaxKeys && defers < opts.CutoverMaxDefers {
			db.vacuum.Start()
			db.writeMu.Unlock()
			defers++
			stats.CutoverDefers++
			if err := applyCatchup(finalOps); err != nil {
				return stats, err
			}
			if err := writer.Flush(); err != nil {
				return stats, err
			}
			continue
		}
		if len(finalOps) > 0 {
			if err := applyCatchup(finalOps); err != nil {
				db.writeMu.Unlock()
				return stats, err
			}
		}

		if err := writer.Sync(); err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		createdSegments, err := writer.createdSegmentsSnapshot()
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		createdIDs, err := writer.createdFileIDs()
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		sourceTotalBytes, err := leafGenerationLogicalRebuildSourceTotalBytes(db.dir, candidate.rawFileIDs)
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		createdTotalBytes, err := leafGenerationLogicalRebuildCreatedTotalBytes(createdSegments)
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		minSavingsBytes := max(int64(1<<20), sourceTotalBytes/100)
		if createdTotalBytes > sourceTotalBytes-minSavingsBytes {
			db.writeMu.Unlock()
			if cleanupErr := cleanupCreated(nil); cleanupErr != nil {
				return stats, cleanupErr
			}
			return stats, ErrLeafGenerationLogicalRebuildNoCandidate
		}
		for _, seg := range createdSegments {
			if err := db.valueLogManager.RegisterSegment(seg.path, seg.fileID); err != nil {
				db.writeMu.Unlock()
				return stats, cleanupCreated(err)
			}
		}

		db.mu.RLock()
		oldGen := db.idx.Load()
		state := db.state.Load()
		baseMeta := db.meta
		currentManifest := db.leafGenerationManifest
		db.mu.RUnlock()
		if oldGen == nil || state == nil || currentManifest == nil {
			db.writeMu.Unlock()
			return stats, fmt.Errorf("leaf logical rebuild: missing db state")
		}

		newSysRoot, err := vacuumClonePagerTreeWithLeafRefs(oldGen.pager, state.SystemRootPageID, newAlloc, newPager)
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}

		nextMeta := baseMeta
		nextMeta.CommitSeq++
		nextMeta.UserRootPageID = newRoot
		nextMeta.SystemRootPageID = newSysRoot
		nextMeta.FreelistHeadID = newAlloc.Head()
		nextMeta.TotalPages = newPager.PageCount()
		stats.CommitSeqAfter = nextMeta.CommitSeq

		stagedManifest := currentManifest.clone()
		rawCreated, changed, err := noteCreatedLeafGenerationFileIDsInManifest(stagedManifest, nextMeta.CommitSeq, createdIDs)
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		if !changed {
			db.writeMu.Unlock()
			return stats, fmt.Errorf("leaf logical rebuild: created no new leaf generations")
		}
		retiredGenerationIDs, err := retireLeafGenerationsInManifest(stagedManifest, candidate.generationIDs, nextMeta.CommitSeq)
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		stats.RetiredGenerationIDs = append(stats.RetiredGenerationIDs[:0], retiredGenerationIDs...)

		if err := writeMetaToPager(newPager, MetaPage0ID, nextMeta); err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		if err := writeMetaToPager(newPager, MetaPage1ID, nextMeta); err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		if err := newPager.Sync(); err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		if err := os.WriteFile(readyPath, []byte("ready\n"), 0o644); err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		if runtime.GOOS != "windows" {
			if dir, err := os.Open(db.dir); err == nil {
				_ = dir.Sync()
				_ = dir.Close()
			}
		}

		_ = os.Remove(bakPath)
		if err := os.Rename(indexPath, bakPath); err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		if err := os.Rename(newPath, indexPath); err != nil {
			_ = os.Rename(bakPath, indexPath)
			db.writeMu.Unlock()
			return stats, err
		}
		_ = os.Remove(readyPath)
		_ = os.Remove(bakPath)
		if runtime.GOOS != "windows" {
			if dir, err := os.Open(db.dir); err == nil {
				_ = dir.Sync()
				_ = dir.Close()
			}
		}

		newZ.SetLeafPageReader(db.valueLogManager)
		newZ.SetLeafPageLog(db.leafPageLog)
		newZ.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)

		newGen := newIndexGen(db.nextIndexID(), newPager, newAlloc, newZ)
		db.trackIndex(newGen)

		var oldState *DBState
		db.mu.Lock()
		oldState = db.state.Load()
		db.idx.Store(newGen)
		db.meta = nextMeta
		db.metaPageID = MetaPage0ID
		db.leafGenerationManifest = stagedManifest
		newState := &DBState{
			CommitSeq:                  nextMeta.CommitSeq,
			RootPageID:                 nextMeta.UserRootPageID,
			SystemRootPageID:           nextMeta.SystemRootPageID,
			ValueLogSet:                db.valueLogManager.CurrentSetNoRefresh(),
			LeafGenerations:            newLeafGenerationView(stagedManifest),
			LeafGenerationStateVersion: db.leafGenerationStateVersion + 1,
		}
		db.leafGenerationStateVersion++
		db.state.Store(newState)
		db.publishSnapshotView(newGen, newState, db.valueLogManager)
		db.mu.Unlock()
		db.clearLeafGenerationReachabilityCaches()
		db.writeMu.Unlock()

		if oldState != nil {
			_ = db.valueLogManager.Release(oldState.ValueLogSet)
		}
		db.releaseIndex(oldGen)
		pagerOpen = false

		if err := db.persistLeafGenerationManifestAndRecordLengthIndexes(stagedManifest, rawCreated); err != nil {
			return stats, err
		}
		writerOpen = false
		_ = writer.Close()
		stats.CreatedFileIDs = append([]uint32(nil), createdIDs...)
		return stats, nil
	}
}

func retireLeafGenerationsInManifest(manifest *leafGenerationManifest, generationIDs []uint64, commitSeq uint64) ([]uint64, error) {
	if len(generationIDs) == 0 {
		return nil, nil
	}
	retired := make([]uint64, 0, len(generationIDs))
	for _, generationID := range generationIDs {
		genRetired, err := retireWholeLeafGenerationInManifest(manifest, generationID, commitSeq)
		if err != nil {
			return nil, err
		}
		retired = append(retired, genRetired...)
	}
	return retired, nil
}

func retireWholeLeafGenerationInManifest(manifest *leafGenerationManifest, generationID, commitSeq uint64) ([]uint64, error) {
	if manifest == nil || generationID == 0 || commitSeq == 0 {
		return nil, nil
	}
	for i := range manifest.Generations {
		gen := &manifest.Generations[i]
		if gen.GenerationID != generationID {
			continue
		}
		if gen.State != leafGenerationStateSealed {
			return nil, fmt.Errorf("leaf logical rebuild: generation %d state=%q, want %q", generationID, gen.State, leafGenerationStateSealed)
		}
		gen.State = leafGenerationStateRetiring
		if commitSeq > gen.RetiredCommitSeq {
			gen.RetiredCommitSeq = commitSeq
		}
		return []uint64{generationID}, nil
	}
	return nil, fmt.Errorf("leaf logical rebuild: generation %d not found", generationID)
}

func (db *DB) selectLeafGenerationLogicalRebuildCandidates(snap *Snapshot, requestedRawFileID uint32, maxPublishedCommitSeq uint64, clusterFilesMax int) ([]leafLogicalRebuildCandidate, []vacuumLeafChild, error) {
	if snap == nil || snap.idx == nil || snap.state == nil || snap.state.LeafGenerations == nil {
		return nil, nil, fmt.Errorf("leaf logical rebuild: missing snapshot state")
	}
	rootID := snap.state.RootPageID
	var children []vacuumLeafChild
	if _, ok := page.DecodeLeafRef(rootID); ok {
		children = []vacuumLeafChild{{key: []byte{}, childID: rootID}}
	} else {
		var err error
		children, err = vacuumCollectLeafRefChildren(snap.idx.pager, rootID)
		if err != nil {
			return nil, nil, err
		}
	}
	if len(children) == 0 {
		return nil, nil, ErrLeafGenerationLogicalRebuildNoCandidate
	}
	systemRawFileIDs, err := leafGenerationLogicalRebuildRootRawFileIDs(snap.idx.pager, snap.state.SystemRootPageID)
	if err != nil {
		return nil, nil, err
	}

	accumByRaw := make(map[uint32]*leafLogicalRebuildSource)
	orderedRaw := make([]uint32, 0, 64)
	view := snap.state.LeafGenerations
	manifestByGen := make(map[uint64]leafGenerationRecord, len(db.leafGenerationManifest.Generations))
	if db.leafGenerationManifest != nil {
		for _, gen := range db.leafGenerationManifest.Generations {
			manifestByGen[gen.GenerationID] = gen
		}
	}

	for i, child := range children {
		ptr, ok := page.DecodeLeafRef(child.childID)
		if !ok {
			return nil, nil, fmt.Errorf("leaf logical rebuild: child %d is not a leaf ref", i)
		}
		rawFileID := page.ValueLogSegmentID(ptr.ValueLogFileID())
		generationID := view.FileToGeneration[rawFileID]
		gen, ok := manifestByGen[generationID]
		accum := accumByRaw[rawFileID]
		if accum == nil {
			accum = &leafLogicalRebuildSource{
				generationID: generationID,
				rawFileID:    rawFileID,
				fileID:       page.ValueLogFileID(rawFileID),
				firstIndex:   i,
				lastIndex:    i,
				ranges:       [][2]int{{i, i}},
				sourceBytes:  leafGenerationLogicalRebuildFileSize(db.dir, rawFileID),
			}
			accumByRaw[rawFileID] = accum
			orderedRaw = append(orderedRaw, rawFileID)
		} else {
			lastRange := &accum.ranges[len(accum.ranges)-1]
			if i <= lastRange[1]+1 {
				lastRange[1] = i
			} else {
				accum.ranges = append(accum.ranges, [2]int{i, i})
			}
		}
		accum.lastIndex = i
		accum.sourcePages++
		if _, blocked := systemRawFileIDs[rawFileID]; blocked {
			continue
		}
		if !ok || gen.State != leafGenerationStateSealed || len(gen.FileIDs) != 1 {
			continue
		}
		if maxPublishedCommitSeq != 0 && gen.PublishedCommitSeq > maxPublishedCommitSeq {
			continue
		}
		if requestedRawFileID != 0 && rawFileID != requestedRawFileID {
			continue
		}
		accum.retireable = true
	}

	allSources := make([]leafLogicalRebuildSource, 0, len(orderedRaw))
	eligibleSources := make([]leafLogicalRebuildSource, 0, len(orderedRaw))
	for _, rawFileID := range orderedRaw {
		accum := accumByRaw[rawFileID]
		if accum == nil || accum.sourcePages == 0 || accum.firstIndex > accum.lastIndex {
			continue
		}
		allSources = append(allSources, *accum)
		if accum.retireable {
			eligibleSources = append(eligibleSources, *accum)
		}
	}
	candidates := buildLeafGenerationLogicalRebuildCandidates(allSources, eligibleSources, clusterFilesMax)
	if len(candidates) == 0 {
		return nil, nil, ErrLeafGenerationLogicalRebuildNoCandidate
	}
	return candidates, children, nil
}

func buildLeafGenerationLogicalRebuildCandidates(allSources, eligibleSources []leafLogicalRebuildSource, clusterFilesMax int) []leafLogicalRebuildCandidate {
	if len(allSources) == 0 || len(eligibleSources) == 0 {
		return nil
	}
	if clusterFilesMax <= 0 {
		clusterFilesMax = len(eligibleSources)
	}
	type windowKey string
	seen := make(map[windowKey]struct{}, len(allSources))
	out := make([]leafLogicalRebuildCandidate, 0, len(eligibleSources))
	for i := range eligibleSources {
		generationIDs := make([]uint64, 0, clusterFilesMax)
		rawFileIDs := make([]uint32, 0, clusterFilesMax)
		fileIDs := make([]uint32, 0, clusterFilesMax)
		runRanges := make([][2]int, 0, clusterFilesMax)
		sourcePages := 0
		sourceBytes := int64(0)
		for j := i; j < len(eligibleSources); j++ {
			src := eligibleSources[j]
			generationIDs = append(generationIDs, src.generationID)
			rawFileIDs = append(rawFileIDs, src.rawFileID)
			fileIDs = append(fileIDs, src.fileID)
			runRanges = append(runRanges, src.ranges...)
			sourcePages += src.sourcePages
			sourceBytes += src.sourceBytes
			if len(rawFileIDs) > clusterFilesMax {
				break
			}
			mergedRanges := mergeLeafLogicalRebuildRanges(runRanges)
			if len(mergedRanges) == 0 {
				continue
			}
			sortedRaw := append([]uint32(nil), rawFileIDs...)
			slices.Sort(sortedRaw)
			key := windowKey(leafLogicalRebuildWindowKey(sortedRaw, mergedRanges))
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, leafLogicalRebuildCandidate{
				generationIDs: append([]uint64(nil), generationIDs...),
				rawFileIDs:    sortedRaw,
				fileIDs:       append([]uint32(nil), fileIDs...),
				runRanges:     mergedRanges,
				windowPages:   leafLogicalRebuildRangePages(mergedRanges),
				sourcePages:   sourcePages,
				sourceBytes:   sourceBytes,
			})
		}
	}
	slices.SortFunc(out, func(a, b leafLogicalRebuildCandidate) int {
		if len(a.rawFileIDs) != len(b.rawFileIDs) {
			if len(a.rawFileIDs) < len(b.rawFileIDs) {
				return -1
			}
			return 1
		}
		if len(a.runRanges) != len(b.runRanges) {
			if len(a.runRanges) < len(b.runRanges) {
				return -1
			}
			return 1
		}
		aDensity := float64(a.sourceBytes)
		if a.windowPages > 0 {
			aDensity /= float64(a.windowPages)
		}
		bDensity := float64(b.sourceBytes)
		if b.windowPages > 0 {
			bDensity /= float64(b.windowPages)
		}
		if aDensity != bDensity {
			if aDensity > bDensity {
				return -1
			}
			return 1
		}
		if a.sourceBytes != b.sourceBytes {
			if a.sourceBytes > b.sourceBytes {
				return -1
			}
			return 1
		}
		if a.sourcePages != b.sourcePages {
			if a.sourcePages > b.sourcePages {
				return -1
			}
			return 1
		}
		if a.windowPages != b.windowPages {
			if a.windowPages < b.windowPages {
				return -1
			}
			return 1
		}
		return 0
	})
	return out
}

func mergeLeafLogicalRebuildRanges(in [][2]int) [][2]int {
	if len(in) == 0 {
		return nil
	}
	out := append([][2]int(nil), in...)
	slices.SortFunc(out, func(a, b [2]int) int {
		if a[0] < b[0] {
			return -1
		}
		if a[0] > b[0] {
			return 1
		}
		if a[1] < b[1] {
			return -1
		}
		if a[1] > b[1] {
			return 1
		}
		return 0
	})
	merged := out[:0]
	for _, rr := range out {
		if len(merged) == 0 {
			merged = append(merged, rr)
			continue
		}
		last := &merged[len(merged)-1]
		if rr[0] <= last[1]+1 {
			if rr[1] > last[1] {
				last[1] = rr[1]
			}
			continue
		}
		merged = append(merged, rr)
	}
	return merged
}

func leafLogicalRebuildRangePages(runRanges [][2]int) int {
	total := 0
	for _, rr := range runRanges {
		total += rr[1] - rr[0] + 1
	}
	return total
}

func leafLogicalRebuildWindowKey(rawFileIDs []uint32, runRanges [][2]int) string {
	return fmt.Sprintf("%v:%v", rawFileIDs, runRanges)
}

func leafGenerationLogicalRebuildRootRawFileIDs(p *pager.Pager, rootID uint64) (map[uint32]struct{}, error) {
	out := make(map[uint32]struct{})
	if p == nil || rootID == 0 {
		return out, nil
	}
	if _, ok := page.DecodeLeafRef(rootID); ok {
		out[page.ValueLogSegmentID(page.DecodeLeafRefID(rootID).ValueLogFileID())] = struct{}{}
		return out, nil
	}
	visited := make(map[uint64]struct{}, 128)
	var walk func(uint64) error
	walk = func(id uint64) error {
		if id == 0 {
			return nil
		}
		if _, ok := page.DecodeLeafRef(id); ok {
			out[page.ValueLogSegmentID(page.DecodeLeafRefID(id).ValueLogFileID())] = struct{}{}
			return nil
		}
		if _, seen := visited[id]; seen {
			return nil
		}
		visited[id] = struct{}{}
		data, err := p.Get(id)
		if err != nil {
			return err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				_, childID, err := n.GetInternalEntryView(i)
				if err != nil {
					return err
				}
				if err := walk(childID); err != nil {
					return err
				}
			}
		case page.PageTypeLeaf:
			// Pager-backed leaves in the System tree do not contribute outer-leaf
			// leafref files, so there is nothing to record here.
		}
		return nil
	}
	if err := walk(rootID); err != nil {
		return nil, err
	}
	return out, nil
}

func leafGenerationLogicalRebuildFileSize(dir string, rawFileID uint32) int64 {
	if dir == "" || rawFileID == 0 {
		return 0
	}
	path := leafGenerationFallbackPath(dir, rawFileID)
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func leafGenerationLogicalRebuildSourceTotalBytes(rootDir string, rawFileIDs []uint32) (int64, error) {
	var total int64
	for _, rawFileID := range rawFileIDs {
		logPath := leafGenerationFallbackPath(rootDir, rawFileID)
		if info, err := os.Stat(logPath); err == nil {
			total += info.Size()
		} else if !os.IsNotExist(err) {
			return 0, err
		}
		idxPath := leafGenerationRecordLengthIndexPath(rootDir, rawFileID)
		if info, err := os.Stat(idxPath); err == nil {
			total += info.Size()
		} else if !os.IsNotExist(err) {
			return 0, err
		}
	}
	return total, nil
}

func leafGenerationLogicalRebuildCreatedTotalBytes(segments []rewriteCreatedSegment) (int64, error) {
	var total int64
	for _, seg := range segments {
		if seg.path == "" {
			continue
		}
		info, err := os.Stat(seg.path)
		if err != nil {
			return 0, err
		}
		total += info.Size()
		idx, err := scanLeafGenerationRecordLengthIndexPath(seg.path, seg.fileID)
		if err != nil {
			return 0, err
		}
		total += leafGenerationRecordLengthIndexEncodedSize(idx)
	}
	return total, nil
}

func leafGenerationRecordLengthIndexEncodedSize(idx *leafGenerationRecordLengthIndex) int64 {
	if idx == nil {
		return 16
	}
	return int64(16 + idx.len()*8)
}

func leafGenerationLogicalRebuildSampleRanges(runRanges [][2]int, maxPages int) [][2]int {
	if maxPages <= 0 || len(runRanges) == 0 {
		return nil
	}
	totalPages := 0
	for _, rr := range runRanges {
		totalPages += rr[1] - rr[0] + 1
	}
	if totalPages == 0 {
		return nil
	}
	if totalPages <= maxPages {
		return append([][2]int(nil), runRanges...)
	}
	slicesN := 3
	perSlice := max(1, maxPages/slicesN)
	out := make([][2]int, 0, slicesN)
	first := runRanges[0]
	out = append(out, [2]int{first[0], min(first[1], first[0]+perSlice-1)})
	midRR := runRanges[len(runRanges)/2]
	midStart := midRR[0] + max(0, (midRR[1]-midRR[0]+1-perSlice)/2)
	out = append(out, [2]int{midStart, min(midRR[1], midStart+perSlice-1)})
	last := runRanges[len(runRanges)-1]
	lastEnd := last[1]
	lastStart := max(last[0], lastEnd-perSlice+1)
	out = append(out, [2]int{lastStart, lastEnd})
	slices.SortFunc(out, func(a, b [2]int) int {
		if a[0] < b[0] {
			return -1
		}
		if a[0] > b[0] {
			return 1
		}
		if a[1] < b[1] {
			return -1
		}
		if a[1] > b[1] {
			return 1
		}
		return 0
	})
	merged := out[:0]
	for _, rr := range out {
		if len(merged) == 0 {
			merged = append(merged, rr)
			continue
		}
		last := &merged[len(merged)-1]
		if rr[0] <= last[1]+1 {
			if rr[1] > last[1] {
				last[1] = rr[1]
			}
			continue
		}
		merged = append(merged, rr)
	}
	return merged
}

func (db *DB) leafGenerationLogicalRebuildSampleSourceBytes(baseSnap *Snapshot, baseChildren []vacuumLeafChild, sampleRanges [][2]int) (int64, error) {
	if db == nil || baseSnap == nil || baseSnap.state == nil || baseSnap.state.LeafGenerations == nil {
		return 0, fmt.Errorf("leaf logical rebuild: missing sample state")
	}
	var total int64
	for _, rr := range sampleRanges {
		for i := rr[0]; i <= rr[1]; i++ {
			if i < 0 || i >= len(baseChildren) {
				continue
			}
			ptr, ok := page.DecodeLeafRef(baseChildren[i].childID)
			if !ok {
				continue
			}
			length, ok, err := db.leafGenerationRecordLengthForPlan(ptr, baseSnap.state.ValueLogSet, baseSnap.state.LeafGenerations)
			if err != nil {
				return 0, err
			}
			if !ok {
				continue
			}
			total += int64(length)
		}
	}
	return total, nil
}

func (db *DB) pilotLeafGenerationLogicalRebuildCandidate(baseSnap *Snapshot, baseChildren []vacuumLeafChild, sampleRanges [][2]int) (int64, error) {
	if db == nil || baseSnap == nil {
		return 0, fmt.Errorf("leaf logical rebuild: missing pilot state")
	}
	set := db.valueLogManager.CurrentSetNoRefresh()
	if set == nil || len(set.Files) == 0 {
		if set != nil {
			_ = db.valueLogManager.Release(set)
		}
		if err := db.valueLogManager.Refresh(); err != nil {
			return 0, err
		}
		set = db.valueLogManager.CurrentSetNoRefresh()
	}
	if set == nil {
		return 0, fmt.Errorf("leaf logical rebuild: value-log set unavailable")
	}
	leafStartSeq := maxRewriteLaneSeqFromSet(set, rewriteLeafLogLaneID)
	nextRID, err := nextRewriteRIDStartFromSet(set)
	_ = db.valueLogManager.Release(set)
	if err != nil {
		return 0, err
	}

	tempLeafDir, err := os.MkdirTemp("", "treedb-leaf-logical-pilot-*")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(tempLeafDir) }()

	writer := newRewriteWriter(tempLeafDir, 0, 0, 0)
	writer.ConfigureLeafLog(tempLeafDir, rewriteLeafLogLaneID, leafStartSeq)
	writer.blockCompression = db.valueLogCompression != ValueLogCompressionOff
	writer.blockCodec = valuelogBlockCodecFromDB(db.valueLogBlockCodec)
	writer.leafBlockCodec = leafPageBlockCodecFromOptions(db.valueLogCompression, db.valueLogAutoPolicy, db.valueLogBlockCodec, db.indexOuterLeavesInValueLog)
	writer.nextRID = nextRID
	defer func() { _ = writer.Close() }()

	if writer.blockCompression {
		state := db.state.Load()
		if state != nil {
			leafDictID, leafDictBytes, leafDictUseRawPages, dictErr := prepareRewriteLeafDict(db, state, db.valueLogDictCurrentForClass, db.valueLogDictLeafPayloadMode, db.valueLogDictLookup, db.valueLogDictPut, db.valueLogDictSetCurrentForClass, db.valueLogDictSetLeafPayloadMode, compression.TrainConfig{})
			if dictErr != nil {
				return 0, dictErr
			}
			if leafDictID != 0 && len(leafDictBytes) > 0 {
				writer.SetLeafDictMode(leafDictID, leafDictBytes, leafDictUseRawPages)
			}
		}
	}

	sampleCandidate := leafLogicalRebuildCandidate{runRanges: sampleRanges}
	if _, _, err := db.buildLogicalRebuildRuns(baseSnap, baseChildren, sampleCandidate, writer); err != nil {
		return 0, err
	}
	if err := writer.Flush(); err != nil {
		return 0, err
	}
	segments, err := writer.createdSegmentsSnapshot()
	if err != nil {
		return 0, err
	}
	return leafGenerationLogicalRebuildCreatedTotalBytes(segments)
}

func (db *DB) buildLogicalRebuildRuns(baseSnap *Snapshot, baseChildren []vacuumLeafChild, candidate leafLogicalRebuildCandidate, writer *rewriteWriter) ([]leafLogicalRebuildRunBuild, int, error) {
	if baseSnap == nil || baseSnap.idx == nil {
		return nil, 0, fmt.Errorf("leaf logical rebuild: missing base snapshot")
	}
	if len(candidate.runRanges) == 0 {
		return nil, 0, fmt.Errorf("leaf logical rebuild: candidate has no runs")
	}
	if len(baseChildren) == 0 {
		return nil, 0, fmt.Errorf("leaf logical rebuild: missing base frontier")
	}
	out := make([]leafLogicalRebuildRunBuild, 0, len(candidate.runRanges))
	totalRecords := 0
	for _, rr := range candidate.runRanges {
		startIndex := rr[0]
		endIndex := rr[1]
		startKey, endKey := leafGenerationLogicalRebuildRunBounds(baseChildren, startIndex, endIndex)
		tempPath, err := leafGenerationLogicalRebuildTempPagerPath()
		if err != nil {
			return nil, 0, err
		}
		tempPager, err := pager.Open(tempPath, db.chunkSize)
		if err != nil {
			_ = os.Remove(tempPath)
			return nil, 0, err
		}
		if _, err := tempPager.Alloc(2); err != nil {
			_ = tempPager.Close()
			_ = os.Remove(tempPath)
			return nil, 0, err
		}
		alloc := &pagerAllocator{p: tempPager}
		beforeRecords := writer.records
		iter := baseSnap.tree.IteratorWithOptions(startKey, endKey, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		buildOpts := bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			PackedValuePtr:        db.indexPackedValuePtr,
			InternalBaseDelta:     db.indexInternalBaseDelta,
			LeafPageLog:           writer,
		}
		rootID, err := bulk.BuildWithOptions(iter, alloc, tempPager, buildOpts)
		_ = iter.Close()
		if err != nil {
			_ = tempPager.Close()
			_ = os.Remove(tempPath)
			return nil, 0, err
		}
		if err := writer.flushPendingDictBatch(); err != nil {
			_ = tempPager.Close()
			_ = os.Remove(tempPath)
			return nil, 0, err
		}
		children, err := leafGenerationLogicalRebuildChildrenForRoot(tempPager, rootID, startKey)
		if err != nil {
			_ = tempPager.Close()
			_ = os.Remove(tempPath)
			return nil, 0, err
		}
		out = append(out, leafLogicalRebuildRunBuild{
			startIndex:  startIndex,
			endIndex:    endIndex,
			startKey:    append([]byte(nil), startKey...),
			endKey:      append([]byte(nil), endKey...),
			replacement: children,
		})
		totalRecords += writer.records - beforeRecords
		_ = tempPager.Close()
		_ = os.Remove(tempPath)
	}
	return out, totalRecords, nil
}

func leafGenerationLogicalRebuildRunBounds(children []vacuumLeafChild, startIndex, endIndex int) ([]byte, []byte) {
	if startIndex < 0 || startIndex >= len(children) {
		return nil, nil
	}
	startKey := append([]byte(nil), children[startIndex].key...)
	if endIndex+1 < len(children) {
		return startKey, append([]byte(nil), children[endIndex+1].key...)
	}
	return startKey, nil
}

func leafGenerationLogicalRebuildTempPagerPath() (string, error) {
	f, err := os.CreateTemp("", "treedb-leaf-logical-run-*.index")
	if err != nil {
		return "", err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func leafGenerationLogicalRebuildChildrenForRoot(p *pager.Pager, rootID uint64, startKey []byte) ([]vacuumLeafChild, error) {
	if _, ok := page.DecodeLeafRef(rootID); ok {
		return []vacuumLeafChild{{
			key:     append([]byte(nil), startKey...),
			childID: rootID,
		}}, nil
	}
	return vacuumCollectLeafRefChildren(p, rootID)
}

func leafGenerationLogicalRebuildRootFromChildren(p *pager.Pager, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, children []vacuumLeafChild, internalBaseDelta bool) (uint64, error) {
	if len(children) == 0 {
		return 0, fmt.Errorf("leaf logical rebuild: no replacement children")
	}
	if len(children) == 1 {
		return children[0].childID, nil
	}
	return vacuumBuildInternalTreeFromChildren(p, alloc, children, internalBaseDelta)
}
