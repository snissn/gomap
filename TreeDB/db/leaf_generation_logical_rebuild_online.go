package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/compression"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

var ErrLeafGenerationLogicalRebuildNoCandidate = errors.New("leaf logical rebuild: no eligible sealed single-file candidate")

type LeafGenerationLogicalRebuildRunOnceOptions struct {
	RawFileID             uint32
	MaxPublishedCommitSeq uint64
	Sync                  bool
	CatchupPassesMax      int
	CutoverMaxKeys        int
	CutoverMaxDefers      int
}

type LeafGenerationLogicalRebuildRunOnceStats struct {
	CommitSeqBefore uint64
	CommitSeqAfter  uint64

	SelectedGenerationID uint64
	SelectedRawFileID    uint32
	SelectedFileID       uint32
	SelectedRunCount     int
	SourceLeafPages      int
	SourceLeafBytes      int64

	ReplacementLeafPages int
	RecordsCopied        int
	BytesCopied          int64

	CreatedFileIDs       []uint32
	RetiredGenerationIDs []uint64

	CatchupPasses    int
	CatchupKeys      int
	FinalCutoverKeys int
	CutoverDefers    int

	WallTimeNanos int64
}

type leafLogicalRebuildCandidate struct {
	generationID uint64
	rawFileID    uint32
	fileID       uint32
	runRanges    [][2]int
	sourcePages  int
	sourceBytes  int64
}

type leafLogicalRebuildRunBuild struct {
	startIndex  int
	endIndex    int
	startKey    []byte
	endKey      []byte
	replacement []vacuumLeafChild
}

func normalizeLeafGenerationLogicalRebuildRunOnceOptions(opts LeafGenerationLogicalRebuildRunOnceOptions) LeafGenerationLogicalRebuildRunOnceOptions {
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

	baseSnap := db.AcquireSnapshot()
	if baseSnap == nil || baseSnap.idx == nil || baseSnap.state == nil || baseSnap.state.LeafGenerations == nil {
		closeRewriteSnapshot(&err, baseSnap)
		return stats, fmt.Errorf("leaf logical rebuild: missing snapshot state")
	}
	stats.CommitSeqBefore = baseSnap.state.CommitSeq

	candidate, baseChildren, err := db.selectLeafGenerationLogicalRebuildCandidate(baseSnap, opts.RawFileID, opts.MaxPublishedCommitSeq)
	if err != nil {
		closeRewriteSnapshot(&err, baseSnap)
		return stats, err
	}
	stats.SelectedGenerationID = candidate.generationID
	stats.SelectedRawFileID = candidate.rawFileID
	stats.SelectedFileID = candidate.fileID
	stats.SelectedRunCount = len(candidate.runRanges)
	stats.SourceLeafPages = candidate.sourcePages
	stats.SourceLeafBytes = candidate.sourceBytes
	stats.BytesCopied = candidate.sourceBytes

	runBuilds, recordsCopied, err := db.buildLogicalRebuildRuns(baseSnap, candidate, writer)
	if err != nil {
		closeRewriteSnapshot(&err, baseSnap)
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
	_ = baseSnap.Close()
	baseSnap = nil
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
		createdLeafBytes, err := leafGenerationLogicalRebuildCreatedBytes(createdSegments)
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
		}
		if createdLeafBytes >= candidate.sourceBytes {
			db.writeMu.Unlock()
			if cleanupErr := cleanupCreated(nil); cleanupErr != nil {
				return stats, cleanupErr
			}
			return stats, ErrLeafGenerationLogicalRebuildNoCandidate
		}
		createdIDs, err := writer.createdFileIDs()
		if err != nil {
			db.writeMu.Unlock()
			return stats, err
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
		retiredGenerationIDs, err := retireWholeLeafGenerationInManifest(stagedManifest, candidate.generationID, nextMeta.CommitSeq)
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

func (db *DB) selectLeafGenerationLogicalRebuildCandidate(snap *Snapshot, requestedRawFileID uint32, maxPublishedCommitSeq uint64) (leafLogicalRebuildCandidate, []vacuumLeafChild, error) {
	var zero leafLogicalRebuildCandidate
	if snap == nil || snap.idx == nil || snap.state == nil || snap.state.LeafGenerations == nil {
		return zero, nil, fmt.Errorf("leaf logical rebuild: missing snapshot state")
	}
	rootID := snap.state.RootPageID
	var children []vacuumLeafChild
	if _, ok := page.DecodeLeafRef(rootID); ok {
		children = []vacuumLeafChild{{key: []byte{}, childID: rootID}}
	} else {
		var err error
		children, err = vacuumCollectLeafRefChildren(snap.idx.pager, rootID)
		if err != nil {
			return zero, nil, err
		}
	}
	if len(children) == 0 {
		return zero, nil, ErrLeafGenerationLogicalRebuildNoCandidate
	}

	type fileAccum struct {
		generationID uint64
		runRanges    [][2]int
		sourcePages  int
		sourceBytes  int64
	}
	accumByRaw := make(map[uint32]*fileAccum)
	view := snap.state.LeafGenerations
	manifestByGen := make(map[uint64]leafGenerationRecord, len(db.leafGenerationManifest.Generations))
	if db.leafGenerationManifest != nil {
		for _, gen := range db.leafGenerationManifest.Generations {
			manifestByGen[gen.GenerationID] = gen
		}
	}

	currentRaw := uint32(0)
	runStart := -1
	finishRun := func(end int) {
		if runStart < 0 || currentRaw == 0 {
			runStart = -1
			currentRaw = 0
			return
		}
		accum := accumByRaw[currentRaw]
		if accum != nil {
			accum.runRanges = append(accum.runRanges, [2]int{runStart, end})
		}
		runStart = -1
		currentRaw = 0
	}

	for i, child := range children {
		ptr, ok := page.DecodeLeafRef(child.childID)
		if !ok {
			return zero, nil, fmt.Errorf("leaf logical rebuild: child %d is not a leaf ref", i)
		}
		rawFileID := page.ValueLogSegmentID(ptr.ValueLogFileID())
		generationID := view.FileToGeneration[rawFileID]
		gen, ok := manifestByGen[generationID]
		if !ok || gen.State != leafGenerationStateSealed || len(gen.FileIDs) != 1 {
			if rawFileID == currentRaw {
				finishRun(i - 1)
			}
			continue
		}
		if maxPublishedCommitSeq != 0 && gen.PublishedCommitSeq > maxPublishedCommitSeq {
			if rawFileID == currentRaw {
				finishRun(i - 1)
			}
			continue
		}
		if requestedRawFileID != 0 && rawFileID != requestedRawFileID {
			if rawFileID == currentRaw {
				finishRun(i - 1)
			}
			continue
		}
		accum := accumByRaw[rawFileID]
		if accum == nil {
			accum = &fileAccum{
				generationID: generationID,
				sourceBytes:  leafGenerationLogicalRebuildFileSize(db.dir, rawFileID),
			}
			accumByRaw[rawFileID] = accum
		}
		accum.sourcePages++
		if currentRaw != rawFileID {
			if currentRaw != 0 {
				finishRun(i - 1)
			}
			currentRaw = rawFileID
			runStart = i
		}
	}
	if currentRaw != 0 {
		finishRun(len(children) - 1)
	}

	best := zero
	bestPages := -1
	bestBytes := int64(-1)
	for rawFileID, accum := range accumByRaw {
		if accum == nil || len(accum.runRanges) == 0 || accum.sourcePages == 0 {
			continue
		}
		if accum.sourcePages > bestPages || (accum.sourcePages == bestPages && accum.sourceBytes > bestBytes) {
			best = leafLogicalRebuildCandidate{
				generationID: accum.generationID,
				rawFileID:    rawFileID,
				fileID:       page.ValueLogFileID(rawFileID),
				runRanges:    append([][2]int(nil), accum.runRanges...),
				sourcePages:  accum.sourcePages,
				sourceBytes:  accum.sourceBytes,
			}
			bestPages = accum.sourcePages
			bestBytes = accum.sourceBytes
		}
	}
	if best.rawFileID == 0 {
		return zero, nil, ErrLeafGenerationLogicalRebuildNoCandidate
	}
	return best, children, nil
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

func leafGenerationLogicalRebuildCreatedBytes(segments []rewriteCreatedSegment) (int64, error) {
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
	}
	return total, nil
}

func (db *DB) buildLogicalRebuildRuns(baseSnap *Snapshot, candidate leafLogicalRebuildCandidate, writer *rewriteWriter) ([]leafLogicalRebuildRunBuild, int, error) {
	if baseSnap == nil || baseSnap.idx == nil {
		return nil, 0, fmt.Errorf("leaf logical rebuild: missing base snapshot")
	}
	if len(candidate.runRanges) == 0 {
		return nil, 0, fmt.Errorf("leaf logical rebuild: candidate has no runs")
	}
	rootID := baseSnap.state.RootPageID
	var baseChildren []vacuumLeafChild
	if _, ok := page.DecodeLeafRef(rootID); ok {
		baseChildren = []vacuumLeafChild{{key: []byte{}, childID: rootID}}
	} else {
		var err error
		baseChildren, err = vacuumCollectLeafRefChildren(baseSnap.idx.pager, rootID)
		if err != nil {
			return nil, 0, err
		}
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
