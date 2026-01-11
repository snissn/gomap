package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// IndexSwapCompactionOptions configures compaction that rebuilds the index into
// a second file and swaps it in atomically.
type IndexSwapCompactionOptions struct {
	// CopyBytesPerSec limits slab copy IO. 0 disables throttling.
	CopyBytesPerSec int64
	// CopyBurstBytes is the limiter burst size. 0 uses a 1-second burst.
	CopyBurstBytes int64

	// Assist is an optional hook invoked periodically during compaction work.
	// It must be fast and must not assume any DB locks are held.
	Assist func()

	// SampleCompressionDict enables a pre-pass that selects a representative slab
	// and trains a dictionary on its values for compaction analysis.
	SampleCompressionDict bool
	// ApplyCompressionShiftPlan uses the sampled shift points to bypass
	// compression for windows where the dict ratio degrades.
	ApplyCompressionShiftPlan bool
	// DisableCompressionIfBaseRatioGTE disables compression during compaction
	// when the sampled base ratio is greater than or equal to this threshold.
	// Set to 0 to disable.
	DisableCompressionIfBaseRatioGTE float64
	// ShiftWindowDivisor overrides the default shift window divisor. 0 uses the
	// built-in default.
	ShiftWindowDivisor int
	// ShiftMinWindowBytes overrides the default shift window minimum size. 0 uses
	// the built-in default.
	ShiftMinWindowBytes int
	// ShiftRatioTolerance controls how much worse a window's compression ratio
	// can be relative to the sample baseline before it is marked as a shift.
	// Negative values can be used for testing to mark more windows as shifts.
	ShiftRatioTolerance float64
	// ShiftRatioToleranceSet enables overriding the default ratio tolerance.
	ShiftRatioToleranceSet bool
	// ShiftMaxPoints overrides the maximum number of shift points kept for the
	// plan. 0 uses the built-in default.
	ShiftMaxPoints int

	// Stats captures timing and byte counters for the compaction run.
	Stats *IndexSwapCompactionStats
}

// IndexSwapCompactionStats summarizes compaction work for observability.
type IndexSwapCompactionStats struct {
	TotalNanos       uint64
	BuildNanos       uint64
	CatchupNanos     uint64
	FinalizeNanos    uint64
	SampleNanos      uint64
	SampleCandidates uint64

	RemapCount uint64
	RemapBytes uint64

	SlabWriteBytes int
	SlabDeadBytes  int

	SampleSlabID      uint32
	SampleBytes       uint64
	SampleRecords     uint64
	SampleDictBytes   uint64
	SampleDictHash    uint64
	SampleDictRatio   float64
	SampleBaseBytes   uint64
	SampleBaseStored  uint64
	SampleBaseRecords uint64
	SampleBaseRatio   float64

	SampleShiftPoints            uint64
	SampleShiftWorstRatio        float64
	SampleShiftAvgRatio          float64
	SampleShiftBytes             uint64
	SampleShiftRecords           uint64
	SampleShiftDictSampleBytes   uint64
	SampleShiftDictSampleRecords uint64
	SampleShiftDictBytes         uint64
	SampleShiftDictHash          uint64
	SampleShiftDictRatio         float64

	ShiftOverrideRecords uint64
	ShiftOverrideBytes   uint64
}

type compactionShiftTuning struct {
	windowDivisor  int
	minWindowBytes int
	ratioTolerance float64
	maxPoints      int
}

// CompactSlabsIndexSwap compacts one or more slab files by rebuilding the user
// index into a new file and swapping it in with a short writer pause.
//
// This avoids large COW B-Tree churn during compaction pointer updates by
// materializing the post-compaction pointer view directly in index.db.new.
//
// Behavior notes:
//   - The active slab cannot be compacted.
//   - Missing slab IDs are ignored (idempotent).
//   - Any concurrent writes are replayed into the new index, but only if the key
//     actually changed since the base snapshot (to avoid undoing pointer remaps).
func (db *DB) CompactSlabsIndexSwap(ctx context.Context, slabIDs []uint32, opts IndexSwapCompactionOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime.GOOS == "windows" {
		return ErrVacuumUnsupported
	}
	if !db.vacuumInProgress.CompareAndSwap(false, true) {
		return ErrVacuumInProgress
	}
	defer db.vacuumInProgress.Store(false)

	if db.dir == "" {
		return errors.New("compaction: missing db dir")
	}

	stats := opts.Stats
	var totalStart time.Time
	if stats != nil {
		totalStart = time.Now()
		defer func() {
			stats.TotalNanos = uint64(time.Since(totalStart))
		}()
	}

	activeID := db.slabManager.ActiveSlabID()
	targets := make(map[uint32]struct{}, len(slabIDs))
	for _, id := range slabIDs {
		if id == activeID {
			return fmt.Errorf("compaction: cannot compact active slab %d", id)
		}
		if _, ok := targets[id]; ok {
			continue
		}
		if _, err := os.Stat(db.slabManager.GetSlabPath(id)); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		targets[id] = struct{}{}
	}
	if len(targets) == 0 {
		return nil
	}

	indexPath := filepath.Join(db.dir, indexFileName)
	newPath := filepath.Join(db.dir, indexNewFileName)
	bakPath := filepath.Join(db.dir, indexBakFileName)
	readyPath := filepath.Join(db.dir, indexReadyFileName)

	// Clean up any previous partial artifacts (best-effort).
	_ = os.Remove(newPath)
	_ = os.Remove(readyPath)

	newPager, err := pager.Open(newPath, db.chunkSize)
	if err != nil {
		return err
	}
	cleanupNewPager := func() {
		_ = newPager.Close()
		_ = os.Remove(newPath)
		_ = os.Remove(readyPath)
	}

	if _, err := newPager.Alloc(2); err != nil {
		cleanupNewPager()
		return err
	}

	newAlloc := freelist.New(newPager, 0)
	newAlloc.SetPreferAppend(db.preferAppendAlloc)
	newAlloc.SetFreelistRegion(db.freelistRegionPages, db.freelistRegionRadius)

	newZ := zipper.New(newPager, newAlloc)
	newZ.SetFillTargets(db.leafFillTargetPPM, db.internalFillTargetPPM)
	newZ.SetPiggybackCompaction(db.piggybackCompaction)
	newZ.SetLeafPrefixCompression(db.leafPrefixCompression)

	db.vacuum.Start()
	defer db.vacuum.Stop()

	baseSnap := db.AcquireSnapshot()
	defer func() { _ = baseSnap.Close() }()

	shiftTuning := compactionShiftTuning{
		windowDivisor:  compactionShiftWindowDivisor,
		minWindowBytes: compactionShiftMinWindowBytes,
		ratioTolerance: compactionShiftRatioTolerance,
		maxPoints:      compactionShiftMaxPoints,
	}
	if opts.ShiftWindowDivisor > 0 {
		shiftTuning.windowDivisor = opts.ShiftWindowDivisor
	}
	if opts.ShiftMinWindowBytes > 0 {
		shiftTuning.minWindowBytes = opts.ShiftMinWindowBytes
	}
	if opts.ShiftRatioToleranceSet {
		shiftTuning.ratioTolerance = opts.ShiftRatioTolerance
	}
	if opts.ShiftMaxPoints > 0 {
		shiftTuning.maxPoints = opts.ShiftMaxPoints
	}
	if shiftTuning.windowDivisor <= 0 {
		shiftTuning.windowDivisor = 1
	}
	if shiftTuning.minWindowBytes <= 0 {
		shiftTuning.minWindowBytes = compactionShiftMinWindowBytes
	}
	if shiftTuning.maxPoints <= 0 {
		shiftTuning.maxPoints = compactionShiftMaxPoints
	}

	var (
		shiftOverride   *compactionShiftOverride
		disableAll      map[uint32]compactionDisableAllOverride
		baseRatioBySlab map[uint32]compactionBaseRatioSample
	)
	if opts.SampleCompressionDict && stats != nil && db.slabManager.Compression() == slab.CompressionZSTD {
		if cfg, ok := db.slabManager.CompressionTrainConfig(); ok && cfg.TrainBytes > 0 {
			sampleStart := time.Now()
			candidates, err := selectCompactionSampleSlabs(ctx, baseSnap, targets, 4)
			if err != nil {
				cleanupNewPager()
				return err
			}
			if len(candidates) > 0 {
				if opts.DisableCompressionIfBaseRatioGTE > 0 {
					disableAll = make(map[uint32]compactionDisableAllOverride, len(candidates))
					baseRatioBySlab = make(map[uint32]compactionBaseRatioSample, len(candidates))
				}
				stats.SampleCandidates = uint64(len(candidates))
				var (
					best       compactionDictSample
					bestID     uint32
					bestFound  bool
					dictFound  bool
					bestDict   compactionDictSample
					bestDictID uint32
				)
				samples, err := collectCompactionSampleSet(ctx, baseSnap, candidates, cfg, db.slabManager.EstimateCompression)
				if err != nil {
					cleanupNewPager()
					return err
				}
				for _, sampleSlabID := range candidates {
					bundle, ok := samples[sampleSlabID]
					if !ok {
						continue
					}
					sample := bundle.sample
					if !bestFound || sample.bytes > best.bytes {
						best = sample
						bestID = sampleSlabID
						bestFound = true
					}
					if sample.dictBytes > 0 && sample.dictRatio > 0 {
						if !dictFound || sample.dictRatio < bestDict.dictRatio {
							bestDict = sample
							bestDictID = sampleSlabID
							dictFound = true
						}
					}
					if opts.DisableCompressionIfBaseRatioGTE > 0 {
						baseRatioBySlab[sampleSlabID] = compactionBaseRatioSample{
							ratio:   bundle.baseRatio,
							raw:     bundle.baseRaw,
							stored:  bundle.baseStored,
							records: bundle.baseRecords,
						}
						if bundle.baseRatio >= opts.DisableCompressionIfBaseRatioGTE {
							disableAll[sampleSlabID] = compactionDisableAllOverride{
								maxRecordBytes: cfg.MaxRecordBytes,
							}
						}
					}
				}
				if dictFound {
					best = bestDict
					bestID = bestDictID
				}
				if bestFound {
					stats.SampleSlabID = bestID
					stats.SampleBytes = best.bytes
					stats.SampleRecords = best.records
					stats.SampleDictBytes = best.dictBytes
					stats.SampleDictHash = best.dictHash
					stats.SampleDictRatio = best.dictRatio

					var (
						baseRatio   float64
						baseRaw     uint64
						baseStored  uint64
						baseRecords uint64
						err         error
					)
					if sample, ok := baseRatioBySlab[bestID]; ok {
						baseRatio = sample.ratio
						baseRaw = sample.raw
						baseStored = sample.stored
						baseRecords = sample.records
					} else {
						baseRatio, baseRaw, baseStored, baseRecords, err = collectCompactionCompressionRatio(ctx, baseSnap, bestID, cfg, db.slabManager.EstimateCompression)
						if err != nil {
							cleanupNewPager()
							return err
						}
					}
					stats.SampleBaseBytes = baseRaw
					stats.SampleBaseStored = baseStored
					stats.SampleBaseRecords = baseRecords
					stats.SampleBaseRatio = baseRatio

					_, disableAllBest := disableAll[bestID]
					if !disableAllBest && baseRatio > 0 {
						shiftPlan, err := collectCompactionShiftPlanWithEstimator(ctx, baseSnap, bestID, cfg, baseRatio, shiftTuning, db.slabManager.EstimateCompression)
						if err != nil {
							cleanupNewPager()
							return err
						}
						stats.SampleShiftPoints = shiftPlan.shiftCount
						stats.SampleShiftWorstRatio = shiftPlan.worstRatio
						stats.SampleShiftAvgRatio = shiftPlan.avgRatio
						stats.SampleShiftBytes = shiftPlan.shiftBytes
						stats.SampleShiftRecords = shiftPlan.shiftRecords
						shiftSample, err := collectCompactionShiftDictSample(ctx, baseSnap, bestID, cfg, shiftPlan, db.slabManager.EstimateCompression)
						if err != nil {
							cleanupNewPager()
							return err
						}
						stats.SampleShiftDictSampleBytes = shiftSample.bytes
						stats.SampleShiftDictSampleRecords = shiftSample.records
						stats.SampleShiftDictBytes = shiftSample.dictBytes
						stats.SampleShiftDictHash = shiftSample.dictHash
						stats.SampleShiftDictRatio = shiftSample.dictRatio
						if opts.ApplyCompressionShiftPlan && len(shiftPlan.points) > 0 && shiftPlan.windowBytes > 0 {
							shiftOverride = &compactionShiftOverride{
								slabID:         bestID,
								plan:           shiftPlan,
								maxRecordBytes: cfg.MaxRecordBytes,
							}
						}
					}
				}
			}
			stats.SampleNanos = uint64(time.Since(sampleStart))
		}
	}

	lim := newIndexSwapLimiter(opts.CopyBytesPerSec, opts.CopyBurstBytes)
	remapIter := newIndexSwapRemapIterator(ctx, baseSnap.tree.Iterator(nil, nil), db, targets, lim, opts.Assist, shiftOverride, disableAll)
	buildStart := time.Now()
	newRoot, buildErr := bulk.BuildWithOptions(remapIter, newAlloc, newPager, bulk.BuildOptions{
		LeafPrefixCompression: db.leafPrefixCompression,
	})
	if stats != nil {
		stats.BuildNanos = uint64(time.Since(buildStart))
		stats.RemapCount = remapIter.remapCount
		stats.RemapBytes = remapIter.remapBytes
		stats.SlabWriteBytes = remapIter.metrics.SlabWriteBytes
		stats.SlabDeadBytes = remapIter.metrics.SlabDeadBytes
		stats.ShiftOverrideRecords = remapIter.shiftOverrideRecords
		stats.ShiftOverrideBytes = remapIter.shiftOverrideBytes
	}
	iterErr := remapIter.Error()
	_ = remapIter.Close()
	if buildErr != nil || iterErr != nil {
		cleanupNewPager()
		return errors.Join(buildErr, iterErr)
	}

	metrics := remapIter.metrics
	remap := remapIter.remap
	adjusted := make(map[page.ValuePtr]struct{}, 128)

	freeRetired := func(retired []uint64) {
		for _, id := range retired {
			_ = newAlloc.Free(id)
		}
	}

	// Online catch-up: replay recorded keys in bounded passes. Filter out keys
	// whose state did not change since the base snapshot so we never override a
	// slab-pointer remap with the old pointer value.
	catchupStart := time.Now()
	for pass := 0; pass < vacuumCatchupPassesMax; pass++ {
		if err := ctx.Err(); err != nil {
			cleanupNewPager()
			return err
		}
		ops := db.vacuum.Drain()
		if len(ops) == 0 {
			break
		}
		var retired []uint64
		newRoot, retired, err = db.applyIndexSwapDelta(newRoot, ops, newZ, baseSnap, targets, remap, adjusted, &metrics)
		if err != nil {
			cleanupNewPager()
			return err
		}
		freeRetired(retired)
		if len(ops) <= vacuumCatchupKeyTarget {
			break
		}
	}
	if stats != nil {
		stats.CatchupNanos = uint64(time.Since(catchupStart))
	}

	// Final cutover: stop recording, apply the tail, rebuild the System tree in
	// the new file, then swap index.db on disk and publish a new index generation.
	finalizeStart := time.Now()
	defers := 0
	for {
		if err := ctx.Err(); err != nil {
			cleanupNewPager()
			return err
		}

		db.writeMu.Lock()
		db.vacuum.Stop()
		finalOps := db.vacuum.Drain()
		if len(finalOps) > vacuumCutoverMaxKeys && defers < vacuumCutoverMaxDefers {
			db.vacuum.Start()
			db.writeMu.Unlock()
			defers++

			var retired []uint64
			newRoot, retired, err = db.applyIndexSwapDelta(newRoot, finalOps, newZ, baseSnap, targets, remap, adjusted, &metrics)
			if err != nil {
				cleanupNewPager()
				return err
			}
			freeRetired(retired)
			continue
		}

		if len(finalOps) > 0 {
			var retired []uint64
			newRoot, retired, err = db.applyIndexSwapDelta(newRoot, finalOps, newZ, baseSnap, targets, remap, adjusted, &metrics)
			if err != nil {
				db.writeMu.Unlock()
				cleanupNewPager()
				return err
			}
			freeRetired(retired)
		}

		db.mu.RLock()
		oldGen := db.idx.Load()
		state := db.state.Load()
		baseMeta := db.meta
		db.mu.RUnlock()
		if oldGen == nil || state == nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return errors.New("compaction: missing db state")
		}

		sysIter := tree.New(oldGen.pager, valueReader{slabs: state.SlabSet, vlogs: state.ValueLogSet}, state.SystemRootPageID).Iterator(nil, nil)
		newSysRoot, err := bulk.BuildWithOptions(sysIter, newAlloc, newPager, bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
		})
		_ = sysIter.Close()
		if err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		if nextSysRoot, sysRetired, err := applySystemStatsUpdatesOnPager(newSysRoot, metrics, newZ, newPager, db); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		} else if nextSysRoot != newSysRoot || len(sysRetired) > 0 {
			newSysRoot = nextSysRoot
			freeRetired(sysRetired)
		}

		// Prepare new meta.
		nextMeta := baseMeta
		nextMeta.CommitSeq++
		nextMeta.UserRootPageID = newRoot
		nextMeta.SystemRootPageID = newSysRoot
		nextMeta.FreelistHeadID = newAlloc.Head()
		nextMeta.ActiveSlabID = db.slabManager.ActiveSlabID()
		nextMeta.ActiveSlabTail = db.slabManager.ActiveSlabTail()
		nextMeta.TotalPages = newPager.PageCount()

		// Ensure slab tail referenced by meta is durable before publishing the
		// new index. Index swap compaction is treated as a durability boundary.
		if err := db.slabManager.Sync(); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		// Write redundant Meta pages (0/1) to the new file and sync it.
		if err := writeMetaToPager(newPager, MetaPage0ID, nextMeta); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := writeMetaToPager(newPager, MetaPage1ID, nextMeta); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := newPager.Sync(); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		if err := os.WriteFile(readyPath, []byte("ready\n"), 0o644); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := syncDirFn(db.dir); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		_ = os.Remove(bakPath)
		if err := os.Rename(indexPath, bakPath); err != nil {
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}
		if err := os.Rename(newPath, indexPath); err != nil {
			_ = os.Rename(bakPath, indexPath)
			db.writeMu.Unlock()
			cleanupNewPager()
			return err
		}

		_ = os.Remove(readyPath)
		_ = os.Remove(bakPath)
		_ = syncDirFn(db.dir)

		// Publish the new index generation (old readers keep oldGen pinned).
		newGen := newIndexGen(db.nextIndexID(), newPager, newAlloc, newZ)
		db.trackIndex(newGen)

		// Mark compacted slabs as zombies so future snapshots stop pinning them.
		for id := range targets {
			if err := db.slabManager.MarkZombie(id); err != nil {
				// Best effort: if this fails, keep the slab pinned; the index no
				// longer references it so safety holds, but space reclamation will
				// be delayed.
				_ = err
			}
		}

		var oldState *DBState
		db.mu.Lock()
		oldState = db.state.Load()
		db.idx.Store(newGen)
		db.meta = nextMeta
		db.metaPageID = MetaPage0ID
		db.state.Store(&DBState{
			CommitSeq:        nextMeta.CommitSeq,
			RootPageID:       nextMeta.UserRootPageID,
			SystemRootPageID: nextMeta.SystemRootPageID,
			SlabSet:          db.slabManager.CurrentSlabSet(),
			ValueLogSet:      db.valueLogManager.CurrentSet(),
		})
		db.mu.Unlock()

		db.writeMu.Unlock()

		if oldState != nil {
			_ = db.slabManager.ReleaseSlabs(oldState.SlabSet)
			_ = db.valueLogManager.Release(oldState.ValueLogSet)
		}

		db.releaseIndex(oldGen)
		if stats != nil {
			stats.FinalizeNanos = uint64(time.Since(finalizeStart))
		}
		return nil
	}
}

// indexSwapLimiter is a minimal token-bucket limiter for slab copy IO.
type indexSwapLimiter struct {
	rate  int64
	burst int64

	tokens   float64
	lastTime time.Time
}

func newIndexSwapLimiter(bytesPerSec int64, burstBytes int64) *indexSwapLimiter {
	if bytesPerSec <= 0 {
		return &indexSwapLimiter{rate: 0}
	}
	if burstBytes <= 0 {
		burstBytes = bytesPerSec
	}
	return &indexSwapLimiter{
		rate:     bytesPerSec,
		burst:    burstBytes,
		tokens:   float64(burstBytes),
		lastTime: time.Now(),
	}
}

func (l *indexSwapLimiter) Wait(ctx context.Context, n int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if l.rate <= 0 || n <= 0 {
		return nil
	}

	now := time.Now()
	elapsed := now.Sub(l.lastTime).Seconds()
	l.lastTime = now

	l.tokens += elapsed * float64(l.rate)
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}

	need := float64(n)
	if l.tokens >= need {
		l.tokens -= need
		return nil
	}

	deficit := need - l.tokens
	sleep := time.Duration(deficit / float64(l.rate) * float64(time.Second)) // ceil-ish not needed
	if sleep > 0 {
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	l.tokens = 0
	return nil
}

type indexSwapRemapIterator struct {
	ctx        context.Context
	under      iterator.UnsafeIterator
	db         *DB
	targets    map[uint32]struct{}
	lim        *indexSwapLimiter
	assist     func()
	shift      *compactionShiftOverride
	disableAll map[uint32]compactionDisableAllOverride

	metrics adaptive.Metrics
	remap   map[page.ValuePtr]page.ValuePtr
	err     error

	remapCount           uint64
	remapBytes           uint64
	shiftPointIdx        int
	shiftRawBytes        uint64
	shiftOverrideBytes   uint64
	shiftOverrideRecords uint64

	lastAssist       time.Time
	bytesSinceAssist int64
}

func newIndexSwapRemapIterator(ctx context.Context, under iterator.UnsafeIterator, db *DB, targets map[uint32]struct{}, lim *indexSwapLimiter, assist func(), shift *compactionShiftOverride, disableAll map[uint32]compactionDisableAllOverride) *indexSwapRemapIterator {
	it := &indexSwapRemapIterator{
		ctx:        ctx,
		under:      under,
		db:         db,
		targets:    targets,
		lim:        lim,
		assist:     assist,
		shift:      shift,
		disableAll: disableAll,
	}
	it.lastAssist = time.Now()
	return it
}

func (it *indexSwapRemapIterator) maybeAssist(force bool) {
	if it.assist == nil {
		return
	}
	if !force {
		const assistEveryBytes = 4 << 20
		const assistEveryDur = 250 * time.Millisecond
		if it.bytesSinceAssist < assistEveryBytes && time.Since(it.lastAssist) < assistEveryDur {
			return
		}
	}
	it.assist()
	it.lastAssist = time.Now()
	it.bytesSinceAssist = 0
}

func (it *indexSwapRemapIterator) Valid() bool {
	return it.err == nil && it.under.Valid()
}

func (it *indexSwapRemapIterator) Next() {
	it.under.Next()
}

func (it *indexSwapRemapIterator) Seek(key []byte) {
	it.under.Seek(key)
}

func (it *indexSwapRemapIterator) UnsafeKey() []byte {
	return it.under.UnsafeKey()
}

func (it *indexSwapRemapIterator) UnsafeValue() []byte {
	return it.under.UnsafeValue()
}

func (it *indexSwapRemapIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	val, ptr, flags := it.under.UnsafeEntry()
	if it.err != nil {
		return nil, page.ValuePtr{}, 0
	}

	if flags&node.FlagPointer == 0 {
		return val, ptr, flags
	}

	// Ensure builders never inline pointer values, even if we loaded them.
	val = nil

	if _, ok := it.targets[ptr.FileID]; !ok {
		return nil, ptr, flags
	}

	select {
	case <-it.ctx.Done():
		it.err = it.ctx.Err()
		return nil, page.ValuePtr{}, 0
	default:
	}

	// Read the pointed-to value bytes from the base snapshot and append the record
	// to the current active slab.
	key := it.under.UnsafeKey()
	value := it.under.UnsafeValue()

	recordBytes := int(page.ValuePtrRecordLength(ptr)) + 4 // include CRC
	if err := it.lim.Wait(it.ctx, recordBytes); err != nil {
		it.err = err
		return nil, page.ValuePtr{}, 0
	}
	it.bytesSinceAssist += int64(recordBytes)
	it.maybeAssist(false)

	disableCompression := it.shouldDisableCompression(ptr, key, value)
	appendOpts := slab.AppendOptions{
		DisableCompression: disableCompression,
		SkipTraining:       true,
		SkipMetrics:        true,
	}
	newPtr, err := it.db.slabManager.AppendWithOptions(key, value, appendOpts)
	if err != nil {
		it.err = err
		return nil, page.ValuePtr{}, 0
	}

	it.remapCount++
	it.remapBytes += uint64(recordBytes)

	if it.remap == nil {
		it.remap = make(map[page.ValuePtr]page.ValuePtr, 1024)
	}
	it.remap[ptr] = newPtr

	if it.metrics.SlabWriteBytesByFile == nil {
		it.metrics.SlabWriteBytesByFile = make(map[uint32]int64, 4)
	}
	it.metrics.SlabWriteBytesByFile[newPtr.FileID] += int64(page.ValuePtrRecordLength(newPtr))

	if it.metrics.SlabDeadBytesByFile == nil {
		it.metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
	}
	it.metrics.SlabDeadBytesByFile[ptr.FileID] += int64(page.ValuePtrRecordLength(ptr))

	it.metrics.SlabWriteBytes += int(page.ValuePtrRecordLength(newPtr))
	it.metrics.SlabDeadBytes += int(page.ValuePtrRecordLength(ptr))

	return nil, newPtr, flags
}

func compactionShiftRawLen(ptr page.ValuePtr, key, value []byte, maxRecordBytes int) int {
	rawLen := len(value)
	if rawLen <= 0 {
		return 0
	}
	if maxRecordBytes > 0 && rawLen > maxRecordBytes {
		rawLen = maxRecordBytes
	}
	if rawLen <= 0 {
		return 0
	}
	if page.ValuePtrIsFullCompressed(ptr) {
		return rawLen + len(key) + 2
	}
	return rawLen
}

func (it *indexSwapRemapIterator) shouldDisableCompression(ptr page.ValuePtr, key, value []byte) bool {
	rawLen := compactionShiftRawLen(ptr, key, value, 0)
	if rawLen <= 0 {
		return false
	}

	if it.disableAll != nil {
		if override, ok := it.disableAll[ptr.FileID]; ok {
			rawLen = compactionShiftRawLen(ptr, key, value, override.maxRecordBytes)
			it.shiftOverrideRecords++
			it.shiftOverrideBytes += uint64(rawLen)
			return true
		}
	}

	shift := it.shift
	if shift == nil || ptr.FileID != shift.slabID {
		return false
	}

	rawLen = compactionShiftRawLen(ptr, key, value, shift.maxRecordBytes)
	if rawLen <= 0 {
		return false
	}
	if shift.disableAll {
		it.shiftOverrideRecords++
		it.shiftOverrideBytes += uint64(rawLen)
		return true
	}
	if len(shift.plan.points) == 0 || shift.plan.windowBytes == 0 {
		return false
	}

	start := it.shiftRawBytes
	end := start + uint64(rawLen)

	for it.shiftPointIdx < len(shift.plan.points) {
		point := shift.plan.points[it.shiftPointIdx]
		windowEnd := point.rawBytes
		windowStart := uint64(0)
		if shift.plan.windowBytes > 0 && windowEnd > shift.plan.windowBytes {
			windowStart = windowEnd - shift.plan.windowBytes
		}
		if end <= windowStart {
			break
		}
		if start < windowEnd && end > windowStart {
			it.shiftRawBytes = end
			if end >= windowEnd {
				it.shiftPointIdx++
			}
			it.shiftOverrideRecords++
			it.shiftOverrideBytes += uint64(rawLen)
			return true
		}
		if start >= windowEnd {
			it.shiftPointIdx++
			continue
		}
		break
	}

	it.shiftRawBytes = end
	return false
}

func (it *indexSwapRemapIterator) Key() []byte   { return it.under.Key() }
func (it *indexSwapRemapIterator) Value() []byte { return it.under.Value() }
func (it *indexSwapRemapIterator) KeyCopy(dst []byte) []byte {
	return it.under.KeyCopy(dst)
}
func (it *indexSwapRemapIterator) ValueCopy(dst []byte) []byte {
	return it.under.ValueCopy(dst)
}
func (it *indexSwapRemapIterator) IsDeleted() bool { return it.under.IsDeleted() }
func (it *indexSwapRemapIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.under.Error()
}
func (it *indexSwapRemapIterator) Close() error { return it.under.Close() }
func (it *indexSwapRemapIterator) Domain() (start, end []byte) {
	return it.under.Domain()
}

type compactionDictSample struct {
	bytes     uint64
	records   uint64
	dictBytes uint64
	dictHash  uint64
	dictRatio float64
	dict      []byte
}

type compactionShiftPoint struct {
	records  uint64
	rawBytes uint64
	ratio    float64
}

type compactionShiftPlan struct {
	points       []compactionShiftPoint
	worstRatio   float64
	avgRatio     float64
	shiftBytes   uint64
	shiftRecords uint64
	shiftCount   uint64
	windowBytes  uint64
}

type compactionShiftOverride struct {
	slabID         uint32
	plan           compactionShiftPlan
	maxRecordBytes int
	disableAll     bool
}

type compactionDisableAllOverride struct {
	maxRecordBytes int
}

type compactionBaseRatioSample struct {
	ratio   float64
	raw     uint64
	stored  uint64
	records uint64
}

type compactionSampleBundle struct {
	sample      compactionDictSample
	baseRatio   float64
	baseRaw     uint64
	baseStored  uint64
	baseRecords uint64
}

const (
	compactionShiftWindowDivisor  = 4
	compactionShiftMinWindowBytes = 128 << 10
	compactionShiftRatioTolerance = 0.15
	compactionShiftMaxPoints      = 16
)

func selectCompactionSampleSlab(ctx context.Context, snap *Snapshot, targets map[uint32]struct{}) (uint32, bool, error) {
	ids, err := selectCompactionSampleSlabs(ctx, snap, targets, 1)
	if err != nil || len(ids) == 0 {
		return 0, false, err
	}
	return ids[0], true, nil
}

func selectCompactionSampleSlabs(ctx context.Context, snap *Snapshot, targets map[uint32]struct{}, max int) ([]uint32, error) {
	if snap == nil || snap.idx == nil {
		return nil, errors.New("compaction: missing base snapshot")
	}
	iter := snap.tree.Iterator(nil, nil)
	defer func() { _ = iter.Close() }()

	liveBytes := make(map[uint32]uint64, len(targets))
	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer == 0 {
			iter.Next()
			continue
		}
		if _, ok := targets[ptr.FileID]; ok {
			liveBytes[ptr.FileID] += uint64(page.ValuePtrRecordLength(ptr))
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	candidates := make([]struct {
		id    uint32
		bytes uint64
	}, 0, len(liveBytes))
	for id, bytes := range liveBytes {
		candidates = append(candidates, struct {
			id    uint32
			bytes uint64
		}{id: id, bytes: bytes})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].bytes == candidates[j].bytes {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].bytes > candidates[j].bytes
	})
	if max > 0 && len(candidates) > max {
		candidates = candidates[:max]
	}
	ids := make([]uint32, len(candidates))
	for i, candidate := range candidates {
		ids[i] = candidate.id
	}
	return ids, nil
}

type compactionSampleAccumulator struct {
	slabID        uint32
	samples       [][]byte
	sampleBytes   uint64
	sampleRecords uint64
	rawBytes      uint64
	storedBytes   uint64
	baseRecords   uint64
	strideCounter int
	done          bool
}

func collectCompactionSampleSet(ctx context.Context, snap *Snapshot, candidates []uint32, cfg slab.CompressionTrainConfig, estimate func([]byte, []byte) (int, int, error)) (map[uint32]compactionSampleBundle, error) {
	if snap == nil || snap.idx == nil {
		return nil, errors.New("compaction: missing base snapshot")
	}
	if cfg.TrainBytes <= 0 || cfg.MinRecords <= 0 {
		return map[uint32]compactionSampleBundle{}, nil
	}
	if len(candidates) == 0 {
		return map[uint32]compactionSampleBundle{}, nil
	}

	accs := make(map[uint32]*compactionSampleAccumulator, len(candidates))
	for _, slabID := range candidates {
		accs[slabID] = &compactionSampleAccumulator{slabID: slabID}
	}
	remaining := len(accs)

	stride := cfg.SampleStride
	if stride <= 1 {
		stride = 1
	}
	targetBytes := uint64(cfg.TrainBytes)
	minRecords := uint64(cfg.MinRecords)

	iter := snap.tree.Iterator(nil, nil)
	defer func() { _ = iter.Close() }()

	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer == 0 {
			iter.Next()
			continue
		}
		acc := accs[ptr.FileID]
		if acc == nil || acc.done {
			iter.Next()
			continue
		}
		acc.strideCounter++
		if stride > 1 && acc.strideCounter%stride != 0 {
			iter.Next()
			continue
		}
		key := iter.UnsafeKey()
		value := iter.UnsafeValue()
		if len(value) == 0 {
			iter.Next()
			continue
		}
		if cfg.MaxRecordBytes > 0 && len(value) > cfg.MaxRecordBytes {
			value = value[:cfg.MaxRecordBytes]
		}

		needSamples := acc.sampleBytes < targetBytes || acc.sampleRecords < minRecords
		needBase := acc.rawBytes < targetBytes || acc.baseRecords < minRecords

		if needSamples {
			cp := make([]byte, len(value))
			copy(cp, value)
			acc.samples = append(acc.samples, cp)
			acc.sampleBytes += uint64(len(cp))
			acc.sampleRecords++
		}
		if needBase {
			rawLen, storedLen, err := estimate(key, value)
			if err != nil {
				return nil, err
			}
			acc.rawBytes += uint64(rawLen)
			acc.storedBytes += uint64(storedLen)
			acc.baseRecords++
		}

		if !(acc.sampleBytes < targetBytes || acc.sampleRecords < minRecords || acc.rawBytes < targetBytes || acc.baseRecords < minRecords) {
			acc.done = true
			remaining--
			if remaining == 0 {
				break
			}
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	out := make(map[uint32]compactionSampleBundle, len(accs))
	for id, acc := range accs {
		sample := buildCompactionDictSample(id, cfg, acc.samples, acc.sampleBytes, acc.sampleRecords)
		ratio := 0.0
		if acc.rawBytes > 0 {
			ratio = float64(acc.storedBytes) / float64(acc.rawBytes)
		}
		out[id] = compactionSampleBundle{
			sample:      sample,
			baseRatio:   ratio,
			baseRaw:     acc.rawBytes,
			baseStored:  acc.storedBytes,
			baseRecords: acc.baseRecords,
		}
	}
	return out, nil
}

func buildCompactionDictSample(slabID uint32, cfg slab.CompressionTrainConfig, samples [][]byte, sampleBytes, sampleRecords uint64) compactionDictSample {
	sample := compactionDictSample{
		bytes:   sampleBytes,
		records: sampleRecords,
	}
	if sampleRecords < uint64(cfg.MinRecords) {
		return sample
	}

	rawTotal := 0
	for _, sample := range samples {
		rawTotal += len(sample)
	}
	if rawTotal < 8 {
		return sample
	}

	dictBytes := cfg.DictBytes
	if dictBytes <= 0 {
		dictBytes = rawTotal
	}
	if dictBytes > rawTotal {
		dictBytes = rawTotal
	}

	history := make([]byte, 0, dictBytes)
	for _, sample := range samples {
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		if len(sample) > need {
			history = append(history, sample[:need]...)
		} else {
			history = append(history, sample...)
		}
	}
	if len(history) < 8 {
		return sample
	}

	dictID := slabID + 1
	if dictID == 0 {
		dictID = 1
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: samples,
		History:  history,
		Level:    cfg.Level,
	})
	if err != nil {
		return sample
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(cfg.Level), zstd.WithEncoderCRC(false), zstd.WithEncoderDict(dict))
	if err != nil {
		return sample
	}
	defer enc.Close()

	storedTotal := 0
	for _, sample := range samples {
		storedTotal += len(enc.EncodeAll(sample, nil))
	}
	if rawTotal > 0 {
		sample.dictRatio = float64(storedTotal) / float64(rawTotal)
	}
	sample.dictBytes = uint64(len(dict))
	sample.dictHash = xxhash.Sum64(dict)
	sample.dict = dict
	return sample
}

func collectCompactionDictSample(ctx context.Context, snap *Snapshot, slabID uint32, cfg slab.CompressionTrainConfig) (compactionDictSample, error) {
	if snap == nil || snap.idx == nil {
		return compactionDictSample{}, errors.New("compaction: missing base snapshot")
	}
	if cfg.TrainBytes <= 0 || cfg.MinRecords <= 0 {
		return compactionDictSample{}, nil
	}

	stride := cfg.SampleStride
	if stride <= 1 {
		stride = 1
	}

	iter := snap.tree.Iterator(nil, nil)
	defer func() { _ = iter.Close() }()

	samples := make([][]byte, 0, cfg.MinRecords)
	var (
		sampleBytes   uint64
		sampleRecords uint64
		strideCounter int
	)

	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return compactionDictSample{}, err
		}
		_, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer == 0 || ptr.FileID != slabID {
			iter.Next()
			continue
		}
		if stride > 1 {
			strideCounter++
			if strideCounter%stride != 0 {
				iter.Next()
				continue
			}
		}
		value := iter.UnsafeValue()
		if len(value) == 0 {
			iter.Next()
			continue
		}
		if cfg.MaxRecordBytes > 0 && len(value) > cfg.MaxRecordBytes {
			value = value[:cfg.MaxRecordBytes]
		}
		cp := make([]byte, len(value))
		copy(cp, value)
		samples = append(samples, cp)
		sampleBytes += uint64(len(cp))
		sampleRecords++
		if sampleBytes >= uint64(cfg.TrainBytes) && sampleRecords >= uint64(cfg.MinRecords) {
			break
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return compactionDictSample{}, err
	}

	sample := compactionDictSample{
		bytes:   sampleBytes,
		records: sampleRecords,
	}
	if sampleRecords < uint64(cfg.MinRecords) {
		return sample, nil
	}

	rawTotal := 0
	for _, sample := range samples {
		rawTotal += len(sample)
	}
	if rawTotal < 8 {
		return sample, nil
	}

	dictBytes := cfg.DictBytes
	if dictBytes <= 0 {
		dictBytes = rawTotal
	}
	if dictBytes > rawTotal {
		dictBytes = rawTotal
	}

	history := make([]byte, 0, dictBytes)
	for _, sample := range samples {
		if len(history) >= dictBytes {
			break
		}
		need := dictBytes - len(history)
		if len(sample) > need {
			history = append(history, sample[:need]...)
		} else {
			history = append(history, sample...)
		}
	}
	if len(history) < 8 {
		return sample, nil
	}

	dictID := slabID + 1
	if dictID == 0 {
		dictID = 1
	}
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       dictID,
		Contents: samples,
		History:  history,
		Level:    cfg.Level,
	})
	if err != nil {
		return sample, nil
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(cfg.Level), zstd.WithEncoderCRC(false), zstd.WithEncoderDict(dict))
	if err != nil {
		return sample, nil
	}
	defer enc.Close()

	storedTotal := 0
	for _, sample := range samples {
		storedTotal += len(enc.EncodeAll(sample, nil))
	}
	if rawTotal > 0 {
		sample.dictRatio = float64(storedTotal) / float64(rawTotal)
	}
	sample.dictBytes = uint64(len(dict))
	sample.dictHash = xxhash.Sum64(dict)
	sample.dict = dict
	return sample, nil
}

func collectCompactionCompressionRatio(ctx context.Context, snap *Snapshot, slabID uint32, cfg slab.CompressionTrainConfig, estimate func([]byte, []byte) (int, int, error)) (float64, uint64, uint64, uint64, error) {
	if snap == nil || snap.idx == nil {
		return 0, 0, 0, 0, errors.New("compaction: missing base snapshot")
	}
	if cfg.TrainBytes <= 0 || cfg.MinRecords <= 0 {
		return 0, 0, 0, 0, nil
	}

	stride := cfg.SampleStride
	if stride <= 1 {
		stride = 1
	}

	iter := snap.tree.Iterator(nil, nil)
	defer func() { _ = iter.Close() }()

	var (
		rawBytes      uint64
		storedBytes   uint64
		records       uint64
		strideCounter int
	)

	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return 0, 0, 0, 0, err
		}
		_, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer == 0 || ptr.FileID != slabID {
			iter.Next()
			continue
		}
		if stride > 1 {
			strideCounter++
			if strideCounter%stride != 0 {
				iter.Next()
				continue
			}
		}
		key := iter.UnsafeKey()
		value := iter.UnsafeValue()
		if len(value) == 0 {
			iter.Next()
			continue
		}
		if cfg.MaxRecordBytes > 0 && len(value) > cfg.MaxRecordBytes {
			value = value[:cfg.MaxRecordBytes]
		}
		rawLen, storedLen, err := estimate(key, value)
		if err != nil {
			return 0, 0, 0, 0, err
		}
		rawBytes += uint64(rawLen)
		storedBytes += uint64(storedLen)
		records++
		if rawBytes >= uint64(cfg.TrainBytes) && records >= uint64(cfg.MinRecords) {
			break
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return 0, 0, 0, 0, err
	}
	if rawBytes == 0 {
		return 0, rawBytes, storedBytes, records, nil
	}
	ratio := float64(storedBytes) / float64(rawBytes)
	return ratio, rawBytes, storedBytes, records, nil
}

func collectCompactionShiftPlanWithEstimator(ctx context.Context, snap *Snapshot, slabID uint32, cfg slab.CompressionTrainConfig, baseRatio float64, tuning compactionShiftTuning, estimate func([]byte, []byte) (int, int, error)) (compactionShiftPlan, error) {
	if snap == nil || snap.idx == nil {
		return compactionShiftPlan{}, errors.New("compaction: missing base snapshot")
	}
	if baseRatio <= 0 {
		return compactionShiftPlan{}, nil
	}
	if cfg.MinRecords <= 0 {
		return compactionShiftPlan{}, nil
	}

	windowBytes := cfg.TrainBytes / tuning.windowDivisor
	if windowBytes < tuning.minWindowBytes {
		windowBytes = tuning.minWindowBytes
	}
	plan := compactionShiftPlan{windowBytes: uint64(windowBytes)}

	stride := cfg.SampleStride
	if stride <= 1 {
		stride = 1
	}

	iter := snap.tree.Iterator(nil, nil)
	defer func() { _ = iter.Close() }()

	var (
		windowRaw     int
		windowStored  int
		windowRecords int
		totalRaw      uint64
		totalRecords  uint64
		strideCounter int
	)
	applyWindow := func() {
		if windowRaw == 0 || windowRecords < cfg.MinRecords {
			return
		}
		ratio := float64(windowStored) / float64(windowRaw)
		if ratio > baseRatio*(1.0+tuning.ratioTolerance) {
			plan.shiftCount++
			if len(plan.points) < tuning.maxPoints {
				plan.points = append(plan.points, compactionShiftPoint{
					records:  totalRecords,
					rawBytes: totalRaw,
					ratio:    ratio,
				})
			}
			plan.shiftBytes += uint64(windowRaw)
			plan.shiftRecords += uint64(windowRecords)
			plan.avgRatio += ratio
			if ratio > plan.worstRatio {
				plan.worstRatio = ratio
			}
		}
		windowRaw = 0
		windowStored = 0
		windowRecords = 0
	}

	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return plan, err
		}
		_, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer == 0 || ptr.FileID != slabID {
			iter.Next()
			continue
		}
		if stride > 1 {
			strideCounter++
			if strideCounter%stride != 0 {
				iter.Next()
				continue
			}
		}
		key := iter.UnsafeKey()
		value := iter.UnsafeValue()
		if len(value) == 0 {
			iter.Next()
			continue
		}
		if cfg.MaxRecordBytes > 0 && len(value) > cfg.MaxRecordBytes {
			value = value[:cfg.MaxRecordBytes]
		}

		rawLen, storedLen, err := estimate(key, value)
		if err != nil {
			return compactionShiftPlan{}, err
		}
		windowRaw += rawLen
		windowStored += storedLen
		windowRecords++
		totalRaw += uint64(rawLen)
		totalRecords++

		if windowRaw >= windowBytes {
			applyWindow()
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return plan, err
	}
	applyWindow()
	if plan.shiftCount > 0 {
		plan.avgRatio /= float64(plan.shiftCount)
	}
	return plan, nil
}

func collectCompactionShiftDictSample(ctx context.Context, snap *Snapshot, slabID uint32, cfg slab.CompressionTrainConfig, plan compactionShiftPlan, estimate func([]byte, []byte) (int, int, error)) (compactionDictSample, error) {
	if snap == nil || snap.idx == nil {
		return compactionDictSample{}, errors.New("compaction: missing base snapshot")
	}
	if cfg.TrainBytes <= 0 || cfg.MinRecords <= 0 {
		return compactionDictSample{}, nil
	}
	if plan.windowBytes == 0 || len(plan.points) == 0 {
		return compactionDictSample{}, nil
	}

	worst := plan.points[0]
	for _, point := range plan.points[1:] {
		if point.ratio > worst.ratio {
			worst = point
		}
	}

	windowEnd := worst.rawBytes
	windowStart := uint64(0)
	if windowEnd > plan.windowBytes {
		windowStart = windowEnd - plan.windowBytes
	}

	targetBytes := cfg.TrainBytes
	if plan.windowBytes > 0 && int(plan.windowBytes) < targetBytes {
		targetBytes = int(plan.windowBytes)
	}

	stride := cfg.SampleStride
	if stride <= 1 {
		stride = 1
	}

	iter := snap.tree.Iterator(nil, nil)
	defer func() { _ = iter.Close() }()

	samples := make([][]byte, 0, cfg.MinRecords)
	var (
		sampleBytes   uint64
		sampleRecords uint64
		strideCounter int
		totalRaw      uint64
	)

	for iter.Valid() {
		if err := ctx.Err(); err != nil {
			return compactionDictSample{}, err
		}
		_, ptr, flags := iter.UnsafeEntry()
		if flags&node.FlagPointer == 0 || ptr.FileID != slabID {
			iter.Next()
			continue
		}
		if stride > 1 {
			strideCounter++
			if strideCounter%stride != 0 {
				iter.Next()
				continue
			}
		}
		key := iter.UnsafeKey()
		value := iter.UnsafeValue()
		if len(value) == 0 {
			iter.Next()
			continue
		}
		if cfg.MaxRecordBytes > 0 && len(value) > cfg.MaxRecordBytes {
			value = value[:cfg.MaxRecordBytes]
		}
		rawLen, _, err := estimate(key, value)
		if err != nil {
			return compactionDictSample{}, err
		}

		start := totalRaw
		end := start + uint64(rawLen)
		totalRaw = end

		if end <= windowStart {
			iter.Next()
			continue
		}
		if start >= windowEnd {
			break
		}
		if end > windowStart && start < windowEnd {
			cp := make([]byte, len(value))
			copy(cp, value)
			samples = append(samples, cp)
			sampleBytes += uint64(len(cp))
			sampleRecords++
			if sampleBytes >= uint64(targetBytes) && sampleRecords >= uint64(cfg.MinRecords) {
				break
			}
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return compactionDictSample{}, err
	}
	return buildCompactionDictSample(slabID, cfg, samples, sampleBytes, sampleRecords), nil
}

func entriesEquivalent(base node.LeafEntry, baseErr error, curr node.LeafEntry, currErr error) bool {
	baseMissing := baseErr != nil
	currMissing := currErr != nil
	if baseMissing && currMissing {
		return true
	}

	// Treat tombstones as missing; iteration never includes them.
	if !baseMissing && base.Flags&node.FlagTombstone != 0 {
		baseMissing = true
	}
	if !currMissing && curr.Flags&node.FlagTombstone != 0 {
		currMissing = true
	}
	if baseMissing || currMissing {
		return baseMissing && currMissing
	}

	basePtr := base.Flags&node.FlagPointer != 0
	currPtr := curr.Flags&node.FlagPointer != 0
	if basePtr != currPtr {
		return false
	}
	if basePtr {
		return base.ValuePtr == curr.ValuePtr
	}
	return bytes.Equal(base.Value, curr.Value)
}

func (db *DB) applyIndexSwapDelta(root uint64, opsMap map[string]batch.Entry, z *zipper.Zipper, baseSnap *Snapshot, targets map[uint32]struct{}, remap map[page.ValuePtr]page.ValuePtr, adjusted map[page.ValuePtr]struct{}, metrics *adaptive.Metrics) (uint64, []uint64, error) {
	if len(opsMap) == 0 {
		return root, nil, nil
	}
	if baseSnap == nil || baseSnap.idx == nil || baseSnap.state == nil {
		return 0, nil, errors.New("compaction: missing base snapshot")
	}

	// We trust the entry from the recorder because it was captured from a
	// committed batch. We do not need to look it up in the old index.

	ops := make([]batch.Entry, 0, vacuumDeltaBatchSize)
	var retired []uint64

	applyOps := func() error {
		if len(ops) == 0 {
			return nil
		}
		b := batch.New(db.slabManager, vacuumInlineThresholdMax)
		defer func() { _ = b.Close() }()
		if err := b.SetOps(ops); err != nil {
			return err
		}
		newRoot, newRetired, _, err := z.Apply(root, b)
		if err != nil {
			return err
		}
		root = newRoot
		if len(newRetired) > 0 {
			retired = append(retired, newRetired...)
		}
		ops = ops[:0]
		return nil
	}

	adjustForBasePtr := func(ptr page.ValuePtr) {
		if metrics == nil || remap == nil {
			return
		}
		if _, ok := adjusted[ptr]; ok {
			return
		}
		newPtr, ok := remap[ptr]
		if !ok {
			return
		}

		if metrics.SlabWriteBytesByFile != nil {
			if cur := metrics.SlabWriteBytesByFile[newPtr.FileID]; cur > 0 {
				cur -= int64(page.ValuePtrRecordLength(newPtr))
				if cur <= 0 {
					delete(metrics.SlabWriteBytesByFile, newPtr.FileID)
				} else {
					metrics.SlabWriteBytesByFile[newPtr.FileID] = cur
				}
			}
		}
		if metrics.SlabDeadBytesByFile != nil {
			if cur := metrics.SlabDeadBytesByFile[ptr.FileID]; cur > 0 {
				cur -= int64(page.ValuePtrRecordLength(ptr))
				if cur <= 0 {
					delete(metrics.SlabDeadBytesByFile, ptr.FileID)
				} else {
					metrics.SlabDeadBytesByFile[ptr.FileID] = cur
				}
			}
		}
		if metrics.SlabWriteBytes > 0 {
			metrics.SlabWriteBytes -= int(page.ValuePtrRecordLength(newPtr))
			if metrics.SlabWriteBytes < 0 {
				metrics.SlabWriteBytes = 0
			}
		}
		if metrics.SlabDeadBytes > 0 {
			metrics.SlabDeadBytes -= int(page.ValuePtrRecordLength(ptr))
			if metrics.SlabDeadBytes < 0 {
				metrics.SlabDeadBytes = 0
			}
		}
		adjusted[ptr] = struct{}{}
	}

	// Sort keys to ensure sequential updates and minimal write amplification.
	sortedKeys := make([]string, 0, len(opsMap))
	for k := range opsMap {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		// Construct currEntry from recorded op
		op := opsMap[key]
		var currEntry node.LeafEntry
		currEntry.Key = op.Key
		if op.Type == batch.OpDelete {
			currEntry.Flags |= node.FlagTombstone
		} else {
			if op.IsPtr {
				currEntry.Flags |= node.FlagPointer
				currEntry.ValuePtr = op.ValuePtr
			} else {
				currEntry.Value = op.Value
			}
		}
		// Recorded op is always "found" (nil error)
		var currErr error = nil

		baseEntry, baseErr := baseSnap.tree.GetEntry([]byte(key))
		if baseErr != nil && !errors.Is(baseErr, tree.ErrKeyNotFound) {
			return 0, nil, baseErr
		}

		if entriesEquivalent(baseEntry, baseErr, currEntry, currErr) {
			continue
		}

		// If the base snapshot pointed at a to-be-compacted slab, this key's
		// remap should not be counted anymore (it was overwritten/deleted).
		if baseErr == nil && baseEntry.Flags&node.FlagPointer != 0 {
			if _, ok := targets[baseEntry.ValuePtr.FileID]; ok {
				adjustForBasePtr(baseEntry.ValuePtr)
			}
		}

		// Append op directly (it is already an Entry)
		ops = append(ops, op)

		if len(ops) >= vacuumDeltaBatchSize {
			if err := applyOps(); err != nil {
				return 0, nil, err
			}
		}
	}

	if err := applyOps(); err != nil {
		return 0, nil, err
	}

	return root, retired, nil
}

func applySystemStatsUpdatesOnPager(sysRootID uint64, metrics adaptive.Metrics, z *zipper.Zipper, p *pager.Pager, db *DB) (uint64, []uint64, error) {
	if len(metrics.SlabWriteBytesByFile) == 0 && len(metrics.SlabDeadBytesByFile) == 0 {
		return sysRootID, nil, nil
	}
	if db == nil || z == nil || p == nil {
		return 0, nil, errors.New("missing dependencies")
	}

	// Determine which slab IDs are touched this commit.
	ids := make([]uint32, 0, len(metrics.SlabWriteBytesByFile)+len(metrics.SlabDeadBytesByFile))
	seen := make(map[uint32]struct{}, len(metrics.SlabWriteBytesByFile)+len(metrics.SlabDeadBytesByFile))
	for id := range metrics.SlabWriteBytesByFile {
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	for id := range metrics.SlabDeadBytesByFile {
		if _, ok := seen[id]; ok {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	sysTree := tree.New(p, valueReader{slabs: db.slabManager, vlogs: db.valueLogManager}, sysRootID)
	sysBatch := batch.New(db.slabManager, page.DefaultInlineThreshold)
	defer func() { _ = sysBatch.Close() }()

	for _, id := range ids {
		key := slabStatsKey(id)

		var dead, total uint64
		if raw, err := sysTree.Get(key); err == nil && raw != nil {
			d, t, err := decodeSlabStatsValue(raw)
			if err == nil {
				dead, total = d, t
			}
		}

		if delta := metrics.SlabWriteBytesByFile[id]; delta > 0 {
			total += uint64(delta)
		}
		if delta := metrics.SlabDeadBytesByFile[id]; delta > 0 {
			dead += uint64(delta)
		}
		if dead > total {
			dead = total
		}

		if err := sysBatch.Set(key, encodeSlabStatsValue(dead, total)); err != nil {
			return 0, nil, err
		}
	}

	newSysRoot, sysRetired, _, err := z.Apply(sysRootID, sysBatch)
	if err != nil {
		return 0, nil, err
	}
	return newSysRoot, sysRetired, nil
}
