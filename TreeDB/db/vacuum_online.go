package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

var ErrVacuumInProgress = errors.New("online vacuum already in progress")
var ErrVacuumUnsupported = errors.New("online vacuum unsupported on this platform")
var ErrVacuumConcurrentMutation = errors.New("online vacuum aborted after concurrent mutations")

const (
	vacuumDeltaBatchSize     = 4096
	vacuumCatchupPassesMax   = 3
	vacuumCatchupKeyTarget   = 4096
	vacuumCutoverMaxKeys     = 8192
	vacuumCutoverMaxDefers   = 3
	vacuumInlineThresholdMax = int(^uint(0) >> 1)
	vacuumMaxGrowthFactor    = 8
)

type vacuumCollectionClonePhase uint8

const (
	vacuumCollectionClonePreclone vacuumCollectionClonePhase = iota + 1
	vacuumCollectionCloneReclone
)

type vacuumPagerSyncPhase uint8

const (
	vacuumPagerSyncPrecutover vacuumPagerSyncPhase = iota + 1
	vacuumPagerSyncFinal
)

type VacuumOnlineStats struct {
	TotalDuration              time.Duration
	UserTreeDuration           time.Duration
	SystemReserveDuration      time.Duration
	CollectionBasisDuration    time.Duration
	PreflushDuration           time.Duration
	CutoverDuration            time.Duration
	SystemTreeDuration         time.Duration
	FinalPagerSyncDuration     time.Duration
	SwapPublishDuration        time.Duration
	MaxWriterPause             time.Duration
	PrecloneTraversalPages     uint64
	RecloneTraversalPages      uint64
	CutoverCloneTraversalPages uint64
	DirtyDescriptors           uint64
	UserTailMutations          uint64
	UserTailPointMutations     uint64
	UserTailRangeMutations     uint64
	DeferredCutovers           uint64
	ConcurrentMutationAborts   uint64
}

// vacuumReservedAllocator keeps the final system-tree writes in pages reserved
// before the larger user and collection trees are built. That lets the
// pre-cutover sync flush those larger trees without the final system rebuild
// dirtying their tail chunks again.
type vacuumReservedAllocator struct {
	base vacuumCollectionAllocator
	next uint64
	end  uint64
}

func newVacuumReservedAllocator(base vacuumCollectionAllocator, start, count uint64) *vacuumReservedAllocator {
	return &vacuumReservedAllocator{
		base: base,
		next: start,
		end:  start + count,
	}
}

func (a *vacuumReservedAllocator) Alloc(hint uint64) (uint64, error) {
	if a == nil || a.base == nil {
		return 0, errors.New("vacuum: missing reserved allocator")
	}
	if a.next < a.end {
		id := a.next
		a.next++
		return id, nil
	}
	return a.base.Alloc(hint)
}

func (a *vacuumReservedAllocator) ReleaseUnused() error {
	if a == nil || a.base == nil {
		return errors.New("vacuum: missing reserved allocator")
	}
	for a.next < a.end {
		id := a.next
		a.next++
		if err := a.base.Free(id); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) vacuumOnlineStatsSnapshot() VacuumOnlineStats {
	if db == nil {
		return VacuumOnlineStats{}
	}
	stats := db.vacuumOnlineLast.Load()
	if stats == nil {
		return VacuumOnlineStats{}
	}
	return *stats
}

type vacuumRecorder struct {
	active atomic.Bool
	mu     sync.Mutex
	ops    map[string]batch.Entry
	ranges []batch.DeleteRange
}

func (r *vacuumRecorder) Active() bool {
	return r.active.Load()
}

func (r *vacuumRecorder) Start() {
	r.mu.Lock()
	r.ops = make(map[string]batch.Entry, 1024)
	r.ranges = nil
	r.mu.Unlock()
	r.active.Store(true)
}

func (r *vacuumRecorder) Stop() {
	r.active.Store(false)
}

func vacuumRecordCopyEntry(entry batch.Entry) batch.Entry {
	out := entry
	out.Key = append([]byte(nil), entry.Key...)
	if entry.Type == batch.OpPut && !entry.IsPtr {
		out.Value = append([]byte(nil), entry.Value...)
	} else {
		out.Value = nil
	}
	return out
}

func vacuumRecordCopyRange(r batch.DeleteRange) batch.DeleteRange {
	cloneBound := func(bound []byte) []byte {
		if bound == nil {
			return nil
		}
		return append([]byte{}, bound...)
	}
	return batch.DeleteRange{
		Start: cloneBound(r.Start),
		End:   cloneBound(r.End),
	}
}

func (r *vacuumRecorder) RecordOps(ops map[string]batch.Entry) {
	if !r.active.Load() || len(ops) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() {
		return
	}
	if r.ops == nil {
		r.ops = make(map[string]batch.Entry, len(ops))
	}
	for k, entry := range ops {
		r.ops[k] = vacuumRecordCopyEntry(entry)
	}
}

func (r *vacuumRecorder) RecordEntries(entries []batch.Entry) {
	r.RecordApplyPlan(entries, nil)
}

func (r *vacuumRecorder) RecordApplyPlan(entries []batch.Entry, ranges []batch.DeleteRange) {
	if !r.active.Load() || (len(entries) == 0 && len(ranges) == 0) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active.Load() {
		return
	}
	if r.ops == nil && len(entries) > 0 {
		r.ops = make(map[string]batch.Entry, len(entries))
	}
	for _, rg := range ranges {
		if batch.IsDeleteRangeNoop(rg.Start, rg.End) {
			continue
		}
		copied := vacuumRecordCopyRange(rg)
		r.ranges = append(r.ranges, copied)
		if len(r.ops) == 0 {
			continue
		}
		start, end := string(copied.Start), string(copied.End)
		for key := range r.ops {
			if copied.Start != nil && key < start {
				continue
			}
			if copied.End != nil && key >= end {
				continue
			}
			delete(r.ops, key)
		}
	}
	for i := range entries {
		entry := entries[i]
		if len(entry.Key) == 0 {
			continue
		}
		if r.ops == nil {
			r.ops = make(map[string]batch.Entry, len(entries)-i)
		}
		r.ops[string(entry.Key)] = vacuumRecordCopyEntry(entry)
	}
}

func (r *vacuumRecorder) Drain() map[string]batch.Entry {
	ops, _ := r.DrainApplyPlan()
	return ops
}

func (r *vacuumRecorder) DrainApplyPlan() (map[string]batch.Entry, []batch.DeleteRange) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ops) == 0 && len(r.ranges) == 0 {
		return nil, nil
	}
	outOps := r.ops
	if len(outOps) == 0 {
		outOps = nil
	}
	outRanges := r.ranges
	r.ops = make(map[string]batch.Entry, 1024)
	r.ranges = nil
	return outOps, outRanges
}

// VacuumIndexOnline rebuilds the index into a new file and swaps it in with a
// short writer pause. Old snapshots remain valid by pinning the previous index
// generation until readers drain; disk space is reclaimed once the old mmap is
// closed.
func (db *DB) VacuumIndexOnline(ctx context.Context) error {
	return db.vacuumIndexOnline(ctx, true)
}

func (db *DB) vacuumIndexOnline(ctx context.Context, lockMaintenance bool) (retErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if db == nil {
		return ErrClosed
	}
	runStarted := time.Now()
	runStats := &VacuumOnlineStats{}
	defer func() {
		runStats.TotalDuration = time.Since(runStarted)
		published := *runStats
		db.vacuumOnlineLast.Store(&published)
	}()
	if db.readOnly {
		return ErrReadOnly
	}
	if runtime.GOOS == "windows" {
		return ErrVacuumUnsupported
	}
	if lockMaintenance {
		db.maintenanceMu.Lock()
		defer db.maintenanceMu.Unlock()
	}
	// A wrapper may drain backend close hooks before it calls DB.Close. Once
	// those hooks have run, maintenance resources such as the production leaf
	// appender are no longer valid even though normal publish APIs remain open
	// long enough for the wrapper's final flush.
	if db.closing.Load() || db.closeHooksHaveRunMaintenanceLocked() {
		return ErrClosed
	}

	if !db.vacuumInProgress.CompareAndSwap(false, true) {
		return ErrVacuumInProgress
	}
	defer db.vacuumInProgress.Store(false)

	if db.dir == "" {
		return errors.New("vacuum: missing db dir")
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

	oldGen := db.idx.Load()
	oldPages := uint64(0)
	if oldGen != nil && oldGen.pager != nil {
		oldPages = oldGen.pager.PageCount()
	}
	maxPages := uint64(0)
	if oldPages > 0 {
		maxPages = oldPages * uint64(vacuumMaxGrowthFactor)
	}
	checkGrowth := func() error {
		if maxPages == 0 {
			return nil
		}
		newPages := newPager.PageCount()
		if newPages > maxPages {
			return fmt.Errorf("vacuum: new index page count %d exceeds %dx old (%d)", newPages, vacuumMaxGrowthFactor, oldPages)
		}
		return nil
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
	newZ.SetIndexColumnarLeaves(db.indexColumnarLeaves)
	newZ.SetIndexPackedValuePtr(db.indexPackedValuePtr)
	newZ.SetIndexInternalBaseDelta(db.indexInternalBaseDelta)
	newZ.SetAdaptiveLeafEncoding(db.indexAdaptiveLeafEncoding)
	newZ.SetMaintenanceOpsPerCoalesce(db.maintenanceOpsPerCoalesce)
	newZ.SetLeafPageReader(db.leafPageReader(db.valueLogManager))
	newZ.SetLeafPageLog(db.leafPageLog)
	newZ.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
	db.idxMu.Lock()
	parallelMergePressureSource := db.zipperParallelMergeSource
	db.idxMu.Unlock()
	newZ.SetParallelMergePressureSource(parallelMergePressureSource)

	db.vacuum.Start()
	defer db.vacuum.Stop()

	// Build a fresh user tree from a stable snapshot.
	baseSnap := db.AcquireSnapshot()
	if baseSnap == nil {
		cleanupNewPager()
		if db.closing.Load() {
			return ErrClosed
		}
		return errors.New("vacuum: missing base snapshot")
	}
	basis := &collectionBasis{snapshot: baseSnap}
	defer func() {
		if basis != nil && basis.snapshot != nil {
			_ = basis.snapshot.Close()
			basis.snapshot = nil
		}
	}()
	baseState := baseSnap.State()
	basePager := baseSnap.Pager()
	if baseState == nil || basePager == nil {
		cleanupNewPager()
		return errors.New("vacuum: missing base snapshot state")
	}
	reserveStarted := time.Now()
	systemTreePages, err := vacuumCountPagerTreePages(basePager, baseState.SystemRootPageID)
	if err != nil {
		cleanupNewPager()
		return err
	}
	maxInt := int(^uint(0) >> 1)
	if systemTreePages > uint64(maxInt) {
		cleanupNewPager()
		return fmt.Errorf("vacuum: system tree page count %d exceeds allocation limit", systemTreePages)
	}
	systemReserveStart, err := newPager.Alloc(int(systemTreePages))
	if err != nil {
		cleanupNewPager()
		return err
	}
	systemAlloc := newVacuumReservedAllocator(newAlloc, systemReserveStart, systemTreePages)
	reservedBytes := systemAlloc.end * uint64(page.PageSize)
	firstPreflushChunk := int((reservedBytes + uint64(db.chunkSize) - 1) / uint64(db.chunkSize))
	runStats.SystemReserveDuration += time.Since(reserveStarted)

	userTreeStarted := time.Now()
	var newRoot uint64
	if db.indexOuterLeavesInValueLog {
		if db.leafPageLog == nil {
			cleanupNewPager()
			return fmt.Errorf("vacuum: leaf page log not configured")
		}

		rootData, err := basePager.Get(baseState.RootPageID)
		if err != nil {
			cleanupNewPager()
			return err
		}
		effectiveInternalBaseDelta := db.indexInternalBaseDelta && !db.indexOuterLeavesInValueLog
		rootNode := node.NewNode(rootData)
		if rootNode.Type() == page.PageTypeLeaf {
			newRoot, err = vacuumClonePagerTreeWithLeafRefs(basePager, baseState.RootPageID, newAlloc, newPager, effectiveInternalBaseDelta)
			if err != nil {
				cleanupNewPager()
				return err
			}
		} else {
			newRoot, err = vacuumBuildInternalTreeFromLeafRefs(basePager, baseState.RootPageID, newPager, newAlloc, effectiveInternalBaseDelta)
			if err != nil {
				cleanupNewPager()
				return err
			}
		}
	} else {
		baseIter := baseSnap.tree.IteratorWithOptions(nil, nil, tree.IteratorOptions{Mode: tree.IteratorModePointerProjection})
		buildOpts := bulk.BuildOptions{
			LeafPrefixCompression: db.leafPrefixCompression,
			LeafColumnar:          db.indexColumnarLeaves,
			PackedValuePtr:        db.indexPackedValuePtr,
			InternalBaseDelta:     db.indexInternalBaseDelta,
		}
		newRoot, err = bulk.BuildWithOptions(baseIter, newAlloc, newPager, buildOpts)
		_ = baseIter.Close()
	}
	if err != nil {
		cleanupNewPager()
		return err
	}
	runStats.UserTreeDuration += time.Since(userTreeStarted)

	cutoverLocked := false
	collectionVisit := func(phase vacuumCollectionClonePhase) func(uint64) {
		return func(sourcePageID uint64) {
			switch phase {
			case vacuumCollectionClonePreclone:
				runStats.PrecloneTraversalPages++
			case vacuumCollectionCloneReclone:
				runStats.RecloneTraversalPages++
			}
			if cutoverLocked {
				runStats.CutoverCloneTraversalPages++
			}
			if hook := db.vacuumCollectionClonePageHook; hook != nil {
				hook(phase, sourcePageID)
			}
		}
	}
	basisStarted := time.Now()
	baseToken, err := db.collectionTokenForSnapshot(baseSnap)
	if err != nil {
		cleanupNewPager()
		return err
	}
	builtBasis, _, err := vacuumBuildCollectionBasis(ctx, nil, baseSnap, baseToken, newAlloc, newPager, collectionVisit(vacuumCollectionClonePreclone))
	if err != nil {
		cleanupNewPager()
		return err
	}
	basis = builtBasis
	runStats.CollectionBasisDuration += time.Since(basisStarted)
	if err := checkGrowth(); err != nil {
		cleanupNewPager()
		return err
	}

	freeRetired := func(retired []uint64) {
		for _, id := range retired {
			_ = newAlloc.Free(id)
		}
	}

	// Online catch-up: replay recorded keys in bounded passes.
	for pass := 0; pass < vacuumCatchupPassesMax; pass++ {
		if err := ctx.Err(); err != nil {
			cleanupNewPager()
			return err
		}
		opsMap, ranges := db.vacuum.DrainApplyPlan()
		if len(opsMap) == 0 && len(ranges) == 0 {
			break
		}
		var retired []uint64
		newRoot, retired, err = db.applyVacuumDelta(newRoot, opsMap, ranges, newZ, nil)
		if err != nil {
			cleanupNewPager()
			return err
		}
		freeRetired(retired)
		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			if err := db.leafPageLog.Flush(); err != nil {
				cleanupNewPager()
				return err
			}
		}
		if err := checkGrowth(); err != nil {
			cleanupNewPager()
			return err
		}
		if len(opsMap)+len(ranges) <= vacuumCatchupKeyTarget {
			break
		}
	}

	applyTail := func(ops map[string]batch.Entry, ranges []batch.DeleteRange) error {
		if len(ops) == 0 && len(ranges) == 0 {
			return nil
		}
		var retired []uint64
		var err error
		newRoot, retired, err = db.applyVacuumDelta(newRoot, ops, ranges, newZ, nil)
		if err != nil {
			return err
		}
		freeRetired(retired)
		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			if err := db.leafPageLog.Flush(); err != nil {
				return err
			}
		}
		return checkGrowth()
	}

	// Final cutover: stop recording, validate the pinned collection basis, apply
	// only a bounded user tail, rebuild the system tree, then perform the existing
	// durability/swap/publication sequence.
	defers := 0
	for {
		if err := ctx.Err(); err != nil {
			cleanupNewPager()
			return err
		}
		// Flush preclone, catch-up, and reclone overflow chunks while writers can
		// continue. Reserved low chunks stay dirty for the final durability sync,
		// which remains in the established crash sequence.
		if hook := db.vacuumPagerSyncHook; hook != nil {
			hook(vacuumPagerSyncPrecutover)
		}
		preflushStarted := time.Now()
		preflushErr := newPager.FlushDirtyChunksFrom(firstPreflushChunk)
		runStats.PreflushDuration += time.Since(preflushStarted)
		if preflushErr != nil {
			cleanupNewPager()
			return preflushErr
		}
		if hook := db.vacuumBeforeCutoverHook; hook != nil {
			hook(defers)
		}

		db.writeMu.Lock()
		cutoverLocked = true
		holdStarted := time.Now()
		unlockCutover := func(completed bool) {
			hold := time.Since(holdStarted)
			if hold > runStats.MaxWriterPause {
				runStats.MaxWriterPause = hold
			}
			if completed {
				runStats.CutoverDuration = hold
			}
			cutoverLocked = false
			db.writeMu.Unlock()
		}

		db.vacuum.Stop()
		finalOps, finalRanges := db.vacuum.DrainApplyPlan()
		tailMutations := len(finalOps) + len(finalRanges)
		runStats.UserTailMutations += uint64(tailMutations)
		runStats.UserTailPointMutations += uint64(len(finalOps))
		runStats.UserTailRangeMutations += uint64(len(finalRanges))
		if db.closing.Load() {
			unlockCutover(false)
			cleanupNewPager()
			return ErrClosed
		}
		currentToken, err := db.currentCollectionTokenLocked()
		if err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}
		coherentDirty := currentToken.indexGenerationID != basis.token.indexGenerationID || currentToken.systemRootPageID != basis.token.systemRootPageID
		epochDirty := currentToken.publishEpoch != basis.token.publishEpoch
		needsDeferredCutover := coherentDirty || epochDirty || tailMutations > vacuumCutoverMaxKeys
		if needsDeferredCutover {
			if defers >= vacuumCutoverMaxDefers {
				runStats.ConcurrentMutationAborts++
				unlockCutover(false)
				cleanupNewPager()
				return ErrVacuumConcurrentMutation
			}
			successor := db.AcquireSnapshot()
			if successor == nil {
				unlockCutover(false)
				cleanupNewPager()
				if db.closing.Load() {
					return ErrClosed
				}
				return errors.New("vacuum: missing successor snapshot")
			}
			successorToken, tokenErr := db.collectionTokenForSnapshot(successor)
			if tokenErr != nil {
				_ = successor.Close()
				unlockCutover(false)
				cleanupNewPager()
				return tokenErr
			}
			db.vacuum.Start()
			unlockCutover(false)
			defers++
			runStats.DeferredCutovers++

			if err := applyTail(finalOps, finalRanges); err != nil {
				_ = successor.Close()
				cleanupNewPager()
				return err
			}
			recloneStarted := time.Now()
			nextBasis, dirtyDescriptors, err := vacuumBuildCollectionBasis(ctx, basis, successor, successorToken, newAlloc, newPager, collectionVisit(vacuumCollectionCloneReclone))
			if err != nil {
				_ = successor.Close()
				cleanupNewPager()
				return err
			}
			runStats.CollectionBasisDuration += time.Since(recloneStarted)
			runStats.DirtyDescriptors += uint64(dirtyDescriptors)
			oldSnapshot := basis.snapshot
			basis.snapshot = nil
			basis = nextBasis
			if err := oldSnapshot.Close(); err != nil {
				cleanupNewPager()
				return err
			}
			if err := checkGrowth(); err != nil {
				cleanupNewPager()
				return err
			}
			continue
		}

		if err := applyTail(finalOps, finalRanges); err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}

		// The coherent token is clean, so the pinned basis is the exact system
		// catalog accepted for cutover. No collection-root clone occurs below.
		oldGen := db.idx.Load()
		state := db.state.Load()
		if oldGen == nil || state == nil || oldGen != basis.snapshot.idx {
			unlockCutover(false)
			cleanupNewPager()
			return errors.New("vacuum: collection basis generation changed")
		}
		db.mu.RLock()
		baseMeta := db.meta
		db.mu.RUnlock()
		collectionRootReplacements, err := basis.replacements()
		if err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}

		var newSysRoot uint64
		effectiveInternalBaseDelta := db.indexInternalBaseDelta && !db.indexOuterLeavesInValueLog
		systemTreeStarted := time.Now()
		if db.indexOuterLeavesInValueLog && len(collectionRootReplacements) == 0 {
			newSysRoot, err = vacuumClonePagerTreeWithLeafRefs(basis.snapshot.idx.pager, basis.token.systemRootPageID, systemAlloc, newPager, effectiveInternalBaseDelta)
		} else {
			newSysRoot, err = vacuumBuildSystemRoot(basis.snapshot.idx.pager, &basis.snapshot.reader, basis.token.systemRootPageID, systemAlloc, newPager, bulk.BuildOptions{
				LeafPrefixCompression: db.leafPrefixCompression,
				LeafColumnar:          db.indexColumnarLeaves,
				PackedValuePtr:        db.indexPackedValuePtr,
				InternalBaseDelta:     effectiveInternalBaseDelta,
			}, collectionRootReplacements)
		}
		if err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}
		if err := systemAlloc.ReleaseUnused(); err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}
		runStats.SystemTreeDuration += time.Since(systemTreeStarted)
		if err := checkGrowth(); err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}

		// Prepare new meta.
		nextMeta := baseMeta
		nextMeta.CommitSeq++
		nextMeta.UserRootPageID = newRoot
		nextMeta.SystemRootPageID = newSysRoot
		nextMeta.FreelistHeadID = newAlloc.Head()
		nextMeta.TotalPages = newPager.PageCount()

		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			if err := db.leafPageLog.Sync(); err != nil {
				unlockCutover(false)
				cleanupNewPager()
				return err
			}
		}
		leafPageLogSegmentsRegistered := true
		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			var err error
			leafPageLogSegmentsRegistered, err = db.registerLeafPageLogSegmentsForPublish()
			if err != nil {
				unlockCutover(false)
				cleanupNewPager()
				return err
			}
		}
		var stagedLeafGenerationView *leafGenerationView
		leafGenerationChanged := false
		if db.indexOuterLeavesInValueLog && db.leafPageLog != nil {
			db.mu.RLock()
			stagedLeafManifest, changed, err := db.stagedLeafGenerationManifestWithPending(db.leafGenerationManifest, 0, nextMeta.CommitSeq)
			db.mu.RUnlock()
			if err != nil {
				unlockCutover(false)
				cleanupNewPager()
				return err
			}
			if changed {
				stagedLeafGenerationView = newLeafGenerationView(stagedLeafManifest)
				leafGenerationChanged = true
			}
		}

		// Write redundant Meta pages (0/1) to the new file and sync it.
		if err := writeMetaToPager(newPager, MetaPage0ID, nextMeta); err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}
		if err := writeMetaToPager(newPager, MetaPage1ID, nextMeta); err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}
		if hook := db.vacuumPagerSyncHook; hook != nil {
			hook(vacuumPagerSyncFinal)
		}
		finalSyncStarted := time.Now()
		finalSyncErr := newPager.Sync()
		runStats.FinalPagerSyncDuration += time.Since(finalSyncStarted)
		if finalSyncErr != nil {
			unlockCutover(false)
			cleanupNewPager()
			return finalSyncErr
		}

		swapPublishStarted := time.Now()
		if err := os.WriteFile(readyPath, []byte("ready\n"), 0o644); err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}
		if runtime.GOOS != "windows" {
			if dir, err := os.Open(db.dir); err == nil {
				_ = dir.Sync()
				_ = dir.Close()
			}
		}

		// Swap index.db -> index.db.bak, index.db.new -> index.db.
		_ = os.Remove(bakPath)
		if err := os.Rename(indexPath, bakPath); err != nil {
			unlockCutover(false)
			cleanupNewPager()
			return err
		}
		if err := os.Rename(newPath, indexPath); err != nil {
			_ = os.Rename(bakPath, indexPath)
			unlockCutover(false)
			cleanupNewPager()
			return err
		}

		_ = os.Remove(readyPath)
		_ = os.Remove(bakPath)
		if runtime.GOOS != "windows" {
			if dir, err := os.Open(db.dir); err == nil {
				_ = dir.Sync()
				_ = dir.Close()
			}
		}

		// Ensure the new generation preserves leaf-page-in-value-log wiring for
		// subsequent writes.
		newZ.SetLeafPageReader(db.leafPageReader(db.valueLogManager))
		newZ.SetLeafPageLog(db.leafPageLog)
		newZ.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)

		// Publish the new index generation (old readers keep oldGen pinned).
		newGen := newIndexGen(db.nextIndexID(), newPager, newAlloc, newZ)
		db.trackIndex(newGen)

		var oldState *DBState
		db.mu.Lock()
		oldState = db.state.Load()
		db.idx.Store(newGen)
		db.meta = nextMeta
		db.metaPageID = MetaPage0ID
		valueLogSet := db.valueLogManager.CurrentSetNoRefresh()
		if !leafPageLogSegmentsRegistered {
			if valueLogSet != nil {
				_ = db.valueLogManager.Release(valueLogSet)
				valueLogSet = nil
			}
			if err := db.valueLogManager.Refresh(); err != nil {
				db.mu.Unlock()
				unlockCutover(false)
				cleanupNewPager()
				return err
			}
			valueLogSet = db.valueLogManager.CurrentSetNoRefresh()
		}
		leafGenerationView := oldState.LeafGenerations
		if leafGenerationChanged {
			leafGenerationView = stagedLeafGenerationView
		}
		newState := &DBState{
			CommitSeq:                  nextMeta.CommitSeq,
			RootPageID:                 nextMeta.UserRootPageID,
			SystemRootPageID:           nextMeta.SystemRootPageID,
			AppliedCommandLSN:          nextMeta.AppliedCommandLSN,
			MaxEntryRevision:           page.EntryRevision(nextMeta.MaxEntryRevision),
			ValueLogSet:                valueLogSet,
			LeafGenerations:            leafGenerationView,
			LeafGenerationStateVersion: oldState.LeafGenerationStateVersion,
		}
		if leafGenerationChanged {
			db.leafGenerationStateVersion++
			newState.LeafGenerationStateVersion = db.leafGenerationStateVersion
		}
		db.state.Store(newState)
		db.publishSnapshotView(newGen, newState, db.valueLogManager)
		db.mu.Unlock()
		db.clearLeafGenerationReachabilityCaches()

		unlockCutover(true)
		runStats.SwapPublishDuration += time.Since(swapPublishStarted)

		if oldState != nil {
			_ = db.valueLogManager.Release(oldState.ValueLogSet)
		}
		if db.leafPageLog != nil {
			db.commitMu.Lock()
			currentCommitSeq := db.meta.CommitSeq
			err := db.noteLeafGenerationPendingFileIDs(0, currentCommitSeq)
			db.commitMu.Unlock()
			if err != nil {
				db.reportError(err)
			}
		}

		// Drop the basis pin and DB-held reference to the previous generation
		// outside the writer pause; closing the old mmap can be expensive.
		if basis.snapshot != nil {
			if err := basis.snapshot.Close(); err != nil {
				db.reportError(err)
			}
			basis.snapshot = nil
		}
		db.releaseIndex(oldGen)

		return nil
	}
}

func (db *DB) collectionTokenForSnapshot(snap *Snapshot) (collectionToken, error) {
	if db == nil || snap == nil || snap.idx == nil || snap.state == nil {
		return collectionToken{}, errors.New("vacuum: missing coherent collection snapshot")
	}
	return collectionToken{
		indexGenerationID: snap.idx.id,
		systemRootPageID:  snap.state.SystemRootPageID,
		commitSeq:         snap.state.CommitSeq,
		publishEpoch:      snap.systemRootPublishEpoch,
	}, nil
}

// currentCollectionTokenLocked must be called while writeMu is held. All root
// publication paths take writeMu, so idx/state form one coherent token here.
func (db *DB) currentCollectionTokenLocked() (collectionToken, error) {
	if db == nil {
		return collectionToken{}, ErrClosed
	}
	view := db.snapshotViewRO.Load()
	if view == nil || view.idx == nil || view.state == nil {
		return collectionToken{}, errors.New("vacuum: missing current coherent collection state")
	}
	return collectionToken{
		indexGenerationID: view.idx.id,
		systemRootPageID:  view.state.SystemRootPageID,
		commitSeq:         view.state.CommitSeq,
		publishEpoch:      view.systemRootPublishEpoch,
	}, nil
}

type vacuumRangeDeltaApplier func(root uint64, b *batch.Batch) (newRoot uint64, retired []uint64, err error)

func applyVacuumRangeDeltaBatches(root uint64, ranges []batch.DeleteRange, retired []uint64, reader batch.ValueReader, apply vacuumRangeDeltaApplier) (uint64, []uint64, error) {
	if len(ranges) == 0 {
		return root, retired, nil
	}
	if apply == nil {
		return 0, nil, errors.New("vacuum: missing range delta applier")
	}
	for start := 0; start < len(ranges); start += vacuumDeltaBatchSize {
		end := start + vacuumDeltaBatchSize
		if end > len(ranges) {
			end = len(ranges)
		}
		b := batch.New(reader, vacuumInlineThresholdMax)
		for _, r := range ranges[start:end] {
			if err := b.DeleteRange(r.Start, r.End); err != nil {
				_ = b.Close()
				return 0, nil, err
			}
		}
		if b.Len() == 0 {
			_ = b.Close()
			continue
		}
		newRoot, newRetired, err := apply(root, b)
		_ = b.Close()
		if err != nil {
			return 0, nil, err
		}
		root = newRoot
		if len(newRetired) > 0 {
			retired = append(retired, newRetired...)
		}
	}
	return root, retired, nil
}

func (db *DB) applyVacuumDelta(root uint64, opsMap map[string]batch.Entry, ranges []batch.DeleteRange, z *zipper.Zipper, retired []uint64) (uint64, []uint64, error) {
	if len(opsMap) == 0 && len(ranges) == 0 {
		return root, retired, nil
	}

	var err error
	root, retired, err = applyVacuumRangeDeltaBatches(root, ranges, retired, db.valueLogManager, func(root uint64, b *batch.Batch) (uint64, []uint64, error) {
		newRoot, newRetired, _, err := z.Apply(root, b)
		return newRoot, newRetired, err
	})
	if err != nil {
		return 0, nil, err
	}

	keys := make([]string, 0, len(opsMap))
	for k := range opsMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ops := make([]batch.Entry, 0, vacuumDeltaBatchSize)
	applyOps := func() error {
		if len(ops) == 0 {
			return nil
		}
		b := batch.New(db.valueLogManager, vacuumInlineThresholdMax)
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

	for _, key := range keys {
		entry := opsMap[key]
		if len(entry.Key) == 0 {
			entry.Key = []byte(key)
		}
		ops = append(ops, entry)

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

type vacuumCloneCtx struct {
	oldPager          *pager.Pager
	newPager          *pager.Pager
	internalBaseDelta bool
	visitSourcePage   func(uint64)
	alloc             interface {
		Alloc(hint uint64) (uint64, error)
	}
	remap map[uint64]uint64
}

func vacuumCountPagerTreePages(p *pager.Pager, rootID uint64) (uint64, error) {
	if p == nil {
		return 0, errors.New("vacuum: missing pager")
	}
	if rootID == 0 {
		return 0, errors.New("vacuum: missing root id")
	}
	seen := make(map[uint64]struct{})
	var walk func(uint64) error
	walk = func(id uint64) error {
		if _, ok := seen[id]; ok {
			return nil
		}
		seen[id] = struct{}{}
		data, err := p.Get(id)
		if err != nil {
			return err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeLeaf:
			return nil
		case page.PageTypeInternal:
			for i := uint16(0); i < n.Count(); i++ {
				_, childRef, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return err
				}
				switch childRef.Kind {
				case page.ChildRefPage:
					if err := walk(childRef.Page); err != nil {
						return err
					}
				case page.ChildRefLeafLog:
					// The leaf page is persistent value-log data, not a pager page.
				default:
					return fmt.Errorf("vacuum: unexpected child ref kind %d at page %d", childRef.Kind, id)
				}
			}
			return nil
		default:
			return fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), id)
		}
	}
	if err := walk(rootID); err != nil {
		return 0, err
	}
	return uint64(len(seen)), nil
}

type vacuumLeafChild struct {
	key      []byte
	childRef page.ChildRef
}

type vacuumInternalTreeLevelBuilder struct {
	builder  *node.Builder
	startKey []byte
}

type vacuumInternalTreeBuilder struct {
	p     *pager.Pager
	alloc interface {
		Alloc(hint uint64) (uint64, error)
	}
	internalBaseDelta bool
	levels            []*vacuumInternalTreeLevelBuilder
	children          int
}

func newVacuumInternalTreeBuilder(p *pager.Pager, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, internalBaseDelta bool) (*vacuumInternalTreeBuilder, error) {
	if p == nil || alloc == nil {
		return nil, errors.New("vacuum: missing pager/allocator")
	}
	b := &vacuumInternalTreeBuilder{
		p:                 p,
		alloc:             alloc,
		internalBaseDelta: internalBaseDelta,
	}
	if err := b.ensureLevel(0); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *vacuumInternalTreeBuilder) newNodeBuilder() (*node.Builder, error) {
	buf := make([]byte, page.PageSize)
	var nb *node.Builder
	if b.internalBaseDelta {
		nb = node.NewBuilderWithOptions(buf, page.PageTypeInternal, node.BuilderOptions{InternalBaseDelta: true})
	} else {
		nb = node.NewBuilder(buf, page.PageTypeInternal)
	}
	pid, err := b.alloc.Alloc(0)
	if err != nil {
		return nil, err
	}
	nb.SetPageID(pid)
	return nb, nil
}

func (b *vacuumInternalTreeBuilder) ensureLevel(lvl int) error {
	for len(b.levels) <= lvl {
		nb, err := b.newNodeBuilder()
		if err != nil {
			return err
		}
		b.levels = append(b.levels, &vacuumInternalTreeLevelBuilder{builder: nb})
	}
	return nil
}

func (b *vacuumInternalTreeBuilder) addParentChild(lvl int, key []byte, childID uint64) error {
	if err := b.ensureLevel(lvl + 1); err != nil {
		return err
	}
	parent := b.levels[lvl+1]
	if parent.startKey == nil {
		parent.startKey = append([]byte(nil), key...)
	}
	err := parent.builder.AddInternalChild(key, childID)
	if err == node.ErrNodeFull {
		if err := b.flush(lvl + 1); err != nil {
			return err
		}
		parent = b.levels[lvl+1]
		if parent.startKey == nil {
			parent.startKey = append([]byte(nil), key...)
		}
		err = parent.builder.AddInternalChild(key, childID)
	}
	return err
}

func (b *vacuumInternalTreeBuilder) flush(lvl int) error {
	lb := b.levels[lvl]
	if lb.builder.Count() == 0 {
		return nil
	}
	n := lb.builder.Finish()
	childID := lb.builder.PageID()
	if err := b.p.Write(childID, n.Data()); err != nil {
		return err
	}

	key := lb.startKey
	if key == nil {
		key = []byte{}
	}
	if err := b.addParentChild(lvl, key, childID); err != nil {
		return err
	}

	nb, err := b.newNodeBuilder()
	if err != nil {
		return err
	}
	lb.builder = nb
	lb.startKey = nil
	return nil
}

func (b *vacuumInternalTreeBuilder) append(key []byte, childRef page.ChildRef) error {
	lb := b.levels[0]
	if lb.startKey == nil {
		lb.startKey = append([]byte(nil), key...)
	}
	err := lb.builder.AddInternalChildRef(key, childRef)
	if err == node.ErrNodeFull {
		if err := b.flush(0); err != nil {
			return err
		}
		lb = b.levels[0]
		if lb.startKey == nil {
			lb.startKey = append([]byte(nil), key...)
		}
		err = lb.builder.AddInternalChildRef(key, childRef)
	}
	if err != nil {
		return err
	}
	b.children++
	return nil
}

func (b *vacuumInternalTreeBuilder) finish() (uint64, error) {
	if b.children == 0 {
		return 0, errors.New("vacuum: missing children")
	}

	currID := uint64(0)
	var root *node.Node
	for i := 0; i < len(b.levels); i++ {
		lb := b.levels[i]
		if lb.builder.Count() == 0 {
			continue
		}
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		if err := b.p.Write(childID, n.Data()); err != nil {
			return 0, err
		}
		currID = childID
		root = n

		if i < len(b.levels)-1 {
			key := lb.startKey
			if key == nil {
				key = []byte{}
			}
			if err := b.addParentChild(i, key, currID); err != nil {
				return 0, err
			}
		}
	}
	if currID == 0 {
		return 0, errors.New("vacuum: missing children")
	}

	if len(b.levels) > 1 && root != nil && root.Count() == 1 {
		childRef, err := root.GetInternalChildRef(0)
		if err == nil && childRef.Kind == page.ChildRefPage {
			return childRef.Page, nil
		}
	}

	return currID, nil
}

func vacuumBuildInternalTreeFromLeafRefs(src *pager.Pager, rootID uint64, dst *pager.Pager, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, internalBaseDelta bool) (uint64, error) {
	return vacuumBuildInternalTreeFromLeafRefsWithObserver(src, rootID, dst, alloc, internalBaseDelta, nil)
}

func vacuumBuildInternalTreeFromLeafRefsWithObserver(src *pager.Pager, rootID uint64, dst *pager.Pager, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, internalBaseDelta bool, visitSourcePage func(uint64)) (uint64, error) {
	if src == nil {
		return 0, errors.New("vacuum: missing pager")
	}
	if rootID == 0 {
		return 0, errors.New("vacuum: missing root id")
	}
	builder, err := newVacuumInternalTreeBuilder(dst, alloc, internalBaseDelta)
	if err != nil {
		return 0, err
	}
	var walk func(uint64) error
	walk = func(id uint64) error {
		if visitSourcePage != nil {
			visitSourcePage(id)
		}
		data, err := src.Get(id)
		if err != nil {
			return err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				keyView, childRef, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return err
				}
				switch childRef.Kind {
				case page.ChildRefLeafLog:
					if err := builder.append(keyView, childRef); err != nil {
						return err
					}
				case page.ChildRefPage:
					if err := walk(childRef.Page); err != nil {
						return err
					}
				default:
					return fmt.Errorf("vacuum: unexpected child ref kind %d at page %d", childRef.Kind, id)
				}
			}
			return nil
		case page.PageTypeLeaf:
			return fmt.Errorf("vacuum: unexpected pager-backed leaf page %d while collecting leafrefs", id)
		default:
			return fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), id)
		}
	}
	if err := walk(rootID); err != nil {
		return 0, err
	}
	return builder.finish()
}

func vacuumTreeAllLeafRefsIfComplete(p *pager.Pager, rootID uint64) (bool, error) {
	return vacuumTreeAllLeafRefsIfCompleteWithObserver(p, rootID, nil)
}

func vacuumTreeAllLeafRefsIfCompleteWithObserver(p *pager.Pager, rootID uint64, visitSourcePage func(uint64)) (bool, error) {
	if p == nil {
		return false, errors.New("vacuum: missing pager")
	}
	if rootID == 0 {
		return false, errors.New("vacuum: missing root id")
	}

	leafRefs := 0
	var walk func(uint64) (bool, error)
	walk = func(id uint64) (bool, error) {
		if visitSourcePage != nil {
			visitSourcePage(id)
		}
		data, err := p.Get(id)
		if err != nil {
			return false, err
		}
		n := node.NewNode(data)
		switch n.Type() {
		case page.PageTypeInternal:
			count := n.Count()
			for i := uint16(0); i < count; i++ {
				_, childRef, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return false, err
				}
				if childRef.Kind == page.ChildRefLeafLog {
					leafRefs++
					continue
				}
				if childRef.Kind != page.ChildRefPage {
					return false, fmt.Errorf("vacuum: unexpected child ref kind %d at page %d", childRef.Kind, id)
				}
				allLeafRefs, err := walk(childRef.Page)
				if err != nil || !allLeafRefs {
					return allLeafRefs, err
				}
			}
			return true, nil
		case page.PageTypeLeaf:
			return false, nil
		default:
			return false, fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), id)
		}
	}
	allLeafRefs, err := walk(rootID)
	if err != nil || !allLeafRefs || leafRefs == 0 {
		return false, err
	}
	return true, nil
}

func vacuumClonePagerTreeWithLeafRefs(oldPager *pager.Pager, rootID uint64, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, newPager *pager.Pager, internalBaseDelta bool) (uint64, error) {
	return vacuumClonePagerTreeWithLeafRefsWithObserver(oldPager, rootID, alloc, newPager, internalBaseDelta, nil)
}

func vacuumClonePagerTreeWithLeafRefsWithObserver(oldPager *pager.Pager, rootID uint64, alloc interface {
	Alloc(hint uint64) (uint64, error)
}, newPager *pager.Pager, internalBaseDelta bool, visitSourcePage func(uint64)) (uint64, error) {
	if oldPager == nil {
		return 0, errors.New("vacuum: missing source pager")
	}
	if newPager == nil {
		return 0, errors.New("vacuum: missing destination pager")
	}
	c := &vacuumCloneCtx{
		oldPager:          oldPager,
		newPager:          newPager,
		internalBaseDelta: internalBaseDelta,
		visitSourcePage:   visitSourcePage,
		alloc:             alloc,
		remap:             make(map[uint64]uint64, 1024),
	}
	return c.cloneNode(rootID)
}

func (c *vacuumCloneCtx) cloneNode(oldID uint64) (uint64, error) {
	if newID, ok := c.remap[oldID]; ok {
		return newID, nil
	}
	if c.oldPager == nil || c.newPager == nil || c.alloc == nil {
		return 0, errors.New("vacuum: clone missing pager/allocator")
	}

	if c.visitSourcePage != nil {
		c.visitSourcePage(oldID)
	}
	data, err := c.oldPager.Get(oldID)
	if err != nil {
		return 0, err
	}
	n := node.NewNode(data)

	switch n.Type() {
	case page.PageTypeInternal:
		newID, err := c.alloc.Alloc(oldID)
		if err != nil {
			return 0, err
		}
		c.remap[oldID] = newID

		buf := make([]byte, page.PageSize)
		b := node.NewBuilderWithOptions(buf, page.PageTypeInternal, node.BuilderOptions{
			InternalBaseDelta: c.internalBaseDelta || n.InternalBaseDeltaEnabled(),
		})
		b.SetPageID(newID)
		if low, high, ok, err := n.InternalFenceBounds(); err != nil {
			return 0, err
		} else if ok {
			b.SetInternalFenceBounds(low, high)
		}

		count := n.Count()
		for i := uint16(0); i < count; i++ {
			keyView, childRef, err := n.GetInternalEntryRefView(i)
			if err != nil {
				return 0, err
			}
			if childRef.Kind == page.ChildRefPage {
				childNew, err := c.cloneNode(childRef.Page)
				if err != nil {
					return 0, err
				}
				childRef = page.PageChildRef(childNew)
			}
			if err := b.AddInternalChildRef(keyView, childRef); err != nil {
				return 0, err
			}
		}

		out := b.Finish()
		if err := c.newPager.Write(newID, out.Data()); err != nil {
			return 0, err
		}
		return newID, nil

	case page.PageTypeLeaf:
		newID, err := c.alloc.Alloc(oldID)
		if err != nil {
			return 0, err
		}
		c.remap[oldID] = newID

		buf := make([]byte, page.PageSize)
		copy(buf, data)
		out := node.NewNode(buf)
		out.SetPageID(newID)
		out.UpdateChecksum()
		if err := c.newPager.Write(newID, out.Data()); err != nil {
			return 0, err
		}
		return newID, nil

	default:
		return 0, fmt.Errorf("vacuum: unexpected page type %d at page %d", n.Type(), oldID)
	}
}
