package db

import (
	"context"
	"errors"
	"sync"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// RootPublicationBuildGroup stages a logical cached apply across bounded
// backend batches. Intermediate batches build private COW roots only. The final
// batch transfers one complete root into one PreparedRootCandidate.
//
// This is an internal cross-package bridge for caching; callers outside TreeDB
// internals must not depend on it as a stable API.
type RootPublicationBuildGroup struct {
	mu sync.Mutex

	db          *DB
	coordinator *rootpublication.Coordinator
	builder     *rootpublication.BuilderToken
	idx         *indexGen
	tracker     *allocTracker
	registryID  int64
	registered  bool

	baseRoot    uint64
	currentRoot uint64
	systemRoot  uint64
	baseSeq     uint64

	retired                 []uint64
	metrics                 adaptive.Metrics
	vlogRefDelta            *valueLogRefDelta
	touchedValueLogSegments map[uint32]struct{}
	pinnedValueLogSegments  map[uint32]struct{}
	maxEntryRevision        page.EntryRevision
	vacuumMutations         []rootPublicationBuildGroupVacuumMutation

	teardownLocked       bool
	writeLocked          bool
	durablePublishLocked bool
	failed               bool
	accepted             bool
	closed               bool
}

type rootPublicationBuildGroupVacuumMutation struct {
	entries []batchpkg.Entry
	ranges  []batchpkg.DeleteRange
}

// BeginRootPublicationBuildGroup admits one logical multi-batch root build.
// Every batch must attach the returned group, marking exactly one final batch.
// Close is mandatory on every path and aborts an unfinished build.
func (db *DB) BeginRootPublicationBuildGroup() (_ *RootPublicationBuildGroup, retErr error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.teardownMu.RLock()
	group := &RootPublicationBuildGroup{db: db, teardownLocked: true}
	defer func() {
		if retErr == nil {
			return
		}
		group.mu.Lock()
		retErr = errors.Join(retErr, group.cleanupLocked(true))
		group.mu.Unlock()
	}()
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if db.closing.Load() {
		return nil, ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return nil, err
	}
	for {
		runtime := db.rootPublication
		if runtime == nil || runtime.coordinator == nil {
			return nil, errors.New("missing root publication coordinator")
		}
		builder, err := runtime.coordinator.AcquireBuilder(context.Background())
		if err != nil {
			return nil, publicRootPublicationErrorV1(err)
		}

		db.writeMu.Lock()
		if err := db.checkWriteAdmissionLocked(); err != nil {
			db.writeMu.Unlock()
			builder.Release()
			return nil, err
		}
		if db.rootPublication != runtime {
			db.writeMu.Unlock()
			builder.Release()
			if db.closing.Load() {
				return nil, ErrClosed
			}
			continue
		}
		group.coordinator = runtime.coordinator
		group.builder = builder
		group.writeLocked = true
		break
	}
	db.durablePublishMu.Lock()
	group.durablePublishLocked = true
	if err := db.commandWALPoisonedError(); err != nil {
		return nil, err
	}
	idx := db.idx.Load()
	if idx == nil {
		return nil, errors.New("missing index")
	}
	group.idx = idx
	group.tracker = newAllocTracker(idx.allocator)

	db.rootReuseMu.RLock()
	db.mu.RLock()
	group.baseRoot = db.meta.UserRootPageID
	group.currentRoot = group.baseRoot
	group.systemRoot = db.meta.SystemRootPageID
	group.baseSeq = db.meta.CommitSeq
	group.registryID = idx.registry.Register(group.baseSeq)
	group.registered = true
	db.mu.RUnlock()
	db.rootReuseMu.RUnlock()
	return group, nil
}

func (group *RootPublicationBuildGroup) validateBatchLocked(b *Batch, syncWrite bool) error {
	if group == nil || group.db == nil || group.closed || group.failed {
		return errors.New("invalid root publication build group")
	}
	if b == nil || b.db != group.db || b.batch == nil || !b.physicalOnly {
		return errors.New("root publication build group requires a physical batch from the same database")
	}
	if !group.writeLocked || !group.durablePublishLocked || group.idx == nil || group.tracker == nil || group.builder == nil {
		return errors.New("root publication build group lost build ownership")
	}
	if !b.rootPublicationBuildGroupFinal && syncWrite {
		return errors.New("intermediate root publication build group batch cannot sync")
	}
	if !b.rootPublicationBuildGroupFinal && b.commandWALPublishIntent != nil {
		return errors.New("intermediate root publication build group batch cannot publish command WAL progress")
	}
	return nil
}

func (group *RootPublicationBuildGroup) pinBatchValueLogSegmentsLocked(delta *batchpkg.Batch) {
	if group == nil || group.db == nil || delta == nil {
		return
	}
	ids := delta.TouchedValueLogSegments()
	if len(ids) == 0 {
		return
	}
	if group.touchedValueLogSegments == nil {
		group.touchedValueLogSegments = make(map[uint32]struct{}, len(ids))
	}
	if group.pinnedValueLogSegments == nil {
		group.pinnedValueLogSegments = make(map[uint32]struct{}, len(ids))
	}
	group.db.pendingValueLogAppendMu.Lock()
	defer group.db.pendingValueLogAppendMu.Unlock()
	for _, fileID := range ids {
		if fileID == 0 {
			continue
		}
		group.touchedValueLogSegments[fileID] = struct{}{}
		if _, ok := group.pinnedValueLogSegments[fileID]; ok {
			continue
		}
		if group.db.pendingValueLogAppendFileIDRefs == nil {
			group.db.pendingValueLogAppendFileIDRefs = make(map[uint32]int)
		}
		group.db.pendingValueLogAppendFileIDRefs[fileID]++
		group.pinnedValueLogSegments[fileID] = struct{}{}
	}
}

func (group *RootPublicationBuildGroup) mergeValueLogRefDeltaLocked(delta *valueLogRefDelta) {
	mergeValueLogRefDeltaInto(&group.vlogRefDelta, delta)
}

func (group *RootPublicationBuildGroup) recordVacuumMutationLocked(entries []batchpkg.Entry, ranges []batchpkg.DeleteRange) {
	if group == nil || group.db == nil || !group.db.vacuum.Active() || (len(entries) == 0 && len(ranges) == 0) {
		return
	}
	mutation := rootPublicationBuildGroupVacuumMutation{
		entries: make([]batchpkg.Entry, 0, len(entries)),
		ranges:  make([]batchpkg.DeleteRange, 0, len(ranges)),
	}
	for i := range entries {
		mutation.entries = append(mutation.entries, vacuumRecordCopyEntry(entries[i]))
	}
	for i := range ranges {
		mutation.ranges = append(mutation.ranges, vacuumRecordCopyRange(ranges[i]))
	}
	group.vacuumMutations = append(group.vacuumMutations, mutation)
}

func (group *RootPublicationBuildGroup) applyBatchLocked(b *Batch) error {
	db := group.db
	rootID := group.currentRoot
	applyOpts := b.flushApplyOptions()
	applyOpts.CollectOldPointerRefs = db.shouldCollectValueLogRefDelta(group.baseSeq)
	prepareBuf := db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	z := group.idx.zipper.CloneWithAllocator(group.tracker)
	var (
		newRoot   uint64
		retired   []uint64
		metrics   adaptive.Metrics
		result    zipper.ApplyResult
		applyErr  error
		spanState flushApplySpanNativePublishSnapshot
	)
	useOptions := flushApplyUseOptions(applyOpts)
	if useOptions {
		result, applyErr = z.ApplyWithOptions(rootID, b.batch, applyOpts)
		spanState = newFlushApplySpanNativePublishSnapshot(result)
		db.observeFlushApplyPrepareResult(result, applyErr)
		db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &result)
		newRoot = result.RootID
		retired = result.PendingRetiredPages
		metrics = result.Metrics
	} else {
		newRoot, retired, metrics, applyErr = z.Apply(rootID, b.batch)
	}
	db.observeRawSpanNativeApplyResult(b.rawSpanNativeBatchPlan(), result, applyErr, useOptions, applyOpts.SpanNativeApply)
	db.observeFlushApplyMetrics(metrics, time.Duration(metrics.ZipperApplyWallNs), applyErr)
	db.observeFlushApplyPreparedOutput(metrics, len(retired))
	if applyErr != nil {
		db.observeRawBatchSpanNativePublishFallback(b.rawSpanNativeBatchPlan(), spanState, FlushSpanRunFallbackOutputOwnershipFailure)
		db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		return applyErr
	}

	entries, ranges := b.batch.ApplyPlan()
	delta, err := db.buildValueLogRefDelta(
		group.idx.pager, rootID, group.baseSeq, entries, ranges,
		&result.OldPointerRefs, result.OldEntriesRemoved, result.OldPointerRefsCollected,
	)
	if err != nil {
		db.observeRawBatchSpanNativePublishFallback(b.rawSpanNativeBatchPlan(), spanState, FlushSpanRunFallbackOutputOwnershipFailure)
		db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		return err
	}
	group.mergeValueLogRefDeltaLocked(delta)
	releaseValueLogRefDelta(delta)
	group.recordVacuumMutationLocked(entries, ranges)
	group.currentRoot = newRoot
	group.retired = append(group.retired, retired...)
	mergeOrderedRootPublishMetrics(&group.metrics, metrics)
	return nil
}

func (group *RootPublicationBuildGroup) releaseWriteLocked() {
	if group == nil || !group.writeLocked {
		return
	}
	group.writeLocked = false
	group.db.writeMu.Unlock()
}

func (group *RootPublicationBuildGroup) releaseDurablePublishLocked() {
	if group == nil || !group.durablePublishLocked {
		return
	}
	group.durablePublishLocked = false
	group.db.durablePublishMu.Unlock()
}

func (group *RootPublicationBuildGroup) touchedValueLogSegmentSliceLocked() []uint32 {
	if len(group.touchedValueLogSegments) == 0 {
		return nil
	}
	ids := make([]uint32, 0, len(group.touchedValueLogSegments))
	for fileID := range group.touchedValueLogSegments {
		ids = append(ids, fileID)
	}
	return ids
}

func (group *RootPublicationBuildGroup) observeAcceptedOutputLocked() {
	group.accepted = true
	group.vlogRefDelta = nil
	group.db.observeFlushApplyInstalledOutput(group.metrics, len(group.retired))
	group.db.invalidateLeafGenerationSubtreeStats(group.tracker.Pages())
}

func (group *RootPublicationBuildGroup) finalizeLocked(b *Batch, syncWrite bool) error {
	db := group.db
	group.releaseWriteLocked()
	publishPrepareGuard, err := db.prepareFlushApplyPublish(syncWrite)
	if err != nil {
		return err
	}
	defer publishPrepareGuard.Release()

	intent := b.commandWALPublishIntent
	commandAppended := false
	if intent != nil {
		if _, err := db.appendRawKVCommandWALIntent(intent, syncWrite); err != nil {
			return err
		}
		commandAppended = true
	}
	recordVacuumMutation := func() {
		for i := range group.vacuumMutations {
			mutation := group.vacuumMutations[i]
			db.vacuum.RecordApplyPlan(mutation.entries, mutation.ranges)
		}
	}
	opts := finalizeCommitOptions{
		skipPrePublishFlush:         true,
		skipConditionalRootConflict: true,
		maxEntryRevision:            group.maxEntryRevision,
		durablePublishLocked:        true,
		durablePublishRelease:       group.releaseDurablePublishLocked,
		rootPublicationBuilder:      group.builder,
		closeTeardownPinned:         true,
		expectedBaseCommitSeq:       group.baseSeq,
		hasExpectedBaseCommitSeq:    true,
		releaseRootSerialization:    func() {},
		recordVacuumMutation:        recordVacuumMutation,
		durableIndex:                group.idx,
	}
	if intent != nil {
		commandOpts := commandWALFinalizeOptions(intent)
		commandOpts.skipPrePublishFlush = opts.skipPrePublishFlush
		commandOpts.skipConditionalRootConflict = opts.skipConditionalRootConflict
		commandOpts.maxEntryRevision = opts.maxEntryRevision
		commandOpts.durablePublishLocked = opts.durablePublishLocked
		commandOpts.durablePublishRelease = opts.durablePublishRelease
		commandOpts.rootPublicationBuilder = opts.rootPublicationBuilder
		commandOpts.closeTeardownPinned = opts.closeTeardownPinned
		commandOpts.expectedBaseCommitSeq = opts.expectedBaseCommitSeq
		commandOpts.hasExpectedBaseCommitSeq = opts.hasExpectedBaseCommitSeq
		commandOpts.releaseRootSerialization = opts.releaseRootSerialization
		commandOpts.recordVacuumMutation = opts.recordVacuumMutation
		commandOpts.durableIndex = opts.durableIndex
		opts = commandOpts
	}
	post, err := db.finalizeCommitLockedWithOptions(
		group.currentRoot, group.systemRoot, group.retired, syncWrite, group.metrics,
		group.touchedValueLogSegmentSliceLocked(), db.indexOuterLeavesInValueLog,
		group.vlogRefDelta, nil, nil, opts,
	)
	// finalizeCommitLockedWithOptions always consumes or releases its builder
	// handle. Keep cleanup from treating the same token as live ownership.
	group.builder = nil
	if err != nil {
		if commandAppended && !post.accepted {
			db.poisonCommandWALAfterPostAppendFailure(intent)
		}
		if post.accepted {
			group.observeAcceptedOutputLocked()
			db.clearLeafGenerationReachabilityCaches()
		} else {
			db.observeFlushApplyAbandonedOutput(group.metrics, len(group.retired))
		}
		return err
	}
	group.observeAcceptedOutputLocked()
	db.finalizeCommitPostWork(post)
	db.clearLeafGenerationReachabilityCaches()
	return nil
}

func (group *RootPublicationBuildGroup) writeBatch(b *Batch, syncWrite bool, maxEntryRevision page.EntryRevision) (retErr error) {
	group.mu.Lock()
	defer group.mu.Unlock()
	if err := group.validateBatchLocked(b, syncWrite); err != nil {
		return err
	}
	group.pinBatchValueLogSegmentsLocked(b.batch)
	defer group.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
	if maxEntryRevision > group.maxEntryRevision {
		group.maxEntryRevision = maxEntryRevision
	}
	if err := group.applyBatchLocked(b); err != nil {
		group.failed = true
		return err
	}
	if !b.rootPublicationBuildGroupFinal {
		return nil
	}
	err := group.finalizeLocked(b, syncWrite)
	group.failed = err != nil
	cleanupErr := group.cleanupLocked(!group.accepted)
	return errors.Join(err, cleanupErr)
}

func (group *RootPublicationBuildGroup) cleanupLocked(abandon bool) error {
	if group == nil || group.closed {
		return nil
	}
	group.closed = true
	var cleanupErr error
	if abandon && group.tracker != nil {
		cleanupErr = errors.Join(cleanupErr, group.tracker.FreeAll())
	}
	if group.vlogRefDelta != nil {
		releaseValueLogRefDelta(group.vlogRefDelta)
		group.vlogRefDelta = nil
	}
	if group.idx != nil && group.registered {
		group.idx.registry.Unregister(group.registryID)
		group.registered = false
	}
	if len(group.pinnedValueLogSegments) != 0 && group.db != nil {
		release := make(map[uint32]int64, len(group.pinnedValueLogSegments))
		for fileID := range group.pinnedValueLogSegments {
			release[fileID] = 1
		}
		group.db.releasePendingValueLogAppendFileIDCounts(release)
		group.pinnedValueLogSegments = nil
	}
	group.releaseWriteLocked()
	group.releaseDurablePublishLocked()
	if group.builder != nil {
		group.builder.Release()
		group.builder = nil
	}
	if group.teardownLocked && group.db != nil {
		group.teardownLocked = false
		group.db.teardownMu.RUnlock()
	}
	return cleanupErr
}

// Close aborts an unfinished logical build. It is idempotent.
func (group *RootPublicationBuildGroup) Close() error {
	if group == nil {
		return nil
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.cleanupLocked(!group.accepted)
}
