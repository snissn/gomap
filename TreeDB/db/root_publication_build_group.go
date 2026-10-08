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
	negativeCoverage *negativeRootCoverage
	mu               sync.Mutex

	db            *DB
	coordinator   *rootpublication.Coordinator
	builder       *rootpublication.BuilderToken
	idx           *indexGen
	tracker       *allocTracker
	registryID    int64
	registered    bool
	capturedBasis *Snapshot

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
	finalizing           bool
	finalizationDone     chan struct{}
}

type rootPublicationBuildGroupVacuumMutation struct {
	entries []batchpkg.Entry
	ranges  []batchpkg.DeleteRange
}

// BeginRootPublicationBuildGroup admits one logical multi-batch root build.
// Every batch must attach the returned group, marking exactly one final batch.
// Close is mandatory on every path and aborts an unfinished build.
func (db *DB) BeginRootPublicationBuildGroup() (*RootPublicationBuildGroup, error) {
	return db.beginRootPublicationBuildGroup(nil)
}

var ErrRootPublicationBasisMismatch = errors.New("root publication captured basis mismatch")

// BeginRootPublicationBuildGroupFromSnapshot uses the captured user tree as
// the build basis. The active snapshot pin excludes in-place changes and page
// reuse, so identical index handle/root identity proves the same user tree.
// Fresh system-only publication state is preserved. A changed user tree must
// first complete an explicit coherent cache handoff; this method never rebases.
func (db *DB) BeginRootPublicationBuildGroupFromSnapshot(basis *Snapshot) (*RootPublicationBuildGroup, error) {
	if basis == nil {
		return nil, ErrRootPublicationBasisMismatch
	}
	return db.beginRootPublicationBuildGroup(basis)
}

func (db *DB) beginRootPublicationBuildGroup(basis *Snapshot) (_ *RootPublicationBuildGroup, retErr error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.teardownMu.RLock()
	group := &RootPublicationBuildGroup{db: db, teardownLocked: true}
	if basis != nil {
		if err := basis.beginRead(); err != nil {
			db.teardownMu.RUnlock()
			return nil, err
		}
		group.capturedBasis = basis
	}

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

	if basis != nil && (basis.db != db || basis.idx != idx || basis.treeRoot != db.meta.UserRootPageID) {
		db.mu.RUnlock()
		db.rootReuseMu.RUnlock()
		return nil, ErrRootPublicationBasisMismatch
	}
	group.baseRoot = db.meta.UserRootPageID
	group.currentRoot = group.baseRoot
	group.systemRoot = db.meta.SystemRootPageID
	group.baseSeq = db.meta.CommitSeq
	if basis == nil {
		group.registryID = idx.registry.Register(group.baseSeq)
	} else {
		group.registryID, _ = idx.registry.RegisterFastWithHint(group.baseSeq, -1)
		if group.registryID == 0 {
			db.mu.RUnlock()
			db.rootReuseMu.RUnlock()
			return nil, ErrSnapshotCapacity
		}
	}
	group.registered = true
	group.negativeCoverage = db.prepareNegativeCoverage(idx, group.baseSeq, group.baseRoot, group.baseRoot, nil)
	db.mu.RUnlock()
	db.rootReuseMu.RUnlock()
	return group, nil
}

func (group *RootPublicationBuildGroup) validateBatchLocked(b *Batch, syncWrite bool) error {
	if group == nil || group.db == nil || group.closed || group.failed || group.finalizing {
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
	if c := group.negativeCoverage; c != nil {
		for i := range entries {
			c.filter.Add(entries[i].Key)
		}
		c.nextRoot = newRoot
	}
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
	// The finalizing owner retains every field and resource while detached.
	// Close and another member must not clean up this private output.
	group.finalizing = true
	group.finalizationDone = make(chan struct{})
	touched := group.touchedValueLogSegmentSliceLocked()
	opts := finalizeCommitOptions{
		negativeCoverage:            group.negativeCoverage,
		skipConditionalRootConflict: true, maxEntryRevision: group.maxEntryRevision,
		rootPublicationBuilder: group.builder, closeTeardownPinned: true,
		expectedBaseCommitSeq: group.baseSeq, hasExpectedBaseCommitSeq: true,
		writerSerialized: true,
		recordVacuumMutation: func() {
			for _, mutation := range group.vacuumMutations {
				db.vacuum.RecordApplyPlan(mutation.entries, mutation.ranges)
			}
		},
	}
	group.mu.Unlock()
	post, err := b.publishWriterOutput(group.idx, group.currentRoot, group.systemRoot, group.retired, syncWrite, group.metrics, touched, group.vlogRefDelta, b.commandWALPublishIntent, nil, opts,
		func() { db.writeMu.Unlock() }, func() { db.writeMu.Lock() })
	group.mu.Lock()
	group.writeLocked = false
	group.durablePublishLocked = false
	// A builder is consumed only by finalization. Pre-capture errors leave its
	// idempotent Release to cleanup, and accepted paths may release it again.
	if err == nil || post.accepted {
		if err != nil {
			releaseValueLogRefDelta(group.vlogRefDelta)
		}
		group.observeAcceptedOutputLocked()
		if err == nil {
			db.finalizeCommitPostWork(post)
		}
		db.clearLeafGenerationReachabilityCaches()
	} else {
		db.observeFlushApplyAbandonedOutput(group.metrics, len(group.retired))
		if errors.Is(err, errDurableRootCandidateStale) {
			err = errors.Join(ErrRootPublicationBasisMismatch, err)
		}
	}
	group.finalizing = false
	close(group.finalizationDone)
	return err
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
	cleanupErr := group.cleanupLocked(!group.accepted && !errors.Is(err, ErrRecoveryRequired))
	return errors.Join(err, cleanupErr)
}

func (group *RootPublicationBuildGroup) cleanupLocked(abandon bool) error {
	if group == nil || group.closed {
		return nil
	}
	if group.capturedBasis != nil {
		group.capturedBasis.endRead()
		group.capturedBasis = nil
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
		group.db.maybeReleaseRetiredIndex(group.idx)
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

// Accepted reports the existing publication receipt independently of write or
// cleanup errors. Acceptance is irreversible and remains observable after Close.
// Coordinators must retain coverage of an accepted group when later work fails.
func (group *RootPublicationBuildGroup) Accepted() bool {
	if group == nil {
		return false
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	return group.accepted
}

// Close aborts an unfinished logical build. It is idempotent.
func (group *RootPublicationBuildGroup) Close() error {
	if group == nil {
		return nil
	}
	for {
		group.mu.Lock()
		if group.finalizing {
			done := group.finalizationDone
			group.mu.Unlock()
			<-done
			continue
		}
		err := group.cleanupLocked(!group.accepted)
		group.mu.Unlock()
		return err
	}
}
