package db

import (
	"errors"
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// Batch implements the cosmos-db Batch interface.
type Batch struct {
	db                                 *DB
	batch                              *batch.Batch
	physicalOnly                       bool
	commandWALPublishIntent            *commandWALBatchIntent
	conditionalTxnID                   uint64
	flushApplySpanNativeFallbackReason FlushSpanRunFallbackReason
	rootPublicationBuildGroup          *RootPublicationBuildGroup
	rootPublicationBuildGroupFinal     bool
}

const optimisticWriteMaxAttempts = 3

const (
	// Public NewBatchWithSize comes from cosmos-db, where callers often pass a
	// byte-oriented flush threshold rather than an entry count. Keep small hints
	// behaving like entry reserves, but normalize larger hints conservatively so
	// a 100kB budget does not preallocate 100k entries.
	publicBatchHintExactEntryReserveMax = 8 * 1024
	publicBatchHintApproxBytesPerEntry  = 256
	publicBatchHintNormalizedEntryCap   = publicBatchHintExactEntryReserveMax
)

func (db *DB) NewBatch() batch.Interface {
	return db.newBatchWithEntryReserve(0)
}

// NewPhysicalBatch creates a backend batch that mutates the physical index
// without appending a RawKVBatch command frame. It is for higher-level command
// WAL executors that have already appended their logical command frames before
// making writes visible through the cached layer.
func (db *DB) NewPhysicalBatch() batch.Interface {
	b := db.newBatchWithEntryReserve(0)
	if pb, ok := b.(*Batch); ok {
		pb.physicalOnly = true
	}
	return b
}

// NewBatchWithSize accepts the public cosmos-db style size hint. Small values
// are treated like exact entry reserves; larger values are normalized as
// approximate byte budgets and capped to avoid preallocating one entry per
// byte. The normalization is intentionally discontinuous at the cutover:
// `publicBatchHintExactEntryReserveMax` still means "reserve that many
// entries", while the next value is treated as a byte budget and normalized
// downward.
func (db *DB) NewBatchWithSize(size int) batch.Interface {
	reserveHint := NormalizePublicBatchReserveHint(size)
	return db.newBatchWithReserveHint(reserveHint)
}

// NewPhysicalBatchWithSize is NewPhysicalBatch with the public size hint.
func (db *DB) NewPhysicalBatchWithSize(size int) batch.Interface {
	reserveHint := NormalizePublicBatchReserveHint(size)
	b := db.newBatchWithReserveHint(reserveHint)
	if pb, ok := b.(*Batch); ok {
		pb.physicalOnly = true
	}
	return b
}

// SetFlushApplySpanNativeFallback forces this internal batch's span-native
// candidate path to use fallback accounting with reason. It is used by cached
// checkpoint/close drains and tests; callers outside TreeDB internals should not
// rely on it as a stable public API.
func (b *Batch) SetFlushApplySpanNativeFallback(reason FlushSpanRunFallbackReason) {
	if b == nil {
		return
	}
	b.flushApplySpanNativeFallbackReason = reason
}

// SetRootPublicationBuildGroup attaches one batch of a logical chunked cached
// apply. Exactly one attached batch must be marked final. Intermediate batches
// build private roots; only the final batch creates a publication candidate.
func (b *Batch) SetRootPublicationBuildGroup(group *RootPublicationBuildGroup, final bool) error {
	if b == nil || b.db == nil || group == nil {
		return errors.New("missing root publication build group")
	}
	if !b.physicalOnly {
		return errors.New("root publication build group requires a physical batch")
	}
	if b.rootPublicationBuildGroup != nil {
		return errors.New("root publication build group already attached")
	}
	runtime := b.db.rootPublication
	if runtime == nil || runtime.coordinator == nil || group.coordinator != runtime.coordinator {
		return errors.New("root publication build group belongs to another database")
	}
	b.rootPublicationBuildGroup = group
	b.rootPublicationBuildGroupFinal = final
	return nil
}

func (b *Batch) flushApplyOptions() zipper.ApplyOptions {
	if b == nil || b.db == nil {
		return zipper.ApplyOptions{}
	}
	opts := b.db.flushApplyOptions()
	reason := b.flushApplySpanNativeFallbackReason
	if opts.SpanNativeApply && reason.Valid() && reason != FlushSpanRunFallbackUnknown {
		opts.SpanNativeForceFallbackReason = reason.String()
	}
	return opts
}

// SetCommandWALPublish records an already-appended command-WAL LSN range to
// publish atomically with this physical batch's root commit.
func (b *Batch) SetCommandWALPublish(appliedLSN uint64, covered []CommandWALLSNRange) error {
	if b == nil {
		return ErrClosed
	}
	if appliedLSN == 0 {
		b.commandWALPublishIntent = nil
		return nil
	}
	if !b.physicalOnly {
		return fmt.Errorf("treedb: command wal publish piggyback requires physical batch")
	}
	if len(covered) != 1 {
		return fmt.Errorf("treedb: command wal publish piggyback requires one covered range")
	}
	r := covered[0]
	if r.First == 0 || r.Last < r.First || r.Last != appliedLSN {
		return fmt.Errorf("%w: invalid coverage range [%d,%d] for applied %d", ErrCommandWALAppliedLSNNonContig, r.First, r.Last, appliedLSN)
	}
	b.commandWALPublishIntent = &commandWALBatchIntent{
		lsn:          appliedLSN,
		coveredRange: [1]CommandWALLSNRange{r},
	}
	return nil
}

// NormalizePublicBatchReserveHint keeps small public hints behaving like entry
// reserves, but treats larger hints as approximate byte budgets so callers do
// not accidentally preallocate one entry per byte. This is intentionally
// discontinuous at the cutover for compatibility with small entry-count hints.
//
// For internal use; behavior may change without notice and is not part of the
// supported external API surface of the db package.
func NormalizePublicBatchReserveHint(size int) int {
	if size <= 0 {
		return 0
	}
	if size <= publicBatchHintExactEntryReserveMax {
		return size
	}
	entries := size / publicBatchHintApproxBytesPerEntry
	if size%publicBatchHintApproxBytesPerEntry != 0 {
		entries++
	}
	// Defensive guard in case the cutover/bytes-per-entry constants change.
	if entries < 1 {
		entries = 1
	}
	if entries > publicBatchHintNormalizedEntryCap {
		entries = publicBatchHintNormalizedEntryCap
	}
	return entries
}

func (db *DB) newBatchWithEntryReserve(entries int) batch.Interface {
	return db.newBatchWithReserveHint(entries)
}

func (db *DB) newBatchWithReserveHint(reserveHint int) batch.Interface {
	if db != nil && db.testBatchCreateHook != nil {
		db.testBatchCreateHook()
	}
	threshold := db.InlineThreshold()
	domains := db.valueLogDomainThresholds
	if reserveHint < 0 {
		reserveHint = 0
	}
	internal := batch.New(db.valueLogManager, threshold)
	if threshold > 0 {
		internal.SetInlineThresholdResolver(func(key []byte) int {
			return resolveBatchInlineThresholdForKey(threshold, key, domains)
		})
	}
	internal.Reserve(reserveHint)
	return &Batch{
		db:    db,
		batch: internal,
	}
}

func resolveBatchInlineThresholdForKey(threshold int, key []byte, domains []ValueLogDomainThreshold) int {
	if threshold > 0 {
		return ResolveInlineThresholdForKey(threshold, key, domains)
	}
	return threshold
}

func (b *Batch) Set(key, value []byte) error {
	return b.batch.Set(key, value)
}

func (b *Batch) SetWithRevision(key, value []byte, revision page.EntryRevision) error {
	return b.batch.SetWithRevision(key, value, revision)
}

// SetView records a Put without copying key/value bytes. Callers must treat
// key/value as immutable until the batch is written or closed.
//
// This is intentionally not part of the public batch.Interface; it is a
// best-effort optimization used by higher-level layers (e.g. cached streaming).
func (b *Batch) SetView(key, value []byte) error {
	return b.batch.SetView(key, value)
}

func (b *Batch) SetViewWithRevision(key, value []byte, revision page.EntryRevision) error {
	return b.batch.SetViewWithRevision(key, value, revision)
}

func (b *Batch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *Batch) DeleteWithRevision(key []byte, revision page.EntryRevision) error {
	return b.batch.DeleteWithRevision(key, revision)
}

func (b *Batch) DeleteRange(start, end []byte) error {
	return b.batch.DeleteRange(start, end)
}

// DeleteView records a Delete without copying the key bytes. Callers must treat
// key as immutable until the batch is written or closed.
func (b *Batch) DeleteView(key []byte) error {
	return b.batch.DeleteView(key)
}

func (b *Batch) DeleteViewWithRevision(key []byte, revision page.EntryRevision) error {
	return b.batch.DeleteViewWithRevision(key, revision)
}

// SetPointer records a pointer without copying the value bytes.
func (b *Batch) SetPointer(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointer(key, ptr)
}

func (b *Batch) SetPointerWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) error {
	return b.batch.SetPointerWithRevision(key, ptr, revision)
}

// SetPointerView records a pointer without copying the key bytes.
func (b *Batch) SetPointerView(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointerView(key, ptr)
}

func (b *Batch) SetPointerViewWithRevision(key []byte, ptr page.ValuePtr, revision page.EntryRevision) error {
	return b.batch.SetPointerViewWithRevision(key, ptr, revision)
}

func (b *Batch) SetOps(ops []batch.Entry) error {
	return b.batch.SetOps(ops)
}

// Reserve forwards best-effort preallocation hints to the internal batch.
func (b *Batch) Reserve(n int) {
	if b == nil || b.batch == nil || n <= 0 {
		return
	}
	b.batch.Reserve(n)
}

func (b *Batch) Write() error {
	return b.write(false)
}

func (b *Batch) WriteSync() error {
	return b.write(true)
}

func (b *Batch) write(sync bool) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("missing db")
	}
	if b.db.readOnly {
		return ErrReadOnly
	}
	if group := b.rootPublicationBuildGroup; group != nil {
		if !b.physicalOnly {
			return errors.New("root publication build group requires a physical batch")
		}
		maxEntryRevision := b.db.assignBatchEntryRevisions(b.batch)
		return group.writeBatch(b, sync, maxEntryRevision)
	}
	b.db.teardownMu.RLock()
	defer b.db.teardownMu.RUnlock()
	if err := b.db.publicationPoisonedError(); err != nil {
		return err
	}
	if b.db.closing.Load() {
		return ErrClosed
	}
	if sync && b.batch != nil && b.batch.Len() == 0 && b.commandWALPublishIntent == nil {
		b.db.observeRawSpanNativeApplyResult(b.rawSpanNativeBatchPlan(), zipper.ApplyResult{}, nil, false, false)
		return b.db.Checkpoint()
	}
	maxEntryRevision := b.db.assignBatchEntryRevisions(b.batch)
	intent := b.commandWALPublishIntent
	if !b.physicalOnly && b.db.commandWAL {
		unlockRawPublish, err := b.db.lockCommandWALPublishWithBarriersTeardownPinned()
		if err != nil {
			return err
		}
		defer unlockRawPublish()
		intent, err = b.db.prepareRawKVCommandWALIntent(b, sync)
		if err != nil {
			return err
		}
		defer releaseUnassignedCommandWALIntent(intent)
	}
	return b.writeWithCommandWALIntent(sync, intent, maxEntryRevision)
}

func (b *Batch) writeWithCommandWALIntent(sync bool, intent *commandWALBatchIntent, maxEntryRevision page.EntryRevision) error {
	return b.writeWithCommandWALIntentPreflight(sync, intent, maxEntryRevision, nil)
}

func (b *Batch) writeWithCommandWALIntentPreflight(sync bool, intent *commandWALBatchIntent, maxEntryRevision page.EntryRevision, conditional *ConditionalTxn) error {
	// If a command frame is appended but root publication later returns an
	// error, the durable frame is intentionally left for reopen recovery. Reuse
	// the same intent across optimistic/serialized attempts so one user batch
	// keeps one command LSN.
	for attempt := 0; attempt < optimisticWriteMaxAttempts; attempt++ {
		committed, err := b.writeOptimistic(sync, intent, maxEntryRevision, conditional)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
		b.db.observeFlushApplyRetry()
	}
	return b.writeSerialized(sync, intent, maxEntryRevision, conditional)
}

func (b *Batch) writeOptimistic(sync bool, intent *commandWALBatchIntent, maxEntryRevision page.EntryRevision, conditional *ConditionalTxn) (bool, error) {
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()
	rawSpanPlan := b.rawSpanNativeBatchPlan()

	b.db.writeMu.RLock()
	if err := b.db.checkReadAdmissionLocked(); err != nil {
		b.db.writeMu.RUnlock()
		return false, err
	}
	if b.db.vacuumCutoverInProgress.Load() {
		b.db.writeMu.RUnlock()
		return false, nil
	}
	idx := b.db.idx.Load()
	if idx == nil {
		b.db.writeMu.RUnlock()
		return false, fmt.Errorf("missing index")
	}

	b.db.rootReuseMu.RLock()
	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	// Register this writer as a "reader" of the base state to prevent the
	// pruner from reclaiming pages we are about to read during z.Apply.
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()
	b.db.rootReuseMu.RUnlock()

	defer idx.registry.Unregister(regID)
	if hook := b.db.testAfterOptimisticBaseCaptureHook; hook != nil {
		hook()
	}

	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	applyOpts := b.flushApplyOptions()
	applyOpts.CollectOldPointerRefs = b.db.shouldCollectValueLogRefDelta(baseSeq)
	prepareBuf := b.db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	var newRoot uint64
	var retired []uint64
	var metrics adaptive.Metrics
	var err error
	var applyResult zipper.ApplyResult
	var spanNativePublishSnapshot flushApplySpanNativePublishSnapshot
	applyWithOptions := flushApplyUseOptions(applyOpts)
	if applyWithOptions {
		result, applyErr := z.ApplyWithOptions(rootID, b.batch, applyOpts)
		applyResult = result
		spanNativePublishSnapshot = newFlushApplySpanNativePublishSnapshot(result)
		b.db.observeFlushApplyPrepareResult(result, applyErr)
		b.db.observeRawSpanNativeApplyResult(rawSpanPlan, result, applyErr, applyWithOptions, applyOpts.SpanNativeApply)
		b.db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &result)
		newRoot = result.RootID
		retired = result.PendingRetiredPages
		metrics = result.Metrics
		err = applyErr
	} else {
		newRoot, retired, metrics, err = z.Apply(rootID, b.batch)
		b.db.observeRawSpanNativeApplyResult(rawSpanPlan, applyResult, err, applyWithOptions, applyOpts.SpanNativeApply)
	}
	b.db.observeFlushApplyMetrics(metrics, time.Duration(metrics.ZipperApplyWallNs), err)
	b.db.observeFlushApplyPreparedOutput(metrics, len(retired))
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	if hook := b.db.testAfterOptimisticApplyHook; hook != nil {
		hook()
	}
	entries, ranges := b.batch.ApplyPlan()
	recordVacuumMutation := func() {
		b.db.vacuum.RecordApplyPlan(entries, ranges)
	}
	conditionalMutation := conditionalCommitMutation{
		entries: entries, ranges: ranges, ownerTxnID: b.conditionalTxnID,
	}
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries, ranges, &applyResult.OldPointerRefs, applyResult.OldEntriesRemoved, applyResult.OldPointerRefsCollected)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()
	b.db.mu.RLock()
	currentRoot := b.db.meta.UserRootPageID
	currentSeq := b.db.meta.CommitSeq
	b.db.mu.RUnlock()
	if currentRoot != rootID || currentSeq != baseSeq {
		b.db.observeFlushApplyMismatch()
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackRootMismatch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}
	preparePublishWithoutRootLock := func() (*finalizeCommitPrepareGuard, error) {
		b.db.writeMu.RUnlock()
		guard, prepareErr := b.db.prepareFlushApplyPublish(sync)
		b.db.writeMu.RLock()
		return guard, prepareErr
	}
	publishPrepareGuard, err := preparePublishWithoutRootLock()
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, err
	}
	defer func() { publishPrepareGuard.Release() }()
	if hook := b.db.testAfterOptimisticPublishPrepareHook; hook != nil {
		publishPrepareGuard.Release()
		hook()
		publishPrepareGuard, err = preparePublishWithoutRootLock()
		if err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
			b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
			b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
			freeErr := tracker.FreeAll()
			b.db.writeMu.RUnlock()
			if freeErr != nil {
				return false, freeErr
			}
			return false, err
		}
	}
	// prepareFlushApplyPublish deliberately drops the read side of writeMu. If
	// online vacuum established its cutover gate in that interval, abandon this
	// old-generation candidate before it can wait on durablePublishMu while
	// retaining writeMu.RLock (which would block vacuum's final relock).
	if b.db.vacuumCutoverInProgress.Load() {
		publishPrepareGuard.Release()
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackRootMismatch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}
	commitWaitStart := time.Now()
	b.db.commitMu.Lock()
	b.db.observeFlushApplyCommitWait(time.Since(commitWaitStart))
	guardedPublishStart := time.Now()
	b.db.mu.RLock()
	currentRoot = b.db.meta.UserRootPageID
	currentSeq = b.db.meta.CommitSeq
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.RUnlock()
	if currentRoot != rootID || currentSeq != baseSeq {
		b.db.observeFlushApplyMismatch()
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackRootMismatch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), false)
		b.db.commitMu.Unlock()
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}
	if conditional != nil {
		if err = conditional.validateReadSetAtPublish(); err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
			b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
			b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), false)
			b.db.commitMu.Unlock()
			freeErr := tracker.FreeAll()
			b.db.writeMu.RUnlock()
			if freeErr != nil {
				return false, freeErr
			}
			return false, err
		}
	}

	rootLocksReleased := false
	releaseRootSerialization := func() {
		b.db.commitMu.Unlock()
		b.db.writeMu.RUnlock()
		rootLocksReleased = true
	}
	var post finalizeCommitPost
	if intent == nil {
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, finalizeCommitOptions{skipPrePublishFlush: true, skipConditionalRootConflict: true, maxEntryRevision: maxEntryRevision, closeTeardownPinned: true, expectedBaseCommitSeq: baseSeq, hasExpectedBaseCommitSeq: true, releaseRootSerialization: releaseRootSerialization, recordVacuumMutation: recordVacuumMutation, conditionalMutation: conditionalMutation})
	} else {
		if _, err = b.db.appendRawKVCommandWALIntent(intent, sync); err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
			b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
			b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
			b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), false)
			b.db.commitMu.Unlock()
			freeErr := tracker.FreeAll()
			b.db.writeMu.RUnlock()
			if freeErr != nil {
				return false, freeErr
			}
			return false, err
		}
		opts := commandWALFinalizeOptions(intent)
		opts.skipPrePublishFlush = true
		opts.skipConditionalRootConflict = true
		opts.maxEntryRevision = maxEntryRevision
		opts.closeTeardownPinned = true
		opts.expectedBaseCommitSeq = baseSeq
		opts.hasExpectedBaseCommitSeq = true
		opts.releaseRootSerialization = releaseRootSerialization
		opts.recordVacuumMutation = recordVacuumMutation
		opts.conditionalMutation = conditionalMutation
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, opts)
		// Poison while still holding commitMu only when the frame remains
		// unapplied. An accepted candidate already made the LSN visible even when
		// its admission wait reports a retryable publisher failure.
		if err != nil && !post.accepted {
			b.db.poisonCommandWALAfterPostAppendFailure(intent)
		}
	}
	b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), err == nil)
	if !rootLocksReleased {
		b.db.commitMu.Unlock()
	}
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		if !rootLocksReleased {
			b.db.writeMu.RUnlock()
		}
		if intent == nil && errors.Is(err, errDurableRootCandidateStale) {
			if freeErr := tracker.FreeAll(); freeErr != nil {
				return false, freeErr
			}
			return false, nil
		}
		return false, err
	}
	b.db.observeFlushApplyInstalledOutput(metrics, len(retired))
	vlogRefDelta = nil
	b.db.invalidateLeafGenerationSubtreeStats(tracker.Pages())
	b.db.finalizeCommitPostWork(post)
	b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
	if !rootLocksReleased {
		b.db.writeMu.RUnlock()
	}
	return true, nil
}

func (b *Batch) writeSerialized(sync bool, intent *commandWALBatchIntent, maxEntryRevision page.EntryRevision, conditional *ConditionalTxn) error {
	for {
		err := b.writeSerializedAttempt(sync, intent, maxEntryRevision, conditional)
		if !errors.Is(err, errDurableRootCandidateStale) {
			return err
		}
		b.db.observeFlushApplyRetry()
	}
}

func (b *Batch) writeSerializedAttempt(sync bool, intent *commandWALBatchIntent, maxEntryRevision page.EntryRevision, conditional *ConditionalTxn) error {
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()
	rawSpanPlan := b.rawSpanNativeBatchPlan()
	rootRuntime, builder, err := b.db.acquireRootPublicationBuilderForRuntimeV1()
	if err != nil {
		return err
	}
	if builder != nil {
		defer func() { builder.Release() }()
	}

	durablePublishLocked := false
	releaseDurablePublish := func() {
		if durablePublishLocked {
			b.db.durablePublishMu.Unlock()
			durablePublishLocked = false
		}
	}
	defer releaseDurablePublish()
	commitWaitStart := time.Now()
	b.db.writeMu.Lock()
	b.db.observeFlushApplyCommitWait(time.Since(commitWaitStart))
	rootLocksReleased := false
	defer func() {
		if !rootLocksReleased {
			b.db.writeMu.Unlock()
		}
	}()
	if err := b.db.checkWriteAdmissionLocked(); err != nil {
		return err
	}
	if b.db.rootPublication != rootRuntime {
		builder.Release()
		rootRuntime, builder, err = b.db.acquireRootPublicationBuilderForRuntimeV1()
		if err != nil {
			return err
		}
	}
	// Serialized fallback must join durable publication before it snapshots its
	// base generation. A preceding publisher may have released writeMu while it
	// is still installing visible state; building first would create a stale COW
	// root that cannot be rolled back through this direct allocator path.
	b.db.durablePublishMu.Lock()
	durablePublishLocked = true

	idx := b.db.idx.Load()
	if idx == nil {
		return fmt.Errorf("missing index")
	}

	b.db.rootReuseMu.RLock()
	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()
	b.db.rootReuseMu.RUnlock()

	defer idx.registry.Unregister(regID)
	if conditional != nil {
		if err := conditional.validateReadSetAtPublish(); err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
			return err
		}
	}

	applyOpts := b.flushApplyOptions()
	applyOpts.CollectOldPointerRefs = b.db.shouldCollectValueLogRefDelta(baseSeq)
	prepareBuf := b.db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	var newRoot uint64
	var retired []uint64
	var metrics adaptive.Metrics
	var applyResult zipper.ApplyResult
	var spanNativePublishSnapshot flushApplySpanNativePublishSnapshot
	applyWithOptions := flushApplyUseOptions(applyOpts)
	if applyWithOptions {
		result, applyErr := idx.zipper.ApplyWithOptions(rootID, b.batch, applyOpts)
		applyResult = result
		spanNativePublishSnapshot = newFlushApplySpanNativePublishSnapshot(result)
		b.db.observeFlushApplyPrepareResult(result, applyErr)
		b.db.observeRawSpanNativeApplyResult(rawSpanPlan, result, applyErr, applyWithOptions, applyOpts.SpanNativeApply)
		b.db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &result)
		newRoot = result.RootID
		retired = result.PendingRetiredPages
		metrics = result.Metrics
		err = applyErr
	} else {
		newRoot, retired, metrics, err = idx.zipper.Apply(rootID, b.batch)
		b.db.observeRawSpanNativeApplyResult(rawSpanPlan, applyResult, err, applyWithOptions, applyOpts.SpanNativeApply)
	}
	b.db.observeFlushApplyMetrics(metrics, time.Duration(metrics.ZipperApplyWallNs), err)
	b.db.observeFlushApplyPreparedOutput(metrics, len(retired))
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		return err
	}
	entries, ranges := b.batch.ApplyPlan()
	recordVacuumMutation := func() {
		b.db.vacuum.RecordApplyPlan(entries, ranges)
	}
	conditionalMutation := conditionalCommitMutation{
		entries: entries, ranges: ranges, ownerTxnID: b.conditionalTxnID,
	}
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries, ranges, &applyResult.OldPointerRefs, applyResult.OldEntriesRemoved, applyResult.OldPointerRefsCollected)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		return err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	b.db.mu.Lock()
	if b.db.meta.UserRootPageID != rootID {
		// This should not happen if writeMu is held and we are the only writer.
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeFlushApplyMismatch()
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackRootMismatch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		b.db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during batch write")
	}
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.Unlock()

	b.db.writeMu.Unlock()
	rootLocksReleased = true
	publishPrepareGuard, err := b.db.prepareFlushApplyPublish(sync)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		return err
	}
	defer publishPrepareGuard.Release()
	releaseRootSerialization := func() {}
	guardedPublishStart := time.Now()
	var post finalizeCommitPost
	if intent == nil {
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, finalizeCommitOptions{skipPrePublishFlush: true, skipConditionalRootConflict: true, maxEntryRevision: maxEntryRevision, durablePublishLocked: true, durablePublishRelease: releaseDurablePublish, rootPublicationBuilder: builder, closeTeardownPinned: true, expectedBaseCommitSeq: baseSeq, hasExpectedBaseCommitSeq: true, releaseRootSerialization: releaseRootSerialization, recordVacuumMutation: recordVacuumMutation, conditionalMutation: conditionalMutation})
	} else {
		// writeMu is released by the deferred unlock above even if the command
		// journal append fails and poisons this open handle.
		if _, err = b.db.appendRawKVCommandWALIntent(intent, sync); err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
			b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
			b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
			b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), false)
			return err
		}
		opts := commandWALFinalizeOptions(intent)
		opts.skipPrePublishFlush = true
		opts.skipConditionalRootConflict = true
		opts.maxEntryRevision = maxEntryRevision
		opts.durablePublishLocked = true
		opts.durablePublishRelease = releaseDurablePublish
		opts.rootPublicationBuilder = builder
		opts.closeTeardownPinned = true
		opts.expectedBaseCommitSeq = baseSeq
		opts.hasExpectedBaseCommitSeq = true
		opts.releaseRootSerialization = releaseRootSerialization
		opts.recordVacuumMutation = recordVacuumMutation
		opts.conditionalMutation = conditionalMutation
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, opts)
	}
	b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), err == nil)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
		b.db.observeRawBatchSpanNativePublishFallback(rawSpanPlan, spanNativePublishSnapshot, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		if intent != nil && !post.accepted {
			b.db.poisonCommandWALAfterPostAppendFailure(intent)
		}
		return err
	}
	b.db.observeFlushApplyInstalledOutput(metrics, len(retired))
	vlogRefDelta = nil
	b.db.finalizeCommitPostWork(post)
	b.db.releasePendingValueLogAppendFileIDsFromBatch(b.batch)
	b.db.clearLeafGenerationReachabilityCaches()
	return nil
}

func (b *Batch) writeConditional(sync bool, conditional *ConditionalTxn) error {
	if b == nil || b.db == nil {
		return fmt.Errorf("missing db")
	}
	if b.db.readOnly {
		return ErrReadOnly
	}
	b.db.teardownMu.RLock()
	defer b.db.teardownMu.RUnlock()
	if b.db.closing.Load() {
		return ErrClosed
	}
	maxEntryRevision := b.db.assignBatchEntryRevisions(b.batch)
	intent := b.commandWALPublishIntent
	var err error
	if !b.physicalOnly && b.db.commandWAL {
		unlockRawPublish, err := b.db.lockCommandWALPublishWithBarriersTeardownPinned()
		if err != nil {
			return err
		}
		defer unlockRawPublish()
		intent, err = b.db.prepareRawKVCommandWALIntent(b, sync)
		if err != nil {
			return err
		}
		defer releaseUnassignedCommandWALIntent(intent)
	}
	err = b.writeWithCommandWALIntentPreflight(sync, intent, maxEntryRevision, conditional)
	if err == nil && intent != nil && intent.lsn != 0 {
		b.db.conditionalTxnCommandWALPayloads.Add(1)
	}
	return err
}

func (b *Batch) Close() error {
	if b.batch != nil {
		err := b.batch.Close()
		b.batch = nil
		b.commandWALPublishIntent = nil
		b.conditionalTxnID = 0
		b.flushApplySpanNativeFallbackReason = FlushSpanRunFallbackUnknown
		b.rootPublicationBuildGroup = nil
		b.rootPublicationBuildGroupFinal = false
		return err
	}
	b.batch = nil
	b.commandWALPublishIntent = nil
	b.conditionalTxnID = 0
	b.flushApplySpanNativeFallbackReason = FlushSpanRunFallbackUnknown
	b.rootPublicationBuildGroup = nil
	b.rootPublicationBuildGroupFinal = false
	return nil
}

// Reset clears the batch for reuse.
func (b *Batch) Reset() {
	if b == nil || b.batch == nil {
		return
	}
	b.batch.Reset()
	b.commandWALPublishIntent = nil
	b.conditionalTxnID = 0
	b.flushApplySpanNativeFallbackReason = FlushSpanRunFallbackUnknown
	b.rootPublicationBuildGroup = nil
	b.rootPublicationBuildGroupFinal = false
}

func (b *Batch) Replay(fn func(batch.Entry) error) error {
	if b == nil || b.batch == nil {
		return nil
	}
	entries := b.batch.OrderedEntries()
	if !b.batch.HasDeleteRanges() {
		entries = b.batch.SortedEntries()
	}
	for _, entry := range entries {
		if entry.IsPtr && entry.Value == nil {
			ptr := entry.ValuePtr
			if !page.IsValueLogFileID(ptr.FileID) {
				return fmt.Errorf("expected value-log pointer, got file=%d", ptr.FileID)
			}
			if b.db == nil || b.db.valueLogManager == nil {
				return fmt.Errorf("missing value log manager")
			}
			val, err := b.db.valueLogManager.Read(ptr)
			if err != nil {
				return err
			}
			entry.Value = val
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) GetByteSize() (int, error) {
	return b.batch.ByteSize(), nil
}
