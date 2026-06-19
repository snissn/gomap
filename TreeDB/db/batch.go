package db

import (
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
	flushApplySpanNativeFallbackReason FlushSpanRunFallbackReason
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

// SetView records a Put without copying key/value bytes. Callers must treat
// key/value as immutable until the batch is written or closed.
//
// This is intentionally not part of the public batch.Interface; it is a
// best-effort optimization used by higher-level layers (e.g. cached streaming).
func (b *Batch) SetView(key, value []byte) error {
	return b.batch.SetView(key, value)
}

func (b *Batch) Delete(key []byte) error {
	return b.batch.Delete(key)
}

func (b *Batch) DeleteRange(start, end []byte) error {
	return b.batch.DeleteRange(start, end)
}

// DeleteView records a Delete without copying the key bytes. Callers must treat
// key as immutable until the batch is written or closed.
func (b *Batch) DeleteView(key []byte) error {
	return b.batch.DeleteView(key)
}

// SetPointer records a pointer without copying the value bytes.
func (b *Batch) SetPointer(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointer(key, ptr)
}

// SetPointerView records a pointer without copying the key bytes.
func (b *Batch) SetPointerView(key []byte, ptr page.ValuePtr) error {
	return b.batch.SetPointerView(key, ptr)
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
	if b.db.closing.Load() {
		return ErrClosed
	}
	if sync && b.batch != nil && b.batch.Len() == 0 && b.commandWALPublishIntent == nil {
		return b.db.Checkpoint()
	}
	intent := b.commandWALPublishIntent
	if !b.physicalOnly && b.db.commandWAL {
		unlockRawPublish := b.db.lockCommandWALRawPublish()
		defer unlockRawPublish()
		if err := b.db.runCommandWALRawPublishBarriers(); err != nil {
			return err
		}
		var err error
		intent, err = b.db.prepareRawKVCommandWALIntent(b)
		if err != nil {
			return err
		}
	}
	return b.writeWithCommandWALIntent(sync, intent)
}

func (b *Batch) writeWithCommandWALIntent(sync bool, intent *commandWALBatchIntent) error {
	// If a command frame is appended but root publication later returns an
	// error, the durable frame is intentionally left for reopen recovery. Reuse
	// the same intent across optimistic/serialized attempts so one user batch
	// keeps one command LSN.
	for attempt := 0; attempt < optimisticWriteMaxAttempts; attempt++ {
		committed, err := b.writeOptimistic(sync, intent)
		if err != nil {
			return err
		}
		if committed {
			return nil
		}
		b.db.observeFlushApplyRetry()
	}
	return b.writeSerialized(sync, intent)
}

func (b *Batch) writeOptimistic(sync bool, intent *commandWALBatchIntent) (bool, error) {
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()

	b.db.writeMu.RLock()
	if b.db.closing.Load() {
		b.db.writeMu.RUnlock()
		return false, ErrClosed
	}
	idx := b.db.idx.Load()
	if idx == nil {
		b.db.writeMu.RUnlock()
		return false, fmt.Errorf("missing index")
	}

	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	// Register this writer as a "reader" of the base state to prevent the
	// pruner from reclaiming pages we are about to read during z.Apply.
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

	tracker := newAllocTracker(idx.allocator)
	z := idx.zipper.CloneWithAllocator(tracker)
	applyOpts := b.flushApplyOptions()
	prepareBuf := b.db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	var newRoot uint64
	var retired []uint64
	var metrics adaptive.Metrics
	var err error
	var applyResult zipper.ApplyResult
	if flushApplyUseOptions(applyOpts) {
		result, applyErr := z.ApplyWithOptions(rootID, b.batch, applyOpts)
		applyResult = result
		b.db.observeFlushApplyPrepareResult(result, applyErr)
		b.db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &result)
		newRoot = result.RootID
		retired = result.PendingRetiredPages
		metrics = result.Metrics
		err = applyErr
	} else {
		newRoot, retired, metrics, err = z.Apply(rootID, b.batch)
	}
	b.db.observeFlushApplyMetrics(metrics, time.Duration(metrics.ZipperApplyWallNs), err)
	b.db.observeFlushApplyPreparedOutput(metrics, len(retired))
	if err != nil {
		abandonedEntries, _ := b.batch.ApplyPlan()
		b.db.releasePendingValueLogAppendFileIDsFromEntries(abandonedEntries)
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
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries, ranges)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
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
	b.db.mu.RUnlock()
	if currentRoot != rootID {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplyMismatch()
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackRootMismatch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		freeErr := tracker.FreeAll()
		b.db.writeMu.RUnlock()
		if freeErr != nil {
			return false, freeErr
		}
		return false, nil
	}
	publishPrepareGuard, err := b.db.prepareFlushApplyPublish(sync)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
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
		publishPrepareGuard, err = b.db.prepareFlushApplyPublish(sync)
		if err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
			b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
			b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
			freeErr := tracker.FreeAll()
			b.db.writeMu.RUnlock()
			if freeErr != nil {
				return false, freeErr
			}
			return false, err
		}
	}
	commitWaitStart := time.Now()
	b.db.commitMu.Lock()
	b.db.observeFlushApplyCommitWait(time.Since(commitWaitStart))
	guardedPublishStart := time.Now()
	b.db.mu.RLock()
	currentRoot = b.db.meta.UserRootPageID
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.RUnlock()
	if currentRoot != rootID {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplyMismatch()
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackRootMismatch)
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

	var post finalizeCommitPost
	if intent == nil {
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, finalizeCommitOptions{skipPrePublishFlush: true})
	} else {
		if _, err = b.db.appendRawKVCommandWALIntent(intent, sync); err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
			b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
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
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, opts)
		// Poison while still holding commitMu so that no concurrent writer can
		// slip past the poison check in appendRawKVCommandWALIntent and publish a
		// root that covers the unapplied frame's LSN.
		if err != nil {
			b.db.poisonCommandWALAfterPostAppendFailure(intent)
		}
	}
	b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), err == nil)
	b.db.commitMu.Unlock()
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		b.db.writeMu.RUnlock()
		return false, err
	}
	b.db.observeFlushApplyInstalledOutput(metrics, len(retired))
	vlogRefDelta = nil
	b.db.invalidateLeafGenerationSubtreeStats(tracker.Pages())
	b.db.finalizeCommitPostWork(post)
	b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
	if b.db.vacuum.Active() {
		b.db.vacuum.RecordApplyPlan(entries, ranges)
	}
	b.db.writeMu.RUnlock()
	return true, nil
}

func (b *Batch) writeSerialized(sync bool, intent *commandWALBatchIntent) error {
	touchedValueLogSegments := b.batch.TouchedValueLogSegments()

	commitWaitStart := time.Now()
	b.db.writeMu.Lock()
	b.db.observeFlushApplyCommitWait(time.Since(commitWaitStart))
	defer b.db.writeMu.Unlock()
	if b.db.closing.Load() {
		return ErrClosed
	}

	idx := b.db.idx.Load()
	if idx == nil {
		return fmt.Errorf("missing index")
	}

	b.db.mu.RLock()
	rootID := b.db.meta.UserRootPageID
	baseSeq := b.db.meta.CommitSeq
	regID := idx.registry.Register(baseSeq)
	b.db.mu.RUnlock()

	defer idx.registry.Unregister(regID)

	applyOpts := b.flushApplyOptions()
	prepareBuf := b.db.acquireFlushApplyReadOnlyPrepareBuffer(applyOpts)
	if prepareBuf != nil {
		applyOpts.ReadOnlyPrepare = prepareBuf.opts
	}
	var newRoot uint64
	var retired []uint64
	var metrics adaptive.Metrics
	var err error
	var applyResult zipper.ApplyResult
	if flushApplyUseOptions(applyOpts) {
		result, applyErr := idx.zipper.ApplyWithOptions(rootID, b.batch, applyOpts)
		applyResult = result
		b.db.observeFlushApplyPrepareResult(result, applyErr)
		b.db.releaseFlushApplyReadOnlyPrepareBuffer(prepareBuf, &result)
		newRoot = result.RootID
		retired = result.PendingRetiredPages
		metrics = result.Metrics
		err = applyErr
	} else {
		newRoot, retired, metrics, err = idx.zipper.Apply(rootID, b.batch)
	}
	b.db.observeFlushApplyMetrics(metrics, time.Duration(metrics.ZipperApplyWallNs), err)
	b.db.observeFlushApplyPreparedOutput(metrics, len(retired))
	if err != nil {
		abandonedEntries, _ := b.batch.ApplyPlan()
		b.db.releasePendingValueLogAppendFileIDsFromEntries(abandonedEntries)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		return err
	}
	entries, ranges := b.batch.ApplyPlan()
	vlogRefDelta, err := b.db.buildValueLogRefDelta(idx.pager, rootID, baseSeq, entries, ranges)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
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
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplyMismatch()
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackRootMismatch)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		b.db.mu.Unlock()
		return fmt.Errorf("concurrent modification detected during batch write")
	}
	sysRoot := b.db.meta.SystemRootPageID
	b.db.mu.Unlock()

	publishPrepareGuard, err := b.db.prepareFlushApplyPublish(sync)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		return err
	}
	defer publishPrepareGuard.Release()
	guardedPublishStart := time.Now()
	var post finalizeCommitPost
	if intent == nil {
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, finalizeCommitOptions{skipPrePublishFlush: true})
	} else {
		// writeMu is released by the deferred unlock above even if the command
		// journal append fails and poisons this open handle.
		if _, err = b.db.appendRawKVCommandWALIntent(intent, sync); err != nil {
			b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
			b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
			b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
			b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), false)
			return err
		}
		opts := commandWALFinalizeOptions(intent)
		opts.skipPrePublishFlush = true
		post, err = b.db.finalizeCommitLockedWithOptions(newRoot, sysRoot, retired, sync, metrics, touchedValueLogSegments, b.db.indexOuterLeavesInValueLog, vlogRefDelta, nil, nil, opts)
	}
	b.db.observeFlushApplyGuardedPublish(time.Since(guardedPublishStart), err == nil)
	if err != nil {
		b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
		b.db.observeFlushApplySpanNativePublishFallback(applyResult, FlushSpanRunFallbackOutputOwnershipFailure)
		b.db.observeFlushApplyAbandonedOutput(metrics, len(retired))
		if intent != nil {
			b.db.poisonCommandWALAfterPostAppendFailure(intent)
		}
		return err
	}
	b.db.observeFlushApplyInstalledOutput(metrics, len(retired))
	vlogRefDelta = nil
	b.db.finalizeCommitPostWork(post)
	b.db.releasePendingValueLogAppendFileIDsFromEntries(entries)
	b.db.clearLeafGenerationReachabilityCaches()
	if b.db.vacuum.Active() {
		b.db.vacuum.RecordApplyPlan(entries, ranges)
	}
	return nil
}

func (b *Batch) Close() error {
	if b.batch != nil {
		err := b.batch.Close()
		b.batch = nil
		b.commandWALPublishIntent = nil
		b.flushApplySpanNativeFallbackReason = FlushSpanRunFallbackUnknown
		return err
	}
	b.batch = nil
	b.commandWALPublishIntent = nil
	b.flushApplySpanNativeFallbackReason = FlushSpanRunFallbackUnknown
	return nil
}

// Reset clears the batch for reuse.
func (b *Batch) Reset() {
	if b == nil || b.batch == nil {
		return
	}
	b.batch.Reset()
	b.commandWALPublishIntent = nil
	b.flushApplySpanNativeFallbackReason = FlushSpanRunFallbackUnknown
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
