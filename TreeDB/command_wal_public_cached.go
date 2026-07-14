package treedb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

func (tdb *DB) appendPublicRawKVPointCommand(op commitlog.RawKVOp, key, value []byte, revision EntryRevision, sync bool) error {
	if tdb == nil || !tdb.commandWALCached {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	lsn, err := tdb.backend.AppendRawKVPointCommandWALTrustedWithRevision(op, key, value, page.EntryRevision(revision), sync)
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	if err == nil && testAfterPublicCommandWALPointAppend != nil {
		testAfterPublicCommandWALPointAppend(commitlog.RawKVOperation{Op: op, Key: key, Value: value, Revision: uint64(revision)})
	}
	return err
}

func (tdb *DB) appendPublicRawKVCommandPayload(payload []byte, sync bool) error {
	if tdb == nil || !tdb.commandWALCached || len(payload) == 0 {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	lsn, err := tdb.backend.AppendRawKVBatchPayloadCommandWALTrusted(payload, sync)
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	return err
}

func (tdb *DB) appendPublicRawKVCommandPayloadMeasured(payload []byte, sync bool) (db.CommandWALRequestTiming, error) {
	var timing db.CommandWALRequestTiming
	if tdb == nil || !tdb.commandWALCached || len(payload) == 0 {
		return timing, nil
	}
	if tdb.backend == nil {
		return timing, ErrClosed
	}
	lsn, timing, err := tdb.backend.AppendRawKVBatchPayloadCommandWALTrustedMeasured(payload, sync)
	postAppendStart := time.Now()
	timing.PostAppendPendingLSNBookkeepingObserved = true
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	timing.PostAppendPendingLSNBookkeeping += time.Since(postAppendStart)
	return timing, err
}

func (tdb *DB) appendPublicRawKVCommandEntries(entries []batch.Entry, sync bool) error {
	if tdb == nil || !tdb.commandWALCached || len(entries) == 0 {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	lsn, err := tdb.backend.AppendRawKVCommandWALOrderedEntries(entries, sync)
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	return err
}

func (tdb *DB) appendPublicRawKVCommandEntryScan(scanEntries func(func(batch.Entry) error) error, opHint int, sync bool) error {
	if tdb == nil || !tdb.commandWALCached || scanEntries == nil {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	lsn, err := tdb.backend.AppendRawKVCommandWALOrderedEntryScanWithHint(scanEntries, opHint, sync)
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	return err
}

func (tdb *DB) appendPublicRawKVCommandEntryScanMeasured(scanEntries func(func(batch.Entry) error) error, opHint int, sync bool) (db.CommandWALRequestTiming, error) {
	var timing db.CommandWALRequestTiming
	if tdb == nil || !tdb.commandWALCached || scanEntries == nil {
		return timing, nil
	}
	if tdb.backend == nil {
		return timing, ErrClosed
	}
	lsn, timing, err := tdb.backend.AppendRawKVCommandWALOrderedEntryScanWithHintMeasured(scanEntries, opHint, sync)
	postAppendStart := time.Now()
	timing.PostAppendPendingLSNBookkeepingObserved = true
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	timing.PostAppendPendingLSNBookkeeping += time.Since(postAppendStart)
	return timing, err
}

func (tdb *DB) commandWALPayloadShouldBypassForValue(key, value []byte) bool {
	return tdb != nil && tdb.cached != nil && tdb.cached.CommandWALPayloadShouldBypassForValue(key, value)
}

func (tdb *DB) appendPublicRawKVDeleteRangeCommand(start, end []byte, sync bool) error {
	if tdb == nil || !tdb.commandWALCached || batch.IsDeleteRangeNoop(start, end) {
		return nil
	}
	return tdb.appendPublicRawKVCommandEntries([]batch.Entry{{
		Type:  batch.OpDeleteRange,
		Key:   start,
		Value: end,
	}}, sync)
}

func (tdb *DB) recordPublicCommandWALPendingLSN(lsn uint64) {
	if tdb == nil || lsn == 0 {
		return
	}
	tdb.commandWALPendingMu.Lock()
	defer tdb.commandWALPendingMu.Unlock()
	tdb.commandWALLiveFrames.Add(1)
	first := tdb.commandWALFirst.Load()
	if first == 0 || lsn < first {
		tdb.commandWALFirst.Store(lsn)
	}
	if last := tdb.commandWALLast.Load(); lsn > last {
		tdb.commandWALLast.Store(lsn)
	}
}

func (tdb *DB) publicCommandWALLiveStatsInto(stats map[string]string) {
	if tdb == nil || stats == nil || !tdb.commandWALCached {
		return
	}
	frames := tdb.commandWALLiveFrames.Load()
	maxLSN := tdb.commandWALLast.Load()
	acceptedMaxLSN := maxLSN
	if current, err := strconv.ParseUint(stats["treedb.command_wal.live_accepted_max_lsn"], 10, 64); err == nil && current > acceptedMaxLSN {
		acceptedMaxLSN = current
	}
	stats["treedb.command_wal.live_frames"] = fmt.Sprintf("%d", frames)
	stats["treedb.command_wal.live_max_lsn"] = fmt.Sprintf("%d", maxLSN)
	publicCommandWALMaxStat(stats, "treedb.command_wal.live_accepted_frames", frames)
	publicCommandWALMaxStat(stats, "treedb.command_wal.live_accepted_max_lsn", maxLSN)
	publicCommandWALMaxStat(stats, "treedb.command_wal.frames", frames)
	publicCommandWALMaxStat(stats, "treedb.command_wal.max_lsn", acceptedMaxLSN)
}

func (tdb *DB) publicCommandWALBatchStatsInto(stats map[string]string) {
	if tdb == nil || stats == nil || !tdb.commandWALCached {
		return
	}
	stats["treedb.command_wal.public_batch.set.calls_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchSetCalls.Load())
	stats["treedb.command_wal.public_batch.set.bytes_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchSetBytes.Load())
	stats["treedb.command_wal.public_batch.set_view.calls_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchSetViewCalls.Load())
	stats["treedb.command_wal.public_batch.set_view.bytes_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchSetViewBytes.Load())
	stats["treedb.command_wal.public_batch.delete.calls_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchDeleteCalls.Load())
	stats["treedb.command_wal.public_batch.delete.bytes_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchDeleteBytes.Load())
	stats["treedb.command_wal.public_batch.delete_view.calls_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchDeleteViewCalls.Load())
	stats["treedb.command_wal.public_batch.delete_view.bytes_total"] = fmt.Sprintf("%d", tdb.commandWALPublicBatchDeleteViewBytes.Load())
}

func publicCommandWALMaxStat(stats map[string]string, key string, value uint64) {
	if value == 0 {
		return
	}
	current, err := strconv.ParseUint(stats[key], 10, 64)
	if err == nil && current >= value {
		return
	}
	stats[key] = fmt.Sprintf("%d", value)
}

func (tdb *DB) publicCommandWALPendingRange() (first, last uint64) {
	if tdb == nil {
		return 0, 0
	}
	tdb.commandWALPendingMu.Lock()
	defer tdb.commandWALPendingMu.Unlock()
	return tdb.commandWALFirst.Load(), tdb.commandWALLast.Load()
}

func (tdb *DB) clearPublicCommandWALPendingThrough(lsn uint64) {
	if tdb == nil || lsn == 0 {
		return
	}
	tdb.commandWALPendingMu.Lock()
	defer tdb.commandWALPendingMu.Unlock()
	first := tdb.commandWALFirst.Load()
	last := tdb.commandWALLast.Load()
	if first == 0 || last == 0 {
		return
	}
	if last <= lsn {
		tdb.commandWALFirst.Store(0)
		tdb.commandWALLast.Store(0)
		return
	}
	if first <= lsn {
		tdb.commandWALFirst.Store(lsn + 1)
	}
}

func (tdb *DB) clearPublishedPublicCommandWALPending() {
	if tdb == nil || !tdb.commandWALCached || tdb.backend == nil {
		return
	}
	state, ok := tdb.backend.StateToken()
	if !ok {
		return
	}
	tdb.clearPublicCommandWALPendingThrough(state.AppliedCommandLSN)
}

func (tdb *DB) snapshotPublicCommandWALCheckpointCutover() {
	if tdb == nil || !tdb.commandWALCached {
		return
	}
	tdb.commandWALPendingMu.Lock()
	defer tdb.commandWALPendingMu.Unlock()
	last := tdb.commandWALLast.Load()
	tdb.commandWALCheckpointCutoverLast.Store(last)
	tdb.commandWALCheckpointCutoverErr = nil
	if last == 0 {
		return
	}
	if tdb.backend == nil {
		tdb.commandWALCheckpointCutoverErr = ErrClosed
		return
	}
	if tdb.backend.CommandWALActiveBytes() > 0 {
		tdb.commandWALCheckpointCutoverErr = tdb.backend.RotateCommandWALActiveSegment(publicCommandWALPublishSync(tdb.durabilityMode, true))
	}
}

func (tdb *DB) preparePublicCommandWALPendingPublish(sync bool) (uint64, []db.CommandWALLSNRange, error) {
	if tdb == nil || !tdb.commandWALCached {
		return 0, nil, nil
	}
	if tdb.backend == nil {
		return 0, nil, ErrClosed
	}
	first, last := tdb.publicCommandWALPendingRange()
	tdb.commandWALPendingMu.Lock()
	cutoverErr := tdb.commandWALCheckpointCutoverErr
	tdb.commandWALPendingMu.Unlock()
	if cutoverErr != nil {
		return 0, nil, cutoverErr
	}
	if first == 0 || last == 0 {
		return 0, nil, nil
	}
	cutoverLast := tdb.commandWALCheckpointCutoverLast.Load()
	if cutoverLast == 0 || first > cutoverLast {
		return 0, nil, nil
	}
	if last > cutoverLast {
		last = cutoverLast
	}
	state, ok := tdb.backend.StateToken()
	if !ok {
		return 0, nil, ErrClosed
	}
	current := state.AppliedCommandLSN
	if last <= current {
		tdb.clearPublicCommandWALPendingThrough(current)
		return 0, nil, nil
	}
	if first > current+1 {
		return 0, nil, fmt.Errorf("%w: pending public command WAL starts at %d after applied %d", db.ErrCommandWALAppliedLSNNonContig, first, current)
	}
	if sync {
		if err := tdb.backend.FlushCommandWAL(true); err != nil {
			return 0, nil, err
		}
	} else if err := tdb.backend.CheckCommandWALPublishReady(); err != nil {
		return 0, nil, err
	}
	return last, []db.CommandWALLSNRange{{First: current + 1, Last: last}}, nil
}

func (tdb *DB) publicCommandWALAutoCheckpointBytes() int64 {
	if tdb == nil || !tdb.commandWALCached || tdb.backend == nil {
		return 0
	}
	return tdb.backend.CommandWALBytes()
}

func (tdb *DB) cleanupPublicCommandWALCheckpoint(sync bool) error {
	if tdb == nil || !tdb.commandWALCached || tdb.backend == nil {
		return nil
	}
	return tdb.backend.CleanupCommandWALCoveredSegments(publicCommandWALPublishSync(tdb.durabilityMode, sync))
}

func publicCommandWALPublishSync(durabilityMode string, sync bool) bool {
	return sync
}

func (tdb *DB) syncPublicCommandWAL() error {
	if tdb == nil || !tdb.commandWALCached {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	return tdb.backend.FlushCommandWALBarrier(publicCommandWALPublishSync(tdb.durabilityMode, true))
}

type commandWALPublicBatch struct {
	db                       *DB
	inner                    Batch
	innerSetViewValidated    commandWALPublicInnerSetViewValidated
	innerSetViewer           commandWALPublicInnerSetViewer
	innerDeleteViewValidated commandWALPublicInnerDeleteViewValidated
	innerDeleteViewer        commandWALPublicInnerDeleteViewer
	payload                  commitlog.RawKVBatchPayloadBuilder
	payloadOpHint            int
	payloadByteHint          int
	payloadSoftCapBytes      int
	payloadBypass            bool
	payloadLeasedToMemtable  bool
	payloadLeasedBufferMask  commitlog.RawKVBatchPayloadBufferMask
	payloadLeaseAttachedMask commitlog.RawKVBatchPayloadBufferMask
	knownZeroValueRef        []byte
	knownZeroValueLen        int
	knownZeroValueValid      bool
	opCount                  int
	hasDeleteRange           bool
	dirty                    bool
	setCalls                 int
	setBytes                 int
	setViewCalls             int
	setViewBytes             int
	deleteCalls              int
	deleteBytes              int
	deleteViewCalls          int
	deleteViewBytes          int
	// retainPayloadAfterWrite is set by adapter-only replay-view helpers. It
	// keeps payload-owned key/value views valid after a successful Write until
	// Reset/Close or the next mutation. Ordinary public Batch callers do not use
	// those helpers and keep the previous reset-after-write behavior.
	retainPayloadAfterWrite bool
	closed                  bool
}

// Seed command payload capacity for moderately sized values. The op hint is
// bounded, so this avoids repeated grow/copy churn without unbounded retention.
const commandWALPublicBatchEstimatedKeyValueBytes = 192

// Keep the public command-WAL batch payload builder below the Celestia-observed
// large-batch peak while preserving the compact fast path for ordinary batches.
const commandWALPublicBatchPayloadSoftCapBytes = 128 << 20

// Pool only ordinary payload buffers. Reusing the public batch wrapper itself is
// unsafe because callers may still hold a closed batch value.
const commandWALPublicBatchPayloadPoolRetainMaxBytes = 1 << 20

const commandWALRawKVBatchHeaderSize = 2 + 4

type commandWALPublicRevisionEntries interface {
	PointRevisionStats() (page.EntryRevision, bool)
	OrderedEntries() []batch.Entry
}

type commandWALPublicInnerSetViewValidated interface {
	SetViewValidated(key, value []byte) error
}

type commandWALPublicInnerSetViewer interface {
	SetView(key, value []byte) error
}

type commandWALPublicInnerDeleteViewValidated interface {
	DeleteViewValidated(key []byte) error
}

type commandWALPublicInnerDeleteViewer interface {
	DeleteView(key []byte) error
}

type commandWALPublicInnerStableViewValueLease interface {
	AttachStableViewValueLease(chunks [][]byte)
	StableViewValueLeaseConsumed() bool
}

func newCommandWALPublicBatch(tdb *DB, inner Batch, opHint int) *commandWALPublicBatch {
	var payload commitlog.RawKVBatchPayloadBuilder
	if tdb != nil {
		if pooled := tdb.commandWALPublicPayloadPool.Get(); pooled != nil {
			if p, ok := pooled.(commitlog.RawKVBatchPayloadBuilder); ok {
				payload = p
			}
		}
	}
	b := &commandWALPublicBatch{db: tdb, inner: inner, payload: payload}
	b.disableInnerStreamingBypass()
	b.rebindInnerViewers()
	opHint = db.NormalizePublicBatchReserveHint(opHint)
	byteHint := 0
	if opHint > 0 && opHint <= int(^uint(0)>>1)/commandWALPublicBatchEstimatedKeyValueBytes {
		byteHint = opHint * commandWALPublicBatchEstimatedKeyValueBytes
	}
	b.payloadOpHint = opHint
	b.payloadByteHint = byteHint
	b.payloadSoftCapBytes = commandWALPublicBatchPayloadSoftCapBytes
	b.resetPayloadWithHint()
	return b
}

func (b *commandWALPublicBatch) resetPayloadWithHint() {
	if b == nil {
		return
	}
	_ = b.payload.ResetWithHint(b.payloadOpHint, b.payloadByteHint)
	if b.payloadShouldCarryEntryRevisions() {
		_ = b.payload.EnableEntryRevisions()
	}
	b.payloadBypass = false
	b.hasDeleteRange = false
	b.knownZeroValueRef = nil
	b.knownZeroValueLen = 0
	b.knownZeroValueValid = false
}

func (b *commandWALPublicBatch) resetPayloadForReuse() {
	if b == nil {
		return
	}
	if b.payloadLeasedToMemtable {
		b.payload.DetachRetainedValueByteBuffers(b.payloadLeasedBufferMask)
		b.payloadLeasedToMemtable = false
		b.payloadLeasedBufferMask = 0
	}
	b.payloadLeaseAttachedMask = 0
	b.resetPayloadWithHint()
}

func (b *commandWALPublicBatch) payloadShouldCarryEntryRevisions() bool {
	if b == nil || b.db == nil || b.inner == nil {
		return false
	}
	_, ok := b.inner.(commandWALPublicRevisionEntries)
	return ok
}

func (b *commandWALPublicBatch) preparePayloadForAppend() {
	if b == nil || !b.retainPayloadAfterWrite || b.dirty {
		return
	}
	// A previous adapter replay-view Write kept payload bytes alive for external
	// Replay/rebuild semantics. If the inner batch is reused directly without an
	// explicit Reset, drop those retained bytes before appending a fresh command
	// payload. The geth adapter detaches borrowed replay views before taking this
	// path after Write.
	b.resetPayloadForReuse()
	b.opCount = 0
	b.hasDeleteRange = false
	b.retainPayloadAfterWrite = false
}

func (b *commandWALPublicBatch) attachStableViewValueLease() bool {
	if b == nil || b.inner == nil || b.payloadBypass || b.payload.Count() == 0 || b.payload.Count() != b.opCount {
		return false
	}
	leaser, ok := b.inner.(commandWALPublicInnerStableViewValueLease)
	if !ok {
		return false
	}
	if b.setCalls+b.setViewCalls == 0 {
		return false
	}
	chunks, mask := b.payload.RetainedValueByteBuffers()
	if len(chunks) == 0 {
		return false
	}
	leaser.AttachStableViewValueLease(chunks)
	b.payloadLeaseAttachedMask = mask
	return true
}

func (b *commandWALPublicBatch) noteStableViewValueLeaseConsumed() {
	if b == nil || b.inner == nil {
		return
	}
	leaser, ok := b.inner.(commandWALPublicInnerStableViewValueLease)
	if ok && leaser.StableViewValueLeaseConsumed() && b.payloadLeaseAttachedMask != 0 {
		b.payloadLeasedToMemtable = true
		b.payloadLeasedBufferMask |= b.payloadLeaseAttachedMask
	}
}

func (b *commandWALPublicBatch) rebindInnerViewers() {
	if b == nil {
		return
	}
	b.innerSetViewValidated = nil
	b.innerSetViewer = nil
	b.innerDeleteViewValidated = nil
	b.innerDeleteViewer = nil
	if b.inner == nil {
		return
	}
	if setter, ok := b.inner.(commandWALPublicInnerSetViewValidated); ok {
		b.innerSetViewValidated = setter
	} else if setter, ok := b.inner.(commandWALPublicInnerSetViewer); ok {
		b.innerSetViewer = setter
	}
	if deleter, ok := b.inner.(commandWALPublicInnerDeleteViewValidated); ok {
		b.innerDeleteViewValidated = deleter
	} else if deleter, ok := b.inner.(commandWALPublicInnerDeleteViewer); ok {
		b.innerDeleteViewer = deleter
	}
}

func (b *commandWALPublicBatch) disableInnerStreamingBypass() {
	if b == nil || b.inner == nil {
		return
	}
	if disabler, ok := b.inner.(interface{ DisableStreamingBypass() }); ok {
		disabler.DisableStreamingBypass()
	}
}

func (b *commandWALPublicBatch) Set(key, value []byte) error {
	_, _, err := b.setView(key, value, false, false)
	return err
}

func (b *commandWALPublicBatch) SetView(key, value []byte) error {
	_, _, err := b.setView(key, value, false, true)
	return err
}

// SetViewWithReplayBytes records a Put and returns payload-owned immutable
// key/value views for higher-level adapters that must preserve replay order
// without making their own hot-path byte clone. The returned views remain valid
// until this batch is Reset or Closed, or until a subsequent mutation after
// Write reuses the payload. If the adapter needs to rebuild/reuse the batch
// after Write, it must copy the views before causing Reset or append reuse on
// the inner batch.
//
// This method is intentionally not part of the public Batch interface.
func (b *commandWALPublicBatch) SetViewWithReplayBytes(key, value []byte) (keyView, valueView []byte, err error) {
	return b.setView(key, value, true, true)
}

func (b *commandWALPublicBatch) setView(key, value []byte, retainReplayViews, useInnerView bool) (keyView, valueView []byte, err error) {
	if b == nil || b.inner == nil {
		return nil, nil, ErrClosed
	}
	b.preparePayloadForAppend()
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	if !retainReplayViews && useInnerView {
		err = b.innerSetView(key, value)
		if err != nil {
			return nil, nil, err
		}
		b.payloadBypass = true
		b.opCount++
		b.recordPointIngress(commitlog.RawKVOpSet, len(key)+len(value), true)
		b.dirty = true
		return nil, nil, nil
	}
	if b.shouldBypassPayloadAppendSet(key, value, retainReplayViews) {
		if useInnerView {
			err = b.innerSetView(key, value)
		} else {
			err = b.inner.Set(key, value)
		}
		if err != nil {
			return nil, nil, err
		}
		b.payloadBypass = true
		b.opCount++
		b.recordPointIngress(commitlog.RawKVOpSet, len(key)+len(value), useInnerView)
		b.dirty = true
		return nil, nil, nil
	}
	if !b.payload.EntryRevisionsEnabled() && commandWALPublicAllZeroBytes(value) {
		if !useInnerView {
			// AppendSet compact-zero tracking keys off the first zero value
			// backing array. Plain Set callers may reuse and mutate their input
			// buffer, so pin compact-zero identity to an immutable batch-owned copy.
			value = append([]byte(nil), value...)
		} else if retainReplayViews {
			// Adapter replay-byte callers may also reuse and mutate their value
			// buffer between Put calls, so keep that compact-zero identity
			// immutable.
			value = append([]byte(nil), value...)
		}
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	if b.payload.EntryRevisionsEnabled() && b.knownZeroValue(value) {
		keyView, valueView, err = b.payload.AppendSetKnownZeroValue(key, value)
	} else {
		keyView, valueView, err = b.payload.AppendSet(key, value)
	}
	if err != nil {
		return nil, nil, err
	}
	if err := b.innerSetView(keyView, valueView); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return nil, nil, err
	}
	b.opCount++
	b.recordPointIngress(commitlog.RawKVOpSet, len(key)+len(value), useInnerView)
	b.dirty = true
	if retainReplayViews {
		b.retainPayloadAfterWrite = true
	}
	return keyView, valueView, nil
}

func (b *commandWALPublicBatch) Delete(key []byte) error {
	_, err := b.deleteView(key, false, false)
	return err
}

func (b *commandWALPublicBatch) DeleteView(key []byte) error {
	_, err := b.deleteView(key, false, true)
	return err
}

// DeleteViewWithReplayBytes records a Delete and returns a payload-owned key
// view for adapter replay logs. See SetViewWithReplayBytes for lifetime rules.
//
// This method is intentionally not part of the public Batch interface.
func (b *commandWALPublicBatch) DeleteViewWithReplayBytes(key []byte) (keyView []byte, err error) {
	return b.deleteView(key, true, true)
}

func (b *commandWALPublicBatch) deleteView(key []byte, retainReplayViews, useInnerView bool) (keyView []byte, err error) {
	if b == nil || b.inner == nil {
		return nil, ErrClosed
	}
	b.preparePayloadForAppend()
	key = normalizeRawKVPointKey(key)
	if !retainReplayViews && useInnerView {
		err = b.innerDeleteView(key)
		if err != nil {
			return nil, err
		}
		b.payloadBypass = true
		b.opCount++
		b.recordPointIngress(commitlog.RawKVOpDelete, len(key), true)
		b.dirty = true
		return nil, nil
	}
	if b.shouldBypassPayloadAppendDelete(key, retainReplayViews) {
		if useInnerView {
			err = b.innerDeleteView(key)
		} else {
			err = b.inner.Delete(key)
		}
		if err != nil {
			return nil, err
		}
		b.payloadBypass = true
		b.opCount++
		b.recordPointIngress(commitlog.RawKVOpDelete, len(key), useInnerView)
		b.dirty = true
		return nil, nil
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	keyView, err = b.payload.AppendDelete(key)
	if err != nil {
		return nil, err
	}
	if err := b.innerDeleteView(keyView); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return nil, err
	}
	b.opCount++
	b.recordPointIngress(commitlog.RawKVOpDelete, len(key), useInnerView)
	b.dirty = true
	if retainReplayViews {
		b.retainPayloadAfterWrite = true
	}
	return keyView, nil
}

func (b *commandWALPublicBatch) DeleteRange(start, end []byte) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if batch.IsDeleteRangeNoop(start, end) {
		return nil
	}
	b.preparePayloadForAppend()
	if err := b.inner.DeleteRange(start, end); err != nil {
		return err
	}
	b.payloadBypass = true
	b.opCount++
	b.hasDeleteRange = true
	b.dirty = true
	return nil
}

func (b *commandWALPublicBatch) recordPointIngress(op commitlog.RawKVOp, bytes int, view bool) {
	if b == nil {
		return
	}
	switch op {
	case commitlog.RawKVOpSet:
		if view {
			b.setViewCalls++
			b.setViewBytes += bytes
			return
		}
		b.setCalls++
		b.setBytes += bytes
	case commitlog.RawKVOpDelete:
		if view {
			b.deleteViewCalls++
			b.deleteViewBytes += bytes
			return
		}
		b.deleteCalls++
		b.deleteBytes += bytes
	}
}

func (b *commandWALPublicBatch) publishPointIngressStats() {
	if b == nil || b.db == nil {
		return
	}
	if b.setCalls != 0 {
		b.db.commandWALPublicBatchSetCalls.Add(uint64(b.setCalls))
	}
	if b.setBytes != 0 {
		b.db.commandWALPublicBatchSetBytes.Add(uint64(b.setBytes))
	}
	if b.setViewCalls != 0 {
		b.db.commandWALPublicBatchSetViewCalls.Add(uint64(b.setViewCalls))
	}
	if b.setViewBytes != 0 {
		b.db.commandWALPublicBatchSetViewBytes.Add(uint64(b.setViewBytes))
	}
	if b.deleteCalls != 0 {
		b.db.commandWALPublicBatchDeleteCalls.Add(uint64(b.deleteCalls))
	}
	if b.deleteBytes != 0 {
		b.db.commandWALPublicBatchDeleteBytes.Add(uint64(b.deleteBytes))
	}
	if b.deleteViewCalls != 0 {
		b.db.commandWALPublicBatchDeleteViewCalls.Add(uint64(b.deleteViewCalls))
	}
	if b.deleteViewBytes != 0 {
		b.db.commandWALPublicBatchDeleteViewBytes.Add(uint64(b.deleteViewBytes))
	}
}

func (b *commandWALPublicBatch) resetPointIngressStats() {
	if b == nil {
		return
	}
	b.setCalls = 0
	b.setBytes = 0
	b.setViewCalls = 0
	b.setViewBytes = 0
	b.deleteCalls = 0
	b.deleteBytes = 0
	b.deleteViewCalls = 0
	b.deleteViewBytes = 0
}

func (b *commandWALPublicBatch) innerSetView(key, value []byte) error {
	if b.innerSetViewValidated != nil {
		return b.innerSetViewValidated.SetViewValidated(key, value)
	}
	if b.innerSetViewer != nil {
		return b.innerSetViewer.SetView(key, value)
	}
	return b.inner.Set(key, value)
}

func (b *commandWALPublicBatch) innerDeleteView(key []byte) error {
	if b.innerDeleteViewValidated != nil {
		return b.innerDeleteViewValidated.DeleteViewValidated(key)
	}
	if b.innerDeleteViewer != nil {
		return b.innerDeleteViewer.DeleteView(key)
	}
	return b.inner.Delete(key)
}

func (b *commandWALPublicBatch) shouldBypassPayloadAppendSet(key, value []byte, retainReplayViews bool) bool {
	if !retainReplayViews && b != nil && b.db != nil && b.db.commandWALPayloadShouldBypassForValue(key, value) {
		return true
	}
	retainedCap, err := b.payload.RetainedCapAfterAppendSet(key, value)
	return b.shouldBypassPayloadAppendRetainedCap(retainedCap, err, retainReplayViews)
}

func (b *commandWALPublicBatch) shouldBypassPayloadAppendDelete(key []byte, retainReplayViews bool) bool {
	retainedCap, err := b.payload.RetainedCapAfterAppendDelete(key)
	return b.shouldBypassPayloadAppendRetainedCap(retainedCap, err, retainReplayViews)
}

func (b *commandWALPublicBatch) shouldBypassPayloadAppendDeleteRange(start, end []byte) bool {
	retainedCap, err := b.payload.RetainedCapAfterAppendDeleteRange(start, end)
	return b.shouldBypassPayloadAppendRetainedCap(retainedCap, err, false)
}

func (b *commandWALPublicBatch) shouldBypassPayloadAppendRetainedCap(retainedCap int, predictionErr error, retainReplayViews bool) bool {
	if b == nil || retainReplayViews || b.payloadSoftCapBytes <= 0 {
		return false
	}
	if b.payloadBypass {
		return true
	}
	if predictionErr != nil {
		return true
	}
	return retainedCap > b.payloadSoftCapBytes
}

func (b *commandWALPublicBatch) Write() error {
	return b.write(false)
}

func (b *commandWALPublicBatch) WriteSync() error {
	return b.write(true)
}

func (b *commandWALPublicBatch) write(sync bool) (err error) {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	start := time.Now()
	if b.db != nil {
		defer func() {
			b.db.observePublicBatchWrite(sync, start, err)
		}()
	}
	phaseEnabled := sync && b.db != nil && b.db.publicBatchWriteSyncPhaseEnabled
	var phaseSample publicBatchWriteSyncPhaseSample
	if phaseEnabled {
		defer func() {
			b.db.publicBatchWriteSyncPhase.observe(start, err, phaseSample)
		}()
	}
	if b.db != nil {
		if err := b.db.beginPublicOperation(); err != nil {
			return err
		}
		defer b.db.lifecycleMu.RUnlock()
	}
	if b.dirty {
		leaseAttached := false
		var writeErr error
		if phaseEnabled {
			writer, ok := b.inner.(interface {
				WriteAfterCommandWALAppendMeasured(sync bool, appendCommand func() error) (caching.CommandWALBatchWriteTiming, error)
			})
			if !ok {
				return fmt.Errorf("treedb: command wal batch requires measured cached command append hook")
			}
			leaseAttached = b.attachStableViewValueLease()
			var commandTiming db.CommandWALRequestTiming
			cachedTiming, measuredErr := writer.WriteAfterCommandWALAppendMeasured(sync, func() error {
				var appendErr error
				commandTiming, appendErr = b.appendCommandWALMeasured(sync)
				return appendErr
			})
			phaseSample.checkpointGate = cachedTiming.CheckpointGate
			phaseSample.preflightMaterialization = cachedTiming.PreflightMaterialization
			phaseSample.commandCallback = cachedTiming.CommandCallback
			phaseSample.memtablePublicationReset = cachedTiming.MemtablePublicationReset
			phaseSample.commandPublicPayloadEntryScanPreparation = commandTiming.PublicPayloadEntryScanPreparation
			phaseSample.commandPublicPreparationObserved = commandTiming.PublicPreparationObserved
			phaseSample.commandPublishLockBarrierWait = commandTiming.PublishLockBarrierWait
			phaseSample.commandPublishLockBarrierWaitObserved = commandTiming.PublishLockBarrierWaitObserved
			phaseSample.commandBackendIntentPlanningSerialization = commandTiming.BackendIntentPlanningSerialization
			phaseSample.commandBackendIntentPlanningObserved = commandTiming.BackendIntentPlanningSerializationObserved
			phaseSample.commandExternalRefOrdering = commandTiming.ExternalRefOrdering
			phaseSample.commandExternalRefOrderingObserved = commandTiming.ExternalRefOrderingObserved
			phaseSample.commandAppend = commandTiming.Append
			phaseSample.commandAppendObserved = commandTiming.AppendObserved
			if commandTiming.Sync {
				phaseSample.commandSync = commandTiming.Flush
				phaseSample.commandSyncObserved = commandTiming.FlushObserved
			} else {
				phaseSample.commandFlush = commandTiming.Flush
				phaseSample.commandFlushObserved = commandTiming.FlushObserved
			}
			phaseSample.commandPostAppendPendingLSNBookkeeping = commandTiming.PostAppendPendingLSNBookkeeping
			phaseSample.commandPostAppendPendingLSNBookkeepingObserved = commandTiming.PostAppendPendingLSNBookkeepingObserved
			writeErr = measuredErr
		} else {
			writer, ok := b.inner.(interface {
				WriteAfterCommandWALAppend(sync bool, appendCommand func() error) error
			})
			if !ok {
				return fmt.Errorf("treedb: command wal batch requires cached command append hook")
			}
			leaseAttached = b.attachStableViewValueLease()
			writeErr = writer.WriteAfterCommandWALAppend(sync, func() error {
				return b.appendCommandWAL(sync)
			})
		}
		if writeErr != nil {
			if leaseAttached {
				b.noteStableViewValueLeaseConsumed()
			}
			return writeErr
		}
		if leaseAttached {
			b.noteStableViewValueLeaseConsumed()
		}
		b.publishPointIngressStats()
		b.disableInnerStreamingBypass()
		if !b.retainPayloadAfterWrite {
			b.resetPayloadForReuse()
		}
		b.opCount = 0
		b.hasDeleteRange = false
		b.resetPointIngressStats()
		b.dirty = false
		return nil
	}
	if sync {
		if phaseEnabled {
			commandStart := time.Now()
			syncErr := b.db.syncPublicCommandWAL()
			phaseSample.commandCallback = time.Since(commandStart)
			phaseSample.commandEmptyBarrier = phaseSample.commandCallback
			phaseSample.commandEmptyBarrierObserved = true
			if syncErr != nil {
				return syncErr
			}
		} else if err := b.db.syncPublicCommandWAL(); err != nil {
			return err
		}
	} else {
		if err := b.inner.Write(); err != nil {
			return err
		}
	}
	b.disableInnerStreamingBypass()
	if !b.retainPayloadAfterWrite {
		b.resetPayloadForReuse()
	}
	b.opCount = 0
	b.hasDeleteRange = false
	b.dirty = false
	return nil
}

func (b *commandWALPublicBatch) Close() error {
	if b == nil || b.inner == nil {
		return nil
	}
	owner := b.db
	// Closing an uncommitted batch intentionally discards staged command-WAL
	// payload bytes, matching ordinary batch semantics: only Write/WriteSync
	// appends and publishes a RawKVBatch command frame.
	err := b.inner.Close()
	payload := b.payload
	keepPayload := false
	if b.payloadLeasedToMemtable {
		payload.DetachRetainedValueByteBuffers(b.payloadLeasedBufferMask)
	} else {
		keepPayload = payload.PrepareForReuse(commandWALPublicBatchPayloadPoolRetainMaxBytes)
	}
	b.inner = nil
	b.innerSetViewValidated = nil
	b.innerSetViewer = nil
	b.innerDeleteViewValidated = nil
	b.innerDeleteViewer = nil
	b.payload = commitlog.RawKVBatchPayloadBuilder{}
	b.payloadBypass = false
	b.payloadOpHint = 0
	b.payloadByteHint = 0
	b.payloadSoftCapBytes = 0
	b.opCount = 0
	b.hasDeleteRange = false
	b.payloadLeasedToMemtable = false
	b.payloadLeasedBufferMask = 0
	b.payloadLeaseAttachedMask = 0
	b.resetPointIngressStats()
	b.dirty = false
	b.retainPayloadAfterWrite = false
	b.closed = true
	if owner != nil && keepPayload {
		owner.commandWALPublicPayloadPool.Put(payload)
	}
	return err
}

func (b *commandWALPublicBatch) Reset() {
	if b == nil || b.inner == nil {
		return
	}
	resetter, ok := b.inner.(interface{ Reset() })
	if !ok {
		b.disableInnerStreamingBypass()
		b.resetPayloadForReuse()
		b.opCount = 0
		b.hasDeleteRange = false
		b.resetPointIngressStats()
		b.dirty = false
		b.retainPayloadAfterWrite = false
		b.closed = false
		return
	}
	resetter.Reset()
	b.rebindInnerViewers()
	b.disableInnerStreamingBypass()
	b.resetPayloadForReuse()
	b.opCount = 0
	b.hasDeleteRange = false
	b.resetPointIngressStats()
	b.dirty = false
	b.retainPayloadAfterWrite = false
	b.closed = false
}

func commandWALPublicAllZeroBytes(p []byte) bool {
	for _, b := range p {
		if b != 0 {
			return false
		}
	}
	return len(p) > 0
}

func commandWALPublicSameNonEmptyBytesData(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

func (b *commandWALPublicBatch) knownZeroValue(value []byte) bool {
	if b == nil || len(value) == 0 {
		return false
	}
	if b.knownZeroValueValid &&
		b.knownZeroValueLen == len(value) &&
		commandWALPublicSameNonEmptyBytesData(value, b.knownZeroValueRef) {
		return true
	}
	if !commandWALPublicAllZeroBytes(value) {
		return false
	}
	b.knownZeroValueRef = value
	b.knownZeroValueLen = len(value)
	b.knownZeroValueValid = true
	return true
}

func (b *commandWALPublicBatch) commandWALPayload() ([]byte, error) {
	if b == nil || b.inner == nil {
		return nil, ErrClosed
	}
	if !b.payloadBypass && b.payload.Count() == b.opCount && b.payload.Count() > 0 {
		if b.payload.EntryRevisionsEnabled() {
			if err := b.stampPayloadEntryRevisions(); err != nil {
				return nil, err
			}
			return b.payload.Payload(), nil
		}
		return b.payload.Payload(), nil
	}
	byteHint, _ := b.inner.GetByteSize()
	sawEntryRevision := false
	scan := func(emit func(commitlog.RawKVOperation) error) error {
		return b.inner.Replay(func(entry batch.Entry) error {
			switch entry.Type {
			case batch.OpDeleteRange:
				if batch.IsDeleteRangeNoop(entry.Key, entry.Value) {
					return nil
				}
				return emit(commitlog.RawKVOperation{Op: commitlog.RawKVOpDeleteRange, Key: entry.Key, Value: entry.Value})
			case batch.OpDelete:
				if entry.Revision != page.LegacyEntryRevision {
					sawEntryRevision = true
				}
				return emit(commitlog.RawKVOperation{Op: commitlog.RawKVOpDelete, Key: entry.Key, Revision: uint64(entry.Revision)})
			case batch.OpPut:
				if entry.Revision != page.LegacyEntryRevision {
					sawEntryRevision = true
				}
				return emit(commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: entry.Key, Value: entry.Value, Revision: uint64(entry.Revision)})
			}
			return nil
		})
	}
	payload, err := commitlog.EncodeRawKVBatchPayloadScanWithHint(scan, b.opCount, byteHint)
	if err == nil {
		return payload, nil
	}
	if !sawEntryRevision || !errors.Is(err, commitlog.ErrCommandWALUnsupportedVersion) {
		return nil, err
	}
	plan, err := commitlog.PlanRawKVBatchPayloadScan(scan)
	if err != nil {
		return nil, err
	}
	return commitlog.EncodeRawKVBatchPayloadPlanned(plan, scan)
}

func (b *commandWALPublicBatch) stampPayloadEntryRevisions() error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if entries, ok := b.inner.(interface{ OrderedEntries() []batch.Entry }); ok {
		return b.stampPayloadEntryRevisionsFromEntries(entries.OrderedEntries())
	}
	return b.payload.StampEntryRevisions(func(emit func(uint64) error) error {
		return b.inner.Replay(func(entry batch.Entry) error {
			switch entry.Type {
			case batch.OpDelete, batch.OpPut:
				if entry.Revision != page.LegacyEntryRevision {
					return emit(uint64(entry.Revision))
				}
				return emit(0)
			case batch.OpDeleteRange:
				return emit(0)
			default:
				return nil
			}
		})
	})
}

func (b *commandWALPublicBatch) stampPayloadEntryRevisionsFromEntries(entries []batch.Entry) error {
	if b == nil {
		return ErrClosed
	}
	payload := b.payload.Payload()
	offsets := b.payload.RevisionSlotOffsets()
	if len(offsets) > 0 {
		seen := 0
		for i := range entries {
			entry := &entries[i]
			revision := uint64(0)
			switch entry.Type {
			case batch.OpDelete, batch.OpPut:
				if entry.Revision != page.LegacyEntryRevision {
					revision = uint64(entry.Revision)
				}
			case batch.OpDeleteRange:
			default:
				continue
			}
			if seen >= len(offsets) {
				return commitlog.ErrCorrupt
			}
			off := int(offsets[seen])
			if off < commandWALRawKVBatchHeaderSize || off+8 > len(payload) {
				return commitlog.ErrCorrupt
			}
			binary.LittleEndian.PutUint64(payload[off:off+8], revision)
			seen++
		}
		if seen != len(offsets) {
			return commitlog.ErrCorrupt
		}
		return nil
	}
	return b.payload.StampEntryRevisions(func(emit func(uint64) error) error {
		for i := range entries {
			entry := &entries[i]
			switch entry.Type {
			case batch.OpDelete, batch.OpPut:
				if entry.Revision != page.LegacyEntryRevision {
					if err := emit(uint64(entry.Revision)); err != nil {
						return err
					}
					continue
				}
				if err := emit(0); err != nil {
					return err
				}
			case batch.OpDeleteRange:
				if err := emit(0); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (b *commandWALPublicBatch) appendCommandWAL(sync bool) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if !b.payloadBypass && b.payload.Count() == b.opCount && b.payload.Count() > 0 {
		payload, err := b.commandWALPayload()
		if err != nil {
			return err
		}
		return b.db.appendPublicRawKVCommandPayload(payload, sync)
	}
	return b.db.appendPublicRawKVCommandEntryScan(b.inner.Replay, b.opCount, sync)
}

func (b *commandWALPublicBatch) appendCommandWALMeasured(sync bool) (db.CommandWALRequestTiming, error) {
	var timing db.CommandWALRequestTiming
	if b == nil || b.inner == nil || b.db == nil {
		return timing, ErrClosed
	}
	preparationStart := time.Now()
	timing.PublicPreparationObserved = true
	if !b.payloadBypass && b.payload.Count() == b.opCount && b.payload.Count() > 0 {
		payload, err := b.commandWALPayload()
		timing.PublicPayloadEntryScanPreparation = time.Since(preparationStart)
		if err != nil {
			return timing, err
		}
		backendTiming, appendErr := b.db.appendPublicRawKVCommandPayloadMeasured(payload, sync)
		backendTiming.PublicPayloadEntryScanPreparation += timing.PublicPayloadEntryScanPreparation
		backendTiming.PublicPreparationObserved = true
		return backendTiming, appendErr
	}
	timing.PublicPayloadEntryScanPreparation = time.Since(preparationStart)
	backendTiming, appendErr := b.db.appendPublicRawKVCommandEntryScanMeasured(b.inner.Replay, b.opCount, sync)
	backendTiming.PublicPayloadEntryScanPreparation += timing.PublicPayloadEntryScanPreparation
	backendTiming.PublicPreparationObserved = true
	return backendTiming, appendErr
}

func (b *commandWALPublicBatch) Replay(fn func(batch.Entry) error) error {
	if b == nil || b.inner == nil {
		return nil
	}
	return b.inner.Replay(fn)
}

func (b *commandWALPublicBatch) GetByteSize() (int, error) {
	if b == nil || b.inner == nil {
		return 0, nil
	}
	return b.inner.GetByteSize()
}
