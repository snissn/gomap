package treedb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func (tdb *DB) appendPublicRawKVPointCommand(op commitlog.RawKVOp, key, value []byte, sync bool) error {
	if tdb == nil || !tdb.commandWALCached {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	lsn, err := tdb.backend.AppendRawKVPointCommandWALTrusted(op, key, value, sync)
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	if err == nil && testAfterPublicCommandWALPointAppend != nil {
		testAfterPublicCommandWALPointAppend(commitlog.RawKVOperation{Op: op, Key: key, Value: value})
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

func (tdb *DB) appendPublicRawKVCommandEntries(entries []batch.Entry, sync bool) error {
	if tdb == nil || !tdb.commandWALCached || len(entries) == 0 {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	intent, err := tdb.backend.NewRawKVCommandWALIntentFromOrderedEntries(entries)
	if err != nil {
		return err
	}
	lsn, err := tdb.backend.AppendCommandWALIntent(intent, sync)
	if lsn != 0 {
		tdb.recordPublicCommandWALPendingLSN(lsn)
	}
	return err
}

func (tdb *DB) appendPublicRawKVDeleteRangeCommand(start, end []byte, sync bool) error {
	if tdb == nil || !tdb.commandWALCached || batch.IsDeleteRangeNoop(start, end) {
		return nil
	}
	var payload commitlog.RawKVBatchPayloadBuilder
	_ = payload.ResetWithHint(1, len(start)+len(end))
	if _, err := payload.AppendDeleteRange(start, end); err != nil {
		return err
	}
	return tdb.appendPublicRawKVCommandPayload(payload.Payload(), sync)
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
	state := tdb.backend.State()
	if state == nil {
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
	state := tdb.backend.State()
	if state == nil {
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
	return sync && strings.HasPrefix(durabilityMode, "wal_on_sync")
}

type commandWALPublicBatch struct {
	db                  *DB
	inner               Batch
	innerSetViewFn      func(key, value []byte) error
	innerDeleteViewFn   func(key []byte) error
	payload             commitlog.RawKVBatchPayloadBuilder
	payloadOpHint       int
	payloadByteHint     int
	payloadSoftCapBytes int
	payloadBypass       bool
	opCount             int
	hasDeleteRange      bool
	dirty               bool
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
const commandWALPublicBatchRawKVOpHeaderBytes = 1 + 4 + 4

func newCommandWALPublicBatch(tdb *DB, inner Batch, opHint int) *commandWALPublicBatch {
	b := &commandWALPublicBatch{db: tdb, inner: inner}
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
	b.payloadBypass = false
	b.hasDeleteRange = false
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
	b.resetPayloadWithHint()
	b.opCount = 0
	b.hasDeleteRange = false
	b.retainPayloadAfterWrite = false
}

func (b *commandWALPublicBatch) rebindInnerViewers() {
	if b == nil {
		return
	}
	b.innerSetViewFn = nil
	b.innerDeleteViewFn = nil
	if b.inner == nil {
		return
	}
	if setter, ok := b.inner.(interface {
		SetViewValidated(key, value []byte) error
	}); ok {
		b.innerSetViewFn = setter.SetViewValidated
	} else if setter, ok := b.inner.(interface {
		SetView(key, value []byte) error
	}); ok {
		b.innerSetViewFn = setter.SetView
	}
	if deleter, ok := b.inner.(interface {
		DeleteViewValidated(key []byte) error
	}); ok {
		b.innerDeleteViewFn = deleter.DeleteViewValidated
	} else if deleter, ok := b.inner.(interface {
		DeleteView(key []byte) error
	}); ok {
		b.innerDeleteViewFn = deleter.DeleteView
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
	_, _, err := b.setView(key, value, false)
	return err
}

func (b *commandWALPublicBatch) SetView(key, value []byte) error {
	_, _, err := b.setView(key, value, false)
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
	return b.setView(key, value, true)
}

func (b *commandWALPublicBatch) setView(key, value []byte, retainReplayViews bool) (keyView, valueView []byte, err error) {
	if b == nil || b.inner == nil {
		return nil, nil, ErrClosed
	}
	b.preparePayloadForAppend()
	key = normalizeRawKVPointKey(key)
	value = normalizeRawKVValue(value)
	if retainReplayViews && commandWALPublicAllZeroBytes(value) {
		// The raw-KV payload builder tracks the first zero value by backing-array
		// identity so repeated immutable zero buffers can avoid rescanning. Adapter
		// replay-byte callers may reuse and mutate their value buffer between Put
		// calls, so keep that compact-zero identity tied to an immutable copy.
		value = append([]byte(nil), value...)
	}
	needed := commandWALPublicBatchRawKVOpHeaderBytes + len(key) + len(value)
	if b.shouldBypassPayloadAppend(needed, retainReplayViews) {
		if err := b.inner.Set(key, value); err != nil {
			return nil, nil, err
		}
		b.payloadBypass = true
		b.opCount++
		b.dirty = true
		return nil, nil, nil
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	keyView, valueView, err = b.payload.AppendSet(key, value)
	if err != nil {
		return nil, nil, err
	}
	if err := b.innerSetView(keyView, valueView); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return nil, nil, err
	}
	b.opCount++
	b.dirty = true
	if retainReplayViews {
		b.retainPayloadAfterWrite = true
	}
	return keyView, valueView, nil
}

func (b *commandWALPublicBatch) Delete(key []byte) error {
	_, err := b.deleteView(key, false)
	return err
}

func (b *commandWALPublicBatch) DeleteView(key []byte) error {
	_, err := b.deleteView(key, false)
	return err
}

// DeleteViewWithReplayBytes records a Delete and returns a payload-owned key
// view for adapter replay logs. See SetViewWithReplayBytes for lifetime rules.
//
// This method is intentionally not part of the public Batch interface.
func (b *commandWALPublicBatch) DeleteViewWithReplayBytes(key []byte) (keyView []byte, err error) {
	return b.deleteView(key, true)
}

func (b *commandWALPublicBatch) deleteView(key []byte, retainReplayViews bool) (keyView []byte, err error) {
	if b == nil || b.inner == nil {
		return nil, ErrClosed
	}
	b.preparePayloadForAppend()
	key = normalizeRawKVPointKey(key)
	needed := commandWALPublicBatchRawKVOpHeaderBytes + len(key)
	if b.shouldBypassPayloadAppend(needed, retainReplayViews) {
		if err := b.inner.Delete(key); err != nil {
			return nil, err
		}
		b.payloadBypass = true
		b.opCount++
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
	needed := commandWALPublicBatchRawKVOpHeaderBytes + commandWALPublicRangeBoundBytes(start) + commandWALPublicRangeBoundBytes(end)
	if b.shouldBypassPayloadAppend(needed, false) {
		if err := b.inner.DeleteRange(start, end); err != nil {
			return err
		}
		b.payloadBypass = true
		b.opCount++
		b.hasDeleteRange = true
		b.dirty = true
		return nil
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	if _, err := b.payload.AppendDeleteRange(start, end); err != nil {
		return err
	}
	if err := b.inner.DeleteRange(start, end); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return err
	}
	b.opCount++
	b.hasDeleteRange = true
	b.dirty = true
	return nil
}

func (b *commandWALPublicBatch) innerSetView(key, value []byte) error {
	if b.innerSetViewFn != nil {
		return b.innerSetViewFn(key, value)
	}
	return b.inner.Set(key, value)
}

func (b *commandWALPublicBatch) innerDeleteView(key []byte) error {
	if b.innerDeleteViewFn != nil {
		return b.innerDeleteViewFn(key)
	}
	return b.inner.Delete(key)
}

func (b *commandWALPublicBatch) shouldBypassPayloadAppend(needed int, retainReplayViews bool) bool {
	if b == nil || retainReplayViews || b.payloadSoftCapBytes <= 0 {
		return false
	}
	if b.payloadBypass {
		return true
	}
	current := b.payload.Len()
	if current >= b.payloadSoftCapBytes {
		return true
	}
	return needed > b.payloadSoftCapBytes-current
}

func commandWALPublicRangeBoundBytes(bound []byte) int {
	if bound == nil {
		return 0
	}
	return len(bound)
}

func (b *commandWALPublicBatch) Write() error {
	return b.write(false)
}

func (b *commandWALPublicBatch) WriteSync() error {
	return b.write(true)
}

func (b *commandWALPublicBatch) write(sync bool) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if b.db != nil {
		unlock, err := b.db.beginPublicOperation()
		if err != nil {
			return err
		}
		defer unlock()
	}
	if b.dirty {
		writer, ok := b.inner.(interface {
			WriteAfterCommandWALAppend(sync bool, appendCommand func() error) error
		})
		if !ok {
			return fmt.Errorf("treedb: command wal batch requires cached command append hook")
		}
		if err := writer.WriteAfterCommandWALAppend(sync, func() error {
			return b.appendCommandWAL(sync)
		}); err != nil {
			return err
		}
		b.disableInnerStreamingBypass()
		if !b.retainPayloadAfterWrite {
			b.resetPayloadWithHint()
		}
		b.opCount = 0
		b.hasDeleteRange = false
		b.dirty = false
		return nil
	}
	if sync {
		if err := b.inner.WriteSync(); err != nil {
			return err
		}
	} else {
		if err := b.inner.Write(); err != nil {
			return err
		}
	}
	b.disableInnerStreamingBypass()
	if !b.retainPayloadAfterWrite {
		b.resetPayloadWithHint()
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
	// Closing an uncommitted batch intentionally discards staged command-WAL
	// payload bytes, matching ordinary batch semantics: only Write/WriteSync
	// appends and publishes a RawKVBatch command frame.
	err := b.inner.Close()
	b.inner = nil
	b.innerSetViewFn = nil
	b.innerDeleteViewFn = nil
	b.resetPayloadWithHint()
	b.opCount = 0
	b.hasDeleteRange = false
	b.dirty = false
	b.retainPayloadAfterWrite = false
	b.closed = true
	return err
}

func (b *commandWALPublicBatch) Reset() {
	if b == nil || b.inner == nil {
		return
	}
	resetter, ok := b.inner.(interface{ Reset() })
	if !ok {
		b.disableInnerStreamingBypass()
		b.resetPayloadWithHint()
		b.opCount = 0
		b.hasDeleteRange = false
		b.dirty = false
		b.retainPayloadAfterWrite = false
		b.closed = false
		return
	}
	resetter.Reset()
	b.rebindInnerViewers()
	b.disableInnerStreamingBypass()
	b.resetPayloadWithHint()
	b.opCount = 0
	b.hasDeleteRange = false
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

func (b *commandWALPublicBatch) commandWALPayload() ([]byte, error) {
	if b == nil || b.inner == nil {
		return nil, ErrClosed
	}
	if !b.payloadBypass && b.payload.Count() == b.opCount && b.payload.Count() > 0 {
		return b.payload.Payload(), nil
	}
	byteHint, _ := b.inner.GetByteSize()
	return commitlog.EncodeRawKVBatchPayloadScanWithHint(func(emit func(commitlog.RawKVOperation) error) error {
		return b.inner.Replay(func(entry batch.Entry) error {
			switch entry.Type {
			case batch.OpDeleteRange:
				if batch.IsDeleteRangeNoop(entry.Key, entry.Value) {
					return nil
				}
				return emit(commitlog.RawKVOperation{Op: commitlog.RawKVOpDeleteRange, Key: entry.Key, Value: entry.Value})
			case batch.OpDelete:
				return emit(commitlog.RawKVOperation{Op: commitlog.RawKVOpDelete, Key: entry.Key})
			case batch.OpPut:
				return emit(commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: entry.Key, Value: entry.Value})
			}
			return nil
		})
	}, b.opCount, byteHint)
}

func (b *commandWALPublicBatch) appendCommandWAL(sync bool) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if !b.hasDeleteRange && b.db != nil && b.db.commandWALCached && b.db.backend != nil {
		var entries []batch.Entry
		hasPointer := false
		if err := b.inner.Replay(func(entry batch.Entry) error {
			if entry.Type == batch.OpPut && entry.IsPtr {
				hasPointer = true
			}
			entries = append(entries, entry)
			return nil
		}); err != nil {
			return err
		}
		if hasPointer {
			return b.db.appendPublicRawKVCommandEntries(entries, sync)
		}
	}
	payload, err := b.commandWALPayload()
	if err != nil {
		return err
	}
	return b.db.appendPublicRawKVCommandPayload(payload, sync)
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
