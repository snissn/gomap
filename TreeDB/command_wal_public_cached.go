package treedb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
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
	tdb.commandWALCheckpointCutoverLast.Store(tdb.commandWALLast.Load())
}

func (tdb *DB) preparePublicCommandWALPendingPublish(sync bool) (uint64, []db.CommandWALLSNRange, error) {
	if tdb == nil || !tdb.commandWALCached {
		return 0, nil, nil
	}
	if tdb.backend == nil {
		return 0, nil, ErrClosed
	}
	first, last := tdb.publicCommandWALPendingRange()
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
	if err := tdb.backend.FlushCommandWAL(sync); err != nil {
		return 0, nil, err
	}
	return last, []db.CommandWALLSNRange{{First: current + 1, Last: last}}, nil
}

func publicCommandWALPublishSync(durabilityMode string, sync bool) bool {
	return sync && strings.HasPrefix(durabilityMode, "wal_on_sync")
}

type commandWALPublicBatch struct {
	db        *DB
	inner     Batch
	setViewer interface {
		SetView(key, value []byte) error
	}
	setViewValidated interface {
		SetViewValidated(key, value []byte) error
	}
	deleteViewer interface {
		DeleteView(key []byte) error
	}
	deleteViewValidated interface {
		DeleteViewValidated(key []byte) error
	}
	payload commitlog.RawKVBatchPayloadBuilder
	opCount int
	dirty   bool
	closed  bool
}

const commandWALPublicBatchEstimatedKeyValueBytes = 48

func newCommandWALPublicBatch(tdb *DB, inner Batch, opHint int) *commandWALPublicBatch {
	b := &commandWALPublicBatch{db: tdb, inner: inner}
	b.disableInnerStreamingBypass()
	b.rebindInnerViewers()
	opHint = db.NormalizePublicBatchReserveHint(opHint)
	byteHint := 0
	if opHint > 0 && opHint <= int(^uint(0)>>1)/commandWALPublicBatchEstimatedKeyValueBytes {
		byteHint = opHint * commandWALPublicBatchEstimatedKeyValueBytes
	}
	b.payload.ResetWithHint(opHint, byteHint)
	return b
}

func (b *commandWALPublicBatch) rebindInnerViewers() {
	if b == nil {
		return
	}
	b.setViewer = nil
	b.setViewValidated = nil
	b.deleteViewer = nil
	b.deleteViewValidated = nil
	if b.inner == nil {
		return
	}
	if setter, ok := b.inner.(interface {
		SetView(key, value []byte) error
	}); ok {
		b.setViewer = setter
	}
	if setter, ok := b.inner.(interface {
		SetViewValidated(key, value []byte) error
	}); ok {
		b.setViewValidated = setter
	}
	if deleter, ok := b.inner.(interface {
		DeleteView(key []byte) error
	}); ok {
		b.deleteViewer = deleter
	}
	if deleter, ok := b.inner.(interface {
		DeleteViewValidated(key []byte) error
	}); ok {
		b.deleteViewValidated = deleter
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
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return caching.ErrKeyEmpty
	}
	if value == nil {
		return caching.ErrValueNil
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	keyView, valueView, err := b.payload.AppendSet(key, value)
	if err != nil {
		return err
	}
	if err := b.innerSetView(keyView, valueView); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return err
	}
	b.opCount++
	b.dirty = true
	return nil
}

func (b *commandWALPublicBatch) SetView(key, value []byte) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return caching.ErrKeyEmpty
	}
	if value == nil {
		return caching.ErrValueNil
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	if _, _, err := b.payload.AppendSet(key, value); err != nil {
		return err
	}
	if err := b.innerSetView(key, value); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return err
	}
	b.opCount++
	b.dirty = true
	return nil
}

func (b *commandWALPublicBatch) Delete(key []byte) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return caching.ErrKeyEmpty
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	keyView, err := b.payload.AppendDelete(key)
	if err != nil {
		return err
	}
	if err := b.innerDeleteView(keyView); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return err
	}
	b.opCount++
	b.dirty = true
	return nil
}

func (b *commandWALPublicBatch) DeleteView(key []byte) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return caching.ErrKeyEmpty
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	if _, err := b.payload.AppendDelete(key); err != nil {
		return err
	}
	if err := b.innerDeleteView(key); err != nil {
		b.payload.Truncate(oldLen, oldCount)
		return err
	}
	b.opCount++
	b.dirty = true
	return nil
}

func (b *commandWALPublicBatch) innerSetView(key, value []byte) error {
	if b.setViewValidated != nil {
		return b.setViewValidated.SetViewValidated(key, value)
	}
	if b.setViewer != nil {
		return b.setViewer.SetView(key, value)
	}
	return b.inner.Set(key, value)
}

func (b *commandWALPublicBatch) innerDeleteView(key []byte) error {
	if b.deleteViewValidated != nil {
		return b.deleteViewValidated.DeleteViewValidated(key)
	}
	if b.deleteViewer != nil {
		return b.deleteViewer.DeleteView(key)
	}
	return b.inner.Delete(key)
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
		b.payload.ResetWithHint(0, 0)
		b.opCount = 0
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
	b.payload.ResetWithHint(0, 0)
	b.opCount = 0
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
	b.setViewer = nil
	b.setViewValidated = nil
	b.deleteViewer = nil
	b.deleteViewValidated = nil
	b.payload.ResetWithHint(0, 0)
	b.dirty = false
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
		b.payload.ResetWithHint(0, 0)
		b.opCount = 0
		b.dirty = false
		b.closed = false
		return
	}
	resetter.Reset()
	b.rebindInnerViewers()
	b.disableInnerStreamingBypass()
	b.payload.ResetWithHint(0, 0)
	b.opCount = 0
	b.dirty = false
	b.closed = false
}

func (b *commandWALPublicBatch) commandWALPayload() ([]byte, error) {
	if b == nil || b.inner == nil {
		return nil, ErrClosed
	}
	if b.payload.Count() == b.opCount && b.payload.Count() > 0 {
		return b.payload.Payload(), nil
	}
	byteHint, _ := b.inner.GetByteSize()
	return commitlog.EncodeRawKVBatchPayloadScanWithHint(func(emit func(commitlog.RawKVOperation) error) error {
		return b.inner.Replay(func(entry batch.Entry) error {
			switch entry.Type {
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
