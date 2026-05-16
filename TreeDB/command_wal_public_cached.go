package treedb

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func (tdb *DB) appendPublicRawKVCommand(ops []commitlog.RawKVOperation, sync bool) error {
	if tdb == nil || !tdb.commandWALCached || len(ops) == 0 {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	payload, err := commitlog.EncodeRawKVBatchPayload(ops)
	if err != nil {
		return err
	}
	return tdb.appendPublicRawKVCommandPayload(payload, sync)
}

func (tdb *DB) appendPublicRawKVSingleCommand(op commitlog.RawKVOperation, sync bool) error {
	if tdb == nil || !tdb.commandWALCached {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	lsn, err := tdb.backend.AppendRawKVSingleCommandWAL(op, sync)
	if err == nil {
		tdb.recordPublicCommandWALPendingLSN(lsn)
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
	lsn, err := tdb.backend.AppendRawKVBatchPayloadCommandWAL(payload, sync)
	if err != nil {
		return err
	}
	tdb.recordPublicCommandWALPendingLSN(lsn)
	return nil
}

func (tdb *DB) recordPublicCommandWALPendingLSN(lsn uint64) {
	if tdb == nil || lsn == 0 {
		return
	}
	tdb.commandWALMu.Lock()
	if tdb.commandWALFirst == 0 || lsn < tdb.commandWALFirst {
		tdb.commandWALFirst = lsn
	}
	if lsn > tdb.commandWALLast {
		tdb.commandWALLast = lsn
	}
	tdb.commandWALMu.Unlock()
}

func (tdb *DB) publishPublicCommandWALPending(sync bool) error {
	if tdb == nil || !tdb.commandWALCached {
		return nil
	}
	if tdb.backend == nil {
		return ErrClosed
	}
	tdb.commandWALMu.Lock()
	first, last := tdb.commandWALFirst, tdb.commandWALLast
	tdb.commandWALMu.Unlock()
	if first == 0 || last == 0 {
		return nil
	}
	state := tdb.backend.State()
	if state == nil {
		return ErrClosed
	}
	current := state.AppliedCommandLSN
	if last <= current {
		tdb.commandWALMu.Lock()
		if tdb.commandWALLast <= current {
			tdb.commandWALFirst = 0
			tdb.commandWALLast = 0
		}
		tdb.commandWALMu.Unlock()
		return nil
	}
	if first > current+1 {
		return fmt.Errorf("%w: pending public command WAL starts at %d after applied %d", db.ErrCommandWALAppliedLSNNonContig, first, current)
	}
	if err := tdb.backend.FlushCommandWAL(sync); err != nil {
		return err
	}
	publishSync := sync && !strings.HasPrefix(tdb.durabilityMode, "wal_on_relaxed_sync")
	if err := tdb.backend.PublishCommandWALAppliedLSN(last, []db.CommandWALLSNRange{{First: current + 1, Last: last}}, publishSync); err != nil {
		return err
	}
	tdb.commandWALMu.Lock()
	if tdb.commandWALLast <= last {
		tdb.commandWALFirst = 0
		tdb.commandWALLast = 0
	} else if tdb.commandWALFirst <= last {
		tdb.commandWALFirst = last + 1
	}
	tdb.commandWALMu.Unlock()
	return nil
}

type commandWALPublicBatch struct {
	db      *DB
	inner   Batch
	payload commitlog.RawKVBatchPayloadBuilder
	opCount int
	dirty   bool
	closed  bool
}

const commandWALPublicBatchEstimatedKeyValueBytes = 48

func newCommandWALPublicBatch(tdb *DB, inner Batch, opHint int) *commandWALPublicBatch {
	b := &commandWALPublicBatch{db: tdb, inner: inner}
	opHint = db.NormalizePublicBatchReserveHint(opHint)
	byteHint := 0
	if opHint > 0 && opHint <= int(^uint(0)>>1)/commandWALPublicBatchEstimatedKeyValueBytes {
		byteHint = opHint * commandWALPublicBatchEstimatedKeyValueBytes
	}
	b.payload.ResetWithHint(opHint, byteHint)
	return b
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
	keyView, valueView, err := b.payload.Append(commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: key, Value: value})
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
	return b.Set(key, value)
}

func (b *commandWALPublicBatch) Delete(key []byte) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return caching.ErrKeyEmpty
	}
	oldLen, oldCount := b.payload.Len(), b.payload.Count()
	keyView, _, err := b.payload.Append(commitlog.RawKVOperation{Op: commitlog.RawKVOpDelete, Key: key})
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
	return b.Delete(key)
}

func (b *commandWALPublicBatch) innerSetView(key, value []byte) error {
	if setter, ok := b.inner.(interface {
		SetView(key, value []byte) error
	}); ok {
		return setter.SetView(key, value)
	}
	return b.inner.Set(key, value)
}

func (b *commandWALPublicBatch) innerDeleteView(key []byte) error {
	if deleter, ok := b.inner.(interface {
		DeleteView(key []byte) error
	}); ok {
		return deleter.DeleteView(key)
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
		if err := b.appendCommandWAL(sync); err != nil {
			return err
		}
	}
	if sync {
		if err := b.inner.WriteSync(); err != nil {
			return err
		}
		b.payload.ResetWithHint(0, 0)
		b.opCount = 0
		b.dirty = false
		return nil
	}
	if err := b.inner.Write(); err != nil {
		return err
	}
	b.payload.ResetWithHint(0, 0)
	b.opCount = 0
	b.dirty = false
	return nil
}

func (b *commandWALPublicBatch) Close() error {
	if b == nil || b.inner == nil {
		return nil
	}
	err := b.inner.Close()
	b.inner = nil
	b.payload.ResetWithHint(0, 0)
	b.dirty = false
	b.closed = true
	return err
}

func (b *commandWALPublicBatch) Reset() {
	if b == nil || b.inner == nil {
		return
	}
	if resetter, ok := b.inner.(interface{ Reset() }); ok {
		resetter.Reset()
		b.payload.ResetWithHint(0, 0)
		b.opCount = 0
		b.dirty = false
		b.closed = false
	}
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
