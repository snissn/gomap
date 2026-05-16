package treedb

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
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
	intent, err := tdb.backend.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		return err
	}
	lsn, err := tdb.backend.AppendCommandWALIntent(intent, sync)
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
	if err := tdb.backend.PublishCommandWALAppliedLSN(last, []db.CommandWALLSNRange{{First: current + 1, Last: last}}, sync); err != nil {
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
	db          *DB
	inner       Batch
	expectedOps int
	dirty       bool
	closed      bool
}

func (b *commandWALPublicBatch) Set(key, value []byte) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if err := b.inner.Set(key, value); err != nil {
		return err
	}
	b.dirty = true
	return nil
}

func (b *commandWALPublicBatch) Delete(key []byte) error {
	if b == nil || b.inner == nil {
		return ErrClosed
	}
	if err := b.inner.Delete(key); err != nil {
		return err
	}
	b.dirty = true
	return nil
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
		ops, err := b.commandWALOps()
		if err != nil {
			return err
		}
		if err := b.db.appendPublicRawKVCommand(ops, sync); err != nil {
			return err
		}
	}
	if sync {
		if err := b.inner.WriteSync(); err != nil {
			return err
		}
		b.dirty = false
		return nil
	}
	if err := b.inner.Write(); err != nil {
		return err
	}
	b.dirty = false
	return nil
}

func (b *commandWALPublicBatch) Close() error {
	if b == nil || b.inner == nil {
		return nil
	}
	err := b.inner.Close()
	b.inner = nil
	b.dirty = false
	b.closed = true
	return err
}

func (b *commandWALPublicBatch) commandWALOps() ([]commitlog.RawKVOperation, error) {
	if b == nil || b.inner == nil {
		return nil, ErrClosed
	}
	ops := make([]commitlog.RawKVOperation, 0, b.expectedOps)
	err := b.inner.Replay(func(entry batch.Entry) error {
		switch entry.Type {
		case batch.OpDelete:
			ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpDelete, Key: entry.Key})
		case batch.OpPut:
			ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: entry.Key, Value: entry.Value})
		}
		return nil
	})
	return ops, err
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
