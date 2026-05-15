package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type commandWALBatchIntent struct {
	payload      []byte
	externalRefs bool
	lsn          uint64
	coveredRange [1]CommandWALLSNRange
}

func (db *DB) prepareRawKVCommandWALIntent(b *Batch) (*commandWALBatchIntent, error) {
	if db == nil || !db.commandWAL || db.durability == DurabilityWALOffRelaxed {
		return nil, nil
	}
	if b == nil || b.batch == nil {
		return nil, nil
	}
	entries := b.batch.SortedEntries()
	if len(entries) == 0 {
		return nil, nil
	}
	externalRefs := false
	flushedExternalRefsForLookup := false
	var smallOps [16]commitlog.RawKVOperation
	ops := smallOps[:0]
	if len(entries) > len(smallOps) {
		ops = make([]commitlog.RawKVOperation, 0, len(entries))
	}
	for i := range entries {
		entry := entries[i]
		switch entry.Type {
		case batchpkg.OpDelete:
			ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpDelete, Key: entry.Key})
		case batchpkg.OpPut:
			if entry.IsPtr {
				externalRefs = true
				if !flushedExternalRefsForLookup {
					if err := db.flushCommandWALExternalRefs(false); err != nil {
						return nil, err
					}
					flushedExternalRefsForLookup = true
				}
				rid, err := db.lookupCommandWALValueLogRID(entry.ValuePtr)
				if err != nil {
					return nil, err
				}
				ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpSetRID, Key: entry.Key, RID: rid})
				continue
			}
			value := entry.Value
			ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: entry.Key, Value: value})
		default:
			return nil, fmt.Errorf("treedb: command wal unknown raw kv batch op %d", entry.Type)
		}
	}
	payload, err := commitlog.EncodeRawKVBatchPayload(ops)
	if err != nil {
		return nil, err
	}
	return &commandWALBatchIntent{payload: payload, externalRefs: externalRefs}, nil
}

func (db *DB) lookupCommandWALValueLogRID(ptr page.ValuePtr) (uint64, error) {
	if db == nil || db.valueLogManager == nil {
		return 0, fmt.Errorf("treedb: command wal raw kv pointer rid reader unavailable")
	}
	if ptr.FileID == 0 || ptr.Length == 0 {
		return 0, fmt.Errorf("treedb: command wal raw kv invalid value-log pointer")
	}
	path := db.valueLogManager.SegmentPath(ptr.FileID)
	r, err := valuelog.NewReader(path, ptr.FileID)
	if err != nil {
		return 0, err
	}
	defer func() { _ = r.Close() }()
	for {
		rid, gotPtr, err := r.ReadNextMeta()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, err
		}
		if gotPtr == ptr {
			if rid == 0 {
				return 0, fmt.Errorf("treedb: command wal raw kv zero value-log rid")
			}
			return rid, nil
		}
	}
	return 0, fmt.Errorf("treedb: command wal raw kv missing value-log rid for file=%d offset=%d length=%d", ptr.FileID, ptr.Offset, ptr.Length)
}

func (db *DB) flushCommandWALExternalRefs(sync bool) error {
	appender := db.currentValueLogAppender()
	if appender == nil {
		return nil
	}
	if sync {
		return appender.Sync()
	}
	return appender.Flush()
}

func (db *DB) appendRawKVCommandWALIntent(intent *commandWALBatchIntent, sync bool) (uint64, error) {
	if intent == nil {
		return 0, nil
	}
	if intent.lsn != 0 {
		return intent.lsn, nil
	}
	if db == nil || db.commandJournal == nil {
		return 0, fmt.Errorf("treedb: command wal journal unavailable")
	}
	if intent.externalRefs {
		if err := db.flushCommandWALExternalRefs(sync); err != nil {
			return 0, err
		}
	}
	db.mu.RLock()
	baseAppliedLSN := db.meta.AppliedCommandLSN
	db.mu.RUnlock()
	lsn, err := db.commandJournal.AppendCommand(commitlog.CommandEnvelope{
		Kind:           commitlog.CommandKindRawKVBatch,
		Scope:          commitlog.CommandScopeRawKV,
		BaseAppliedLSN: baseAppliedLSN,
		PayloadFormat:  commitlog.PayloadFormatRawKVBatchV1,
		Payload:        intent.payload,
	})
	if err != nil {
		return 0, err
	}
	if sync {
		err = db.commandJournal.Sync()
	} else {
		err = db.commandJournal.Flush()
	}
	if err != nil {
		return 0, err
	}
	intent.lsn = lsn
	intent.coveredRange[0] = CommandWALLSNRange{First: lsn, Last: lsn}
	return lsn, nil
}

func commandWALFinalizeOptions(intent *commandWALBatchIntent) finalizeCommitOptions {
	if intent == nil || intent.lsn == 0 {
		return finalizeCommitOptions{}
	}
	if intent.coveredRange[0].First == 0 {
		intent.coveredRange[0] = CommandWALLSNRange{First: intent.lsn, Last: intent.lsn}
	}
	return finalizeCommitOptions{
		commandWALPublish: true,
		appliedCommandLSN: intent.lsn,
		appliedRanges:     intent.coveredRange[:],
	}
}

func applyRawKVCommandWALFrame(db *DB, env commitlog.CommandEnvelope, ridMap map[uint64]page.ValuePtr) error {
	if db == nil {
		return fmt.Errorf("treedb: command wal recovery missing db")
	}
	if env.Kind != commitlog.CommandKindRawKVBatch || env.Scope != commitlog.CommandScopeRawKV || env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		return commitlog.ErrCommandWALUnsupportedKind
	}
	b := db.NewBatch()
	defer func() { _ = b.Close() }()
	ptrBatch, hasPtrBatch := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	opCount := 0
	if err := commitlog.ScanRawKVBatchPayload(env.Payload, func(op commitlog.RawKVOp, key, value []byte) error {
		opCount++
		switch op {
		case commitlog.RawKVOpSet:
			return b.Set(key, value)
		case commitlog.RawKVOpDelete:
			return b.Delete(key)
		case commitlog.RawKVOpSetRID:
			if !hasPtrBatch {
				return fmt.Errorf("treedb: command wal raw kv pointer batch unavailable")
			}
			rid := binary.LittleEndian.Uint64(value)
			ptr, ok := ridMap[rid]
			if !ok {
				return fmt.Errorf("treedb: command wal missing value-log rid %d", rid)
			}
			return ptrBatch.SetPointer(key, ptr)
		default:
			return commitlog.ErrCorrupt
		}
	}); err != nil {
		return err
	}
	if opCount == 0 {
		db.mu.RLock()
		rootID := db.meta.UserRootPageID
		sysRootID := db.meta.SystemRootPageID
		db.mu.RUnlock()
		return db.publishCommandWALRoots(rootID, sysRootID, env.LSN, []CommandWALLSNRange{{First: env.LSN, Last: env.LSN}}, true)
	}
	if raw, ok := b.(*Batch); ok {
		return raw.writeWithCommandWALIntent(true, &commandWALBatchIntent{lsn: env.LSN})
	}
	return errors.New("treedb: command wal recovery batch type mismatch")
}
