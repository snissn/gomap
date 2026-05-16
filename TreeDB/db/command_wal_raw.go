package db

import (
	"errors"
	"fmt"
	"io"
	"os"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type commandWALBatchIntent struct {
	kind               commitlog.CommandKind
	scope              commitlog.CommandScope
	payloadFormat      commitlog.PayloadFormat
	payload            []byte
	externalRefs       bool
	externalRefFileIDs []uint32
	lsn                uint64
	coveredRange       [1]CommandWALLSNRange
}

// CommandWALIntent is an opaque command-WAL append/finalize token used by
// higher-level deterministic command executors such as collections.
type CommandWALIntent struct {
	inner commandWALBatchIntent
}

var ErrCommandWALMissingValueLogRID = errors.New("treedb: command wal missing value-log rid")

func (db *DB) CommandWALEnabled() bool {
	return db != nil && db.commandWAL
}

func (db *DB) NewCommandWALIntent(kind commitlog.CommandKind, scope commitlog.CommandScope, payloadFormat commitlog.PayloadFormat, payload []byte) (*CommandWALIntent, error) {
	if db == nil || !db.commandWAL {
		return nil, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return nil, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	return &CommandWALIntent{inner: commandWALBatchIntent{
		kind:          kind,
		scope:         scope,
		payloadFormat: payloadFormat,
		payload:       payload,
	}}, nil
}

func NewCommandWALReplayIntent(env commitlog.CommandEnvelope) *CommandWALIntent {
	return &CommandWALIntent{inner: commandWALBatchIntent{
		kind:          env.Kind,
		scope:         env.Scope,
		payloadFormat: env.PayloadFormat,
		payload:       env.Payload,
		lsn:           env.LSN,
		coveredRange:  [1]CommandWALLSNRange{{First: env.LSN, Last: env.LSN}},
	}}
}

func (db *DB) prepareRawKVCommandWALIntent(b *Batch) (*commandWALBatchIntent, error) {
	if db == nil || !db.commandWAL {
		return nil, nil
	}
	if db.durability == DurabilityWALOffRelaxed {
		return nil, fmt.Errorf("%w: WAL-off durability is incompatible with command WAL", ErrCommandWALUnsupported)
	}
	if b == nil || b.batch == nil {
		return nil, nil
	}
	// SortedEntries is idempotent after the first sort/compaction; later write
	// paths reuse the sorted+compacted batch-owned slice without resorting.
	entries := b.batch.SortedEntries()
	if len(entries) == 0 {
		return nil, nil
	}
	externalRefs := false
	var ridCache map[page.ValuePtr]uint64
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
				if ridCache == nil {
					ridCache = make(map[page.ValuePtr]uint64)
				}
				rid, err := db.lookupCommandWALValueLogRID(entry.ValuePtr, ridCache)
				if err != nil {
					return nil, err
				}
				ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpSetRID, Key: entry.Key, RID: rid})
				continue
			}
			ops = append(ops, commitlog.RawKVOperation{Op: commitlog.RawKVOpSet, Key: entry.Key, Value: entry.Value})
		default:
			return nil, fmt.Errorf("treedb: command wal unknown raw kv batch op %d", entry.Type)
		}
	}
	payload, err := commitlog.EncodeRawKVBatchPayload(ops)
	if err != nil {
		return nil, err
	}
	var externalRefFileIDs []uint32
	if externalRefs {
		externalRefFileIDs = rawKVCommandWALExternalRefFileIDs(entries)
	}
	return &commandWALBatchIntent{
		kind:               commitlog.CommandKindRawKVBatch,
		scope:              commitlog.CommandScopeRawKV,
		payloadFormat:      commitlog.PayloadFormatRawKVBatchV1,
		payload:            payload,
		externalRefs:       externalRefs,
		externalRefFileIDs: externalRefFileIDs,
	}, nil
}

func rawKVCommandWALExternalRefFileIDs(entries []batchpkg.Entry) []uint32 {
	var ids []uint32
	for i := range entries {
		entry := entries[i]
		if entry.Type != batchpkg.OpPut || !entry.IsPtr || entry.ValuePtr.FileID == 0 {
			continue
		}
		seen := false
		for _, id := range ids {
			if id == entry.ValuePtr.FileID {
				seen = true
				break
			}
		}
		if !seen {
			ids = append(ids, entry.ValuePtr.FileID)
		}
	}
	return ids
}

func (db *DB) lookupCommandWALValueLogRID(ptr page.ValuePtr, ridCache map[page.ValuePtr]uint64) (uint64, error) {
	if db == nil || db.valueLogManager == nil {
		return 0, fmt.Errorf("treedb: command wal raw kv pointer rid reader unavailable (file=%d offset=%d len=%d)", ptr.FileID, ptr.Offset, ptr.Length)
	}
	if ptr.FileID == 0 || ptr.Length == 0 {
		return 0, fmt.Errorf("treedb: command wal raw kv invalid value-log pointer (file=%d offset=%d len=%d)", ptr.FileID, ptr.Offset, ptr.Length)
	}
	if ridCache == nil {
		return 0, fmt.Errorf("treedb: command wal raw kv rid cache unavailable")
	}
	if rid, ok := ridCache[ptr]; ok {
		return rid, nil
	}
	path := db.valueLogManager.SegmentPath(ptr.FileID)
	rid, err := readCommandWALValueLogRIDAt(path, ptr)
	if isCommandWALRIDLookupVisibilityError(err) {
		if flushErr := db.flushCommandWALExternalRefs(false, nil); flushErr != nil {
			return 0, flushErr
		}
		rid, err = readCommandWALValueLogRIDAt(path, ptr)
	}
	if err != nil {
		return 0, err
	}
	ridCache[ptr] = rid
	return rid, nil
}

func readCommandWALValueLogRIDAt(path string, ptr page.ValuePtr) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	return valuelog.ReadRIDAt(f, ptr)
}

func isCommandWALRIDLookupVisibilityError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (db *DB) flushCommandWALExternalRefs(sync bool, fileIDs []uint32) error {
	appender := db.currentValueLogAppender()
	if appender == nil && len(fileIDs) == 0 {
		return ErrValueLogAppenderUnavailable
	}
	var activeFileID uint32
	if appender != nil {
		if _, fileID, ok := appender.CurrentValueLogSegment(); ok {
			activeFileID = fileID
		}
		if sync {
			if err := appender.Sync(); err != nil {
				return err
			}
		} else if err := appender.Flush(); err != nil {
			return err
		}
	}
	if !sync {
		return nil
	}
	for _, fileID := range fileIDs {
		if fileID == 0 || fileID == activeFileID {
			continue
		}
		if err := db.syncCommandWALExternalRefSegment(fileID); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) syncCommandWALExternalRefSegment(fileID uint32) error {
	if db == nil || db.valueLogManager == nil || fileID == 0 {
		return ErrValueLogAppenderUnavailable
	}
	path := db.valueLogManager.SegmentPath(fileID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

func (db *DB) appendRawKVCommandWALIntent(intent *commandWALBatchIntent, sync bool) (uint64, error) {
	return db.appendCommandWALIntent(intent, sync)
}

func (db *DB) appendPublicCommandWALIntent(intent *CommandWALIntent, sync bool) (uint64, error) {
	if intent == nil {
		return 0, nil
	}
	return db.appendCommandWALIntent(&intent.inner, sync)
}

func (db *DB) PublishCommandWALNoop(intent *CommandWALIntent, sync bool) error {
	if intent == nil {
		return nil
	}
	if db == nil {
		return ErrClosed
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.commitMu.Lock()
	if _, err := db.appendPublicCommandWALIntent(intent, sync); err != nil {
		db.commitMu.Unlock()
		return err
	}
	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	post, err := db.finalizeCommitLockedWithOptions(userRoot, systemRoot, nil, sync, adaptive.Metrics{}, nil, false, nil, nil, nil, commandWALFinalizeOptionsForPublicIntent(intent))
	if err != nil {
		db.poisonCommandWALAfterPublicPostAppendFailure(intent)
		db.commitMu.Unlock()
		return err
	}
	db.commitMu.Unlock()
	db.finalizeCommitPostWork(post)
	return nil
}

func (db *DB) appendCommandWALIntent(intent *commandWALBatchIntent, sync bool) (uint64, error) {
	if intent == nil {
		return 0, nil
	}
	if intent.lsn != 0 {
		// The frame was already durably appended. Fail closed if poison was set
		// after the append (e.g. by finalizeCommitLockedWithOptions failing on a
		// subsequent attempt in the same retry loop) so we don't re-enter finalize
		// with a stale LSN on a poisoned handle.
		if db != nil && db.commandWALFlushPoisoned.Load() {
			return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
		}
		return intent.lsn, nil
	}
	if db == nil || db.commandJournal == nil {
		return 0, fmt.Errorf("treedb: command wal journal unavailable")
	}
	if db.commandWALFlushPoisoned.Load() {
		return 0, fmt.Errorf("%w: command wal post-append failure; reopen required", ErrRecoveryRequired)
	}
	if intent.externalRefs {
		// SetRID frames reference value-log positions by offset. The
		// value-log data MUST be durable before the command frame is
		// written, otherwise a power loss could leave the command frame
		// referencing a non-durable RID and cause a hard recovery failure
		// ("missing value-log RID"). For external-ref batches only, always
		// sync regardless of the caller's sync flag.
		if err := db.flushCommandWALExternalRefs(true, intent.externalRefFileIDs); err != nil {
			return 0, err
		}
	}
	db.mu.RLock()
	baseAppliedLSN := db.meta.AppliedCommandLSN
	db.mu.RUnlock()
	lsn, err := db.commandJournal.AppendCommand(commitlog.CommandEnvelope{
		Kind:           intent.kind,
		Scope:          intent.scope,
		BaseAppliedLSN: baseAppliedLSN,
		PayloadFormat:  intent.payloadFormat,
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
	// testFailCommandWALFlush fires after the real Flush/Sync succeeds. The
	// frame is replay-visible, and sync writes are durable; the later error
	// makes the commit result ambiguous, so the open handle must fail closed.
	if err == nil && db.testFailCommandWALFlush.Load() {
		err = errTestCommandWALFlushFailpoint
	}
	if err != nil {
		// AppendCommand already assigned a logical LSN, and the frame may be
		// replayed if the append reached disk. A later flush/sync failure is
		// commit-ambiguous: reopen recovery may apply the frame, so this handle
		// must fail closed instead of allowing a retry to create an LSN gap.
		db.commandWALFlushPoisoned.Store(true)
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
	// Return a copy rather than a slice aliasing the array embedded in intent so
	// that downstream consumers cannot mutate coveredRange[0] through the slice.
	appliedRange := intent.coveredRange[0]
	if appliedRange.First == 0 {
		appliedRange = CommandWALLSNRange{First: intent.lsn, Last: intent.lsn}
	}
	return finalizeCommitOptions{
		commandWALPublish: true,
		appliedCommandLSN: intent.lsn,
		appliedRanges:     []CommandWALLSNRange{appliedRange},
	}
}

func commandWALFinalizeOptionsForPublicIntent(intent *CommandWALIntent) finalizeCommitOptions {
	if intent == nil {
		return finalizeCommitOptions{}
	}
	return commandWALFinalizeOptions(&intent.inner)
}

func (db *DB) poisonCommandWALAfterPostAppendFailure(intent *commandWALBatchIntent) {
	if db == nil || intent == nil || intent.lsn == 0 {
		// intent.lsn == 0 means the frame was never durably appended
		// (appendRawKVCommandWALIntent sets lsn only on success). No need
		// to poison; appendRawKVCommandWALIntent already poisons its own
		// flush/sync failures.
		return
	}
	// appendRawKVCommandWALIntent poisons its own flush/sync failures. This
	// path covers the later case where a command frame was appended but root
	// publication failed before AppliedCommandLSN could be published.
	db.commandWALFlushPoisoned.Store(true)
}

func (db *DB) poisonCommandWALAfterPublicPostAppendFailure(intent *CommandWALIntent) {
	if intent == nil {
		return
	}
	db.poisonCommandWALAfterPostAppendFailure(&intent.inner)
}

func applyRawKVCommandWALFrame(db *DB, env commitlog.CommandEnvelope, ridMap map[uint64]page.ValuePtr, inlineAppender *replayInlineAppender, ensureReplayLogSupport commandWALReplayLogSupportFunc) error {
	if db == nil {
		return fmt.Errorf("treedb: command wal recovery missing db")
	}
	if env.Kind != commitlog.CommandKindRawKVBatch || env.Scope != commitlog.CommandScopeRawKV || env.PayloadFormat != commitlog.PayloadFormatRawKVBatchV1 {
		return commitlog.ErrCommandWALUnsupportedKind
	}
	ops, err := commitlog.DecodeRawKVBatchPayload(env.Payload)
	if err != nil {
		return err
	}
	if len(ops) == 0 {
		// Production empty writes currently return before appending a frame.
		// Empty RawKVBatch frames are still part of the replay contract so
		// fixtures/future command kinds can explicitly advance AppliedCommandLSN
		// without changing roots. This path is only reached during recovery
		// replay; there are no concurrent writers at that point.
		db.mu.RLock()
		rootID := db.meta.UserRootPageID
		sysRootID := db.meta.SystemRootPageID
		db.mu.RUnlock()
		return db.publishCommandWALRoots(rootID, sysRootID, env.LSN, []CommandWALLSNRange{{First: env.LSN, Last: env.LSN}}, true)
	}
	b := db.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	for i := range ops {
		entry := ops[i]
		switch entry.Op {
		case commitlog.RawKVOpSet:
			if err := b.Set(entry.Key, entry.Value); err != nil {
				if !errors.Is(err, batchpkg.ErrValueTooLarge) {
					// Non-ErrValueTooLarge errors abort frame replay entirely.
					// The caller will propagate the error; recovery will retry
					// the full frame on next reopen (at-least-once semantics).
					// Any replay value-log bytes appended by earlier ops in this
					// failed frame are unreachable until normal value-log GC.
					return err
				}
				if inlineAppender == nil {
					if ensureReplayLogSupport == nil {
						return fmt.Errorf("treedb: command wal replay log support unavailable")
					}
					var err error
					// ensureReplayLogSupport owns the replay appender lifecycle;
					// these assignments only cache the frame-local handles.
					ridMap, inlineAppender, err = ensureReplayLogSupport()
					if err != nil {
						return err
					}
				}
				if inlineAppender == nil {
					return fmt.Errorf("treedb: command wal missing replay value-log appender")
				}
				ptr, err := inlineAppender.append(entry.Value)
				if err != nil {
					return err
				}
				if err := b.SetPointer(entry.Key, ptr); err != nil {
					return err
				}
			}
		case commitlog.RawKVOpDelete:
			if err := b.Delete(entry.Key); err != nil {
				return err
			}
		case commitlog.RawKVOpSetRID:
			if ridMap == nil {
				if ensureReplayLogSupport == nil {
					return fmt.Errorf("treedb: command wal replay log support unavailable")
				}
				var err error
				// ensureReplayLogSupport is a closure that updates the outer
				// ridMap and inlineAppender via capture as a side effect.
				// Assigning to the local parameters here caches the handles
				// for subsequent iterations within this frame.
				ridMap, inlineAppender, err = ensureReplayLogSupport()
				if err != nil {
					return err
				}
			}
			ptr, ok := ridMap[entry.RID]
			if !ok {
				return fmt.Errorf("%w: %d", ErrCommandWALMissingValueLogRID, entry.RID)
			}
			if err := b.SetPointer(entry.Key, ptr); err != nil {
				return err
			}
		default:
			return commitlog.ErrCorrupt
		}
	}
	if inlineAppender != nil {
		if err := inlineAppender.syncIfDirty(); err != nil {
			return err
		}
	}
	return b.writeWithCommandWALIntent(true, &commandWALBatchIntent{
		lsn:          env.LSN,
		coveredRange: [1]CommandWALLSNRange{{First: env.LSN, Last: env.LSN}},
	})
}
