package db

import (
	"errors"
	"sync"
	"unsafe"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	conditionalTxnInlineReadSetCap  = 8
	conditionalTxnInlineWriteSetCap = 8
	conditionalTxnInlineKeyArena    = 256
	conditionalTxnReadMapThreshold  = 16
	conditionalTxnWriteMapThreshold = 16
)

// ConditionalTxn is a native TreeDB snapshot transaction with optimistic
// compare-on-commit semantics.
//
// Reads observe the transaction's opening snapshot and record the visible
// per-entry revision. Writes are staged in TreeDB's native batch and commit
// through the ordinary batch/WAL/root-publish pipeline. Commit rejects with
// ErrConcurrentModification when any read key changed after the transaction
// opened, including absent keys that were inserted and deleted again while the
// transaction was active.
type ConditionalTxn struct {
	db             *DB
	snap           *Snapshot
	batch          *Batch
	startCommitSeq uint64

	closed bool

	reads        []conditionalReadPrecondition
	inlineReads  [conditionalTxnInlineReadSetCap]conditionalReadPrecondition
	readIndex    map[string]int
	writes       []conditionalStagedWrite
	inlineWrites [conditionalTxnInlineWriteSetCap]conditionalStagedWrite
	writeIndex   map[string]int
	keyArena     []byte
	inlineKeys   [conditionalTxnInlineKeyArena]byte
}

type conditionalReadPrecondition struct {
	key      []byte
	revision page.EntryRevision
	found    bool
}

type conditionalStagedWrite struct {
	key        []byte
	entryIndex int
}

type conditionalConflictOracle struct {
	mu      sync.Mutex
	history *txnhistory.History
}

// NewConditionalTxn opens a native conditional transaction on the current
// backend snapshot.
func (db *DB) NewConditionalTxn() (*ConditionalTxn, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if db.privateCOWOwner != nil {
		return nil, ErrConditionalTxnUnsupported
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if db.closing.Load() {
		return nil, ErrClosed
	}

	// The standalone compatibility snapshot must include previously
	// acknowledged registered producers. Otherwise this transaction's own
	// sync-time drain would turn those earlier writes into a later root barrier.
	// Drain before taking a snapshot or any transaction-owned publication gate.
	if db.commandWAL {
		unlock, err := db.LockCommandWALPublishWithBarriers()
		if err != nil {
			return nil, err
		}
		unlock()
	} else if err := db.drainNoWALSyncPublishBarriers(); err != nil {
		return nil, err
	}

	db.conditionalActiveTxnCount.Add(1)
	snap := db.AcquireSnapshot()
	if snap == nil {
		db.conditionalActiveTxnCount.Add(-1)
		return nil, ErrClosed
	}
	start := uint64(0)
	if snap.state != nil {
		start = snap.state.CommitSeq
	}

	b, ok := db.newBatchWithReserveHint(0).(*Batch)
	if !ok {
		db.conditionalActiveTxnCount.Add(-1)
		_ = snap.Close()
		return nil, ErrConditionalTxnUnsupported
	}
	db.conditionalTxnStarted.Add(1)
	tx := &ConditionalTxn{
		db:    db,
		snap:  snap,
		batch: b,

		startCommitSeq: start,
	}
	tx.reads = tx.inlineReads[:0]
	tx.writes = tx.inlineWrites[:0]
	tx.keyArena = tx.inlineKeys[:0]
	return tx, nil
}

// Snapshot returns the transaction's pinned opening snapshot. The transaction
// owns the snapshot and closes it on Commit, CommitSync, or Close.
func (tx *ConditionalTxn) Snapshot() *Snapshot {
	if tx == nil || tx.closed {
		return nil
	}
	return tx.snap
}

// ReserveReadSet reserves read-precondition capacity for high-fanout
// transactions. It is optional and does not change transaction semantics.
func (tx *ConditionalTxn) ReserveReadSet(n int) {
	if tx == nil || n <= cap(tx.reads) {
		return
	}
	next := make([]conditionalReadPrecondition, len(tx.reads), n)
	copy(next, tx.reads)
	tx.reads = next
	if n >= conditionalTxnReadMapThreshold {
		tx.ensureReadIndex(n)
	}
}

// ReserveWrites reserves native batch capacity for staged writes.
func (tx *ConditionalTxn) ReserveWrites(n int) {
	if tx == nil || tx.batch == nil || n <= 0 {
		return
	}
	if n > cap(tx.writes) {
		next := make([]conditionalStagedWrite, len(tx.writes), n)
		copy(next, tx.writes)
		tx.writes = next
		if n >= conditionalTxnWriteMapThreshold {
			tx.ensureWriteIndex(n)
		}
	}
	tx.batch.Reserve(n)
}

// Get returns the snapshot value for key and records that key as a commit
// precondition. Missing keys return nil, nil.
func (tx *ConditionalTxn) Get(key []byte) ([]byte, error) {
	value, _, err := tx.GetVersioned(key)
	return value, err
}

// GetVersioned returns the snapshot value and native entry revision for key
// and records that key as a commit precondition. Missing keys return nil, the
// visible tombstone revision when one exists, and nil error.
func (tx *ConditionalTxn) GetVersioned(key []byte) ([]byte, page.EntryRevision, error) {
	out, revision, err := tx.GetVersionedAppend(key, nil)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, revision, nil
	}
	if err != nil {
		return nil, revision, err
	}
	if len(out) == 0 {
		return []byte{}, revision, nil
	}
	if cap(out) == len(out) {
		return out, revision, nil
	}
	owned := make([]byte, len(out))
	copy(owned, out)
	return owned, revision, nil
}

// GetVersionedAppend appends the snapshot value for key to dst, returns the
// native entry revision, and records key as a commit precondition. Missing keys
// return dst, the visible tombstone revision when one exists, and
// tree.ErrKeyNotFound.
func (tx *ConditionalTxn) GetVersionedAppend(key, dst []byte) ([]byte, page.EntryRevision, error) {
	if err := tx.ensureOpen(); err != nil {
		return dst, page.LegacyEntryRevision, err
	}
	key = normalizeRawKVPointKey(key)
	if staged, ok := tx.findStagedWrite(key); ok {
		switch staged.Type {
		case batchpkg.OpPut:
			if staged.IsPtr {
				return dst, staged.Revision, batchpkg.ErrMissingValueReader
			}
			return append(dst, staged.Value...), staged.Revision, nil
		case batchpkg.OpDelete:
			return dst, staged.Revision, tree.ErrKeyNotFound
		}
	}
	out, revision, err := tx.snap.GetVersionedAppend(key, dst)
	if errors.Is(err, tree.ErrKeyNotFound) {
		if recErr := tx.recordRead(key, revision, false); recErr != nil {
			return dst, revision, recErr
		}
		return dst, revision, err
	}
	if err != nil {
		return dst, revision, err
	}
	if recErr := tx.recordRead(key, revision, true); recErr != nil {
		return dst, revision, recErr
	}
	return out, revision, nil
}

// RequireReadVersion records a caller-observed key revision as a commit
// precondition. It is intended for values read through an external pinned
// snapshot owned by the caller.
func (tx *ConditionalTxn) RequireReadVersion(key []byte, revision page.EntryRevision, found bool) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	if err := tx.validateCurrentReadVersion(key, revision, found); err != nil {
		return err
	}
	return tx.recordRead(key, revision, found)
}

// RecordReadVersion records an opening-snapshot read precondition without
// validating current state. It is for transaction-owned snapshot reads that
// should keep returning the pinned view and defer conflicts until commit.
func (tx *ConditionalTxn) RecordReadVersion(key []byte, revision page.EntryRevision, found bool) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	key = normalizeRawKVPointKey(key)
	return tx.recordRead(key, revision, found)
}

func (tx *ConditionalTxn) validateCurrentReadVersion(key []byte, revision page.EntryRevision, found bool) error {
	currentRevision, currentFound, err := tx.currentReadRevision(key)
	if err != nil {
		return err
	}
	return tx.validateCurrentReadVersionMatch(key, revision, found, currentRevision, currentFound)
}

func (tx *ConditionalTxn) currentReadRevision(key []byte) (page.EntryRevision, bool, error) {
	snap := tx.db.AcquireSnapshot()
	if snap == nil {
		return page.LegacyEntryRevision, false, ErrClosed
	}
	defer snap.Close()
	entry, err := snap.GetEntry(key)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return page.LegacyEntryRevision, false, nil
	}
	if err != nil {
		return page.LegacyEntryRevision, false, err
	}
	return entry.Revision, entry.Flags&node.FlagTombstone == 0, nil
}

func (tx *ConditionalTxn) validateCurrentReadVersionMatch(key []byte, wantRevision page.EntryRevision, wantFound bool, gotRevision page.EntryRevision, gotFound bool) error {
	if wantFound == gotFound && wantRevision == gotRevision {
		return nil
	}
	if tx.db.conditionalReadKeyChangedSince(tx.startCommitSeq, key) {
		return nil
	}
	return ErrConcurrentModification
}

// Has reports whether key exists in the opening snapshot and records the key as
// a commit precondition.
func (tx *ConditionalTxn) Has(key []byte) (bool, error) {
	_, _, err := tx.GetVersionedAppend(key, nil)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Set stages a snapshot-conditional put in the native TreeDB batch.
func (tx *ConditionalTxn) Set(key, value []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.Set(key, value); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// SetWithRevision stages a put with an explicit native entry revision.
func (tx *ConditionalTxn) SetWithRevision(key, value []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.SetWithRevision(key, value, revision); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// SetView stages a put without copying key/value bytes. The caller must keep
// both slices immutable until Commit, CommitSync, or Close returns.
func (tx *ConditionalTxn) SetView(key, value []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.SetView(key, value); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// SetViewWithRevision is SetView with an explicit native entry revision.
func (tx *ConditionalTxn) SetViewWithRevision(key, value []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.SetViewWithRevision(key, value, revision); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// Delete stages a snapshot-conditional tombstone in the native TreeDB batch.
func (tx *ConditionalTxn) Delete(key []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.Delete(key); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// DeleteWithRevision stages a tombstone with an explicit native entry revision.
func (tx *ConditionalTxn) DeleteWithRevision(key []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.DeleteWithRevision(key, revision); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// DeleteView stages a tombstone without copying key bytes. The caller must keep
// key immutable until Commit, CommitSync, or Close returns.
func (tx *ConditionalTxn) DeleteView(key []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.DeleteView(key); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// DeleteViewWithRevision is DeleteView with an explicit native entry revision.
func (tx *ConditionalTxn) DeleteViewWithRevision(key []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	entryIndex := tx.batch.batch.Len()
	if err := tx.batch.DeleteViewWithRevision(key, revision); err != nil {
		return err
	}
	tx.recordStagedWriteAt(entryIndex)
	return nil
}

// Commit validates recorded reads and publishes staged writes without forcing a
// sync durability boundary.
func (tx *ConditionalTxn) Commit() error {
	return tx.commit(false)
}

// CommitSync validates recorded reads and publishes staged writes with a sync
// durability boundary.
func (tx *ConditionalTxn) CommitSync() error {
	return tx.commit(true)
}

// Close releases the transaction without publishing staged writes.
func (tx *ConditionalTxn) Close() error {
	return tx.finish()
}

func (tx *ConditionalTxn) commit(sync bool) (err error) {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	defer func() {
		tx.db.observeConditionalTxnCommit(len(tx.reads), err)
		if closeErr := tx.finish(); err == nil {
			err = closeErr
		}
	}()
	if tx.batch == nil || tx.batch.batch == nil || tx.batch.batch.Len() == 0 {
		if sync && !tx.db.commandWAL {
			if err := tx.db.Checkpoint(); err != nil {
				return err
			}
		}
		return tx.validateReadSetOnly()
	}
	return tx.batch.writeConditional(sync, tx)
}

func (tx *ConditionalTxn) validateReadSetOnly() error {
	if len(tx.reads) == 0 {
		return nil
	}
	if hook := tx.db.testConditionalReadOnlyAfterClosePreflight; hook != nil {
		hook()
	}
	tx.db.writeMu.RLock()
	defer tx.db.writeMu.RUnlock()
	if err := tx.db.checkReadAdmissionLocked(); err != nil {
		return err
	}
	tx.db.commitMu.Lock()
	err := tx.validateReadSetAtPublish()
	tx.db.commitMu.Unlock()
	return err
}

func (tx *ConditionalTxn) validateReadSetAtPublish() error {
	if err := tx.db.conditionalHistoryError(tx.startCommitSeq); err != nil {
		return err
	}
	if tx.db.conditionalReadSetChangedSince(tx.startCommitSeq, tx.reads) {
		return ErrConcurrentModification
	}
	if tx.batch != nil {
		for _, e := range tx.batch.batch.OrderedEntries() {
			if tx.db.conditionalReadKeyChangedSince(tx.startCommitSeq, e.Key) {
				return ErrConcurrentModification
			}
		}
	}
	return nil
}

func (tx *ConditionalTxn) ensureOpen() error {
	if tx == nil || tx.closed || tx.db == nil || tx.snap == nil || tx.batch == nil {
		return ErrConditionalTxnClosed
	}
	if tx.db.closing.Load() {
		return ErrClosed
	}
	return nil
}

func (tx *ConditionalTxn) finish() error {
	if tx == nil || tx.closed {
		return nil
	}
	tx.closed = true
	db := tx.db
	var err error
	if tx.snap != nil {
		err = tx.snap.Close()
		tx.snap = nil
	}
	if tx.batch != nil {
		if closeErr := tx.batch.Close(); err == nil {
			err = closeErr
		}
		tx.batch = nil
	}
	if db != nil {
		db.conditionalActiveTxnCount.Add(-1)
		db.conditionalTxnClosed.Add(1)
	}
	return err
}

func (tx *ConditionalTxn) recordStagedWriteAt(entryIndex int) {
	if tx == nil || tx.batch == nil || tx.batch.batch == nil {
		return
	}
	entries := tx.batch.batch.OrderedEntries()
	if entryIndex < 0 || entryIndex >= len(entries) {
		return
	}
	entry := entries[entryIndex]
	switch entry.Type {
	case batchpkg.OpPut, batchpkg.OpDelete:
		tx.recordStagedPointWrite(entry.Key, entryIndex)
	}
}

func (tx *ConditionalTxn) recordStagedPointWrite(key []byte, entryIndex int) {
	if tx == nil {
		return
	}
	if tx.writeIndex != nil {
		mapKey := conditionalBytesString(key)
		if idx, ok := tx.writeIndex[mapKey]; ok {
			tx.writes[idx].key = key
			tx.writes[idx].entryIndex = entryIndex
			return
		}
		tx.writeIndex[mapKey] = len(tx.writes)
		tx.writes = append(tx.writes, conditionalStagedWrite{key: key, entryIndex: entryIndex})
		return
	}
	for i := len(tx.writes) - 1; i >= 0; i-- {
		if bytesEqual(tx.writes[i].key, key) {
			tx.writes[i].key = key
			tx.writes[i].entryIndex = entryIndex
			return
		}
	}
	tx.writes = append(tx.writes, conditionalStagedWrite{key: key, entryIndex: entryIndex})
	if len(tx.writes) >= conditionalTxnWriteMapThreshold {
		tx.ensureWriteIndex(len(tx.writes) * 2)
	}
}

func (tx *ConditionalTxn) findStagedWrite(key []byte) (batchpkg.Entry, bool) {
	if tx == nil || tx.batch == nil || tx.batch.batch == nil || len(tx.writes) == 0 {
		return batchpkg.Entry{}, false
	}
	var staged conditionalStagedWrite
	if tx.writeIndex != nil {
		idx, ok := tx.writeIndex[conditionalBytesString(key)]
		if !ok {
			return batchpkg.Entry{}, false
		}
		staged = tx.writes[idx]
	} else {
		found := false
		for i := len(tx.writes) - 1; i >= 0; i-- {
			if bytesEqual(tx.writes[i].key, key) {
				staged = tx.writes[i]
				found = true
				break
			}
		}
		if !found {
			return batchpkg.Entry{}, false
		}
	}
	entries := tx.batch.batch.OrderedEntries()
	if staged.entryIndex < 0 || staged.entryIndex >= len(entries) {
		return batchpkg.Entry{}, false
	}
	entry := entries[staged.entryIndex]
	if !bytesEqual(entry.Key, key) {
		return batchpkg.Entry{}, false
	}
	switch entry.Type {
	case batchpkg.OpPut, batchpkg.OpDelete:
		return entry, true
	default:
		return batchpkg.Entry{}, false
	}
}

func (tx *ConditionalTxn) recordRead(key []byte, revision page.EntryRevision, found bool) error {
	if idx, ok := tx.findRead(key); ok {
		prev := tx.reads[idx]
		if prev.found != found || prev.revision != revision {
			return ErrConcurrentModification
		}
		return nil
	}
	start := len(tx.keyArena)
	tx.keyArena = append(tx.keyArena, key...)
	owned := tx.keyArena[start:]
	idx := len(tx.reads)
	tx.reads = append(tx.reads, conditionalReadPrecondition{
		key:      owned,
		revision: revision,
		found:    found,
	})
	if tx.readIndex != nil {
		tx.readIndex[conditionalBytesString(owned)] = idx
	} else if len(tx.reads) >= conditionalTxnReadMapThreshold {
		tx.ensureReadIndex(len(tx.reads) * 2)
		tx.readIndex[conditionalBytesString(owned)] = idx
	}
	return nil
}

func (tx *ConditionalTxn) findRead(key []byte) (int, bool) {
	if tx.readIndex != nil {
		idx, ok := tx.readIndex[conditionalBytesString(key)]
		return idx, ok
	}
	for i := range tx.reads {
		if bytesEqual(tx.reads[i].key, key) {
			return i, true
		}
	}
	return 0, false
}

func (tx *ConditionalTxn) ensureReadIndex(capHint int) {
	if tx.readIndex != nil {
		return
	}
	if capHint < len(tx.reads) {
		capHint = len(tx.reads)
	}
	tx.readIndex = make(map[string]int, capHint)
	for i := range tx.reads {
		tx.readIndex[conditionalBytesString(tx.reads[i].key)] = i
	}
}

func (tx *ConditionalTxn) ensureWriteIndex(capHint int) {
	if tx.writeIndex != nil {
		return
	}
	if capHint < len(tx.writes) {
		capHint = len(tx.writes)
	}
	tx.writeIndex = make(map[string]int, capHint)
	for i := range tx.writes {
		tx.writeIndex[conditionalBytesString(tx.writes[i].key)] = i
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (db *DB) conditionalHistoryLocked() *txnhistory.History {
	if db.conditionalOracle.history == nil {
		db.conditionalOracle.history, _ = txnhistory.New(4096, 4<<20, nil)
	}
	return db.conditionalOracle.history
}
func (db *DB) conditionalRecordCommittedEntries(entries []batchpkg.Entry, ranges []batchpkg.DeleteRange, commitSeq uint64) {
	if db == nil || db.privateCOWOwner != nil || (len(entries) == 0 && len(ranges) == 0) {
		return
	}
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	j := db.conditionalHistoryLocked()
	p, err := j.Prepare(entries, ranges, false)
	if err != nil {
		panic(err)
	}
	j.Install(p, commitSeq)
	db.conditionalOracleRecordedPoints.Add(uint64(len(entries)))
	db.conditionalOracleRecordedRanges.Add(uint64(len(ranges)))
}
func (db *DB) conditionalRecordRootCommit(oldRootID, newRootID, commitSeq uint64) {
	if db == nil || db.privateCOWOwner != nil || oldRootID == newRootID || commitSeq == 0 {
		return
	}
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	j := db.conditionalHistoryLocked()
	p, err := j.Prepare(nil, nil, true)
	if err != nil {
		panic(err)
	}
	j.Install(p, commitSeq)
	db.conditionalOracleRootMarkers.Add(1)
}

func (db *DB) observeConditionalTxnCommit(readSetLen int, err error) {
	if db == nil {
		return
	}
	db.conditionalTxnCommitAttempts.Add(1)
	db.conditionalTxnReadSetSamples.Add(1)
	addIntMetric(&db.conditionalTxnReadSetEntries, readSetLen)
	storeUint64Max(&db.conditionalTxnReadSetMax, uint64(readSetLen))
	if errors.Is(err, ErrConcurrentModification) {
		db.conditionalTxnConflicts.Add(1)
		return
	}
	if err == nil {
		db.conditionalTxnCommits.Add(1)
	}
}

func (db *DB) conditionalHistoryError(start uint64) error {
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	if db.conditionalOracle.history == nil {
		return nil
	}
	if err := db.conditionalOracle.history.Check(start); err != nil {
		return err
	}
	if db.conditionalOracle.history.BarrierConflict(start) {
		return ErrConcurrentModification
	}
	return nil
}
func (db *DB) conditionalReadSetChangedSince(start uint64, reads []conditionalReadPrecondition) bool {
	if db == nil {
		return false
	}
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	for _, r := range reads {
		if db.conditionalOracle.history.PointChanged(start, r.key) {
			return true
		}
	}
	return false
}
func (db *DB) conditionalReadKeyChangedSince(start uint64, key []byte) bool {
	if db == nil {
		return false
	}
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	return db.conditionalOracle.history.PointChanged(start, key)
}

func conditionalBytesString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}
