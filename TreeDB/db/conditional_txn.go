package db

import (
	"errors"
	"sync"
	"unsafe"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
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
	id             uint64
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
	nextID  uint64
	active  map[uint64]uint64
	recent  map[string]uint64
	ranges  []conditionalRecentRange
	rootSeq uint64
}

type conditionalRecentRange struct {
	start string
	end   string
	seq   uint64
}

// NewConditionalTxn opens a native conditional transaction on the current
// backend snapshot.
func (db *DB) NewConditionalTxn() (*ConditionalTxn, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	if db.closing.Load() {
		return nil, ErrClosed
	}

	id := db.conditionalRegisterTxn()
	snap := db.AcquireSnapshot()
	if snap == nil {
		db.conditionalUnregisterTxn(id)
		return nil, ErrClosed
	}
	start := uint64(0)
	if snap.state != nil {
		start = snap.state.CommitSeq
	}
	db.conditionalSetTxnStart(id, start)

	b, ok := db.newBatchWithReserveHint(0).(*Batch)
	if !ok {
		db.conditionalUnregisterTxn(id)
		_ = snap.Close()
		return nil, ErrConditionalTxnUnsupported
	}
	db.conditionalTxnStarted.Add(1)
	tx := &ConditionalTxn{
		db:             db,
		snap:           snap,
		batch:          b,
		id:             id,
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
		return tx.validateReadSetOnly()
	}
	tx.batch.conditionalTxnID = tx.id
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
	if len(tx.reads) == 0 {
		return nil
	}
	if tx.db.conditionalReadSetChangedSince(tx.startCommitSeq, tx.reads) {
		return ErrConcurrentModification
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
	id := tx.id
	tx.id = 0
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
	if db != nil && id != 0 {
		db.conditionalUnregisterTxn(id)
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

func (db *DB) conditionalRegisterTxn() uint64 {
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	db.conditionalOracle.nextID++
	id := db.conditionalOracle.nextID
	if db.conditionalOracle.active == nil {
		db.conditionalOracle.active = make(map[uint64]uint64)
	}
	db.conditionalOracle.active[id] = 0
	db.conditionalActiveTxnCount.Add(1)
	return id
}

func (db *DB) conditionalSetTxnStart(id uint64, start uint64) {
	if db == nil || id == 0 {
		return
	}
	db.conditionalOracle.mu.Lock()
	if _, ok := db.conditionalOracle.active[id]; ok {
		db.conditionalOracle.active[id] = start
	}
	db.conditionalPruneOracleLocked()
	db.conditionalOracle.mu.Unlock()
}

func (db *DB) conditionalUnregisterTxn(id uint64) {
	if db == nil || id == 0 {
		return
	}
	db.conditionalOracle.mu.Lock()
	if _, ok := db.conditionalOracle.active[id]; ok {
		delete(db.conditionalOracle.active, id)
		db.conditionalActiveTxnCount.Add(-1)
	}
	db.conditionalPruneOracleLocked()
	db.conditionalOracle.mu.Unlock()
}

func (db *DB) conditionalRecordCommittedEntries(entries []batchpkg.Entry, ranges []batchpkg.DeleteRange, ownerTxnID uint64, commitSeq uint64) {
	if db == nil || db.conditionalActiveTxnCount.Load() <= 0 || (len(entries) == 0 && len(ranges) == 0) {
		return
	}
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	if len(db.conditionalOracle.active) == 0 {
		return
	}
	if ownerTxnID != 0 && len(db.conditionalOracle.active) == 1 {
		if _, ok := db.conditionalOracle.active[ownerTxnID]; ok {
			return
		}
	}
	if len(entries) > 0 && db.conditionalOracle.recent == nil {
		db.conditionalOracle.recent = make(map[string]uint64, len(entries))
	}
	pointCount := 0
	for i := range entries {
		switch entries[i].Type {
		case batchpkg.OpPut, batchpkg.OpDelete:
			db.conditionalOracle.recent[string(entries[i].Key)] = commitSeq
			pointCount++
		}
	}
	for i := range ranges {
		db.conditionalOracle.ranges = append(db.conditionalOracle.ranges, conditionalRecentRange{
			start: string(ranges[i].Start),
			end:   string(ranges[i].End),
			seq:   commitSeq,
		})
	}
	if pointCount > 0 {
		db.conditionalOracleRecordedPoints.Add(uint64(pointCount))
	}
	if len(ranges) > 0 {
		db.conditionalOracleRecordedRanges.Add(uint64(len(ranges)))
	}
}

func (db *DB) conditionalRecordRootCommit(oldRootID uint64, newRootID uint64, commitSeq uint64) {
	if db == nil || oldRootID == newRootID || commitSeq == 0 || db.conditionalActiveTxnCount.Load() <= 0 {
		return
	}
	db.conditionalOracle.mu.Lock()
	if len(db.conditionalOracle.active) > 0 && commitSeq > db.conditionalOracle.rootSeq {
		db.conditionalOracle.rootSeq = commitSeq
		db.conditionalOracleRootMarkers.Add(1)
	}
	db.conditionalOracle.mu.Unlock()
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

func (db *DB) conditionalReadSetChangedSince(start uint64, reads []conditionalReadPrecondition) bool {
	if db == nil || len(reads) == 0 {
		return false
	}
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	if db.conditionalOracle.rootSeq > start {
		return true
	}
	for i := range reads {
		if db.conditionalOracle.recent[conditionalBytesString(reads[i].key)] > start {
			return true
		}
		for j := range db.conditionalOracle.ranges {
			r := db.conditionalOracle.ranges[j]
			if r.seq > start && conditionalKeyInStringRange(reads[i].key, r.start, r.end) {
				return true
			}
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
	if db.conditionalOracle.rootSeq > start {
		return true
	}
	if db.conditionalOracle.recent[conditionalBytesString(key)] > start {
		return true
	}
	for i := range db.conditionalOracle.ranges {
		r := db.conditionalOracle.ranges[i]
		if r.seq > start && conditionalKeyInStringRange(key, r.start, r.end) {
			return true
		}
	}
	return false
}

func (db *DB) conditionalPruneOracleLocked() {
	db.conditionalOraclePrunes.Add(1)
	pointsBefore := len(db.conditionalOracle.recent)
	rangesBefore := len(db.conditionalOracle.ranges)
	if len(db.conditionalOracle.active) == 0 {
		clear(db.conditionalOracle.recent)
		clear(db.conditionalOracle.ranges)
		db.conditionalOracle.ranges = db.conditionalOracle.ranges[:0]
		db.conditionalOracle.rootSeq = 0
		addIntMetric(&db.conditionalOraclePrunedPoints, pointsBefore)
		addIntMetric(&db.conditionalOraclePrunedRanges, rangesBefore)
		return
	}
	oldest := uint64(^uint64(0))
	for _, start := range db.conditionalOracle.active {
		if start < oldest {
			oldest = start
		}
	}
	for key, seq := range db.conditionalOracle.recent {
		if seq <= oldest {
			delete(db.conditionalOracle.recent, key)
		}
	}
	kept := db.conditionalOracle.ranges[:0]
	for i := range db.conditionalOracle.ranges {
		if db.conditionalOracle.ranges[i].seq > oldest {
			kept = append(kept, db.conditionalOracle.ranges[i])
		}
	}
	clear(db.conditionalOracle.ranges[len(kept):])
	db.conditionalOracle.ranges = kept
	if db.conditionalOracle.rootSeq <= oldest {
		db.conditionalOracle.rootSeq = 0
	}
	addIntMetric(&db.conditionalOraclePrunedPoints, pointsBefore-len(db.conditionalOracle.recent))
	addIntMetric(&db.conditionalOraclePrunedRanges, rangesBefore-len(db.conditionalOracle.ranges))
}

func conditionalBytesString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func conditionalKeyInStringRange(key []byte, start, end string) bool {
	return (len(start) == 0 || compareBytesString(key, start) >= 0) &&
		(len(end) == 0 || compareBytesString(key, end) < 0)
}

func compareBytesString(key []byte, bound string) int {
	n := len(key)
	if len(bound) < n {
		n = len(bound)
	}
	for i := 0; i < n; i++ {
		if key[i] < bound[i] {
			return -1
		}
		if key[i] > bound[i] {
			return 1
		}
	}
	switch {
	case len(key) < len(bound):
		return -1
	case len(key) > len(bound):
		return 1
	default:
		return 0
	}
}
