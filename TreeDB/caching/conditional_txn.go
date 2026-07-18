package caching

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

const (
	conditionalTxnInlineReadSetCap = 16
	conditionalTxnInlineKeyArena   = 256
	conditionalTxnReadMapThreshold = 16
)

// ConditionalTxn is a cached TreeDB point-read transaction with optimistic
// compare-on-commit semantics.
//
// Reads record the currently visible per-entry revision. If a read key changed
// after the transaction opened, the read or commit fails with
// ErrConcurrentModification instead of observing post-open state. Writes are
// staged in the ordinary cached batch and commit through the normal
// memtable/journal path after validation under the exclusive writer gate.
type ConditionalTxn struct {
	db       *DB
	batch    *Batch
	snap     *Snapshot
	id       uint64
	startSeq uint64

	closed bool

	reads       []conditionalReadPrecondition
	inlineReads [conditionalTxnInlineReadSetCap]conditionalReadPrecondition
	readIndex   map[string]int
	keyArena    []byte
	inlineKeys  [conditionalTxnInlineKeyArena]byte
}

type conditionalReadPrecondition struct {
	key      []byte
	revision page.EntryRevision
	found    bool
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

// NewConditionalTxn opens a cached conditional transaction.
func (db *DB) NewConditionalTxn() (*ConditionalTxn, error) {
	tx := new(ConditionalTxn)
	if err := db.InitConditionalTxn(tx); err != nil {
		return nil, err
	}
	return tx, nil
}

// InitConditionalTxn initializes tx as a cached conditional transaction.
// Callers must pass zero-value or closed transaction storage.
func (db *DB) InitConditionalTxn(tx *ConditionalTxn) error {
	return db.initConditionalTxn(tx, false)
}

// InitConditionalTxnWithSnapshot initializes tx and pins the opening snapshot.
// Point reads use that snapshot. Public callers must not rely on range scans as
// commit preconditions until native conditional range guards are supported. The
// snapshot is owned by the transaction and closed by Commit, CommitSync, or
// Close.
func (db *DB) InitConditionalTxnWithSnapshot(tx *ConditionalTxn) error {
	return db.initConditionalTxn(tx, true)
}

func (db *DB) initConditionalTxn(tx *ConditionalTxn, withSnapshot bool) error {
	if tx == nil {
		return backenddb.ErrConditionalTxnClosed
	}
	if tx.id != 0 || (!tx.closed && (tx.db != nil || tx.batch != nil)) {
		return backenddb.ErrConditionalTxnClosed
	}
	if db == nil || db.backend == nil {
		return backenddb.ErrClosed
	}
	if err := db.beginExclusiveWrite(); err != nil {
		return err
	}
	defer db.writeMu.Unlock()
	if db.closing.Load() {
		return backenddb.ErrClosed
	}

	id := db.conditionalRegisterTxn()
	start := db.conditionalSeq.Load()
	db.conditionalSetTxnStart(id, start)

	var snap *Snapshot
	if withSnapshot {
		snap = db.AcquireSnapshot()
		if snap == nil {
			db.conditionalUnregisterTxn(id)
			return backenddb.ErrClosed
		}
	}

	*tx = ConditionalTxn{
		db:       db,
		batch:    db.NewBatch(),
		snap:     snap,
		id:       id,
		startSeq: start,
	}
	tx.reads = tx.inlineReads[:0]
	tx.keyArena = tx.inlineKeys[:0]
	db.conditionalTxnStarted.Add(1)
	return nil
}

// Snapshot returns the transaction's pinned opening snapshot when this
// transaction was initialized with InitConditionalTxnWithSnapshot. The
// transaction owns the snapshot and closes it on Commit, CommitSync, or Close.
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
	tx.batch.Reserve(n)
}

// Get returns the transaction-visible value for key and records that key as a
// commit precondition. Missing keys return nil, nil.
func (tx *ConditionalTxn) Get(key []byte) ([]byte, error) {
	value, _, err := tx.GetVersioned(key)
	return value, err
}

// GetVersioned returns the transaction-visible value and native entry revision
// for key and records that key as a commit precondition.
func (tx *ConditionalTxn) GetVersioned(key []byte) ([]byte, page.EntryRevision, error) {
	scratch := getOwnedReadScratch()
	defer putOwnedReadScratch(scratch)

	out, revision, err := tx.GetVersionedAppend(key, scratch.buf[:0])
	if errors.Is(err, tree.ErrKeyNotFound) {
		return nil, revision, nil
	}
	if err != nil {
		return nil, revision, err
	}
	if len(out) == 0 {
		return []byte{}, revision, nil
	}
	return ownedReadResult(out, scratch), revision, nil
}

// GetVersionedAppend appends the transaction-visible value for key to dst,
// returns the native entry revision, and records key as a commit precondition.
func (tx *ConditionalTxn) GetVersionedAppend(key, dst []byte) ([]byte, page.EntryRevision, error) {
	if err := tx.ensureOpen(); err != nil {
		return dst, page.LegacyEntryRevision, err
	}
	key = normalizeRawKVPointKey(key)
	if out, revision, staged, deleted, err := tx.stagedReadAppend(key, dst); staged {
		if err != nil {
			return dst, revision, err
		}
		if deleted {
			return dst, revision, tree.ErrKeyNotFound
		}
		return out, revision, nil
	}
	out, revision, err := tx.getCommittedVersionedAppend(key, dst)
	if errors.Is(err, tree.ErrKeyNotFound) {
		if recErr := tx.recordRead(key, revision, false); recErr != nil {
			return dst, revision, recErr
		}
		if tx.snap == nil && tx.db.conditionalReadKeyChangedSince(tx.startSeq, key) {
			return dst, revision, backenddb.ErrConcurrentModification
		}
		return dst, revision, err
	}
	if err != nil {
		return dst, revision, err
	}
	if recErr := tx.recordRead(key, revision, true); recErr != nil {
		return dst, revision, recErr
	}
	if tx.snap == nil && tx.db.conditionalReadKeyChangedSince(tx.startSeq, key) {
		return dst, revision, backenddb.ErrConcurrentModification
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
		return page.LegacyEntryRevision, false, backenddb.ErrClosed
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
	if tx.db.conditionalReadKeyChangedSince(tx.startSeq, key) {
		return nil
	}
	return backenddb.ErrConcurrentModification
}

func (tx *ConditionalTxn) getCommittedVersionedAppend(key, dst []byte) ([]byte, page.EntryRevision, error) {
	if tx.snap != nil {
		return tx.snap.GetVersionedAppend(key, dst)
	}
	return tx.db.GetVersionedAppend(key, dst)
}

func (tx *ConditionalTxn) stagedReadAppend(key, dst []byte) ([]byte, page.EntryRevision, bool, bool, error) {
	if tx == nil || tx.batch == nil || len(tx.batch.entries) == 0 {
		return dst, page.LegacyEntryRevision, false, false, nil
	}
	for i := len(tx.batch.entries) - 1; i >= 0; i-- {
		entry := tx.batch.entries[i]
		switch entry.Type {
		case batch.OpPut:
			if !bytesEqual(entry.Key, key) {
				continue
			}
			if entry.IsPtr {
				out, err := tx.db.readValueLogAppend(entry.Key, entry.ValuePtr, dst)
				return out, entry.Revision, true, false, err
			}
			return append(dst, entry.Value...), entry.Revision, true, false, nil
		case batch.OpDelete:
			if bytesEqual(entry.Key, key) {
				return dst, entry.Revision, true, true, nil
			}
		case batch.OpDeleteRange:
			if batch.DeleteRangeContainsKey(batch.DeleteRange{Start: entry.Key, End: entry.Value}, key) {
				return dst, entry.Revision, true, true, nil
			}
		}
	}
	return dst, page.LegacyEntryRevision, false, false, nil
}

// Has reports whether key exists in the transaction-visible state and records
// the key as a commit precondition.
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

// Set stages a conditional put in the cached TreeDB batch.
func (tx *ConditionalTxn) Set(key, value []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.Set(key, value)
}

// SetWithRevision stages a put with an explicit native entry revision.
func (tx *ConditionalTxn) SetWithRevision(key, value []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.SetWithRevision(key, value, revision)
}

// SetView stages a put without copying key/value bytes. The caller must keep
// both slices immutable until Commit, CommitSync, or Close returns.
func (tx *ConditionalTxn) SetView(key, value []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.SetView(key, value)
}

// SetViewWithRevision is SetView with an explicit native entry revision.
func (tx *ConditionalTxn) SetViewWithRevision(key, value []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.SetViewWithRevision(key, value, revision)
}

// Delete stages a conditional tombstone in the cached TreeDB batch.
func (tx *ConditionalTxn) Delete(key []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.Delete(key)
}

// DeleteWithRevision stages a tombstone with an explicit native entry revision.
func (tx *ConditionalTxn) DeleteWithRevision(key []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.DeleteWithRevision(key, revision)
}

// DeleteView stages a tombstone without copying key bytes. The caller must keep
// key immutable until Commit, CommitSync, or Close returns.
func (tx *ConditionalTxn) DeleteView(key []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.DeleteView(key)
}

// DeleteViewWithRevision is DeleteView with an explicit native entry revision.
func (tx *ConditionalTxn) DeleteViewWithRevision(key []byte, revision page.EntryRevision) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.DeleteViewWithRevision(key, revision)
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

	if err := tx.db.beginExclusiveWrite(); err != nil {
		return err
	}
	writeMuHeld := true
	unlockWriteMu := func() {
		if writeMuHeld {
			writeMuHeld = false
			tx.db.writeMu.Unlock()
		}
	}
	defer unlockWriteMu()

	if err := tx.validateReadSetAtPublish(); err != nil {
		return err
	}
	if tx.batch == nil || len(tx.batch.entries) == 0 {
		return nil
	}
	tx.batch.streamBypassOff = true
	tx.batch.conditionalTxnID = tx.id
	return tx.batch.writeRegularLocked(sync, unlockWriteMu)
}

func (tx *ConditionalTxn) validateReadSetAtPublish() error {
	if len(tx.reads) == 0 {
		return nil
	}
	if tx.db.conditionalReadSetChangedSince(tx.startSeq, tx.reads) {
		return backenddb.ErrConcurrentModification
	}
	return nil
}

func (tx *ConditionalTxn) ensureOpen() error {
	if tx == nil || tx.closed || tx.db == nil || tx.batch == nil {
		return backenddb.ErrConditionalTxnClosed
	}
	if tx.db.closing.Load() {
		return backenddb.ErrClosed
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
		err = errors.Join(err, tx.snap.Close())
		tx.snap = nil
	}
	if tx.batch != nil {
		err = errors.Join(err, tx.batch.Close())
		tx.batch = nil
	}
	if db != nil && id != 0 {
		db.conditionalUnregisterTxn(id)
		db.conditionalTxnClosed.Add(1)
	}
	return err
}

func (tx *ConditionalTxn) recordRead(key []byte, revision page.EntryRevision, found bool) error {
	if idx, ok := tx.findRead(key); ok {
		prev := tx.reads[idx]
		if prev.found != found || prev.revision != revision {
			return backenddb.ErrConcurrentModification
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

func (db *DB) conditionalRecordCommittedEntries(entries []batch.Entry, ranges []batch.DeleteRange, ownerTxnID uint64) {
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
	seq := db.conditionalSeq.Add(1)
	pointCount := 0
	rangeCount := 0
	for i := range entries {
		switch entries[i].Type {
		case batch.OpPut, batch.OpDelete:
			if db.conditionalOracle.recent == nil {
				db.conditionalOracle.recent = make(map[string]uint64, len(entries))
			}
			db.conditionalOracle.recent[string(entries[i].Key)] = seq
			pointCount++
		case batch.OpDeleteRange:
			if batch.IsDeleteRangeNoop(entries[i].Key, entries[i].Value) {
				continue
			}
			db.conditionalOracle.ranges = append(db.conditionalOracle.ranges, conditionalRecentRange{
				start: string(entries[i].Key),
				end:   string(entries[i].Value),
				seq:   seq,
			})
			rangeCount++
		}
	}
	for i := range ranges {
		db.conditionalOracle.ranges = append(db.conditionalOracle.ranges, conditionalRecentRange{
			start: string(ranges[i].Start),
			end:   string(ranges[i].End),
			seq:   seq,
		})
	}
	rangeCount += len(ranges)
	if pointCount > 0 {
		db.conditionalOracleRecordedPoints.Add(uint64(pointCount))
	}
	if rangeCount > 0 {
		db.conditionalOracleRecordedRanges.Add(uint64(rangeCount))
	}
}

func (db *DB) conditionalRecordPointWrite(op batch.OpType, key []byte) {
	if db == nil || db.conditionalActiveTxnCount.Load() <= 0 {
		return
	}
	entry := [1]batch.Entry{{Type: op, Key: key}}
	db.conditionalRecordCommittedEntries(entry[:], nil, 0)
}

func (db *DB) observeConditionalTxnCommit(readSetLen int, err error) {
	if db == nil {
		return
	}
	db.conditionalTxnCommitAttempts.Add(1)
	db.conditionalTxnReadSetSamples.Add(1)
	conditionalAddIntMetric(&db.conditionalTxnReadSetEntries, readSetLen)
	conditionalStoreUint64Max(&db.conditionalTxnReadSetMax, uint64(readSetLen))
	if errors.Is(err, backenddb.ErrConcurrentModification) {
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
		conditionalAddIntMetric(&db.conditionalOraclePrunedPoints, pointsBefore)
		conditionalAddIntMetric(&db.conditionalOraclePrunedRanges, rangesBefore)
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
	conditionalAddIntMetric(&db.conditionalOraclePrunedPoints, pointsBefore-len(db.conditionalOracle.recent))
	conditionalAddIntMetric(&db.conditionalOraclePrunedRanges, rangesBefore-len(db.conditionalOracle.ranges))
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

func conditionalAddIntMetric(dst *atomic.Uint64, delta int) {
	if dst == nil || delta <= 0 {
		return
	}
	dst.Add(uint64(delta))
}

func conditionalStoreUint64Max(dst *atomic.Uint64, value uint64) {
	if dst == nil {
		return
	}
	for {
		old := dst.Load()
		if value <= old {
			return
		}
		if dst.CompareAndSwap(old, value) {
			return
		}
	}
}
