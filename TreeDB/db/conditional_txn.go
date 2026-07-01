package db

import (
	"errors"
	"sync"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/tree"
)

var (
	// ErrConditionalTxnClosed indicates a conditional transaction was already
	// committed or closed.
	ErrConditionalTxnClosed = errors.New("treedb: conditional transaction closed")
	// ErrConditionalRangeUnsupported is returned by conditional transactions
	// for range deletes until TreeDB has a range-intent API with bounded
	// conflict and replay semantics.
	ErrConditionalRangeUnsupported = errors.New("treedb: conditional transaction range delete unsupported")
)

// ConditionalTxn is an optimistic read/write transaction for raw TreeDB keys.
// Reads are served from one snapshot and recorded in a read set; Commit fails
// with ErrConcurrentModification if a committed point or range write touched
// any read key after that snapshot. The transaction is not safe for concurrent
// method calls.
type ConditionalTxn struct {
	db       *DB
	snap     *Snapshot
	batch    *Batch
	oracleID uint64
	baseSeq  uint64
	readKeys map[string][]byte
	closed   bool
}

type conditionalWriteSet struct {
	points []batch.Entry
	ranges []batch.DeleteRange
}

type conditionalCommittedWrites struct {
	seq    uint64
	points map[string]struct{}
	ranges []batch.DeleteRange
}

type conditionalCommitCheck struct {
	baseSeq  uint64
	readKeys map[string][]byte
}

type conditionalConflictOracle struct {
	mu     sync.Mutex
	nextID uint64
	active map[uint64]uint64
	writes []conditionalCommittedWrites
}

func newConditionalWriteSet(points []batch.Entry, ranges []batch.DeleteRange) *conditionalWriteSet {
	if len(points) == 0 && len(ranges) == 0 {
		return nil
	}
	return &conditionalWriteSet{points: points, ranges: ranges}
}

func snapshotRevision(s *Snapshot) uint64 {
	if s == nil || s.state == nil {
		return 0
	}
	return s.state.CommitSeq
}

func (o *conditionalConflictOracle) register(baseSeq uint64) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.nextID++
	id := o.nextID
	if o.active == nil {
		o.active = make(map[uint64]uint64)
	}
	o.active[id] = baseSeq
	return id
}

func (o *conditionalConflictOracle) setBaseSeq(id uint64, baseSeq uint64) {
	o.mu.Lock()
	if o.active != nil {
		if _, ok := o.active[id]; ok {
			o.active[id] = baseSeq
		}
	}
	o.pruneLocked()
	o.mu.Unlock()
}

func (o *conditionalConflictOracle) unregister(id uint64) {
	if id == 0 {
		return
	}
	o.mu.Lock()
	if o.active != nil {
		delete(o.active, id)
	}
	o.pruneLocked()
	o.mu.Unlock()
}

func (o *conditionalConflictOracle) recordCommit(seq uint64, writes *conditionalWriteSet) {
	if seq == 0 || writes == nil || (len(writes.points) == 0 && len(writes.ranges) == 0) {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.active) == 0 {
		return
	}
	committed := conditionalCommittedWrites{seq: seq}
	if len(writes.points) > 0 {
		committed.points = make(map[string]struct{}, len(writes.points))
		for _, entry := range writes.points {
			switch entry.Type {
			case batch.OpPut, batch.OpDelete:
				committed.points[string(entry.Key)] = struct{}{}
			}
		}
	}
	if len(writes.ranges) > 0 {
		committed.ranges = make([]batch.DeleteRange, 0, len(writes.ranges))
		for _, r := range writes.ranges {
			committed.ranges = append(committed.ranges, batch.DeleteRange{
				Start: append([]byte(nil), r.Start...),
				End:   append([]byte(nil), r.End...),
			})
		}
	}
	if len(committed.points) == 0 && len(committed.ranges) == 0 {
		return
	}
	o.writes = append(o.writes, committed)
	o.pruneLocked()
}

func (o *conditionalConflictOracle) hasConflict(baseSeq uint64, readKeys map[string][]byte) bool {
	if len(readKeys) == 0 {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, committed := range o.writes {
		if committed.seq <= baseSeq {
			continue
		}
		for keyString, key := range readKeys {
			if _, ok := committed.points[keyString]; ok {
				return true
			}
			for _, r := range committed.ranges {
				if batch.DeleteRangeContainsKey(r, key) {
					return true
				}
			}
		}
	}
	return false
}

func (db *DB) validateConditionalCommit(check *conditionalCommitCheck) error {
	if check == nil {
		return nil
	}
	if db.conditionalOracle.hasConflict(check.baseSeq, check.readKeys) {
		return ErrConcurrentModification
	}
	return nil
}

func (o *conditionalConflictOracle) stats() (active int, retainedWrites int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.active), len(o.writes)
}

func (o *conditionalConflictOracle) pruneLocked() {
	if len(o.active) == 0 {
		o.writes = nil
		return
	}
	minBase := uint64(^uint64(0))
	for _, base := range o.active {
		if base < minBase {
			minBase = base
		}
	}
	keep := 0
	for _, committed := range o.writes {
		if committed.seq > minBase {
			o.writes[keep] = committed
			keep++
		}
	}
	for i := keep; i < len(o.writes); i++ {
		o.writes[i] = conditionalCommittedWrites{}
	}
	o.writes = o.writes[:keep]
}

// NewConditionalTxn opens a conditional read/write transaction over raw keys.
func (db *DB) NewConditionalTxn() (*ConditionalTxn, error) {
	if db == nil || db.closing.Load() {
		return nil, ErrClosed
	}
	if db.readOnly {
		return nil, ErrReadOnly
	}
	id := db.conditionalOracle.register(0)
	snap, err := db.acquireSnapshotOrErr()
	if err != nil {
		db.conditionalOracle.unregister(id)
		return nil, err
	}
	baseSeq := snapshotRevision(snap)
	db.conditionalOracle.setBaseSeq(id, baseSeq)
	b, ok := db.NewBatch().(*Batch)
	if !ok || b == nil {
		_ = snap.Close()
		db.conditionalOracle.unregister(id)
		return nil, ErrClosed
	}
	return &ConditionalTxn{
		db:       db,
		snap:     snap,
		batch:    b,
		oracleID: id,
		baseSeq:  baseSeq,
		readKeys: make(map[string][]byte),
	}, nil
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

func (tx *ConditionalTxn) recordRead(key []byte) {
	if tx.readKeys == nil {
		tx.readKeys = make(map[string][]byte)
	}
	if _, ok := tx.readKeys[string(key)]; !ok {
		tx.readKeys[string(key)] = cloneRawKVPointKey(key)
	}
}

// Get returns the value visible in the transaction snapshot. Missing keys
// return nil, nil and are still tracked for conflict detection.
func (tx *ConditionalTxn) Get(key []byte) ([]byte, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	key = normalizeRawKVPointKey(key)
	tx.recordRead(key)
	value, err := tx.snap.GetAppend(key, nil)
	if err == tree.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

// GetVersioned returns the value visible in the transaction snapshot plus its
// durable entry revision. Missing keys are tracked as reads.
func (tx *ConditionalTxn) GetVersioned(key []byte) (VersionedEntry, bool, error) {
	if err := tx.ensureOpen(); err != nil {
		return VersionedEntry{}, false, err
	}
	key = normalizeRawKVPointKey(key)
	tx.recordRead(key)
	entry, found, err := tx.snap.GetVersioned(key)
	if err != nil {
		return VersionedEntry{}, false, err
	}
	return entry, found, nil
}

// Has reports whether key exists in the transaction snapshot and records the
// key as a conflict-checked read.
func (tx *ConditionalTxn) Has(key []byte) (bool, error) {
	if err := tx.ensureOpen(); err != nil {
		return false, err
	}
	key = normalizeRawKVPointKey(key)
	tx.recordRead(key)
	return tx.snap.Has(key)
}

// Set stages a key/value write for Commit.
func (tx *ConditionalTxn) Set(key, value []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.Set(normalizeRawKVPointKey(key), normalizeRawKVValue(value))
}

// Delete stages a key delete for Commit.
func (tx *ConditionalTxn) Delete(key []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return tx.batch.Delete(normalizeRawKVPointKey(key))
}

// DeleteRange fails closed until range-intent transactions are supported.
func (tx *ConditionalTxn) DeleteRange(start, end []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	return ErrConditionalRangeUnsupported
}

// Commit publishes staged writes if no read key changed since the transaction
// snapshot.
func (tx *ConditionalTxn) Commit() error {
	return tx.commit(false)
}

// CommitSync is Commit with a sync durability boundary.
func (tx *ConditionalTxn) CommitSync() error {
	return tx.commit(true)
}

func (tx *ConditionalTxn) commit(sync bool) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if tx.db.conditionalOracle.hasConflict(tx.baseSeq, tx.readKeys) {
		return tx.closeWithError(ErrConcurrentModification)
	}
	tx.batch.conditionalCheck = &conditionalCommitCheck{
		baseSeq:  tx.baseSeq,
		readKeys: tx.readKeys,
	}
	var err error
	if sync {
		err = tx.batch.WriteSync()
	} else {
		err = tx.batch.Write()
	}
	return tx.closeWithError(err)
}

// Close releases transaction resources without committing staged writes.
func (tx *ConditionalTxn) Close() error {
	return tx.closeWithError(nil)
}

func (tx *ConditionalTxn) closeWithError(err error) error {
	if tx == nil || tx.closed {
		if err != nil {
			return err
		}
		return nil
	}
	tx.closed = true
	if tx.db != nil {
		tx.db.conditionalOracle.unregister(tx.oracleID)
	}
	if tx.snap != nil {
		if closeErr := tx.snap.Close(); err == nil {
			err = closeErr
		}
		tx.snap = nil
	}
	if tx.batch != nil {
		if closeErr := tx.batch.Close(); err == nil {
			err = closeErr
		}
		tx.batch = nil
	}
	return err
}
