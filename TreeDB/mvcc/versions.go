package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

var (
	ErrDiscardFloorRegression   = errors.New("mvcc: discard floor cannot move backward")
	ErrReadBeforeDiscardFloor   = errors.New("mvcc: read timestamp is at or below discard floor")
	ErrVersionBelowDiscardFloor = errors.New("mvcc: commit timestamp is not above discard floor")
	ErrSnapshotUnavailable      = errors.New("mvcc: TreeDB snapshot support is unavailable")
)

// Metadata lives under an explicit zero-version marker followed by "M". It is
// outside the versioned-key range and does not consume a future codec version.
var discardFloorKey = []byte{0x00, 'T', 'D', 'B', 'M', 'V', 'C', 'C', 0x00, 'M', 0x01, 'd', 'f'}

const discardFloorRecordV1 byte = 0x01

type snapshotDB interface {
	AcquireSnapshot() treedb.Snapshot
}

// Version describes one retained external-MVCC record. Key and Value are
// caller-owned copies. Tombstones have State Tombstone and a nil Value.
type Version struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	State     ReadState
}

// VersionIteratorOptions defines one snapshot-bound retained-version scan.
// LowerBound is inclusive and UpperBound is exclusive in logical-key order.
// Prefix is combined with those bounds. ReadTimestamp zero means no timestamp
// ceiling; otherwise only versions at or below it are returned. A persisted
// discard floor is the greatest timestamp that may be discarded, so nonzero
// read timestamps must be strictly greater than it.
//
// Forward order is (logical key ascending, timestamp descending). Reverse
// order is (logical key descending, timestamp ascending), exactly mirroring
// the physical codec order.
type VersionIteratorOptions struct {
	Prefix        []byte
	LowerBound    []byte
	UpperBound    []byte
	ReadTimestamp uint64
	Reverse       bool
}

// VersionIteratorStats is exact for physical records examined by an iterator.
type VersionIteratorStats struct {
	Visited  uint64
	Skipped  uint64
	Retained uint64
}

// VersionIterator owns a TreeDB snapshot and must be closed. Entry returns a
// caller-owned record that remains valid after Next, Seek, or Close.
type VersionIterator struct {
	raw      treedb.Iterator
	snapshot treedb.Snapshot
	options  VersionIteratorOptions
	keyBuf   []byte
	current  Version
	stats    VersionIteratorStats
	err      error
	closed   bool
}

// IterateVersions opens a snapshot-bound scan of every retained version in the
// requested logical range. A nonzero read timestamp at or below the persisted
// discard floor is rejected. Iterators opened before a later floor advance keep
// their pinned view until Close.
func (s *Store) IterateVersions(options VersionIteratorOptions) (*VersionIterator, error) {
	if s == nil || s.db == nil {
		return nil, storageError("open version iterator", treedb.ErrClosed)
	}
	options = copyVersionIteratorOptions(options)
	if err := s.lockDiscardFloorRead(); err != nil {
		return nil, err
	}
	floor := s.discardFloor
	if options.ReadTimestamp != 0 && floor != 0 && options.ReadTimestamp <= floor {
		s.mu.RUnlock()
		return nil, fmt.Errorf("%w: read timestamp %d is at or below floor %d", ErrReadBeforeDiscardFloor, options.ReadTimestamp, floor)
	}
	if options.UpperBound != nil && options.LowerBound != nil && bytes.Compare(options.LowerBound, options.UpperBound) >= 0 {
		s.mu.RUnlock()
		return &VersionIterator{options: options}, nil
	}

	lower, upper, err := versionPhysicalBounds(options)
	if err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	snapshotter, ok := s.db.(snapshotDB)
	if !ok {
		s.mu.RUnlock()
		return nil, ErrSnapshotUnavailable
	}
	snapshot := snapshotter.AcquireSnapshot()
	if snapshot == nil {
		s.mu.RUnlock()
		return nil, storageError("acquire version snapshot", treedb.ErrClosed)
	}
	var raw treedb.Iterator
	if options.Reverse {
		raw, err = snapshot.ReverseIterator(lower, upper)
	} else {
		raw, err = snapshot.Iterator(lower, upper)
	}
	s.mu.RUnlock()
	if err != nil {
		return nil, errors.Join(storageError("open version iterator", err), storageError("close version snapshot", snapshot.Close()))
	}
	it := &VersionIterator{raw: raw, snapshot: snapshot, options: options}
	it.advance()
	return it, nil
}

func copyVersionIteratorOptions(options VersionIteratorOptions) VersionIteratorOptions {
	options.Prefix = cloneBytesPreserveNil(options.Prefix)
	options.LowerBound = cloneBytesPreserveNil(options.LowerBound)
	options.UpperBound = cloneBytesPreserveNil(options.UpperBound)
	return options
}

func cloneBytesPreserveNil(value []byte) []byte {
	if value == nil {
		return nil
	}
	clone := make([]byte, len(value))
	copy(clone, value)
	return clone
}

func (it *VersionIterator) Valid() bool {
	return it != nil && !it.closed && it.err == nil && it.raw != nil && it.raw.Valid()
}

func (it *VersionIterator) Entry() Version {
	if !it.Valid() {
		return Version{}
	}
	return Version{
		Key: append([]byte(nil), it.current.Key...), Value: append([]byte(nil), it.current.Value...),
		Timestamp: it.current.Timestamp, State: it.current.State,
	}
}

func (it *VersionIterator) Next() {
	if !it.Valid() {
		return
	}
	it.raw.Next()
	it.advance()
}

// Seek repositions inside the iterator's original domain. Timestamp must be
// nonzero. Direction follows the iterator: forward seeks to the first encoded
// record >= (logical,timestamp), reverse to the first <= it.
func (it *VersionIterator) Seek(logical []byte, timestamp uint64) {
	if it == nil || it.closed || it.err != nil || it.raw == nil {
		return
	}
	physical, err := mvcckey.Encode(logical, timestamp)
	if err != nil {
		it.err = fmt.Errorf("%w: %w", ErrInvalidKey, err)
		return
	}
	it.raw.Seek(physical)
	it.advance()
}

func (it *VersionIterator) Stats() VersionIteratorStats {
	if it == nil {
		return VersionIteratorStats{}
	}
	return it.stats
}

func (it *VersionIterator) Error() error {
	if it == nil {
		return nil
	}
	if it.err != nil {
		return it.err
	}
	if it.raw != nil {
		return storageError("iterate versions", it.raw.Error())
	}
	return nil
}

func (it *VersionIterator) Close() error {
	if it == nil || it.closed {
		return nil
	}
	it.closed = true
	var rawErr, snapshotErr error
	if it.raw != nil {
		rawErr = storageError("close version iterator", it.raw.Close())
	}
	if it.snapshot != nil {
		snapshotErr = storageError("close version snapshot", it.snapshot.Close())
	}
	return errors.Join(rawErr, snapshotErr)
}

func (it *VersionIterator) advance() {
	for it.raw != nil && it.raw.Valid() {
		it.stats.Visited++
		var timestamp uint64
		var err error
		it.keyBuf, timestamp, err = mvcckey.DecodeAppend(it.keyBuf[:0], it.raw.Key())
		if err != nil {
			it.err = fmt.Errorf("%w: %w", ErrMalformedRecord, err)
			return
		}
		logical := it.keyBuf
		if !logicalMatchesOptions(logical, it.options) || (it.options.ReadTimestamp != 0 && timestamp > it.options.ReadTimestamp) {
			it.stats.Skipped++
			it.raw.Next()
			continue
		}
		record := it.raw.Value()
		if rawErr := it.raw.Error(); rawErr != nil {
			it.err = storageError("read version iterator value", rawErr)
			return
		}
		state, value, err := decodeRecordView(record)
		if err != nil {
			it.err = err
			return
		}
		it.current = Version{Key: logical, Value: value, Timestamp: timestamp, State: state}
		it.stats.Retained++
		return
	}
	if it.raw != nil && it.raw.Error() != nil {
		it.err = storageError("iterate versions", it.raw.Error())
	}
}

func logicalMatchesOptions(logical []byte, options VersionIteratorOptions) bool {
	if options.Prefix != nil && !bytes.HasPrefix(logical, options.Prefix) {
		return false
	}
	if options.LowerBound != nil && bytes.Compare(logical, options.LowerBound) < 0 {
		return false
	}
	return options.UpperBound == nil || bytes.Compare(logical, options.UpperBound) < 0
}

func versionPhysicalBounds(options VersionIteratorOptions) ([]byte, []byte, error) {
	lower := mvcckey.AppendNamespaceLower(nil)
	upper := mvcckey.AppendNamespaceUpper(nil)
	if options.Prefix != nil {
		prefixLower, err := mvcckey.AppendLogicalPrefixLower(nil, options.Prefix)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: prefix: %w", ErrInvalidKey, err)
		}
		prefixUpper, err := mvcckey.AppendLogicalPrefixUpper(nil, options.Prefix)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: prefix: %w", ErrInvalidKey, err)
		}
		lower = maxBytes(lower, prefixLower)
		upper = minBytes(upper, prefixUpper)
	}
	if options.LowerBound != nil {
		bound, err := mvcckey.AppendKeyVersionsLower(nil, options.LowerBound)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: lower bound: %w", ErrInvalidKey, err)
		}
		lower = maxBytes(lower, bound)
	}
	if options.UpperBound != nil {
		bound, err := mvcckey.AppendKeyVersionsLower(nil, options.UpperBound)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: upper bound: %w", ErrInvalidKey, err)
		}
		upper = minBytes(upper, bound)
	}
	return lower, upper, nil
}

func maxBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) >= 0 {
		return a
	}
	return b
}
func minBytes(a, b []byte) []byte {
	if bytes.Compare(a, b) <= 0 {
		return a
	}
	return b
}

// DiscardFloor returns the greatest timestamp declared discardable. Zero means
// no versions have yet been declared obsolete. Once nonzero, historical reads
// are accepted only at timestamps strictly greater than the floor.
func (s *Store) DiscardFloor() (uint64, error) {
	if s == nil || s.db == nil {
		return 0, storageError("read discard floor", treedb.ErrClosed)
	}
	if err := s.lockDiscardFloorRead(); err != nil {
		return 0, err
	}
	floor := s.discardFloor
	s.mu.RUnlock()
	return floor, nil
}

// AdvanceDiscardFloor monotonically persists the greatest discardable
// timestamp. The floor is published before PruneVersions can remove any
// obsolete record.
func (s *Store) AdvanceDiscardFloor(timestamp uint64, mode CommitMode) error {
	if timestamp == 0 {
		return ErrZeroTimestamp
	}
	if mode != CommitRelaxed && mode != CommitDurable {
		return ErrInvalidCommitMode
	}
	if s == nil || s.db == nil {
		return storageError("advance discard floor", treedb.ErrClosed)
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadDiscardFloorLocked(); err != nil {
		return err
	}
	currentFloor := s.discardFloor
	if timestamp < currentFloor {
		return fmt.Errorf("%w: current %d, requested %d", ErrDiscardFloorRegression, currentFloor, timestamp)
	}
	if timestamp == currentFloor {
		if mode == CommitDurable {
			if err := s.persistDiscardFloorLocked(timestamp, CommitDurable); err != nil {
				s.invalidateDiscardFloorLocked()
				return err
			}
		}
		return nil
	}
	if err := s.persistDiscardFloorLocked(timestamp, mode); err != nil {
		s.invalidateDiscardFloorLocked()
		return err
	}
	s.discardFloor = timestamp
	return nil
}

// PruneOptions controls bounded physical deletion. BatchSize defaults to 256.
type PruneOptions struct {
	BatchSize int
	Mode      CommitMode
}

// PruneStats accounts for the snapshot scanned and successfully committed
// deletes. On success, Visited equals Retained plus Pruned. Skipped is the
// subset of Retained whose timestamps are above the discard floor; it is not a
// disjoint outcome counter. On error, staged but uncommitted deletes can make
// the success relationship incomplete. RetainedBytes is the physical key/value
// bytes observed and kept; PrunedBytes is the physical key+value record bytes
// deleted (TreeDB tombstones add their own implementation-specific write
// amplification). DeleteWriteBytes is the sum of TreeDB's delete-batch
// byte-size estimates.
type PruneStats struct {
	Visited, Skipped, Retained, Pruned uint64
	RetainedBytes, PrunedBytes         uint64
	DeleteWriteBytes                   uint64
	Batches                            uint64
}

// PruneVersions removes versions made obsolete by the persisted floor using a
// bounded reverse scan. For each key it retains every version above the floor
// plus the newest value at/below it. A newest-at-floor tombstone is removed only
// after every older version has been deleted, preventing value resurrection.
// Durable mode re-syncs the floor before the first delete, so interruption and
// restart are idempotent and cannot recover pruned data without its floor.
func (s *Store) PruneVersions(options PruneOptions) (stats PruneStats, err error) {
	if options.Mode != CommitRelaxed && options.Mode != CommitDurable {
		return stats, ErrInvalidCommitMode
	}
	if s == nil || s.db == nil {
		return stats, storageError("prune versions", treedb.ErrClosed)
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 256
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()

	s.mu.Lock()
	if err := s.loadDiscardFloorLocked(); err != nil {
		s.mu.Unlock()
		return stats, err
	}
	floor := s.discardFloor
	if floor == 0 {
		s.mu.Unlock()
		return stats, nil
	}
	if options.Mode == CommitDurable {
		if err := s.persistDiscardFloorLocked(floor, CommitDurable); err != nil {
			s.mu.Unlock()
			return stats, err
		}
	}
	snapshotter, ok := s.db.(snapshotDB)
	if !ok {
		s.mu.Unlock()
		return stats, ErrSnapshotUnavailable
	}
	snapshot := snapshotter.AcquireSnapshot()
	s.mu.Unlock()
	if snapshot == nil {
		return stats, storageError("acquire prune snapshot", treedb.ErrClosed)
	}
	defer func() { err = errors.Join(err, storageError("close prune snapshot", snapshot.Close())) }()
	raw, err := snapshot.ReverseIterator(mvcckey.AppendNamespaceLower(nil), mvcckey.AppendNamespaceUpper(nil))
	if err != nil {
		return stats, storageError("open prune iterator", err)
	}
	defer func() { err = errors.Join(err, storageError("close prune iterator", raw.Close())) }()

	type candidate struct {
		physical  []byte
		tombstone bool
		bytes     uint64
	}
	var currentLogical []byte
	var logicalBuf []byte
	var anchor candidate
	haveLogical, haveAnchor := false, false
	var batch treedb.Batch
	staged, stagedBytes := 0, uint64(0)
	defer func() {
		if batch != nil {
			err = errors.Join(err, storageError("close incomplete prune batch", batch.Close()))
		}
	}()
	flush := func() error {
		if staged == 0 {
			return nil
		}
		writeBytes, sizeErr := batch.GetByteSize()
		if sizeErr != nil {
			return storageError("measure prune batch", sizeErr)
		}
		var writeErr error
		if options.Mode == CommitDurable {
			writeErr = batch.WriteSync()
		} else {
			writeErr = batch.Write()
		}
		closeErr := batch.Close()
		batch = nil
		if writeErr != nil || closeErr != nil {
			return errors.Join(storageError("commit prune batch", writeErr), storageError("close prune batch", closeErr))
		}
		stats.Pruned += uint64(staged)
		stats.PrunedBytes += stagedBytes
		stats.DeleteWriteBytes += uint64(writeBytes)
		stats.Batches++
		staged, stagedBytes = 0, 0
		return nil
	}
	stageDelete := func(c candidate) error {
		if batch == nil {
			batch = s.db.NewBatchWithSize(options.BatchSize)
			if batch == nil {
				return storageError("create prune batch", treedb.ErrClosed)
			}
		}
		if err := batch.Delete(c.physical); err != nil {
			return storageError("stage prune delete", err)
		}
		staged++
		stagedBytes += c.bytes
		if staged >= options.BatchSize {
			return flush()
		}
		return nil
	}
	finalizeAnchor := func() error {
		if !haveAnchor {
			return nil
		}
		if anchor.tombstone {
			if err := stageDelete(anchor); err != nil {
				return err
			}
		} else {
			stats.Retained++
			stats.RetainedBytes += anchor.bytes
		}
		haveAnchor = false
		return nil
	}

	for raw.Valid() {
		stats.Visited++
		physical := raw.Key()
		var timestamp uint64
		var decodeErr error
		logicalBuf, timestamp, decodeErr = mvcckey.DecodeAppend(logicalBuf[:0], physical)
		if decodeErr != nil {
			return stats, fmt.Errorf("%w: %w", ErrMalformedRecord, decodeErr)
		}
		logical := logicalBuf
		record := raw.Value()
		if rawErr := raw.Error(); rawErr != nil {
			return stats, storageError("read prune iterator value", rawErr)
		}
		state, decodeErr := validateRecord(record)
		if decodeErr != nil {
			return stats, decodeErr
		}
		if !haveLogical || !bytes.Equal(logical, currentLogical) {
			if err := finalizeAnchor(); err != nil {
				return stats, err
			}
			currentLogical = append(currentLogical[:0], logical...)
			haveLogical = true
		}
		recordBytes := uint64(len(physical) + len(record))
		if timestamp > floor {
			if err := finalizeAnchor(); err != nil {
				return stats, err
			}
			stats.Skipped++
			stats.Retained++
			stats.RetainedBytes += recordBytes
			raw.Next()
			continue
		}
		if haveAnchor {
			if err := stageDelete(anchor); err != nil {
				return stats, err
			}
		}
		anchor = candidate{physical: append(anchor.physical[:0], physical...), tombstone: state == Tombstone, bytes: recordBytes}
		haveAnchor = true
		raw.Next()
	}
	if raw.Error() != nil {
		return stats, storageError("iterate prune snapshot", raw.Error())
	}
	if err := finalizeAnchor(); err != nil {
		return stats, err
	}
	if err := flush(); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Store) lockDiscardFloorRead() error {
	for {
		s.mu.RLock()
		if s.floorLoaded {
			return nil
		}
		s.mu.RUnlock()
		s.mu.Lock()
		err := s.loadDiscardFloorLocked()
		s.mu.Unlock()
		if err != nil {
			return err
		}
		// Loop so an ambiguous advance that invalidated the cache between
		// load and reacquiring RLock cannot expose the temporary zero floor.
	}
}

func (s *Store) loadDiscardFloorLocked() (err error) {
	if s.floorLoaded {
		return nil
	}
	upper := append([]byte(nil), discardFloorKey...)
	upper[len(upper)-1]++
	it, err := s.db.Iterator(discardFloorKey, upper)
	if err != nil {
		return storageError("read discard floor", err)
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			s.floorLoaded = false
			err = errors.Join(err, storageError("close discard floor iterator", closeErr))
		}
	}()
	if !it.Valid() {
		if it.Error() != nil {
			return storageError("read discard floor", it.Error())
		}
		s.discardFloor = 0
		s.floorLoaded = true
		return nil
	}
	if !bytes.Equal(it.Key(), discardFloorKey) {
		s.discardFloor = 0
		s.floorLoaded = true
		return nil
	}
	record := it.Value()
	if iterErr := it.Error(); iterErr != nil {
		return storageError("read discard floor value", iterErr)
	}
	if len(record) != 9 || record[0] != discardFloorRecordV1 {
		return fmt.Errorf("%w: invalid discard floor record", ErrMalformedRecord)
	}
	floor := binary.BigEndian.Uint64(record[1:])
	if floor == 0 {
		return fmt.Errorf("%w: zero discard floor record", ErrMalformedRecord)
	}
	s.discardFloor = floor
	s.floorLoaded = true
	return nil
}

func (s *Store) invalidateDiscardFloorLocked() {
	s.discardFloor = 0
	s.floorLoaded = false
}

func (s *Store) persistDiscardFloorLocked(floor uint64, mode CommitMode) error {
	record := make([]byte, 9)
	record[0] = discardFloorRecordV1
	binary.BigEndian.PutUint64(record[1:], floor)
	batch := s.db.NewBatchWithSize(1)
	if batch == nil {
		return storageError("create discard floor batch", treedb.ErrClosed)
	}
	if err := batch.Set(discardFloorKey, record); err != nil {
		return errors.Join(storageError("stage discard floor", err), storageError("close discard floor batch", batch.Close()))
	}
	var err error
	if mode == CommitDurable {
		err = batch.WriteSync()
	} else {
		err = batch.Write()
	}
	return errors.Join(storageError("persist discard floor", err), storageError("close discard floor batch", batch.Close()))
}

func validateRecord(record []byte) (ReadState, error) {
	if len(record) == 0 {
		return Absent, fmt.Errorf("%w: empty value envelope", ErrMalformedRecord)
	}
	switch record[0] {
	case recordValueV1:
		return Present, nil
	case recordTombstoneV1:
		if len(record) != 1 {
			return Absent, fmt.Errorf("%w: tombstone has payload", ErrMalformedRecord)
		}
		return Tombstone, nil
	default:
		return Absent, fmt.Errorf("%w: unknown envelope tag 0x%02x", ErrMalformedRecord, record[0])
	}
}

func decodeRecordView(record []byte) (ReadState, []byte, error) {
	state, err := validateRecord(record)
	if err != nil {
		return Absent, nil, err
	}
	if state == Present {
		return state, record[1:], nil
	}
	return state, nil, nil
}
