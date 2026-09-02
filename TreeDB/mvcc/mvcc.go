// Package mvcc provides an opt-in external-timestamp MVCC layer over TreeDB.
//
// A Store owns TreeDB's reserved external-MVCC physical-key namespace. Mixing
// unrelated raw TreeDB writes into that namespace is unsupported. The package
// is pre-alpha: its API and on-disk record format may change without migration.
package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// CommitMode selects the acknowledgement and durability boundary for CommitAt.
type CommitMode uint8

const (
	// CommitRelaxed publishes one atomic TreeDB batch with Batch.Write. It does
	// not promise an fsync boundary and may be lost after a crash.
	CommitRelaxed CommitMode = iota
	// CommitDurable publishes one atomic batch with Batch.WriteSync. Production
	// profiles support this explicit durability opt-up even when ordinary ACKs
	// are relaxed.
	CommitDurable
)

// Mutation is one logical-key mutation in a CommitAt batch. Delete selects a
// tombstone; otherwise Value is stored verbatim, including nil/empty values.
type Mutation struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// CommitGroup is one caller-assigned timestamp and its mutations. CommitGroupAt
// publishes every non-empty group in its input through one TreeDB batch, so it
// is useful when an external MVCC coordinator has already formed several
// commits that must be made visible together.
type CommitGroup struct {
	Timestamp uint64
	Mutations []Mutation
}

// ReadState identifies the visibility result returned by GetAt.
type ReadState uint8

const (
	// Absent means no retained version exists at or below the read timestamp.
	Absent ReadState = iota
	// Present means Value and Timestamp describe the visible retained value.
	Present
	// Tombstone means Timestamp identifies the visible deletion marker.
	Tombstone
)

// Result is the newest retained version visible at a requested timestamp.
// Value is a caller-owned copy for Present results.
type Result struct {
	State     ReadState
	Value     []byte
	Timestamp uint64
}

var (
	ErrZeroTimestamp     = errors.New("mvcc: timestamp zero is reserved")
	ErrDuplicateKey      = errors.New("mvcc: duplicate logical key in batch")
	ErrInvalidCommitMode = errors.New("mvcc: invalid commit mode")
	ErrInvalidKey        = errors.New("mvcc: invalid logical key")
	ErrMalformedRecord   = errors.New("mvcc: malformed physical record")
	ErrStorage           = errors.New("mvcc: storage error")
)

const (
	recordValueV1     byte = 0x01
	recordTombstoneV1 byte = 0x02
)

type treeDB interface {
	Iterator(start, end []byte) (treedb.Iterator, error)
	NewBatchWithSize(size int) treedb.Batch
}

// pointWriter is an optional optimization implemented by the public TreeDB
// handle. Keeping it separate from treeDB preserves the narrow test and
// maintenance adapters that intentionally support only the batch contract.
type pointWriter interface {
	Set(key, value []byte) error
}

type pointSuccessorDB interface {
	SeekGE(start, end []byte) (key, value []byte, found bool, err error)
}

// Store owns the external-version namespace of one TreeDB handle.
type Store struct {
	db treeDB

	// maintenanceMu serializes floor advancement and pruning. The lock order is
	// maintenanceMu then mu; foreground reads and commits never take it.
	maintenanceMu sync.Mutex

	// mu guards the discard-floor cache and serializes its publication against
	// commits and snapshot acquisition. Reads release it as soon as their
	// point-in-time iterator is pinned.
	mu           sync.RWMutex
	discardFloor uint64
	floorLoaded  bool
}

// New returns the one opt-in MVCC owner for db. Callers must keep exactly one
// Store per TreeDB handle and must not use raw writes in the reserved MVCC
// namespace while it owns that handle. A second Store would have independent
// in-memory floor cache and maintenance serialization and is unsupported.
func New(db *treedb.DB) *Store {
	if db == nil {
		return &Store{}
	}
	return newStore(db)
}

func newStore(db treeDB) *Store {
	return &Store{db: db}
}

// CommitAt atomically writes all mutations at one caller-assigned timestamp.
//
// Duplicate logical keys, including nil and empty keys (which are the same
// logical key), are rejected before a TreeDB batch is created. This deliberately
// avoids an order-dependent last-write-wins policy. Empty batches are no-ops.
// After timestamp, mode, and non-nil Store validation, an empty batch returns
// without accessing TreeDB or checking its configured durability/open state.
// A returned storage error can be commit-ambiguous, but visibility is always
// whole-batch: readers may see all mutations or none, never a prefix.
func (s *Store) CommitAt(timestamp uint64, mutations []Mutation, mode CommitMode) error {
	// Preserve the established CommitAt validation precedence for callers that
	// distinguish the reserved timestamp from a later mode/store failure.
	if timestamp == 0 {
		return ErrZeroTimestamp
	}
	return s.CommitGroupAt([]CommitGroup{{Timestamp: timestamp, Mutations: mutations}}, mode)
}

// CommitGroupAt validates and atomically publishes timestamped mutation groups.
// A single relaxed physical record uses TreeDB's equivalent point-write path
// when the handle supports it; durable and larger publications use exactly one
// TreeDB Batch.Write or Batch.WriteSync call. Every group is validated before
// storage is accessed: timestamps must be non-zero and
// above the discard floor, keys must fit the MVCC codec, and no physical MVCC
// key may occur twice. Thus the same logical key at distinct timestamps is
// allowed, while duplicate logical keys at the same timestamp are rejected
// even when they arrive in different groups.
//
// Empty groups are validated for timestamp and contribute no record. An empty
// group list (or a list containing only empty groups) is a no-op after Store
// and mode validation. As with CommitAt, a storage error can be commit
// ambiguous; group visibility is nevertheless all-or-none, never a prefix.
func (s *Store) CommitGroupAt(groups []CommitGroup, mode CommitMode) error {
	if mode != CommitRelaxed && mode != CommitDurable {
		return ErrInvalidCommitMode
	}
	if s == nil || s.db == nil {
		return storageError("create batch", treedb.ErrClosed)
	}
	type stagedMutation struct {
		physical  []byte
		record    []byte
		timestamp uint64
		group     int
		key       int
	}
	total := 0
	for groupIndex := range groups {
		group := &groups[groupIndex]
		if group.Timestamp == 0 {
			return fmt.Errorf("%w at group index %d", ErrZeroTimestamp, groupIndex)
		}
		total += len(group.Mutations)
	}
	if total == 0 {
		return nil
	}

	staged := make([]stagedMutation, 0, total)
	var seen map[string]struct{}
	if total > 1 {
		seen = make(map[string]struct{}, total)
	}
	for groupIndex := range groups {
		group := &groups[groupIndex]
		for keyIndex := range group.Mutations {
			mutation := &group.Mutations[keyIndex]
			physical, err := mvcckey.Encode(mutation.Key, group.Timestamp)
			if err != nil {
				return fmt.Errorf("%w at group index %d key index %d: %w", ErrInvalidKey, groupIndex, keyIndex, err)
			}
			// The physical encoding contains both logical key and timestamp. This
			// simultaneously catches duplicates within one commit and across
			// groups, without rejecting distinct versions of the same logical key.
			if seen != nil {
				identity := string(physical)
				if _, exists := seen[identity]; exists {
					return fmt.Errorf("%w at group index %d key index %d", ErrDuplicateKey, groupIndex, keyIndex)
				}
				seen[identity] = struct{}{}
			}

			entry := stagedMutation{physical: physical, timestamp: group.Timestamp, group: groupIndex, key: keyIndex}
			if mutation.Delete {
				entry.record = []byte{recordTombstoneV1}
			} else {
				entry.record = make([]byte, 1+len(mutation.Value))
				entry.record[0] = recordValueV1
				copy(entry.record[1:], mutation.Value)
			}
			staged = append(staged, entry)
		}
	}
	if err := s.lockDiscardFloorRead(); err != nil {
		return err
	}
	defer s.mu.RUnlock()
	floor := s.discardFloor
	for _, entry := range staged {
		if entry.timestamp <= floor {
			return fmt.Errorf("%w: group index %d timestamp %d is not above floor %d", ErrVersionBelowDiscardFloor, entry.group, entry.timestamp, floor)
		}
	}
	if mode == CommitRelaxed && len(staged) == 1 {
		if writer, ok := s.db.(pointWriter); ok {
			if err := writer.Set(staged[0].physical, staged[0].record); err != nil {
				return storageError("commit point", err)
			}
			return nil
		}
	}

	batch := s.db.NewBatchWithSize(len(staged))
	if batch == nil {
		return storageError("create batch", treedb.ErrClosed)
	}
	for i := range staged {
		if err := batch.Set(staged[i].physical, staged[i].record); err != nil {
			stageErr := storageError(fmt.Sprintf("stage group index %d key index %d", staged[i].group, staged[i].key), err)
			return errors.Join(stageErr, storageError("close batch", batch.Close()))
		}
	}

	var err error
	if mode == CommitDurable {
		err = batch.WriteSync()
	} else {
		err = batch.Write()
	}
	if err != nil {
		err = storageError("commit batch", err)
	}
	return errors.Join(err, storageError("close batch", batch.Close()))
}

// GetAt returns the newest retained version of logical at or below timestamp.
// The lookup constructs an exact lower bound at timestamp and seeks TreeDB
// directly into that key/time region; it does not materialize key history.
func (s *Store) GetAt(logical []byte, timestamp uint64) (result Result, err error) {
	if timestamp == 0 {
		return Result{}, ErrZeroTimestamp
	}
	if s == nil || s.db == nil {
		return Result{}, storageError("open iterator", treedb.ErrClosed)
	}
	lower, err := mvcckey.Encode(logical, timestamp)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	if err := s.lockDiscardFloorRead(); err != nil {
		return Result{}, err
	}
	floor := s.discardFloor
	if floor != 0 && timestamp <= floor {
		s.mu.RUnlock()
		return Result{}, fmt.Errorf("%w: read timestamp %d is at or below floor %d", ErrReadBeforeDiscardFloor, timestamp, floor)
	}
	if seeker, ok := s.db.(pointSuccessorDB); ok {
		physical, record, found, seekErr := seeker.SeekGE(lower, upper)
		s.mu.RUnlock()
		if seekErr != nil {
			return Result{}, storageError("seek version", seekErr)
		}
		if !found {
			return Result{State: Absent}, nil
		}
		return decodePointResult(logical, timestamp, physical, record)
	}
	it, err := s.db.Iterator(lower, upper)
	s.mu.RUnlock()
	if err != nil {
		return Result{}, storageError("open iterator", err)
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			err = errors.Join(err, storageError("close iterator", closeErr))
		}
	}()

	if !it.Valid() {
		if iterErr := it.Error(); iterErr != nil {
			return Result{}, storageError("iterate", iterErr)
		}
		return Result{State: Absent}, nil
	}

	physical := it.Key()
	record := it.Value()
	if iterErr := it.Error(); iterErr != nil {
		return Result{}, storageError("read value", iterErr)
	}
	return decodePointResult(logical, timestamp, physical, record)
}

func decodePointResult(logical []byte, timestamp uint64, physical, record []byte) (Result, error) {
	decoded, version, decodeErr := mvcckey.Decode(physical)
	if decodeErr != nil || !bytes.Equal(decoded, logical) || version > timestamp {
		if decodeErr == nil {
			decodeErr = errors.New("decoded key or timestamp is outside requested bound")
		}
		return Result{}, fmt.Errorf("%w: %w", ErrMalformedRecord, decodeErr)
	}
	if len(record) == 0 {
		return Result{}, fmt.Errorf("%w: empty value envelope", ErrMalformedRecord)
	}
	switch record[0] {
	case recordValueV1:
		return Result{
			State:     Present,
			Value:     append([]byte(nil), record[1:]...),
			Timestamp: version,
		}, nil
	case recordTombstoneV1:
		if len(record) != 1 {
			return Result{}, fmt.Errorf("%w: tombstone has payload", ErrMalformedRecord)
		}
		return Result{State: Tombstone, Timestamp: version}, nil
	default:
		return Result{}, fmt.Errorf("%w: unknown envelope tag 0x%02x", ErrMalformedRecord, record[0])
	}
}

func storageError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %s: %w", ErrStorage, operation, err)
}
