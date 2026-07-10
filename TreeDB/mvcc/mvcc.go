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
	"strings"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// CommitMode selects the acknowledgement and durability boundary for CommitAt.
type CommitMode uint8

const (
	// CommitRelaxed publishes one atomic TreeDB batch with Batch.Write. It does
	// not promise an fsync boundary and may be lost after a crash.
	CommitRelaxed CommitMode = iota
	// CommitDurable requires a TreeDB opened in DurabilityDurable mode and
	// publishes one atomic batch with Batch.WriteSync.
	CommitDurable
)

// Mutation is one logical-key mutation in a CommitAt batch. Delete selects a
// tombstone; otherwise Value is stored verbatim, including nil/empty values.
type Mutation struct {
	Key    []byte
	Value  []byte
	Delete bool
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
	ErrZeroTimestamp         = errors.New("mvcc: timestamp zero is reserved")
	ErrDuplicateKey          = errors.New("mvcc: duplicate logical key in batch")
	ErrInvalidCommitMode     = errors.New("mvcc: invalid commit mode")
	ErrDurabilityUnavailable = errors.New("mvcc: durable commit requires durable TreeDB mode")
	ErrInvalidKey            = errors.New("mvcc: invalid logical key")
	ErrMalformedRecord       = errors.New("mvcc: malformed physical record")
)

const (
	recordValueV1     byte = 0x01
	recordTombstoneV1 byte = 0x02
)

type treeDB interface {
	Iterator(start, end []byte) (treedb.Iterator, error)
	NewBatchWithSize(size int) treedb.Batch
	DurabilityMode() string
}

// Store owns the external-version namespace of one TreeDB handle.
type Store struct {
	db treeDB
}

// New returns an opt-in MVCC store over db. Callers must not use raw writes in
// the reserved MVCC namespace while the Store owns it.
func New(db *treedb.DB) *Store {
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
// A returned storage error can be commit-ambiguous, but visibility is always
// whole-batch: readers may see all mutations or none, never a prefix.
func (s *Store) CommitAt(timestamp uint64, mutations []Mutation, mode CommitMode) error {
	if timestamp == 0 {
		return ErrZeroTimestamp
	}
	if mode != CommitRelaxed && mode != CommitDurable {
		return ErrInvalidCommitMode
	}
	if s == nil || s.db == nil {
		return fmt.Errorf("mvcc: create batch: %w", treedb.ErrClosed)
	}
	if mode == CommitDurable && !durableTreeDBMode(s.db.DurabilityMode()) {
		return fmt.Errorf("%w: configured mode %q", ErrDurabilityUnavailable, s.db.DurabilityMode())
	}
	if len(mutations) == 0 {
		return nil
	}

	type stagedMutation struct {
		physical []byte
		record   []byte
	}
	staged := make([]stagedMutation, len(mutations))
	var seen map[string]struct{}
	if len(mutations) > 1 {
		seen = make(map[string]struct{}, len(mutations))
	}
	for i := range mutations {
		mutation := &mutations[i]
		if seen != nil {
			identity := string(mutation.Key)
			if _, exists := seen[identity]; exists {
				return fmt.Errorf("%w: key index %d", ErrDuplicateKey, i)
			}
			seen[identity] = struct{}{}
		}

		physical, err := mvcckey.Encode(mutation.Key, timestamp)
		if err != nil {
			return fmt.Errorf("%w at key index %d: %w", ErrInvalidKey, i, err)
		}
		staged[i].physical = physical
		if mutation.Delete {
			staged[i].record = []byte{recordTombstoneV1}
			continue
		}
		record := make([]byte, 1+len(mutation.Value))
		record[0] = recordValueV1
		copy(record[1:], mutation.Value)
		staged[i].record = record
	}

	batch := s.db.NewBatchWithSize(len(staged))
	if batch == nil {
		return fmt.Errorf("mvcc: create batch: %w", treedb.ErrClosed)
	}
	for i := range staged {
		if err := batch.Set(staged[i].physical, staged[i].record); err != nil {
			return errors.Join(fmt.Errorf("mvcc: stage key index %d: %w", i, err), batch.Close())
		}
	}

	var err error
	if mode == CommitDurable {
		err = batch.WriteSync()
	} else {
		err = batch.Write()
	}
	if err != nil {
		err = fmt.Errorf("mvcc: commit batch: %w", err)
	}
	return errors.Join(err, batch.Close())
}

// GetAt returns the newest retained version of logical at or below timestamp.
// The lookup constructs an exact lower bound at timestamp and seeks TreeDB
// directly into that key/time region; it does not materialize key history.
func (s *Store) GetAt(logical []byte, timestamp uint64) (result Result, err error) {
	if timestamp == 0 {
		return Result{}, ErrZeroTimestamp
	}
	if s == nil || s.db == nil {
		return Result{}, fmt.Errorf("mvcc: open iterator: %w", treedb.ErrClosed)
	}

	lower, err := mvcckey.Encode(logical, timestamp)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	upper, err := mvcckey.AppendKeyVersionsUpper(nil, logical)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrInvalidKey, err)
	}
	it, err := s.db.Iterator(lower, upper)
	if err != nil {
		return Result{}, fmt.Errorf("mvcc: open iterator: %w", err)
	}
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("mvcc: close iterator: %w", closeErr))
		}
	}()

	if !it.Valid() {
		if iterErr := it.Error(); iterErr != nil {
			return Result{}, fmt.Errorf("mvcc: iterate: %w", iterErr)
		}
		return Result{State: Absent}, nil
	}

	physical := it.Key()
	decoded, version, decodeErr := mvcckey.Decode(physical)
	if decodeErr != nil || !bytes.Equal(decoded, logical) || version > timestamp {
		if decodeErr == nil {
			decodeErr = errors.New("decoded key or timestamp is outside requested bound")
		}
		return Result{}, fmt.Errorf("%w: %w", ErrMalformedRecord, decodeErr)
	}
	record := it.Value()
	if iterErr := it.Error(); iterErr != nil {
		return Result{}, fmt.Errorf("mvcc: read value: %w", iterErr)
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

func durableTreeDBMode(mode string) bool {
	return mode == "wal_on_sync" || strings.HasPrefix(mode, "wal_on_sync+")
}
