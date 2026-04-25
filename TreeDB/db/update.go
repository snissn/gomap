package db

import (
	"errors"
	"fmt"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
)

// ErrNilUpdateFunc indicates a nil callback was passed to Update.
var ErrNilUpdateFunc = errors.New("treedb: nil update function")

// ErrUpdateValueNil indicates an Update callback requested Set with a nil value.
var ErrUpdateValueNil = errors.New("value cannot be nil")

// UpdateOp describes the write produced by an Update callback.
type UpdateOp uint8

const (
	// UpdateNoop leaves the key unchanged.
	UpdateNoop UpdateOp = iota
	// UpdateSet replaces the key with Value.
	UpdateSet
	// UpdateDelete removes the key.
	UpdateDelete
)

// UpdateResult is returned by an Update callback.
type UpdateResult struct {
	Op    UpdateOp
	Value []byte
}

// SetUpdate returns an Update result that replaces the key with value.
func SetUpdate(value []byte) UpdateResult {
	return UpdateResult{Op: UpdateSet, Value: value}
}

// DeleteUpdate returns an Update result that removes the key.
func DeleteUpdate() UpdateResult {
	return UpdateResult{Op: UpdateDelete}
}

// NoopUpdate returns an Update result that leaves the key unchanged.
func NoopUpdate() UpdateResult {
	return UpdateResult{Op: UpdateNoop}
}

// UpdateFunc transforms the current value for a key into a mutation. The old
// value is nil when the key is absent and is a safe copy when present.
type UpdateFunc func(old []byte) (UpdateResult, error)

// Update applies fn to the current value for key and writes the returned
// mutation without forcing an fsync boundary.
func (db *DB) Update(key []byte, fn UpdateFunc) error {
	return db.update(key, fn, false)
}

// UpdateSync applies fn to the current value for key and writes the returned
// mutation with a sync durability boundary.
func (db *DB) UpdateSync(key []byte, fn UpdateFunc) error {
	return db.update(key, fn, true)
}

func (db *DB) update(key []byte, fn UpdateFunc, syncWrite bool) error {
	if db == nil {
		return ErrClosed
	}
	if len(key) == 0 {
		return batchpkg.ErrKeyEmpty
	}
	if fn == nil {
		return ErrNilUpdateFunc
	}
	if db.readOnly {
		return ErrReadOnly
	}

	unlock := db.lockUpdateKey(key)
	defer unlock()

	old, err := db.Get(key)
	if err != nil {
		return err
	}
	result, err := fn(old)
	if err != nil {
		return err
	}
	switch result.Op {
	case UpdateNoop:
		return nil
	case UpdateSet:
		if result.Value == nil {
			return ErrUpdateValueNil
		}
		return db.setPoint(key, result.Value, syncWrite)
	case UpdateDelete:
		return db.deletePoint(key, syncWrite)
	default:
		return fmt.Errorf("treedb: unknown update op %d", result.Op)
	}
}

func (db *DB) lockUpdateKey(key []byte) func() {
	if db == nil {
		return func() {}
	}
	return db.updateLocks.Lock(key)
}
