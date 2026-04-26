package db

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/tree"
)

// ErrNilUpdateFunc indicates a nil callback was passed to Update.
var ErrNilUpdateFunc = errors.New("treedb: nil update function")

// ErrUpdateValueNil indicates an Update callback requested Set with a nil value.
var ErrUpdateValueNil = errors.New("treedb: value cannot be nil")

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
// value is nil when the key is absent and is a safe copy when present. The
// callback may be retried if the key changes before the mutation commits.
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
	stableKey := cloneUpdateKey(key)

	for {
		updateMu := db.lockUpdateKey(stableKey)
		old, err := db.getForUpdate(stableKey)
		unlockUpdateKey(updateMu)
		if err != nil {
			return err
		}
		observed := cloneUpdateValue(old)

		result, err := fn(old)
		if err != nil {
			return err
		}
		if result.Op == UpdateSet && result.Value == nil {
			return ErrUpdateValueNil
		}
		if result.Op != UpdateNoop && result.Op != UpdateSet && result.Op != UpdateDelete {
			return fmt.Errorf("treedb: unknown update op %d", result.Op)
		}

		updateMu = db.lockUpdateKey(stableKey)
		latest, err := db.getForUpdate(stableKey)
		if err != nil {
			unlockUpdateKey(updateMu)
			return err
		}
		if !sameUpdateValue(observed, latest) {
			unlockUpdateKey(updateMu)
			continue
		}

		switch result.Op {
		case UpdateNoop:
			unlockUpdateKey(updateMu)
			return nil
		case UpdateSet:
			err = db.setPoint(stableKey, result.Value, syncWrite)
			unlockUpdateKey(updateMu)
			return err
		case UpdateDelete:
			err = db.deletePoint(stableKey, syncWrite)
			unlockUpdateKey(updateMu)
			return err
		default:
			unlockUpdateKey(updateMu)
			return fmt.Errorf("treedb: unknown update op %d", result.Op)
		}
	}
}

func cloneUpdateKey(key []byte) []byte {
	return append([]byte{}, key...)
}

func (db *DB) lockUpdateKey(key []byte) *sync.Mutex {
	if db == nil {
		return nil
	}
	return db.updateLocks.LockMutex(key)
}

func unlockUpdateKey(mu *sync.Mutex) {
	if mu != nil {
		mu.Unlock()
	}
}

func (db *DB) getForUpdate(key []byte) ([]byte, error) {
	dst := make([]byte, 0)
	old, err := db.GetAppend(key, dst)
	if err == tree.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return old[:len(old):len(old)], nil
}

func cloneUpdateValue(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func sameUpdateValue(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a, b)
}
