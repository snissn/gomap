package caching

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

type BackendDirectBridge interface {
	Iterator(start, end []byte) (iterator.UnsafeIterator, error)
	NewBatch() batch.Interface
	GetSystem(key []byte) ([]byte, error)
	SetSystem(key, value []byte) error
	NewSystemBatch() batch.Interface
	SystemIterator(start, end []byte) (iterator.UnsafeIterator, error)
	SystemRootVersion() uint64
	GetAtRoot(rootID uint64, key []byte) ([]byte, error)
	GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error)
	HasAtRoot(rootID uint64, key []byte) (bool, error)
	IteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error)
	MutateRootWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
	MutateRootsWithFormats(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error)
	MutateRootAndUserWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
	MutateRootsWithFuncs(sync bool, rootIDs []uint64, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error)
	MutateRoot(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
	MutateRootAndUser(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error)
}

func (db *DB) directBridge() (BackendDirectBridge, error) {
	if db == nil {
		return nil, errDBClosing
	}
	bridge, ok := db.backend.(BackendDirectBridge)
	if !ok {
		return nil, errors.New("cachingdb: backend does not support direct bridge operations")
	}
	return bridge, nil
}

// WithBackendDirectWrite runs a backend-direct operation while cached writers
// and checkpoints are quiesced. This is an interim compatibility hook for
// collection named-root/system-root plumbing until cached-native support lands.
func (db *DB) WithBackendDirectWrite(fn func(BackendDirectBridge) error) error {
	if db == nil {
		return errDBClosing
	}
	db.waitForCheckpoint()
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	bridge, err := db.directBridge()
	if err != nil {
		return err
	}
	return fn(bridge)
}

// DirectBridge exposes the backend-direct bridge after waiting for any
// checkpoint in progress. Callers must not use the returned bridge concurrently
// with collection/root writes; this is intended for short-lived read/iterator
// compatibility paths only.
func (db *DB) DirectBridge() (BackendDirectBridge, error) {
	if db == nil {
		return nil, errDBClosing
	}
	db.waitForCheckpoint()
	return db.directBridge()
}

var _ BackendDirectBridge = (*backenddb.DB)(nil)
