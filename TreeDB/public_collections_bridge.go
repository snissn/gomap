package treedb

import (
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

type bridgeSystemBatch struct {
	db      *DB
	entries []batch.Entry
	closed  bool
}

func (db *DB) collectionsBridge() (caching.BackendDirectBridge, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	if db.cached != nil {
		return db.cached.DirectBridge()
	}
	return db.backend, nil
}

func (db *DB) withCollectionsBridgeWrite(fn func(caching.BackendDirectBridge) error) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.WithBackendDirectWrite(fn)
	}
	return fn(db.backend)
}

// IteratorUnsafe exposes the backend iterator contract for packages that need
// internal deleted-entry visibility. This is an interim bridge for collections
// while cached-native named-root support is implemented.
func (db *DB) IteratorUnsafe(start, end []byte) (iterator.UnsafeIterator, error) {
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.Iterator(start, end)
}

// GetSystem reads directly from the backend system catalog root.
func (db *DB) GetSystem(key []byte) ([]byte, error) {
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.GetSystem(key)
}

// SetSystem writes directly into the backend system catalog root.
func (db *DB) SetSystem(key, value []byte) error {
	return db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		return bridge.SetSystem(key, value)
	})
}

// NewSystemBatch returns a staged system-root batch that commits through the
// synchronized backend bridge when Write/WriteSync is called.
func (db *DB) NewSystemBatch() batch.Interface {
	if err := db.ensureOpen(); err != nil {
		return nil
	}
	return &bridgeSystemBatch{db: db, entries: make([]batch.Entry, 0, 16)}
}

// SystemIterator exposes the backend system catalog iterator.
func (db *DB) SystemIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.SystemIterator(start, end)
}

// SystemRootVersion exposes the backend system-root version counter.
func (db *DB) SystemRootVersion() uint64 {
	if db == nil || db.backend == nil {
		return 0
	}
	return db.backend.SystemRootVersion()
}

// GetAtRoot reads from a backend named root.
func (db *DB) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.GetAtRoot(rootID, key)
}

// GetAtRootAppend reads from a backend named root into the provided buffer.
func (db *DB) GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error) {
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.GetAtRootAppend(rootID, key, dst)
}

// HasAtRoot reports whether a backend named root currently contains the key.
func (db *DB) HasAtRoot(rootID uint64, key []byte) (bool, error) {
	bridge, err := db.collectionsBridge()
	if err != nil {
		return false, err
	}
	return bridge.HasAtRoot(rootID, key)
}

// IteratorAtRoot exposes the backend named-root iterator.
func (db *DB) IteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.IteratorAtRoot(rootID, start, end)
}

// MutateRootWithFormat delegates named-root mutation to the backend.
func (db *DB) MutateRootWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	var out uint64
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootWithFormat(rootID, format, sync, mutateRoot, updateSystem)
		return err
	})
	return out, err
}

// MutateRootsWithFormats delegates multi-root mutation to the backend.
func (db *DB) MutateRootsWithFormats(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	var out []uint64
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootsWithFormats(sync, rootIDs, formats, mutateRoots, updateSystem)
		return err
	})
	return out, err
}

// MutateRootAndUserWithFormat delegates mixed root/user mutation to the backend.
func (db *DB) MutateRootAndUserWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	var out uint64
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootAndUserWithFormat(rootID, format, sync, mutateRoot, mutateUser, updateSystem)
		return err
	})
	return out, err
}

// MutateRootsWithFuncs delegates multi-root mutation to the backend.
func (db *DB) MutateRootsWithFuncs(sync bool, rootIDs []uint64, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	var out []uint64
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootsWithFuncs(sync, rootIDs, mutateRoots, updateSystem)
		return err
	})
	return out, err
}

// MutateRoot delegates single-root mutation to the backend.
func (db *DB) MutateRoot(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	var out uint64
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRoot(rootID, sync, mutateRoot, updateSystem)
		return err
	})
	return out, err
}

// MutateRootAndUser delegates mixed root/user mutation to the backend.
func (db *DB) MutateRootAndUser(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	var out uint64
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootAndUser(rootID, sync, mutateRoot, mutateUser, updateSystem)
		return err
	})
	return out, err
}

func (b *bridgeSystemBatch) Set(key, value []byte) error {
	k := append([]byte{}, key...)
	v := append([]byte{}, value...)
	b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: k, Value: v})
	return nil
}

func (b *bridgeSystemBatch) Delete(key []byte) error {
	k := append([]byte{}, key...)
	b.entries = append(b.entries, batch.Entry{Type: batch.OpDelete, Key: k})
	return nil
}

func (b *bridgeSystemBatch) SetOps(ops []batch.Entry) error {
	for _, op := range ops {
		if op.IsPtr {
			return batch.ErrValueTooLarge
		}
		switch op.Type {
		case batch.OpPut:
			if err := b.Set(op.Key, op.Value); err != nil {
				return err
			}
		case batch.OpDelete:
			if err := b.Delete(op.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *bridgeSystemBatch) write(sync bool) error {
	if b.closed {
		return batch.ErrBatchClosed
	}
	b.closed = true
	return b.db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		target := bridge.NewSystemBatch()
		if err := target.SetOps(b.entries); err != nil {
			_ = target.Close()
			return err
		}
		if sync {
			defer target.Close()
			return target.WriteSync()
		}
		defer target.Close()
		return target.Write()
	})
}

func (b *bridgeSystemBatch) Write() error {
	return b.write(false)
}

func (b *bridgeSystemBatch) WriteSync() error {
	return b.write(true)
}

func (b *bridgeSystemBatch) Close() error {
	b.closed = true
	b.entries = nil
	return nil
}

func (b *bridgeSystemBatch) Replay(fn func(batch.Entry) error) error {
	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *bridgeSystemBatch) GetByteSize() (int, error) {
	size := 0
	for _, entry := range b.entries {
		size += len(entry.Key) + len(entry.Value)
	}
	return size, nil
}
