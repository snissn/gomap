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

type recordingSystemBatch struct {
	target  batch.Interface
	entries []batch.Entry
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

// GetSystem reads the system catalog view visible through this handle.
func (db *DB) GetSystem(key []byte) ([]byte, error) {
	if db.cached != nil {
		return db.cached.GetSystem(key)
	}
	return db.backend.GetSystem(key)
}

// SetSystem stages a system catalog write through this handle.
func (db *DB) SetSystem(key, value []byte) error {
	if err := db.ensureOpen(); err != nil {
		return err
	}
	if db.cached != nil {
		return db.cached.SetSystem(key, value)
	}
	return db.backend.SetSystem(key, value)
}

// NewSystemBatch returns a staged system-root batch for this handle.
func (db *DB) NewSystemBatch() batch.Interface {
	if err := db.ensureOpen(); err != nil {
		return nil
	}
	if db.cached != nil {
		return db.cached.NewSystemBatch()
	}
	if db.backend != nil {
		return db.backend.NewSystemBatch()
	}
	return &bridgeSystemBatch{db: db, entries: make([]batch.Entry, 0, 16)}
}

// SystemIterator exposes the system catalog iterator visible through this handle.
func (db *DB) SystemIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	if db.cached != nil {
		return db.cached.SystemIterator(start, end)
	}
	return db.backend.SystemIterator(start, end)
}

// SystemRootVersion exposes the current system-root cache invalidation token.
func (db *DB) SystemRootVersion() uint64 {
	if db == nil {
		return 0
	}
	if db.cached != nil {
		return db.cached.SystemRootVersion()
	}
	if db.backend == nil {
		return 0
	}
	return db.backend.SystemRootVersion()
}

// GetAtRoot reads from a backend named root.
func (db *DB) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	if db.cached != nil && db.hasBufferedNamedRoot(rootID) {
		return db.bufferedGetAtRoot(rootID, key)
	}
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.GetAtRoot(rootID, key)
}

// GetAtRootAppend reads from a backend named root into the provided buffer.
func (db *DB) GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error) {
	if db.cached != nil && db.hasBufferedNamedRoot(rootID) {
		return db.bufferedGetAtRootAppend(rootID, key, dst)
	}
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.GetAtRootAppend(rootID, key, dst)
}

// HasAtRoot reports whether a backend named root currently contains the key.
func (db *DB) HasAtRoot(rootID uint64, key []byte) (bool, error) {
	if db.cached != nil && db.hasBufferedNamedRoot(rootID) {
		return db.bufferedHasAtRoot(rootID, key)
	}
	bridge, err := db.collectionsBridge()
	if err != nil {
		return false, err
	}
	return bridge.HasAtRoot(rootID, key)
}

// HasPrefixAtRoot reports whether the named root currently contains any
// non-deleted key with the provided prefix.
func (db *DB) HasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error) {
	if db.cached != nil && db.hasBufferedNamedRoot(rootID) {
		return db.bufferedHasPrefixAtRoot(rootID, prefix)
	}
	bridge, err := db.collectionsBridge()
	if err != nil {
		return false, err
	}
	return bridge.HasPrefixAtRoot(rootID, prefix)
}

// IteratorAtRoot exposes the backend named-root iterator.
func (db *DB) IteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	if db.cached != nil && db.hasBufferedNamedRoot(rootID) {
		return db.bufferedIteratorAtRoot(rootID, start, end)
	}
	bridge, err := db.collectionsBridge()
	if err != nil {
		return nil, err
	}
	return bridge.IteratorAtRoot(rootID, start, end)
}

// MutateRootWithFormat delegates named-root mutation to the backend.
func (db *DB) MutateRootWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	if db.cached != nil {
		out, err := db.bufferNamedRootMutations(sync, []uint64{rootID}, []*rootfmt.Format{format}, []func(batch.Interface) error{mutateRoot}, func(sys batch.Interface, newRootIDs []uint64) error {
			if updateSystem == nil {
				return nil
			}
			return updateSystem(sys, newRootIDs[0])
		})
		if err != nil {
			return 0, err
		}
		return out[0], nil
	}
	var out uint64
	wrappedUpdate, applyMirror := db.wrapSystemMirrorUpdateOne(updateSystem)
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootWithFormat(rootID, format, sync, mutateRoot, wrappedUpdate)
		return err
	})
	if err != nil {
		return out, err
	}
	if err := applyMirror(); err != nil {
		return out, err
	}
	return out, nil
}

// MutateRootsWithFormats delegates multi-root mutation to the backend.
func (db *DB) MutateRootsWithFormats(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	if db.cached != nil {
		return db.bufferNamedRootMutations(sync, rootIDs, formats, mutateRoots, updateSystem)
	}
	var out []uint64
	wrappedUpdate, applyMirror := db.wrapSystemMirrorUpdateMany(updateSystem)
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootsWithFormats(sync, rootIDs, formats, mutateRoots, wrappedUpdate)
		return err
	})
	if err != nil {
		return out, err
	}
	if err := applyMirror(); err != nil {
		return out, err
	}
	return out, nil
}

// MutateRootAndUserWithFormat delegates mixed root/user mutation to the backend.
func (db *DB) MutateRootAndUserWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	var out uint64
	wrappedUpdate, applyMirror := db.wrapSystemMirrorUpdateOne(updateSystem)
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootAndUserWithFormat(rootID, format, sync, mutateRoot, mutateUser, wrappedUpdate)
		return err
	})
	if err != nil {
		return out, err
	}
	if err := applyMirror(); err != nil {
		return out, err
	}
	return out, nil
}

// MutateRootsWithFuncs delegates multi-root mutation to the backend.
func (db *DB) MutateRootsWithFuncs(sync bool, rootIDs []uint64, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	if db.cached != nil {
		return db.bufferNamedRootMutations(sync, rootIDs, nil, mutateRoots, updateSystem)
	}
	var out []uint64
	wrappedUpdate, applyMirror := db.wrapSystemMirrorUpdateMany(updateSystem)
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootsWithFuncs(sync, rootIDs, mutateRoots, wrappedUpdate)
		return err
	})
	if err != nil {
		return out, err
	}
	if err := applyMirror(); err != nil {
		return out, err
	}
	return out, nil
}

// MutateRoot delegates single-root mutation to the backend.
func (db *DB) MutateRoot(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	if db.cached != nil {
		out, err := db.bufferNamedRootMutations(sync, []uint64{rootID}, nil, []func(batch.Interface) error{mutateRoot}, func(sys batch.Interface, newRootIDs []uint64) error {
			if updateSystem == nil {
				return nil
			}
			return updateSystem(sys, newRootIDs[0])
		})
		if err != nil {
			return 0, err
		}
		return out[0], nil
	}
	var out uint64
	wrappedUpdate, applyMirror := db.wrapSystemMirrorUpdateOne(updateSystem)
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRoot(rootID, sync, mutateRoot, wrappedUpdate)
		return err
	})
	if err != nil {
		return out, err
	}
	if err := applyMirror(); err != nil {
		return out, err
	}
	return out, nil
}

// MutateRootAndUser delegates mixed root/user mutation to the backend.
func (db *DB) MutateRootAndUser(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	var out uint64
	wrappedUpdate, applyMirror := db.wrapSystemMirrorUpdateOne(updateSystem)
	err := db.withCollectionsBridgeWrite(func(bridge caching.BackendDirectBridge) error {
		var err error
		out, err = bridge.MutateRootAndUser(rootID, sync, mutateRoot, mutateUser, wrappedUpdate)
		return err
	})
	if err != nil {
		return out, err
	}
	if err := applyMirror(); err != nil {
		return out, err
	}
	return out, nil
}

func (db *DB) wrapSystemMirrorUpdateOne(update func(batch.Interface, uint64) error) (func(batch.Interface, uint64) error, func() error) {
	if update == nil || db == nil || db.cached == nil || !db.cached.PendingSystemOverlay() {
		return update, func() error { return nil }
	}
	var recorded []batch.Entry
	wrapped := func(target batch.Interface, rootID uint64) error {
		recorder := &recordingSystemBatch{target: target}
		if err := update(recorder, rootID); err != nil {
			return err
		}
		recorded = recorder.entries
		return nil
	}
	return wrapped, func() error {
		return db.cached.ApplySystemOverlayEntriesOwned(recorded)
	}
}

func (db *DB) wrapSystemMirrorUpdateMany(update func(batch.Interface, []uint64) error) (func(batch.Interface, []uint64) error, func() error) {
	if update == nil || db == nil || db.cached == nil || !db.cached.PendingSystemOverlay() {
		return update, func() error { return nil }
	}
	var recorded []batch.Entry
	wrapped := func(target batch.Interface, rootIDs []uint64) error {
		recorder := &recordingSystemBatch{target: target}
		if err := update(recorder, rootIDs); err != nil {
			return err
		}
		recorded = recorder.entries
		return nil
	}
	return wrapped, func() error {
		return db.cached.ApplySystemOverlayEntriesOwned(recorded)
	}
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

func (b *recordingSystemBatch) Set(key, value []byte) error {
	if err := b.target.Set(key, value); err != nil {
		return err
	}
	b.entries = append(b.entries, batch.Entry{
		Type:  batch.OpPut,
		Key:   key,
		Value: value,
	})
	return nil
}

func (b *recordingSystemBatch) Delete(key []byte) error {
	if err := b.target.Delete(key); err != nil {
		return err
	}
	b.entries = append(b.entries, batch.Entry{
		Type: batch.OpDelete,
		Key:  key,
	})
	return nil
}

func (b *recordingSystemBatch) SetOps(ops []batch.Entry) error {
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

func (b *recordingSystemBatch) Write() error {
	return b.target.Write()
}

func (b *recordingSystemBatch) WriteSync() error {
	return b.target.WriteSync()
}

func (b *recordingSystemBatch) Close() error {
	return b.target.Close()
}

func (b *recordingSystemBatch) Replay(fn func(batch.Entry) error) error {
	for _, entry := range b.entries {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (b *recordingSystemBatch) GetByteSize() (int, error) {
	size := 0
	for _, entry := range b.entries {
		size += len(entry.Key) + len(entry.Value)
	}
	return size, nil
}
