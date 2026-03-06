package treedb

import (
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/rootfmt"
)

type collectionManagerAdapter struct {
	db *DB
}

type collectionReplayBatch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Write() error
	WriteSync() error
	Close() error
	Replay(func(batch.Entry) error) error
	GetByteSize() (int, error)
}

type collectionPublicBatchAdapter struct {
	batch collectionReplayBatch
}

// NewCollectionManager binds the collections layer to the public cached TreeDB
// handle. This is an interim bridge that keeps backend-direct collection logic
// usable while cached-native named-root support is implemented.
func NewCollectionManager(database *DB) *collections.CollectionManager {
	return collections.NewCollectionManager(&collectionManagerAdapter{db: database})
}

func (a *collectionManagerAdapter) Get(key []byte) ([]byte, error) {
	return a.db.Get(key)
}

func (a *collectionManagerAdapter) HasAtRoot(rootID uint64, key []byte) (bool, error) {
	return a.db.HasAtRoot(rootID, key)
}

func (a *collectionManagerAdapter) HasPrefixAtRoot(rootID uint64, prefix []byte) (bool, error) {
	return a.db.HasPrefixAtRoot(rootID, prefix)
}

func (a *collectionManagerAdapter) GetAtRoot(rootID uint64, key []byte) ([]byte, error) {
	return a.db.GetAtRoot(rootID, key)
}

func (a *collectionManagerAdapter) GetAtRootAppend(rootID uint64, key, dst []byte) ([]byte, error) {
	return a.db.GetAtRootAppend(rootID, key, dst)
}

func (a *collectionManagerAdapter) Set(key, value []byte) error {
	return a.db.Set(key, value)
}

func (a *collectionManagerAdapter) Delete(key []byte) error {
	return a.db.Delete(key)
}

func (a *collectionManagerAdapter) GetSystem(key []byte) ([]byte, error) {
	return a.db.GetSystem(key)
}

func (a *collectionManagerAdapter) SetSystem(key, value []byte) error {
	return a.db.SetSystem(key, value)
}

func (a *collectionManagerAdapter) NewBatch() batch.Interface {
	return &collectionPublicBatchAdapter{batch: a.db.NewBatch()}
}

func (a *collectionManagerAdapter) NewSystemBatch() batch.Interface {
	return a.db.NewSystemBatch()
}

func (a *collectionManagerAdapter) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return a.db.IteratorUnsafe(start, end)
}

func (a *collectionManagerAdapter) IteratorAtRoot(rootID uint64, start, end []byte) (iterator.UnsafeIterator, error) {
	return a.db.IteratorAtRoot(rootID, start, end)
}

func (a *collectionManagerAdapter) SystemIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	return a.db.SystemIterator(start, end)
}

func (a *collectionManagerAdapter) MutateRootsWithFormatOps(sync bool, rootIDs []uint64, formats []*rootfmt.Format, rootOps [][]batch.Entry, buildSystemOps func([]uint64) ([]batch.Entry, error)) ([]uint64, error) {
	return a.db.MutateRootsWithFormatOps(sync, rootIDs, formats, rootOps, buildSystemOps)
}

func (a *collectionManagerAdapter) MutateRootsWithFormats(sync bool, rootIDs []uint64, formats []*rootfmt.Format, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	return a.db.MutateRootsWithFormats(sync, rootIDs, formats, mutateRoots, updateSystem)
}

func (a *collectionManagerAdapter) MutateRootWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return a.db.MutateRootWithFormat(rootID, format, sync, mutateRoot, updateSystem)
}

func (a *collectionManagerAdapter) MutateRootAndUserWithFormat(rootID uint64, format *rootfmt.Format, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return a.db.MutateRootAndUserWithFormat(rootID, format, sync, mutateRoot, mutateUser, updateSystem)
}

func (a *collectionManagerAdapter) MutateRootsWithFuncs(sync bool, rootIDs []uint64, mutateRoots []func(batch.Interface) error, updateSystem func(batch.Interface, []uint64) error) ([]uint64, error) {
	return a.db.MutateRootsWithFuncs(sync, rootIDs, mutateRoots, updateSystem)
}

func (a *collectionManagerAdapter) MutateRoot(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return a.db.MutateRoot(rootID, sync, mutateRoot, updateSystem)
}

func (a *collectionManagerAdapter) MutateRootAndUser(rootID uint64, sync bool, mutateRoot func(batch.Interface) error, mutateUser func(batch.Interface) error, updateSystem func(batch.Interface, uint64) error) (uint64, error) {
	return a.db.MutateRootAndUser(rootID, sync, mutateRoot, mutateUser, updateSystem)
}

func (a *collectionManagerAdapter) SystemRootVersion() uint64 {
	return a.db.SystemRootVersion()
}

func (a *collectionPublicBatchAdapter) Set(key, value []byte) error {
	return a.batch.Set(key, value)
}

func (a *collectionPublicBatchAdapter) Delete(key []byte) error {
	return a.batch.Delete(key)
}

func (a *collectionPublicBatchAdapter) SetOps(ops []batch.Entry) error {
	for _, op := range ops {
		switch op.Type {
		case batch.OpPut:
			if op.IsPtr {
				return batch.ErrValueTooLarge
			}
			if err := a.batch.Set(op.Key, op.Value); err != nil {
				return err
			}
		case batch.OpDelete:
			if err := a.batch.Delete(op.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *collectionPublicBatchAdapter) Write() error {
	return a.batch.Write()
}

func (a *collectionPublicBatchAdapter) WriteSync() error {
	return a.batch.WriteSync()
}

func (a *collectionPublicBatchAdapter) Close() error {
	return a.batch.Close()
}

func (a *collectionPublicBatchAdapter) Replay(fn func(batch.Entry) error) error {
	return a.batch.Replay(fn)
}

func (a *collectionPublicBatchAdapter) GetByteSize() (int, error) {
	return a.batch.GetByteSize()
}
