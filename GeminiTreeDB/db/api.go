package db

import (
	"fmt"

	"github.com/snissn/gomap-gemini/TreeDB/internal/iterator"
	"github.com/snissn/gomap-gemini/TreeDB/tree"
)

// --- Public API ---

// Get returns the value for a key.
func (db *DB) Get(key []byte) ([]byte, error) {
	snap := db.AcquireSnapshot()
	defer snap.Close()
	val, err := snap.Get(key)
	if err == tree.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// Has checks if a key exists.
func (db *DB) Has(key []byte) (bool, error) {
	val, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return val != nil, nil
}

// Set sets the value for a key.
func (db *DB) Set(key, value []byte) error {
	batch := db.NewBatch()
	if err := batch.Set(key, value); err != nil {
		return err
	}
	return batch.Write() // Using Write for better throughput (async)
}

// SetSync sets the value and syncs to disk.
func (db *DB) SetSync(key, value []byte) error {
	batch := db.NewBatch()
	if err := batch.Set(key, value); err != nil {
		return err
	}
	return batch.WriteSync()
}

// Delete removes a key.
func (db *DB) Delete(key []byte) error {
	batch := db.NewBatch()
	if err := batch.Delete(key); err != nil {
		return err
	}
	return batch.Write() // Using Write for better throughput (async)
}

// DeleteSync removes a key and syncs.
func (db *DB) DeleteSync(key []byte) error {
	batch := db.NewBatch()
	if err := batch.Delete(key); err != nil {
		return err
	}
	return batch.WriteSync()
}

// DBIterator wraps tree.Iterator and holds a Snapshot.
type DBIterator struct {
	snap *Snapshot
	iter iterator.UnsafeIterator
}

func (it *DBIterator) Next() {
	it.iter.Next()
}

func (it *DBIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *DBIterator) Key() []byte {
	return it.iter.Key()
}

func (it *DBIterator) Value() []byte {
	return it.iter.Value()
}

func (it *DBIterator) Error() error {
	return it.iter.Error()
}

func (it *DBIterator) Close() error {
	err := it.iter.Close()
	if e := it.snap.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	return err
}

// UnsafeIterator methods
func (it *DBIterator) Seek(key []byte) {
	it.iter.Seek(key)
}

func (it *DBIterator) UnsafeKey() []byte {
	return it.iter.UnsafeKey()
}

func (it *DBIterator) UnsafeValue() []byte {
	return it.iter.UnsafeValue()
}

func (it *DBIterator) IsDeleted() bool {
	return it.iter.IsDeleted()
}

func (it *DBIterator) Domain() (start, end []byte) {
	return it.iter.Domain()
}

// Iterator returns an iterator.
func (db *DB) Iterator(start, end []byte) (iterator.UnsafeIterator, error) {
	snap := db.AcquireSnapshot()
	it := snap.tree.Iterator(start, end)
	return &DBIterator{snap: snap, iter: it}, nil
}

// ReverseIterator returns a reverse iterator.
func (db *DB) ReverseIterator(start, end []byte) (iterator.UnsafeIterator, error) {
	snap := db.AcquireSnapshot()
	it := snap.tree.ReverseIterator(start, end)
	return &DBIterator{snap: snap, iter: it}, nil
}

// Stats returns database statistics.
func (db *DB) Stats() map[string]string {
	stats := make(map[string]string)
	stats["cosmos.db.type"] = "treedb"

	state := db.state.Load()
	stats["treedb.commit_seq"] = fmt.Sprintf("%d", state.CommitSeq)
	stats["treedb.root_page"] = fmt.Sprintf("%d", state.RootPageID)

	stats["treedb.pages.total"] = fmt.Sprintf("%d", db.pager.PageCount())

	stats["treedb.slabs.active_id"] = fmt.Sprintf("%d", db.slabManager.ActiveSlabID())
	stats["treedb.slabs.zombies"] = fmt.Sprintf("%d", db.slabManager.ZombieCount())

	return stats
}

// Print debugs the tree (simple dump).
func (db *DB) Print() error {
	// Not implemented fully
	return nil
}
