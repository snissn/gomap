package db

import (
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
	"sync"
)

// CollectionRootRelocationPrepare prepares snapshot-free collection metadata
// under nonblocking admission gates at online vacuum cutover. The snapshot and
// certified old-to-new root map are borrowed only for this call. The returned
// completion installs prepared metadata on success and releases admission on
// either outcome. It must not fail or call back into DB mutation.
type CollectionRootRelocationPrepare func(*Snapshot, *pager.Pager, map[uint64]uint64) (func(bool), error)

// RegisterCollectionRootRelocation binds the one collections coordinator to its
// backend lifetime. Unregister waits for any accepted cutover to complete.
func (db *DB) RegisterCollectionRootRelocation(prepare CollectionRootRelocationPrepare) func() {
	db.collectionRelocationMu.Lock()
	db.collectionRelocationID++
	id := db.collectionRelocationID
	db.collectionRelocation = prepare
	db.collectionRelocationMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			db.collectionRelocationMu.Lock()
			defer db.collectionRelocationMu.Unlock()
			if db.collectionRelocationID == id {
				db.collectionRelocation = nil
			}
		})
	}
}

func (db *DB) prepareCollectionRelocation(snap *Snapshot, next *pager.Pager, roots map[uint64]uint64) (func(bool), error) {
	if !db.collectionRelocationMu.TryLock() {
		return nil, rootpublication.ErrResourcePinned
	}
	if db.collectionRelocation == nil {
		// First registration must also wait for this cutover; otherwise it can
		// install old-pager collection authority after the empty prepare.
		return func(bool) { db.collectionRelocationMu.Unlock() }, nil
	}
	finish, err := db.collectionRelocation(snap, next, roots)
	if err != nil {
		db.collectionRelocationMu.Unlock()
		return nil, err
	}
	return func(committed bool) { defer db.collectionRelocationMu.Unlock(); finish(committed) }, nil
}
