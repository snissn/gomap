package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/tree"
)

// PublishSystemRootIterator builds and commits a new system root from an
// ordered iterator without detached-batch replay. The current user root is
// preserved across the commit.
func (db *DB) PublishSystemRootIterator(iter iterator.UnsafeIterator) (uint64, error) {
	if db == nil {
		return 0, ErrClosed
	}
	if iter == nil {
		return 0, errors.New("nil system root iterator")
	}
	defer iter.Close()

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, ErrReadOnly
	}
	idx := db.idx.Load()
	if idx == nil {
		return 0, errors.New("missing index")
	}

	db.mu.RLock()
	state := db.state.Load()
	userRoot := db.meta.UserRootPageID
	systemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()

	retired := make([]uint64, 0)
	if systemRoot != 0 {
		sysTree := tree.New(idx.pager, newValueReader(state.ValueLogSet), systemRoot)
		pageIDs, err := sysTree.CollectPageIDs()
		if err != nil {
			return 0, err
		}
		retired = append(retired, pageIDs...)
	}

	newSystemRoot, err := bulk.BuildWithOptions(iter, &pagerAllocator{p: idx.pager}, idx.pager, bulk.BuildOptions{
		LeafPrefixCompression: db.leafPrefixCompression,
		LeafColumnar:          db.indexColumnarLeaves,
		PackedValuePtr:        db.indexPackedValuePtr,
		InternalBaseDelta:     db.indexInternalBaseDelta,
	})
	if err != nil {
		return 0, err
	}

	db.mu.Lock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.Unlock()
	if curUserRoot != userRoot || curSystemRoot != systemRoot {
		return 0, errors.New("concurrent modification detected during system root publish")
	}

	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, adaptive.Metrics{}, nil, true, nil, nil, nil); err != nil {
		return 0, err
	}
	return newSystemRoot, nil
}
