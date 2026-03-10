package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/tree"
)

type systemRootPublishStats struct {
	warmAttempts         uint64
	warmRebuildFallbacks uint64
}

type systemRootPublishPlan uint8

const (
	systemRootPublishPlanColdBuild systemRootPublishPlan = iota
	systemRootPublishPlanWarmFallbackRebuild
	systemRootPublishPlanWarmNativeApply
)

func (db *DB) systemRootPublishStatsSnapshot() systemRootPublishStats {
	if db == nil {
		return systemRootPublishStats{}
	}
	return systemRootPublishStats{
		warmAttempts:         db.systemRootWarmPublishAttempts.Load(),
		warmRebuildFallbacks: db.systemRootWarmPublishRebuildFallbacks.Load(),
	}
}

func selectSystemRootPublishPlan(hasExistingEntries bool) systemRootPublishPlan {
	if !hasExistingEntries {
		return systemRootPublishPlanColdBuild
	}
	// R4 scaffolding: until warm native apply exists, non-empty steady-state
	// system-root publishes explicitly select rebuild fallback.
	return systemRootPublishPlanWarmFallbackRebuild
}

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

		sysIter := sysTree.Iterator(nil, nil)
		warm := sysIter.Valid()
		iterErr := sysIter.Error()
		sysIter.Close()
		if iterErr != nil {
			return 0, iterErr
		}
		switch selectSystemRootPublishPlan(warm) {
		case systemRootPublishPlanWarmFallbackRebuild:
			db.systemRootWarmPublishAttempts.Add(1)
			// R4 scaffolding: warm native apply is not implemented yet, so
			// steady-state publishes over an existing non-empty system root
			// explicitly record rebuild fallback selection and use the current
			// full-build path below.
			db.systemRootWarmPublishRebuildFallbacks.Add(1)
		case systemRootPublishPlanWarmNativeApply:
			db.systemRootWarmPublishAttempts.Add(1)
		}
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

	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, adaptive.Metrics{}, nil, true, nil); err != nil {
		return 0, err
	}
	return newSystemRoot, nil
}
