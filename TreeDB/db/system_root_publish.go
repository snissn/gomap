package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type systemRootPublishStats struct {
	warmAttempts            uint64
	warmNativeApplyAttempts uint64
	warmRebuildFallbacks    uint64
	warmPreservedPages      uint64
	warmRewrittenPages      uint64
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
		warmAttempts:            db.systemRootWarmPublishAttempts.Load(),
		warmNativeApplyAttempts: db.systemRootWarmNativeApplyAttempts.Load(),
		warmRebuildFallbacks:    db.systemRootWarmPublishRebuildFallbacks.Load(),
		warmPreservedPages:      db.systemRootWarmPreservedPages.Load(),
		warmRewrittenPages:      db.systemRootWarmRewrittenPages.Load(),
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

const defaultSystemRootWarmMaxDeltaOps = 256

func (db *DB) systemRootWarmMaxDeltaOps() int {
	if db != nil && db.testSystemRootWarmMaxDeltaOps > 0 {
		return db.testSystemRootWarmMaxDeltaOps
	}
	return defaultSystemRootWarmMaxDeltaOps
}

func selectSystemRootWarmPublishPlan(hasExistingEntries bool, deltaOps int, maxDeltaOps int) systemRootPublishPlan {
	switch selectOrderedRootWarmPublishPlan(hasExistingEntries, deltaOps, maxDeltaOps) {
	case orderedRootPublishPlanColdBuild:
		return systemRootPublishPlanColdBuild
	case orderedRootPublishPlanWarmNativeApply:
		return systemRootPublishPlanWarmNativeApply
	default:
		return systemRootPublishPlanWarmFallbackRebuild
	}
}

func systemRootOrderedPublishOptions(db *DB) orderedRootPublishOptions {
	return orderedRootPublishOptions{
		maxWarmDeltaOps:       db.systemRootWarmMaxDeltaOps(),
		leafPrefixCompression: db.leafPrefixCompression,
		leafColumnar:          db.indexColumnarLeaves,
		packedValuePtr:        db.indexPackedValuePtr,
		internalBaseDelta:     db.indexInternalBaseDelta,
	}
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

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if db.readOnly {
		return 0, ErrReadOnly
	}
	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()

	newSystemRoot, retired, metrics, publishStats, vlogRefDelta, err := db.publishOrderedRootIterator(baseSystemRoot, iter, systemRootOrderedPublishOptions(db), true)
	if err != nil {
		return 0, err
	}
	db.systemRootWarmPublishAttempts.Add(publishStats.warmAttempts)
	db.systemRootWarmNativeApplyAttempts.Add(publishStats.warmNativeApplyAttempts)
	db.systemRootWarmPublishRebuildFallbacks.Add(publishStats.warmRebuildFallbacks)
	db.systemRootWarmPreservedPages.Add(publishStats.warmPreservedPages)
	db.systemRootWarmRewrittenPages.Add(publishStats.warmRewrittenPages)

	db.mu.Lock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.Unlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, errors.New("concurrent modification detected during system root publish")
	}

	if err := db.finalizeCommit(userRoot, newSystemRoot, retired, false, metrics, nil, true, vlogRefDelta); err != nil {
		return 0, err
	}
	return newSystemRoot, nil
}
