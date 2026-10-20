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

const defaultSystemRootWarmMaxDeltaOps = 256

func (db *DB) systemRootWarmMaxDeltaOps() int {
	if db != nil && db.testSystemRootWarmMaxDeltaOps > 0 {
		return db.testSystemRootWarmMaxDeltaOps
	}
	return defaultSystemRootWarmMaxDeltaOps
}

func systemRootOrderedPublishOptions(db *DB) orderedRootPublishOptions {
	return orderedRootPublishOptions{
		maxWarmDeltaOps:       db.systemRootWarmMaxDeltaOps(),
		leafPrefixCompression: db.leafPrefixCompression,
		leafColumnar:          db.indexColumnarLeaves,
		packedValuePtr:        db.indexPackedValuePtr,
		internalBaseDelta:     db.indexInternalBaseDelta,
		outerLeavesInValueLog: db.indexOuterLeavesInValueLog,
		leafPageLog:           db.leafPageLog,
	}
}

// PublishSystemRootIterator builds and commits a new system root from an
// ordered iterator without detached-batch replay. The current user root is
// preserved across the commit.
func (db *DB) PublishSystemRootIterator(iter iterator.UnsafeIterator) (uint64, error) {
	if db == nil {
		return 0, ErrClosed
	}
	if db.closing.Load() {
		return 0, ErrClosed
	}
	if err := db.rejectUnloggedCommandWALRootPublish(); err != nil {
		return 0, err
	}
	if iter == nil {
		return 0, errors.New("nil system root iterator")
	}

	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return 0, err
	}

	if db.readOnly {
		return 0, ErrReadOnly
	}
	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	ptrCollector, collectedIter := newPendingValueLogAppendPtrCollectingIterator(iter)
	defer db.releasePendingValueLogAppendPtrCollector(ptrCollector)
	newSystemRoot, retired, metrics, publishStats, vlogRefDelta, err := db.publishOrderedRootIterator(baseSystemRoot, collectedIter, systemRootOrderedPublishOptions(db), true)
	if err != nil {
		return 0, err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()
	db.systemRootWarmPublishAttempts.Add(publishStats.warmAttempts)
	db.systemRootWarmNativeApplyAttempts.Add(publishStats.warmNativeApplyAttempts)
	db.systemRootWarmPublishRebuildFallbacks.Add(publishStats.warmRebuildFallbacks)
	db.systemRootWarmPreservedPages.Add(publishStats.warmPreservedPages)
	db.systemRootWarmRewrittenPages.Add(publishStats.warmRewrittenPages)
	touchedValueLogSegments := positiveValueLogRefDeltaFileIDs(vlogRefDelta, nil)
	if publishStats.collectionRootDescriptorReachabilityMayChange() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		vlogRefDelta = nil
	}

	db.mu.Lock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.Unlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, errors.New("concurrent modification detected during system root publish")
	}

	post, err := db.finalizeCommitReleasingRootSerialization(
		userRoot, newSystemRoot, retired, false, metrics, touchedValueLogSegments,
		true, vlogRefDelta, nil, nil, finalizeCommitOptions{},
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return 0, err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	return newSystemRoot, nil
}
