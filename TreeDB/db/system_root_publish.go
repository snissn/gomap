package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
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

// leafPageNoDictLog is implemented by leaf-page logs that can disable
// outer-leaf dictionary encoding per append call without mutating shared
// writer state (so SystemRootPageID outer leaves stay dictID=0 safely alongside
// concurrent dictionary-encoded user/collection leaf appends).
type leafPageNoDictLog interface {
	AppendLeafPageNoDict(leafPage []byte) (page.LeafLogPtr, error)
	AppendLeafPagesNoDict(leafPages [][]byte) ([]page.LeafLogPtr, error)
}

type leafPageNoDictStableLog interface {
	AppendLeafPageWithStableResourcesNoDict(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error)
	AppendLeafPagesWithStableResourcesNoDict(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error)
}

type leafPageNoDictCapable interface {
	supportsLeafPageNoDict() bool
}

type noDictRewriteLeafPageLog struct {
	base bulk.LeafPageAppender
}

func wrapNoDictLeafPageLog(log bulk.LeafPageAppender) bulk.LeafPageAppender {
	if log == nil {
		return nil
	}
	if _, ok := log.(*noDictRewriteLeafPageLog); ok {
		return log
	}
	if _, ok := log.(leafPageNoDictLog); ok {
		if capable, hasCap := log.(leafPageNoDictCapable); hasCap && !capable.supportsLeafPageNoDict() {
			return log
		}
		return &noDictRewriteLeafPageLog{base: log}
	}
	return log
}

func (l *noDictRewriteLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if l == nil || l.base == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	if noDict, ok := l.base.(leafPageNoDictLog); ok {
		return noDict.AppendLeafPageNoDict(leafPage)
	}
	return l.base.AppendLeafPage(leafPage)
}

func (l *noDictRewriteLeafPageLog) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.base == nil {
		return page.LeafLogPtr{}, nil, errors.New("leaf page log unavailable")
	}
	if noDict, ok := l.base.(leafPageNoDictStableLog); ok {
		return noDict.AppendLeafPageWithStableResourcesNoDict(leafPage)
	}
	stable, ok := l.base.(LeafPageStableLog)
	if !ok {
		return page.LeafLogPtr{}, nil, rootpublication.ErrUnresolvedResource
	}
	return stable.AppendLeafPageWithStableResources(leafPage)
}

func (l *noDictRewriteLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if l == nil || l.base == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	if noDict, ok := l.base.(leafPageNoDictLog); ok {
		return noDict.AppendLeafPagesNoDict(leafPages)
	}
	if batcher, ok := l.base.(LeafPageBatchLog); ok {
		return batcher.AppendLeafPages(leafPages)
	}
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	for i, lp := range leafPages {
		ptr, err := l.base.AppendLeafPage(lp)
		if err != nil {
			return nil, err
		}
		ptrs[i] = ptr
	}
	return ptrs, nil
}

func (l *noDictRewriteLeafPageLog) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if l == nil || l.base == nil {
		return nil, nil, errors.New("leaf page log unavailable")
	}
	if noDict, ok := l.base.(leafPageNoDictStableLog); ok {
		return noDict.AppendLeafPagesWithStableResourcesNoDict(leafPages)
	}
	stableBatch, ok := l.base.(LeafPageStableBatchLog)
	if !ok {
		return nil, nil, rootpublication.ErrUnresolvedResource
	}
	return stableBatch.AppendLeafPagesWithStableResources(leafPages)
}

func (l *noDictRewriteLeafPageLog) ConcurrentLeafPageAppends() bool {
	if l == nil || l.base == nil {
		return false
	}
	concurrent, ok := l.base.(LeafPageConcurrentAppendLog)
	return ok && concurrent.ConcurrentLeafPageAppends()
}

func (l *noDictRewriteLeafPageLog) LeafPageLogLane(workerIndex int) (LeafPageLog, bool) {
	if l == nil || l.base == nil {
		return nil, false
	}
	provider, ok := l.base.(LeafPageLogLaneProvider)
	if !ok {
		return nil, false
	}
	lane, ok := provider.LeafPageLogLane(workerIndex)
	if !ok || lane == nil {
		return nil, false
	}
	if _, already := lane.(*noDictRewriteLeafPageLog); already {
		return lane, true
	}
	return &noDictRewriteLeafPageLog{base: lane}, true
}

func (l *noDictRewriteLeafPageLog) LeafPageLogLaneAny(workerIndex int) (any, bool) {
	return l.LeafPageLogLane(workerIndex)
}

func (l *noDictRewriteLeafPageLog) Flush() error {
	if l == nil || l.base == nil {
		return nil
	}
	if flusher, ok := l.base.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

func (l *noDictRewriteLeafPageLog) Sync() error {
	if l == nil || l.base == nil {
		return nil
	}
	if syncer, ok := l.base.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return nil
}

func systemRootOrderedPublishOptions(db *DB) orderedRootPublishOptions {
	opts := defaultOrderedRootPublishOptions(db)
	opts.outerLeavesInValueLog = db.systemRootUsesOuterLeaves()
	if !opts.outerLeavesInValueLog {
		opts.leafPageLog = nil
	} else {
		// SystemRootPageID outer leaves written through a maintenance/compact
		// rewriteWriter must stay non-dictionary-encoded (dictID=0) so reading
		// dict/bytes/<dictID> from SystemRootPageID can never self-recurse.
		opts.leafPageLog = wrapNoDictLeafPageLog(opts.leafPageLog)
	}
	return opts
}

func (db *DB) systemRootUsesOuterLeaves() bool {
	return db.indexOuterLeavesInValueLog && !db.indexSystemLeavesInPager
}

func defaultOrderedRootPublishOptions(db *DB) orderedRootPublishOptions {
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
