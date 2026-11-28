package db

import (
	"errors"
	"sync"

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

type noDictRewriteLeafPageLog struct {
	mu      sync.Mutex
	base    bulk.LeafPageAppender
	writers []*rewriteWriter
}

func collectRewriteWriters(log any) []*rewriteWriter {
	var out []*rewriteWriter
	var visit func(any)
	visit = func(cur any) {
		switch v := cur.(type) {
		case nil:
			return
		case *rewriteWriter:
			if v != nil {
				out = append(out, v)
			}
		case *leafPageLogWithRecordLengthHints:
			if v != nil {
				visit(v.inner)
			}
		case *leafPageLogLaneGroup:
			if v != nil {
				for _, lane := range v.lanes {
					visit(lane)
				}
			}
		case *noDictRewriteLeafPageLog:
			if v != nil {
				out = append(out, v.writers...)
			}
		}
	}
	visit(log)
	return out
}

func (l *noDictRewriteLeafPageLog) withoutLeafDict(fn func() error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	type savedState struct {
		id   uint64
		dict []byte
	}
	saved := make([]savedState, len(l.writers))
	for i, w := range l.writers {
		saved[i] = savedState{id: w.leafDictID, dict: w.leafDict}
		w.leafDictID = 0
		w.leafDict = nil
	}
	defer func() {
		for i, w := range l.writers {
			w.leafDictID = saved[i].id
			w.leafDict = saved[i].dict
		}
	}()
	return fn()
}

func (l *noDictRewriteLeafPageLog) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	var ptr page.LeafLogPtr
	err := l.withoutLeafDict(func() error {
		var e error
		ptr, e = l.base.AppendLeafPage(leafPage)
		return e
	})
	return ptr, err
}

func (l *noDictRewriteLeafPageLog) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	stable, ok := l.base.(LeafPageStableLog)
	if !ok {
		return page.LeafLogPtr{}, nil, rootpublication.ErrUnresolvedResource
	}
	var (
		ptr page.LeafLogPtr
		res *rootpublication.StableResourceSet
	)
	err := l.withoutLeafDict(func() error {
		var e error
		ptr, res, e = stable.AppendLeafPageWithStableResources(leafPage)
		return e
	})
	return ptr, res, err
}

func (l *noDictRewriteLeafPageLog) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if batcher, ok := l.base.(LeafPageBatchLog); ok {
		var ptrs []page.LeafLogPtr
		err := l.withoutLeafDict(func() error {
			var e error
			ptrs, e = batcher.AppendLeafPages(leafPages)
			return e
		})
		return ptrs, err
	}
	ptrs := make([]page.LeafLogPtr, len(leafPages))
	err := l.withoutLeafDict(func() error {
		for i, lp := range leafPages {
			ptr, e := l.base.AppendLeafPage(lp)
			if e != nil {
				return e
			}
			ptrs[i] = ptr
		}
		return nil
	})
	return ptrs, err
}

func (l *noDictRewriteLeafPageLog) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	stableBatch, ok := l.base.(LeafPageStableBatchLog)
	if !ok {
		return nil, nil, rootpublication.ErrUnresolvedResource
	}
	var (
		ptrs []page.LeafLogPtr
		res  *rootpublication.StableResourceSet
	)
	err := l.withoutLeafDict(func() error {
		var e error
		ptrs, res, e = stableBatch.AppendLeafPagesWithStableResources(leafPages)
		return e
	})
	return ptrs, res, err
}

func (l *noDictRewriteLeafPageLog) Flush() error {
	if flusher, ok := l.base.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

func (l *noDictRewriteLeafPageLog) Sync() error {
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
	} else if writers := collectRewriteWriters(opts.leafPageLog); len(writers) > 0 {
		// SystemRootPageID outer leaves written through a maintenance/compact
		// rewriteWriter must stay non-dictionary-encoded (dictID=0) so reading
		// dict/bytes/<dictID> from SystemRootPageID can never self-recurse.
		opts.leafPageLog = &noDictRewriteLeafPageLog{base: opts.leafPageLog, writers: writers}
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
