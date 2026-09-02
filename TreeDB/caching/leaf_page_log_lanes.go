package caching

import (
	"errors"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLogGroup struct {
	db *DB
}

var _ backenddb.LeafPageLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageStableLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageStableBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedStableLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedStableBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedChildRefBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedChildRefStableBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageConcurrentAppendLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogCreatedSegmentProvider = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogCurrentSegmentProvider = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogSegmentRegistrationObserver = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogLaneProvider = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogCompactStorageHandoff = (*cachingLeafPageLogGroup)(nil)

func (g *cachingLeafPageLogGroup) laneForWorkerIndex(workerIndex int) (*lane, bool) {
	if g == nil || g.db == nil || !g.db.indexOuterLeavesInValueLog {
		return nil, false
	}
	l := g.db.leafLogAppendLaneForWorkerIndex(workerIndex)
	if l == nil {
		return nil, false
	}
	return l, true
}

func (g *cachingLeafPageLogGroup) laneLog(workerIndex int) (*cachingLeafPageLog, bool) {
	l, ok := g.laneForWorkerIndex(workerIndex)
	if !ok || l == nil {
		return nil, false
	}
	return &cachingLeafPageLog{db: g.db, lane: l}, true
}

func (g *cachingLeafPageLogGroup) defaultLog() *cachingLeafPageLog {
	if g == nil || g.db == nil {
		return nil
	}
	return &cachingLeafPageLog{db: g.db, lane: g.db.leafLogAppendLaneForWorkerIndex(0)}
}

func (g *cachingLeafPageLogGroup) LeafPageLogLane(workerIndex int) (backenddb.LeafPageLog, bool) {
	return g.laneLog(workerIndex)
}

func (g *cachingLeafPageLogGroup) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	log := g.defaultLog()
	if log == nil {
		return page.LeafLogPtr{}, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendLeafPage(leafPage)
}

func (g *cachingLeafPageLogGroup) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	log := g.defaultLog()
	if log == nil {
		return nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendLeafPages(leafPages)
}

func (g *cachingLeafPageLogGroup) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	log := g.defaultLog()
	if log == nil {
		return page.LeafLogPtr{}, nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendLeafPageWithStableResources(leafPage)
}

func (g *cachingLeafPageLogGroup) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	log := g.defaultLog()
	if log == nil {
		return nil, nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendLeafPagesWithStableResources(leafPages)
}

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error) {
	log := g.defaultLog()
	if log == nil {
		return page.LeafLogPtr{}, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPage(leafPage, preparedPayload)
}

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPageWithStableResources(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	log := g.defaultLog()
	if log == nil {
		return page.LeafLogPtr{}, nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPageWithStableResources(leafPage, preparedPayload)
}

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error) {
	log := g.defaultLog()
	if log == nil {
		return nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPages(leafPages, preparedPayloads)
}

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPagesWithStableResources(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	log := g.defaultLog()
	if log == nil {
		return nil, nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPagesWithStableResources(leafPages, preparedPayloads)
}

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPageChildRefs(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, error) {
	log := g.defaultLog()
	if log == nil {
		return nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPageChildRefs(leafPages, preparedPayloads, refs)
}

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPageChildRefsWithStableResources(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, *rootpublication.StableResourceSet, error) {
	log := g.defaultLog()
	if log == nil {
		return nil, nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPageChildRefsWithStableResources(leafPages, preparedPayloads, refs)
}

func (g *cachingLeafPageLogGroup) ConcurrentLeafPageAppends() bool { return true }

func (g *cachingLeafPageLogGroup) CompactStorageCachedWrapperOwner() bool { return true }

func (g *cachingLeafPageLogGroup) PreparedLeafPageAppends() bool { return true }

func (g *cachingLeafPageLogGroup) PreparedLeafPageBatchAppends() bool { return true }

func (g *cachingLeafPageLogGroup) CreatedLeafPageLogSegmentsSnapshot() ([]backenddb.LeafPageLogSegment, error) {
	if g == nil || g.db == nil || !g.db.indexOuterLeavesInValueLog {
		return nil, nil
	}
	segments := make([]backenddb.LeafPageLogSegment, 0, 4)
	seen := make(map[uint32]struct{}, 4)
	for _, l := range g.db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		log := &cachingLeafPageLog{db: g.db, lane: l}
		laneSegments, err := log.CreatedLeafPageLogSegmentsSnapshot()
		if err != nil {
			return nil, err
		}
		for _, seg := range laneSegments {
			if seg.FileID == 0 || seg.Path == "" {
				continue
			}
			if _, ok := seen[seg.FileID]; ok {
				continue
			}
			seen[seg.FileID] = struct{}{}
			segments = append(segments, seg)
		}
	}
	return segments, nil
}

func (g *cachingLeafPageLogGroup) CurrentLeafPageLogSegmentsSnapshot() ([]backenddb.LeafPageLogSegment, error) {
	if g == nil || g.db == nil || !g.db.indexOuterLeavesInValueLog {
		return nil, nil
	}
	segments := make([]backenddb.LeafPageLogSegment, 0, 4)
	seen := make(map[uint32]struct{}, 4)
	for _, l := range g.db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		log := &cachingLeafPageLog{db: g.db, lane: l}
		laneSegments, err := log.CurrentLeafPageLogSegmentsSnapshot()
		if err != nil {
			return nil, err
		}
		for _, seg := range laneSegments {
			if seg.FileID == 0 || seg.Path == "" {
				continue
			}
			if _, ok := seen[seg.FileID]; ok {
				continue
			}
			seen[seg.FileID] = struct{}{}
			segments = append(segments, seg)
		}
	}
	return segments, nil
}

func (g *cachingLeafPageLogGroup) CurrentValueLogSegment() (string, uint32, bool) {
	segments, err := g.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil || len(segments) == 0 {
		return "", 0, false
	}
	return segments[0].Path, segments[0].FileID, true
}

func (g *cachingLeafPageLogGroup) ProtectedLeafGenerationRootIDs() []uint64 {
	if g == nil || g.db == nil {
		return nil
	}
	protectedRootIDs, _ := g.db.publishedLeafGenerationProtectionIDs()
	return protectedRootIDs
}

func (g *cachingLeafPageLogGroup) ProtectedLeafGenerationSystemRootIDs() []uint64 {
	if g == nil || g.db == nil {
		return nil
	}
	_, protectedSystemRootIDs := g.db.publishedLeafGenerationProtectionIDs()
	return protectedSystemRootIDs
}

func (g *cachingLeafPageLogGroup) ProtectedLeafGenerationRootIDPair() ([]uint64, []uint64) {
	if g == nil || g.db == nil {
		return nil, nil
	}
	return g.db.publishedLeafGenerationProtectionIDs()
}

func (g *cachingLeafPageLogGroup) ProtectedLeafGenerationRootIDPairSnapshot() ([]uint64, []uint64, uint64) {
	if g == nil || g.db == nil {
		return nil, nil, 0
	}
	return g.db.publishedLeafGenerationProtectionSnapshot()
}

func (g *cachingLeafPageLogGroup) MarkLeafPageLogSegmentsRegistered(segments []backenddb.LeafPageLogSegment) {
	if g == nil || g.db == nil || len(segments) == 0 || !g.db.indexOuterLeavesInValueLog {
		return
	}
	for _, l := range g.db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		(&cachingLeafPageLog{db: g.db, lane: l}).MarkLeafPageLogSegmentsRegistered(segments)
	}
}

func (g *cachingLeafPageLogGroup) Flush() error {
	return g.forEachLane(func(log *cachingLeafPageLog) error { return log.Flush() })
}

func (g *cachingLeafPageLogGroup) Sync() error {
	return g.forEachLane(func(log *cachingLeafPageLog) error { return log.Sync() })
}

func (g *cachingLeafPageLogGroup) AdvanceCompactStorageLeafPageLogSeqAtLeast(seq uint32) error {
	if g == nil || g.db == nil {
		return nil
	}
	return g.db.advanceCompactStorageLeafPageLogSeqAtLeast(seq)
}

func (g *cachingLeafPageLogGroup) Close() error {
	return g.forEachLane(func(log *cachingLeafPageLog) error { return log.Close() })
}

func (g *cachingLeafPageLogGroup) forEachLane(fn func(*cachingLeafPageLog) error) error {
	if g == nil || g.db == nil || fn == nil || !g.db.indexOuterLeavesInValueLog {
		return nil
	}
	var errs []error
	for _, l := range g.db.leafLogAppendLanesSnapshot() {
		if l == nil {
			continue
		}
		if err := fn(&cachingLeafPageLog{db: g.db, lane: l}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
