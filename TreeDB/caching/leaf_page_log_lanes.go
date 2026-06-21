package caching

import (
	"errors"
	"sort"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

type cachingLeafPageLogGroup struct {
	db *DB
}

var _ backenddb.LeafPageLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPagePreparedBatchLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageConcurrentAppendLog = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogCreatedSegmentProvider = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogCurrentSegmentProvider = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogSegmentRegistrationObserver = (*cachingLeafPageLogGroup)(nil)
var _ backenddb.LeafPageLogLaneProvider = (*cachingLeafPageLogGroup)(nil)

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

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error) {
	log := g.defaultLog()
	if log == nil {
		return page.LeafLogPtr{}, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPage(leafPage, preparedPayload)
}

func (g *cachingLeafPageLogGroup) AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error) {
	log := g.defaultLog()
	if log == nil {
		return nil, errors.New("cachingdb: leaf page log unavailable")
	}
	return log.AppendPreparedLeafPages(leafPages, preparedPayloads)
}

func (g *cachingLeafPageLogGroup) ConcurrentLeafPageAppends() bool { return true }

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

func dedupeCachingLeafPageLogSegments(segments []backenddb.LeafPageLogSegment) []backenddb.LeafPageLogSegment {
	if len(segments) == 0 {
		return nil
	}
	sort.SliceStable(segments, func(i, j int) bool { return segments[i].FileID < segments[j].FileID })
	out := segments[:0]
	seen := make(map[uint32]struct{}, len(segments))
	for _, seg := range segments {
		if seg.Path == "" || seg.FileID == 0 {
			continue
		}
		if _, ok := seen[seg.FileID]; ok {
			continue
		}
		seen[seg.FileID] = struct{}{}
		out = append(out, seg)
	}
	return out
}
