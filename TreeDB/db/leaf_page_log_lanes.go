package db

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafPageLogLaneProvider interface {
	leafPageLogLane(workerIndex int) (LeafPageLog, bool)
}

type leafPageLogLaneCloner interface {
	cloneLeafPageLogLane(*leafLogSeqAllocator) (LeafPageLog, error)
}

type leafPageLogSeqAllocatorSetter interface {
	setLeafPageLogSeqAllocator(*leafLogSeqAllocator)
}

type leafLogSeqAllocator struct {
	next atomic.Uint32
}

func newLeafLogSeqAllocator(start uint32) *leafLogSeqAllocator {
	a := &leafLogSeqAllocator{}
	a.next.Store(start)
	return a
}

func (a *leafLogSeqAllocator) Next() (uint32, error) {
	if a == nil {
		return 0, errors.New("leaf log sequence allocator unavailable")
	}
	for {
		current := a.next.Load()
		next := current + 1
		if next <= current {
			return 0, fmt.Errorf("leaf log sequence space exhausted")
		}
		if a.next.CompareAndSwap(current, next) {
			return next, nil
		}
	}
}

func (a *leafLogSeqAllocator) AdvanceAtLeast(seq uint32) {
	if a == nil {
		return
	}
	for {
		current := a.next.Load()
		if current >= seq {
			return
		}
		if a.next.CompareAndSwap(current, seq) {
			return
		}
	}
}

type leafPageLogLaneGroup struct {
	mu       sync.RWMutex
	lanes    []LeafPageLog
	cloner   leafPageLogLaneCloner
	seqAlloc *leafLogSeqAllocator
}

func wrapLeafPageLogWithLaneSelection(log LeafPageLog) LeafPageLog {
	if log == nil {
		return nil
	}
	if group, ok := log.(*leafPageLogLaneGroup); ok {
		return group
	}
	group := &leafPageLogLaneGroup{lanes: []LeafPageLog{log}}
	if cloner, ok := log.(leafPageLogLaneCloner); ok {
		group.cloner = cloner
		group.seqAlloc = newLeafLogSeqAllocator(leafPageLogMaxSeq(log))
		if setter, ok := log.(leafPageLogSeqAllocatorSetter); ok {
			setter.setLeafPageLogSeqAllocator(group.seqAlloc)
		}
	}
	return group
}

func leafPageLogMaxSeq(log LeafPageLog) uint32 {
	if log == nil {
		return 0
	}
	maxSeq := uint32(0)
	for _, segments := range [][]LeafPageLogSegment{mustLeafPageLogCreatedSegments(log), mustLeafPageLogCurrentSegments(log)} {
		for _, seg := range segments {
			if !isLeafLogWriterLane(seg.FileID) {
				continue
			}
			_, seq := valuelog.DecodeFileID(seg.FileID)
			if seq > maxSeq {
				maxSeq = seq
			}
		}
	}
	return maxSeq
}

func mustLeafPageLogCreatedSegments(log LeafPageLog) []LeafPageLogSegment {
	segments, err := leafPageLogCreatedSegments(log)
	if err != nil {
		return nil
	}
	return segments
}

func mustLeafPageLogCurrentSegments(log LeafPageLog) []LeafPageLogSegment {
	segments, err := leafPageLogCurrentSegments(log)
	if err != nil {
		return nil
	}
	return segments
}

func isLeafLogLane(fileID uint32) bool {
	lane, _ := valuelog.DecodeFileID(fileID)
	return lane == rewriteLeafLogLaneID
}

func isLeafLogWriterLane(fileID uint32) bool {
	return isLeafLogLane(fileID)
}

func valueLogDirForLane(dir string, fileID uint32) string {
	if isLeafLogLane(fileID) {
		return LeafLogDirPath(dir)
	}
	return ValueLogDirPath(dir)
}

func maxSegmentTargetBytesForLane(dir string, fileID uint32, defaultTarget, leafTarget int64) int64 {
	if isLeafLogLane(fileID) {
		if leafTarget > 0 {
			return leafTarget
		}
		return defaultTarget
	}
	return defaultTarget
}

func (g *leafPageLogLaneGroup) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	lane := g.defaultLane()
	if lane == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	ptr, err := lane.AppendLeafPage(leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	if g != nil && g.seqAlloc != nil {
		_, seq := valuelog.DecodeFileID(ptr.ValuePtr().FileID)
		g.seqAlloc.AdvanceAtLeast(seq)
	}
	return ptr, nil
}

func (g *leafPageLogLaneGroup) Flush() error {
	return g.forEachLane(func(lane LeafPageLog) error { return lane.Flush() })
}

func (g *leafPageLogLaneGroup) Sync() error {
	return g.forEachLane(func(lane LeafPageLog) error { return lane.Sync() })
}

func (g *leafPageLogLaneGroup) Close() error {
	return g.forEachLane(func(lane LeafPageLog) error {
		closer, ok := lane.(interface{ Close() error })
		if !ok {
			return nil
		}
		return closer.Close()
	})
}

func (g *leafPageLogLaneGroup) leafPageLogLane(workerIndex int) (LeafPageLog, bool) {
	if g == nil {
		return nil, false
	}
	if workerIndex < 0 {
		workerIndex = 0
	}
	if workerIndex == 0 {
		return g, true
	}
	g.mu.RLock()
	if workerIndex < len(g.lanes) && g.lanes[workerIndex] != nil {
		lane := g.lanes[workerIndex]
		g.mu.RUnlock()
		return lane, true
	}
	g.mu.RUnlock()
	if g.cloner == nil {
		return nil, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if workerIndex < len(g.lanes) && g.lanes[workerIndex] != nil {
		return g.lanes[workerIndex], true
	}
	for len(g.lanes) <= workerIndex {
		g.lanes = append(g.lanes, nil)
	}
	lane, err := g.cloner.cloneLeafPageLogLane(g.seqAlloc)
	if err != nil {
		return nil, false
	}
	g.lanes[workerIndex] = lane
	return lane, true
}

func (g *leafPageLogLaneGroup) defaultLane() LeafPageLog {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.lanes) == 0 {
		return nil
	}
	return g.lanes[0]
}

func (g *leafPageLogLaneGroup) CurrentValueLogSegment() (path string, fileID uint32, ok bool) {
	if g == nil {
		return "", 0, false
	}
	g.mu.RLock()
	if len(g.lanes) > 0 && g.lanes[0] != nil {
		lane := g.lanes[0]
		g.mu.RUnlock()
		provider, ok := lane.(leafPageLogCurrentSegmentProvider)
		if ok {
			return provider.CurrentValueLogSegment()
		}
	}
	g.mu.RUnlock()
	segments, err := g.CurrentLeafPageLogSegmentsSnapshot()
	if err != nil || len(segments) == 0 {
		return "", 0, false
	}
	return segments[0].Path, segments[0].FileID, true
}

func (g *leafPageLogLaneGroup) CurrentLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if g == nil {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.lanes) == 0 {
		return nil, nil
	}
	out := make([]LeafPageLogSegment, 0, len(g.lanes))
	for _, lane := range g.lanes {
		if lane == nil {
			continue
		}
		segments, err := leafPageLogCurrentSegments(lane)
		if err != nil {
			return nil, err
		}
		out = append(out, segments...)
	}
	return sanitizeLeafPageLogCreatedSegments(out), nil
}

func (g *leafPageLogLaneGroup) CreatedLeafPageLogSegmentsSnapshot() ([]LeafPageLogSegment, error) {
	if g == nil {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.lanes) == 0 {
		return nil, nil
	}
	out := make([]LeafPageLogSegment, 0, len(g.lanes))
	for _, lane := range g.lanes {
		if lane == nil {
			continue
		}
		segments, err := leafPageLogCreatedSegments(lane)
		if err != nil {
			return nil, err
		}
		out = append(out, segments...)
	}
	return sanitizeLeafPageLogCreatedSegments(out), nil
}

func (g *leafPageLogLaneGroup) MarkLeafPageLogSegmentsRegistered(segments []LeafPageLogSegment) {
	if g == nil || len(segments) == 0 {
		return
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, lane := range g.lanes {
		markLeafPageLogSegmentsRegistered(lane, segments)
	}
}

func (g *leafPageLogLaneGroup) leafValueLogLanes() []LeafPageLog {
	if g == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lanes
}

func (g *leafPageLogLaneGroup) forEachLane(fn func(LeafPageLog) error) error {
	if g == nil || fn == nil {
		return nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	var firstErr error
	for _, lane := range g.lanes {
		if lane == nil {
			continue
		}
		if err := fn(lane); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *leafPageLogWithRecordLengthHints) Close() error {
	if l == nil || l.inner == nil {
		return nil
	}
	closer, ok := l.inner.(interface{ Close() error })
	if !ok {
		return nil
	}
	return closer.Close()
}

func (l *leafPageLogWithRecordLengthHints) setLeafPageLogSeqAllocator(seqAlloc *leafLogSeqAllocator) {
	if l == nil || l.inner == nil {
		return
	}
	setter, ok := l.inner.(leafPageLogSeqAllocatorSetter)
	if !ok {
		return
	}
	setter.setLeafPageLogSeqAllocator(seqAlloc)
}

func (l *leafPageLogWithRecordLengthHints) cloneLeafPageLogLane(seqAlloc *leafLogSeqAllocator) (LeafPageLog, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	cloner, ok := l.inner.(leafPageLogLaneCloner)
	if !ok {
		return nil, fmt.Errorf("leaf page log lane cloning unavailable")
	}
	clone, err := cloner.cloneLeafPageLogLane(seqAlloc)
	if err != nil {
		return nil, err
	}
	return &leafPageLogWithRecordLengthHints{db: l.db, inner: clone}, nil
}
