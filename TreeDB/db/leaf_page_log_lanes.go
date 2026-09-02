package db

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type leafPageLogLaneCloner interface {
	cloneLeafPageLogLane(*leafLogSeqAllocator, *rewriteRIDAllocator) (LeafPageLog, error)
}

type leafPageLogSeqAllocatorSetter interface {
	setLeafPageLogSeqAllocator(*leafLogSeqAllocator)
}

type leafPageLogSeqFloorProvider interface {
	leafPageLogSeqFloor() uint32
}

type leafPageLogRIDAllocatorProvider interface {
	leafPageLogRIDAllocator() *rewriteRIDAllocator
	setLeafPageLogRIDAllocator(*rewriteRIDAllocator)
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
		if _, err := valuelog.EncodeFileID(rewriteLeafLogLaneID, next); err != nil {
			if errors.Is(err, valuelog.ErrSegmentIDRange) {
				return 0, fmt.Errorf("leaf log sequence space exhausted")
			}
			return 0, err
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
	mu        sync.RWMutex
	lanes     []LeafPageLog
	laneLocks []*sync.Mutex
	cloner    leafPageLogLaneCloner
	seqAlloc  *leafLogSeqAllocator
	ridAlloc  *rewriteRIDAllocator
}

func wrapLeafPageLogWithLaneSelection(log LeafPageLog) LeafPageLog {
	if log == nil {
		return nil
	}
	if wrapped, ok := log.(*leafPageLogWithRecordLengthHints); ok {
		if _, ok := wrapped.inner.(LeafPageLogLaneProvider); ok {
			return log
		}
	} else if _, ok := log.(LeafPageLogLaneProvider); ok {
		return log
	}
	if group, ok := log.(*leafPageLogLaneGroup); ok {
		return group
	}
	group := &leafPageLogLaneGroup{lanes: []LeafPageLog{log}, laneLocks: []*sync.Mutex{newLeafPageLogLaneLock()}}
	if cloner, ok := log.(leafPageLogLaneCloner); ok {
		group.cloner = cloner
		group.seqAlloc = newLeafLogSeqAllocator(leafPageLogMaxSeq(log))
		if setter, ok := log.(leafPageLogSeqAllocatorSetter); ok {
			setter.setLeafPageLogSeqAllocator(group.seqAlloc)
		}
		if ridProvider, ok := log.(leafPageLogRIDAllocatorProvider); ok {
			group.ridAlloc = ridProvider.leafPageLogRIDAllocator()
			ridProvider.setLeafPageLogRIDAllocator(group.ridAlloc)
		}
	}
	return group
}

func newLeafPageLogLaneLock() *sync.Mutex { return &sync.Mutex{} }

func leafPageLogMaxSeq(log LeafPageLog) uint32 {
	if log == nil {
		return 0
	}
	maxSeq := uint32(0)
	if provider, ok := log.(leafPageLogSeqFloorProvider); ok {
		maxSeq = provider.leafPageLogSeqFloor()
	}
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
	return g.appendLeafPageAt(0, leafPage)
}

func (g *leafPageLogLaneGroup) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	return g.appendLeafPagesAt(0, leafPages)
}

func (g *leafPageLogLaneGroup) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return g.appendLeafPageStableAt(0, leafPage)
}

func (g *leafPageLogLaneGroup) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return g.appendLeafPagesStableAt(0, leafPages)
}

func (g *leafPageLogLaneGroup) Flush() error {
	return g.forEachLane(func(lane LeafPageLog) error { return lane.Flush() })
}

func (g *leafPageLogLaneGroup) Sync() error {
	return g.forEachLane(func(lane LeafPageLog) error { return lane.Sync() })
}

func (g *leafPageLogLaneGroup) Close() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	lanes := append([]LeafPageLog(nil), g.lanes...)
	for i := range g.lanes {
		g.lanes[i] = nil
	}
	g.mu.Unlock()
	var err error
	for _, lane := range lanes {
		if lane == nil {
			continue
		}
		closer, ok := lane.(interface{ Close() error })
		if !ok {
			continue
		}
		err = errors.Join(err, closer.Close())
	}
	return err
}

func (g *leafPageLogLaneGroup) CloseSelectedLanes() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	lanes := append([]LeafPageLog(nil), g.lanes...)
	for i := 1; i < len(g.lanes); i++ {
		g.lanes[i] = nil
	}
	g.mu.Unlock()
	var err error
	for i := 1; i < len(lanes); i++ {
		lane := lanes[i]
		if lane == nil {
			continue
		}
		closer, ok := lane.(interface{ Close() error })
		if !ok {
			continue
		}
		err = errors.Join(err, closer.Close())
	}
	return err
}

func (g *leafPageLogLaneGroup) LeafPageLogLane(workerIndex int) (LeafPageLog, bool) {
	if g == nil {
		return nil, false
	}
	if workerIndex < 0 {
		workerIndex = 0
	}
	if workerIndex == 0 {
		return &leafPageLogLaneHandle{group: g, index: 0}, true
	}
	g.mu.RLock()
	if workerIndex < len(g.lanes) && g.lanes[workerIndex] != nil {
		g.mu.RUnlock()
		return &leafPageLogLaneHandle{group: g, index: workerIndex}, true
	}
	g.mu.RUnlock()
	if g.cloner == nil {
		return nil, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if workerIndex < len(g.lanes) && g.lanes[workerIndex] != nil {
		return &leafPageLogLaneHandle{group: g, index: workerIndex}, true
	}
	for len(g.lanes) <= workerIndex {
		g.lanes = append(g.lanes, nil)
		g.laneLocks = append(g.laneLocks, newLeafPageLogLaneLock())
	}
	lane, err := g.cloner.cloneLeafPageLogLane(g.seqAlloc, g.ridAlloc)
	if err != nil {
		return nil, false
	}
	g.lanes[workerIndex] = lane
	return &leafPageLogLaneHandle{group: g, index: workerIndex}, true
}

func (g *leafPageLogLaneGroup) LeafPageLogLaneAny(workerIndex int) (any, bool) {
	return g.LeafPageLogLane(workerIndex)
}

func (g *leafPageLogLaneGroup) ConcurrentLeafPageAppends() bool { return g != nil }

func (g *leafPageLogLaneGroup) leafPageLogLane(workerIndex int) (LeafPageLog, bool) {
	return g.LeafPageLogLane(workerIndex)
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

type leafPageLogLaneHandle struct {
	group *leafPageLogLaneGroup
	index int
}

func (h *leafPageLogLaneHandle) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if h == nil || h.group == nil {
		return page.LeafLogPtr{}, errors.New("leaf page log unavailable")
	}
	return h.group.appendLeafPageAt(h.index, leafPage)
}

func (h *leafPageLogLaneHandle) AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if h == nil || h.group == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	return h.group.appendLeafPagesAt(h.index, leafPages)
}

func (h *leafPageLogLaneHandle) AppendLeafPageWithStableResources(leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if h == nil || h.group == nil {
		return page.LeafLogPtr{}, nil, errors.New("leaf page log unavailable")
	}
	return h.group.appendLeafPageStableAt(h.index, leafPage)
}

func (h *leafPageLogLaneHandle) AppendLeafPagesWithStableResources(leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if h == nil || h.group == nil {
		return nil, nil, errors.New("leaf page log unavailable")
	}
	return h.group.appendLeafPagesStableAt(h.index, leafPages)
}

func (h *leafPageLogLaneHandle) Flush() error {
	if h == nil || h.group == nil {
		return nil
	}
	return h.group.withLane(h.index, func(lane LeafPageLog) error { return lane.Flush() })
}

func (h *leafPageLogLaneHandle) Sync() error {
	if h == nil || h.group == nil {
		return nil
	}
	return h.group.withLane(h.index, func(lane LeafPageLog) error { return lane.Sync() })
}

func (h *leafPageLogLaneHandle) LastLeafPageRecordLength() uint32 {
	if h == nil || h.group == nil {
		return 0
	}
	var recordLen uint32
	_ = h.group.withLane(h.index, func(lane LeafPageLog) error {
		if provider, ok := lane.(leafPageLogRecordLengthProvider); ok {
			recordLen = provider.LastLeafPageRecordLength()
		}
		return nil
	})
	return recordLen
}

func (g *leafPageLogLaneGroup) appendLeafPageAt(index int, leafPage []byte) (page.LeafLogPtr, error) {
	var ptr page.LeafLogPtr
	err := g.withLane(index, func(lane LeafPageLog) error {
		var err error
		ptr, err = lane.AppendLeafPage(leafPage)
		return err
	})
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	g.noteAppendedLeafPtr(ptr)
	return ptr, nil
}

func (g *leafPageLogLaneGroup) appendLeafPagesAt(index int, leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if len(leafPages) == 0 {
		return nil, nil
	}
	var ptrs []page.LeafLogPtr
	err := g.withLane(index, func(lane LeafPageLog) error {
		if batcher, ok := lane.(LeafPageBatchLog); ok {
			var err error
			ptrs, err = batcher.AppendLeafPages(leafPages)
			return err
		}
		ptrs = make([]page.LeafLogPtr, len(leafPages))
		for i, leafPage := range leafPages {
			ptr, err := lane.AppendLeafPage(leafPage)
			if err != nil {
				return err
			}
			ptrs[i] = ptr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(ptrs) != len(leafPages) {
		return nil, fmt.Errorf("leaf page batch log returned %d ptrs for %d leaf pages", len(ptrs), len(leafPages))
	}
	for _, ptr := range ptrs {
		g.noteAppendedLeafPtr(ptr)
	}
	return ptrs, nil
}

func (g *leafPageLogLaneGroup) appendLeafPageStableAt(index int, leafPage []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	var ptr page.LeafLogPtr
	var resources *rootpublication.StableResourceSet
	err := g.withLane(index, func(lane LeafPageLog) error {
		stable, ok := lane.(LeafPageStableLog)
		if !ok {
			return fmt.Errorf("%w: leaf page lane lacks stable append", rootpublication.ErrUnresolvedResource)
		}
		var err error
		ptr, resources, err = stable.AppendLeafPageWithStableResources(leafPage)
		return err
	})
	if err != nil {
		if resources != nil {
			resources.Release()
		}
		return page.LeafLogPtr{}, nil, err
	}
	if resources == nil {
		return page.LeafLogPtr{}, nil, fmt.Errorf("%w: stable leaf lane returned no resource authority", rootpublication.ErrUnresolvedResource)
	}
	if err := validateLeafPageStableResources([]page.LeafLogPtr{ptr}, resources); err != nil {
		resources.Release()
		return page.LeafLogPtr{}, nil, err
	}
	g.noteAppendedLeafPtr(ptr)
	return ptr, resources, nil
}

func (g *leafPageLogLaneGroup) appendLeafPagesStableAt(index int, leafPages [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	if len(leafPages) == 0 {
		return nil, nil, nil
	}
	var ptrs []page.LeafLogPtr
	var resources *rootpublication.StableResourceSet
	err := g.withLane(index, func(lane LeafPageLog) error {
		stable, ok := lane.(LeafPageStableBatchLog)
		if !ok {
			return fmt.Errorf("%w: leaf page lane lacks stable batch append", rootpublication.ErrUnresolvedResource)
		}
		var err error
		ptrs, resources, err = stable.AppendLeafPagesWithStableResources(leafPages)
		return err
	})
	if err != nil {
		if resources != nil {
			resources.Release()
		}
		return nil, nil, err
	}
	if resources == nil {
		return nil, nil, fmt.Errorf("%w: stable leaf lane batch returned no resource authority", rootpublication.ErrUnresolvedResource)
	}
	if len(ptrs) != len(leafPages) {
		resources.Release()
		return nil, nil, fmt.Errorf("leaf page stable batch log returned %d ptrs for %d leaf pages", len(ptrs), len(leafPages))
	}
	if err := validateLeafPageStableResources(ptrs, resources); err != nil {
		resources.Release()
		return nil, nil, err
	}
	for _, ptr := range ptrs {
		g.noteAppendedLeafPtr(ptr)
	}
	return ptrs, resources, nil
}

func (g *leafPageLogLaneGroup) withLane(index int, fn func(LeafPageLog) error) error {
	if g == nil || fn == nil {
		return errors.New("leaf page log unavailable")
	}
	lane, lock := g.laneAndLock(index)
	if lane == nil {
		return errors.New("leaf page log unavailable")
	}
	lock.Lock()
	defer lock.Unlock()
	return fn(lane)
}

func (g *leafPageLogLaneGroup) laneAndLock(index int) (LeafPageLog, *sync.Mutex) {
	if g == nil {
		return nil, newLeafPageLogLaneLock()
	}
	if index < 0 {
		index = 0
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	if index >= len(g.lanes) || g.lanes[index] == nil {
		return nil, newLeafPageLogLaneLock()
	}
	lock := leafPageLogLaneLockAt(g.laneLocks, index)
	return g.lanes[index], lock
}

func leafPageLogLaneLockAt(locks []*sync.Mutex, index int) *sync.Mutex {
	if index >= 0 && index < len(locks) && locks[index] != nil {
		return locks[index]
	}
	return newLeafPageLogLaneLock()
}

func (g *leafPageLogLaneGroup) noteAppendedLeafPtr(ptr page.LeafLogPtr) {
	if g == nil || g.seqAlloc == nil {
		return
	}
	_, seq := valuelog.DecodeFileID(ptr.ValuePtr().FileID)
	g.seqAlloc.AdvanceAtLeast(seq)
}

func (g *leafPageLogLaneGroup) CurrentValueLogSegment() (path string, fileID uint32, ok bool) {
	if g == nil {
		return "", 0, false
	}
	_ = g.withLane(0, func(lane LeafPageLog) error {
		provider, hasProvider := lane.(leafPageLogCurrentSegmentProvider)
		if hasProvider {
			path, fileID, ok = provider.CurrentValueLogSegment()
		}
		return nil
	})
	if ok {
		return path, fileID, true
	}
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
	lanes, locks := g.snapshotLanesAndLocks()
	if len(lanes) == 0 {
		return nil, nil
	}
	out := make([]LeafPageLogSegment, 0, len(lanes))
	for i, lane := range lanes {
		if lane == nil {
			continue
		}
		lock := leafPageLogLaneLockAt(locks, i)
		lock.Lock()
		segments, err := leafPageLogCurrentSegments(lane)
		lock.Unlock()
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
	lanes, locks := g.snapshotLanesAndLocks()
	if len(lanes) == 0 {
		return nil, nil
	}
	out := make([]LeafPageLogSegment, 0, len(lanes))
	for i, lane := range lanes {
		if lane == nil {
			continue
		}
		lock := leafPageLogLaneLockAt(locks, i)
		lock.Lock()
		segments, err := leafPageLogCreatedSegments(lane)
		lock.Unlock()
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
	lanes, locks := g.snapshotLanesAndLocks()
	for i, lane := range lanes {
		if lane == nil {
			continue
		}
		lock := leafPageLogLaneLockAt(locks, i)
		lock.Lock()
		markLeafPageLogSegmentsRegistered(lane, segments)
		lock.Unlock()
	}
}

func (g *leafPageLogLaneGroup) advanceCompactStorageLeafPageLogSeqAtLeast(seq uint32) error {
	if g == nil || seq == 0 {
		return nil
	}
	if g.seqAlloc != nil {
		g.seqAlloc.AdvanceAtLeast(seq)
	}
	lanes, locks := g.snapshotLanesAndLocks()
	var err error
	for i, lane := range lanes {
		if lane == nil {
			continue
		}
		lock := leafPageLogLaneLockAt(locks, i)
		lock.Lock()
		err = errors.Join(err, compactStorageAdvanceLeafPageLogSeqAtLeast(lane, seq))
		lock.Unlock()
	}
	return err
}

func (g *leafPageLogLaneGroup) leafValueLogLanes() []LeafPageLog {
	if g == nil {
		return nil
	}
	lanes, _ := g.snapshotLanesAndLocks()
	return lanes
}

func (g *leafPageLogLaneGroup) snapshotLanesAndLocks() ([]LeafPageLog, []*sync.Mutex) {
	if g == nil {
		return nil, nil
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return append([]LeafPageLog(nil), g.lanes...), append([]*sync.Mutex(nil), g.laneLocks...)
}

func (g *leafPageLogLaneGroup) forEachLane(fn func(LeafPageLog) error) error {
	if g == nil || fn == nil {
		return nil
	}
	lanes, locks := g.snapshotLanesAndLocks()
	var firstErr error
	for i, lane := range lanes {
		if lane == nil {
			continue
		}
		lock := leafPageLogLaneLockAt(locks, i)
		lock.Lock()
		err := fn(lane)
		lock.Unlock()
		if err != nil && firstErr == nil {
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

func (l *leafPageLogWithRecordLengthHints) leafPageLogSeqFloor() uint32 {
	if l == nil || l.inner == nil {
		return 0
	}
	provider, ok := l.inner.(leafPageLogSeqFloorProvider)
	if !ok {
		return 0
	}
	return provider.leafPageLogSeqFloor()
}

func (l *leafPageLogWithRecordLengthHints) leafPageLogRIDAllocator() *rewriteRIDAllocator {
	if l == nil || l.inner == nil {
		return nil
	}
	provider, ok := l.inner.(leafPageLogRIDAllocatorProvider)
	if !ok {
		return nil
	}
	return provider.leafPageLogRIDAllocator()
}

func (l *leafPageLogWithRecordLengthHints) setLeafPageLogRIDAllocator(ridAlloc *rewriteRIDAllocator) {
	if l == nil || l.inner == nil {
		return
	}
	provider, ok := l.inner.(leafPageLogRIDAllocatorProvider)
	if !ok {
		return
	}
	provider.setLeafPageLogRIDAllocator(ridAlloc)
}

func (l *leafPageLogWithRecordLengthHints) cloneLeafPageLogLane(seqAlloc *leafLogSeqAllocator, ridAlloc *rewriteRIDAllocator) (LeafPageLog, error) {
	if l == nil || l.inner == nil {
		return nil, errors.New("leaf page log unavailable")
	}
	cloner, ok := l.inner.(leafPageLogLaneCloner)
	if !ok {
		return nil, fmt.Errorf("leaf page log lane cloning unavailable")
	}
	clone, err := cloner.cloneLeafPageLogLane(seqAlloc, ridAlloc)
	if err != nil {
		return nil, err
	}
	return &leafPageLogWithRecordLengthHints{db: l.db, inner: clone}, nil
}
