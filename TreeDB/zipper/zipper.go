package zipper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type PageAllocator interface {
	Alloc(hint uint64) (uint64, error)
}

type LeafPageLog interface {
	AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error)
}

type LeafPageBatchLog interface {
	AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error)
}

type LeafPagePreparedLog interface {
	AppendPreparedLeafPage(leafPage []byte, preparedPayload []byte) (page.LeafLogPtr, error)
}

type LeafPagePreparedAppendLog interface {
	PreparedLeafPageAppends() bool
}

type LeafPagePreparedBatchAppendLog interface {
	PreparedLeafPageBatchAppends() bool
}

type LeafPageConcurrentAppendLog interface {
	ConcurrentLeafPageAppends() bool
}

type LeafPageLogLaneProvider interface {
	LeafPageLogLane(workerIndex int) (LeafPageLog, bool)
}

type leafPageLogLaneAnyProvider interface {
	LeafPageLogLaneAny(workerIndex int) (any, bool)
}

type LeafPagePreparedBatchLog interface {
	AppendPreparedLeafPages(leafPages [][]byte, preparedPayloads [][]byte) ([]page.LeafLogPtr, error)
}

type LeafPagePreparedChildRefBatchLog interface {
	AppendPreparedLeafPageChildRefs(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, error)
}

type LeafPageReader interface {
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}

type leafPageUnsafeToReader interface {
	ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
}

type leafPageUnsafeToReaderWithCacheHit interface {
	ReadUnsafeToWithCacheHit(ptr page.ValuePtr, dst []byte) ([]byte, bool, bool, error)
}

var leafPageScratchPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, page.PageSize)
	},
}

type outerLeafBuildPage struct {
	buf [page.PageSize]byte
}

type leafRefCachePage struct {
	buf [page.PageSize]byte
}

// leafRefCachePageChunk amortizes allocation object count for page-sized cache
// buffers used during one in-flight leaf-ref cache.
type leafRefCachePageChunk struct {
	pages [mergeLeafRefCachePagesPerChunk]leafRefCachePage
}

// outerLeafBuildPagePool recycles page-sized buffers used to build rewritten
// outer-leaf pages on non-maintenance apply paths.
var outerLeafBuildPagePool = sync.Pool{
	New: func() any {
		return &outerLeafBuildPage{}
	},
}

func getLeafPageScratch() []byte {
	buf, _ := leafPageScratchPool.Get().([]byte)
	if cap(buf) != page.PageSize {
		return make([]byte, 0, page.PageSize)
	}
	return buf[:0]
}

func putLeafPageScratch(buf []byte) {
	if cap(buf) != page.PageSize {
		return
	}
	leafPageScratchPool.Put(buf[:0])
}

func getOuterLeafBuildPage() *outerLeafBuildPage {
	p, _ := outerLeafBuildPagePool.Get().(*outerLeafBuildPage)
	if p == nil {
		p = &outerLeafBuildPage{}
	}
	clear(p.buf[:])
	return p
}

func putOuterLeafBuildPage(p *outerLeafBuildPage) {
	if p == nil {
		return
	}
	outerLeafBuildPagePool.Put(p)
}

type Zipper struct {
	pager     *pager.Pager
	allocator PageAllocator

	outerLeavesInValueLog bool
	leafPageLog           LeafPageLog
	leafPageReader        LeafPageReader
	leafRefCacheMu        sync.RWMutex
	leafRefCacheActive    atomic.Bool
	leafRefCache          map[page.LeafLogPtr][]byte
	leafRefCacheScratch   *mergeScratch

	leafReserveBytes          int
	internalReserveBytes      int
	piggybackCompaction       bool
	leafPrefixCompression     bool
	indexColumnarLeaves       bool
	indexPackedValuePtr       bool
	indexInternalBaseDelta    bool
	adaptiveLeafEncoding      bool
	maintenanceOpsPerCoalesce int
	parallelMergePressure     ParallelMergePressureSource

	scratchMu        sync.Mutex
	applyScratch     *mergeScratch
	applyScratchFree []*mergeScratch
}

type ParallelMergePressureLevel uint8

const (
	ParallelMergePressureNormal ParallelMergePressureLevel = iota
	ParallelMergePressureHigh
	ParallelMergePressureCritical
)

type ParallelMergePressureSource func() ParallelMergePressureLevel

type Split struct {
	Key []byte
	Ref page.ChildRef
}

type pendingLeafPagePersist struct {
	data     []byte
	root     bool
	splitIdx int
	pooled   *outerLeafBuildPage
}

const (
	mergeSplitKeyArenaInitCap         = page.PageSize
	mergeSplitKeyArenaKeepCap         = 1 << 20
	mergeSplitArenaInitCap            = 64
	mergeSplitArenaKeepCap            = 1 << 15
	mergeOuterLeafPageInitCap         = 16
	mergeOuterLeafPageKeepCap         = 128
	mergeLeafPageScratchInit          = 16
	mergeLeafPageScratchKeep          = 128
	mergeNodeKeyScratchInit           = 16
	mergeNodeKeyScratchKeep           = 128
	mergeNodeKeyScratchMaxCap         = 1 << 20
	mergePendingLeafPersistInit       = 8
	mergePendingLeafPersistKeep       = 64
	mergePendingLeafPersistMaxCap     = 512
	mergeLeafPageBatchInit            = 8
	mergeLeafPageBatchKeep            = 64
	mergeLeafPageBatchMaxCap          = 512
	mergeLeafRefCachePageInit         = 16
	mergeLeafRefCachePageKeep         = 4096
	mergeLeafRefCachePagesPerChunk    = 64
	mergeLeafRefCacheGlobalPageKeep   = 16 * 1024
	mergeChildRefBatchInit            = 8
	mergeChildRefBatchKeep            = 64
	mergeChildRefBatchMaxCap          = 512
	mergeSpanNativeRangeScratchMaxCap = 4096
	mergeSpanNativeRootRefMaxCap      = 4096
	mergeSpanNativeOutputKeepCap      = 1 << 20
	mergeSpanNativeOutputLogKeepBytes = mergeSpanNativeOutputKeepCap * page.LogRecordRefSize
	mergeSpanNativeOutputPageKeepCap  = mergeSpanNativeOutputKeepCap
	applyScratchKeep                  = 8

	mergeInternalMinParallelChildren         = 8
	mergeInternalMinParallelOps              = 4096
	mergeInternalHighPressureMinChildren     = 16
	mergeInternalHighPressureMinOps          = 16 * 1024
	mergeInternalCriticalPressureMinChildren = 32
	mergeInternalCriticalPressureMinOps      = 32 * 1024
)

// globalLeafRefCachePages lets apply scratches hand off overflow pages after
// cache close instead of allocating fresh page objects on the next apply.
var globalLeafRefCachePages = struct {
	mu    sync.Mutex
	pages []*leafRefCachePage
}{
	pages: make([]*leafRefCachePage, 0, mergeLeafRefCachePagesPerChunk),
}

type mergeScratch struct {
	mu                        sync.Mutex
	splitKeyArena             []byte
	splitArena                []Split
	outerLeafBuildPages       []*outerLeafBuildPage
	leafPageScratch           [][]byte
	nodeKeyScratch            [][]byte
	pendingLeafPersistScratch [][]pendingLeafPagePersist
	leafPageBatchScratch      [][][]byte
	leafRefCachePages         []*leafRefCachePage
	leafRefCacheActivePages   []*leafRefCachePage
	childRefBatchScratch      [][]page.ChildRef
	spanWorkerRangeScratch    []ReadOnlyLeafSpanWorkerRange
	spanWorkerScratchScratch  []*mergeScratch
	spanRangeMetricsScratch   []adaptive.Metrics
	spanRangeRetiredScratch   [][]uint64
	spanRangeSplitsScratch    []spanNativeLeafSplitRange
	spanRootRefsScratch       []Split
}

func newMergeScratch() *mergeScratch {
	return &mergeScratch{
		splitKeyArena:             make([]byte, 0, mergeSplitKeyArenaInitCap),
		splitArena:                make([]Split, 0, mergeSplitArenaInitCap),
		outerLeafBuildPages:       make([]*outerLeafBuildPage, 0, mergeOuterLeafPageInitCap),
		leafPageScratch:           make([][]byte, 0, mergeLeafPageScratchInit),
		nodeKeyScratch:            make([][]byte, 0, mergeNodeKeyScratchInit),
		pendingLeafPersistScratch: make([][]pendingLeafPagePersist, 0, mergePendingLeafPersistInit),
		leafPageBatchScratch:      make([][][]byte, 0, mergeLeafPageBatchInit),
		leafRefCachePages:         make([]*leafRefCachePage, 0, mergeLeafRefCachePageInit),
		leafRefCacheActivePages:   make([]*leafRefCachePage, 0, mergeLeafRefCachePageInit),
		childRefBatchScratch:      make([][]page.ChildRef, 0, mergeChildRefBatchInit),
	}
}

func (s *mergeScratch) cloneSplitKey(src []byte) []byte {
	if len(src) == 0 {
		return []byte{}
	}
	if s == nil {
		return append([]byte(nil), src...)
	}
	s.mu.Lock()
	start := len(s.splitKeyArena)
	s.splitKeyArena = append(s.splitKeyArena, src...)
	out := s.splitKeyArena[start : start+len(src)]
	s.mu.Unlock()
	return out
}

func (s *mergeScratch) reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cap(s.splitKeyArena) > mergeSplitKeyArenaKeepCap {
		s.splitKeyArena = make([]byte, 0, mergeSplitKeyArenaInitCap)
	} else {
		s.splitKeyArena = s.splitKeyArena[:0]
	}
	if cap(s.splitArena) > mergeSplitArenaKeepCap {
		s.splitArena = make([]Split, 0, mergeSplitArenaInitCap)
	} else {
		clear(s.splitArena)
		s.splitArena = s.splitArena[:0]
	}
	if cap(s.outerLeafBuildPages) > mergeOuterLeafPageKeepCap {
		s.outerLeafBuildPages = make([]*outerLeafBuildPage, 0, mergeOuterLeafPageInitCap)
	}
	if n := len(s.leafPageScratch); n > mergeLeafPageScratchKeep {
		extra := s.leafPageScratch[mergeLeafPageScratchKeep:]
		for i := range extra {
			putLeafPageScratch(extra[i])
			extra[i] = nil
		}
		s.leafPageScratch = s.leafPageScratch[:mergeLeafPageScratchKeep]
	}
	if n := len(s.nodeKeyScratch); n > mergeNodeKeyScratchKeep {
		extra := s.nodeKeyScratch[mergeNodeKeyScratchKeep:]
		for i := range extra {
			extra[i] = nil
		}
		s.nodeKeyScratch = s.nodeKeyScratch[:mergeNodeKeyScratchKeep]
	}
	if n := len(s.pendingLeafPersistScratch); n > mergePendingLeafPersistKeep {
		extra := s.pendingLeafPersistScratch[mergePendingLeafPersistKeep:]
		for i := range extra {
			extra[i] = nil
		}
		s.pendingLeafPersistScratch = s.pendingLeafPersistScratch[:mergePendingLeafPersistKeep]
	}
	if n := len(s.leafPageBatchScratch); n > mergeLeafPageBatchKeep {
		extra := s.leafPageBatchScratch[mergeLeafPageBatchKeep:]
		for i := range extra {
			extra[i] = nil
		}
		s.leafPageBatchScratch = s.leafPageBatchScratch[:mergeLeafPageBatchKeep]
	}
	if n := len(s.leafRefCachePages); n > mergeLeafRefCachePageKeep {
		extra := s.leafRefCachePages[mergeLeafRefCachePageKeep:]
		putGlobalLeafRefCachePages(extra)
		clear(extra)
		s.leafRefCachePages = s.leafRefCachePages[:mergeLeafRefCachePageKeep]
	}
	clear(s.leafRefCacheActivePages)
	s.leafRefCacheActivePages = s.leafRefCacheActivePages[:0]
	if n := len(s.childRefBatchScratch); n > mergeChildRefBatchKeep {
		extra := s.childRefBatchScratch[mergeChildRefBatchKeep:]
		for i := range extra {
			extra[i] = nil
		}
		s.childRefBatchScratch = s.childRefBatchScratch[:mergeChildRefBatchKeep]
	}
	if cap(s.spanWorkerRangeScratch) > mergeSpanNativeRangeScratchMaxCap {
		s.spanWorkerRangeScratch = nil
	} else {
		s.spanWorkerRangeScratch = s.spanWorkerRangeScratch[:0]
	}
	if cap(s.spanWorkerScratchScratch) > mergeSpanNativeRangeScratchMaxCap {
		s.spanWorkerScratchScratch = nil
	} else {
		clear(s.spanWorkerScratchScratch)
		s.spanWorkerScratchScratch = s.spanWorkerScratchScratch[:0]
	}
	if cap(s.spanRangeMetricsScratch) > mergeSpanNativeRangeScratchMaxCap {
		s.spanRangeMetricsScratch = nil
	} else {
		clear(s.spanRangeMetricsScratch)
		s.spanRangeMetricsScratch = s.spanRangeMetricsScratch[:0]
	}
	if cap(s.spanRangeRetiredScratch) > mergeSpanNativeRangeScratchMaxCap {
		s.spanRangeRetiredScratch = nil
	} else {
		clear(s.spanRangeRetiredScratch)
		s.spanRangeRetiredScratch = s.spanRangeRetiredScratch[:0]
	}
	if cap(s.spanRangeSplitsScratch) > mergeSpanNativeRangeScratchMaxCap {
		s.spanRangeSplitsScratch = nil
	} else {
		clear(s.spanRangeSplitsScratch)
		s.spanRangeSplitsScratch = s.spanRangeSplitsScratch[:0]
	}
	if cap(s.spanRootRefsScratch) > mergeSpanNativeRootRefMaxCap {
		s.spanRootRefsScratch = nil
	} else {
		clear(s.spanRootRefsScratch)
		s.spanRootRefsScratch = s.spanRootRefsScratch[:0]
	}
}

type mergeSplitSegment struct {
	scratch *mergeScratch
	start   int
	data    []Split
}

func newMergeSplitSegment(s *mergeScratch) mergeSplitSegment {
	if s == nil {
		return mergeSplitSegment{}
	}
	s.mu.Lock()
	start := len(s.splitArena)
	data := s.splitArena[start:start:start]
	s.mu.Unlock()
	return mergeSplitSegment{scratch: s, start: start, data: data}
}

func (seg *mergeSplitSegment) append(split Split) []Split {
	if seg.scratch == nil {
		seg.data = append(seg.data, split)
		return seg.data
	}
	s := seg.scratch
	s.mu.Lock()
	if len(s.splitArena) == seg.start+len(seg.data) {
		s.splitArena = append(s.splitArena, split)
		seg.data = s.splitArena[seg.start:len(s.splitArena):len(s.splitArena)]
		s.mu.Unlock()
		return seg.data
	}
	s.mu.Unlock()

	// This should only happen if a caller appends to an older returned segment.
	// Fall back to an owned slice so previously returned split ranges stay stable.
	owned := make([]Split, len(seg.data), len(seg.data)+1)
	copy(owned, seg.data)
	owned = append(owned, split)
	seg.scratch = nil
	seg.data = owned
	return seg.data
}

func (s *mergeScratch) acquireOuterLeafBuildPage() *outerLeafBuildPage {
	if s == nil {
		return getOuterLeafBuildPage()
	}
	s.mu.Lock()
	n := len(s.outerLeafBuildPages)
	if n > 0 {
		p := s.outerLeafBuildPages[n-1]
		s.outerLeafBuildPages[n-1] = nil
		s.outerLeafBuildPages = s.outerLeafBuildPages[:n-1]
		s.mu.Unlock()
		clear(p.buf[:])
		return p
	}
	s.mu.Unlock()
	return getOuterLeafBuildPage()
}

func (s *mergeScratch) releaseOuterLeafBuildPage(p *outerLeafBuildPage) {
	if p == nil {
		return
	}
	if s == nil {
		putOuterLeafBuildPage(p)
		return
	}
	s.mu.Lock()
	if len(s.outerLeafBuildPages) < mergeOuterLeafPageKeepCap {
		s.outerLeafBuildPages = append(s.outerLeafBuildPages, p)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	putOuterLeafBuildPage(p)
}

func (s *mergeScratch) acquireLeafPageScratch() []byte {
	if s == nil {
		return getLeafPageScratch()
	}
	s.mu.Lock()
	n := len(s.leafPageScratch)
	if n > 0 {
		buf := s.leafPageScratch[n-1]
		s.leafPageScratch[n-1] = nil
		s.leafPageScratch = s.leafPageScratch[:n-1]
		s.mu.Unlock()
		if cap(buf) == page.PageSize {
			return buf[:0]
		}
		return getLeafPageScratch()
	}
	s.mu.Unlock()
	return getLeafPageScratch()
}

func (s *mergeScratch) releaseLeafPageScratch(buf []byte) {
	if cap(buf) != page.PageSize {
		return
	}
	if s == nil {
		putLeafPageScratch(buf)
		return
	}
	s.mu.Lock()
	if len(s.leafPageScratch) < mergeLeafPageScratchKeep {
		s.leafPageScratch = append(s.leafPageScratch, buf[:0])
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	putLeafPageScratch(buf)
}

func (s *mergeScratch) acquireNodeKeyScratch() []byte {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	n := len(s.nodeKeyScratch)
	if n > 0 {
		buf := s.nodeKeyScratch[n-1]
		s.nodeKeyScratch[n-1] = nil
		s.nodeKeyScratch = s.nodeKeyScratch[:n-1]
		s.mu.Unlock()
		return buf[:0]
	}
	s.mu.Unlock()
	return nil
}

func (s *mergeScratch) releaseNodeKeyScratch(buf []byte) {
	if s == nil || cap(buf) == 0 {
		return
	}
	if cap(buf) > mergeNodeKeyScratchMaxCap {
		return
	}
	s.mu.Lock()
	if len(s.nodeKeyScratch) < mergeNodeKeyScratchKeep {
		s.nodeKeyScratch = append(s.nodeKeyScratch, buf[:0])
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
}

func (s *mergeScratch) acquirePendingLeafPagePersists(capacity int) []pendingLeafPagePersist {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([]pendingLeafPagePersist, 0, capacity)
	}
	s.mu.Lock()
	n := len(s.pendingLeafPersistScratch)
	if n > 0 {
		buf := s.pendingLeafPersistScratch[n-1]
		s.pendingLeafPersistScratch[n-1] = nil
		s.pendingLeafPersistScratch = s.pendingLeafPersistScratch[:n-1]
		s.mu.Unlock()
		if cap(buf) >= capacity {
			return buf[:0]
		}
	} else {
		s.mu.Unlock()
	}
	return make([]pendingLeafPagePersist, 0, capacity)
}

func (s *mergeScratch) releasePendingLeafPagePersists(buf []pendingLeafPagePersist) {
	if buf == nil {
		return
	}
	full := buf[:cap(buf)]
	clear(full)
	if s == nil || cap(buf) > mergePendingLeafPersistMaxCap {
		return
	}
	s.mu.Lock()
	if len(s.pendingLeafPersistScratch) < mergePendingLeafPersistKeep {
		s.pendingLeafPersistScratch = append(s.pendingLeafPersistScratch, full[:0])
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
}

func (s *mergeScratch) acquireLeafPageBatch(capacity int) [][]byte {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([][]byte, 0, capacity)
	}
	s.mu.Lock()
	n := len(s.leafPageBatchScratch)
	if n > 0 {
		buf := s.leafPageBatchScratch[n-1]
		s.leafPageBatchScratch[n-1] = nil
		s.leafPageBatchScratch = s.leafPageBatchScratch[:n-1]
		s.mu.Unlock()
		if cap(buf) >= capacity {
			return buf[:0]
		}
	} else {
		s.mu.Unlock()
	}
	return make([][]byte, 0, capacity)
}

func (s *mergeScratch) releaseLeafPageBatch(buf [][]byte) {
	if buf == nil {
		return
	}
	full := buf[:cap(buf)]
	clear(full)
	if s == nil || cap(buf) > mergeLeafPageBatchMaxCap {
		return
	}
	s.mu.Lock()
	if len(s.leafPageBatchScratch) < mergeLeafPageBatchKeep {
		s.leafPageBatchScratch = append(s.leafPageBatchScratch, full[:0])
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
}

func (s *mergeScratch) cloneLeafRefCachePage(src []byte) []byte {
	if s == nil || len(src) != page.PageSize {
		owned := make([]byte, len(src))
		copy(owned, src)
		return owned
	}
	s.mu.Lock()
	p := s.acquireLeafRefCachePageLocked()
	s.leafRefCacheActivePages = append(s.leafRefCacheActivePages, p)
	s.mu.Unlock()
	copy(p.buf[:], src)
	return p.buf[:]
}

// acquireLeafRefCacheBuildPage returns a page buffer whose lifetime is tied to
// the active in-flight leaf-ref cache. Callers may let the cache adopt it, but
// must not mutate it after persistence.
func (s *mergeScratch) acquireLeafRefCacheBuildPage() []byte {
	if s == nil {
		return make([]byte, page.PageSize)
	}
	s.mu.Lock()
	p := s.acquireLeafRefCachePageLocked()
	s.leafRefCacheActivePages = append(s.leafRefCacheActivePages, p)
	s.mu.Unlock()
	clear(p.buf[:])
	return p.buf[:]
}

func (s *mergeScratch) acquireLeafRefCachePageLocked() *leafRefCachePage {
	n := len(s.leafRefCachePages)
	if n == 0 {
		s.refillLeafRefCachePagesLocked()
		n = len(s.leafRefCachePages)
	}
	p := s.leafRefCachePages[n-1]
	s.leafRefCachePages[n-1] = nil
	s.leafRefCachePages = s.leafRefCachePages[:n-1]
	return p
}

func (s *mergeScratch) refillLeafRefCachePagesLocked() {
	s.leafRefCachePages = takeGlobalLeafRefCachePages(s.leafRefCachePages)
	if len(s.leafRefCachePages) > 0 {
		return
	}
	chunk := new(leafRefCachePageChunk)
	for i := range chunk.pages {
		s.leafRefCachePages = append(s.leafRefCachePages, &chunk.pages[i])
	}
}

func takeGlobalLeafRefCachePages(dst []*leafRefCachePage) []*leafRefCachePage {
	globalLeafRefCachePages.mu.Lock()
	pages := globalLeafRefCachePages.pages
	if n := len(pages); n > 0 {
		take := mergeLeafRefCachePagesPerChunk
		if take > n {
			take = n
		}
		start := n - take
		dst = append(dst, pages[start:n]...)
		clear(pages[start:n])
		globalLeafRefCachePages.pages = pages[:start]
	}
	globalLeafRefCachePages.mu.Unlock()
	return dst
}

func putGlobalLeafRefCachePages(pages []*leafRefCachePage) {
	if len(pages) == 0 {
		return
	}
	globalLeafRefCachePages.mu.Lock()
	if remaining := mergeLeafRefCacheGlobalPageKeep - len(globalLeafRefCachePages.pages); remaining > 0 {
		if remaining > len(pages) {
			remaining = len(pages)
		}
		globalLeafRefCachePages.pages = append(globalLeafRefCachePages.pages, pages[:remaining]...)
	}
	globalLeafRefCachePages.mu.Unlock()
}

func (s *mergeScratch) releaseLeafRefCachePages() {
	if s == nil {
		return
	}
	s.mu.Lock()
	active := s.leafRefCacheActivePages
	keep := mergeLeafRefCachePageKeep - len(s.leafRefCachePages)
	if keep < 0 {
		keep = 0
	}
	if keep > len(active) {
		keep = len(active)
	}
	if keep > 0 {
		s.leafRefCachePages = append(s.leafRefCachePages, active[:keep]...)
	}
	if keep < len(active) {
		putGlobalLeafRefCachePages(active[keep:])
	}
	clear(active)
	s.leafRefCacheActivePages = active[:0]
	s.mu.Unlock()
}

func (s *mergeScratch) acquireChildRefBatch(capacity int) []page.ChildRef {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([]page.ChildRef, 0, capacity)
	}
	s.mu.Lock()
	n := len(s.childRefBatchScratch)
	if n > 0 {
		buf := s.childRefBatchScratch[n-1]
		s.childRefBatchScratch[n-1] = nil
		s.childRefBatchScratch = s.childRefBatchScratch[:n-1]
		s.mu.Unlock()
		if cap(buf) >= capacity {
			return buf[:0]
		}
	} else {
		s.mu.Unlock()
	}
	return make([]page.ChildRef, 0, capacity)
}

func (s *mergeScratch) releaseChildRefBatch(buf []page.ChildRef) {
	if buf == nil {
		return
	}
	if s == nil || cap(buf) > mergeChildRefBatchMaxCap {
		return
	}
	s.mu.Lock()
	if len(s.childRefBatchScratch) < mergeChildRefBatchKeep {
		s.childRefBatchScratch = append(s.childRefBatchScratch, buf[:0])
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
}

func (s *mergeScratch) acquireSpanWorkerRanges(capacity int) []ReadOnlyLeafSpanWorkerRange {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([]ReadOnlyLeafSpanWorkerRange, 0, capacity)
	}
	s.mu.Lock()
	buf := s.spanWorkerRangeScratch
	s.spanWorkerRangeScratch = nil
	s.mu.Unlock()
	if cap(buf) >= capacity {
		return buf[:0]
	}
	return make([]ReadOnlyLeafSpanWorkerRange, 0, capacity)
}

func (s *mergeScratch) releaseSpanWorkerRanges(buf []ReadOnlyLeafSpanWorkerRange) {
	if s == nil || cap(buf) == 0 || cap(buf) > mergeSpanNativeRangeScratchMaxCap {
		return
	}
	s.mu.Lock()
	s.spanWorkerRangeScratch = buf[:0]
	s.mu.Unlock()
}

func (s *mergeScratch) acquireSpanWorkerScratchSlots(capacity int) []*mergeScratch {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([]*mergeScratch, capacity)
	}
	s.mu.Lock()
	buf := s.spanWorkerScratchScratch
	s.spanWorkerScratchScratch = nil
	s.mu.Unlock()
	if cap(buf) >= capacity {
		buf = buf[:capacity]
		clear(buf)
		return buf
	}
	return make([]*mergeScratch, capacity)
}

func (s *mergeScratch) releaseSpanWorkerScratchSlots(buf []*mergeScratch) {
	if s == nil || cap(buf) == 0 || cap(buf) > mergeSpanNativeRangeScratchMaxCap {
		return
	}
	full := buf[:cap(buf)]
	clear(full)
	s.mu.Lock()
	s.spanWorkerScratchScratch = full[:0]
	s.mu.Unlock()
}

func (s *mergeScratch) acquireSpanRangeMetrics(capacity int) []adaptive.Metrics {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([]adaptive.Metrics, capacity)
	}
	s.mu.Lock()
	buf := s.spanRangeMetricsScratch
	s.spanRangeMetricsScratch = nil
	s.mu.Unlock()
	if cap(buf) >= capacity {
		buf = buf[:capacity]
		clear(buf)
		return buf
	}
	return make([]adaptive.Metrics, capacity)
}

func (s *mergeScratch) releaseSpanRangeMetrics(buf []adaptive.Metrics) {
	if s == nil || cap(buf) == 0 || cap(buf) > mergeSpanNativeRangeScratchMaxCap {
		return
	}
	full := buf[:cap(buf)]
	clear(full)
	s.mu.Lock()
	s.spanRangeMetricsScratch = full[:0]
	s.mu.Unlock()
}

func (s *mergeScratch) acquireSpanRangeRetired(capacity int) [][]uint64 {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([][]uint64, capacity)
	}
	s.mu.Lock()
	buf := s.spanRangeRetiredScratch
	s.spanRangeRetiredScratch = nil
	s.mu.Unlock()
	if cap(buf) >= capacity {
		buf = buf[:capacity]
		clear(buf)
		return buf
	}
	return make([][]uint64, capacity)
}

func (s *mergeScratch) releaseSpanRangeRetired(buf [][]uint64) {
	if s == nil || cap(buf) == 0 || cap(buf) > mergeSpanNativeRangeScratchMaxCap {
		return
	}
	full := buf[:cap(buf)]
	clear(full)
	s.mu.Lock()
	s.spanRangeRetiredScratch = full[:0]
	s.mu.Unlock()
}

func (s *mergeScratch) acquireSpanRangeSplits(capacity int) []spanNativeLeafSplitRange {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([]spanNativeLeafSplitRange, capacity)
	}
	s.mu.Lock()
	buf := s.spanRangeSplitsScratch
	s.spanRangeSplitsScratch = nil
	s.mu.Unlock()
	if cap(buf) >= capacity {
		buf = buf[:capacity]
		clear(buf)
		return buf
	}
	return make([]spanNativeLeafSplitRange, capacity)
}

func (s *mergeScratch) releaseSpanRangeSplits(buf []spanNativeLeafSplitRange) {
	if s == nil || cap(buf) == 0 || cap(buf) > mergeSpanNativeRangeScratchMaxCap {
		return
	}
	full := buf[:cap(buf)]
	clear(full)
	s.mu.Lock()
	s.spanRangeSplitsScratch = full[:0]
	s.mu.Unlock()
}

func (s *mergeScratch) acquireSpanRootRefs(capacity int) []Split {
	if capacity < 0 {
		capacity = 0
	}
	if s == nil {
		return make([]Split, 0, capacity)
	}
	s.mu.Lock()
	buf := s.spanRootRefsScratch
	s.spanRootRefsScratch = nil
	s.mu.Unlock()
	if cap(buf) >= capacity {
		return buf[:0]
	}
	return make([]Split, 0, capacity)
}

func (s *mergeScratch) releaseSpanRootRefs(buf []Split) {
	if s == nil || cap(buf) == 0 || cap(buf) > mergeSpanNativeRootRefMaxCap {
		return
	}
	full := buf[:cap(buf)]
	clear(full)
	s.mu.Lock()
	s.spanRootRefsScratch = full[:0]
	s.mu.Unlock()
}

func acquireLeafPageScratch(s *mergeScratch) []byte {
	if s == nil {
		return getLeafPageScratch()
	}
	return s.acquireLeafPageScratch()
}

func releaseLeafPageScratch(s *mergeScratch, buf []byte) {
	if s == nil {
		putLeafPageScratch(buf)
		return
	}
	s.releaseLeafPageScratch(buf)
}

func shortestSeparator(left, right []byte) []byte {
	if len(right) == 0 {
		return nil
	}
	if len(left) == 0 {
		return append([]byte(nil), right...)
	}
	if bytes.Compare(left, right) >= 0 {
		return append([]byte(nil), right...)
	}
	if len(left) == 8 && len(right) == 8 {
		lv := binary.BigEndian.Uint64(left)
		rv := binary.BigEndian.Uint64(right)
		if lv < rv {
			x := lv ^ rv
			if x != 0 {
				i := bits.LeadingZeros64(x) / 8
				lb := left[i]
				rb := right[i]
				if int(lb)+1 < int(rb) {
					sep := make([]byte, i+1)
					copy(sep, left[:i])
					sep[i] = lb + 1
					return sep
				}
			}
		}
		return append([]byte(nil), right...)
	}

	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	i := 0
	for i < n && left[i] == right[i] {
		i++
	}
	if i == n {
		return append([]byte(nil), right...)
	}
	if left[i]+1 < right[i] {
		sep := make([]byte, i+1)
		copy(sep, left[:i])
		sep[i] = left[i] + 1
		return sep
	}
	return append([]byte(nil), right...)
}

func cloneKeyIntoArena(src []byte, arena *[]byte) []byte {
	if len(src) == 0 {
		return []byte{}
	}
	start := len(*arena)
	*arena = append(*arena, src...)
	return (*arena)[start : start+len(src)]
}

func shortestSeparatorIntoArena(left, right []byte, arena *[]byte) []byte {
	if len(right) == 0 {
		return nil
	}
	if len(left) == 0 {
		return cloneKeyIntoArena(right, arena)
	}
	if bytes.Compare(left, right) >= 0 {
		return cloneKeyIntoArena(right, arena)
	}
	if len(left) == 8 && len(right) == 8 {
		lv := binary.BigEndian.Uint64(left)
		rv := binary.BigEndian.Uint64(right)
		if lv < rv {
			x := lv ^ rv
			if x != 0 {
				i := bits.LeadingZeros64(x) / 8
				lb := left[i]
				rb := right[i]
				if int(lb)+1 < int(rb) {
					start := len(*arena)
					*arena = append(*arena, left[:i]...)
					*arena = append(*arena, lb+1)
					return (*arena)[start : start+i+1]
				}
			}
		}
		return cloneKeyIntoArena(right, arena)
	}

	n := len(left)
	if len(right) < n {
		n = len(right)
	}
	i := 0
	for i < n && left[i] == right[i] {
		i++
	}
	if i == n {
		return cloneKeyIntoArena(right, arena)
	}
	if left[i]+1 < right[i] {
		start := len(*arena)
		*arena = append(*arena, left[:i]...)
		*arena = append(*arena, left[i]+1)
		return (*arena)[start : start+i+1]
	}
	return cloneKeyIntoArena(right, arena)
}

type internalEntry struct {
	key   []byte
	child page.ChildRef
}

type childWork struct {
	key       []byte
	low       []byte
	high      []byte
	child     page.ChildRef
	ops       []batch.Entry
	newChild  page.ChildRef
	splits    []Split
	retired   []uint64
	childStat adaptive.Metrics
}

const maxChildWorkCap = 1 << 14

var childWorkPool sync.Pool

const maxInternalEntryCap = 1 << 15

var internalEntryPool sync.Pool

const (
	internalKeyArenaInitCap = page.PageSize
	internalKeyArenaMaxCap  = 1 << 16
)

var internalKeyArenaPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, internalKeyArenaInitCap)
	},
}

func putInternalKeyArena(arena []byte) {
	if cap(arena) <= internalKeyArenaMaxCap {
		internalKeyArenaPool.Put(arena[:0])
	}
}

var leafBuilderPool = sync.Pool{
	New: func() any {
		return &node.Builder{}
	},
}

const maxLeafHeuristicScratchCap = 2048

var leafHeuristicEntriesPool = sync.Pool{
	New: func() any {
		return make([]node.LeafHeuristicEntry, 0, 256)
	},
}

type maintenanceBudget struct {
	remaining int64
}

func newMaintenanceBudget(ops, deletes, opsPerCoalesce int) *maintenanceBudget {
	if opsPerCoalesce <= 0 || ops <= 0 {
		return nil
	}
	allowed := ops / opsPerCoalesce
	if deletes > 0 {
		deleteK := opsPerCoalesce / 256
		if deleteK < 1 {
			deleteK = 1
		}
		deleteAllowed := deletes / deleteK
		if deleteAllowed > allowed {
			allowed = deleteAllowed
		}
	}
	if allowed < 1 {
		allowed = 1
	}
	return &maintenanceBudget{remaining: int64(allowed)}
}

func (b *maintenanceBudget) allow() bool {
	if b == nil {
		return true
	}
	return atomic.LoadInt64(&b.remaining) > 0
}

func (b *maintenanceBudget) take(n int64) bool {
	if b == nil {
		return true
	}
	for {
		cur := atomic.LoadInt64(&b.remaining)
		if cur < n {
			return false
		}
		if atomic.CompareAndSwapInt64(&b.remaining, cur, cur-n) {
			return true
		}
	}
}

func getChildWorkSlice(capacity int) []childWork {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxChildWorkCap {
		return make([]childWork, 0, capacity)
	}
	if v := childWorkPool.Get(); v != nil {
		s := v.([]childWork)
		if cap(s) >= capacity {
			return s[:0]
		}
	}
	return make([]childWork, 0, capacity)
}

func putChildWorkSlice(children []childWork) {
	if cap(children) > maxChildWorkCap {
		return
	}
	for i := range children {
		children[i] = childWork{}
	}
	childWorkPool.Put(children[:0])
}

func getInternalEntrySlice(capacity int) []internalEntry {
	if capacity < 0 {
		capacity = 0
	}
	if capacity > maxInternalEntryCap {
		return make([]internalEntry, 0, capacity)
	}
	if v := internalEntryPool.Get(); v != nil {
		s := v.([]internalEntry)
		if cap(s) >= capacity {
			return s[:0]
		}
	}
	return make([]internalEntry, 0, capacity)
}

func putInternalEntrySlice(entries []internalEntry) {
	if cap(entries) > maxInternalEntryCap {
		return
	}
	for i := range entries {
		entries[i] = internalEntry{}
	}
	internalEntryPool.Put(entries[:0])
}

func New(p *pager.Pager, a PageAllocator) *Zipper {
	return &Zipper{
		pager:     p,
		allocator: a,
	}
}

func (z *Zipper) acquireApplyScratch() *mergeScratch {
	if z == nil {
		return newMergeScratch()
	}
	z.scratchMu.Lock()
	var s *mergeScratch
	if n := len(z.applyScratchFree); n > 0 {
		s = z.applyScratchFree[n-1]
		z.applyScratchFree[n-1] = nil
		z.applyScratchFree = z.applyScratchFree[:n-1]
	} else {
		s = z.applyScratch
		z.applyScratch = nil
	}
	z.scratchMu.Unlock()
	if s == nil {
		s = newMergeScratch()
	}
	s.reset()
	return s
}

func (z *Zipper) releaseApplyScratch(s *mergeScratch) {
	if z == nil || s == nil {
		return
	}
	s.reset()
	z.scratchMu.Lock()
	if len(z.applyScratchFree) < applyScratchKeep {
		z.applyScratchFree = append(z.applyScratchFree, s)
		z.scratchMu.Unlock()
		return
	}
	if z.applyScratch == nil {
		z.applyScratch = s
		z.scratchMu.Unlock()
		return
	}
	z.scratchMu.Unlock()
}

// CloneWithAllocator returns a zipper that shares config/pager with z but uses
// the provided allocator.
func (z *Zipper) CloneWithAllocator(a PageAllocator) *Zipper {
	return z.CloneWithPagerAllocator(z.pager, a)
}

// CloneWithPagerAllocator returns a zipper that shares z's configuration while
// using a different pager and allocator. Private COW maintenance uses this to
// build an overlay tree without allocating in the live index pager.
func (z *Zipper) CloneWithPagerAllocator(p *pager.Pager, a PageAllocator) *Zipper {
	return &Zipper{
		pager:                     p,
		allocator:                 a,
		outerLeavesInValueLog:     z.outerLeavesInValueLog,
		leafPageLog:               z.leafPageLog,
		leafPageReader:            z.leafPageReader,
		leafReserveBytes:          z.leafReserveBytes,
		internalReserveBytes:      z.internalReserveBytes,
		piggybackCompaction:       z.piggybackCompaction,
		leafPrefixCompression:     z.leafPrefixCompression,
		indexColumnarLeaves:       z.indexColumnarLeaves,
		indexPackedValuePtr:       z.indexPackedValuePtr,
		indexInternalBaseDelta:    z.indexInternalBaseDelta,
		adaptiveLeafEncoding:      z.adaptiveLeafEncoding,
		maintenanceOpsPerCoalesce: z.maintenanceOpsPerCoalesce,
		parallelMergePressure:     z.parallelMergePressure,
	}
}

// SetFillTargets configures soft-full thresholds for newly written pages.
// Targets are in parts-per-million where 1_000_000 means "allow full pages".
func (z *Zipper) SetFillTargets(leafPPM, internalPPM uint32) {
	z.leafReserveBytes = reserveBytesFromPPM(leafPPM)
	z.internalReserveBytes = reserveBytesFromPPM(internalPPM)
}

func (z *Zipper) SetPiggybackCompaction(enabled bool) {
	z.piggybackCompaction = enabled
}

func (z *Zipper) SetLeafPrefixCompression(enabled bool) {
	z.leafPrefixCompression = enabled
}

func (z *Zipper) SetIndexColumnarLeaves(enabled bool) {
	z.indexColumnarLeaves = enabled
}

func (z *Zipper) SetIndexPackedValuePtr(enabled bool) {
	z.indexPackedValuePtr = enabled
}

func (z *Zipper) SetIndexInternalBaseDelta(enabled bool) {
	z.indexInternalBaseDelta = enabled
}

func (z *Zipper) SetOuterLeavesInValueLog(enabled bool) {
	z.outerLeavesInValueLog = enabled
	if enabled {
		// Leaf refs encode value-log pointers into internal child IDs, which are
		// incompatible with base-delta child ID encodings.
		z.indexInternalBaseDelta = false
	}
}

func (z *Zipper) SetLeafPageLog(log LeafPageLog) {
	z.leafPageLog = log
}

func (z *Zipper) SetLeafPageReader(reader LeafPageReader) {
	z.leafPageReader = reader
}

func (z *Zipper) SetAdaptiveLeafEncoding(enabled bool) {
	z.adaptiveLeafEncoding = enabled
}

// SetMaintenanceOpsPerCoalesce sets the approximate ops-per-maintenance ratio.
// Values <= 0 disable maintenance budgeting (full coalesce behavior).
func (z *Zipper) SetMaintenanceOpsPerCoalesce(opsPerCoalesce int) {
	z.maintenanceOpsPerCoalesce = opsPerCoalesce
}

// SetParallelMergePressureSource configures an optional pressure signal for the
// internal-node merge fan-out gate. Nil preserves the baseline thresholds.
func (z *Zipper) SetParallelMergePressureSource(src ParallelMergePressureSource) {
	z.parallelMergePressure = src
}

func (z *Zipper) parallelMergePressureLevel() ParallelMergePressureLevel {
	if z == nil || z.parallelMergePressure == nil {
		return ParallelMergePressureNormal
	}
	switch level := z.parallelMergePressure(); level {
	case ParallelMergePressureHigh, ParallelMergePressureCritical:
		return level
	default:
		return ParallelMergePressureNormal
	}
}

func internalMergeParallelThresholds(maintenance bool, pressure ParallelMergePressureLevel) (minChildren, minOps int) {
	minChildren = mergeInternalMinParallelChildren
	minOps = mergeInternalMinParallelOps
	if maintenance {
		return minChildren, minOps
	}
	switch pressure {
	case ParallelMergePressureCritical:
		return mergeInternalCriticalPressureMinChildren, mergeInternalCriticalPressureMinOps
	case ParallelMergePressureHigh:
		return mergeInternalHighPressureMinChildren, mergeInternalHighPressureMinOps
	default:
		return minChildren, minOps
	}
}

func shouldUseParallelInternalMerge(childCount, opsCount, gomaxprocs int, maintenance bool, pressure ParallelMergePressureLevel) bool {
	if gomaxprocs <= 1 {
		return false
	}
	minChildren, minOps := internalMergeParallelThresholds(maintenance, pressure)
	return childCount >= minChildren && opsCount >= minOps
}

func (z *Zipper) newLeafBuilder(data []byte, ops []batch.Entry) *node.Builder {
	opts := z.leafBuilderOptions(ops)
	if opts.LeafPrefixCompression || opts.LeafColumnar || opts.PackedValuePtr || opts.InternalBaseDelta {
		return node.NewBuilderWithOptions(data, page.PageTypeLeaf, opts)
	}
	return node.NewBuilder(data, page.PageTypeLeaf)
}

func (z *Zipper) newPooledLeafBuilder(data []byte, ops []batch.Entry) *node.Builder {
	return z.newPooledBuilderForType(data, page.PageTypeLeaf, ops)
}

// newPooledLeafBuilderWithEntryRevisions reuses the enclosing leaf's already
// determined revision encoding when a split starts another builder.
func (z *Zipper) newPooledLeafBuilderWithEntryRevisions(data []byte, ops []batch.Entry, entryRevisions bool) *node.Builder {
	b := leafBuilderPool.Get().(*node.Builder)
	b.ResetWithOptions(data, page.PageTypeLeaf, z.leafBuilderOptionsWithEntryRevisions(ops, entryRevisions))
	return b
}

func (z *Zipper) leafBuilderOptions(ops []batch.Entry) node.BuilderOptions {
	return z.leafBuilderOptionsWithEntryRevisions(ops, batchHasEntryRevisions(ops))
}

func (z *Zipper) leafBuilderOptionsWithEntryRevisions(ops []batch.Entry, entryRevisions bool) node.BuilderOptions {
	base := node.BuilderOptions{
		LeafPrefixCompression: z != nil && z.leafPrefixCompression,
		LeafColumnar:          z != nil && z.indexColumnarLeaves,
		PackedValuePtr:        z != nil && z.indexPackedValuePtr,
		InternalBaseDelta:     z != nil && z.indexInternalBaseDelta,
		EntryRevisions:        entryRevisions,
	}
	if z == nil || !z.adaptiveLeafEncoding {
		return base
	}
	if len(ops) == 0 {
		return base
	}
	entries, _ := leafHeuristicEntriesPool.Get().([]node.LeafHeuristicEntry)
	if cap(entries) < len(ops) {
		entries = make([]node.LeafHeuristicEntry, 0, len(ops))
	}
	entries = entries[:0]
	defer func() {
		for i := range entries {
			entries[i] = node.LeafHeuristicEntry{}
		}
		if cap(entries) <= maxLeafHeuristicScratchCap {
			leafHeuristicEntriesPool.Put(entries[:0])
		}
	}()
	for i := range ops {
		op := ops[i]
		if op.Type == batch.OpDelete {
			entries = append(entries, node.LeafHeuristicEntry{Key: op.Key, Flags: node.FlagTombstone})
			continue
		}
		flags := byte(node.FlagInline)
		if op.IsPtr {
			flags = node.FlagPointer
		}
		entries = append(entries, node.LeafHeuristicEntry{Key: op.Key, Flags: flags})
	}
	sorted := true
	for i := 1; i < len(entries); i++ {
		cmp := bytes.Compare(entries[i-1].Key, entries[i].Key)
		if cmp > 0 || (cmp == 0 && entries[i-1].Flags > entries[i].Flags) {
			sorted = false
			break
		}
	}
	if !sorted {
		sort.Slice(entries, func(i, j int) bool {
			cmp := bytes.Compare(entries[i].Key, entries[j].Key)
			if cmp != 0 {
				return cmp < 0
			}
			return entries[i].Flags < entries[j].Flags
		})
	}
	return node.AdaptiveLeafBuilderOptions(base, entries)
}

func batchHasEntryRevisions(ops []batch.Entry) bool {
	for i := range ops {
		if ops[i].Revision != page.LegacyEntryRevision {
			return true
		}
	}
	return false
}

func (z *Zipper) enableLeafBuilderEntryRevisions(b *node.Builder, ops []batch.Entry) {
	if b == nil || b.LeafEntryRevisionsEnabled() {
		return
	}
	pageID := b.PageID()
	opts := z.leafBuilderOptions(ops)
	opts.EntryRevisions = true
	b.ResetWithOptions(b.Data(), page.PageTypeLeaf, opts)
	b.SetPageID(pageID)
}

func (z *Zipper) newPooledBuilderForType(data []byte, typ page.PageType, ops []batch.Entry) *node.Builder {
	b := leafBuilderPool.Get().(*node.Builder)
	if typ == page.PageTypeLeaf {
		opts := z.leafBuilderOptions(ops)
		b.ResetWithOptions(data, page.PageTypeLeaf, opts)
		return b
	}
	opts := node.BuilderOptions{}
	if z != nil {
		opts.InternalBaseDelta = z.indexInternalBaseDelta
	}
	b.ResetWithOptions(data, typ, opts)
	return b
}

func releasePooledBuilder(b *node.Builder) {
	if b == nil {
		return
	}
	b.ReleaseScratch()
	leafBuilderPool.Put(b)
}

func releasePooledLeafBuilder(b *node.Builder) {
	releasePooledBuilder(b)
}

func (z *Zipper) newBuilderForType(data []byte, typ page.PageType, ops []batch.Entry) *node.Builder {
	if typ == page.PageTypeLeaf {
		return z.newLeafBuilder(data, ops)
	}
	if z != nil && z.indexInternalBaseDelta {
		return node.NewBuilderWithOptions(data, typ, node.BuilderOptions{
			InternalBaseDelta: z.indexInternalBaseDelta,
		})
	}
	return node.NewBuilder(data, typ)
}

func reserveBytesFromPPM(ppm uint32) int {
	if ppm >= 1_000_000 {
		return 0
	}
	// Reserve a fixed fraction of the page size.
	reserve := int((uint64(page.PageSize) * uint64(1_000_000-ppm)) / 1_000_000)
	if reserve < 0 {
		return 0
	}
	return reserve
}

func (z *Zipper) leafSoftFull(b *node.Builder, entrySize int) bool {
	if z.leafReserveBytes <= 0 || b == nil || b.Count() == 0 {
		return false
	}
	return b.FreeSpace() < entrySize+node.DirectoryEntrySize+z.leafReserveBytes
}

func (z *Zipper) internalSoftFull(b *node.Builder, entrySize int) bool {
	if z.internalReserveBytes <= 0 || b == nil || b.Count() == 0 {
		return false
	}
	return b.FreeSpace() < entrySize+node.DirectoryEntrySize+z.internalReserveBytes
}

func (z *Zipper) shouldRunMaintenance(ops []batch.Entry) (maintenance bool, deleteCount int) {
	hasDeletes := false
	for _, op := range ops {
		if op.Type == batch.OpDelete {
			hasDeletes = true
			deleteCount++
		}
	}
	// Maintenance is intentionally limited to delete-containing batches, which can
	// create empty/underfull pages. Soft-full targets (reserve bytes) and
	// piggyback compaction should not, by themselves, force coalesce/packing work
	// on pure-put workloads; that would add high overhead to the steady-state
	// write path.
	maintenance = hasDeletes
	return maintenance, deleteCount
}

type zipperNodeLoadSource uint8

const (
	zipperNodeLoadPager zipperNodeLoadSource = iota
	zipperNodeLoadLeafLogCache
	zipperNodeLoadLeafLogView
	zipperNodeLoadLeafLogScratch
)

func recordZipperNodeLoad(metrics *adaptive.Metrics, ref page.ChildRef, _ node.Node, source zipperNodeLoadSource) {
	if metrics == nil {
		return
	}
	metrics.ZipperNodeLoads++
	if ref.Kind == page.ChildRefLeafLog {
		metrics.ZipperLeafLogNodeLoads++
		metrics.ZipperLeafLogNodeBytesRead += page.PageSize
		if hint := int(ref.Log.RecordLength()); hint > 0 {
			metrics.ZipperLeafLogRecordHintBytesRead += hint
		}
		switch source {
		case zipperNodeLoadLeafLogCache:
			metrics.ZipperLeafLogCacheHits++
		case zipperNodeLoadLeafLogView:
			metrics.ZipperLeafLogReaderCalls++
			metrics.ZipperLeafLogViewReads++
		case zipperNodeLoadLeafLogScratch:
			metrics.ZipperLeafLogReaderCalls++
			metrics.ZipperLeafLogScratchReads++
		default:
			metrics.ZipperLeafLogReaderCalls++
		}
	} else {
		metrics.ZipperPagerNodeLoads++
		metrics.ZipperPagerNodeBytesRead += page.PageSize
	}
}

func recordZipperLeafPageWrite(metrics *adaptive.Metrics, outerLeavesInValueLog bool) {
	if metrics == nil {
		return
	}
	metrics.ZipperLeafPagesWritten++
	metrics.ZipperLeafPageBytesWritten += page.PageSize
	if outerLeavesInValueLog {
		metrics.ZipperLeafLogPagesWritten++
		metrics.ZipperLeafLogPageBytesWritten += page.PageSize
		return
	}
	metrics.ZipperPagerLeafPagesWritten++
	metrics.ZipperPagerLeafPageBytesWritten += page.PageSize
}

func recordZipperLeafLogPageRecordHintWrite(metrics *adaptive.Metrics, ref page.ChildRef) {
	if metrics == nil || ref.Kind != page.ChildRefLeafLog {
		return
	}
	if hint := int(ref.Log.RecordLength()); hint > 0 {
		metrics.ZipperLeafLogRecordHintBytesWritten += hint
	}
}

func recordZipperInternalPageWrite(metrics *adaptive.Metrics) {
	if metrics == nil {
		return
	}
	metrics.ZipperInternalPagesWritten++
	metrics.ZipperInternalPageBytesWritten += page.PageSize
}

func recordZipperInternalChildRef(metrics *adaptive.Metrics, ref page.ChildRef) {
	if metrics == nil {
		return
	}
	metrics.ZipperInternalChildRefs++
	if ref.Kind == page.ChildRefLeafLog {
		metrics.ZipperInternalLeafLogRefs++
		return
	}
	metrics.ZipperInternalPageChildRefs++
}

func recordZipperInternalLeafLogRefCopy(metrics *adaptive.Metrics) {
	if metrics == nil {
		return
	}
	metrics.ZipperInternalLeafLogRefCopies++
}

func recordZipperInternalParallelMerge(metrics *adaptive.Metrics, activeChildren, workers, ops int) {
	if metrics == nil {
		return
	}
	metrics.ZipperInternalParallelMerges++
	if activeChildren > 0 {
		metrics.ZipperInternalParallelChildren += activeChildren
	}
	if workers > 0 {
		metrics.ZipperInternalParallelWorkers += workers
	}
	if ops > 0 {
		metrics.ZipperInternalParallelOps += ops
	}
}

func recordZipperLeafLogOutputAppend(metrics *adaptive.Metrics, wait time.Duration, pages int, success bool) {
	if metrics == nil {
		return
	}
	metrics.ZipperLeafLogOutputAppendCalls++
	waitNs := wait.Nanoseconds()
	if waitNs <= 0 {
		waitNs = 1
	}
	metrics.ZipperLeafLogOutputAppendWaitNs += waitNs
	if success && pages > 0 {
		metrics.ZipperLeafLogOutputAppendPages += pages
	}
}

func validateLoadedLeafLogNode(data []byte) (node.Node, error) {
	if len(data) != page.PageSize {
		return node.Node{}, errors.New("zipper: leaf page has invalid size")
	}
	n := node.NewNodeView(data)
	if n.Type() != page.PageTypeLeaf {
		flags := uint16(0)
		count := uint16(0)
		if len(data) >= node.NodeHeaderSize {
			flags = binary.LittleEndian.Uint16(data[12:14])
			count = binary.LittleEndian.Uint16(data[14:16])
		}
		return node.Node{}, fmt.Errorf("zipper: leafref resolved to non-leaf page type=%d flags=0x%04x count=%d", n.Type(), flags, count)
	}
	return n, nil
}

func validateLoadedLeafLogNodeFrom(source string, data []byte) (node.Node, error) {
	n, err := validateLoadedLeafLogNode(data)
	if err != nil {
		return node.Node{}, fmt.Errorf("zipper: %s invalid leaf-log page: %w", source, err)
	}
	return n, nil
}

type leafPagePersistSink interface {
	persistLeafPageData(z *Zipper, leafPage []byte, metrics *adaptive.Metrics) (page.ChildRef, error)
	persistLeafPageBatchDataTo(z *Zipper, leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error)
}

type leafPageCacheOwnership uint8

const (
	leafPageCacheClone leafPageCacheOwnership = iota
	leafPageCacheAdoptOwned
)

type applyRunConfig struct {
	maxParallelWorkers int
	workerPool         *ApplyWorkerPool
	leafPagePersister  leafPagePersistSink
	oldPointerRefs     *PointerRefCounts
	oldEntriesRemoved  *uint64
}

// Apply applies the batch to the tree rooted at rootID.
// Returns the new root page ID, list of retired pages, and commit metrics.
func (z *Zipper) Apply(rootID uint64, b *batch.Batch) (newRootID uint64, retiredPages []uint64, metrics adaptive.Metrics, err error) {
	return z.applyWithConfig(rootID, b, applyRunConfig{})
}

func (z *Zipper) applyWithConfig(rootID uint64, b *batch.Batch, cfg applyRunConfig) (newRootID uint64, retiredPages []uint64, metrics adaptive.Metrics, err error) {
	applyStart := time.Now()
	defer func() {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
	}()
	var ops []batch.Entry
	var ranges []batch.DeleteRange
	if b != nil && b.HasDeleteRanges() {
		ops, ranges = b.ApplyPlan()
	} else if b != nil {
		ops = b.SortedEntries()
	}
	if len(ops) == 0 && len(ranges) == 0 {
		return rootID, nil, metrics, nil
	}
	metrics.ZipperApplyOps = len(ops) + len(ranges)

	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)

	// Underfull merge/rebalance maintenance is only beneficial when the batch
	// includes point deletes. Range deletes are applied by streaming affected
	// spans without materializing point tombstones.
	maintenance, deleteCount := z.shouldRunMaintenance(ops)
	if len(ranges) > 0 {
		maintenance = false
		deleteCount = 0
	}
	var budget *maintenanceBudget
	if maintenance && z.maintenanceOpsPerCoalesce > 0 {
		budget = newMaintenanceBudget(len(ops), deleteCount, z.maintenanceOpsPerCoalesce)
	}

	if z != nil && z.outerLeavesInValueLog && maintenance {
		// Fresh outer-leaf pages only need in-memory ownership during Apply when
		// maintenance may reload newly written leaf refs before the value log is
		// flushed. Pure put/restore applies do not revisit those fresh leaves, so
		// avoid retaining their page buffers for the whole commit.
		z.beginLeafRefCache(scratch)
		defer z.endLeafRefCache()
	}

	var retired []uint64
	newRootRef, splits, err := z.writeRecursive(page.PageChildRef(rootID), ops, ranges, maintenance, budget, &metrics, nil, nil, &retired, scratch, true, cfg)
	if err != nil {
		return 0, nil, metrics, err
	}

	rootReduceStart := time.Now()
	defer func() {
		metrics.ZipperRootReduceNs += time.Since(rootReduceStart).Nanoseconds()
	}()

	if len(splits) > 0 {
		// Root split!
		// The children for the next level are:
		// 1. The new version of the old root (newRoot) with Key=[] (effectively min key)
		// 2. The splits (siblings) generated from it.

		currentLevelNodes := []Split{{Key: []byte{}, Ref: newRootRef}}
		currentLevelNodes = append(currentLevelNodes, splits...)

		// Iteratively build levels up until all nodes fit in one root.
		for {
			// If we only have 1 node left, that is our new root.
			if len(currentLevelNodes) == 1 {
				rootID, err := z.ensureRootPage(currentLevelNodes[0].Key, currentLevelNodes[0].Ref, &metrics)
				return rootID, retired, metrics, err
			}
			metrics.ZipperRootSplitLevels++

			var nextLevelNodes []Split

			// Allocate a node for the current batch of children
			var currentBuilder *node.Builder

			// We need to track the "Start Key" of the current builder to promote it.
			var currentStartKey []byte

			for i, child := range currentLevelNodes {
				if currentBuilder == nil {
					// Start new node
					allocHint := uint64(0)
					if child.Ref.Kind == page.ChildRefPage {
						allocHint = child.Ref.Page
					}
					pid, err := z.allocator.Alloc(allocHint)
					if err != nil {
						return 0, nil, metrics, err
					}
					data, err := z.pager.GetForWrite(pid)
					if err != nil {
						return 0, nil, metrics, err
					}

					currentBuilder = z.newBuilderForType(data, page.PageTypeInternal, nil)
					currentBuilder.SetPageID(pid)

					currentStartKey = child.Key
					currentBuilder.SetInternalFenceBoundsBorrowed(currentStartKey, nil)
				}

				// Add child
				childKey := child.Key
				if childKey == nil {
					childKey = []byte{}
				}
				childSize := 2 + 8 + len(childKey)
				if child.Ref.Kind == page.ChildRefLeafLog {
					childSize = 2 + page.LogRecordRefSize + len(childKey)
				} else if z.indexInternalBaseDelta {
					childSize = 2 + 4 + len(childKey)
				}
				var err error
				if z.internalSoftFull(currentBuilder, childSize) {
					err = node.ErrNodeFull
				} else {
					err = currentBuilder.AddInternalChildRef(childKey, child.Ref)
					if err == nil {
						recordZipperInternalChildRef(&metrics, child.Ref)
					}
				}
				if err == node.ErrNodeFull {
					// Finish current
					currentBuilder.FinishNoNode()
					recordZipperInternalPageWrite(&metrics)
					// Promote
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})

					// Start new for THIS child (retry)
					pid, err := z.allocator.Alloc(currentBuilder.PageID())
					if err != nil {
						return 0, nil, metrics, err
					}
					data, err := z.pager.GetForWrite(pid)
					if err != nil {
						return 0, nil, metrics, err
					}
					currentBuilder = z.newBuilderForType(data, page.PageTypeInternal, nil)
					currentBuilder.SetPageID(pid)
					currentStartKey = child.Key
					currentBuilder.SetInternalFenceBoundsBorrowed(currentStartKey, nil)

					if err := currentBuilder.AddInternalChildRef(childKey, child.Ref); err != nil {
						return 0, nil, metrics, fmt.Errorf("zipper: root split retry page=%d key_len=%d ref=%+v: %w", pid, len(childKey), child.Ref, err)
					}
					recordZipperInternalChildRef(&metrics, child.Ref)
				} else if err != nil {
					return 0, nil, metrics, err
				}

				// If this was the last child, finish
				if i == len(currentLevelNodes)-1 {
					currentBuilder.FinishNoNode()
					recordZipperInternalPageWrite(&metrics)
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})
					currentBuilder = nil
				}
			}

			// Move up
			currentLevelNodes = nextLevelNodes
		}
	}

	finalRootID, err := z.ensureRootPage([]byte{}, newRootRef, &metrics)
	if err != nil {
		return 0, nil, metrics, err
	}
	return finalRootID, retired, metrics, nil
}

func (z *Zipper) loadNode(id uint64, scratchCtx *mergeScratch) (node.Node, bool, []byte, bool, zipperNodeLoadSource, error) {
	return z.loadNodeRef(page.PageChildRef(id), scratchCtx)
}

func (z *Zipper) loadNodeRef(ref page.ChildRef, scratchCtx *mergeScratch) (node.Node, bool, []byte, bool, zipperNodeLoadSource, error) {
	if z == nil || z.pager == nil {
		return node.Node{}, false, nil, false, zipperNodeLoadPager, errors.New("zipper: missing pager")
	}
	if ref.Kind == page.ChildRefLeafLog {
		ptr := ref.Log
		if z.outerLeavesInValueLog && z.leafRefCacheActive.Load() {
			z.leafRefCacheMu.RLock()
			data, cached := z.leafRefCache[ptr]
			z.leafRefCacheMu.RUnlock()
			if cached {
				n, err := validateLoadedLeafLogNodeFrom("leaf-ref cache", data)
				if err != nil {
					return node.Node{}, false, nil, false, zipperNodeLoadLeafLogCache, err
				}
				return n, false, nil, false, zipperNodeLoadLeafLogCache, nil
			}
		}
		if z.leafPageReader == nil {
			return node.Node{}, false, nil, false, zipperNodeLoadLeafLogView, errors.New("zipper: missing leaf page reader")
		}
		if r, ok := z.leafPageReader.(leafPageUnsafeToReaderWithCacheHit); ok {
			scratch := acquireLeafPageScratch(scratchCtx)
			data, usedScratch, cacheHit, err := r.ReadUnsafeToWithCacheHit(ptr.ValuePtr(), scratch[:0])
			if err != nil {
				releaseLeafPageScratch(scratchCtx, scratch)
				return node.Node{}, false, nil, false, zipperNodeLoadLeafLogView, err
			}
			source := zipperNodeLoadLeafLogScratch
			sourceLabel := "leaf-page reader scratch"
			if cacheHit {
				source = zipperNodeLoadLeafLogCache
				sourceLabel = "leaf-page reader cache"
			} else if !usedScratch {
				source = zipperNodeLoadLeafLogView
				sourceLabel = "leaf-page reader view"
			}
			if !usedScratch {
				releaseLeafPageScratch(scratchCtx, scratch)
				scratch = nil
			}
			n, err := validateLoadedLeafLogNodeFrom(sourceLabel, data)
			if err != nil {
				if scratch != nil {
					releaseLeafPageScratch(scratchCtx, scratch)
				}
				return node.Node{}, false, nil, false, source, fmt.Errorf("%w ptr=(file=%d off=%d len=0x%08x sub=%d)", err, ptr.FileID, ptr.Offset, ptr.RecordLengthHint, ptr.SubIndex)
			}
			if scratch != nil {
				return n, false, scratch, true, source, nil
			}
			return n, false, nil, false, source, nil
		}
		if r, ok := z.leafPageReader.(leafPageUnsafeToReader); ok {
			scratch := acquireLeafPageScratch(scratchCtx)
			data, usedScratch, err := r.ReadUnsafeTo(ptr.ValuePtr(), scratch[:0])
			if err != nil {
				releaseLeafPageScratch(scratchCtx, scratch)
				return node.Node{}, false, nil, false, zipperNodeLoadLeafLogView, err
			}
			source := zipperNodeLoadLeafLogScratch
			sourceLabel := "leaf-page reader scratch"
			if !usedScratch {
				releaseLeafPageScratch(scratchCtx, scratch)
				scratch = nil
				source = zipperNodeLoadLeafLogView
				sourceLabel = "leaf-page reader view"
			}
			n, err := validateLoadedLeafLogNodeFrom(sourceLabel, data)
			if err != nil {
				if scratch != nil {
					releaseLeafPageScratch(scratchCtx, scratch)
				}
				return node.Node{}, false, nil, false, source, err
			}
			if scratch != nil {
				return n, false, scratch, true, source, nil
			}
			return n, false, nil, false, source, nil
		}

		data, err := z.leafPageReader.ReadUnsafe(ptr.ValuePtr())
		if err != nil {
			return node.Node{}, false, nil, false, zipperNodeLoadLeafLogView, err
		}
		n, err := validateLoadedLeafLogNodeFrom("leaf-page reader unsafe", data)
		if err != nil {
			return node.Node{}, false, nil, false, zipperNodeLoadLeafLogView, err
		}
		return n, false, nil, false, zipperNodeLoadLeafLogView, nil
	}
	data, err := z.pager.Get(ref.Page)
	if err != nil {
		return node.Node{}, false, nil, false, zipperNodeLoadPager, fmt.Errorf("zipper: load page=%d page_count=%d: %w", ref.Page, z.pager.PageCount(), err)
	}
	return node.NewNodeView(data), true, nil, false, zipperNodeLoadPager, nil
}

func (z *Zipper) persistLeafPage(b *node.Builder, metrics *adaptive.Metrics) (page.ChildRef, error) {
	return z.persistLeafPageWithConfig(b, metrics, applyRunConfig{})
}

func (z *Zipper) persistLeafPageWithConfig(b *node.Builder, metrics *adaptive.Metrics, cfg applyRunConfig) (page.ChildRef, error) {
	return z.persistLeafPageWithConfigAndCacheOwnership(b, metrics, cfg, leafPageCacheClone)
}

func (z *Zipper) persistLeafPageWithConfigAndCacheOwnership(b *node.Builder, metrics *adaptive.Metrics, cfg applyRunConfig, ownership leafPageCacheOwnership) (page.ChildRef, error) {
	if b == nil {
		return page.ChildRef{}, errors.New("zipper: nil leaf builder")
	}
	if !z.outerLeavesInValueLog {
		return page.PageChildRef(b.PageID()), nil
	}
	return z.persistLeafPageDataWithConfigAndCacheOwnership(b.Data(), metrics, cfg, ownership)
}

func (z *Zipper) persistLeafPageDataWithConfig(leafPage []byte, metrics *adaptive.Metrics, cfg applyRunConfig) (page.ChildRef, error) {
	return z.persistLeafPageDataWithConfigAndCacheOwnership(leafPage, metrics, cfg, leafPageCacheClone)
}

func (z *Zipper) persistLeafPageDataWithConfigAndCacheOwnership(leafPage []byte, metrics *adaptive.Metrics, cfg applyRunConfig, ownership leafPageCacheOwnership) (page.ChildRef, error) {
	if cfg.leafPagePersister != nil {
		return cfg.leafPagePersister.persistLeafPageData(z, leafPage, metrics)
	}
	return z.persistLeafPageDataWithCacheOwnership(leafPage, metrics, ownership)
}

func (z *Zipper) persistLeafPageData(leafPage []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	return z.persistLeafPageDataWithCacheOwnership(leafPage, metrics, leafPageCacheClone)
}

func (z *Zipper) persistLeafPageDataWithCacheOwnership(leafPage []byte, metrics *adaptive.Metrics, ownership leafPageCacheOwnership) (page.ChildRef, error) {
	return z.persistLeafPageDataToLogWithCacheOwnership(z.leafPageLog, leafPage, metrics, ownership)
}

func (z *Zipper) persistLeafPageDataToLog(log LeafPageLog, leafPage []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	return z.persistLeafPageDataToLogWithCacheOwnership(log, leafPage, metrics, leafPageCacheClone)
}

func (z *Zipper) persistLeafPageDataToLogWithCacheOwnership(log LeafPageLog, leafPage []byte, metrics *adaptive.Metrics, ownership leafPageCacheOwnership) (page.ChildRef, error) {
	if log == nil {
		return page.ChildRef{}, errors.New("zipper: missing leaf page log")
	}
	appendStart := time.Now()
	ptr, err := log.AppendLeafPage(leafPage)
	recordZipperLeafLogOutputAppend(metrics, time.Since(appendStart), 1, err == nil)
	if err != nil {
		return page.ChildRef{}, err
	}
	z.cachePersistedLeafPage(ptr, leafPage, ownership)
	return page.LeafLogChildRef(ptr), nil
}

func (z *Zipper) persistLeafPageBatchData(leafPages [][]byte, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	return z.persistLeafPageBatchDataTo(leafPages, nil, metrics)
}

func (z *Zipper) persistLeafPageBatchDataTo(leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	return z.persistLeafPageBatchDataToWithConfig(leafPages, refs, metrics, applyRunConfig{})
}

func (z *Zipper) persistPreparedLeafPageDataTo(leafPage []byte, preparedPayload []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	return z.persistPreparedLeafPageDataToLog(z.leafPageLog, leafPage, preparedPayload, metrics)
}

func (z *Zipper) persistPreparedLeafPageDataToLog(log LeafPageLog, leafPage []byte, preparedPayload []byte, metrics *adaptive.Metrics) (page.ChildRef, error) {
	if prepared, ok := log.(LeafPagePreparedLog); ok {
		appendStart := time.Now()
		ptr, err := prepared.AppendPreparedLeafPage(leafPage, preparedPayload)
		appendWait := time.Since(appendStart)
		if err != nil {
			recordZipperLeafLogOutputAppend(metrics, appendWait, 1, false)
			return page.ChildRef{}, err
		}
		recordZipperLeafLogOutputAppend(metrics, appendWait, 1, true)
		z.cachePersistedLeafPage(ptr, leafPage, leafPageCacheClone)
		return page.LeafLogChildRef(ptr), nil
	}
	var onePage [1][]byte
	var onePayload [1][]byte
	onePage[0] = leafPage
	onePayload[0] = preparedPayload
	refs, err := z.persistPreparedLeafPageBatchDataToLog(log, onePage[:], onePayload[:], nil, metrics)
	if err != nil {
		return page.ChildRef{}, err
	}
	if len(refs) != 1 {
		return page.ChildRef{}, fmt.Errorf("zipper: prepared leaf page returned %d refs for one page", len(refs))
	}
	return refs[0], nil
}

func (z *Zipper) persistPreparedLeafPageBatchDataTo(leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	return z.persistPreparedLeafPageBatchDataToLog(z.leafPageLog, leafPages, preparedPayloads, refs, metrics)
}

func (z *Zipper) persistPreparedLeafPageBatchDataToLog(log LeafPageLog, leafPages [][]byte, preparedPayloads [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	refs = refs[:0]
	if len(leafPages) == 0 {
		return refs, nil
	}
	if len(preparedPayloads) != len(leafPages) {
		return nil, fmt.Errorf("zipper: prepared leaf payload count %d for %d pages", len(preparedPayloads), len(leafPages))
	}
	if refBatcher, ok := log.(LeafPagePreparedChildRefBatchLog); ok {
		appendStart := time.Now()
		out, err := refBatcher.AppendPreparedLeafPageChildRefs(leafPages, preparedPayloads, refs)
		appendWait := time.Since(appendStart)
		if err != nil {
			recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), false)
			return nil, err
		}
		if len(out) != len(leafPages) {
			recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), false)
			return nil, fmt.Errorf("zipper: prepared leaf page child-ref batch returned %d refs for %d pages", len(out), len(leafPages))
		}
		for i, ref := range out {
			if !ref.IsLeafLog() {
				recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), false)
				return nil, fmt.Errorf("zipper: prepared leaf page child-ref batch returned non-leaf-log ref at %d", i)
			}
		}
		recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), true)
		for i, ref := range out {
			z.cachePersistedLeafPage(ref.Log, leafPages[i], leafPageCacheClone)
		}
		return out, nil
	}
	preparedBatcher, ok := log.(LeafPagePreparedBatchLog)
	if !ok {
		return z.persistLeafPageBatchDataToLog(log, leafPages, refs, metrics)
	}
	appendStart := time.Now()
	ptrs, err := preparedBatcher.AppendPreparedLeafPages(leafPages, preparedPayloads)
	appendWait := time.Since(appendStart)
	if err != nil {
		recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), false)
		return nil, err
	}
	if len(ptrs) != len(leafPages) {
		recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), false)
		return nil, fmt.Errorf("zipper: prepared leaf page batch returned %d ptrs for %d pages", len(ptrs), len(leafPages))
	}
	recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), true)
	if cap(refs) < len(ptrs) {
		refs = make([]page.ChildRef, len(ptrs))
	} else {
		refs = refs[:len(ptrs)]
	}
	for i, ptr := range ptrs {
		z.cachePersistedLeafPage(ptr, leafPages[i], leafPageCacheClone)
		refs[i] = page.LeafLogChildRef(ptr)
	}
	return refs, nil
}

func (z *Zipper) leafPageLogConsumesPreparedPayloads(batch bool) bool {
	if z == nil {
		return false
	}
	return leafPageLogConsumesPreparedPayloads(z.leafPageLog, batch)
}

func leafPageLogConsumesPreparedPayloads(log LeafPageLog, batch bool) bool {
	if log == nil {
		return false
	}
	if batch {
		if prepared, ok := log.(LeafPagePreparedBatchAppendLog); ok {
			return prepared.PreparedLeafPageBatchAppends()
		}
		if _, ok := log.(LeafPagePreparedBatchLog); ok {
			return true
		}
		_, ok := log.(LeafPagePreparedChildRefBatchLog)
		return ok
	}
	if prepared, ok := log.(LeafPagePreparedAppendLog); ok {
		return prepared.PreparedLeafPageAppends()
	}
	_, ok := log.(LeafPagePreparedLog)
	return ok
}

func (z *Zipper) persistLeafPageBatchDataToWithConfig(leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics, cfg applyRunConfig) ([]page.ChildRef, error) {
	if cfg.leafPagePersister != nil {
		return cfg.leafPagePersister.persistLeafPageBatchDataTo(z, leafPages, refs, metrics)
	}
	return z.persistLeafPageBatchDataToLog(z.leafPageLog, leafPages, refs, metrics)
}

func (z *Zipper) persistLeafPageBatchDataToLog(log LeafPageLog, leafPages [][]byte, refs []page.ChildRef, metrics *adaptive.Metrics) ([]page.ChildRef, error) {
	refs = refs[:0]
	if len(leafPages) == 0 {
		return refs, nil
	}
	if len(leafPages) == 1 {
		ref, err := z.persistLeafPageDataToLog(log, leafPages[0], metrics)
		if err != nil {
			return nil, err
		}
		return append(refs, ref), nil
	}
	if log == nil {
		return nil, errors.New("zipper: missing leaf page log")
	}
	batcher, ok := log.(LeafPageBatchLog)
	if !ok {
		if cap(refs) < len(leafPages) {
			refs = make([]page.ChildRef, len(leafPages))
		} else {
			refs = refs[:len(leafPages)]
		}
		for i, leafPage := range leafPages {
			ref, err := z.persistLeafPageDataToLog(log, leafPage, metrics)
			if err != nil {
				return nil, err
			}
			refs[i] = ref
		}
		return refs, nil
	}
	appendStart := time.Now()
	ptrs, err := batcher.AppendLeafPages(leafPages)
	appendWait := time.Since(appendStart)
	if err != nil {
		recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), false)
		return nil, err
	}
	if len(ptrs) != len(leafPages) {
		recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), false)
		return nil, fmt.Errorf("zipper: leaf page batch returned %d ptrs for %d pages", len(ptrs), len(leafPages))
	}
	recordZipperLeafLogOutputAppend(metrics, appendWait, len(leafPages), true)
	if cap(refs) < len(ptrs) {
		refs = make([]page.ChildRef, len(ptrs))
	} else {
		refs = refs[:len(ptrs)]
	}
	for i, ptr := range ptrs {
		z.cachePersistedLeafPage(ptr, leafPages[i], leafPageCacheClone)
		refs[i] = page.LeafLogChildRef(ptr)
	}
	return refs, nil
}

func (z *Zipper) beginLeafRefCache(scratch *mergeScratch) {
	if z == nil {
		return
	}
	z.leafRefCacheMu.Lock()
	z.leafRefCache = make(map[page.LeafLogPtr][]byte)
	z.leafRefCacheScratch = scratch
	z.leafRefCacheActive.Store(true)
	z.leafRefCacheMu.Unlock()
}

func (z *Zipper) endLeafRefCache() {
	if z == nil {
		return
	}
	z.leafRefCacheMu.Lock()
	scratch := z.leafRefCacheScratch
	z.leafRefCache = nil
	z.leafRefCacheScratch = nil
	z.leafRefCacheActive.Store(false)
	z.leafRefCacheMu.Unlock()
	scratch.releaseLeafRefCachePages()
}

func (z *Zipper) cachePersistedLeafPage(ptr page.LeafLogPtr, leafPage []byte, ownership leafPageCacheOwnership) {
	if z == nil || !z.leafRefCacheActive.Load() {
		return
	}
	z.leafRefCacheMu.Lock()
	if z.leafRefCache != nil {
		// Persist paths often hand us builder/scratch buffers that are reused later
		// in the same Apply call (for example when split leaves are batched before
		// parent internals are built). The in-flight leaf-ref cache must own the
		// bytes it serves; otherwise a later scratch reuse can turn a cached leaf
		// into an internal page and make maintenance rewrites fail validation.
		owned := leafPage
		if ownership != leafPageCacheAdoptOwned {
			owned = z.leafRefCacheScratch.cloneLeafRefCachePage(leafPage)
		}
		z.leafRefCache[ptr] = owned
	}
	z.leafRefCacheMu.Unlock()
}

func (z *Zipper) ensureRootPage(key []byte, ref page.ChildRef, metrics *adaptive.Metrics) (uint64, error) {
	if ref.Kind == page.ChildRefPage {
		return ref.Page, nil
	}
	if z == nil || z.allocator == nil || z.pager == nil {
		return 0, errors.New("zipper: missing root allocator")
	}
	rootID, err := z.allocator.Alloc(0)
	if err != nil {
		return 0, err
	}
	data, err := z.pager.GetForWrite(rootID)
	if err != nil {
		return 0, err
	}
	b := z.newBuilderForType(data, page.PageTypeInternal, nil)
	b.SetPageID(rootID)
	if key == nil {
		key = []byte{}
	}
	b.SetInternalFenceBoundsBorrowed(key, nil)
	if err := b.AddInternalChildRef(key, ref); err != nil {
		return 0, fmt.Errorf("zipper: root wrapper page=%d key_len=%d ref=%+v: %w", rootID, len(key), ref, err)
	}
	recordZipperInternalChildRef(metrics, ref)
	b.FinishNoNode()
	recordZipperInternalPageWrite(metrics)
	return rootID, nil
}

// writeRecursive handles the COW merge.
// Returns: newPageID, splits, error.
func (z *Zipper) writeRecursive(ref page.ChildRef, ops []batch.Entry, ranges []batch.DeleteRange, maintenance bool, budget *maintenanceBudget, metrics *adaptive.Metrics, low, high []byte, retired *[]uint64, scratch *mergeScratch, isRoot bool, cfg applyRunConfig) (page.ChildRef, []Split, error) {
	oldNode, oldFromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(ref, scratch)
	if err != nil {
		return page.ChildRef{}, nil, err
	}
	recordZipperNodeLoad(metrics, ref, oldNode, loadSource)
	if leafScratchRef {
		defer releaseLeafPageScratch(scratch, leafScratch)
	}
	if oldFromPager && retired != nil && ref.Kind == page.ChildRefPage && ref.Page != 0 {
		*retired = append(*retired, ref.Page)
	}

	switch oldNode.Type() {
	case page.PageTypeLeaf, 0:
		if z.outerLeavesInValueLog {
			if z.leafPageLog == nil {
				return page.ChildRef{}, nil, errors.New("zipper: outer leaves in value log enabled without leaf page log")
			}
			reuseOuterLeafPages := !maintenance
			// Delete maintenance may reload freshly written leaf-log refs before
			// Apply ends. Build those leaves into cache-owned pages so the cache
			// can adopt stable bytes without per-leaf heap allocation here.
			cacheOwnedOuterLeafPages := maintenance && scratch != nil && z.leafRefCacheActive.Load()
			var (
				newData       []byte
				newDataPooled *outerLeafBuildPage
			)
			if reuseOuterLeafPages {
				newDataPooled = scratch.acquireOuterLeafBuildPage()
				newData = newDataPooled.buf[:]
			} else if cacheOwnedOuterLeafPages {
				newData = scratch.acquireLeafRefCacheBuildPage()
			} else {
				newData = make([]byte, page.PageSize)
			}
			builder := z.newPooledLeafBuilder(newData, ops)
			defer func() {
				releasePooledBuilder(builder)
				if reuseOuterLeafPages {
					scratch.releaseOuterLeafBuildPage(newDataPooled)
				}
			}()
			builder.SetPageID(0)
			newRef, splits, err := z.mergeLeaf(&oldNode, builder, ops, ranges, metrics, scratch, reuseOuterLeafPages, cacheOwnedOuterLeafPages, cfg)
			if err != nil {
				return page.ChildRef{}, nil, fmt.Errorf("zipper: merge leaf ref=%+v count=%d ops=%d low_len=%d high_len=%d: %w", ref, oldNode.Count(), len(ops), len(low), len(high), err)
			}
			return newRef, splits, nil
		}

		// Pager-backed leaf.
		newPageID, err := z.allocator.Alloc(ref.Page)
		if err != nil {
			return page.ChildRef{}, nil, err
		}
		newData, err := z.pager.GetForWrite(newPageID)
		if err != nil {
			return page.ChildRef{}, nil, err
		}
		builder := z.newPooledLeafBuilder(newData, ops)
		defer releasePooledBuilder(builder)
		builder.SetPageID(newPageID)
		return z.mergeLeaf(&oldNode, builder, ops, ranges, metrics, scratch, false, false, cfg)

	case page.PageTypeInternal:
		// Internal merge is always pager-backed.
		newPageID, err := z.allocator.Alloc(ref.Page)
		if err != nil {
			return page.ChildRef{}, nil, err
		}
		newData, err := z.pager.GetForWrite(newPageID)
		if err != nil {
			return page.ChildRef{}, nil, err
		}
		builder := z.newPooledBuilderForType(newData, page.PageTypeInternal, ops)
		defer releasePooledBuilder(builder)
		builder.SetPageID(newPageID)
		builder.SetInternalFenceBoundsBorrowed(low, high)
		nr, splits, err := z.mergeInternal(&oldNode, builder, ops, ranges, maintenance, budget, metrics, retired, low, high, scratch, cfg)
		if err != nil {
			return page.ChildRef{}, nil, fmt.Errorf("zipper: merge internal ref=%+v new_page=%d count=%d ops=%d low_len=%d high_len=%d: %w", ref, newPageID, oldNode.Count(), len(ops), len(low), len(high), err)
		}
		n := builder.Finish()
		metrics.IndexWriteBytes += page.PageSize
		recordZipperInternalPageWrite(metrics)

		// If the root collapsed to a single child and produced no splits, skip
		// writing the redundant level by returning the child directly. Only the
		// root may shrink this way: collapsing a non-root internal page would give
		// its parent a shorter child subtree than its siblings, violating B+Tree
		// height balance and allowing repeated root splits to build tall spines.
		if len(splits) == 0 && n.Count() == 1 && isRoot {
			childRef, err := n.GetInternalChildRef(0)
			if err == nil && childRef.Kind == page.ChildRefPage {
				if retired != nil {
					*retired = append(*retired, nr.Page)
				}
				return childRef, nil, nil
			}
		}
		return nr, splits, nil
	}

	return page.ChildRef{}, nil, page.ErrInvalidPageType
}

func (z *Zipper) mergeLeaf(oldNode *node.Node, builder *node.Builder, ops []batch.Entry, ranges []batch.DeleteRange, metrics *adaptive.Metrics, scratch *mergeScratch, reuseOuterLeafPages bool, cacheOwnedOuterLeafPages bool, cfg applyRunConfig) (page.ChildRef, []Split, error) {
	if metrics != nil {
		metrics.ZipperLeafMerges++
	}
	oldIdx := uint16(0)
	oldCount := oldNode.Count()
	opIdx := 0
	if oldNode.LeafEntryRevisionsEnabled() {
		z.enableLeafBuilderEntryRevisions(builder, ops)
	}

	var (
		splits                  []Split
		rootNodeRef             page.ChildRef
		rootPersisted           bool
		pendingSplitIdx         int
		pendingLeafPagePersists []pendingLeafPagePersist
		splitSegment            mergeSplitSegment
	)
	pendingSplitIdx = -1
	batchLeafPagePersists := z.outerLeavesInValueLog && reuseOuterLeafPages
	defer func() {
		for i := range pendingLeafPagePersists {
			if pendingLeafPagePersists[i].pooled != nil && scratch != nil {
				scratch.releaseOuterLeafBuildPage(pendingLeafPagePersists[i].pooled)
				pendingLeafPagePersists[i].pooled = nil
			}
		}
		if scratch != nil {
			scratch.releasePendingLeafPagePersists(pendingLeafPagePersists)
		}
	}()

	if scratch != nil {
		if keyScratch := scratch.acquireNodeKeyScratch(); keyScratch != nil {
			oldNode.SetKeyScratch(keyScratch)
		}
		defer func() {
			scratch.releaseNodeKeyScratch(oldNode.TakeKeyScratch())
		}()
	}

	// Current target builder
	target := builder
	targetPooled := false
	targetOuterLeafData := (*outerLeafBuildPage)(nil)
	targetOuterLeafDataPooled := false
	defer func() {
		if target != builder && targetPooled {
			releasePooledLeafBuilder(target)
			if targetOuterLeafDataPooled {
				scratch.releaseOuterLeafBuildPage(targetOuterLeafData)
			}
		}
	}()

	persistTarget := func() (page.ChildRef, error) {
		target.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-target.FreeSpace()) / float64(page.PageSize)
		recordZipperLeafPageWrite(metrics, z.outerLeavesInValueLog)
		if target != builder {
			metrics.Splits++
		}

		if batchLeafPagePersists {
			if pendingLeafPagePersists == nil && scratch != nil {
				pendingLeafPagePersists = scratch.acquirePendingLeafPagePersists(mergePendingLeafPersistInit)
			}
			pending := pendingLeafPagePersist{data: target.Data(), splitIdx: pendingSplitIdx}
			if target == builder {
				pending.root = true
				rootPersisted = true
			} else if targetOuterLeafDataPooled {
				pending.pooled = targetOuterLeafData
			}
			pendingLeafPagePersists = append(pendingLeafPagePersists, pending)
			if target != builder && targetPooled {
				releasePooledLeafBuilder(target)
				targetPooled = false
				targetOuterLeafDataPooled = false
				targetOuterLeafData = nil
			}
			return page.ChildRef{}, nil
		}

		ownership := leafPageCacheClone
		if z.outerLeavesInValueLog && !reuseOuterLeafPages {
			// Delete-maintenance leaf pages are built into fresh, non-pooled page
			// buffers. The in-flight leaf-ref cache can adopt those bytes instead
			// of cloning them while still avoiding scratch-mutated cache contents.
			ownership = leafPageCacheAdoptOwned
		}
		nodeRef, err := z.persistLeafPageWithConfigAndCacheOwnership(target, metrics, cfg, ownership)
		if err != nil {
			return page.ChildRef{}, err
		}
		recordZipperLeafLogPageRecordHintWrite(metrics, nodeRef)

		if target == builder {
			rootNodeRef = nodeRef
			rootPersisted = true
		} else if pendingSplitIdx >= 0 && pendingSplitIdx < len(splits) {
			splits[pendingSplitIdx].Ref = nodeRef
		}

		if target != builder && targetPooled {
			releasePooledLeafBuilder(target)
			if targetOuterLeafDataPooled {
				scratch.releaseOuterLeafBuildPage(targetOuterLeafData)
			}
			targetPooled = false
			targetOuterLeafDataPooled = false
			targetOuterLeafData = nil
		}

		return nodeRef, nil
	}

	recordRemovedEntry := func(flags byte, ptr page.ValuePtr) {
		if cfg.oldEntriesRemoved != nil {
			(*cfg.oldEntriesRemoved)++
		}
		if flags&node.FlagPointer == 0 {
			return
		}
		if cfg.oldPointerRefs != nil {
			cfg.oldPointerRefs.add(ptr.FileID, 1)
		}
		metrics.SlabDeadBytes += int(ptr.Length)
		if metrics.SlabDeadBytesByFile == nil {
			metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
		}
		metrics.SlabDeadBytesByFile[ptr.FileID] += int64(ptr.Length)
	}
	leafEntrySizeWithRevision := func(b *node.Builder, key, val []byte, flags byte, revision page.EntryRevision) (entrySize, prefixLen, suffixLen int) {
		entrySize, prefixLen, suffixLen = b.LeafEntrySizeWithPrefix(key, val, flags)
		if revision != page.LegacyEntryRevision && !b.LeafEntryRevisionsEnabled() {
			entrySize = b.LeafEntrySizeWithRevision(key, val, flags, revision)
		}
		return entrySize, prefixLen, suffixLen
	}

	for {
		// Pick next key: min(oldNode[oldIdx], ops[opIdx])
		var useBatch bool
		var oldLoaded bool
		var oldKey, oldVal []byte
		var oldPtr page.ValuePtr
		var oldRevision page.EntryRevision
		var oldFlags byte

		if oldIdx >= oldCount && opIdx >= len(ops) {
			break
		}

		if oldIdx >= oldCount {
			useBatch = true
		} else if opIdx >= len(ops) {
			// useOld = true
		} else {
			// Compare
			// Optimization: decode old entry once; reuse it below when we
			// consume from oldNode in the same loop iteration.
			k, v, ptr, f, rev, err := oldNode.GetLeafEntryViewWithRevision(oldIdx)
			if err != nil {
				return page.ChildRef{}, nil, err
			}
			oldLoaded = true
			oldKey = k
			oldVal = v
			oldPtr = ptr
			oldRevision = rev
			oldFlags = f
			batchKey := ops[opIdx].Key

			cmp := bytes.Compare(k, batchKey)
			if cmp < 0 {
				// useOld = true
			} else if cmp > 0 {
				useBatch = true
			} else {
				// Equal: Update (Batch wins). The old entry is being overwritten
				// or deleted; if it was a pointer, track it as dead bytes.
				recordRemovedEntry(f, ptr)

				useBatch = true
				oldIdx++ // Skip old
			}
		}

		// Key/Val to insert
		var key, val []byte
		var flags byte
		var valPtr page.ValuePtr
		revision := page.LegacyEntryRevision
		insertedFromBatch := false

		if useBatch {
			op := ops[opIdx]
			opIdx++
			if op.Type == batch.OpDelete {
				continue // Skip insert
			}
			insertedFromBatch = true
			key = op.Key
			revision = op.Revision
			if op.IsPtr {
				flags = node.FlagPointer
				valPtr = op.ValuePtr
			} else {
				flags = node.FlagInline
				val = op.Value
			}
		} else {
			// useOld
			if oldLoaded {
				key = oldKey
				flags = oldFlags
				revision = oldRevision
				if oldFlags&node.FlagPointer != 0 {
					valPtr = oldPtr
				} else {
					val = oldVal
				}
			} else {
				// Optimization: View
				k, v, ptr, f, rev, err := oldNode.GetLeafEntryViewWithRevision(oldIdx)
				if err != nil {
					return page.ChildRef{}, nil, err
				}
				key = k
				flags = f
				revision = rev
				if f&node.FlagPointer != 0 {
					valPtr = ptr
				} else {
					val = v
				}
			}
			oldIdx++
			if flags&node.FlagTombstone != 0 {
				continue // Skip tombstones
			}
			if batch.DeleteRangesContainKey(ranges, key) {
				recordRemovedEntry(flags, valPtr)
				continue
			}
		}

		// Insert into target builder
		entrySize, prefixLen, suffixLen := leafEntrySizeWithRevision(target, key, val, flags, revision)
		var err error
		if z.leafSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else {
			err = target.AddLeafEntryWithPrefixRevision(key, val, flags, valPtr, revision, entrySize, prefixLen, suffixLen)
		}
		if err == node.ErrNodeFull {
			// SPLIT!
			allocHint := uint64(0)
			if !z.outerLeavesInValueLog {
				allocHint = target.PageID()
			}
			targetEntryRevisions := target.LeafEntryRevisionsEnabled()

			// 1. Finish the current leaf.
			if _, err := persistTarget(); err != nil {
				return page.ChildRef{}, nil, err
			}

			// 2. Create NEW split node (right sibling).
			var (
				sid         uint64
				sdata       []byte
				sdataPooled *outerLeafBuildPage
				splitE      Split
			)
			if z.outerLeavesInValueLog {
				if reuseOuterLeafPages {
					sdataPooled = scratch.acquireOuterLeafBuildPage()
					sdata = sdataPooled.buf[:]
				} else if cacheOwnedOuterLeafPages {
					sdata = scratch.acquireLeafRefCacheBuildPage()
				} else {
					sdata = make([]byte, page.PageSize)
				}
				splitE.Ref = page.ChildRef{}
			} else {
				sid, err = z.allocator.Alloc(allocHint)
				if err != nil {
					return page.ChildRef{}, nil, err
				}
				sdata, err = z.pager.GetForWrite(sid)
				if err != nil {
					return page.ChildRef{}, nil, err
				}
				splitE.Ref = page.PageChildRef(sid)
			}

			// New Builder
			startIdx := opIdx
			if insertedFromBatch && startIdx > 0 {
				startIdx--
			}
			splitBuilder := z.newPooledLeafBuilderWithEntryRevisions(sdata, ops[startIdx:], targetEntryRevisions)
			splitBuilder.SetPageID(sid)

			// Record split
			// Use the full first key of the right node as the parent separator.
			// Shortened separators are unsafe for sparse/fence layouts where leaf
			// entries are not a complete key set.
			// Split keys escape this call via the returned []Split, so detach
			// from source buffers into apply-lifetime scratch.
			splitE.Key = scratch.cloneSplitKey(key)
			if splits == nil {
				splitSegment = newMergeSplitSegment(scratch)
				splits = splitSegment.data
			}
			splits = splitSegment.append(splitE)
			pendingSplitIdx = len(splits) - 1

			target = splitBuilder
			targetPooled = true
			targetOuterLeafData = sdataPooled
			targetOuterLeafDataPooled = z.outerLeavesInValueLog && reuseOuterLeafPages

			// Retry insert
			entrySize, prefixLen, suffixLen = leafEntrySizeWithRevision(target, key, val, flags, revision)
			err = target.AddLeafEntryWithPrefixRevision(key, val, flags, valPtr, revision, entrySize, prefixLen, suffixLen)
			if err != nil {
				return page.ChildRef{}, nil, fmt.Errorf("zipper: leaf split retry key_len=%d val_len=%d flags=%d entry_size=%d free=%d: %w", len(key), len(val), flags, entrySize, target.FreeSpace(), err)
			}
		} else if err != nil {
			return page.ChildRef{}, nil, err
		}
	}

	if !rootPersisted || target != builder {
		if _, err := persistTarget(); err != nil {
			return page.ChildRef{}, nil, err
		}
	}

	if len(pendingLeafPagePersists) == 1 {
		pending := pendingLeafPagePersists[0]
		ref, err := z.persistLeafPageDataWithConfig(pending.data, metrics, cfg)
		if err != nil {
			return page.ChildRef{}, nil, err
		}
		recordZipperLeafLogPageRecordHintWrite(metrics, ref)
		if pending.root {
			rootNodeRef = ref
		} else if pending.splitIdx >= 0 && pending.splitIdx < len(splits) {
			splits[pending.splitIdx].Ref = ref
		}
	} else if len(pendingLeafPagePersists) > 1 {
		var leafPages [][]byte
		if scratch != nil {
			leafPages = scratch.acquireLeafPageBatch(len(pendingLeafPagePersists))
		} else {
			leafPages = make([][]byte, 0, len(pendingLeafPagePersists))
		}
		leafPages = leafPages[:len(pendingLeafPagePersists)]
		for i := range pendingLeafPagePersists {
			leafPages[i] = pendingLeafPagePersists[i].data
		}
		var refScratch []page.ChildRef
		if scratch != nil {
			refScratch = scratch.acquireChildRefBatch(len(pendingLeafPagePersists))
		}
		refs, err := z.persistLeafPageBatchDataToWithConfig(leafPages, refScratch, metrics, cfg)
		if scratch != nil {
			scratch.releaseLeafPageBatch(leafPages)
		}
		if err != nil {
			if scratch != nil {
				scratch.releaseChildRefBatch(refScratch)
			}
			return page.ChildRef{}, nil, err
		}
		if len(refs) != len(pendingLeafPagePersists) {
			if scratch != nil {
				scratch.releaseChildRefBatch(refs)
			}
			return page.ChildRef{}, nil, fmt.Errorf("zipper: leaf page batch returned %d refs for %d pages", len(refs), len(pendingLeafPagePersists))
		}
		for i, pending := range pendingLeafPagePersists {
			ref := refs[i]
			recordZipperLeafLogPageRecordHintWrite(metrics, ref)
			if pending.root {
				rootNodeRef = ref
				continue
			}
			if pending.splitIdx >= 0 && pending.splitIdx < len(splits) {
				splits[pending.splitIdx].Ref = ref
			}
		}
		if scratch != nil {
			scratch.releaseChildRefBatch(refs)
		}
	}

	return rootNodeRef, splits, nil
}

func (z *Zipper) mergeInternal(oldNode *node.Node, builder *node.Builder, ops []batch.Entry, ranges []batch.DeleteRange, maintenance bool, budget *maintenanceBudget, metrics *adaptive.Metrics, retired *[]uint64, low, high []byte, scratch *mergeScratch, cfg applyRunConfig) (page.ChildRef, []Split, error) {
	if metrics != nil {
		metrics.ZipperInternalMerges++
	}
	count := oldNode.Count()

	var splits []Split

	var err error

	opIdx := 0

	gomaxprocs := runtime.GOMAXPROCS(0)
	useParallel := len(ranges) == 0 && shouldUseParallelInternalMerge(int(count), len(ops), gomaxprocs, maintenance, ParallelMergePressureNormal)
	if !useParallel && cfg.maxParallelWorkers > 1 && len(ranges) == 0 && !maintenance && gomaxprocs > 1 && count >= 2 {
		// The M2 opt-in path is already gated by the read-only leaf-span plan at
		// the DB boundary. Allow smaller internal fan-out than the legacy auto path
		// so planned spans can use the reusable worker pool without changing the
		// default apply behavior.
		useParallel = true
	}
	if useParallel && !maintenance {
		pressure := z.parallelMergePressureLevel()
		if pressure != ParallelMergePressureNormal {
			// Under sampled heap pressure, stay on the streaming path unless the
			// batch is materially larger. This avoids the childWork allocation and
			// extra recursive fan-out on the restore/fast path.
			useParallel = shouldUseParallelInternalMerge(int(count), len(ops), gomaxprocs, maintenance, pressure)
		}
	}

	copyKeys := oldNode.InternalBaseDeltaEnabled()
	var keyArena []byte
	if copyKeys {
		if v := internalKeyArenaPool.Get(); v != nil {
			keyArena = v.([]byte)[:0]
		} else {
			keyArena = make([]byte, 0, internalKeyArenaInitCap)
		}
		defer func() { putInternalKeyArena(keyArena) }()
	}
	cloneKey := func(src []byte) []byte {
		if !copyKeys || len(src) == 0 {
			return src
		}
		start := len(keyArena)
		keyArena = append(keyArena, src...)
		return keyArena[start : start+len(src)]
	}
	childLowKeyForOps := func(lowKey []byte, childOps []batch.Entry) []byte {
		if len(lowKey) == 0 || len(childOps) == 0 {
			return lowKey
		}
		if bytes.Compare(childOps[0].Key, lowKey) < 0 {
			return childOps[0].Key
		}
		return lowKey
	}

	target := builder
	appendInternalMaybeCopied := func(sourceIndex uint16, key []byte, childRef page.ChildRef, first bool, copySourceLeafLog bool) error {
		pageCount := z.pager.PageCount()
		if childRef.Kind == page.ChildRefPage && childRef.Page >= pageCount {
			return fmt.Errorf("zipper: detected OOB child ID %d (page_count=%d)", childRef.Page, pageCount)
		}
		if first && key == nil {
			key = []byte{}
		}
		entrySize := 2 + 8 + len(key)
		if childRef.Kind == page.ChildRefLeafLog {
			entrySize = 2 + page.LogRecordRefSize + len(key)
		} else if z.indexInternalBaseDelta {
			entrySize = 2 + 4 + len(key)
		}
		if z.internalSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else if copySourceLeafLog && childRef.Kind == page.ChildRefLeafLog && oldNode.InternalLeafLogRefsEnabled() {
			err = target.AddInternalLeafLogChildFromNode(oldNode, sourceIndex)
			if err == nil {
				recordZipperInternalChildRef(metrics, childRef)
				recordZipperInternalLeafLogRefCopy(metrics)
			}
		} else {
			err = target.AddInternalChildRef(key, childRef)
			if err == nil {
				recordZipperInternalChildRef(metrics, childRef)
			}
		}
		if err == node.ErrNodeFull {
			target, err = z.createNewSplitInternal(target, builder, &splits, key, childRef, metrics, scratch)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	appendInternal := func(key []byte, childRef page.ChildRef, first bool) error {
		return appendInternalMaybeCopied(0, key, childRef, first, false)
	}
	appendExistingInternal := func(sourceIndex uint16, key []byte, childRef page.ChildRef, first bool) error {
		return appendInternalMaybeCopied(sourceIndex, key, childRef, first, true)
	}

	// Fast path: most benchmarked writes are non-maintenance and below the
	// parallel threshold. Stream child processing directly and avoid building a
	// large childWork slice.
	if !maintenance && !useParallel {
		firstEntry := true
		var curKey []byte
		var curChild page.ChildRef
		if count > 0 {
			curKey, curChild, err = oldNode.GetInternalEntryRefView(0)
			if err != nil {
				return page.ChildRef{}, nil, err
			}
			if curKey == nil {
				curKey = []byte{}
			}
		}
		for i := uint16(0); i < count; i++ {
			lowKey := cloneKey(curKey)

			var (
				endKey    []byte
				nextKey   []byte
				nextChild page.ChildRef
			)
			if i+1 < count {
				nextKey, nextChild, err = oldNode.GetInternalEntryRefView(i + 1)
				if err != nil {
					return page.ChildRef{}, nil, err
				}
				if nextKey == nil {
					nextKey = []byte{}
				}
				endKey = nextKey
			}
			childHigh := high
			if endKey != nil {
				childHigh = endKey
			}

			startOpIdx := opIdx
			for opIdx < len(ops) {
				if endKey == nil || bytes.Compare(ops[opIdx].Key, endKey) < 0 {
					opIdx++
					continue
				}
				break
			}
			childOps := ops[startOpIdx:opIdx]
			childLowKey := childLowKeyForOps(lowKey, childOps)
			childRanges := deleteRangesForSpan(ranges, childLowKey, childHigh)

			newChildRef := curChild
			var childSplits []Split
			if len(childOps) > 0 || len(childRanges) > 0 {
				newChildRef, childSplits, err = z.writeRecursive(curChild, childOps, childRanges, maintenance, budget, metrics, childLowKey, childHigh, retired, scratch, false, cfg)
				if err != nil {
					return page.ChildRef{}, nil, err
				}
			}

			if len(childOps) == 0 && len(childRanges) == 0 {
				if err := appendExistingInternal(i, lowKey, newChildRef, firstEntry); err != nil {
					return page.ChildRef{}, nil, err
				}
			} else {
				if err := appendInternal(childLowKey, newChildRef, firstEntry); err != nil {
					return page.ChildRef{}, nil, err
				}
			}
			firstEntry = false
			for _, s := range childSplits {
				if err := appendInternal(s.Key, s.Ref, firstEntry); err != nil {
					return page.ChildRef{}, nil, err
				}
				firstEntry = false
			}

			curKey = nextKey
			curChild = nextChild
		}

		if target != builder {
			target.FinishNoNode()
			metrics.IndexWriteBytes += page.PageSize
			recordZipperInternalPageWrite(metrics)
		}
		return page.PageChildRef(builder.PageID()), splits, nil
	}

	children := getChildWorkSlice(int(count))
	defer putChildWorkSlice(children)

	for i := uint16(0); i < count; i++ {
		key, childRef, err := oldNode.GetInternalEntryRefView(i)
		if err != nil {
			return page.ChildRef{}, nil, err
		}
		if key == nil {
			key = []byte{}
		}
		keyCopy := cloneKey(key)
		children = append(children, childWork{
			key:   keyCopy,
			low:   keyCopy,
			child: childRef,
		})
	}

	for i := range children {
		var endKey []byte
		if i+1 < len(children) {
			endKey = children[i+1].key
		}
		childHigh := high
		if endKey != nil {
			childHigh = endKey
		}
		children[i].high = childHigh

		startOpIdx := opIdx
		for opIdx < len(ops) {
			if endKey == nil || bytes.Compare(ops[opIdx].Key, endKey) < 0 {
				opIdx++
				continue
			}
			break
		}
		children[i].ops = ops[startOpIdx:opIdx]
		if len(children[i].ops) > 0 {
			children[i].low = childLowKeyForOps(children[i].low, children[i].ops)
			children[i].key = children[i].low
		}
	}

	activeChildren := 0
	for i := range children {
		if len(children[i].ops) > 0 {
			activeChildren++
		}
	}
	if cfg.maxParallelWorkers == 1 {
		useParallel = false
	}
	if useParallel {
		const minParallelActiveChildren = 2
		minParallelOpsPerChild := 256
		if cfg.maxParallelWorkers > 1 {
			minParallelOpsPerChild = 4
		}
		if activeChildren < minParallelActiveChildren || len(ops)/activeChildren < minParallelOpsPerChild {
			useParallel = false
		}
	}

	// Best-effort: prefetch child pages before we start rewriting them. This can
	// help overlap read-ahead / fault handling with compute, especially in the
	// parallel path.
	if z.pager != nil {
		for i := range children {
			if len(children[i].ops) == 0 {
				continue
			}
			if children[i].child.Kind == page.ChildRefPage {
				z.pager.PrefetchPage(children[i].child.Page)
			}
		}
	}

	if useParallel {
		maxParallel := gomaxprocs
		if cfg.maxParallelWorkers > 1 && maxParallel > cfg.maxParallelWorkers {
			maxParallel = cfg.maxParallelWorkers
		}
		if activeChildren > 0 && maxParallel > activeChildren {
			maxParallel = activeChildren
		}
		if maxParallel < 1 {
			maxParallel = 1
		}
		recordZipperInternalParallelMerge(metrics, activeChildren, maxParallel, len(ops))
		for i := range children {
			if len(children[i].ops) == 0 {
				children[i].newChild = children[i].child
			}
		}
		var firstErr error
		var errOnce sync.Once
		childCfg := cfg
		if childCfg.workerPool != nil {
			// Avoid recursive worker-pool waits from a pool worker. Child subtrees are
			// independent and deterministic, but nested use of the same bounded pool can
			// deadlock when all workers are occupied by the parent run.
			childCfg.workerPool = nil
			childCfg.maxParallelWorkers = 1
		}
		var childScratches []*mergeScratch
		var childOldPointerRefs []PointerRefCounts
		var childOldEntriesRemoved []uint64
		if cfg.oldPointerRefs != nil {
			childOldPointerRefs = make([]PointerRefCounts, len(children))
		}
		if cfg.oldEntriesRemoved != nil {
			childOldEntriesRemoved = make([]uint64, len(children))
		}
		if scratch != nil {
			childScratches = scratch.acquireSpanWorkerScratchSlots(len(children))
			defer func() {
				for i := range childScratches {
					z.releaseApplyScratch(childScratches[i])
					childScratches[i] = nil
				}
				scratch.releaseSpanWorkerScratchSlots(childScratches)
			}()
		}
		runChild := func(i int) {
			if len(children[i].ops) == 0 {
				return
			}
			childScratch := scratch
			if childScratches != nil {
				childScratch = z.acquireApplyScratch()
				childScratches[i] = childScratch
			}
			var childMetrics adaptive.Metrics
			childRet := children[i].retired[:0]
			localCfg := childCfg
			if childOldPointerRefs != nil {
				localCfg.oldPointerRefs = &childOldPointerRefs[i]
			}
			if childOldEntriesRemoved != nil {
				localCfg.oldEntriesRemoved = &childOldEntriesRemoved[i]
			}
			ncID, cs, err := z.writeRecursive(children[i].child, children[i].ops, nil, maintenance, budget, &childMetrics, children[i].low, children[i].high, &childRet, childScratch, false, localCfg)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
			children[i].newChild = ncID
			children[i].splits = cs
			children[i].retired = childRet
			children[i].childStat = childMetrics
		}
		if cfg.workerPool != nil {
			if err := cfg.workerPool.Run(maxParallel, len(children), func(_ int, i int) { runChild(i) }); err != nil {
				return page.ChildRef{}, nil, err
			}
		} else {
			var nextJob int64 = -1
			var wg sync.WaitGroup
			worker := func() {
				defer wg.Done()
				for {
					i := int(atomic.AddInt64(&nextJob, 1))
					if i >= len(children) {
						return
					}
					runChild(i)
				}
			}
			for i := 0; i < maxParallel; i++ {
				wg.Add(1)
				go worker()
			}
			wg.Wait()
		}
		if firstErr != nil {
			return page.ChildRef{}, nil, firstErr
		}
		for i := range children {
			if len(children[i].ops) == 0 {
				continue
			}
			mergeMetrics(metrics, &children[i].childStat)
			if retired != nil && len(children[i].retired) > 0 {
				*retired = append(*retired, children[i].retired...)
			}
			if childOldPointerRefs != nil {
				cfg.oldPointerRefs.merge(&childOldPointerRefs[i])
			}
			if childOldEntriesRemoved != nil {
				*cfg.oldEntriesRemoved += childOldEntriesRemoved[i]
			}
		}
	} else {
		for i := range children {
			if len(children[i].ops) > 0 {
				ncID, cs, err := z.writeRecursive(children[i].child, children[i].ops, nil, maintenance, budget, metrics, children[i].low, children[i].high, retired, scratch, false, cfg)
				if err != nil {
					return page.ChildRef{}, nil, err
				}
				children[i].newChild = ncID
				children[i].splits = cs
			} else {
				children[i].newChild = children[i].child
			}
		}
	}

	if !maintenance {
		firstEntry := true
		for i := range children {
			child := &children[i]
			if len(child.ops) == 0 {
				if err := appendExistingInternal(uint16(i), child.key, child.newChild, firstEntry); err != nil {
					return page.ChildRef{}, nil, err
				}
			} else {
				if err := appendInternal(child.key, child.newChild, firstEntry); err != nil {
					return page.ChildRef{}, nil, err
				}
			}
			firstEntry = false
			for _, s := range child.splits {
				if err := appendInternal(s.Key, s.Ref, firstEntry); err != nil {
					return page.ChildRef{}, nil, err
				}
				firstEntry = false
			}
		}
		if target != builder {
			target.FinishNoNode()
			metrics.IndexWriteBytes += page.PageSize
			recordZipperInternalPageWrite(metrics)
		}
		return page.PageChildRef(builder.PageID()), splits, nil
	}

	totalEntries := len(children)
	for i := range children {
		totalEntries += len(children[i].splits)
	}
	entries := getInternalEntrySlice(totalEntries)
	defer func() { putInternalEntrySlice(entries) }()
	for i := range children {
		child := children[i]
		entries = append(entries, internalEntry{key: child.key, child: child.newChild})

		// Add sibling splits
		for _, s := range child.splits {
			entries = append(entries, internalEntry{key: s.Key, child: s.Ref})
		}
	}

	coalesced := entries
	var extraRetired []uint64
	coalesced, extraRetired, err = z.coalesceLeafChildren(entries, budget, metrics, scratch)
	if err != nil {
		return page.ChildRef{}, nil, err
	}
	if retired != nil && len(extraRetired) > 0 {
		*retired = append(*retired, extraRetired...)
	}

	coalesced, extraRetired, err = z.coalesceInternalChildren(coalesced, budget, metrics)
	if err != nil {
		return page.ChildRef{}, nil, err
	}
	if retired != nil && len(extraRetired) > 0 {
		*retired = append(*retired, extraRetired...)
	}

	// Write final internal entries, splitting if needed.
	for i := range coalesced {
		if err := appendInternal(coalesced[i].key, coalesced[i].child, i == 0); err != nil {
			return page.ChildRef{}, nil, err
		}
	}

	// Finalize last split node
	if target != builder {
		target.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		recordZipperInternalPageWrite(metrics)
	}

	// builder finalized by caller.
	return page.PageChildRef(builder.PageID()), splits, nil
}

func deleteRangesForSpan(ranges []batch.DeleteRange, low, high []byte) []batch.DeleteRange {
	if len(ranges) == 0 {
		return nil
	}
	start := -1
	end := -1
	for i, r := range ranges {
		if batch.DeleteRangeOverlapsSpan(r, low, high) {
			if start < 0 {
				start = i
			}
			end = i + 1
			continue
		}
		if start >= 0 {
			break
		}
		if high != nil && r.Start != nil && bytes.Compare(r.Start, high) >= 0 {
			break
		}
	}
	if start < 0 {
		return nil
	}
	return ranges[start:end]
}

func mergeMetrics(dst, src *adaptive.Metrics) {
	if dst == nil || src == nil {
		return
	}
	dst.LeafFill += src.LeafFill
	dst.Splits += src.Splits
	dst.IndexWriteBytes += src.IndexWriteBytes
	dst.SlabWriteBytes += src.SlabWriteBytes
	dst.SlabDeadBytes += src.SlabDeadBytes
	dst.ZipperApplyOps += src.ZipperApplyOps
	dst.ZipperNodeLoads += src.ZipperNodeLoads
	dst.ZipperPagerNodeLoads += src.ZipperPagerNodeLoads
	dst.ZipperLeafLogNodeLoads += src.ZipperLeafLogNodeLoads
	dst.ZipperLeafLogCacheHits += src.ZipperLeafLogCacheHits
	dst.ZipperLeafLogReaderCalls += src.ZipperLeafLogReaderCalls
	dst.ZipperLeafLogViewReads += src.ZipperLeafLogViewReads
	dst.ZipperLeafLogScratchReads += src.ZipperLeafLogScratchReads
	dst.ZipperPagerNodeBytesRead += src.ZipperPagerNodeBytesRead
	dst.ZipperLeafLogNodeBytesRead += src.ZipperLeafLogNodeBytesRead
	dst.ZipperLeafLogRecordHintBytesRead += src.ZipperLeafLogRecordHintBytesRead
	dst.ZipperLeafMerges += src.ZipperLeafMerges
	dst.ZipperInternalMerges += src.ZipperInternalMerges
	dst.ZipperInternalParallelMerges += src.ZipperInternalParallelMerges
	dst.ZipperInternalParallelChildren += src.ZipperInternalParallelChildren
	dst.ZipperInternalParallelWorkers += src.ZipperInternalParallelWorkers
	dst.ZipperInternalParallelOps += src.ZipperInternalParallelOps
	dst.ZipperLeafPagesWritten += src.ZipperLeafPagesWritten
	dst.ZipperPagerLeafPagesWritten += src.ZipperPagerLeafPagesWritten
	dst.ZipperLeafLogPagesWritten += src.ZipperLeafLogPagesWritten
	dst.ZipperLeafPageBytesWritten += src.ZipperLeafPageBytesWritten
	dst.ZipperPagerLeafPageBytesWritten += src.ZipperPagerLeafPageBytesWritten
	dst.ZipperLeafLogPageBytesWritten += src.ZipperLeafLogPageBytesWritten
	dst.ZipperLeafLogRecordHintBytesWritten += src.ZipperLeafLogRecordHintBytesWritten
	dst.ZipperInternalPagesWritten += src.ZipperInternalPagesWritten
	dst.ZipperInternalPageBytesWritten += src.ZipperInternalPageBytesWritten
	dst.ZipperInternalChildRefs += src.ZipperInternalChildRefs
	dst.ZipperInternalPageChildRefs += src.ZipperInternalPageChildRefs
	dst.ZipperInternalLeafLogRefs += src.ZipperInternalLeafLogRefs
	dst.ZipperInternalLeafLogRefCopies += src.ZipperInternalLeafLogRefCopies
	dst.ZipperRootSplitLevels += src.ZipperRootSplitLevels
	dst.ZipperApplyWallNs += src.ZipperApplyWallNs
	dst.ZipperRootReduceNs += src.ZipperRootReduceNs
	dst.ZipperLeafLogOutputReservationWaitNs += src.ZipperLeafLogOutputReservationWaitNs
	dst.ZipperLeafLogOutputAppendWaitNs += src.ZipperLeafLogOutputAppendWaitNs
	dst.ZipperLeafLogOutputAppendCalls += src.ZipperLeafLogOutputAppendCalls
	dst.ZipperLeafLogOutputAppendPages += src.ZipperLeafLogOutputAppendPages
	if src.ZipperLeafLogOutputLaneTaskTotal > 0 {
		dst.ZipperLeafLogOutputLaneTaskTotal += src.ZipperLeafLogOutputLaneTaskTotal
		for i := range dst.ZipperLeafLogOutputLaneTasks {
			dst.ZipperLeafLogOutputLaneTasks[i] += src.ZipperLeafLogOutputLaneTasks[i]
		}
	}
	dst.ZipperLeafLogOutputLaneTaskOverflow += src.ZipperLeafLogOutputLaneTaskOverflow
	dst.ZipperSpanNativeWorkerBusyNs += src.ZipperSpanNativeWorkerBusyNs
	dst.ZipperSpanNativeWorkerIdleNs += src.ZipperSpanNativeWorkerIdleNs
	dst.ZipperSpanNativeWorkerWaitNs += src.ZipperSpanNativeWorkerWaitNs
	dst.ZipperSpanNativeReadyTasks += src.ZipperSpanNativeReadyTasks
	dst.ZipperSpanNativeDispatchedTasks += src.ZipperSpanNativeDispatchedTasks
	dst.ZipperSpanNativeCompletedTasks += src.ZipperSpanNativeCompletedTasks
	if src.ZipperSpanNativeQueueDepthMax > dst.ZipperSpanNativeQueueDepthMax {
		dst.ZipperSpanNativeQueueDepthMax = src.ZipperSpanNativeQueueDepthMax
	}
	dst.ZipperSpanNativeScheduledWorkers += src.ZipperSpanNativeScheduledWorkers
	if src.ZipperSpanNativeScheduledWorkersMax > dst.ZipperSpanNativeScheduledWorkersMax {
		dst.ZipperSpanNativeScheduledWorkersMax = src.ZipperSpanNativeScheduledWorkersMax
	}
	dst.ZipperSpanNativeTaskSpansTotal += src.ZipperSpanNativeTaskSpansTotal
	if src.ZipperSpanNativeTaskSpansMin > 0 && (dst.ZipperSpanNativeTaskSpansMin == 0 || src.ZipperSpanNativeTaskSpansMin < dst.ZipperSpanNativeTaskSpansMin) {
		dst.ZipperSpanNativeTaskSpansMin = src.ZipperSpanNativeTaskSpansMin
	}
	if src.ZipperSpanNativeTaskSpansMax > dst.ZipperSpanNativeTaskSpansMax {
		dst.ZipperSpanNativeTaskSpansMax = src.ZipperSpanNativeTaskSpansMax
	}
	dst.ZipperSpanNativeTaskOpsTotal += src.ZipperSpanNativeTaskOpsTotal
	if src.ZipperSpanNativeTaskOpsMin > 0 && (dst.ZipperSpanNativeTaskOpsMin == 0 || src.ZipperSpanNativeTaskOpsMin < dst.ZipperSpanNativeTaskOpsMin) {
		dst.ZipperSpanNativeTaskOpsMin = src.ZipperSpanNativeTaskOpsMin
	}
	if src.ZipperSpanNativeTaskOpsMax > dst.ZipperSpanNativeTaskOpsMax {
		dst.ZipperSpanNativeTaskOpsMax = src.ZipperSpanNativeTaskOpsMax
	}
	dst.ZipperSpanNativeTaskBytesTotal += src.ZipperSpanNativeTaskBytesTotal
	if src.ZipperSpanNativeTaskBytesMin > 0 && (dst.ZipperSpanNativeTaskBytesMin == 0 || src.ZipperSpanNativeTaskBytesMin < dst.ZipperSpanNativeTaskBytesMin) {
		dst.ZipperSpanNativeTaskBytesMin = src.ZipperSpanNativeTaskBytesMin
	}
	if src.ZipperSpanNativeTaskBytesMax > dst.ZipperSpanNativeTaskBytesMax {
		dst.ZipperSpanNativeTaskBytesMax = src.ZipperSpanNativeTaskBytesMax
	}
	dst.ZipperSpanNativeSingleSpanTasks += src.ZipperSpanNativeSingleSpanTasks

	if src.SlabWriteBytesByFile != nil {
		if dst.SlabWriteBytesByFile == nil {
			dst.SlabWriteBytesByFile = make(map[uint32]int64, len(src.SlabWriteBytesByFile))
		}
		for id, n := range src.SlabWriteBytesByFile {
			dst.SlabWriteBytesByFile[id] += n
		}
	}
	if src.SlabDeadBytesByFile != nil {
		if dst.SlabDeadBytesByFile == nil {
			dst.SlabDeadBytesByFile = make(map[uint32]int64, len(src.SlabDeadBytesByFile))
		}
		for id, n := range src.SlabDeadBytesByFile {
			dst.SlabDeadBytesByFile[id] += n
		}
	}
}

func (z *Zipper) coalesceLeafChildren(entries []internalEntry, budget *maintenanceBudget, metrics *adaptive.Metrics, scratch *mergeScratch) ([]internalEntry, []uint64, error) {
	if len(entries) < 2 {
		return entries, nil, nil
	}
	if budget != nil && !budget.allow() {
		return entries, nil, nil
	}

	var retired []uint64

	loadLeaf := func(ref page.ChildRef) (node.Node, bool, bool, []byte, bool, error) {
		n, fromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(ref, scratch)
		if err != nil {
			if leafScratchRef {
				releaseLeafPageScratch(scratch, leafScratch)
			}
			return node.Node{}, false, false, nil, false, err
		}
		recordZipperNodeLoad(metrics, ref, n, loadSource)
		if n.Type() != page.PageTypeLeaf {
			if leafScratchRef {
				releaseLeafPageScratch(scratch, leafScratch)
			}
			return node.Node{}, false, false, nil, false, nil
		}
		return n, fromPager, true, leafScratch, leafScratchRef, nil
	}

	// First pass: prune empty leaf children (except keep the first slot).
	if budget != nil && !budget.take(1) {
		return entries, nil, nil
	}
	out := entries[:0]
	for i, e := range entries {
		if i == 0 {
			out = append(out, e)
			continue
		}
		n, fromPager, ok, leafScratch, leafScratchRef, err := loadLeaf(e.child)
		if err != nil {
			return nil, nil, err
		}
		if ok && n.Count() == 0 {
			if leafScratchRef {
				releaseLeafPageScratch(scratch, leafScratch)
			}
			if fromPager {
				retired = append(retired, e.child.Page)
			}
			continue
		}
		if leafScratchRef {
			releaseLeafPageScratch(scratch, leafScratch)
		}
		out = append(out, e)
	}
	entries = out
	if len(entries) < 2 {
		return entries, retired, nil
	}

	const underfullPPM = 350_000
	pageCap := page.PageSize - node.NodeHeaderSize

	leafEntryBytes := func(key, val []byte, ptr page.ValuePtr, flags byte, entryRevisions bool) int {
		entrySize := 7 + len(key)
		if flags&node.FlagPointer != 0 {
			entrySize += page.ValuePtrSize
		} else {
			entrySize += len(val)
		}
		if entryRevisions {
			entrySize += page.EntryRevisionSize
		}
		return entrySize + node.DirectoryEntrySize
	}

	leafRequiredBytes := func(n *node.Node) (int, error) {
		sum := 0
		entryRevisions := n.LeafEntryRevisionsEnabled()
		for i := uint16(0); i < n.Count(); i++ {
			k, v, ptr, flags, _, err := n.GetLeafEntryViewWithRevision(i)
			if err != nil {
				return 0, err
			}
			if flags&node.FlagTombstone != 0 {
				continue
			}
			sum += leafEntryBytes(k, v, ptr, flags, entryRevisions)
			if sum > pageCap {
				return sum, nil
			}
		}
		return sum, nil
	}

	fillPPM := func(n *node.Node) uint32 {
		used := page.PageSize - n.FreeSpace()
		return uint32((used * 1_000_000) / page.PageSize)
	}

	buildMergedLeaf := func(left, right *node.Node) (page.ChildRef, bool, error) {
		var (
			pid  uint64
			data []byte
			err  error
		)
		if z.outerLeavesInValueLog {
			data = make([]byte, page.PageSize)
		} else {
			pid, err = z.allocator.Alloc(left.PageID())
			if err != nil {
				return page.ChildRef{}, false, err
			}
			data, err = z.pager.GetForWrite(pid)
			if err != nil {
				return page.ChildRef{}, false, err
			}
		}
		b := z.newLeafBuilder(data, nil)
		b.SetPageID(pid)
		if left.LeafEntryRevisionsEnabled() || right.LeafEntryRevisionsEnabled() {
			z.enableLeafBuilderEntryRevisions(b, nil)
		}

		addAll := func(n *node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, v, ptr, flags, revision, err := n.GetLeafEntryViewWithRevision(i)
				if err != nil {
					return err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				if err := b.AddLeafEntryWithRevision(k, v, flags, ptr, revision); err != nil {
					return err
				}
			}
			return nil
		}

		if err := addAll(left); err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, pid)
			}
			if err == node.ErrNodeFull {
				return page.ChildRef{}, false, nil
			}
			return page.ChildRef{}, false, err
		}
		if err := addAll(right); err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, pid)
			}
			if err == node.ErrNodeFull {
				return page.ChildRef{}, false, nil
			}
			return page.ChildRef{}, false, err
		}

		b.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-b.FreeSpace()) / float64(page.PageSize)
		recordZipperLeafPageWrite(metrics, z.outerLeavesInValueLog)
		leafID, err := z.persistLeafPage(b, metrics)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, pid)
			}
			return page.ChildRef{}, false, err
		}
		recordZipperLeafLogPageRecordHintWrite(metrics, leafID)
		return leafID, true, nil
	}

	copyLeaf := func(ref page.ChildRef, hint uint64) (page.ChildRef, error) {
		n, _, ok, leafScratch, leafScratchRef, err := loadLeaf(ref)
		if err != nil {
			return page.ChildRef{}, err
		}
		if !ok {
			if leafScratchRef {
				releaseLeafPageScratch(scratch, leafScratch)
			}
			return page.ChildRef{}, errors.New("copyLeaf: not a leaf")
		}
		if leafScratchRef {
			defer releaseLeafPageScratch(scratch, leafScratch)
		}

		var (
			pid  uint64
			data []byte
		)
		if z.outerLeavesInValueLog {
			data = make([]byte, page.PageSize)
		} else {
			pid, err = z.allocator.Alloc(hint)
			if err != nil {
				return page.ChildRef{}, err
			}
			data, err = z.pager.GetForWrite(pid)
			if err != nil {
				return page.ChildRef{}, err
			}
		}
		b := z.newLeafBuilder(data, nil)
		b.SetPageID(pid)
		if n.LeafEntryRevisionsEnabled() {
			z.enableLeafBuilderEntryRevisions(b, nil)
		}

		for i := uint16(0); i < n.Count(); i++ {
			k, v, ptr, flags, revision, err := n.GetLeafEntryViewWithRevision(i)
			if err != nil {
				return page.ChildRef{}, err
			}
			if flags&node.FlagTombstone != 0 {
				continue
			}
			if err := b.AddLeafEntryWithRevision(k, v, flags, ptr, revision); err != nil {
				return page.ChildRef{}, err
			}
		}
		b.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		recordZipperLeafPageWrite(metrics, z.outerLeavesInValueLog)
		leafID, err := z.persistLeafPage(b, metrics)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, pid)
			}
			return page.ChildRef{}, err
		}
		recordZipperLeafLogPageRecordHintWrite(metrics, leafID)
		return leafID, nil
	}

	rebalanceLeaves := func(left, right *node.Node) (leftID page.ChildRef, rightID page.ChildRef, rightStart []byte, ok bool, err error) {
		var (
			lid uint64
			rid uint64
		)
		var (
			ldata []byte
			rdata []byte
		)
		if z.outerLeavesInValueLog {
			ldata = make([]byte, page.PageSize)
			rdata = make([]byte, page.PageSize)
		} else if allocMany, ok := z.allocator.(interface {
			AllocMany(count int, hint uint64) ([]uint64, error)
		}); ok {
			ids, err := allocMany.AllocMany(2, left.PageID())
			if err != nil {
				if len(ids) > 0 {
					retired = append(retired, ids...)
				}
				return page.ChildRef{}, page.ChildRef{}, nil, false, err
			}
			if len(ids) < 2 {
				return page.ChildRef{}, page.ChildRef{}, nil, false, errors.New("rebalanceLeaves: insufficient pages allocated")
			}
			lid, rid = ids[0], ids[1]
		} else {
			lid, err = z.allocator.Alloc(left.PageID())
			if err != nil {
				return page.ChildRef{}, page.ChildRef{}, nil, false, err
			}
			rid, err = z.allocator.Alloc(lid)
			if err != nil {
				retired = append(retired, lid)
				return page.ChildRef{}, page.ChildRef{}, nil, false, err
			}
		}
		if !z.outerLeavesInValueLog {
			ldata, err = z.pager.GetForWrite(lid)
			if err != nil {
				return page.ChildRef{}, page.ChildRef{}, nil, false, err
			}
			rdata, err = z.pager.GetForWrite(rid)
			if err != nil {
				retired = append(retired, lid, rid)
				return page.ChildRef{}, page.ChildRef{}, nil, false, err
			}
		}
		lb := z.newLeafBuilder(ldata, nil)
		lb.SetPageID(lid)
		entryRevisions := left.LeafEntryRevisionsEnabled() || right.LeafEntryRevisionsEnabled()
		if entryRevisions {
			z.enableLeafBuilderEntryRevisions(lb, nil)
		}

		// Collect combined entries in-order without copying.
		type ev struct {
			k        []byte
			v        []byte
			ptr      page.ValuePtr
			revision page.EntryRevision
			flags    byte
			size     int
		}
		combined := make([]ev, 0, int(left.Count()+right.Count()))
		for _, src := range []*node.Node{left, right} {
			for i := uint16(0); i < src.Count(); i++ {
				k, v, ptr, flags, revision, err := src.GetLeafEntryViewWithRevision(i)
				if err != nil {
					if !z.outerLeavesInValueLog {
						retired = append(retired, lid, rid)
					}
					return page.ChildRef{}, page.ChildRef{}, nil, false, err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				if z.leafPrefixCompression {
					k = append([]byte(nil), k...)
				}
				combined = append(combined, ev{k: k, v: v, ptr: ptr, revision: revision, flags: flags, size: leafEntryBytes(k, v, ptr, flags, entryRevisions)})
			}
		}
		if len(combined) < 2 {
			if !z.outerLeavesInValueLog {
				retired = append(retired, lid, rid)
			}
			return page.ChildRef{}, page.ChildRef{}, nil, false, nil
		}

		// Choose a split point by bytes (closest to 50/50) to avoid repeated
		// underfull siblings and to guarantee the rebalance fits.
		prefixBytes := make([]int, len(combined)+1)
		for i, e := range combined {
			prefixBytes[i+1] = prefixBytes[i] + e.size
		}
		totalBytes := prefixBytes[len(combined)]

		// Enforce the configured soft-full reserve (if any) by ensuring the
		// constructed pages leave at least leafReserveBytes free space.
		cap := pageCap
		if z.leafReserveBytes > 0 && z.leafReserveBytes < pageCap {
			cap = pageCap - z.leafReserveBytes
		}

		bestSplitAt := -1
		bestDelta := int(^uint(0) >> 1) // MaxInt
		for splitAt := 1; splitAt < len(combined); splitAt++ {
			leftBytes := prefixBytes[splitAt]
			rightBytes := totalBytes - leftBytes
			if leftBytes > cap || rightBytes > cap {
				continue
			}
			delta := leftBytes - rightBytes
			if delta < 0 {
				delta = -delta
			}
			if delta < bestDelta {
				bestDelta = delta
				bestSplitAt = splitAt
			}
		}
		if bestSplitAt <= 0 || bestSplitAt >= len(combined) {
			if !z.outerLeavesInValueLog {
				retired = append(retired, lid, rid)
			}
			return page.ChildRef{}, page.ChildRef{}, nil, false, nil
		}

		rb := z.newLeafBuilder(rdata, nil)
		rb.SetPageID(rid)
		if entryRevisions {
			z.enableLeafBuilderEntryRevisions(rb, nil)
		}

		for i := 0; i < bestSplitAt; i++ {
			if err := lb.AddLeafEntryWithRevision(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr, combined[i].revision); err != nil {
				if !z.outerLeavesInValueLog {
					retired = append(retired, lid, rid)
				}
				return page.ChildRef{}, page.ChildRef{}, nil, false, err
			}
		}
		rightStart = append([]byte(nil), combined[bestSplitAt].k...)
		for i := bestSplitAt; i < len(combined); i++ {
			if err := rb.AddLeafEntryWithRevision(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr, combined[i].revision); err != nil {
				if !z.outerLeavesInValueLog {
					retired = append(retired, lid, rid)
				}
				return page.ChildRef{}, page.ChildRef{}, nil, false, err
			}
		}

		lb.FinishNoNode()
		rb.FinishNoNode()
		metrics.IndexWriteBytes += 2 * page.PageSize
		metrics.LeafFill += float64(page.PageSize-lb.FreeSpace()) / float64(page.PageSize)
		metrics.LeafFill += float64(page.PageSize-rb.FreeSpace()) / float64(page.PageSize)
		recordZipperLeafPageWrite(metrics, z.outerLeavesInValueLog)
		recordZipperLeafPageWrite(metrics, z.outerLeavesInValueLog)
		leftID, err = z.persistLeafPage(lb, metrics)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, lid, rid)
			}
			return page.ChildRef{}, page.ChildRef{}, nil, false, err
		}
		recordZipperLeafLogPageRecordHintWrite(metrics, leftID)
		rightID, err = z.persistLeafPage(rb, metrics)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, lid, rid)
			}
			return page.ChildRef{}, page.ChildRef{}, nil, false, err
		}
		recordZipperLeafLogPageRecordHintWrite(metrics, rightID)
		return leftID, rightID, rightStart, true, nil
	}

	// Second pass: attempt sibling merge/rebalance for underfull adjacent leaves.
	i := 0
	for i < len(entries)-1 {
		if budget != nil && !budget.take(1) {
			break
		}
		leftID := entries[i].child
		rightID := entries[i+1].child

		left, leftFromPager, okL, leftScratch, leftScratchRef, err := loadLeaf(leftID)
		if err != nil {
			return nil, nil, err
		}
		right, rightFromPager, okR, rightScratch, rightScratchRef, err := loadLeaf(rightID)
		if err != nil {
			if leftScratchRef {
				releaseLeafPageScratch(scratch, leftScratch)
			}
			return nil, nil, err
		}
		releaseLeafScratch := func() {
			if leftScratchRef {
				releaseLeafPageScratch(scratch, leftScratch)
				leftScratch = nil
				leftScratchRef = false
			}
			if rightScratchRef {
				releaseLeafPageScratch(scratch, rightScratch)
				rightScratch = nil
				rightScratchRef = false
			}
		}
		if !okL || !okR {
			releaseLeafScratch()
			i++
			continue
		}

		if left.Count() == 0 {
			// If this is a non-first child it would have been pruned already.
			releaseLeafScratch()
			i++
			continue
		}

		leftFill := fillPPM(&left)
		rightFill := fillPPM(&right)
		if leftFill >= underfullPPM && rightFill >= underfullPPM {
			// If not merging/rebalancing, check piggyback
			if z.piggybackCompaction && !z.outerLeavesInValueLog && leftFromPager && rightFromPager && leftID.Kind == page.ChildRefPage && rightID.Kind == page.ChildRefPage {
				const distanceThreshold = 2500 // ~10MB
				dist := int64(leftID.Page) - int64(rightID.Page)
				if dist < 0 {
					dist = -dist
				}

				if dist > distanceThreshold {
					// Move the "older" one (lower ID) towards the newer one.
					if leftID.Page < rightID.Page {
						newID, err := copyLeaf(leftID, rightID.Page)
						if err == nil {
							retired = append(retired, leftID.Page)
							entries[i].child = newID
						}
					} else {
						newID, err := copyLeaf(rightID, leftID.Page)
						if err == nil {
							retired = append(retired, rightID.Page)
							entries[i+1].child = newID
						}
					}
				}
			}
			releaseLeafScratch()
			i++
			continue
		}

		leftBytes, err := leafRequiredBytes(&left)
		if err != nil {
			releaseLeafScratch()
			return nil, nil, err
		}
		rightBytes, err := leafRequiredBytes(&right)
		if err != nil {
			releaseLeafScratch()
			return nil, nil, err
		}

		mergeCap := pageCap
		if z.leafReserveBytes > 0 && z.leafReserveBytes < pageCap {
			mergeCap = pageCap - z.leafReserveBytes
		}
		if leftBytes+rightBytes <= mergeCap {
			mergedID, ok, err := buildMergedLeaf(&left, &right)
			if err != nil {
				releaseLeafScratch()
				return nil, nil, err
			}
			if ok {
				if leftFromPager && leftID.Kind == page.ChildRefPage {
					retired = append(retired, leftID.Page)
				}
				if rightFromPager && rightID.Kind == page.ChildRefPage {
					retired = append(retired, rightID.Page)
				}
				entries[i].child = mergedID
				copy(entries[i+1:], entries[i+2:])
				entries = entries[:len(entries)-1]
				if i > 0 {
					i--
				}
				releaseLeafScratch()
				continue
			}
		}

		// If merge isn't possible, attempt a bounded rebalance.
		leftNewID, rightNewID, rightStart, ok, err := rebalanceLeaves(&left, &right)
		if err != nil {
			releaseLeafScratch()
			return nil, nil, err
		}
		if ok && len(rightStart) > 0 {
			if leftFromPager && leftID.Kind == page.ChildRefPage {
				retired = append(retired, leftID.Page)
			}
			if rightFromPager && rightID.Kind == page.ChildRefPage {
				retired = append(retired, rightID.Page)
			}
			entries[i].child = leftNewID
			entries[i+1].child = rightNewID
			entries[i+1].key = rightStart
		}
		releaseLeafScratch()
		i++
	}

	return entries, retired, nil
}

func (z *Zipper) coalesceInternalChildren(entries []internalEntry, budget *maintenanceBudget, metrics *adaptive.Metrics) ([]internalEntry, []uint64, error) {
	if len(entries) < 2 {
		return entries, nil, nil
	}
	if budget != nil && !budget.allow() {
		return entries, nil, nil
	}

	var retired []uint64

	loadInternal := func(ref page.ChildRef) (*node.Node, bool, error) {
		if ref.Kind != page.ChildRefPage {
			return nil, false, nil
		}
		id := ref.Page
		pageCount := z.pager.PageCount()
		if id >= pageCount {
			return nil, false, fmt.Errorf("zipper: detected OOB child ID %d (page_count=%d)", id, pageCount)
		}
		data, err := z.pager.Get(id)
		if err != nil {
			return nil, false, err
		}
		n := node.NewNode(data)
		recordZipperNodeLoad(metrics, ref, *n, zipperNodeLoadPager)
		if n.Type() != page.PageTypeInternal {
			return nil, false, nil
		}
		return n, true, nil
	}

	fillPPM := func(n *node.Node) uint32 {
		used := page.PageSize - n.FreeSpace()
		return uint32((used * 1_000_000) / page.PageSize)
	}

	pageCap := page.PageSize - node.NodeHeaderSize
	internalEntryBytes := func(key []byte) int {
		if key == nil {
			key = []byte{}
		}
		// Internal entry: keylen(uint16) + child + key bytes + directory entry.
		entrySize := 2 + 8 + len(key)
		if z.indexInternalBaseDelta {
			entrySize = 2 + 4 + len(key)
		}
		return entrySize + node.DirectoryEntrySize
	}
	internalRequiredBytes := func(n *node.Node) (int, error) {
		sum := 0
		for i := uint16(0); i < n.Count(); i++ {
			k, _, err := n.GetInternalEntryRefView(i)
			if err != nil {
				return 0, err
			}
			sum += internalEntryBytes(k)
			if sum > pageCap {
				return sum, nil
			}
		}
		if z.indexInternalBaseDelta && sum > 0 {
			// internal base-delta footer tail:
			// [u16 lowLen][u16 highLen][u16 prefixLen][u64 baseChildID]
			sum += 14
		}
		return sum, nil
	}

	buildMergedInternal := func(left, right *node.Node) (page.ChildRef, bool, error) {
		pid, err := z.allocator.Alloc(left.PageID())
		if err != nil {
			return page.ChildRef{}, false, err
		}
		data, err := z.pager.GetForWrite(pid)
		if err != nil {
			return page.ChildRef{}, false, err
		}
		b := z.newBuilderForType(data, page.PageTypeInternal, nil)
		b.SetPageID(pid)

		addAll := func(n *node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, child, err := n.GetInternalEntryRefView(i)
				if err != nil {
					return err
				}
				if k == nil {
					k = []byte{}
				}
				entrySize := 2 + 8 + len(k)
				if z.indexInternalBaseDelta {
					entrySize = 2 + 4 + len(k)
				}
				if z.internalSoftFull(b, entrySize) {
					return node.ErrNodeFull
				}
				if err := b.AddInternalChildRef(k, child); err != nil {
					return err
				}
				recordZipperInternalChildRef(metrics, child)
			}
			return nil
		}

		if err := addAll(left); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return page.ChildRef{}, false, nil
			}
			return page.ChildRef{}, false, err
		}
		if err := addAll(right); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return page.ChildRef{}, false, nil
			}
			return page.ChildRef{}, false, err
		}

		b.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		recordZipperInternalPageWrite(metrics)
		return page.PageChildRef(pid), true, nil
	}

	rebalanceInternals := func(left, right *node.Node) (leftID uint64, rightID uint64, rightStart []byte, ok bool, err error) {
		var (
			lid uint64
			rid uint64
		)
		if allocMany, ok := z.allocator.(interface {
			AllocMany(count int, hint uint64) ([]uint64, error)
		}); ok {
			ids, err := allocMany.AllocMany(2, left.PageID())
			if err != nil {
				if len(ids) > 0 {
					retired = append(retired, ids...)
				}
				return 0, 0, nil, false, err
			}
			if len(ids) < 2 {
				return 0, 0, nil, false, errors.New("rebalanceInternals: insufficient pages allocated")
			}
			lid, rid = ids[0], ids[1]
		} else {
			lid, err = z.allocator.Alloc(left.PageID())
			if err != nil {
				return 0, 0, nil, false, err
			}
			rid, err = z.allocator.Alloc(lid)
			if err != nil {
				retired = append(retired, lid)
				return 0, 0, nil, false, err
			}
		}
		ldata, err := z.pager.GetForWrite(lid)
		if err != nil {
			return 0, 0, nil, false, err
		}
		rdata, err := z.pager.GetForWrite(rid)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}

		combined := make([]internalEntry, 0, int(left.Count()+right.Count()))
		for _, src := range []*node.Node{left, right} {
			for i := uint16(0); i < src.Count(); i++ {
				k, child, err := src.GetInternalEntryRefView(i)
				if err != nil {
					retired = append(retired, lid, rid)
					return 0, 0, nil, false, err
				}
				if k == nil {
					k = []byte{}
				}
				kCopy := append([]byte(nil), k...)
				combined = append(combined, internalEntry{key: kCopy, child: child})
			}
		}
		if len(combined) < 2 {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, nil
		}

		splitAt := len(combined) / 2
		if splitAt < 1 {
			splitAt = 1
		}
		if splitAt >= len(combined) {
			splitAt = len(combined) - 1
		}

		build := func(b *node.Builder, list []internalEntry) error {
			for i, e := range list {
				k := e.key
				if k == nil {
					k = []byte{}
				}
				entrySize := 2 + 8 + len(k)
				if z.indexInternalBaseDelta {
					entrySize = 2 + 4 + len(k)
				}
				if i > 0 && z.internalSoftFull(b, entrySize) {
					return node.ErrNodeFull
				}
				if err := b.AddInternalChildRef(k, e.child); err != nil {
					return err
				}
				recordZipperInternalChildRef(metrics, e.child)
			}
			return nil
		}

		try := func(splitAt int) ([]byte, bool, error) {
			lb2 := z.newBuilderForType(ldata, page.PageTypeInternal, nil)
			lb2.SetPageID(lid)
			rb2 := z.newBuilderForType(rdata, page.PageTypeInternal, nil)
			rb2.SetPageID(rid)

			if err := build(lb2, combined[:splitAt]); err != nil {
				if err == node.ErrNodeFull {
					return nil, false, nil
				}
				return nil, false, err
			}
			if err := build(rb2, combined[splitAt:]); err != nil {
				if err == node.ErrNodeFull {
					return nil, false, nil
				}
				return nil, false, err
			}
			lb2.FinishNoNode()
			rb2.FinishNoNode()
			rs := combined[splitAt].key
			if rs == nil {
				rs = []byte{}
			}
			return append([]byte(nil), rs...), true, nil
		}

		// Adjust split point until both sides fit.
		rightStart, ok, err = try(splitAt)
		if err != nil {
			retired = append(retired, lid, rid)
			return 0, 0, nil, false, err
		}
		if !ok {
			for d := 1; d < len(combined)-1; d++ {
				if splitAt-d >= 1 {
					if rs, ok2, err2 := try(splitAt - d); err2 != nil {
						retired = append(retired, lid, rid)
						return 0, 0, nil, false, err2
					} else if ok2 {
						rightStart = rs
						ok = true
						break
					}
				}
				if splitAt+d < len(combined) {
					if rs, ok2, err2 := try(splitAt + d); err2 != nil {
						retired = append(retired, lid, rid)
						return 0, 0, nil, false, err2
					} else if ok2 {
						rightStart = rs
						ok = true
						break
					}
				}
			}
			if !ok {
				retired = append(retired, lid, rid)
				return 0, 0, nil, false, nil
			}
		}

		metrics.IndexWriteBytes += 2 * page.PageSize
		recordZipperInternalPageWrite(metrics)
		recordZipperInternalPageWrite(metrics)
		return lid, rid, rightStart, true, nil
	}

	const underfullPPM = 350_000

	if budget != nil && !budget.take(1) {
		return entries, retired, nil
	}

	i := 0
	for i < len(entries)-1 {
		if budget != nil && !budget.take(1) {
			break
		}
		leftID := entries[i].child
		rightID := entries[i+1].child

		left, okL, err := loadInternal(leftID)
		if err != nil {
			return nil, nil, err
		}
		right, okR, err := loadInternal(rightID)
		if err != nil {
			return nil, nil, err
		}
		if !okL || !okR {
			i++
			continue
		}

		leftFill := fillPPM(left)
		rightFill := fillPPM(right)
		if leftFill >= underfullPPM && rightFill >= underfullPPM {
			i++
			continue
		}

		leftBytes, err := internalRequiredBytes(left)
		if err != nil {
			return nil, nil, err
		}
		rightBytes, err := internalRequiredBytes(right)
		if err != nil {
			return nil, nil, err
		}

		// Attempt a full sibling merge when the combined entries should fit in one page
		// while still respecting the configured soft-full reserve (if any).
		if leftBytes+rightBytes <= pageCap-z.internalReserveBytes {
			mergedID, ok, err := buildMergedInternal(left, right)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				retired = append(retired, leftID.Page, rightID.Page)
				entries[i].child = mergedID
				copy(entries[i+1:], entries[i+2:])
				entries = entries[:len(entries)-1]
				if i > 0 {
					i--
				}
				continue
			}
		}

		leftNewID, rightNewID, rightStart, ok, err := rebalanceInternals(left, right)
		if err != nil {
			return nil, nil, err
		}
		if ok && len(rightStart) > 0 {
			retired = append(retired, leftID.Page, rightID.Page)
			entries[i].child = page.PageChildRef(leftNewID)
			entries[i+1].child = page.PageChildRef(rightNewID)
			entries[i+1].key = rightStart
		}
		i++
	}

	return entries, retired, nil
}

func (z *Zipper) createNewSplitInternal(currentTarget, rootBuilder *node.Builder, splits *[]Split, key []byte, val page.ChildRef, metrics *adaptive.Metrics, scratch *mergeScratch) (*node.Builder, error) {
	// 1. Finish current (if not rootBuilder)
	if currentTarget != rootBuilder {
		currentTarget.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		recordZipperInternalPageWrite(metrics)
	}

	// 2. Alloc new
	sid, err := z.allocator.Alloc(currentTarget.PageID())
	if err != nil {
		return nil, err
	}

	sdata, err := z.pager.GetForWrite(sid)
	if err != nil {
		return nil, err
	}

	sb := z.newBuilderForType(sdata, page.PageTypeInternal, nil)
	sb.SetPageID(sid)
	sb.SetInternalFenceBoundsBorrowed(key, nil)

	*splits = append(*splits, Split{Key: scratch.cloneSplitKey(key), Ref: page.PageChildRef(sid)})

	// Retry insert
	if err := sb.AddInternalChildRef(key, val); err != nil {
		return nil, fmt.Errorf("zipper: internal split retry page=%d key_len=%d ref=%+v: %w", sid, len(key), val, err)
	}
	recordZipperInternalChildRef(metrics, val)

	return sb, nil
}
