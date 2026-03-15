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
	AppendLeafPage(leafPage []byte) (page.ValuePtr, error)
}

type LeafPageReader interface {
	ReadUnsafe(ptr page.ValuePtr) ([]byte, error)
}

type leafPageUnsafeToReader interface {
	ReadUnsafeTo(ptr page.ValuePtr, dst []byte) ([]byte, bool, error)
}

var leafPageScratchPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, page.PageSize)
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

type Zipper struct {
	pager     *pager.Pager
	allocator PageAllocator

	outerLeavesInValueLog bool
	leafPageLog           LeafPageLog
	leafPageReader        LeafPageReader
	leafRefCacheMu        sync.RWMutex
	leafRefCache          map[uint64][]byte

	leafReserveBytes          int
	internalReserveBytes      int
	piggybackCompaction       bool
	leafPrefixCompression     bool
	indexColumnarLeaves       bool
	indexPackedValuePtr       bool
	indexInternalBaseDelta    bool
	adaptiveLeafEncoding      bool
	maintenanceOpsPerCoalesce int

	scratchMu    sync.Mutex
	applyScratch *mergeScratch
}

type Split struct {
	Key    []byte
	NodeID uint64
}

const (
	mergeSplitKeyArenaInitCap = page.PageSize
	mergeSplitKeyArenaKeepCap = 1 << 20
)

type mergeScratch struct {
	mu            sync.Mutex
	splitKeyArena []byte
}

func newMergeScratch() *mergeScratch {
	return &mergeScratch{
		splitKeyArena: make([]byte, 0, mergeSplitKeyArenaInitCap),
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
	if cap(s.splitKeyArena) > mergeSplitKeyArenaKeepCap {
		s.splitKeyArena = make([]byte, 0, mergeSplitKeyArenaInitCap)
		return
	}
	s.splitKeyArena = s.splitKeyArena[:0]
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
	child uint64
}

func isLeafRefPageID(id uint64) bool {
	_, ok := page.DecodeLeafRef(id)
	return ok
}

type childWork struct {
	key       []byte
	low       []byte
	high      []byte
	childID   uint64
	ops       []batch.Entry
	newChild  uint64
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
	s := z.applyScratch
	z.applyScratch = nil
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
	return &Zipper{
		pager:                     z.pager,
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

func (z *Zipper) leafBuilderOptions(ops []batch.Entry) node.BuilderOptions {
	base := node.BuilderOptions{
		LeafPrefixCompression: z != nil && z.leafPrefixCompression,
		LeafColumnar:          z != nil && z.indexColumnarLeaves,
		PackedValuePtr:        z != nil && z.indexPackedValuePtr,
		InternalBaseDelta:     z != nil && z.indexInternalBaseDelta,
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

// Apply applies the batch to the tree rooted at rootID.
// Returns the new root page ID, list of retired pages, and commit metrics.
func (z *Zipper) Apply(rootID uint64, b *batch.Batch) (uint64, []uint64, adaptive.Metrics, error) {
	var metrics adaptive.Metrics
	ops := b.SortedEntries()
	if len(ops) == 0 {
		return rootID, nil, metrics, nil
	}

	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)

	// Underfull merge/rebalance maintenance is only beneficial when the batch
	// includes deletes (can create empty/underfull pages).
	maintenance, deleteCount := z.shouldRunMaintenance(ops)
	var budget *maintenanceBudget
	if maintenance && z.maintenanceOpsPerCoalesce > 0 {
		budget = newMaintenanceBudget(len(ops), deleteCount, z.maintenanceOpsPerCoalesce)
	}

	if z != nil && z.outerLeavesInValueLog && maintenance {
		// Fresh outer-leaf pages only need in-memory ownership during Apply when
		// maintenance may reload newly written leaf refs before the value log is
		// flushed. Pure put/restore applies do not revisit those fresh leaves, so
		// avoid retaining their page buffers for the whole commit.
		z.leafRefCacheMu.Lock()
		z.leafRefCache = make(map[uint64][]byte)
		z.leafRefCacheMu.Unlock()
		defer func() {
			z.leafRefCacheMu.Lock()
			z.leafRefCache = nil
			z.leafRefCacheMu.Unlock()
		}()
	}

	var retired []uint64
	newRoot, splits, err := z.writeRecursive(rootID, ops, maintenance, budget, &metrics, nil, nil, &retired, scratch)
	if err != nil {
		return 0, nil, metrics, err
	}

	if len(splits) > 0 {
		// Root split!
		// The children for the next level are:
		// 1. The new version of the old root (newRoot) with Key=[] (effectively min key)
		// 2. The splits (siblings) generated from it.

		currentLevelNodes := []Split{{Key: []byte{}, NodeID: newRoot}}
		currentLevelNodes = append(currentLevelNodes, splits...)

		// Iteratively build levels up until all nodes fit in one root.
		for {
			// If we only have 1 node left, that is our new root.
			if len(currentLevelNodes) == 1 {
				return currentLevelNodes[0].NodeID, retired, metrics, nil
			}

			var nextLevelNodes []Split

			// Allocate a node for the current batch of children
			var currentBuilder *node.Builder

			// We need to track the "Start Key" of the current builder to promote it.
			var currentStartKey []byte

			for i, child := range currentLevelNodes {
				if currentBuilder == nil {
					// Start new node
					allocHint := newRoot
					if _, ok := page.DecodeLeafRef(allocHint); ok {
						allocHint = 0
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
					currentBuilder.SetInternalFenceBounds(currentStartKey, nil)
				}

				// Add child
				childKey := child.Key
				if childKey == nil {
					childKey = []byte{}
				}
				childSize := 2 + 8 + len(childKey)
				if z.indexInternalBaseDelta {
					childSize = 2 + 4 + len(childKey)
				}
				var err error
				if z.internalSoftFull(currentBuilder, childSize) {
					err = node.ErrNodeFull
				} else {
					err = currentBuilder.AddInternalChild(childKey, child.NodeID)
				}
				if err == node.ErrNodeFull {
					// Finish current
					currentBuilder.FinishNoNode()
					// Promote
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, NodeID: currentBuilder.PageID()})

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
					currentBuilder.SetInternalFenceBounds(currentStartKey, nil)

					if err := currentBuilder.AddInternalChild(childKey, child.NodeID); err != nil {
						return 0, nil, metrics, err // Should fit in empty node
					}
				} else if err != nil {
					return 0, nil, metrics, err
				}

				// If this was the last child, finish
				if i == len(currentLevelNodes)-1 {
					currentBuilder.FinishNoNode()
					nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, NodeID: currentBuilder.PageID()})
					currentBuilder = nil
				}
			}

			// Move up
			currentLevelNodes = nextLevelNodes
		}
	}

	return newRoot, retired, metrics, nil
}

func (z *Zipper) loadNode(id uint64) (node.Node, bool, []byte, bool, error) {
	if z == nil || z.pager == nil {
		return node.Node{}, false, nil, false, errors.New("zipper: missing pager")
	}
	if ptr, ok := page.DecodeLeafRef(id); ok {
		if z.outerLeavesInValueLog {
			z.leafRefCacheMu.RLock()
			data, cached := z.leafRefCache[id]
			z.leafRefCacheMu.RUnlock()
			if cached {
				if len(data) != page.PageSize {
					return node.Node{}, false, nil, false, errors.New("zipper: cached leaf page has invalid size")
				}
				n := node.NewNodeView(data)
				if n.Type() != page.PageTypeLeaf {
					return node.Node{}, false, nil, false, errors.New("zipper: cached leafref resolved to non-leaf page")
				}
				return n, false, nil, false, nil
			}
		}
		if z.leafPageReader == nil {
			return node.Node{}, false, nil, false, errors.New("zipper: missing leaf page reader")
		}
		if r, ok := z.leafPageReader.(leafPageUnsafeToReader); ok {
			scratch := getLeafPageScratch()
			data, usedScratch, err := r.ReadUnsafeTo(ptr, scratch[:0])
			if err != nil {
				putLeafPageScratch(scratch)
				return node.Node{}, false, nil, false, err
			}
			if !usedScratch {
				putLeafPageScratch(scratch)
				scratch = nil
			}
			if len(data) != page.PageSize {
				if scratch != nil {
					putLeafPageScratch(scratch)
				}
				return node.Node{}, false, nil, false, errors.New("zipper: leaf page has invalid size")
			}
			n := node.NewNodeView(data)
			if n.Type() != page.PageTypeLeaf {
				if scratch != nil {
					putLeafPageScratch(scratch)
				}
				return node.Node{}, false, nil, false, errors.New("zipper: leafref resolved to non-leaf page")
			}
			if scratch != nil {
				return n, false, scratch, true, nil
			}
			return n, false, nil, false, nil
		}

		data, err := z.leafPageReader.ReadUnsafe(ptr)
		if err != nil {
			return node.Node{}, false, nil, false, err
		}
		if len(data) != page.PageSize {
			return node.Node{}, false, nil, false, errors.New("zipper: leaf page has invalid size")
		}
		n := node.NewNodeView(data)
		if n.Type() != page.PageTypeLeaf {
			return node.Node{}, false, nil, false, errors.New("zipper: leafref resolved to non-leaf page")
		}
		return n, false, nil, false, nil
	}
	data, err := z.pager.Get(id)
	if err != nil {
		return node.Node{}, false, nil, false, err
	}
	return node.NewNodeView(data), true, nil, false, nil
}

func (z *Zipper) persistLeafPage(b *node.Builder) (uint64, error) {
	if b == nil {
		return 0, errors.New("zipper: nil leaf builder")
	}
	if !z.outerLeavesInValueLog {
		return b.PageID(), nil
	}
	if z.leafPageLog == nil {
		return 0, errors.New("zipper: missing leaf page log")
	}
	ptr, err := z.leafPageLog.AppendLeafPage(b.Data())
	if err != nil {
		return 0, err
	}
	leafID, err := page.EncodeLeafRef(ptr)
	if err != nil {
		return 0, err
	}
	z.leafRefCacheMu.Lock()
	if z.leafRefCache != nil {
		z.leafRefCache[leafID] = b.Data()
	}
	z.leafRefCacheMu.Unlock()
	return leafID, nil
}

// writeRecursive handles the COW merge.
// Returns: newPageID, splits, error.
func (z *Zipper) writeRecursive(pageID uint64, ops []batch.Entry, maintenance bool, budget *maintenanceBudget, metrics *adaptive.Metrics, low, high []byte, retired *[]uint64, scratch *mergeScratch) (uint64, []Split, error) {
	oldNode, oldFromPager, leafScratch, leafScratchRef, err := z.loadNode(pageID)
	if err != nil {
		return 0, nil, err
	}
	if leafScratchRef {
		defer putLeafPageScratch(leafScratch)
	}
	if oldFromPager && retired != nil && pageID != 0 {
		*retired = append(*retired, pageID)
	}

	switch oldNode.Type() {
	case page.PageTypeLeaf, 0:
		if z.outerLeavesInValueLog {
			if z.leafPageLog == nil {
				return 0, nil, errors.New("zipper: outer leaves in value log enabled without leaf page log")
			}
			newData := make([]byte, page.PageSize)
			builder := z.newPooledLeafBuilder(newData, ops)
			defer releasePooledBuilder(builder)
			builder.SetPageID(0)
			return z.mergeLeaf(&oldNode, builder, ops, metrics, scratch)
		}

		// Pager-backed leaf.
		newPageID, err := z.allocator.Alloc(pageID)
		if err != nil {
			return 0, nil, err
		}
		newData, err := z.pager.GetForWrite(newPageID)
		if err != nil {
			return 0, nil, err
		}
		builder := z.newPooledLeafBuilder(newData, ops)
		defer releasePooledBuilder(builder)
		builder.SetPageID(newPageID)
		return z.mergeLeaf(&oldNode, builder, ops, metrics, scratch)

	case page.PageTypeInternal:
		// Internal merge is always pager-backed.
		newPageID, err := z.allocator.Alloc(pageID)
		if err != nil {
			return 0, nil, err
		}
		newData, err := z.pager.GetForWrite(newPageID)
		if err != nil {
			return 0, nil, err
		}
		builder := z.newPooledBuilderForType(newData, page.PageTypeInternal, ops)
		defer releasePooledBuilder(builder)
		builder.SetPageID(newPageID)
		builder.SetInternalFenceBounds(low, high)
		nr, splits, err := z.mergeInternal(&oldNode, builder, ops, maintenance, budget, metrics, retired, low, high, scratch)
		if err != nil {
			return 0, nil, err
		}
		n := builder.Finish()
		metrics.IndexWriteBytes += page.PageSize

		// If this internal page collapsed to a single child and produced no splits,
		// skip writing the redundant level by returning the child directly.
		// This helps delete-heavy workloads shrink tree height without requiring
		// an explicit vacuum.
		if len(splits) == 0 && n.Count() == 1 {
			childID, err := n.GetInternalChildID(0)
			if err == nil {
				if retired != nil {
					*retired = append(*retired, nr)
				}
				return childID, nil, nil
			}
		}
		return nr, splits, nil
	}

	return 0, nil, page.ErrInvalidPageType
}

func (z *Zipper) mergeLeaf(oldNode *node.Node, builder *node.Builder, ops []batch.Entry, metrics *adaptive.Metrics, scratch *mergeScratch) (uint64, []Split, error) {
	oldIdx := uint16(0)
	oldCount := oldNode.Count()
	opIdx := 0

	var (
		splits          []Split
		rootNodeID      uint64
		rootPersisted   bool
		pendingSplitIdx int
	)
	pendingSplitIdx = -1

	// Current target builder
	target := builder
	targetPooled := false
	defer func() {
		if target != builder && targetPooled {
			releasePooledLeafBuilder(target)
		}
	}()

	persistTarget := func() (uint64, error) {
		target.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-target.FreeSpace()) / float64(page.PageSize)
		if target != builder {
			metrics.Splits++
		}

		nodeID, err := z.persistLeafPage(target)
		if err != nil {
			return 0, err
		}

		if target == builder {
			rootNodeID = nodeID
			rootPersisted = true
		} else if pendingSplitIdx >= 0 && pendingSplitIdx < len(splits) {
			splits[pendingSplitIdx].NodeID = nodeID
		}

		if target != builder && targetPooled {
			releasePooledLeafBuilder(target)
			targetPooled = false
		}

		return nodeID, nil
	}

	for {
		// Pick next key: min(oldNode[oldIdx], ops[opIdx])
		var useBatch bool
		var oldLoaded bool
		var oldKey, oldVal []byte
		var oldPtr page.ValuePtr
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
			k, v, ptr, f, err := oldNode.GetLeafEntryView(oldIdx)
			if err != nil {
				return 0, nil, err
			}
			oldLoaded = true
			oldKey = k
			oldVal = v
			oldPtr = ptr
			oldFlags = f
			batchKey := ops[opIdx].Key

			cmp := bytes.Compare(k, batchKey)
			if cmp < 0 {
				// useOld = true
			} else if cmp > 0 {
				useBatch = true
			} else {
				// Equal: Update (Batch wins)
				// The old entry is being overwritten or deleted.
				// If it was a pointer, track it as dead bytes.
				if f&node.FlagPointer != 0 {
					metrics.SlabDeadBytes += int(ptr.Length)
					if metrics.SlabDeadBytesByFile == nil {
						metrics.SlabDeadBytesByFile = make(map[uint32]int64, 4)
					}
					metrics.SlabDeadBytesByFile[ptr.FileID] += int64(ptr.Length)
				}

				useBatch = true
				oldIdx++ // Skip old
			}
		}

		// Key/Val to insert
		var key, val []byte
		var flags byte
		var valPtr page.ValuePtr
		insertedFromBatch := false

		if useBatch {
			op := ops[opIdx]
			opIdx++
			if op.Type == batch.OpDelete {
				continue // Skip insert
			}
			insertedFromBatch = true
			key = op.Key
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
				if oldFlags&node.FlagPointer != 0 {
					valPtr = oldPtr
				} else {
					val = oldVal
				}
			} else {
				// Optimization: View
				k, v, ptr, f, err := oldNode.GetLeafEntryView(oldIdx)
				if err != nil {
					return 0, nil, err
				}
				key = k
				flags = f
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
		}

		// Insert into target builder
		entrySize, prefixLen, suffixLen := target.LeafEntrySizeWithPrefix(key, val, flags)
		var err error
		if z.leafSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else {
			err = target.AddLeafEntryWithPrefix(key, val, flags, valPtr, entrySize, prefixLen, suffixLen)
		}
		if err == node.ErrNodeFull {
			// SPLIT!
			allocHint := uint64(0)
			if !z.outerLeavesInValueLog {
				allocHint = target.PageID()
			}

			// 1. Finish the current leaf.
			if _, err := persistTarget(); err != nil {
				return 0, nil, err
			}

			// 2. Create NEW split node (right sibling).
			var (
				sid    uint64
				sdata  []byte
				splitE Split
			)
			if z.outerLeavesInValueLog {
				sdata = make([]byte, page.PageSize)
				splitE.NodeID = 0
			} else {
				sid, err = z.allocator.Alloc(allocHint)
				if err != nil {
					return 0, nil, err
				}
				sdata, err = z.pager.GetForWrite(sid)
				if err != nil {
					return 0, nil, err
				}
				splitE.NodeID = sid
			}

			// New Builder
			startIdx := opIdx
			if insertedFromBatch && startIdx > 0 {
				startIdx--
			}
			splitBuilder := z.newPooledLeafBuilder(sdata, ops[startIdx:])
			splitBuilder.SetPageID(sid)

			// Record split
			// Use the full first key of the right node as the parent separator.
			// Shortened separators are unsafe for sparse/fence layouts where leaf
			// entries are not a complete key set.
			// Split keys escape this call via the returned []Split, so detach
			// from source buffers into apply-lifetime scratch.
			splitE.Key = scratch.cloneSplitKey(key)
			splits = append(splits, splitE)
			pendingSplitIdx = len(splits) - 1

			target = splitBuilder
			targetPooled = true

			// Retry insert
			entrySize, prefixLen, suffixLen = target.LeafEntrySizeWithPrefix(key, val, flags)
			err = target.AddLeafEntryWithPrefix(key, val, flags, valPtr, entrySize, prefixLen, suffixLen)
			if err != nil {
				return 0, nil, err
			}
		} else if err != nil {
			return 0, nil, err
		}
	}

	if !rootPersisted || target != builder {
		if _, err := persistTarget(); err != nil {
			return 0, nil, err
		}
	}

	return rootNodeID, splits, nil
}

func (z *Zipper) mergeInternal(oldNode *node.Node, builder *node.Builder, ops []batch.Entry, maintenance bool, budget *maintenanceBudget, metrics *adaptive.Metrics, retired *[]uint64, low, high []byte, scratch *mergeScratch) (uint64, []Split, error) {
	count := oldNode.Count()

	var splits []Split

	var err error

	opIdx := 0

	const (
		minParallelChildren = 4
		minParallelOps      = 1024
	)
	useParallel := int(count) >= minParallelChildren && len(ops) >= minParallelOps && runtime.GOMAXPROCS(0) > 1

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

	target := builder
	appendInternal := func(key []byte, childID uint64, first bool) error {
		pageCount := z.pager.PageCount()
		if z.outerLeavesInValueLog {
			if !isLeafRefPageID(childID) && childID >= pageCount {
				return fmt.Errorf("zipper: detected OOB child ID %d (page_count=%d)", childID, pageCount)
			}
		} else if childID >= pageCount {
			return fmt.Errorf("zipper: detected OOB child ID %d (page_count=%d)", childID, pageCount)
		}
		if first && key == nil {
			key = []byte{}
		}
		entrySize := 2 + 8 + len(key)
		if z.indexInternalBaseDelta {
			entrySize = 2 + 4 + len(key)
		}
		if z.internalSoftFull(target, entrySize) {
			err = node.ErrNodeFull
		} else {
			err = target.AddInternalChild(key, childID)
		}
		if err == node.ErrNodeFull {
			target, err = z.createNewSplitInternal(target, builder, &splits, key, childID, metrics, scratch)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}

	// Fast path: most benchmarked writes are non-maintenance and below the
	// parallel threshold. Stream child processing directly and avoid building a
	// large childWork slice.
	if !maintenance && !useParallel {
		firstEntry := true
		var curKey []byte
		var curChild uint64
		if count > 0 {
			curKey, curChild, err = oldNode.GetInternalEntryView(0)
			if err != nil {
				return 0, nil, err
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
				nextChild uint64
			)
			if i+1 < count {
				nextKey, nextChild, err = oldNode.GetInternalEntryView(i + 1)
				if err != nil {
					return 0, nil, err
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

			newChildID := curChild
			var childSplits []Split
			if len(childOps) > 0 {
				newChildID, childSplits, err = z.writeRecursive(curChild, childOps, maintenance, budget, metrics, lowKey, childHigh, retired, scratch)
				if err != nil {
					return 0, nil, err
				}
			}

			if err := appendInternal(lowKey, newChildID, firstEntry); err != nil {
				return 0, nil, err
			}
			firstEntry = false
			for _, s := range childSplits {
				if err := appendInternal(s.Key, s.NodeID, firstEntry); err != nil {
					return 0, nil, err
				}
				firstEntry = false
			}

			curKey = nextKey
			curChild = nextChild
		}

		if target != builder {
			target.FinishNoNode()
			metrics.IndexWriteBytes += page.PageSize
		}
		return builder.PageID(), splits, nil
	}

	children := getChildWorkSlice(int(count))
	defer putChildWorkSlice(children)

	for i := uint16(0); i < count; i++ {
		key, childID, err := oldNode.GetInternalEntryView(i)
		if err != nil {
			return 0, nil, err
		}
		if key == nil {
			key = []byte{}
		}
		keyCopy := cloneKey(key)
		children = append(children, childWork{
			key:     keyCopy,
			low:     keyCopy,
			childID: childID,
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
	}

	// Best-effort: prefetch child pages before we start rewriting them. This can
	// help overlap read-ahead / fault handling with compute, especially in the
	// parallel path.
	if z.pager != nil {
		for i := range children {
			if len(children[i].ops) == 0 {
				continue
			}
			z.pager.PrefetchPage(children[i].childID)
		}
	}

	if useParallel {
		maxParallel := runtime.GOMAXPROCS(0)
		if maxParallel < 1 {
			maxParallel = 1
		}
		for i := range children {
			if len(children[i].ops) == 0 {
				children[i].newChild = children[i].childID
			}
		}
		var nextJob int64 = -1
		var wg sync.WaitGroup
		var firstErr error
		var errOnce sync.Once
		worker := func() {
			defer wg.Done()
			for {
				i := int(atomic.AddInt64(&nextJob, 1))
				if i >= len(children) {
					return
				}
				if len(children[i].ops) == 0 {
					continue
				}
				var childMetrics adaptive.Metrics
				childRet := children[i].retired[:0]
				ncID, cs, err := z.writeRecursive(children[i].childID, children[i].ops, maintenance, budget, &childMetrics, children[i].low, children[i].high, &childRet, scratch)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					continue
				}
				children[i].newChild = ncID
				children[i].splits = cs
				children[i].retired = childRet
				children[i].childStat = childMetrics
			}
		}
		for i := 0; i < maxParallel; i++ {
			wg.Add(1)
			go worker()
		}
		wg.Wait()
		if firstErr != nil {
			return 0, nil, firstErr
		}
		for i := range children {
			if len(children[i].ops) == 0 {
				continue
			}
			mergeMetrics(metrics, &children[i].childStat)
			if retired != nil && len(children[i].retired) > 0 {
				*retired = append(*retired, children[i].retired...)
			}
		}
	} else {
		for i := range children {
			if len(children[i].ops) > 0 {
				ncID, cs, err := z.writeRecursive(children[i].childID, children[i].ops, maintenance, budget, metrics, children[i].low, children[i].high, retired, scratch)
				if err != nil {
					return 0, nil, err
				}
				children[i].newChild = ncID
				children[i].splits = cs
			} else {
				children[i].newChild = children[i].childID
			}
		}
	}

	if !maintenance {
		firstEntry := true
		for i := range children {
			child := &children[i]
			if err := appendInternal(child.key, child.newChild, firstEntry); err != nil {
				return 0, nil, err
			}
			firstEntry = false
			for _, s := range child.splits {
				if err := appendInternal(s.Key, s.NodeID, firstEntry); err != nil {
					return 0, nil, err
				}
				firstEntry = false
			}
		}
		if target != builder {
			target.FinishNoNode()
			metrics.IndexWriteBytes += page.PageSize
		}
		return builder.PageID(), splits, nil
	}

	totalEntries := len(children)
	for i := range children {
		totalEntries += len(children[i].splits)
	}
	entries := getInternalEntrySlice(totalEntries)
	defer func() { putInternalEntrySlice(entries) }()
	pageCount := z.pager.PageCount()
	for i := range children {
		child := children[i]
		if (!z.outerLeavesInValueLog || !isLeafRefPageID(child.newChild)) && child.newChild >= pageCount {
			return 0, nil, fmt.Errorf("zipper: detected OOB child ID %d (page_count=%d)", child.newChild, pageCount)
		}
		entries = append(entries, internalEntry{key: child.key, child: child.newChild})

		// Add sibling splits
		for _, s := range child.splits {
			if (!z.outerLeavesInValueLog || !isLeafRefPageID(s.NodeID)) && s.NodeID >= pageCount {
				return 0, nil, fmt.Errorf("zipper: detected OOB split child ID %d (page_count=%d)", s.NodeID, pageCount)
			}
			entries = append(entries, internalEntry{key: s.Key, child: s.NodeID})
		}
	}

	coalesced := entries
	var extraRetired []uint64
	coalesced, extraRetired, err = z.coalesceLeafChildren(entries, budget, metrics)
	if err != nil {
		return 0, nil, err
	}
	if retired != nil && len(extraRetired) > 0 {
		*retired = append(*retired, extraRetired...)
	}

	coalesced, extraRetired, err = z.coalesceInternalChildren(coalesced, budget, metrics)
	if err != nil {
		return 0, nil, err
	}
	if retired != nil && len(extraRetired) > 0 {
		*retired = append(*retired, extraRetired...)
	}

	// Write final internal entries, splitting if needed.
	for i := range coalesced {
		if err := appendInternal(coalesced[i].key, coalesced[i].child, i == 0); err != nil {
			return 0, nil, err
		}
	}

	// Finalize last split node
	if target != builder {
		target.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
	}

	// builder finalized by caller.
	return builder.PageID(), splits, nil
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

func (z *Zipper) coalesceLeafChildren(entries []internalEntry, budget *maintenanceBudget, metrics *adaptive.Metrics) ([]internalEntry, []uint64, error) {
	if len(entries) < 2 {
		return entries, nil, nil
	}
	if budget != nil && !budget.allow() {
		return entries, nil, nil
	}

	var retired []uint64

	loadLeaf := func(id uint64) (node.Node, bool, bool, []byte, bool, error) {
		n, fromPager, leafScratch, leafScratchRef, err := z.loadNode(id)
		if err != nil {
			if leafScratchRef {
				putLeafPageScratch(leafScratch)
			}
			return node.Node{}, false, false, nil, false, err
		}
		if n.Type() != page.PageTypeLeaf {
			if leafScratchRef {
				putLeafPageScratch(leafScratch)
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
				putLeafPageScratch(leafScratch)
			}
			if fromPager {
				retired = append(retired, e.child)
			}
			continue
		}
		if leafScratchRef {
			putLeafPageScratch(leafScratch)
		}
		out = append(out, e)
	}
	entries = out
	if len(entries) < 2 {
		return entries, retired, nil
	}

	const underfullPPM = 350_000
	pageCap := page.PageSize - node.NodeHeaderSize

	leafEntryBytes := func(key, val []byte, ptr page.ValuePtr, flags byte) int {
		entrySize := 7 + len(key)
		if flags&node.FlagPointer != 0 {
			entrySize += page.ValuePtrSize
		} else {
			entrySize += len(val)
		}
		return entrySize + node.DirectoryEntrySize
	}

	leafRequiredBytes := func(n *node.Node) (int, error) {
		sum := 0
		for i := uint16(0); i < n.Count(); i++ {
			k, v, ptr, flags, err := n.GetLeafEntryView(i)
			if err != nil {
				return 0, err
			}
			if flags&node.FlagTombstone != 0 {
				continue
			}
			sum += leafEntryBytes(k, v, ptr, flags)
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

	buildMergedLeaf := func(left, right *node.Node) (uint64, bool, error) {
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
				return 0, false, err
			}
			data, err = z.pager.GetForWrite(pid)
			if err != nil {
				return 0, false, err
			}
		}
		b := z.newLeafBuilder(data, nil)
		b.SetPageID(pid)

		addAll := func(n *node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, v, ptr, flags, err := n.GetLeafEntryView(i)
				if err != nil {
					return err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				if err := b.AddLeafEntry(k, v, flags, ptr); err != nil {
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
				return 0, false, nil
			}
			return 0, false, err
		}
		if err := addAll(right); err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, pid)
			}
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}

		b.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		metrics.LeafFill += float64(page.PageSize-b.FreeSpace()) / float64(page.PageSize)
		leafID, err := z.persistLeafPage(b)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, pid)
			}
			return 0, false, err
		}
		return leafID, true, nil
	}

	copyLeaf := func(id uint64, hint uint64) (uint64, error) {
		n, _, ok, leafScratch, leafScratchRef, err := loadLeaf(id)
		if err != nil {
			return 0, err
		}
		if !ok {
			if leafScratchRef {
				putLeafPageScratch(leafScratch)
			}
			return 0, errors.New("copyLeaf: not a leaf")
		}
		if leafScratchRef {
			defer putLeafPageScratch(leafScratch)
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
				return 0, err
			}
			data, err = z.pager.GetForWrite(pid)
			if err != nil {
				return 0, err
			}
		}
		b := z.newLeafBuilder(data, nil)
		b.SetPageID(pid)

		for i := uint16(0); i < n.Count(); i++ {
			k, v, ptr, flags, err := n.GetLeafEntryView(i)
			if err != nil {
				return 0, err
			}
			if flags&node.FlagTombstone != 0 {
				continue
			}
			if err := b.AddLeafEntry(k, v, flags, ptr); err != nil {
				return 0, err
			}
		}
		b.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		leafID, err := z.persistLeafPage(b)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, pid)
			}
			return 0, err
		}
		return leafID, nil
	}

	rebalanceLeaves := func(left, right *node.Node) (leftID uint64, rightID uint64, rightStart []byte, ok bool, err error) {
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
				return 0, 0, nil, false, err
			}
			if len(ids) < 2 {
				return 0, 0, nil, false, errors.New("rebalanceLeaves: insufficient pages allocated")
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
		if !z.outerLeavesInValueLog {
			ldata, err = z.pager.GetForWrite(lid)
			if err != nil {
				return 0, 0, nil, false, err
			}
			rdata, err = z.pager.GetForWrite(rid)
			if err != nil {
				retired = append(retired, lid, rid)
				return 0, 0, nil, false, err
			}
		}
		lb := z.newLeafBuilder(ldata, nil)
		lb.SetPageID(lid)

		// Collect combined entries in-order without copying.
		type ev struct {
			k     []byte
			v     []byte
			ptr   page.ValuePtr
			flags byte
			size  int
		}
		combined := make([]ev, 0, int(left.Count()+right.Count()))
		for _, src := range []*node.Node{left, right} {
			for i := uint16(0); i < src.Count(); i++ {
				k, v, ptr, flags, err := src.GetLeafEntryView(i)
				if err != nil {
					if !z.outerLeavesInValueLog {
						retired = append(retired, lid, rid)
					}
					return 0, 0, nil, false, err
				}
				if flags&node.FlagTombstone != 0 {
					continue
				}
				if z.leafPrefixCompression {
					k = append([]byte(nil), k...)
				}
				combined = append(combined, ev{k: k, v: v, ptr: ptr, flags: flags, size: leafEntryBytes(k, v, ptr, flags)})
			}
		}
		if len(combined) < 2 {
			if !z.outerLeavesInValueLog {
				retired = append(retired, lid, rid)
			}
			return 0, 0, nil, false, nil
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
			return 0, 0, nil, false, nil
		}

		rb := z.newLeafBuilder(rdata, nil)
		rb.SetPageID(rid)

		for i := 0; i < bestSplitAt; i++ {
			if err := lb.AddLeafEntry(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr); err != nil {
				if !z.outerLeavesInValueLog {
					retired = append(retired, lid, rid)
				}
				return 0, 0, nil, false, err
			}
		}
		rightStart = append([]byte(nil), combined[bestSplitAt].k...)
		for i := bestSplitAt; i < len(combined); i++ {
			if err := rb.AddLeafEntry(combined[i].k, combined[i].v, combined[i].flags, combined[i].ptr); err != nil {
				if !z.outerLeavesInValueLog {
					retired = append(retired, lid, rid)
				}
				return 0, 0, nil, false, err
			}
		}

		lb.FinishNoNode()
		rb.FinishNoNode()
		metrics.IndexWriteBytes += 2 * page.PageSize
		metrics.LeafFill += float64(page.PageSize-lb.FreeSpace()) / float64(page.PageSize)
		metrics.LeafFill += float64(page.PageSize-rb.FreeSpace()) / float64(page.PageSize)
		leftID, err = z.persistLeafPage(lb)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, lid, rid)
			}
			return 0, 0, nil, false, err
		}
		rightID, err = z.persistLeafPage(rb)
		if err != nil {
			if !z.outerLeavesInValueLog {
				retired = append(retired, lid, rid)
			}
			return 0, 0, nil, false, err
		}
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
				putLeafPageScratch(leftScratch)
			}
			return nil, nil, err
		}
		releaseLeafScratch := func() {
			if leftScratchRef {
				putLeafPageScratch(leftScratch)
				leftScratch = nil
				leftScratchRef = false
			}
			if rightScratchRef {
				putLeafPageScratch(rightScratch)
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
			if z.piggybackCompaction && !z.outerLeavesInValueLog && leftFromPager && rightFromPager {
				const distanceThreshold = 2500 // ~10MB
				dist := int64(leftID) - int64(rightID)
				if dist < 0 {
					dist = -dist
				}

				if dist > distanceThreshold {
					// Move the "older" one (lower ID) towards the newer one.
					if leftID < rightID {
						newID, err := copyLeaf(leftID, rightID)
						if err == nil {
							retired = append(retired, leftID)
							entries[i].child = newID
						}
					} else {
						newID, err := copyLeaf(rightID, leftID)
						if err == nil {
							retired = append(retired, rightID)
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
				if leftFromPager {
					retired = append(retired, leftID)
				}
				if rightFromPager {
					retired = append(retired, rightID)
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
			if leftFromPager {
				retired = append(retired, leftID)
			}
			if rightFromPager {
				retired = append(retired, rightID)
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

	loadInternal := func(id uint64) (*node.Node, bool, error) {
		if z.outerLeavesInValueLog {
			if _, ok := page.DecodeLeafRef(id); ok {
				return nil, false, nil
			}
		}
		pageCount := z.pager.PageCount()
		if id >= pageCount {
			return nil, false, fmt.Errorf("zipper: detected OOB child ID %d (page_count=%d)", id, pageCount)
		}
		data, err := z.pager.Get(id)
		if err != nil {
			return nil, false, err
		}
		n := node.NewNode(data)
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
			k, _, err := n.GetInternalEntryView(i)
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

	buildMergedInternal := func(left, right *node.Node) (uint64, bool, error) {
		pid, err := z.allocator.Alloc(left.PageID())
		if err != nil {
			return 0, false, err
		}
		data, err := z.pager.GetForWrite(pid)
		if err != nil {
			return 0, false, err
		}
		b := z.newBuilderForType(data, page.PageTypeInternal, nil)
		b.SetPageID(pid)

		addAll := func(n *node.Node) error {
			for i := uint16(0); i < n.Count(); i++ {
				k, child, err := n.GetInternalEntryView(i)
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
				if err := b.AddInternalChild(k, child); err != nil {
					return err
				}
			}
			return nil
		}

		if err := addAll(left); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}
		if err := addAll(right); err != nil {
			retired = append(retired, pid)
			if err == node.ErrNodeFull {
				return 0, false, nil
			}
			return 0, false, err
		}

		b.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
		return pid, true, nil
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
				k, child, err := src.GetInternalEntryView(i)
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
				if err := b.AddInternalChild(k, e.child); err != nil {
					return err
				}
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
				retired = append(retired, leftID, rightID)
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
			retired = append(retired, leftID, rightID)
			entries[i].child = leftNewID
			entries[i+1].child = rightNewID
			entries[i+1].key = rightStart
		}
		i++
	}

	return entries, retired, nil
}

func (z *Zipper) createNewSplitInternal(currentTarget, rootBuilder *node.Builder, splits *[]Split, key []byte, val uint64, metrics *adaptive.Metrics, scratch *mergeScratch) (*node.Builder, error) {
	// 1. Finish current (if not rootBuilder)
	if currentTarget != rootBuilder {
		currentTarget.FinishNoNode()
		metrics.IndexWriteBytes += page.PageSize
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
	sb.SetInternalFenceBounds(key, nil)

	*splits = append(*splits, Split{Key: scratch.cloneSplitKey(key), NodeID: sid})

	// Retry insert
	if err := sb.AddInternalChild(key, val); err != nil {
		return nil, err
	}

	return sb, nil
}
