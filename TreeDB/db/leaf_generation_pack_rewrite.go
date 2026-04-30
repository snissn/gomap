package db

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

const leafRefRewriteMapInitCap = 128    // initial map capacity for small leaf-ref rewrite batches
const leafRefRewriteInlineChildCap = 64 // stack-backed child-id scratch for common small internal nodes
const leafRefRewriteInlineRemapCap = 8  // inline remap cache before promoting to map

func (db *DB) cleanupRewriteCreatedSegments(createdSegments []rewriteCreatedSegment) error {
	if len(createdSegments) == 0 {
		return nil
	}
	var errs []error
	for _, seg := range createdSegments {
		if seg.fileID == 0 || seg.path == "" {
			continue
		}
		if db != nil && db.valueLogManager != nil && db.valueLogManager.HasSegment(seg.fileID) {
			if err := db.valueLogManager.RemoveSegmentForce(seg.fileID); err != nil {
				errs = append(errs, fmt.Errorf("remove rewrite-created segment %d: %w", seg.fileID, err))
			}
			continue
		}
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove rewrite-created segment %d: %w", seg.fileID, err))
		}
	}
	return errors.Join(errs...)
}

type leafRefRewriteCtx struct {
	ctx context.Context
	db  *DB

	pager       *pager.Pager
	leafReader  tree.SlabReader
	leafToer    unsafeToReader
	leafScratch []byte
	alloc       interface {
		Alloc(hint uint64) (uint64, error)
	}

	writer   *rewriteWriter
	ridAlloc *rewriteRIDAllocator
	zipper   *zipper.Zipper

	sourceIDs            map[uint32]struct{}
	sourceChunks         map[valueLogChunkKey]ValueLogRewritePlanChunk
	sourceChunkBytes     int64
	singleSourceID       uint32
	hasSingleID          bool
	sourceGenerationIDs  map[uint64]struct{}
	useSubtreeStatsPrune bool

	leafPtrMap         map[page.LeafLogPtr]page.ChildRef
	leafMap            map[uint64]uint64 // legacy inline cache kept until the typed rewrite code is fully simplified.
	leafRemapInline    [leafRefRewriteInlineRemapCap]leafRefRewriteRemap
	leafRemapInlineLen int

	internalMap            map[uint64]uint64 // old internal page id -> new page id
	internalRemapInline    [leafRefRewriteInlineRemapCap]leafRefRewriteRemap
	internalRemapInlineLen int

	retired         []uint64
	copied          int
	copiedBytes     int64
	internalVisited int
	subtreesPruned  int
	maxCopiedBytes  int64
	leafFrameK      int
	leafFrames      int
	maxLeafFrameK   int

	readRefreshRetried bool
}

type leafRefRewritePageAppender struct {
	ctx *leafRefRewriteCtx
}

func (a *leafRefRewritePageAppender) AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if a == nil || a.ctx == nil {
		return page.LeafLogPtr{}, errors.New("vlog-rewrite: missing leaf-ref rewrite appender state")
	}
	return a.ctx.appendLeafPage(leafPage)
}

type leafRefRewriteRunStats struct {
	InternalPagesVisited int
	SubtreesPruned       int
	LeafFramesWritten    int
	MaxLeafFrameK        int
}

type leafRefRewriteRemap struct {
	oldID uint64
	newID uint64
}

func (c *leafRefRewriteCtx) readLeafPage(ptr page.ValuePtr) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("vlog-rewrite: value-log snapshot reader unavailable")
	}
	if c.leafReader == nil && c.leafToer == nil && (c.db == nil || c.db.valueLogManager == nil) {
		return nil, fmt.Errorf("vlog-rewrite: value-log snapshot reader unavailable")
	}
	if c.db != nil && c.db.valueLogManager != nil {
		if cap(c.leafScratch) < page.PageSize {
			c.leafScratch = make([]byte, 0, page.PageSize)
		} else {
			c.leafScratch = c.leafScratch[:0]
		}
		leafPage, usedScratch, err := c.db.valueLogManager.ReadUnsafeTo(ptr, c.leafScratch[:0])
		if err != nil && errors.Is(err, valuelog.ErrFileNotFound) && !c.readRefreshRetried {
			if refreshErr := c.db.RefreshValueLogSet(); refreshErr != nil {
				return nil, refreshErr
			}
			c.readRefreshRetried = true
			leafPage, usedScratch, err = c.db.valueLogManager.ReadUnsafeTo(ptr, c.leafScratch[:0])
		}
		if err != nil {
			return nil, err
		}
		if usedScratch {
			c.leafScratch = leafPage[:0]
		}
		return leafPage, nil
	}
	if c.leafToer != nil {
		if cap(c.leafScratch) < page.PageSize {
			c.leafScratch = make([]byte, 0, page.PageSize)
		} else {
			c.leafScratch = c.leafScratch[:0]
		}
		leafPage, usedScratch, err := c.leafToer.ReadUnsafeTo(ptr, c.leafScratch[:0])
		if err != nil {
			return nil, err
		}
		if usedScratch {
			// Keep the caller-provided decode buffer hot across leafref rewrites.
			c.leafScratch = leafPage[:0]
		}
		return leafPage, nil
	}
	return c.leafReader.ReadUnsafe(ptr)
}

func (c *leafRefRewriteCtx) lookupLeafRemap(id uint64) (uint64, bool) {
	if c.leafMap != nil {
		mapped, ok := c.leafMap[id]
		return mapped, ok
	}
	for i := 0; i < c.leafRemapInlineLen; i++ {
		pair := c.leafRemapInline[i]
		if pair.oldID == id {
			return pair.newID, true
		}
	}
	return 0, false
}

func (c *leafRefRewriteCtx) storeLeafRemap(oldID, newID uint64) {
	if c.leafMap != nil {
		c.leafMap[oldID] = newID
		return
	}
	if c.leafRemapInlineLen < len(c.leafRemapInline) {
		c.leafRemapInline[c.leafRemapInlineLen] = leafRefRewriteRemap{oldID: oldID, newID: newID}
		c.leafRemapInlineLen++
		return
	}
	c.leafMap = make(map[uint64]uint64, leafRefRewriteMapInitCap)
	for i := 0; i < c.leafRemapInlineLen; i++ {
		pair := c.leafRemapInline[i]
		c.leafMap[pair.oldID] = pair.newID
	}
	c.leafMap[oldID] = newID
}

func (c *leafRefRewriteCtx) lookupInternalRemap(id uint64) (uint64, bool) {
	if c.internalMap != nil {
		mapped, ok := c.internalMap[id]
		return mapped, ok
	}
	for i := 0; i < c.internalRemapInlineLen; i++ {
		pair := c.internalRemapInline[i]
		if pair.oldID == id {
			return pair.newID, true
		}
	}
	return 0, false
}

func (c *leafRefRewriteCtx) storeInternalRemap(oldID, newID uint64) {
	if c.internalMap != nil {
		c.internalMap[oldID] = newID
		return
	}
	if c.internalRemapInlineLen < len(c.internalRemapInline) {
		c.internalRemapInline[c.internalRemapInlineLen] = leafRefRewriteRemap{oldID: oldID, newID: newID}
		c.internalRemapInlineLen++
		return
	}
	c.internalMap = make(map[uint64]uint64, leafRefRewriteMapInitCap)
	for i := 0; i < c.internalRemapInlineLen; i++ {
		pair := c.internalRemapInline[i]
		c.internalMap[pair.oldID] = pair.newID
	}
	c.internalMap[oldID] = newID
}

func (c *leafRefRewriteCtx) lookupLeafPtrRemap(ptr page.LeafLogPtr) (page.ChildRef, bool) {
	if c == nil || c.leafPtrMap == nil {
		return page.ChildRef{}, false
	}
	ref, ok := c.leafPtrMap[ptr]
	return ref, ok
}

func (c *leafRefRewriteCtx) storeLeafPtrRemap(old page.LeafLogPtr, next page.ChildRef) {
	if c == nil {
		return
	}
	if c.leafPtrMap == nil {
		c.leafPtrMap = make(map[page.LeafLogPtr]page.ChildRef, leafRefRewriteMapInitCap)
	}
	c.leafPtrMap[old] = next
}

func (c *leafRefRewriteCtx) shouldRewriteLeafRef(ptr page.LeafLogPtr) (bool, error) {
	if c == nil {
		return false, errors.New("vlog-rewrite: nil leaf-ref rewrite ctx")
	}
	if c.hasSingleID {
		if ptr.ValueLogFileID() != c.singleSourceID {
			return false, nil
		}
	} else if c.sourceIDs != nil {
		if _, ok := c.sourceIDs[ptr.ValueLogFileID()]; !ok {
			return false, nil
		}
	}
	if c.sourceChunks != nil {
		ok, err := rewriteSourceChunkSelected(ptr.ValuePtr(), c.sourceChunks, c.sourceChunkBytes)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (c *leafRefRewriteCtx) subtreeMayContainRewriteSource(pageID uint64) bool {
	if c == nil || !c.useSubtreeStatsPrune || c.db == nil || len(c.sourceGenerationIDs) == 0 {
		return true
	}
	stats, ok := c.db.loadLeafGenerationSubtreeStats(pageID)
	if !ok {
		return true
	}
	if len(c.sourceGenerationIDs) <= len(stats) {
		for generationID := range c.sourceGenerationIDs {
			totals := stats[generationID]
			if totals.LivePages > 0 || totals.LiveBytes > 0 {
				return true
			}
		}
		return false
	}
	for generationID, totals := range stats {
		if _, selected := c.sourceGenerationIDs[generationID]; selected && (totals.LivePages > 0 || totals.LiveBytes > 0) {
			return true
		}
	}
	return false
}

func (c *leafRefRewriteCtx) appendLeafPage(leafPage []byte) (page.LeafLogPtr, error) {
	if c == nil || c.writer == nil || c.ridAlloc == nil {
		return page.LeafLogPtr{}, errors.New("vlog-rewrite: missing leaf-ref rewrite appender state")
	}
	if len(leafPage) != page.PageSize {
		return page.LeafLogPtr{}, fmt.Errorf("vlog-rewrite: leaf page has invalid size: got=%dB want=%dB", len(leafPage), page.PageSize)
	}
	if c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			return page.LeafLogPtr{}, err
		}
	}
	if c.maxCopiedBytes > 0 && c.copiedBytes >= c.maxCopiedBytes && c.copied > 0 {
		return page.LeafLogPtr{}, fmt.Errorf("vlog-rewrite: leaf-ref copy budget exhausted: copied=%dB max=%dB", c.copiedBytes, c.maxCopiedBytes)
	}
	rid, err := c.ridAlloc.Next()
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	ptr, err := c.writer.appendLeafPageWithRID(rid, leafPage)
	if err != nil {
		return page.LeafLogPtr{}, err
	}
	c.copied++
	c.copiedBytes += int64(len(leafPage))
	c.leafFrames++
	if c.maxLeafFrameK < 1 {
		c.maxLeafFrameK = 1
	}
	return ptr, nil
}

func (c *leafRefRewriteCtx) appendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error) {
	if c == nil || c.writer == nil || c.ridAlloc == nil {
		return nil, errors.New("vlog-rewrite: missing leaf-ref rewrite appender state")
	}
	if len(leafPages) == 0 {
		return nil, nil
	}
	if c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			return nil, err
		}
	}
	if c.maxCopiedBytes > 0 && c.copiedBytes >= c.maxCopiedBytes && c.copied > 0 {
		return nil, fmt.Errorf("vlog-rewrite: leaf-ref copy budget exhausted: copied=%dB max=%dB", c.copiedBytes, c.maxCopiedBytes)
	}
	for i, leafPage := range leafPages {
		if len(leafPage) != page.PageSize {
			return nil, fmt.Errorf("vlog-rewrite: leaf page %d has invalid size: got=%dB want=%dB", i, len(leafPage), page.PageSize)
		}
	}
	startRID, err := c.ridAlloc.Reserve(len(leafPages))
	if err != nil {
		return nil, err
	}
	ptrs, err := c.writer.appendLeafPagesWithRIDStart(startRID, leafPages)
	if err != nil {
		return nil, err
	}
	if len(ptrs) != len(leafPages) {
		return nil, fmt.Errorf("vlog-rewrite: leaf batch pointer count mismatch got=%d want=%d", len(ptrs), len(leafPages))
	}
	c.copied += len(leafPages)
	c.copiedBytes += int64(len(leafPages) * page.PageSize)
	c.leafFrames++
	if len(leafPages) > c.maxLeafFrameK {
		c.maxLeafFrameK = len(leafPages)
	}
	return ptrs, nil
}

func (c *leafRefRewriteCtx) rewriteLeafRef(ptr page.LeafLogPtr) (page.ChildRef, bool, error) {
	if mapped, ok := c.lookupLeafPtrRemap(ptr); ok {
		return mapped, true, nil
	}
	shouldRewrite, err := c.shouldRewriteLeafRef(ptr)
	if err != nil {
		return page.ChildRef{}, false, err
	}
	if !shouldRewrite {
		return page.LeafLogChildRef(ptr), false, nil
	}
	if c.writer == nil || c.ridAlloc == nil {
		return page.ChildRef{}, false, fmt.Errorf("vlog-rewrite: rewrite writer unavailable")
	}
	leafPage, err := c.readLeafPage(ptr.ValuePtr())
	if err != nil {
		return page.ChildRef{}, false, err
	}
	leafLogPtr, err := c.appendLeafPage(leafPage)
	if err != nil {
		return page.ChildRef{}, false, err
	}
	next := page.LeafLogChildRef(leafLogPtr)
	c.storeLeafPtrRemap(ptr, next)
	return next, true, nil
}

func (c *leafRefRewriteCtx) rewriteLeafRefBatch(ptrs []page.LeafLogPtr) ([]page.ChildRef, error) {
	if len(ptrs) == 0 {
		return nil, nil
	}
	out := make([]page.ChildRef, len(ptrs))
	for start := 0; start < len(ptrs); {
		k := c.leafFrameK
		if k <= 0 {
			k = 1
		}
		if k > valuelog.MaxFrameK {
			k = valuelog.MaxFrameK
		}
		end := start + k
		if end > len(ptrs) {
			end = len(ptrs)
		}
		pages := make([][]byte, end-start)
		for i := start; i < end; i++ {
			leafPage, err := c.readLeafPage(ptrs[i].ValuePtr())
			if err != nil {
				return nil, err
			}
			pages[i-start] = append([]byte(nil), leafPage...)
		}
		newPtrs, err := c.appendLeafPages(pages)
		if err != nil {
			return nil, err
		}
		for i, newPtr := range newPtrs {
			ref := page.LeafLogChildRef(newPtr)
			oldPtr := ptrs[start+i]
			c.storeLeafPtrRemap(oldPtr, ref)
			out[start+i] = ref
		}
		start = end
	}
	return out, nil
}

func (c *leafRefRewriteCtx) rewriteNode(id uint64) (uint64, bool, error) {

	if c == nil {
		return id, false, errors.New("vlog-rewrite: nil leaf-ref rewrite ctx")
	}
	if c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			return id, false, err
		}
	}
	if c.maxCopiedBytes > 0 && c.copiedBytes >= c.maxCopiedBytes && c.copied > 0 {
		return id, false, nil
	}
	if id == 0 {
		return 0, false, nil
	}

	if mapped, ok := c.lookupInternalRemap(id); ok {
		return mapped, mapped != id, nil
	}

	if c.pager == nil {
		return id, false, errors.New("vlog-rewrite: missing pager")
	}
	if !c.subtreeMayContainRewriteSource(id) {
		c.subtreesPruned++
		return id, false, nil
	}
	data, err := c.pager.Get(id)
	if err != nil {
		return id, false, err
	}
	n := node.NewNodeView(data)
	if c.pager.VerifyOnRead() || !c.pager.IsVerified(id) {
		if !n.VerifyChecksum() {
			return id, false, fmt.Errorf("checksum mismatch on page %d", id)
		}
		if !c.pager.VerifyOnRead() {
			c.pager.MarkVerified(id)
		}
	}
	switch n.Type() {
	case page.PageTypeInternal:
		c.internalVisited++
		count := n.Count()
		if count == 0 {
			return id, false, nil
		}
		var childRefs []page.ChildRef
		var childRefsInline [leafRefRewriteInlineChildCap]page.ChildRef
		if int(count) <= len(childRefsInline) {
			childRefs = childRefsInline[:int(count)]
		} else {
			childRefs = make([]page.ChildRef, int(count))
		}
		changed := false
		type pendingLeafRewrite struct {
			index int
			ptr   page.LeafLogPtr
		}
		var pending []pendingLeafRewrite
		var pendingInline [leafRefRewriteInlineChildCap]pendingLeafRewrite
		pending = pendingInline[:0]
		flushPending := func() error {
			if len(pending) == 0 {
				return nil
			}
			ptrs := make([]page.LeafLogPtr, len(pending))
			for i := range pending {
				ptrs[i] = pending[i].ptr
			}
			refs, err := c.rewriteLeafRefBatch(ptrs)
			if err != nil {
				return err
			}
			for i, ref := range refs {
				childRefs[pending[i].index] = ref
			}
			changed = true
			pending = pending[:0]
			return nil
		}
		for i := uint16(0); i < count; i++ {
			_, childRef, err := n.GetInternalEntryRefView(i)
			if err != nil {
				return id, false, err
			}
			childRefs[int(i)] = childRef
			if childRef.Kind == page.ChildRefLeafLog {
				if mapped, ok := c.lookupLeafPtrRemap(childRef.Log); ok {
					if err := flushPending(); err != nil {
						return id, false, err
					}
					childRefs[int(i)] = mapped
					changed = true
					continue
				}
				shouldRewrite, err := c.shouldRewriteLeafRef(childRef.Log)
				if err != nil {
					return id, false, err
				}
				if !shouldRewrite {
					if err := flushPending(); err != nil {
						return id, false, err
					}
					continue
				}
				pending = append(pending, pendingLeafRewrite{index: int(i), ptr: childRef.Log})
				k := c.leafFrameK
				if k <= 0 {
					k = 1
				}
				if k > valuelog.MaxFrameK {
					k = valuelog.MaxFrameK
				}
				if len(pending) >= k {
					if err := flushPending(); err != nil {
						return id, false, err
					}
				}
			} else {
				if err := flushPending(); err != nil {
					return id, false, err
				}
				nextID, childChanged, err := c.rewriteNode(childRef.Page)
				if err != nil {
					return id, false, err
				}
				if childChanged {
					childRefs[int(i)] = page.PageChildRef(nextID)
					changed = true
				}
			}
		}
		if err := flushPending(); err != nil {
			return id, false, err
		}
		if !changed {
			return id, false, nil
		}
		if c.alloc == nil {
			return id, false, errors.New("vlog-rewrite: missing allocator")
		}
		newID, err := c.alloc.Alloc(id)
		if err != nil {
			return id, false, err
		}
		buf, err := c.pager.GetForWrite(newID)
		if err != nil {
			return id, false, err
		}
		b := node.NewBuilderWithOptions(buf, page.PageTypeInternal, node.BuilderOptions{
			InternalBaseDelta: n.InternalBaseDeltaEnabled(),
		})
		b.SetPageID(newID)
		if low, high, ok, err := n.InternalFenceBounds(); err != nil {
			return id, false, err
		} else if ok {
			b.SetInternalFenceBounds(low, high)
		}
		for i := uint16(0); i < count; i++ {
			keyView, _, err := n.GetInternalEntryRefView(i)
			if err != nil {
				return id, false, err
			}
			if err := b.AddInternalChildRef(keyView, childRefs[int(i)]); err != nil {
				return id, false, err
			}
		}
		b.FinishNoNode()
		if id != 0 {
			c.retired = append(c.retired, id)
		}
		c.storeInternalRemap(id, newID)
		return newID, true, nil

	case page.PageTypeLeaf:
		// Pager-backed leaf pages are not expected in outer-leaves-in-vlog mode.
		// Keep them intact.
		return id, false, nil

	default:
		return id, false, fmt.Errorf("vlog-rewrite: unexpected page type %d at page %d", n.Type(), id)
	}
}

func (c *leafRefRewriteCtx) applySystemRootCollectionRootReplacements(rootID uint64, replacements []vacuumCollectionRootReplacement) (uint64, bool, error) {
	if c == nil {
		return rootID, false, errors.New("vlog-rewrite: nil leafref rewrite ctx")
	}
	if len(replacements) == 0 {
		return rootID, false, nil
	}
	if c.db == nil {
		return rootID, false, errors.New("vlog-rewrite: missing db")
	}
	if c.zipper == nil {
		return rootID, false, errors.New("vlog-rewrite: missing system root zipper")
	}
	if rootID == 0 {
		return 0, false, nil
	}
	if c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			return rootID, false, err
		}
	}

	inlineThreshold := c.db.InlineThreshold()
	for _, replacement := range replacements {
		if len(replacement.value) > inlineThreshold {
			inlineThreshold = len(replacement.value)
		}
	}
	delta := batch.Acquire(c.db.valueLogManager, inlineThreshold)
	defer batch.Release(delta)
	delta.Reserve(len(replacements))
	for _, replacement := range replacements {
		if c.ctx != nil {
			if err := c.ctx.Err(); err != nil {
				return rootID, false, err
			}
		}
		if err := delta.Set(replacement.key, replacement.value); err != nil {
			return rootID, false, err
		}
	}
	if c.ctx != nil {
		if err := c.ctx.Err(); err != nil {
			return rootID, false, err
		}
	}
	// For this collections-only maintenance path, cancellation during Apply is
	// observed by the leaf page appender before each rewritten outer leaf is
	// persisted.
	newRoot, retired, _, err := c.zipper.Apply(rootID, delta)
	if err != nil {
		return rootID, false, err
	}
	c.retired = append(c.retired, retired...)
	return newRoot, newRoot != rootID || len(retired) > 0, nil
}

func (db *DB) rewriteLeafRefsOnline(ctx context.Context, writer *rewriteWriter, ridAlloc *rewriteRIDAllocator, sourceIDs map[uint32]struct{}, sourceChunks map[valueLogChunkKey]ValueLogRewritePlanChunk, sourceChunkBytes int64, singleSourceID uint32, hasSingleSourceID bool, maxCopiedBytes int64, sync bool, leafFrameK int, runStats *leafRefRewriteRunStats) (copied int, copiedBytes int64, err error) {
	if db == nil {
		return 0, 0, fmt.Errorf("missing db")
	}
	if !db.indexOuterLeavesInValueLog {
		return 0, 0, nil
	}
	if db.readOnly {
		return 0, 0, ErrReadOnly
	}
	if db.valueLogManager == nil {
		return 0, 0, fmt.Errorf("value log manager unavailable")
	}
	if writer == nil || ridAlloc == nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: missing writer/rid state")
	}
	// Treat nil sourceIDs (with no single-source constraint) as "all sources"
	// and an empty, non-nil map as "no sources".
	if !hasSingleSourceID && sourceIDs != nil && len(sourceIDs) == 0 {
		return 0, 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		closeRewriteSnapshot(&err, snap)
		return 0, 0, fmt.Errorf("missing snapshot state")
	}
	defer closeRewriteSnapshot(&err, snap)

	idx := snap.idx
	rootID := snap.state.RootPageID
	sysRoot := snap.state.SystemRootPageID

	tracker := newAllocTracker(idx.allocator)
	defer func() {
		if tracker == nil {
			return
		}
		freeErr := tracker.FreeAll()
		if freeErr == nil {
			return
		}
		if err != nil {
			err = errors.Join(err, freeErr)
			return
		}
		err = freeErr
	}()

	var sourceGenerationIDs map[uint64]struct{}
	if snap.state.LeafGenerations != nil && sourceChunks == nil {
		for rawFileID, generationID := range snap.state.LeafGenerations.FileToGeneration {
			valueFileID := page.ValueLogFileID(rawFileID)
			if hasSingleSourceID {
				if valueFileID != singleSourceID {
					continue
				}
			} else if sourceIDs != nil {
				if _, ok := sourceIDs[valueFileID]; !ok {
					continue
				}
			}
			if sourceGenerationIDs == nil {
				sourceGenerationIDs = make(map[uint64]struct{})
			}
			sourceGenerationIDs[generationID] = struct{}{}
		}
	}

	leafCtx := &leafRefRewriteCtx{
		ctx:                  ctx,
		db:                   db,
		pager:                idx.pager,
		leafReader:           &snap.reader,
		alloc:                tracker,
		writer:               writer,
		ridAlloc:             ridAlloc,
		sourceIDs:            sourceIDs,
		sourceChunks:         sourceChunks,
		sourceChunkBytes:     normalizeValueLogRewriteChunkBytes(sourceChunkBytes),
		singleSourceID:       singleSourceID,
		hasSingleID:          hasSingleSourceID,
		sourceGenerationIDs:  sourceGenerationIDs,
		useSubtreeStatsPrune: len(sourceGenerationIDs) > 0,
		maxCopiedBytes:       maxCopiedBytes,
		leafFrameK:           leafFrameK,
	}
	leafCtx.zipper = idx.zipper.CloneWithAllocator(tracker)
	leafCtx.zipper.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
	leafCtx.zipper.SetIndexInternalBaseDelta(db.indexInternalBaseDelta && !db.indexOuterLeavesInValueLog)
	if db.indexOuterLeavesInValueLog {
		leafCtx.zipper.SetLeafPageLog(&leafRefRewritePageAppender{ctx: leafCtx})
	}
	if runStats != nil {
		defer func() {
			runStats.InternalPagesVisited = leafCtx.internalVisited
			runStats.SubtreesPruned = leafCtx.subtreesPruned
			runStats.LeafFramesWritten = leafCtx.leafFrames
			runStats.MaxLeafFrameK = leafCtx.maxLeafFrameK
		}()
	}
	if toer, ok := leafCtx.leafReader.(unsafeToReader); ok {
		leafCtx.leafToer = toer
		leafCtx.leafScratch = make([]byte, 0, page.PageSize)
	}

	descriptors, err := vacuumCollectCollectionRootDescriptorsWithContext(ctx, idx.pager, &snap.reader, sysRoot)
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: collect collection root descriptors: %w", err)
	}
	collectionRootReplacements, err := vacuumRewriteCollectionRootDescriptors(descriptors, func(descriptor vacuumCollectionRootDescriptor) (uint64, error) {
		newRoot, _, err := leafCtx.rewriteNode(descriptor.rootID)
		return newRoot, err
	}, "vlog-rewrite: rewrite collection leaf root")
	if err != nil {
		return 0, 0, err
	}

	var (
		newSysRoot uint64
		sysChanged bool
	)
	newSysRoot, sysChanged, err = leafCtx.rewriteNode(sysRoot)
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: rewrite system leaf root: %w", err)
	}
	if len(collectionRootReplacements) > 0 {
		if sysChanged {
			if err := writer.Flush(); err != nil {
				return 0, 0, fmt.Errorf("vlog-rewrite: flush rewritten system leaf root: %w", err)
			}
		}
		replacedSysRoot, replacementChanged, err := leafCtx.applySystemRootCollectionRootReplacements(newSysRoot, collectionRootReplacements)
		if err != nil {
			return 0, 0, fmt.Errorf("vlog-rewrite: rewrite system collection descriptors: %w", err)
		}
		newSysRoot = replacedSysRoot
		sysChanged = sysChanged || replacementChanged
	}
	newRoot, userChanged, err := leafCtx.rewriteNode(rootID)
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: rewrite user leaf root: %w", err)
	}
	if !sysChanged && !userChanged {
		return 0, 0, nil
	}

	// Ensure the copied leaf-page records are visible before publishing new leaf
	// refs that point at them.
	if sync {
		if err := writer.Sync(); err != nil {
			return 0, 0, fmt.Errorf("vlog-rewrite: sync rewritten leaf refs: %w", err)
		}
	} else {
		if err := writer.Flush(); err != nil {
			return 0, 0, fmt.Errorf("vlog-rewrite: flush rewritten leaf refs: %w", err)
		}
	}
	createdIDs, err := writer.createdFileIDs()
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: list created leaf ref files: %w", err)
	}
	createdSegments, err := writer.createdSegmentsSnapshot()
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: snapshot created leaf ref segments: %w", err)
	}
	cleanupCreatedSegments := func(baseErr error) (int, int64, error) {
		closeErr := writer.Close()
		cleanupErr := db.cleanupRewriteCreatedSegments(createdSegments)
		if closeErr != nil || cleanupErr != nil {
			baseErr = errors.Join(baseErr, closeErr, cleanupErr)
		}
		return 0, 0, baseErr
	}
	if len(createdSegments) > 0 {
		// Register rewrite-created segments before commit publication so
		// finalizeCommit can publish CurrentSetNoRefresh without forcing a
		// filesystem rescan in leafref-heavy rewrite paths.
		for _, seg := range createdSegments {
			if err := db.valueLogManager.RegisterSegment(seg.path, seg.fileID); err != nil {
				return cleanupCreatedSegments(fmt.Errorf("vlog-rewrite: register rewritten leaf segment %d: %w", seg.fileID, err))
			}
		}
	}
	var leafManifest *leafGenerationManifest
	var leafManifestRawFileIDs []uint32
	if db.leafGenerationManifest != nil {
		stagedManifest := db.leafGenerationManifest.clone()
		rawFileIDs, changed, err := noteCreatedLeafGenerationFileIDsInManifest(stagedManifest, snap.state.CommitSeq+1, createdIDs)
		if err != nil {
			return 0, 0, fmt.Errorf("vlog-rewrite: update leaf generation manifest: %w", err)
		}
		if changed {
			leafManifest = stagedManifest
			leafManifestRawFileIDs = rawFileIDs
		}
	}

	if err := db.finalizeCommit(newRoot, newSysRoot, leafCtx.retired, sync, adaptive.Metrics{}, createdIDs, false, nil, leafManifest, leafManifestRawFileIDs); err != nil {
		if finalizeCommitErrorAllowsCreatedSegmentCleanup(err) {
			return cleanupCreatedSegments(fmt.Errorf("vlog-rewrite: finalize rewritten leaf refs: %w", err))
		}
		return 0, 0, fmt.Errorf("vlog-rewrite: finalize rewritten leaf refs: %w", err)
	}
	db.invalidateLeafGenerationSubtreeStats(tracker.Pages())
	tracker = nil
	return leafCtx.copied, leafCtx.copiedBytes, nil
}
