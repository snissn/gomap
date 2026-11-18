package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
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

// Private leaf-pack index pages live in a separate pager and use a namespace
// that can never alias an ordinary committed page ID.
const leafGenerationPackPrivatePageIDBase = uint64(1) << 63

type leafGenerationPackStagingAllocator struct {
	pager *pager.Pager
	pages []uint64
}

func (a *leafGenerationPackStagingAllocator) Alloc(uint64) (uint64, error) {
	if a == nil || a.pager == nil {
		return 0, errors.New("leaf generation pack: missing staging pager")
	}
	id, err := a.pager.Alloc(1)
	if err != nil {
		return 0, err
	}
	if id < leafGenerationPackPrivatePageIDBase {
		return 0, errors.New("leaf generation pack: staging allocator returned committed-namespace id")
	}
	a.pages = append(a.pages, id)
	return id, nil
}

func (a *leafGenerationPackStagingAllocator) Pages() []uint64 {
	if a == nil {
		return nil
	}
	return append([]uint64(nil), a.pages...)
}

// leafGenerationPackPublishAllocator appends pages through the generation-COW
// allocator after exact revalidation while writeMu is held. Failed publication
// retires the unpublished pages rather than shrinking the pager behind the
// allocator's logical high-water.
type leafGenerationPackPublishAllocator struct {
	primary         *primaryarena.Arena
	zipper          *zipper.Zipper
	primaryRoots    []uint64
	pager           *pager.Pager
	allocator       *freelist.Allocator
	retireCommitSeq uint64
	pages           []uint64
}

func newLeafGenerationPackPublishAllocator(idx *indexGen, retireCommitSeq uint64) *leafGenerationPackPublishAllocator {
	if idx == nil {
		return &leafGenerationPackPublishAllocator{retireCommitSeq: retireCommitSeq}
	}
	return &leafGenerationPackPublishAllocator{
		pager: idx.pager, allocator: idx.allocator, retireCommitSeq: retireCommitSeq, primary: idx.primary, zipper: idx.zipper,
	}
}

func (a *leafGenerationPackPublishAllocator) Alloc(uint64) (uint64, error) {
	if a == nil || a.pager == nil || a.allocator == nil {
		return 0, errors.New("leaf generation pack: missing publish allocator")
	}
	id, err := a.allocator.AllocAppend()
	if err != nil {
		return 0, err
	}
	if id >= leafGenerationPackPrivatePageIDBase {
		return 0, errors.New("leaf generation pack: committed page id namespace exhausted")
	}
	a.pages = append(a.pages, id)
	return id, nil
}

func (a *leafGenerationPackPublishAllocator) Pages() []uint64 {
	if a == nil {
		return nil
	}
	return append([]uint64(nil), a.pages...)
}

func (a *leafGenerationPackPublishAllocator) Rollback() error {
	if a == nil {
		return nil
	}
	var bankErr error
	for _, id := range a.primaryRoots {
		bankErr = errors.Join(bankErr, a.primary.ReleaseUntakenOrdinaryBundle(id))
	}
	a.primaryRoots = nil
	if bankErr != nil {
		return bankErr
	}
	if len(a.pages) == 0 {
		return nil
	}
	if a.allocator == nil || a.retireCommitSeq == 0 {
		return errors.New("leaf generation pack: cannot retire unpublished pages")
	}
	if err := a.allocator.RetireCOWV1(a.pages, a.retireCommitSeq); err != nil {
		return fmt.Errorf("leaf generation pack: retire unpublished pages: %w", err)
	}
	a.pages = nil
	return nil
}

func (db *DB) cleanupRewriteCreatedSegments(createdSegments []rewriteCreatedSegment) error {
	if len(createdSegments) == 0 {
		return nil
	}
	var errs []error
	for _, seg := range createdSegments {
		if seg.fileID == 0 || seg.path == "" {
			continue
		}
		if seg.identity != (rootpublication.StableIdentity{}) {
			file, err := os.Open(seg.path)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				errs = append(errs, err)
				continue
			}
			identity, identityErr := rootpublication.StableIdentityFromFile(file)
			closeErr := file.Close()
			if identityErr != nil || closeErr != nil {
				errs = append(errs, errors.Join(identityErr, closeErr))
				continue
			}
			if !rootpublication.SamePhysicalIdentity(identity, seg.identity) {
				errs = append(errs, errors.Join(
					fmt.Errorf("%w: rewrite-created segment %d path was rebound", rootpublication.ErrResourceConflict, seg.fileID),
					ErrRecoveryRequired,
				))
				continue
			}
		}
		if db != nil && db.valueLogManager != nil {
			if err := db.valueLogManager.RegisterSegment(seg.path, seg.fileID); err != nil {
				errs = append(errs, fmt.Errorf("register rewrite-created segment %d for removal: %w", seg.fileID, err))
				continue
			}
			var removeErr error
			if seg.identity != (rootpublication.StableIdentity{}) {
				removeErr = db.valueLogManager.RemoveSegmentExpectedIdentity(seg.fileID, seg.identity)
			} else {
				removeErr = db.valueLogManager.RemoveSegmentForce(seg.fileID)
			}
			if removeErr != nil {
				errs = append(errs, fmt.Errorf("remove rewrite-created segment %d: %w", seg.fileID, errors.Join(removeErr, ErrRecoveryRequired)))
			}
			continue
		}
		if _, err := removePersistentFile(filepath.Dir(seg.path), seg.path, valueLogResourceForPath(seg.path)); err != nil {
			errs = append(errs, fmt.Errorf("remove rewrite-created segment %d: %w", seg.fileID, err))
		}
	}
	return errors.Join(errs...)
}

type leafRefRewriteCtx struct {
	ctx context.Context
	db  *DB

	pager             *pager.Pager
	leafReader        tree.SlabReader
	leafToer          unsafeToReader
	privateLeafReader *valuelog.Manager
	leafScratch       []byte
	alloc             interface {
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

	sourceScan                  *leafGenerationScanContext
	sourceGenerationByFile      map[uint32]uint64
	sourceLiveMovedByGeneration map[uint64]leafGenerationLiveTotals

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
	privateCOWOwner                        *transactionowner.Owner
	handoffTransition                      leafGenerationPackHandoffTransition
	transition                             LeafGenerationPackTransition
	maintenanceLimits                      LeafGenerationMaintenanceLimits
	InternalPagesVisited                   int
	SubtreesPruned                         int
	LeafFramesWritten                      int
	MaxLeafFrameK                          int
	CopyTimeNanos                          int64
	PublishWaitNanos                       int64
	PublishHoldNanos                       int64
	PrivatePages                           int
	PublishConflict                        bool
	ApplyStages                            LeafGenerationPackApplyStageStats
	trackCarry                             bool
	trackSourceLiveMoved                   bool
	protectedRootIDs                       []uint64
	protectedSystemRootIDs                 []uint64
	sourceStateKey                         treeReachabilityCacheKey
	publishedState                         *DBState
	protectedRootsOverlapSourceMaintenance bool
	sourceLiveMovedByGeneration            map[uint64]leafGenerationLiveTotals
}

type leafGenerationPackCopyPhase uint8

const leafGenerationPackCopyComplete leafGenerationPackCopyPhase = 1

type leafGenerationPackCopyEvent struct {
	Phase          leafGenerationPackCopyPhase
	Attempt        int
	CreatedFileIDs []uint32
	PrivatePageIDs []uint64
}

var leafGenerationPackCopyHook struct {
	mu sync.Mutex
	fn func(leafGenerationPackCopyEvent)
}

func registerLeafGenerationPackCopyHook(hook func(leafGenerationPackCopyEvent)) func() {
	leafGenerationPackCopyHook.mu.Lock()
	previous := leafGenerationPackCopyHook.fn
	leafGenerationPackCopyHook.fn = hook
	leafGenerationPackCopyHook.mu.Unlock()
	return func() {
		leafGenerationPackCopyHook.mu.Lock()
		leafGenerationPackCopyHook.fn = previous
		leafGenerationPackCopyHook.mu.Unlock()
	}
}

func runLeafGenerationPackCopyHook(event leafGenerationPackCopyEvent) {
	leafGenerationPackCopyHook.mu.Lock()
	hook := leafGenerationPackCopyHook.fn
	leafGenerationPackCopyHook.mu.Unlock()
	if hook != nil {
		hook(event)
	}
}

type leafGenerationPackPublishPhase uint8

const (
	leafGenerationPackBeforePromotion leafGenerationPackPublishPhase = iota + 1
	leafGenerationPackAfterPromotion
	leafGenerationPackAfterDirectorySync
	leafGenerationPackBeforeRegistration
	leafGenerationPackAfterRegistration
	leafGenerationPackBeforeMetaWrite
	// Keep this value appended so existing failpoint ordinals remain stable.
	// The event itself runs before publication locks are acquired.
	leafGenerationPackAfterManifestPreparation
)

type leafGenerationPackPublishEvent struct {
	Phase   leafGenerationPackPublishPhase
	Attempt int
	FileIDs []uint32
}

var leafGenerationPackPublishHook struct {
	mu sync.Mutex
	fn func(leafGenerationPackPublishEvent) error
}

var closeLeafGenerationPackStagingReaderFn = func(reader *valuelog.Manager) error {
	return reader.Close()
}

func registerLeafGenerationPackPublishHook(hook func(leafGenerationPackPublishEvent) error) func() {
	leafGenerationPackPublishHook.mu.Lock()
	previous := leafGenerationPackPublishHook.fn
	leafGenerationPackPublishHook.fn = hook
	leafGenerationPackPublishHook.mu.Unlock()
	return func() {
		leafGenerationPackPublishHook.mu.Lock()
		leafGenerationPackPublishHook.fn = previous
		leafGenerationPackPublishHook.mu.Unlock()
	}
}

func runLeafGenerationPackPublishHook(event leafGenerationPackPublishEvent) error {
	leafGenerationPackPublishHook.mu.Lock()
	hook := leafGenerationPackPublishHook.fn
	leafGenerationPackPublishHook.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(event)
}

var errLeafGenerationPackPublishConflict = errors.New("leaf generation pack: copy basis changed before publish")

type leafGenerationPackSourceGeneration struct {
	state   string
	fileIDs []uint32
}

type leafGenerationPackCopyBasis struct {
	idx                        *indexGen
	commitSeq                  uint64
	rootPageID                 uint64
	systemRootPageID           uint64
	leafGenerationStateVersion uint64
	sourceGenerations          map[uint64]leafGenerationPackSourceGeneration
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
	if c.privateLeafReader != nil && c.writer != nil && !c.privateLeafReader.HasSegment(ptr.FileID) {
		for _, fileID := range c.writer.createdIDs {
			if fileID != ptr.FileID {
				continue
			}
			if err := c.writer.Flush(); err != nil {
				return nil, err
			}
			if err := c.privateLeafReader.Refresh(); err != nil {
				return nil, err
			}
			break
		}
	}
	if c.privateLeafReader != nil && c.privateLeafReader.HasSegment(ptr.FileID) {
		if cap(c.leafScratch) < page.PageSize {
			c.leafScratch = make([]byte, 0, page.PageSize)
		} else {
			c.leafScratch = c.leafScratch[:0]
		}
		leafPage, usedScratch, err := c.privateLeafReader.ReadUnsafeTo(ptr, c.leafScratch[:0])
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
	if c.leafReader != nil {
		return c.leafReader.ReadUnsafe(ptr)
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
	return nil, fmt.Errorf("vlog-rewrite: value-log snapshot reader unavailable")
}

func (c *leafRefRewriteCtx) ReadUnsafe(ptr page.ValuePtr) ([]byte, error) {
	return c.readLeafPage(ptr)
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
	if err := c.noteSourceLeafMoved(ptr); err != nil {
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
			if err := c.noteSourceLeafMoved(oldPtr); err != nil {
				return nil, err
			}
			c.storeLeafPtrRemap(oldPtr, ref)
			out[start+i] = ref
		}
		start = end
	}
	return out, nil
}

func (c *leafRefRewriteCtx) noteSourceLeafMoved(ptr page.LeafLogPtr) error {
	if c == nil || c.db == nil || c.sourceScan == nil || c.sourceScan.snap == nil || c.sourceScan.snap.state == nil {
		return nil
	}
	generationID := c.sourceGenerationByFile[ptr.FileID]
	if generationID == 0 {
		return nil
	}
	recordLen := ptr.RecordLength()
	if recordLen == 0 {
		var err error
		recordLen, err = c.db.valueLogRecordLengthForRewriteInSet(ptr.ValuePtr(), c.sourceScan.snap.state.ValueLogSet)
		if err != nil {
			return err
		}
	}
	liveBytes := recordLen
	if liveBytes <= ^uint32(0)-4 {
		liveBytes += 4
	}
	if ptr.IsGrouped() {
		info, grouped, err := c.db.leafGenerationGroupedFrameInfo(c.sourceScan, ptr, recordLen)
		if err != nil {
			return err
		}
		if grouped {
			if contribution, ok := info.liveByteContribution(ptr.SubIndex); ok {
				liveBytes = contribution
			}
		}
	}
	if c.sourceLiveMovedByGeneration == nil {
		c.sourceLiveMovedByGeneration = make(map[uint64]leafGenerationLiveTotals)
	}
	totals := c.sourceLiveMovedByGeneration[generationID]
	totals.LivePages++
	totals.LiveBytes += int64(liveBytes)
	c.sourceLiveMovedByGeneration[generationID] = totals
	return nil
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
	case page.PageTypePrimaryDirectory:
		directory, err := node.DecodePrimaryDirectory(data)
		if err != nil {
			return id, false, err
		}
		base, seq := directory.Base()
		baseImage, err := c.pager.Get(base.Ref.Page)
		if err != nil {
			return id, false, err
		}
		if !node.VerifyPrimaryOperand(base, baseImage) {
			return id, false, node.ErrPrimaryDirectory
		}
		entries := make([]node.PrimaryDirectoryEntry, directory.Count())
		for i := range entries {
			entries[i], _ = directory.Entry(i)
			if entries[i].InlineAbsence() {
				continue
			}
			if entries[i].Operand.Ref.Kind != page.ChildRefPage {
				return id, false, node.ErrPrimaryDirectory
			}
			image, err := c.pager.Get(entries[i].Operand.Ref.Page)
			if err != nil {
				return id, false, err
			}
			if err = node.ValidatePrimaryComponent(entries[i], image); err != nil {
				return id, false, err
			}
		}
		newBase, changed, err := c.rewriteNode(base.Ref.Page)
		if err != nil || !changed {
			return id, false, err
		}
		baseImage, err = c.pager.Get(newBase)
		if err != nil {
			return id, false, err
		}
		base = node.PrimaryOperand{Ref: page.PageChildRef(newBase), Digest: sha256.Sum256(baseImage)}
		newID, err := c.alloc.Alloc(id)
		if err != nil {
			return id, false, err
		}
		image, err := c.pager.GetForWrite(newID)
		if err != nil {
			return id, false, err
		}
		if err = node.EncodePrimaryDirectory(image, newID, seq+1, base, entries); err != nil {
			return id, false, err
		}
		if !primaryarena.IsPage(id) {
			c.retired = append(c.retired, id)
		}
		c.storeInternalRemap(id, newID)
		return newID, true, nil
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
		// Staging mixes virtual private IDs and ordinary source IDs. Full-width
		// child refs avoid the uint32 span limit of base-delta encoding.
		b := node.NewBuilder(buf, page.PageTypeInternal)
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

func captureLeafGenerationPackCopyBasis(snap *Snapshot, sourceIDs map[uint32]struct{}, singleSourceID uint32, hasSingleSourceID bool) leafGenerationPackCopyBasis {
	basis := leafGenerationPackCopyBasis{sourceGenerations: make(map[uint64]leafGenerationPackSourceGeneration)}
	if snap == nil || snap.state == nil {
		return basis
	}
	basis.idx = snap.idx
	basis.commitSeq = snap.state.CommitSeq
	basis.rootPageID = snap.state.RootPageID
	basis.systemRootPageID = snap.state.SystemRootPageID
	basis.leafGenerationStateVersion = snap.state.LeafGenerationStateVersion
	view := snap.state.LeafGenerations
	if view == nil {
		return basis
	}
	for rawFileID, generationID := range view.FileToGeneration {
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
		generation, ok := view.Generations[generationID]
		if !ok {
			continue
		}
		basis.sourceGenerations[generationID] = leafGenerationPackSourceGeneration{
			state:   generation.State,
			fileIDs: append([]uint32(nil), generation.FileIDs...),
		}
	}
	return basis
}

func equalLeafGenerationPackFileIDs(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (db *DB) leafGenerationPackCopyBasisMatches(basis leafGenerationPackCopyBasis) bool {
	if db == nil || db.idx.Load() != basis.idx {
		return false
	}
	state := db.State()
	if state == nil ||
		state.CommitSeq != basis.commitSeq ||
		state.RootPageID != basis.rootPageID ||
		state.SystemRootPageID != basis.systemRootPageID ||
		state.LeafGenerationStateVersion != basis.leafGenerationStateVersion {
		return false
	}
	if len(basis.sourceGenerations) == 0 {
		return true
	}
	if state.LeafGenerations == nil {
		return false
	}
	for generationID, expected := range basis.sourceGenerations {
		current, ok := state.LeafGenerations.Generations[generationID]
		if !ok || current.State != expected.state || !equalLeafGenerationPackFileIDs(current.FileIDs, expected.fileIDs) {
			return false
		}
		for _, rawFileID := range expected.fileIDs {
			if state.LeafGenerations.FileToGeneration[rawFileID] != generationID {
				return false
			}
		}
	}
	return true
}

func cloneLeafGenerationPackStagedNode(staged *pager.Pager, id uint64, alloc *leafGenerationPackPublishAllocator, remap map[uint64]uint64) (uint64, error) {
	if id == 0 || id < leafGenerationPackPrivatePageIDBase {
		return id, nil
	}
	if mapped, ok := remap[id]; ok {
		return mapped, nil
	}
	if staged == nil || alloc == nil {
		return 0, errors.New("leaf generation pack: missing staged publish state")
	}
	data, err := staged.Get(id)
	if err != nil {
		return 0, err
	}
	n := node.NewNodeView(data)
	if !n.VerifyChecksum() {
		return 0, fmt.Errorf("leaf generation pack: staged page %d checksum mismatch", id)
	}
	if n.Type() == page.PageTypePrimaryDirectory {
		if alloc.primary == nil || alloc.zipper == nil {
			return 0, errors.New("leaf generation pack: primary directory lacks bank owner")
		}
		directory, err := node.DecodePrimaryDirectory(data)
		if err != nil {
			return 0, err
		}
		base, sequence := directory.Base()
		baseID, err := cloneLeafGenerationPackStagedNode(staged, base.Ref.Page, alloc, remap)
		if err != nil {
			return 0, err
		}
		entries := make([]node.PrimaryDirectoryEntry, directory.Count())
		for i := range entries {
			entries[i], err = directory.Entry(i)
			if err != nil {
				return 0, err
			}
			if !entries[i].InlineAbsence() && !primaryarena.IsPage(entries[i].Operand.Ref.Page) {
				return 0, errors.Join(err, node.ErrPrimaryDirectory)
			}
		}
		root, err := alloc.zipper.RebasePrimaryRoot(baseID, sequence, entries)
		if err != nil {
			return 0, err
		}
		alloc.primaryRoots = append(alloc.primaryRoots, root)
		remap[id] = root
		return root, nil
	}
	if n.Type() != page.PageTypeInternal {
		return 0, fmt.Errorf("leaf generation pack: staged page %d has unexpected type %d", id, n.Type())
	}
	newID, err := alloc.Alloc(id)
	if err != nil {
		return 0, err
	}
	remap[id] = newID
	buf, err := alloc.pager.GetForWrite(newID)
	if err != nil {
		return 0, err
	}
	// Virtual and source IDs may differ by more than uint32, so staged nodes use
	// full-width refs. Publish keeps that representation for the rewritten path.
	b := node.NewBuilder(buf, page.PageTypeInternal)
	b.SetPageID(newID)
	if low, high, ok, err := n.InternalFenceBounds(); err != nil {
		return 0, err
	} else if ok {
		b.SetInternalFenceBounds(low, high)
	}
	for i := uint16(0); i < n.Count(); i++ {
		key, ref, err := n.GetInternalEntryRefView(i)
		if err != nil {
			return 0, err
		}
		if ref.Kind == page.ChildRefPage {
			ref.Page, err = cloneLeafGenerationPackStagedNode(staged, ref.Page, alloc, remap)
			if err != nil {
				return 0, err
			}
		}
		if err := b.AddInternalChildRef(key, ref); err != nil {
			return 0, err
		}
	}
	b.FinishNoNode()
	return newID, nil
}

func rebaseLeafGenerationPackCollectionReplacements(staged *pager.Pager, replacements []vacuumCollectionRootReplacement, alloc *leafGenerationPackPublishAllocator, remap map[uint64]uint64) ([]vacuumCollectionRootReplacement, error) {
	if len(replacements) == 0 {
		return nil, nil
	}
	out := make([]vacuumCollectionRootReplacement, 0, len(replacements))
	for _, replacement := range replacements {
		allowList := bytes.HasPrefix(replacement.key, vacuumCollectionRootOverlayDescriptorPrefixBytes)
		rootIDs, err := decodeCollectionRootDescriptorRootIDs(replacement.key, replacement.value, allowList)
		if err != nil {
			return nil, err
		}
		for i, rootID := range rootIDs {
			rootIDs[i], err = cloneLeafGenerationPackStagedNode(staged, rootID, alloc, remap)
			if err != nil {
				return nil, fmt.Errorf("leaf generation pack: publish collection root %d: %w", rootID, err)
			}
		}
		out = append(out, vacuumCollectionRootReplacement{
			key:   append([]byte(nil), replacement.key...),
			value: encodeCollectionRootDescriptorRootIDs(rootIDs),
		})
	}
	return out, nil
}

func leafGenerationPackCommittedRetired(ids []uint64) []uint64 {
	out := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if id != 0 && id < leafGenerationPackPrivatePageIDBase && !primaryarena.IsPage(id) {
			out = append(out, id)
		}
	}
	return out
}

func (db *DB) rewriteLeafRefsOnline(ctx context.Context, writer *rewriteWriter, ridAlloc *rewriteRIDAllocator, sourceIDs map[uint32]struct{}, sourceChunks map[valueLogChunkKey]ValueLogRewritePlanChunk, sourceChunkBytes int64, singleSourceID uint32, hasSingleSourceID bool, maxCopiedBytes int64, _ bool, leafFrameK int, attempt int, runStats *leafRefRewriteRunStats) (copied int, copiedBytes int64, err error) {
	applyStarted := time.Now()
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
	authority, err := newLeafGenerationPackPromotionAuthority(db, writer.leafStagingRoot, writer.leafDir, resolveStorageLayout(db.dir).leafVLogDir)
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: prepare packed promotion authority: %w", err)
	}
	if err := authority.captureDictionary(ctx, writer.leafDictID, writer.leafDict); err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: capture packed dictionary authority: %w", errors.Join(err, authority.release()))
	}
	defer func() {
		if !authority.retainedForRecovery {
			err = errors.Join(err, authority.release())
		}
	}()

	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if db.closing.Load() {
		return 0, 0, ErrClosed
	}

	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		closeRewriteSnapshot(&err, snap)
		return 0, 0, fmt.Errorf("missing snapshot state")
	}
	defer closeRewriteSnapshot(&err, snap)
	if runStats != nil {
		if err := runStats.maintenanceLimits.admitSnapshot(ctx, snap); err != nil {
			return 0, 0, err
		}
	}

	idx := snap.idx
	transitionBefore, _ := stateTokenFromState(snap.state)
	if runStats != nil && runStats.privateCOWOwner != nil {
		transitionBefore, err = runStats.handoffTransition.sourceForCopy(db, runStats.privateCOWOwner, snap)
		if err != nil {
			return 0, 0, err
		}
	}
	rootID := snap.state.RootPageID
	sysRoot := snap.state.SystemRootPageID
	basis := captureLeafGenerationPackCopyBasis(snap, sourceIDs, singleSourceID, hasSingleSourceID)
	if runStats != nil && runStats.trackCarry {
		runStats.sourceStateKey, _ = leafGenerationLiveStatsKeyForState(snap.state)
		if runStats.trackSourceLiveMoved {
			runStats.protectedRootsOverlapSourceMaintenance, err = leafGenerationProtectedRootsOverlapMaintenance(
				ctx,
				snap,
				runStats.protectedRootIDs,
				runStats.protectedSystemRootIDs,
			)
			if err != nil {
				return 0, 0, fmt.Errorf("leaf generation pack: compare protected roots with source maintenance roots: %w", err)
			}
		}
	}

	stagingPager, err := pager.NewOverlay(db.chunkSize, leafGenerationPackPrivatePageIDBase, idx.pager)
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: create private index staging pager: %w", err)
	}
	defer func() {
		if closeErr := stagingPager.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	tracker := &leafGenerationPackStagingAllocator{pager: stagingPager}

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

	if runStats != nil {
		runStats.ApplyStages.SetupTimeNanos += time.Since(applyStarted).Nanoseconds()
	}
	copyStarted := time.Now()
	leafCtx := &leafRefRewriteCtx{
		ctx:                  ctx,
		db:                   db,
		pager:                stagingPager,
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
	if runStats != nil && runStats.trackSourceLiveMoved {
		leafCtx.sourceScan = &leafGenerationScanContext{
			snap:          snap,
			groupedFrames: newLeafGenerationGroupedFrameScanCache(leafGenerationGroupedFrameScanCacheEntries),
		}
		if snap.state.LeafGenerations != nil {
			leafCtx.sourceGenerationByFile = snap.state.LeafGenerations.FileToGeneration
		}
	}
	privateLeafReader, err := valuelog.NewManager(writer.leafDir)
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: open private leaf reader: %w", err)
	}
	privateLeafReader.SetDictLookup(db.valueLogDictLookup)
	leafCtx.privateLeafReader = privateLeafReader
	defer func() {
		if privateLeafReader == nil {
			return
		}
		if closeErr := closeLeafGenerationPackStagingReaderFn(privateLeafReader); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
	leafCtx.zipper = idx.zipper.CloneWithPagerAllocator(stagingPager, tracker)
	// This zipper applies collection descriptors to SYSTEM only. The user
	// PRIMARY directory is rewritten separately above its real operands; never
	// wrap a materialized SYSTEM delta in the inherited user-root preference.
	leafCtx.zipper.SetPrimaryDirectory(false)
	leafCtx.zipper.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
	leafCtx.zipper.SetLeafPageReader(leafCtx)
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
			if tracker != nil {
				runStats.PrivatePages = len(tracker.Pages())
			}
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
	preCollectionSysRoot := newSysRoot
	if len(collectionRootReplacements) > 0 {
		if sysChanged {
			if err := writer.Flush(); err != nil {
				return 0, 0, fmt.Errorf("vlog-rewrite: flush rewritten system leaf root: %w", err)
			}
			if err := privateLeafReader.Refresh(); err != nil {
				return 0, 0, fmt.Errorf("vlog-rewrite: refresh private leaf reader: %w", err)
			}
		}
		retiredBeforeCollectionDelta := len(leafCtx.retired)
		replacedSysRoot, replacementChanged, err := leafCtx.applySystemRootCollectionRootReplacements(newSysRoot, collectionRootReplacements)
		if err != nil {
			return 0, 0, fmt.Errorf("vlog-rewrite: rewrite system collection descriptors: %w", err)
		}
		// The staged descriptor delta proves and accounts for the copy-time work,
		// but its virtual root IDs must be rebased during publish. Its retired list
		// therefore has no ownership in the committed pager.
		leafCtx.retired = leafCtx.retired[:retiredBeforeCollectionDelta]
		newSysRoot = replacedSysRoot
		sysChanged = sysChanged || replacementChanged
	}
	newRoot, userChanged, err := leafCtx.rewriteNode(rootID)
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: rewrite user leaf root: %w", err)
	}
	if !sysChanged && !userChanged {
		if runStats != nil {
			runStats.ApplyStages.TreeRewriteTimeNanos += time.Since(copyStarted).Nanoseconds()
			runStats.CopyTimeNanos = time.Since(copyStarted).Nanoseconds()
		}
		return 0, 0, nil
	}

	createdIDs, err := writer.createdFileIDs()
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: list created leaf ref files: %w", err)
	}
	privatePages := tracker.Pages()
	if runStats != nil {
		runStats.PrivatePages = len(privatePages)
	}
	runLeafGenerationPackCopyHook(leafGenerationPackCopyEvent{
		Phase:          leafGenerationPackCopyComplete,
		Attempt:        attempt,
		CreatedFileIDs: append([]uint32(nil), createdIDs...),
		PrivatePageIDs: append([]uint64(nil), privatePages...),
	})
	if runStats != nil {
		runStats.ApplyStages.TreeRewriteTimeNanos += time.Since(copyStarted).Nanoseconds()
	}

	// Pack publication is always durable. Copy fsync remains outside writeMu;
	// Sync=false is retained only for source compatibility with the pre-alpha
	// API and no longer weakens the root/segment publication contract.
	leafSyncStarted := time.Now()
	if err := writer.Sync(); err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: sync rewritten leaf refs: %w", err)
	}
	if runStats != nil {
		runStats.ApplyStages.LeafSyncTimeNanos += time.Since(leafSyncStarted).Nanoseconds()
	}
	copyCloseStarted := time.Now()
	// The private index pages live only in memory and are never a publication
	// candidate. Their reconstructed live-pager pages are synchronized before the
	// alternate meta page.
	createdSegments, err := writer.createdSegmentsSnapshot()
	if err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: snapshot created leaf ref segments: %w", err)
	}
	if err := authority.capture(createdSegments, leafGenerationPackPointers(leafCtx)); err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: capture packed segment authority: %w", err)
	}
	// The staging manager may mmap copied segments while applying collection
	// deltas. Windows forbids renaming those files while the mappings are open,
	// so release every staging read handle before entering publish.
	if err := closeLeafGenerationPackStagingReaderFn(privateLeafReader); err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: close copied leaf ref reader: %w", err)
	}
	privateLeafReader = nil
	leafCtx.privateLeafReader = nil
	if err := writer.Close(); err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: close copied leaf refs: %w", err)
	}
	if runStats != nil {
		runStats.ApplyStages.CopyCloseTimeNanos += time.Since(copyCloseStarted).Nanoseconds()
		runStats.CopyTimeNanos = time.Since(copyStarted).Nanoseconds()
	}

	publishEvent := leafGenerationPackPublishEvent{
		Attempt: attempt,
		FileIDs: append([]uint32(nil), createdIDs...),
	}
	publishEvent.Phase = leafGenerationPackBeforePromotion
	if err := runLeafGenerationPackPublishHook(publishEvent); err != nil {
		return 0, 0, fmt.Errorf("vlog-rewrite: promotion failpoint: %w", err)
	}
	var (
		publishAlloc     *leafGenerationPackPublishAllocator
		durableResources *rootpublication.StableResourceSet
	)
	cleanupCreatedSegments := func(baseErr error) (int, int64, error) {
		if publishAlloc != nil {
			baseErr = errors.Join(baseErr, publishAlloc.Rollback())
		}
		if durableResources != nil {
			if releaseErr := durableResources.ReleaseWithError(); releaseErr != nil {
				// Keep the exact refused Set/descendants in original error custody.
				// Destructive segment cleanup cannot follow failed pin disposal.
				db.retainOriginalCleanupError(releaseErr)
				return 0, 0, errors.Join(baseErr, releaseErr)
			}
			durableResources = nil
		}
		// Release the candidate set and capture pins before asking the manager's
		// deletion owner to acquire an exact-identity delete lease.
		if releaseErr := authority.releaseCaptured(); releaseErr != nil {
			return 0, 0, errors.Join(baseErr, authority.retainCleanupFailure(releaseErr))
		}
		// createdSegments borrows authority.promoted. Keep that original backing
		// intact until the manager has consumed every actual deletion request.
		cleanupErr := db.cleanupRewriteCreatedSegments(createdSegments)
		if cleanupErr != nil {
			baseErr = errors.Join(baseErr, cleanupErr)
		}
		if len(createdSegments) > 0 {
			leafVLogDir := resolveStorageLayout(db.dir).leafVLogDir
			if syncErr := syncDeletionNamespaceDirectory(leafVLogDir, durabilitycut.ResourceOuterLeaf); syncErr != nil {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("vlog-rewrite: sync cleaned leaf directory: %w", syncErr))
				baseErr = errors.Join(baseErr, cleanupErr)
			}
		}
		if cleanupErr != nil {
			return 0, 0, errors.Join(baseErr, authority.retainCleanupFailure(cleanupErr))
		}
		return 0, 0, errors.Join(baseErr, authority.release())
	}
	// The finalizer binds the one immutable manifest after capture, when the
	// promoted pack files and any late SYSTEM leaf appends are both known.
	if snap.state.LeafGenerations == nil || snap.state.LeafGenerations.sourceManifest == nil {
		return cleanupCreatedSegments(fmt.Errorf("%w: packed publication has no source leaf-generation manifest", rootpublication.ErrUnresolvedResource))
	}
	leafManifest := snap.state.LeafGenerations.sourceManifest
	leafManifestRawFileIDs := filterLeafGenerationRawFileIDs(createdIDs)
	afterManifestPreparation := func() error {
		event := publishEvent
		event.Phase = leafGenerationPackAfterManifestPreparation
		if err := runLeafGenerationPackPublishHook(event); err != nil {
			return fmt.Errorf("vlog-rewrite: manifest preparation failpoint: %w", err)
		}
		return nil
	}

	// Promotion registers files in the live value-log set. Acquire durable
	// ordering before the visibility gate so a preceding publisher can finish
	// capturing its set without seeing tentative files or waiting on us.
	publishWaitStarted := time.Now()
	db.writeMu.Lock()
	publishWait := time.Since(publishWaitStarted)
	publishStarted := time.Now()
	if runStats != nil {
		runStats.PublishWaitNanos = publishWait.Nanoseconds()
	}
	db.publishPrepareMu.RLock()
	buildLocked := true
	unlockBuild := func() {
		if !buildLocked {
			return
		}
		buildLocked = false
		db.publishPrepareMu.RUnlock()
		if runStats != nil {
			runStats.PublishHoldNanos = time.Since(publishStarted).Nanoseconds()
		}
		db.writeMu.Unlock()
	}
	var rootBuilder *rootpublication.BuilderToken
	if runtime := db.rootPublication; runtime != nil {
		var err error
		rootBuilder, err = runtime.coordinator.AcquireBuilder(ctx)
		if err != nil {
			unlockBuild()
			_, _, cleanupErr := cleanupCreatedSegments(publicRootPublicationErrorV1(err))
			return 0, 0, cleanupErr
		}
		defer rootBuilder.Release()
	}
	db.durablePublishMu.Lock()
	durableLocked := true
	unlockDurable := func() {
		if durableLocked {
			durableLocked = false
			db.durablePublishMu.Unlock()
		}
	}
	defer unlockDurable()
	db.valueLogPublicationMu.Lock()
	visibilityLocked := true
	unlockVisibility := func() {
		if visibilityLocked {
			visibilityLocked = false
			db.valueLogPublicationMu.Unlock()
		}
	}
	unlockPublish := func() {
		unlockVisibility()
		unlockDurable()
		unlockBuild()
	}
	cleanupAndUnlock := func(baseErr error) (int, int64, error) {
		copied, copiedBytes, cleanupErr := cleanupCreatedSegments(baseErr)
		unlockPublish()
		return copied, copiedBytes, cleanupErr
	}
	unlockAndCleanupBeforeVisibility := func(baseErr error) (int, int64, error) {
		unlockPublish()
		copied, copiedBytes, cleanupErr := cleanupCreatedSegments(baseErr)
		return copied, copiedBytes, cleanupErr
	}
	if db.closing.Load() {
		return unlockAndCleanupBeforeVisibility(ErrClosed)
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return unlockAndCleanupBeforeVisibility(err)
	}
	// Refresh can publish a new state without writeMu. Revalidate only after the
	// visibility gate is held so every basis field stays stable through promote
	// and publication.
	if !db.leafGenerationPackCopyBasisMatches(basis) {
		if runStats != nil {
			runStats.PublishConflict = true
			runStats.ApplyStages.RevalidateTimeNanos += time.Since(publishStarted).Nanoseconds()
		}
		return unlockAndCleanupBeforeVisibility(errLeafGenerationPackPublishConflict)
	}
	if runStats != nil {
		runStats.ApplyStages.RevalidateTimeNanos += time.Since(publishStarted).Nanoseconds()
	}

	promotionStarted := time.Now()
	promotedSegments, _, promoteErr := authority.promote()
	if runStats != nil {
		runStats.ApplyStages.PromotionTimeNanos += time.Since(promotionStarted).Nanoseconds()
		runStats.ApplyStages.DirectorySyncTimeNanos += authority.namespaceSyncNanos
	}
	createdSegments = promotedSegments
	if promoteErr != nil {
		promoteErr = fmt.Errorf("vlog-rewrite: promote copied leaf refs: %w", promoteErr)
		if authority.retainedForRecovery {
			promoteErr = errors.Join(promoteErr, ErrRecoveryRequired)
			db.publicationPoisoned.Store(true)
			unlockPublish()
			return 0, 0, promoteErr
		}
		return cleanupAndUnlock(promoteErr)
	}
	publishEvent.Phase = leafGenerationPackAfterPromotion
	if err := runLeafGenerationPackPublishHook(publishEvent); err != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: promoted leaf directory failpoint: %w", err))
	}
	// Exact packed promotion already synchronized both the staging and
	// destination parents once as one namespace batch. Do not issue the former
	// path-based destination-directory sync here.

	packedResources, resourceErr := authority.takeStableResources()
	if resourceErr != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: transfer exact packed resources: %w", resourceErr))
	}
	// Preserve the packed producer's physical authority while admitting its
	// retained descriptor into the DB's metadata owner.
	metadata := db.valueLogIdentityPins.MetadataOwner()
	durableResources, resourceErr = rootpublication.ImportStableResourceSetMetadata(metadata, packedResources)
	if resourceErr = errors.Join(resourceErr, packedResources.ReleaseWithError()); resourceErr != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: admit exact packed resources: %w", resourceErr))
	}

	// Allocate committed IDs only after exact revalidation. The allocator owns
	// this append range through publication or retirement on refusal.
	relocateStarted := time.Now()
	publishAlloc = newLeafGenerationPackPublishAllocator(idx, basis.commitSeq)
	remap := make(map[uint64]uint64, len(privatePages))
	publishCollectionReplacements, err := rebaseLeafGenerationPackCollectionReplacements(stagingPager, collectionRootReplacements, publishAlloc, remap)
	if err != nil {
		return cleanupAndUnlock(err)
	}
	publishedRoot, err := cloneLeafGenerationPackStagedNode(stagingPager, newRoot, publishAlloc, remap)
	if err != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: publish user root: %w", err))
	}
	publishedSysRoot, err := cloneLeafGenerationPackStagedNode(stagingPager, preCollectionSysRoot, publishAlloc, remap)
	if err != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: publish system root: %w", err))
	}
	relocateDuration := time.Since(relocateStarted)
	if runStats != nil {
		runStats.ApplyStages.RelocationTimeNanos += relocateDuration.Nanoseconds()
	}
	publishRetired := leafGenerationPackCommittedRetired(leafCtx.retired)
	// The durable-root candidate synchronizes its exact index identity only
	// after root serialization is released. Syncing these pages here would put
	// index I/O back under write/root-build locks.
	pageSyncDuration := time.Duration(0)
	publishEvent.Phase = leafGenerationPackAfterDirectorySync
	if err := runLeafGenerationPackPublishHook(publishEvent); err != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: directory sync failpoint: %w", err))
	}

	publishEvent.Phase = leafGenerationPackBeforeRegistration
	if err := runLeafGenerationPackPublishHook(publishEvent); err != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: registration failpoint: %w", err))
	}
	registerStarted := time.Now()
	if len(createdSegments) > 0 {
		// Registration belongs to publish: staged files are invisible to Refresh
		// until exact revalidation has succeeded under writeMu.
		for _, seg := range createdSegments {
			if err := db.valueLogManager.RegisterSegment(seg.path, seg.fileID); err != nil {
				return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: register rewritten leaf segment %d: %w", seg.fileID, err))
			}
		}
		if err := authority.verifyManagerRegistration(); err != nil {
			return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: verify registered packed segment identity: %w", err))
		}
	}
	publishEvent.Phase = leafGenerationPackAfterRegistration
	if err := runLeafGenerationPackPublishHook(publishEvent); err != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: post-registration failpoint: %w", err))
	}
	registerDuration := time.Since(registerStarted)
	if runStats != nil {
		runStats.ApplyStages.RegistrationTimeNanos += registerDuration.Nanoseconds()
	}

	collectionPublishStarted := time.Now()
	if len(publishCollectionReplacements) > 0 {
		if db.leafPageLog == nil {
			return cleanupAndUnlock(errors.New("vlog-rewrite: collection root publication requires leaf page log"))
		}
		publishCtx := &leafRefRewriteCtx{
			ctx:        ctx,
			db:         db,
			pager:      idx.pager,
			leafReader: db.valueLogManager,
			alloc:      publishAlloc,
		}
		publishCtx.zipper = idx.zipper.CloneWithPagerAllocator(idx.pager, publishAlloc)
		publishCtx.zipper.SetPrimaryDirectory(false)
		publishCtx.zipper.SetOuterLeavesInValueLog(db.indexOuterLeavesInValueLog)
		publishCtx.zipper.SetLeafPageReader(db.valueLogManager)
		publishCtx.zipper.SetLeafPageLog(db.leafPageLog)
		publishedSysRoot, _, err = publishCtx.applySystemRootCollectionRootReplacements(publishedSysRoot, publishCollectionReplacements)
		if err != nil {
			return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: publish collection descriptors: %w", err))
		}
		publishRetired = append(publishRetired, publishCtx.retired...)
		// Expose the append frontier to exact resource capture without performing
		// physical dependency I/O while publication locks are held. The candidate
		// resource set owns the subsequent stable-file sync.
		if err := db.leafPageLog.Flush(); err != nil {
			return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: flush published collection descriptors: %w", err))
		}
	}
	if runStats != nil {
		runStats.ApplyStages.CollectionPublishTimeNanos += time.Since(collectionPublishStarted).Nanoseconds()
	}
	relocationFinish := func(bool) {}
	if len(descriptors) > 0 {
		rootRelocations := make(map[uint64]uint64, len(descriptors))
		for _, descriptor := range descriptors {
			next := descriptor.rootID
			if staged, ok := leafCtx.lookupInternalRemap(next); ok {
				next = staged
			}
			if next >= leafGenerationPackPrivatePageIDBase {
				var ok bool
				next, ok = remap[next]
				if !ok {
					return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: missing published collection root remap for %d", descriptor.rootID))
				}
			}
			rootRelocations[descriptor.rootID] = next
		}
		relocationFinish, err = db.prepareCollectionRelocation(snap, idx.pager, rootRelocations)
		if err != nil {
			return cleanupAndUnlock(err)
		}
	}
	relocationCommitted := false
	defer func() {
		if !relocationCommitted {
			relocationFinish(false)
		}
	}()
	publishEvent.Phase = leafGenerationPackBeforeMetaWrite
	if err := runLeafGenerationPackPublishHook(publishEvent); err != nil {
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: meta write failpoint: %w", err))
	}

	var equivalentOwner *transactionowner.Owner
	if runStats != nil {
		equivalentOwner = runStats.privateCOWOwner
	}
	var acceptedStateToken StateToken
	packPublication := &packedRootPublication{
		afterActivation:          unlockVisibility,
		afterManifestPreparation: afterManifestPreparation,
	}
	finalizeStarted := time.Now()
	post, finalizeErr := db.finalizeCommitLockedWithOptions(publishedRoot, publishedSysRoot, publishRetired, true, adaptive.Metrics{}, createdIDs, len(publishCollectionReplacements) > 0, nil, leafManifest, leafManifestRawFileIDs, finalizeCommitOptions{
		privateCOWRewrite:         newPrivateCOWRewriteFinalization(equivalentOwner, &acceptedStateToken),
		skipPrePublishFlush:       true,
		syncMetaPageOnly:          true,
		durableIndex:              idx,
		expectedBaseCommitSeq:     basis.commitSeq,
		hasExpectedBaseCommitSeq:  true,
		durablePublishLocked:      true,
		durablePublishRelease:     unlockDurable,
		rootPublicationBuilder:    rootBuilder,
		releaseRootSerialization:  unlockBuild,
		durableResources:          durableResources,
		packedRootPublication:     packPublication,
		valueLogPublicationLocked: true,
	})
	// The finalizer consumes producer closures on every path. A prepared retry
	// candidate, when present, now owns their exact handles independently of the
	// construction authority below.
	durableResources = nil
	// Acceptance means the replacement roots are already visible even if a
	// subsequent admission/durability wait fails. Catalog relocation follows
	// that visibility boundary, not the successful return of the finalizer.
	if finalizeErr == nil || CommitPublicationAccepted(finalizeErr) {
		if runStats != nil && equivalentOwner != nil {
			// The copy basis was revalidated under the visibility gate above.
			// rewriteLeafRef/Batch preserve decoded entries and asset references;
			// descriptor publication substitutes only their physical root IDs.
			// The finalizer fills this caller's exact installed scalar endpoint, including
			// after accepted-error cleanup; sampling db.state here would race a
			// newer publication after root serialization was released.
			before, ok := stateTokenFromState(snap.state)
			// The queued finalizer may CopyPrimaryRoot/BuildPrimaryRoot to normalize
			// the physical USER directory while preserving its decoded contents.
			// Retain that actual accepted RootPageID, not pre-finalizer publishedRoot.
			after := acceptedStateToken
			if ok && after.CommitSeq > before.CommitSeq && after.CommitSeq == before.CommitSeq+1 &&
				after.LeafGenerationStateVersion > before.LeafGenerationStateVersion &&
				after.LeafGenerationStateVersion == before.LeafGenerationStateVersion+1 &&
				after.SystemRootPageID == publishedSysRoot &&
				after.AppliedCommandLSN == before.AppliedCommandLSN && after.MaxEntryRevision == before.MaxEntryRevision {
				runStats.transition = LeafGenerationPackTransition{
					db: db, owner: equivalentOwner, idx: idx,
					before: transitionBefore, copyBasis: before, after: after,
				}
			}
		}
		relocationFinish(true)
		relocationCommitted = true
	}
	finalizeDuration := time.Since(finalizeStarted)
	if runStats != nil {
		runStats.ApplyStages.FinalizeTimeNanos += finalizeDuration.Nanoseconds()
	}
	if finalizeErr != nil {
		if durableLocked && db.durableRoot.pending != nil {
			// Before target-meta mutation the exact COW/resource candidate remains
			// retryable and owns its allocator reservations. Rolling back pages or
			// unlinking packed children here would corrupt that retained candidate.
			if releaseErr := authority.release(); releaseErr != nil {
				finalizeErr = errors.Join(finalizeErr, releaseErr)
			}
			unlockPublish()
			return 0, 0, fmt.Errorf("vlog-rewrite: finalize rewritten leaf refs: %w", finalizeErr)
		}
		if !finalizeCommitErrorAllowsCreatedSegmentCleanup(finalizeErr) || errors.Is(finalizeErr, ErrRecoveryRequired) {
			// A meta outcome or rollback of the deferred stable manifest can be
			// ambiguous. Retain the exact candidate files until reopen.
			db.publicationPoisoned.Store(true)
			retentionErr := authority.retainForRecovery()
			unlockPublish()
			return 0, 0, errors.Join(
				fmt.Errorf("vlog-rewrite: finalize rewritten leaf refs: %w", finalizeErr),
				db.commandWALPoisonedError(), retentionErr,
			)
		}
		return cleanupAndUnlock(fmt.Errorf("vlog-rewrite: finalize rewritten leaf refs: %w", finalizeErr))
	}
	// Metadata now makes the manager-owned exact identities reachable. Its
	// observers take over deletion fencing, so the local packed candidate set
	// can be released only at this point.
	if err := authority.release(); err != nil {
		db.publicationPoisoned.Store(true)
		unlockPublish()
		return 0, 0, errors.Join(fmt.Errorf("vlog-rewrite: release packed promotion authority: %w", err), ErrRecoveryRequired)
	}
	db.invalidateLeafGenerationSubtreeStats(publishAlloc.Pages())
	if runStats != nil && runStats.trackCarry {
		runStats.publishedState = db.state.Load()
		if runStats.trackSourceLiveMoved {
			runStats.sourceLiveMovedByGeneration = cloneLeafGenerationLiveTotalsMap(leafCtx.sourceLiveMovedByGeneration)
		}
	}
	commitTimingPrintf("treedb: leaf_pack_publish relocate=%s page_sync=%s namespace_sync=%s register=%s finalize=%s total=%s\n", relocateDuration, pageSyncDuration, time.Duration(authority.namespaceSyncNanos), registerDuration, finalizeDuration, time.Since(publishStarted))
	unlockPublish()
	postWorkStarted := time.Now()
	db.finalizeCommitPostWork(post)
	if runStats != nil {
		runStats.ApplyStages.PostWorkTimeNanos += time.Since(postWorkStarted).Nanoseconds()
	}
	return leafCtx.copied, leafCtx.copiedBytes, nil
}
