package bulk

import (
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type bulkLeafBatchSlab struct {
	pages      [valuelog.MaxFrameK][page.PageSize]byte
	pageSlices [valuelog.MaxFrameK][]byte
	keys       [valuelog.MaxFrameK][]byte
	keyArena   []byte
	count      int
}

func (s *bulkLeafBatchSlab) reset() {
	for i := 0; i < s.count; i++ {
		s.pageSlices[i] = nil
		s.keys[i] = nil
	}
	s.count = 0
	s.keyArena = s.keyArena[:0]
}

var bulkLeafBatchSlabPool = sync.Pool{
	New: func() any {
		return &bulkLeafBatchSlab{}
	},
}

func acquireBulkLeafBatchSlab() *bulkLeafBatchSlab {
	s := bulkLeafBatchSlabPool.Get().(*bulkLeafBatchSlab)
	s.reset()
	return s
}

func releaseBulkLeafBatchSlab(s *bulkLeafBatchSlab) {
	if s == nil {
		return
	}
	s.reset()
	if cap(s.keyArena) > 1<<20 {
		s.keyArena = nil
	}
	bulkLeafBatchSlabPool.Put(s)
}

type Allocator interface {
	Alloc(hint uint64) (uint64, error)
}

// LeafPageAppender persists leaf pages as value-log records and returns their pointers.
//
// When BuildOptions.LeafPageLog is non-nil, BuildWithOptions writes leaf pages
// via this interface and stores explicit leaf-log child references in the
// bottom internal level. The returned root is always a normal index page ID.
type LeafPageAppender interface {
	AppendLeafPage(leafPage []byte) (page.LeafLogPtr, error)
}

type LeafPageBatchAppender interface {
	AppendLeafPages(leafPages [][]byte) ([]page.LeafLogPtr, error)
}

type levelBuilder struct {
	builder  *node.Builder
	startKey []byte
}

type BuildOptions struct {
	LeafPrefixCompression bool
	LeafColumnar          bool
	InternalBaseDelta     bool
	PackedValuePtr        bool
	EntryRevisions        bool
	LeafPageLog           LeafPageAppender
	// MaxOutputPages bounds emitted pager and leaf-log pages. Zero leaves the
	// ordinary builder unrestricted. OutputPages receives the attempted count.
	MaxOutputPages uint64
	OutputPages    *uint64
}

var ErrPageOutputLimit = errors.New("bulk: page output limit")

// Build creates a new B-Tree from a sorted iterator.
func Build(iter iterator.UnsafeIterator, alloc Allocator, p *pager.Pager) (uint64, error) {
	return BuildWithOptions(iter, alloc, p, BuildOptions{})
}

// EntryEmitter receives one sorted entry during BuildWithScan.
type EntryEmitter func(key, val []byte, ptr page.ValuePtr, flags byte, rev page.EntryRevision) error

// BuildWithOptions creates a new B-Tree from a sorted iterator with custom options.
func BuildWithOptions(iter iterator.UnsafeIterator, alloc Allocator, p *pager.Pager, opts BuildOptions) (uint64, error) {
	return BuildWithScan(func(emit EntryEmitter) error {
		for iter.Valid() {
			key := iter.UnsafeKey()
			val, ptr, flags, revision := iterator.UnsafeEntryWithRevision(iter)
			if err := emit(key, val, ptr, flags, revision); err != nil {
				return err
			}
			if !iter.Valid() {
				if err := iter.Error(); err != nil {
					return err
				}
				break
			}
			iter.Next()
		}
		return nil
	}, alloc, p, opts)
}

// BuildWithScan creates a new B-Tree from a push-based sorted entry scan.
func BuildWithScan(scan func(emit EntryEmitter) error, alloc Allocator, p *pager.Pager, opts BuildOptions) (uint64, error) {
	leafLog := opts.LeafPageLog
	var outputPages uint64
	if opts.OutputPages != nil {
		*opts.OutputPages = 0
	}
	countPage := func() error {
		if opts.MaxOutputPages != 0 && outputPages >= opts.MaxOutputPages {
			return ErrPageOutputLimit
		}
		outputPages++
		if opts.OutputPages != nil {
			*opts.OutputPages = outputPages
		}
		return nil
	}

	var levels []*levelBuilder
	batcher, hasBatcher := leafLog.(LeafPageBatchAppender)
	canPipelineLeafBatches := leafLog != nil && hasBatcher && !durabilitycut.Enabled()

	var leafSlabs [2]*bulkLeafBatchSlab
	activeSlabIdx := 0
	type leafBatchResult struct {
		ptrs []page.LeafLogPtr
		err  error
	}
	var inFlightCh chan leafBatchResult
	inFlightSlabIdx := -1
	defer func() {
		if inFlightSlabIdx >= 0 && inFlightCh != nil {
			<-inFlightCh
			inFlightSlabIdx = -1
		}
		releaseBulkLeafBatchSlab(leafSlabs[0])
		releaseBulkLeafBatchSlab(leafSlabs[1])
	}()

	allocLeafBuf := func() []byte {
		if leafLog == nil {
			return make([]byte, page.PageSize)
		}
		if leafSlabs[activeSlabIdx] == nil {
			leafSlabs[activeSlabIdx] = acquireBulkLeafBatchSlab()
		}
		s := leafSlabs[activeSlabIdx]
		buf := s.pages[s.count][:]
		clear(buf)
		return buf
	}

	newBuilder := func(buf []byte, typ page.PageType) *node.Builder {
		if typ == page.PageTypeLeaf {
			return newLeafBuilder(buf, opts)
		}
		if opts.InternalBaseDelta {
			return node.NewBuilderWithOptions(buf, typ, node.BuilderOptions{
				InternalBaseDelta: opts.InternalBaseDelta,
			})
		}
		return node.NewBuilder(buf, typ)
	}

	ensureLevel := func(lvl int) error {
		for len(levels) <= lvl {
			typ := page.PageTypeInternal
			var buf []byte
			if len(levels) == 0 {
				typ = page.PageTypeLeaf
				buf = allocLeafBuf()
			} else {
				buf = make([]byte, page.PageSize)
			}

			b := newBuilder(buf, typ)
			if typ == page.PageTypeLeaf && leafLog != nil {
				b.SetPageID(0)
			} else {
				pid, err := alloc.Alloc(0)
				if err != nil {
					return err
				}
				b.SetPageID(pid)
			}
			levels = append(levels, &levelBuilder{
				builder:  b,
				startKey: nil,
			})
		}
		return nil
	}

	resetLevelBuilder := func(lvl int) error {
		lb := levels[lvl]
		typ := page.PageTypeInternal
		var buf []byte
		if lvl == 0 {
			typ = page.PageTypeLeaf
			buf = allocLeafBuf()
		} else {
			buf = make([]byte, page.PageSize)
		}
		lb.builder = newBuilder(buf, typ)
		if typ == page.PageTypeLeaf && leafLog != nil {
			lb.builder.SetPageID(0)
		} else {
			pid, err := alloc.Alloc(0)
			if err != nil {
				return err
			}
			lb.builder.SetPageID(pid)
		}
		lb.startKey = nil
		return nil
	}

	// flush finishes the current node at lvl, writes it, and promotes to lvl+1.
	// It replaces the builder at lvl with a new one.
	var flush func(int) error
	promoteChild := func(lvl int, key []byte, childRef page.ChildRef) error {
		if err := ensureLevel(lvl + 1); err != nil {
			return err
		}
		parent := levels[lvl+1]
		if key == nil {
			key = []byte{}
		}
		if parent.startKey == nil {
			parent.startKey = append([]byte(nil), key...)
		}
		err := parent.builder.AddInternalChildRef(key, childRef)
		if err == node.ErrNodeFull {
			if err := flush(lvl + 1); err != nil {
				return err
			}
			parent = levels[lvl+1]
			if parent.startKey == nil {
				parent.startKey = append([]byte(nil), key...)
			}
			if err := parent.builder.AddInternalChildRef(key, childRef); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		return nil
	}
	drainInFlightLeafPages := func() error {
		if inFlightSlabIdx < 0 {
			return nil
		}
		res := <-inFlightCh
		s := leafSlabs[inFlightSlabIdx]
		inFlightSlabIdx = -1
		if res.err != nil {
			s.reset()
			return res.err
		}
		if len(res.ptrs) != s.count {
			n := s.count
			s.reset()
			return fmt.Errorf("bulk: leaf page batch returned %d ptrs for %d pages", len(res.ptrs), n)
		}
		for i, ptr := range res.ptrs {
			if err := promoteChild(0, s.keys[i], page.LeafLogChildRef(ptr)); err != nil {
				s.reset()
				return err
			}
		}
		s.reset()
		return nil
	}
	flushPendingLeafPages := func(async bool) error {
		s := leafSlabs[activeSlabIdx]
		if s == nil || s.count == 0 {
			if !async {
				return drainInFlightLeafPages()
			}
			return nil
		}
		if err := drainInFlightLeafPages(); err != nil {
			return err
		}
		pages := s.pageSlices[:s.count]
		if async && canPipelineLeafBatches {
			if inFlightCh == nil {
				inFlightCh = make(chan leafBatchResult, 1)
			}
			inFlightSlabIdx = activeSlabIdx
			nextIdx := activeSlabIdx ^ 1
			if leafSlabs[nextIdx] == nil {
				leafSlabs[nextIdx] = acquireBulkLeafBatchSlab()
			}
			activeSlabIdx = nextIdx
			go func(batchPages [][]byte) {
				ptrs, err := batcher.AppendLeafPages(batchPages)
				inFlightCh <- leafBatchResult{ptrs: ptrs, err: err}
			}(pages)
			return nil
		}
		var ptrs []page.LeafLogPtr
		if hasBatcher {
			var err error
			ptrs, err = batcher.AppendLeafPages(pages)
			if err != nil {
				s.reset()
				return err
			}
		} else {
			ptrs = make([]page.LeafLogPtr, len(pages))
			for i, leafPage := range pages {
				ptr, err := leafLog.AppendLeafPage(leafPage)
				if err != nil {
					s.reset()
					return err
				}
				ptrs[i] = ptr
			}
		}
		if len(ptrs) != s.count {
			n := s.count
			s.reset()
			return fmt.Errorf("bulk: leaf page batch returned %d ptrs for %d pages", len(ptrs), n)
		}
		for i, ptr := range ptrs {
			if err := promoteChild(0, s.keys[i], page.LeafLogChildRef(ptr)); err != nil {
				s.reset()
				return err
			}
		}
		s.reset()
		return nil
	}
	appendPendingLeafPage := func(key, leafData []byte) {
		s := leafSlabs[activeSlabIdx]
		off := len(s.keyArena)
		s.keyArena = append(s.keyArena, key...)
		s.keys[s.count] = s.keyArena[off:len(s.keyArena):len(s.keyArena)]
		s.pageSlices[s.count] = leafData
		s.count++
	}
	flush = func(lvl int) error {
		lb := levels[lvl]
		// An entry that cannot fit an empty page cannot make progress by promotion.
		if lb.builder.Count() == 0 {
			return node.ErrNodeFull
		}
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		key := lb.startKey
		if key == nil {
			key = []byte{}
		}
		if lvl == 0 && leafLog != nil {
			if err := countPage(); err != nil {
				return err
			}
			appendPendingLeafPage(key, n.Data())
			if leafSlabs[activeSlabIdx].count >= valuelog.MaxFrameK {
				if err := flushPendingLeafPages(true); err != nil {
					return err
				}
			}
			return resetLevelBuilder(lvl)
		}
		childRef := page.PageChildRef(childID)
		if err := countPage(); err != nil {
			return err
		}
		if err := p.Write(childID, n.Data()); err != nil {
			return err
		}
		if err := promoteChild(lvl, key, childRef); err != nil {
			return err
		}
		return resetLevelBuilder(lvl)
	}

	emitted := false
	emitEntry := func(key, val []byte, ptr page.ValuePtr, flags byte, revision page.EntryRevision) error {
		if !emitted {
			if err := ensureLevel(0); err != nil {
				return err
			}
			emitted = true
		}
		lb := levels[0]
		if lb.startKey == nil {
			lb.startKey = append([]byte(nil), key...)
		}

		err := lb.builder.AddLeafEntryWithRevision(key, val, flags, ptr, revision)
		if err == node.ErrNodeFull {
			if err := flush(0); err != nil {
				return err
			}
			lb = levels[0]
			if lb.startKey == nil {
				lb.startKey = append([]byte(nil), key...)
			}
			err = lb.builder.AddLeafEntryWithRevision(key, val, flags, ptr, revision)
		}
		return err
	}
	if err := scan(emitEntry); err != nil {
		return 0, err
	}
	if !emitted {
		// Empty tree: return a new empty root.
		if err := countPage(); err != nil {
			return 0, err
		}
		buf := make([]byte, page.PageSize)
		b := newLeafBuilder(buf, opts)
		if leafLog != nil {
			b.SetPageID(0)
			n := b.Finish()
			ptr, err := leafLog.AppendLeafPage(n.Data())
			if err != nil {
				return 0, err
			}
			if err := countPage(); err != nil {
				return 0, err
			}
			return buildSingleLeafLogRoot(alloc, p, []byte{}, ptr)
		}

		rootID, err := alloc.Alloc(0)
		if err != nil {
			return 0, err
		}
		b.SetPageID(rootID)
		n := b.Finish()
		if err := p.Write(rootID, n.Data()); err != nil {
			return 0, err
		}
		return rootID, nil
	}

	// Finalize all levels
	currID := uint64(0)
	currRef := page.ChildRef{}
	for i := 0; i < len(levels); i++ {
		lb := levels[i]
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		key := lb.startKey
		if key == nil {
			key = []byte{}
		}
		if i == 0 && leafLog != nil {
			if err := countPage(); err != nil {
				return 0, err
			}
			appendPendingLeafPage(key, n.Data())
			if err := flushPendingLeafPages(false); err != nil {
				return 0, err
			}
			continue
		}

		childRef := page.PageChildRef(childID)
		if err := countPage(); err != nil {
			return 0, err
		}
		if err := p.Write(childID, n.Data()); err != nil {
			return 0, err
		}
		currID = childID
		currRef = childRef

		// If this is not the top level, add to parent.
		if i < len(levels)-1 {
			if err := promoteChild(i, key, currRef); err != nil {
				return 0, err
			}
		}
	}

	// Reduce root if possible
	if len(levels) > 1 {
		root := levels[len(levels)-1].builder.Finish()
		if root.Count() == 1 {
			childRef, err := root.GetInternalChildRef(0)
			if err == nil && childRef.Kind == page.ChildRefPage {
				return childRef.Page, nil
			}
		}
	}

	if leafLog != nil && currRef.Kind == page.ChildRefLeafLog {
		key := levels[0].startKey
		if key == nil {
			key = []byte{}
		}
		if err := countPage(); err != nil {
			return 0, err
		}
		return buildSingleLeafLogRoot(alloc, p, key, currRef.Log)
	}

	return currID, nil
}

func buildSingleLeafLogRoot(alloc Allocator, p *pager.Pager, key []byte, ptr page.LeafLogPtr) (uint64, error) {
	rootID, err := alloc.Alloc(0)
	if err != nil {
		return 0, err
	}
	buf, err := p.GetForWrite(rootID)
	if err != nil {
		return 0, err
	}
	b := node.NewBuilder(buf, page.PageTypeInternal)
	b.SetPageID(rootID)
	b.SetInternalFenceBounds(key, nil)
	if err := b.AddInternalChildRef(key, page.LeafLogChildRef(ptr)); err != nil {
		return 0, err
	}
	b.FinishNoNode()
	return rootID, nil
}

func leafBuilderOptions(opts BuildOptions) node.BuilderOptions {
	return node.BuilderOptions{LeafPrefixCompression: opts.LeafPrefixCompression, LeafColumnar: opts.LeafColumnar, InternalBaseDelta: opts.InternalBaseDelta, PackedValuePtr: opts.PackedValuePtr, EntryRevisions: opts.EntryRevisions}
}
func newLeafBuilder(buf []byte, opts BuildOptions) *node.Builder {
	return node.NewBuilderWithOptions(buf, page.PageTypeLeaf, leafBuilderOptions(opts))
}
