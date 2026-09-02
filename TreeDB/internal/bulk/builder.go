package bulk

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

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
}

// Build creates a new B-Tree from a sorted iterator.
func Build(iter iterator.UnsafeIterator, alloc Allocator, p *pager.Pager) (uint64, error) {
	return BuildWithOptions(iter, alloc, p, BuildOptions{})
}

// BuildWithOptions creates a new B-Tree from a sorted iterator with custom options.
func BuildWithOptions(iter iterator.UnsafeIterator, alloc Allocator, p *pager.Pager, opts BuildOptions) (uint64, error) {
	leafLog := opts.LeafPageLog
	if !iter.Valid() {
		// Empty tree? Return a new empty root.
		buf := make([]byte, page.PageSize)
		b := newLeafBuilder(buf, opts)
		if leafLog != nil {
			b.SetPageID(0)
			n := b.Finish()
			ptr, err := leafLog.AppendLeafPage(n.Data())
			if err != nil {
				return 0, err
			}
			return buildSingleLeafLogRoot(alloc, p, []byte{}, ptr)
		}

		rootID, err := alloc.Alloc(0)
		if err != nil {
			return 0, err
		}
		// Write empty leaf
		b.SetPageID(rootID)
		n := b.Finish()
		if err := p.Write(rootID, n.Data()); err != nil {
			return 0, err
		}
		return rootID, nil
	}

	var levels []*levelBuilder

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
			buf := make([]byte, page.PageSize)

			typ := page.PageTypeInternal
			if len(levels) == 0 {
				typ = page.PageTypeLeaf
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

	if err := ensureLevel(0); err != nil {
		return 0, err
	}

	type pendingLeafPagePromotion struct {
		key  []byte
		page []byte
	}
	var pendingLeafPages []pendingLeafPagePromotion

	resetLevelBuilder := func(lvl int) error {
		lb := levels[lvl]
		buf := make([]byte, page.PageSize)
		typ := page.PageTypeInternal
		if lvl == 0 {
			typ = page.PageTypeLeaf
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
	flushPendingLeafPages := func() error {
		if len(pendingLeafPages) == 0 {
			return nil
		}
		pages := make([][]byte, len(pendingLeafPages))
		for i := range pendingLeafPages {
			pages[i] = pendingLeafPages[i].page
		}
		var ptrs []page.LeafLogPtr
		if batcher, ok := leafLog.(LeafPageBatchAppender); ok {
			var err error
			ptrs, err = batcher.AppendLeafPages(pages)
			if err != nil {
				return err
			}
		} else {
			ptrs = make([]page.LeafLogPtr, len(pages))
			for i, leafPage := range pages {
				ptr, err := leafLog.AppendLeafPage(leafPage)
				if err != nil {
					return err
				}
				ptrs[i] = ptr
			}
		}
		if len(ptrs) != len(pendingLeafPages) {
			return fmt.Errorf("bulk: leaf page batch returned %d ptrs for %d pages", len(ptrs), len(pendingLeafPages))
		}
		for i, ptr := range ptrs {
			if err := promoteChild(0, pendingLeafPages[i].key, page.LeafLogChildRef(ptr)); err != nil {
				return err
			}
		}
		for i := range pendingLeafPages {
			pendingLeafPages[i] = pendingLeafPagePromotion{}
		}
		pendingLeafPages = pendingLeafPages[:0]
		return nil
	}
	flush = func(lvl int) error {
		lb := levels[lvl]
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		key := lb.startKey
		if key == nil {
			key = []byte{}
		}
		if lvl == 0 && leafLog != nil {
			pendingLeafPages = append(pendingLeafPages, pendingLeafPagePromotion{
				key:  append([]byte(nil), key...),
				page: n.Data(),
			})
			if len(pendingLeafPages) >= valuelog.MaxFrameK {
				if err := flushPendingLeafPages(); err != nil {
					return err
				}
			}
			return resetLevelBuilder(lvl)
		}
		childRef := page.PageChildRef(childID)
		if err := p.Write(childID, n.Data()); err != nil {
			return err
		}
		if err := promoteChild(lvl, key, childRef); err != nil {
			return err
		}
		return resetLevelBuilder(lvl)
	}

	for iter.Valid() {
		key := iter.UnsafeKey()
		val, ptr, flags, revision := iterator.UnsafeEntryWithRevision(iter)

		lb := levels[0]
		if lb.startKey == nil {
			lb.startKey = append([]byte(nil), key...)
		}

		err := lb.builder.AddLeafEntryWithRevision(key, val, flags, ptr, revision)
		if err == node.ErrNodeFull {
			if err := flush(0); err != nil {
				return 0, err
			}
			lb = levels[0]
			if lb.startKey == nil {
				lb.startKey = append([]byte(nil), key...)
			}
			err = lb.builder.AddLeafEntryWithRevision(key, val, flags, ptr, revision)
		}

		if err != nil {
			return 0, err
		}

		if !iter.Valid() {
			if err := iter.Error(); err != nil {
				return 0, err
			}
			break
		}
		iter.Next()
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
			pendingLeafPages = append(pendingLeafPages, pendingLeafPagePromotion{
				key:  append([]byte(nil), key...),
				page: n.Data(),
			})
			if err := flushPendingLeafPages(); err != nil {
				return 0, err
			}
			continue
		}

		childRef := page.PageChildRef(childID)
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

func newLeafBuilder(buf []byte, opts BuildOptions) *node.Builder {
	if opts.LeafPrefixCompression || opts.LeafColumnar || opts.InternalBaseDelta || opts.PackedValuePtr || opts.EntryRevisions {
		return node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: opts.LeafPrefixCompression,
			LeafColumnar:          opts.LeafColumnar,
			InternalBaseDelta:     opts.InternalBaseDelta,
			PackedValuePtr:        opts.PackedValuePtr,
			EntryRevisions:        opts.EntryRevisions,
		})
	}
	return node.NewBuilder(buf, page.PageTypeLeaf)
}
