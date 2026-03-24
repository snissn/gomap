package bulk

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
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
// via this interface and returns LeafRef ids (encoded from the returned
// ValuePtr) in internal children and as the tree root when the height is 1.
type LeafPageAppender interface {
	AppendLeafPage(leafPage []byte) (page.ValuePtr, error)
}

// LeafPageBatchAppender optionally appends multiple leaf pages at once while
// preserving append order. Builders use this only as an optimization; callers
// must preserve the exact one-page-per-record semantics of AppendLeafPage.
type LeafPageBatchAppender interface {
	AppendLeafPages(leafPages [][]byte) ([]page.ValuePtr, error)
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
	LeafPageLog           LeafPageAppender
}

// Build creates a new B-Tree from a sorted iterator.
func Build(iter iterator.UnsafeIterator, alloc Allocator, p *pager.Pager) (uint64, error) {
	return BuildWithOptions(iter, alloc, p, BuildOptions{})
}

// BuildWithOptions creates a new B-Tree from a sorted iterator with custom options.
func BuildWithOptions(iter iterator.UnsafeIterator, alloc Allocator, p *pager.Pager, opts BuildOptions) (uint64, error) {
	leafLog := opts.LeafPageLog
	leafBatchLog, _ := leafLog.(LeafPageBatchAppender)
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
			return page.EncodeLeafRef(ptr)
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

	type pendingLeafPage struct {
		startKey []byte
		data     []byte
	}
	const leafPageBatchFlushCount = 8
	var pendingLeafPages []pendingLeafPage

	resetLevel := func(lvl int) error {
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

	var flush func(int) error

	promoteChild := func(lvl int, key []byte, childID uint64) error {
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

		err := parent.builder.AddInternalChild(key, childID)
		if err == node.ErrNodeFull {
			if err := flush(lvl + 1); err != nil {
				return err
			}
			parent = levels[lvl+1]
			if parent.startKey == nil {
				parent.startKey = append([]byte(nil), key...)
			}
			if err := parent.builder.AddInternalChild(key, childID); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		return nil
	}

	flushPendingLeafPages := func() error {
		if leafBatchLog == nil || len(pendingLeafPages) == 0 {
			return nil
		}
		pages := make([][]byte, len(pendingLeafPages))
		for i := range pendingLeafPages {
			pages[i] = pendingLeafPages[i].data
		}
		ptrs, err := leafBatchLog.AppendLeafPages(pages)
		if err != nil {
			return err
		}
		if len(ptrs) != len(pendingLeafPages) {
			return fmt.Errorf("bulk: batched leaf page append count mismatch: got=%d want=%d", len(ptrs), len(pendingLeafPages))
		}
		for i := range pendingLeafPages {
			childID, err := page.EncodeLeafRef(ptrs[i])
			if err != nil {
				return err
			}
			if err := promoteChild(0, pendingLeafPages[i].startKey, childID); err != nil {
				return err
			}
		}
		pendingLeafPages = pendingLeafPages[:0]
		return nil
	}

	// flush finishes the current node at lvl, writes it, and promotes to lvl+1.
	// It replaces the builder at lvl with a new one.
	flush = func(lvl int) error {
		lb := levels[lvl]
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		if lvl == 0 && leafLog != nil {
			if leafBatchLog != nil {
				key := lb.startKey
				if key == nil {
					key = []byte{}
				}
				pendingLeafPages = append(pendingLeafPages, pendingLeafPage{
					startKey: append([]byte(nil), key...),
					data:     n.Data(),
				})
				if err := resetLevel(lvl); err != nil {
					return err
				}
				if len(pendingLeafPages) >= leafPageBatchFlushCount {
					return flushPendingLeafPages()
				}
				return nil
			}
			ptr, err := leafLog.AppendLeafPage(n.Data())
			if err != nil {
				return err
			}
			id, err := page.EncodeLeafRef(ptr)
			if err != nil {
				return err
			}
			childID = id
		} else {
			if err := p.Write(childID, n.Data()); err != nil {
				return err
			}
		}

		key := lb.startKey
		if err := promoteChild(lvl, key, childID); err != nil {
			return err
		}
		return resetLevel(lvl)
	}

	for iter.Valid() {
		key := iter.UnsafeKey()
		val, ptr, flags := iter.UnsafeEntry()

		lb := levels[0]
		if lb.startKey == nil {
			lb.startKey = append([]byte(nil), key...)
		}

		err := lb.builder.AddLeafEntry(key, val, flags, ptr)
		if err == node.ErrNodeFull {
			if err := flush(0); err != nil {
				return 0, err
			}
			lb = levels[0]
			if lb.startKey == nil {
				lb.startKey = append([]byte(nil), key...)
			}
			err = lb.builder.AddLeafEntry(key, val, flags, ptr)
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

	if err := flushPendingLeafPages(); err != nil {
		return 0, err
	}

	// Finalize all levels
	currID := uint64(0)
	for i := 0; i < len(levels); i++ {
		lb := levels[i]
		n := lb.builder.Finish()
		childID := lb.builder.PageID()
		if i == 0 && leafLog != nil {
			ptr, err := leafLog.AppendLeafPage(n.Data())
			if err != nil {
				return 0, err
			}
			id, err := page.EncodeLeafRef(ptr)
			if err != nil {
				return 0, err
			}
			childID = id
		} else {
			if err := p.Write(childID, n.Data()); err != nil {
				return 0, err
			}
		}
		currID = childID

		// If this is not the top level, add to parent
		if i < len(levels)-1 {
			parent := levels[i+1]
			key := lb.startKey
			if key == nil {
				key = []byte{}
			}
			if parent.startKey == nil {
				parent.startKey = append([]byte(nil), key...)
			}

			err := parent.builder.AddInternalChild(key, currID)
			if err == node.ErrNodeFull {
				if err := flush(i + 1); err != nil {
					return 0, err
				}
				parent = levels[i+1]
				if parent.startKey == nil {
					parent.startKey = append([]byte(nil), key...)
				}
				if err := parent.builder.AddInternalChild(key, currID); err != nil {
					return 0, err
				}
			} else if err != nil {
				return 0, err
			}
		}
	}

	// Reduce root if possible
	if len(levels) > 1 {
		root := levels[len(levels)-1].builder.Finish()
		if root.Count() == 1 {
			childID, err := root.GetInternalChildID(0)
			if err == nil {
				return childID, nil
			}
		}
	}

	return currID, nil
}

func newLeafBuilder(buf []byte, opts BuildOptions) *node.Builder {
	if opts.LeafPrefixCompression || opts.LeafColumnar || opts.InternalBaseDelta || opts.PackedValuePtr {
		return node.NewBuilderWithOptions(buf, page.PageTypeLeaf, node.BuilderOptions{
			LeafPrefixCompression: opts.LeafPrefixCompression,
			LeafColumnar:          opts.LeafColumnar,
			InternalBaseDelta:     opts.InternalBaseDelta,
			PackedValuePtr:        opts.PackedValuePtr,
		})
	}
	return node.NewBuilder(buf, page.PageTypeLeaf)
}
