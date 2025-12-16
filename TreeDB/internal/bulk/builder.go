package bulk

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

type Allocator interface {
	Alloc() (uint64, error)
}

type levelBuilder struct {
	builder  *node.Builder
	startKey []byte
}

// Build creates a new B-Tree from a sorted iterator.
func Build(iter iterator.UnsafeIterator, alloc Allocator, p *pager.Pager) (uint64, error) {
	if !iter.Valid() {
		// Empty tree? Return a new empty root.
		rootID, err := alloc.Alloc()
		if err != nil {
			return 0, err
		}
		// Write empty leaf
		buf := make([]byte, page.PageSize)
		b := node.NewBuilder(buf, page.PageTypeLeaf)
		b.SetPageID(rootID)
		n := b.Finish()
		if err := p.Write(rootID, n.Data()); err != nil {
			return 0, err
		}
		return rootID, nil
	}

	var levels []*levelBuilder

	ensureLevel := func(lvl int) error {
		for len(levels) <= lvl {
			pid, err := alloc.Alloc()
			if err != nil {
				return err
			}
			buf := make([]byte, page.PageSize)

			typ := page.PageTypeInternal
			if len(levels) == 0 {
				typ = page.PageTypeLeaf
			}

			b := node.NewBuilder(buf, typ)
			b.SetPageID(pid)
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

	// flush finishes the current node at lvl, writes it, and promotes to lvl+1.
	// It replaces the builder at lvl with a new one.
	var flush func(int) error
	flush = func(lvl int) error {
		lb := levels[lvl]
		n := lb.builder.Finish()
		if err := p.Write(lb.builder.PageID(), n.Data()); err != nil {
			return err
		}

		// Promote to parent
		if err := ensureLevel(lvl + 1); err != nil {
			return err
		}
		parent := levels[lvl+1]

		// The key to promote is the startKey of the node we just finished.
		// For leaf nodes, startKey was set when we added the first entry.
		// For internal nodes, startKey was set when we added the first child.
		key := lb.startKey
		if key == nil {
			// Should not happen for valid tree
			key = []byte{}
		}
		childID := lb.builder.PageID()

		// Add to parent
		if parent.startKey == nil {
			parent.startKey = append([]byte(nil), key...)
		}

		err := parent.builder.AddInternalChild(key, childID)
		if err == node.ErrNodeFull {
			// Parent full. Flush parent recursively.
			if err := flush(lvl + 1); err != nil {
				return err
			}
			// Retry add to new parent
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

		// Reset current level builder
		pid, err := alloc.Alloc()
		if err != nil {
			return err
		}
		buf := make([]byte, page.PageSize)
		typ := page.PageTypeInternal
		if lvl == 0 {
			typ = page.PageTypeLeaf
		}
		lb.builder = node.NewBuilder(buf, typ)
		lb.builder.SetPageID(pid)
		lb.startKey = nil

		return nil
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
			if err := lb.builder.AddLeafEntry(key, val, flags, ptr); err != nil {
				return 0, err
			}
		} else if err != nil {
			return 0, err
		}
		iter.Next()
	}

	// Finalize all levels
	currID := uint64(0)
	for i := 0; i < len(levels); i++ {
		lb := levels[i]
		n := lb.builder.Finish()
		if err := p.Write(lb.builder.PageID(), n.Data()); err != nil {
			return 0, err
		}
		currID = lb.builder.PageID()

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
