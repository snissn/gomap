package tree

import (
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// SeekLTWithFixedScratch returns the greatest DATA entry in [start,end).
// Like SeekGE it includes tombstones, borrows the exact immutable tree and
// caller-admitted scratch, and reloads compact ancestor frames at boundaries.
// It creates no iterator, page cache, PRIMARY interpretation, or owning edge.
func (t *Tree) SeekLTWithFixedScratch(start, end, keyScratch, leafScratch []byte, scratch *OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	if scratch == nil || cap(keyScratch) < page.PageSize || cap(leafScratch) < page.PageSize {
		return node.LeafEntry{}, false, node.ErrCorruptedNode
	}
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return node.LeafEntry{}, false, nil
	}
	ref := page.PageChildRef(t.rootPageID)
	depth := 0
	rightmost := end == nil
	for {
		if depth >= maxTraversalDepth {
			return node.LeafEntry{}, false, node.ErrCorruptedNode
		}
		var n node.Node
		if err := t.loadReadRefInto(&n, ref, true, keyScratch, leafScratch, readLeaf, true); err != nil {
			return node.LeafEntry{}, false, err
		}
		switch n.Type() {
		case page.PageTypeInternal:
			if n.Count() == 0 {
				return node.LeafEntry{}, false, node.ErrCorruptedNode
			}
			index := n.Count() - 1
			if !rightmost {
				index, _ = n.SearchInternal(end)
			}
			child, err := n.GetInternalChildRef(index)
			if err != nil {
				return node.LeafEntry{}, false, err
			}
			// next is the number of children preceding the selected child.
			scratch.frames[depth] = successorFrame{ref: ref, next: index}
			depth++
			ref = child
			continue
		case page.PageTypeLeaf:
			index := n.Count()
			if !rightmost {
				var err error
				index, _, err = n.SearchLeaf(end)
				if err != nil {
					return node.LeafEntry{}, false, err
				}
			}
			if index > 0 {
				k, v, p, f, r, err := n.GetLeafEntryViewWithRevision(index - 1)
				if err != nil {
					return node.LeafEntry{}, false, err
				}
				if start != nil && compareTreeKey(k, start) < 0 {
					return node.LeafEntry{}, false, nil
				}
				return node.LeafEntry{Key: k, Value: v, ValuePtr: p, Flags: f, Revision: r}, true, nil
			}
		default:
			return node.LeafEntry{}, false, page.ErrInvalidPageType
		}
		advanced := false
		for depth > 0 {
			frame := &scratch.frames[depth-1]
			var parent node.Node
			if err := t.loadReadRefInto(&parent, frame.ref, true, keyScratch, leafScratch, readLeaf, true); err != nil {
				return node.LeafEntry{}, false, err
			}
			if parent.Type() != page.PageTypeInternal {
				return node.LeafEntry{}, false, node.ErrCorruptedNode
			}
			if frame.next > 0 {
				frame.next--
				var err error
				ref, err = parent.GetInternalChildRef(frame.next)
				if err != nil {
					return node.LeafEntry{}, false, err
				}
				rightmost, advanced = true, true
				break
			}
			depth--
		}
		if !advanced {
			return node.LeafEntry{}, false, nil
		}
	}
}
