package tree

import (
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type successorFrame struct {
	ref  page.ChildRef
	next uint16
}

// OwnedSuccessorScratch is caller-admitted storage, never pooled or retained by
// the tree. Compact ancestors reload immutable parent pages at leaf boundaries.
type OwnedSuccessorScratch struct {
	frames [maxTraversalDepth]successorFrame
}

func (t *Tree) loadReadRefInto(n *node.Node, ref page.ChildRef, verify bool, keyScratch, leafScratch []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), bounded bool) error {
	if bounded && ref.Kind == page.ChildRefLeafLog {
		if readLeaf == nil {
			return ErrOwnedIteratorLeafReader
		}
		data, err := readLeaf(ref.Log, leafScratch[:0])
		if err != nil {
			return err
		}
		if len(data) != page.PageSize || cap(data) != page.PageSize || &data[0] != &leafScratch[:page.PageSize][0] {
			return ErrOwnedIteratorLeafReader
		}
		if err = validateLeafLogNodeInto(n, data, ref.Log, t.shouldVerifyLeafRefChecksum(), false); err != nil {
			return err
		}
	} else if err := t.loadChildRefViewInto(n, ref, verify, false); err != nil {
		return err
	}
	if bounded && keyScratch != nil {
		n.SetFixedKeyScratch(keyScratch)
	}
	return nil
}

// SeekGEWithFixedScratch returns borrowed metadata from this exact captured tree.
// It includes physical tombstones so the existing cache merge owns suppression.
// Every scratch backing must be admitted and exclusively owned by the caller.
func (t *Tree) SeekGEWithFixedScratch(start, end, keyScratch, leafScratch []byte, scratch *OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	if scratch == nil || cap(keyScratch) < page.PageSize || cap(leafScratch) < page.PageSize {
		return node.LeafEntry{}, false, node.ErrCorruptedNode
	}
	if start != nil && end != nil && compareTreeKey(start, end) >= 0 {
		return node.LeafEntry{}, false, nil
	}
	var root node.Node
	if err := t.loadReadRefInto(&root, page.PageChildRef(t.rootPageID), true, keyScratch, leafScratch, readLeaf, true); err != nil {
		return node.LeafEntry{}, false, err
	}
	rootRef := page.PageChildRef(t.rootPageID)
	return t.seekBaseGE(rootRef, start, end, keyScratch, leafScratch, scratch, readLeaf)
}

func (t *Tree) seekBaseGE(ref page.ChildRef, start, end, keyScratch, leafScratch []byte, scratch *OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	depth := 0
	leftmost := false
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
			var index uint16
			if !leftmost && start != nil {
				index, _ = n.SearchInternal(start)
			}
			child, err := n.GetInternalChildRef(index)
			if err != nil {
				return node.LeafEntry{}, false, err
			}
			scratch.frames[depth] = successorFrame{ref: ref, next: index + 1}
			depth++
			ref = child
			continue
		case page.PageTypeLeaf:
			var index uint16
			if !leftmost && start != nil {
				var err error
				index, _, err = n.SearchLeaf(start)
				if err != nil {
					return node.LeafEntry{}, false, err
				}
			}
			if index < n.Count() {
				k, v, p, f, r, err := n.GetLeafEntryViewWithRevision(index)
				if err != nil {
					return node.LeafEntry{}, false, err
				}
				if end != nil && compareTreeKey(k, end) >= 0 {
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
			if frame.next < parent.Count() {
				var err error
				ref, err = parent.GetInternalChildRef(frame.next)
				if err != nil {
					return node.LeafEntry{}, false, err
				}
				frame.next++
				leftmost = true
				advanced = true
				break
			}
			depth--
		}
		if !advanced {
			return node.LeafEntry{}, false, nil
		}
	}
}
