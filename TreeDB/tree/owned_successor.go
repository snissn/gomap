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
	frames    [maxTraversalDepth]successorFrame
	batch     bool
	batchTree *Tree
	batchLeaf []byte
}

// BeginLeafBatch enables reuse only until EndLeafBatch on this exclusively
// borrowed scratch. No leaf/keys may escape their original tree/page owner.
func (s *OwnedSuccessorScratch) BeginLeafBatch() {
	s.batch = true
	s.batchTree = nil
	s.batchLeaf = nil
}
func (s *OwnedSuccessorScratch) EndLeafBatch() { s.batch = false; s.batchTree = nil; s.batchLeaf = nil }

// batchEntry can certify a successor only inside this page's complete physical
// key extent. Outside it, the original root traversal must decide gaps/absence.
func (t *Tree) batchEntry(start, end, keyScratch []byte, s *OwnedSuccessorScratch) (node.LeafEntry, bool, bool, error) {
	if !s.batch || s.batchTree != t || len(s.batchLeaf) == 0 || start == nil {
		return node.LeafEntry{}, false, false, nil
	}
	n := node.NewNodeView(s.batchLeaf)
	n.SetFixedKeyScratch(keyScratch)
	if n.Type() != page.PageTypeLeaf || n.Count() == 0 {
		return node.LeafEntry{}, false, false, nil
	}
	first, _, _, _, _, err := n.GetLeafEntryViewWithRevision(0)
	if err != nil {
		return node.LeafEntry{}, false, true, err
	}
	if compareTreeKey(start, first) < 0 {
		return node.LeafEntry{}, false, false, nil
	}
	last, _, _, _, _, err := n.GetLeafEntryViewWithRevision(n.Count() - 1)
	if err != nil {
		return node.LeafEntry{}, false, true, err
	}
	if compareTreeKey(start, last) > 0 {
		return node.LeafEntry{}, false, false, nil
	}
	index, _, err := n.SearchLeaf(start)
	if err != nil {
		return node.LeafEntry{}, false, true, err
	}
	if index >= n.Count() {
		return node.LeafEntry{}, false, true, node.ErrCorruptedNode
	}
	k, v, p, f, r, err := n.GetLeafEntryViewWithRevision(index)
	if err != nil {
		return node.LeafEntry{}, false, true, err
	}
	if end != nil && compareTreeKey(k, end) >= 0 {
		return node.LeafEntry{}, false, true, nil
	}
	return node.LeafEntry{Key: k, Value: v, ValuePtr: p, Flags: f, Revision: r}, true, true, nil
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
	if entry, found, eligible, err := t.batchEntry(start, end, keyScratch, scratch); eligible {
		return entry, found, err
	}
	// Any new traversal can overwrite the page/key scratch, including PRIMARY.
	scratch.batchTree, scratch.batchLeaf = nil, nil
	var root node.Node
	if err := t.loadReadRefInto(&root, page.PageChildRef(t.rootPageID), true, keyScratch, leafScratch, readLeaf, true); err != nil {
		return node.LeafEntry{}, false, err
	}
	if root.Type() != page.PageTypePrimaryDirectory {
		return t.seekBaseGE(&root, page.PageChildRef(t.rootPageID), start, end, keyScratch, leafScratch, scratch, readLeaf, true)
	}
	return t.seekPrimaryGE(root.Data(), start, end, keyScratch, leafScratch, scratch, readLeaf)
}

// Keep PRIMARY validation/merge state off the ordinary successor frame. The
// directory bytes remain borrowed from the captured immutable tree owner.
func (t *Tree) seekPrimaryGE(rootData, start, end, keyScratch, leafScratch []byte, scratch *OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, bool, error) {
	directory, err := node.DecodePrimaryDirectory(rootData)
	if err != nil {
		return node.LeafEntry{}, false, err
	}
	// The same complete base/digest validation used by ordinary PRIMARY reads.
	base, _ := directory.Base()
	var n node.Node
	if err = t.loadReadRefInto(&n, base.Ref, true, keyScratch, leafScratch, readLeaf, true); err != nil {
		return node.LeafEntry{}, false, err
	}
	if !node.VerifyPrimaryOperand(base, n.Data()) || (n.Type() != page.PageTypeInternal && n.Type() != page.PageTypeLeaf) {
		return node.LeafEntry{}, false, node.ErrPrimaryDirectory
	}
	best, found, err := t.seekBaseGE(&n, base.Ref, start, end, keyScratch, leafScratch, scratch, readLeaf, false)
	if err != nil {
		return best, found, err
	}
	// Directory order and equal-key precedence are the maintained PRIMARY merge
	// rule. Only the winning component is materialized and validated.
	lo, hi := 0, directory.Count()
	for lo < hi {
		mid := lo + (hi-lo)/2
		e, er := directory.Entry(mid)
		if er != nil {
			return node.LeafEntry{}, false, er
		}
		if start != nil && compareTreeKey(e.Key, start) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo >= directory.Count() {
		return best, found, nil
	}
	delta, err := directory.Entry(lo)
	if err != nil {
		return node.LeafEntry{}, false, err
	}
	if end != nil && compareTreeKey(delta.Key, end) >= 0 || found && compareTreeKey(delta.Key, best.Key) > 0 {
		return best, found, nil
	}
	if delta.InlineAbsence() {
		return node.LeafEntry{Key: delta.Key, Flags: node.FlagTombstone, Revision: delta.Revision}, true, nil
	}
	ref, _, err := t.selectPrimaryOperand(rootData, delta.Key, readLeaf, leafScratch, true)
	if err != nil {
		return node.LeafEntry{}, false, err
	}
	if err = t.loadReadRefInto(&n, ref, true, keyScratch, leafScratch, readLeaf, true); err != nil {
		return node.LeafEntry{}, false, err
	}
	k, v, p, f, r, err := n.GetLeafEntryViewWithRevision(0)
	return node.LeafEntry{Key: k, Value: v, ValuePtr: p, Flags: f, Revision: r}, err == nil, err
}

func (t *Tree) seekBaseGE(validatedRoot *node.Node, ref page.ChildRef, start, end, keyScratch, leafScratch []byte, scratch *OwnedSuccessorScratch, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), ordinary bool) (node.LeafEntry, bool, error) {
	depth := 0
	leftmost := false
	for {
		if depth >= maxTraversalDepth {
			return node.LeafEntry{}, false, node.ErrCorruptedNode
		}
		var loaded node.Node
		n := validatedRoot
		if n != nil {
			// The caller just validated this exact root in the same captured
			// tree. Consume its view once, before any child can overwrite the
			// caller's leaf scratch. Descendants and reloaded ancestors still
			// pass through the normal integrity checks.
			validatedRoot = nil
		} else {
			if err := t.loadReadRefInto(&loaded, ref, true, keyScratch, leafScratch, readLeaf, true); err != nil {
				return node.LeafEntry{}, false, err
			}
			n = &loaded
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
			if scratch.batch && ordinary && n.Count() > 0 {
				scratch.batchTree, scratch.batchLeaf = t, n.Data()
			}
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
