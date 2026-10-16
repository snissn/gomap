package tree

import (
	"bytes"
	"crypto/sha256"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// NativeMVCCSelectionV6 borrows one ACTUAL completed physical projection.
// It grants no current-root mutation or floor permission. Keep the original
// snapshot and projection stationary through consumption; advancing, closing,
// or replacing its physical source invalidates the borrow.
type NativeMVCCSelectionV6 struct {
	projection *ChargedProjection
	record     node.LeafEntry
	leaf       page.ChildRef
	base       node.PrimaryOperand
	sequence   uint64
	delta      bool
	entry      node.PrimaryDirectoryEntry
	keyDigest  [32]byte
	position   int
}

func (s NativeMVCCSelectionV6) Record() node.LeafEntry       { return s.record }
func (s NativeMVCCSelectionV6) LeafReference() page.ChildRef { return s.leaf }
func (s NativeMVCCSelectionV6) DirectoryDelta() (node.PrimaryDirectoryEntry, bool) {
	return s.entry, s.delta
}
func (s NativeMVCCSelectionV6) BaseOperand() (node.PrimaryOperand, uint64) { return s.base, s.sequence }

func (c *ChargedProjection) NativeMVCCSelectionV6(w *iterator.OrdinalScanWork) (NativeMVCCSelectionV6, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(NativeMVCCSelectionV6{}))+uint64(len(c.record.Key))) {
		return NativeMVCCSelectionV6{}, false, nil
	}
	if c.closed || c.closing {
		return NativeMVCCSelectionV6{}, false, ErrProjectionClosed
	}
	if c.err != nil {
		return NativeMVCCSelectionV6{}, false, c.err
	}
	if c.pending || !c.valid {
		return NativeMVCCSelectionV6{}, false, nil
	}
	s := NativeMVCCSelectionV6{projection: c, record: c.record, delta: c.primary && c.selectedDelta, keyDigest: sha256.Sum256(c.record.Key)}
	if c.primary {
		s.base, s.sequence = c.directory.Base()
	}
	if s.delta {
		s.entry, s.leaf = c.entry, c.entry.Operand.Ref
	} else {
		if c.base.depth == 0 {
			return NativeMVCCSelectionV6{}, false, ErrProjectionOwner
		}
		s.leaf, s.position = c.base.stack[c.base.depth-1].Ref, c.base.stack[c.base.depth-1].Index
	}
	return s, true, nil
}

// MatchesNativeMVCCSelectionV6 rechecks this actual consumer's retained Tree
// identity and current record. Pointer identity of a foreign witness is never
// accepted as proof; no constructor or physical root is rediscovered.
func (t *Tree) MatchesNativeMVCCSelectionV6(s NativeMVCCSelectionV6, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || !w.Reserve(1, 2*uint64(unsafe.Sizeof(Tree{}))+2*uint64(unsafe.Sizeof(node.LeafEntry{}))+2*uint64(len(s.record.Key))) {
		return false, nil
	}
	c := s.projection
	if t == nil || c == nil {
		return false, ErrProjectionOwner
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.closing || c.pending || !c.valid || c.err != nil {
		return false, ErrProjectionOwner
	}
	if t.pager != c.tree.pager || t.rootPageID != c.tree.rootPageID || t.pageLimit != c.tree.pageLimit || t.pageNamespace != c.tree.pageNamespace || t.primaryLocal != c.tree.primaryLocal || c.record.Revision != s.record.Revision || c.record.Flags != s.record.Flags || c.record.ValuePtr != s.record.ValuePtr || !bytes.Equal(c.record.Key, s.record.Key) || sha256.Sum256(c.record.Key) != s.keyDigest {
		return false, ErrProjectionOwner
	}
	if s.delta != (c.primary && c.selectedDelta) {
		return false, ErrProjectionOwner
	}
	if s.delta {
		if s.leaf != c.entry.Operand.Ref || s.entry.Revision != c.entry.Revision || s.entry.ClassSlot != c.entry.ClassSlot {
			return false, ErrProjectionOwner
		}
	} else if c.base.depth == 0 || s.leaf != c.base.stack[c.base.depth-1].Ref || s.position != c.base.stack[c.base.depth-1].Index {
		return false, ErrProjectionOwner
	}
	return true, nil
}
