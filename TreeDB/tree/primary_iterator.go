package tree

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// primaryIterator merges the independently complete directory with its base.
// Its memory is constant in the number of base rows. The directory is borrowed
// from the same captured index owner as the base, never from a current head.
type primaryIterator struct {
	baseTree                                     Tree
	base                                         *Iterator
	delta                                        Iterator
	slot                                         [1]CursorItem
	directory                                    node.PrimaryDirectoryView
	start, end                                   []byte
	opts                                         IteratorOptions
	reverse, selectedDelta, equal, valid, closed bool
	position                                     int
	inlineRevision                               page.EntryRevision
	err                                          error
	readLeaf                                     func(page.LeafLogPtr, []byte) ([]byte, error)
	leafScratch                                  []byte
	bounded                                      bool
}

func (t *Tree) initPrimaryIterator(p *primaryIterator, base *Iterator, start, end []byte, opts IteratorOptions, reverse bool, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), leafScratch []byte, bounded bool) (bool, error) {
	var root node.Node
	if err := t.loadNodeViewWithLoadKindInto(&root, t.rootPageID, true, true); err != nil {
		return false, err
	}
	if root.Type() != page.PageTypePrimaryDirectory {
		return false, nil
	}
	directory, err := node.DecodePrimaryDirectory(root.Data())
	if err != nil {
		return true, err
	}
	op, _ := directory.Base()
	if err = t.validatePrimaryBase(root.Data()); err != nil {
		return true, err
	}
	*p = primaryIterator{baseTree: *t, base: base, directory: directory, start: start, end: end, opts: opts, reverse: reverse, readLeaf: readLeaf, leafScratch: leafScratch, bounded: bounded}
	p.baseTree.SetRoot(op.Ref.Page)
	p.base.tree = &p.baseTree
	p.Seek(nil)
	return true, nil
}

func (p *primaryIterator) Seek(key []byte) {
	if p.closed {
		return
	}
	p.err = nil
	p.valid = false
	p.equal = false
	p.base.Seek(key)
	p.position = 0
	if p.reverse {
		p.position = p.directory.Count() - 1
	}
	for p.position >= 0 && p.position < p.directory.Count() {
		e, _ := p.directory.Entry(p.position)
		fits := (p.start == nil || bytes.Compare(e.Key, p.start) >= 0) && (p.end == nil || bytes.Compare(e.Key, p.end) < 0)
		if key != nil {
			if p.reverse {
				fits = fits && bytes.Compare(e.Key, key) <= 0
			} else {
				fits = fits && bytes.Compare(e.Key, key) >= 0
			}
		}
		if fits {
			break
		}
		p.stepDelta()
	}
	p.choose()
}
func (p *primaryIterator) stepDelta() {
	if p.reverse {
		p.position--
	} else {
		p.position++
	}
}
func (p *primaryIterator) choose() {
	p.valid = false
	for {
		if p.base.Error() != nil {
			p.err = p.base.Error()
			return
		}
		hasDelta := p.position >= 0 && p.position < p.directory.Count()
		var e node.PrimaryDirectoryEntry
		if hasDelta {
			e, _ = p.directory.Entry(p.position)
			if (p.start != nil && bytes.Compare(e.Key, p.start) < 0) || (p.end != nil && bytes.Compare(e.Key, p.end) >= 0) {
				hasDelta = false
			}
		}
		if !hasDelta && !p.base.Valid() {
			return
		}
		p.selectedDelta = hasDelta
		p.equal = false
		if hasDelta && p.base.Valid() {
			cmp := bytes.Compare(e.Key, p.base.UnsafeKey())
			p.equal = cmp == 0
			p.selectedDelta = cmp <= 0
			if p.reverse {
				p.selectedDelta = cmp >= 0
			}
		}
		if !p.selectedDelta {
			p.valid = true
			return
		}
		p.inlineRevision = 0
		if e.InlineAbsence() {
			if !p.opts.IncludeTombstones {
				if p.equal {
					p.base.Next()
				}
				p.stepDelta()
				continue
			}
			p.inlineRevision = e.Revision
			p.delta = Iterator{tree: &p.baseTree, currKey: e.Key, flags: node.FlagTombstone, valid: true, ptrOK: true, valOK: true, mode: normalizeIteratorMode(p.opts.Mode)}
			p.valid = true
			return
		}
		var n node.Node
		if p.bounded && e.Operand.Ref.Kind == page.ChildRefLeafLog {
			if p.readLeaf == nil {
				p.err = ErrOwnedIteratorLeafReader
				return
			}
			data, err := p.readLeaf(e.Operand.Ref.Log, p.leafScratch[:0])
			if err != nil {
				p.err = err
				return
			}
			if len(data) != page.PageSize || cap(data) != page.PageSize || &data[0] != &p.leafScratch[:page.PageSize][0] {
				p.err = ErrOwnedIteratorLeafReader
				return
			}
			n = *node.NewNode(data)
		} else if err := p.baseTree.loadChildRefViewInto(&n, e.Operand.Ref, true, true); err != nil {
			p.err = err
			return
		}
		if err := node.ValidatePrimaryComponent(e, n.Data()); err != nil {
			p.err = err
			return
		}
		if e.Kind == node.PrimaryAbsence && !p.opts.IncludeTombstones {
			if p.equal {
				p.base.Next()
			}
			p.stepDelta()
			continue
		}
		k, _, _, flags, _, err := n.GetLeafEntryViewWithRevision(0)
		if err != nil {
			p.err = err
			return
		}
		p.slot[0] = CursorItem{PageID: e.Operand.Ref.Page, Ref: e.Operand.Ref, Node: n, Index: 0}
		p.delta = Iterator{tree: &p.baseTree, stack: p.slot[:], currKey: k, flags: flags, valid: true, ptrOK: flags&node.FlagPointer == 0, mode: normalizeIteratorMode(p.opts.Mode), slabAppender: p.base.slabAppender, slabKeyReader: p.base.slabKeyReader, slabKeyAppender: p.base.slabKeyAppender}
		p.valid = true
		return
	}
}
func (p *primaryIterator) current() *Iterator {
	if p.selectedDelta {
		return &p.delta
	}
	return p.base
}
func (p *primaryIterator) Valid() bool {
	return !p.closed && p.valid && p.err == nil && p.current().Error() == nil
}
func (p *primaryIterator) Next() {
	if !p.Valid() {
		panic("iterator invalid")
	}
	if p.selectedDelta {
		if p.equal {
			p.base.Next()
		}
		p.stepDelta()
	} else {
		p.base.Next()
	}
	p.choose()
}
func (p *primaryIterator) UnsafeKey() []byte {
	if !p.Valid() {
		return nil
	}
	return p.current().UnsafeKey()
}
func (p *primaryIterator) UnsafeValue() []byte {
	if !p.Valid() {
		return nil
	}
	return p.current().UnsafeValue()
}
func (p *primaryIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !p.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	return p.current().UnsafeEntry()
}
func (p *primaryIterator) UnsafeEntryWithRevision() ([]byte, page.ValuePtr, byte, page.EntryRevision) {
	if !p.Valid() {
		return nil, page.ValuePtr{}, 0, 0
	}
	if p.selectedDelta && p.inlineRevision != 0 {
		return nil, page.ValuePtr{}, node.FlagTombstone, p.inlineRevision
	}
	return p.current().UnsafeEntryWithRevision()
}
func (p *primaryIterator) Key() []byte                 { return p.UnsafeKey() }
func (p *primaryIterator) Value() []byte               { return p.UnsafeValue() }
func (p *primaryIterator) KeyCopy(dst []byte) []byte   { return append(dst[:0], p.Key()...) }
func (p *primaryIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], p.Value()...) }
func (p *primaryIterator) IsDeleted() bool             { return p.Valid() && p.current().IsDeleted() }
func (p *primaryIterator) Error() error {
	if p.err != nil {
		return p.err
	}
	if p.closed {
		return nil
	}
	return p.current().Error()
}
func (p *primaryIterator) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	p.valid = false
	return p.base.Close()
}
func (p *primaryIterator) Domain() ([]byte, []byte) { return p.start, p.end }

var _ iterator.UnsafeIterator = (*primaryIterator)(nil)
