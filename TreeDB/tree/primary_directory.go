package tree

import (
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// selectPrimaryOperand interprets the complete top-level operand. It never
// follows a previous publication. Every selected component binds native bytes
// as well as its exact key, revision and put/absence class.
func (t *Tree) selectPrimaryOperand(data, key []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), leafScratch []byte, bounded bool) (page.ChildRef, node.PrimaryDirectoryEntry, error) {
	directory, err := node.DecodePrimaryDirectory(data)
	if err != nil {
		return page.ChildRef{}, node.PrimaryDirectoryEntry{}, err
	}
	op, _ := directory.Base()
	entry, found := directory.Search(key)
	if found {
		if entry.InlineAbsence() {
			return page.ChildRef{}, entry, nil
		}
		op = entry.Operand
	}
	var n node.Node
	if bounded && op.Ref.Kind == page.ChildRefLeafLog {
		if readLeaf == nil {
			return page.ChildRef{}, node.PrimaryDirectoryEntry{}, ErrOwnedIteratorLeafReader
		}
		image, e := readLeaf(op.Ref.Log, leafScratch[:0])
		if e != nil {
			return page.ChildRef{}, node.PrimaryDirectoryEntry{}, e
		}
		if len(image) != page.PageSize || cap(image) != page.PageSize || &image[0] != &leafScratch[:page.PageSize][0] {
			return page.ChildRef{}, node.PrimaryDirectoryEntry{}, ErrOwnedIteratorLeafReader
		}
		n = *node.NewNode(image)
	} else if err = t.loadChildRefViewInto(&n, op.Ref, true, false); err != nil {
		return page.ChildRef{}, node.PrimaryDirectoryEntry{}, err
	}
	if !node.VerifyPrimaryOperand(op, n.Data()) {
		return page.ChildRef{}, node.PrimaryDirectoryEntry{}, node.ErrPrimaryDirectory
	}
	if found {
		if err = node.ValidatePrimaryComponent(entry, n.Data()); err != nil {
			return page.ChildRef{}, node.PrimaryDirectoryEntry{}, err
		}
	} else if n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal {
		return page.ChildRef{}, node.PrimaryDirectoryEntry{}, node.ErrPrimaryDirectory
	}
	return op.Ref, node.PrimaryDirectoryEntry{}, nil
}

func (t *Tree) validatePrimaryBase(data []byte) error {
	directory, err := node.DecodePrimaryDirectory(data)
	if err != nil {
		return err
	}
	op, _ := directory.Base()
	var n node.Node
	if err = t.loadChildRefViewInto(&n, op.Ref, true, false); err != nil {
		return err
	}
	if !node.VerifyPrimaryOperand(op, n.Data()) || (n.Type() != page.PageTypeLeaf && n.Type() != page.PageTypeInternal) {
		return node.ErrPrimaryDirectory
	}
	return nil
}
