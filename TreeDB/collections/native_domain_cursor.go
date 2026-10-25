package collections

import (
	"bytes"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeDomainCursor borrows the same exact domain/snapshot as nativeDomainRead.
// Its containing storage, bounds and physical projection are admitted by the
// enclosing operation. Keep this value at a stable address and close it before
// releasing that operation's read/capture/receipt. Memory probes retain the
// existing allocation-free source traversal; no additional source pins exist.
type nativeDomainCursor struct {
	read       nativeDomainRead
	start, end []byte
	physical   iterator.UnsafeIterator
}

// The caller has inspected the physical source BEFORE this method, including
// empty or shadowed sources. The owned constructor eagerly seeks. Sequential
// movement consumes at most the prior physical candidate; a larger candidate
// survives memory-only winners. Arbitrary Seek resets the physical traversal.
func (c *nativeDomainCursor) physicalEntry(start []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error), forward bool, admit func(uint64) error, inspect func() error) (node.LeafEntry, bool, error) {
	if c.physical == nil {
		if err := admit(memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize())); err != nil {
			return node.LeafEntry{}, false, err
		}
		it, err := c.read.base.OwnedPointerProjectionIterator(c.start, c.end, readLeaf)
		if err != nil {
			return node.LeafEntry{}, false, err
		}
		c.physical = it
		if !bytes.Equal(start, c.start) {
			if inspect != nil {
				if err := inspect(); err != nil {
					return node.LeafEntry{}, false, err
				}
			}
			it.Seek(start)
		}
	} else if forward {
		if c.physical.Valid() && bytes.Compare(c.physical.UnsafeKey(), start) < 0 {
			c.physical.Next()
		}
	} else {
		c.physical.Seek(start)
	}
	it := c.physical
	if err := it.Error(); err != nil {
		return node.LeafEntry{}, false, err
	}
	if !it.Valid() {
		return node.LeafEntry{}, false, nil
	}
	value, ptr, flags, revision := iterator.UnsafeEntryWithRevision(it)
	if err := it.Error(); err != nil {
		return node.LeafEntry{}, false, err
	}
	return node.LeafEntry{Key: it.UnsafeKey(), Value: value, ValuePtr: ptr, Flags: flags, Revision: revision}, true, nil
}

func (c *nativeDomainCursor) close() error {
	var err error
	if c.physical != nil {
		err = c.physical.Close()
	}
	*c = nativeDomainCursor{}
	return err
}
