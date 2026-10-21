package memtable

import (
	"sync"
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

type COWTraversalWork = iterator.OrdinalScanWork
type COWTraversalOperation = btree.Operation

const (
	COWSeekGE = btree.SeekGE
	COWSeekLE = btree.SeekLE
	COWFirst  = btree.First
	COWLast   = btree.Last
	COWNext   = btree.Next
	COWPrev   = btree.Prev
)

// COWTraversal owns its immutable root and preadmitted cursor stack. It never
// captures another backend or builds an index. Record borrows generation-owned
// metadata; pointed payload validation is deliberately the caller's obligation.
// Each cursor serializes its requests; independent cursors can run concurrently.
type COWTraversal struct {
	mu                               sync.Mutex
	view                             *COWView
	cursor                           btree.StringCursor[cowValue]
	ordered                          cowOrderedTraversal
	hasOrdered                       bool
	pending, valid, rawSelected      bool
	runRaw, runOrdered               bool
	op                               COWTraversalOperation
	rawOp, orderedOp                 COWTraversalOperation
	direction                        int
	key                              string
	rawKey, orderedKey               string
	phase                            uint8
	compareOffset                    int
	rawRecord, orderedRecord, record COWRecord
	closing                          bool
}

// NewTraversal opens an UNPOSITIONED cursor. No height walk or cold seek runs:
// the maintained cardinality proves an upper stack bound for degree-32 trees.
// Allocation/zeroing and owner admission are charged before their effects.
// extraBytes admits the existing enclosing adapter's allocation, not its work.
func (r *COWRoot) NewTraversal(work *COWTraversalWork, extraBytes uint64) (*COWTraversal, error) {
	if work == nil || !work.Reserve(2, uint64(unsafe.Sizeof(COWTraversal{}))+uint64(unsafe.Sizeof(COWView{}))+uint64(unsafe.Sizeof(cowViewReleaseLease{}))) {
		return nil, btree.ErrWork
	}
	depth := cowMaximumHeight(r.Len())
	stackBytes := btree.StringCursorBytes[cowValue](depth) - uint64(unsafe.Sizeof(btree.StringCursor[cowValue]{}))
	orderedBytes := uint64(0)
	orderedAllocation := uint64(0)
	coreRecords := uint64(1)
	if r.ordered != nil {
		orderedBytes = cowOrderedTraversalBytes(r.ordered)
		orderedAllocation = orderedBytes - uint64(unsafe.Sizeof(cowOrderedTraversal{}))
		coreRecords += 3
	}
	if ^uint64(0)-stackBytes < orderedAllocation {
		return nil, ErrCOWCapacity
	}
	stackBytes += orderedAllocation
	extra, ok := cowAdd(cowAllocation(uint64(unsafe.Sizeof(COWTraversal{}))), cowAllocation(stackBytes))
	if !ok {
		return nil, ErrCOWCapacity
	}
	extra, ok = cowAdd(extra, extraBytes)
	if !ok {
		return nil, ErrCOWCapacity
	}
	// Check core work fit before creating a public owner or allocating its stack.
	coreBytes := btree.StringCursorBytes[cowValue](depth) + orderedBytes
	if work.Records > work.RecordLimit || coreRecords > work.RecordLimit-work.Records || work.Bytes > work.ByteLimit || coreBytes > work.ByteLimit-work.Bytes {
		return nil, btree.ErrWork
	}
	v, err := r.Acquire(extra)
	if err != nil {
		return nil, err
	}
	core, initialized := btree.NewStringCursor(r.tree, depth, work)
	if !initialized {
		panic("pre-admitted immutable cursor initialization refused")
	}
	c := &COWTraversal{view: v, cursor: core}
	if r.ordered != nil {
		c.ordered, initialized = newCOWOrderedTraversal(r.ordered, work)
		if !initialized {
			panic("pre-admitted ordered cursor initialization refused")
		}
		c.hasOrdered = true
	}
	return c, nil
}

// Begin borrows an immutable seek key; retain it until completion or CloseStep.
// A paused request cannot be replaced by a different request. CloseStep can
// cancel it, clearing stack references through charged phases before release.
func (c *COWTraversal) Begin(op COWTraversalOperation, key string, work *COWTraversalWork) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil || c.closing {
		return ErrCOWClosed
	}
	if op > btree.Prev {
		return btree.ErrOperation
	}
	if work == nil {
		return btree.ErrWork
	}
	if c.hasOrdered {
		if c.pending {
			return btree.ErrPending
		}
		if !work.Reserve(1, uint64(unsafe.Sizeof(*c))) {
			return btree.ErrWork
		}
		c.runRaw, c.runOrdered = true, true
		c.rawOp, c.orderedOp, c.rawKey, c.orderedKey = op, op, key, key
		if (op == btree.Next || op == btree.Prev) && c.valid {
			direction := 1
			if op == btree.Prev {
				direction = -1
			}
			if c.direction == direction {
				c.runRaw, c.runOrdered = c.rawSelected, !c.rawSelected
			} else {
				seek := btree.SeekGE
				if direction < 0 {
					seek = btree.SeekLE
				}
				if c.rawSelected {
					c.orderedOp, c.orderedKey = seek, c.record.Key
				} else {
					c.rawOp, c.rawKey = seek, c.record.Key
				}
			}
		}
		c.op, c.key, c.pending, c.valid, c.phase = op, key, true, false, 1
		return nil
	}
	return c.cursor.Begin(op, key, work)
}
func (c *COWTraversal) Step(work *COWTraversalWork) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil || c.closing {
		return false, ErrCOWClosed
	}
	if work == nil {
		return false, btree.ErrWork
	}
	if c.hasOrdered {
		return c.stepMerged(work)
	}
	return c.cursor.Step(work)
}
func (c *COWTraversal) Record(work *COWTraversalWork) (COWRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil || c.closing {
		return COWRecord{}, false, ErrCOWClosed
	}
	if c.hasOrdered {
		if c.pending || !c.valid {
			return COWRecord{}, false, nil
		}
		if work == nil || !work.Reserve(1, uint64(unsafe.Sizeof(COWRecord{}))) {
			return COWRecord{}, false, btree.ErrWork
		}
		return c.record, true, nil
	}
	if !c.cursor.Ready() {
		return COWRecord{}, false, nil
	}
	// Include the fixed metadata read before decoding the packed value, in
	// addition to the stored headers and unchanged public output record.
	if work == nil || !work.Reserve(1, uint64(unsafe.Sizeof(COWRecord{}))+2*uint64(unsafe.Sizeof(cowValue("")))+2*uint64(unsafe.Sizeof(""))+cowValueMetadataBytes) {
		return COWRecord{}, false, btree.ErrWork
	}
	key, v, valid := c.cursor.Current()
	if !valid {
		return COWRecord{}, false, nil
	}
	return v.record(key), true, nil
}

// CloseStep returns a once-owned retirement only when charged stack cleanup and
// view release finish. It does NOT call Drain, certify physical resource death,
// or refund generation history before resource callbacks actually finish.
// The existing retirement owner must complete that separately counted work.
func (c *COWTraversal) CloseStep(work *COWTraversalWork) (bool, COWRetirement, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil {
		return true, COWRetirement{}, nil
	}
	if work == nil {
		return false, COWRetirement{}, btree.ErrWork
	}
	if c.hasOrdered {
		return c.closeMerged(work)
	}
	if !c.closing {
		if err := c.cursor.Begin(btree.Close, "", work); err != nil {
			return false, COWRetirement{}, err
		}
		c.closing = true
	}
	done, err := c.cursor.Step(work)
	if err != nil || !done {
		return false, COWRetirement{}, err
	}
	retirement, ready, err := c.view.ReleaseWithWork(work)
	if !ready || err != nil {
		return false, COWRetirement{}, err
	}
	c.view = nil
	return true, retirement, nil
}
