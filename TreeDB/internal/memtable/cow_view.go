package memtable

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"sync"
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

// COWView is an independently closable pin. Close races safely with Get/Seek
// and cursor traversal. No published-header Copy or mutable traversal occurs.
type COWView struct {
	mu           sync.Mutex
	root         *COWRoot
	leaseStorage cowViewReleaseLease
}

func (r *COWRoot) Acquire(extraBytes uint64) (*COWView, error) {
	if !r.RetainOwned() {
		return nil, ErrCOWClosed
	}
	b := r.generation.budget
	charge, ok := cowAdd(cowAllocation(uint64(unsafe.Sizeof(COWView{}))), extraBytes)
	if !ok {
		retired := r.Release()
		retired.Drain()
		return nil, ErrCOWCapacity
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		retired := r.Release()
		retired.Drain()
		return nil, ErrCOWClosed
	}
	if b.stats.Views >= b.limits.MaxViews || !b.addLocked(charge) {
		b.mu.Unlock()
		retired := r.Release()
		retired.Drain()
		return nil, ErrCOWCapacity
	}
	b.stats.Views++
	// Admission and the generation pin are complete. Allocate the admitted
	// wrapper outside the budget lock; the retained reference from RetainOwned
	// keeps the root alive even if its previous owners or the budget close
	// before allocation completes.
	b.mu.Unlock()
	return &COWView{root: r, leaseStorage: cowViewReleaseLease{budget: b, charge: charge}}, nil
}

func (v *COWView) Get(key []byte) (COWRecord, bool, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.root == nil {
		return COWRecord{}, false, ErrCOWClosed
	}
	r, ok := v.root.Get(key)
	return r, ok, nil
}

// ReleaseWithWork transfers original generation retirement and the admitted
// view allocation lease. The caller clears its view/cursor aliases before
// consuming that receipt. It performs no resource callback or capacity refund.
func (v *COWView) ReleaseWithWork(w *iterator.OrdinalScanWork) (COWRetirement, bool, error) {
	if w != nil && !w.Reserve(3, 2*uint64(unsafe.Sizeof(COWView{}))+
		2*uint64(unsafe.Sizeof(COWRoot{}))+2*uint64(unsafe.Sizeof(cowGeneration{}))+
		2*uint64(unsafe.Sizeof(COWBudget{}))) {
		return COWRetirement{}, false, nil
	}
	v.mu.Lock()
	r := v.root
	if r == nil {
		v.mu.Unlock()
		return COWRetirement{}, true, nil
	}
	v.root = nil
	lease := &v.leaseStorage
	v.mu.Unlock()
	v = nil
	retirement := r.Release()
	r = nil
	b := lease.budget
	b.mu.Lock()
	b.stats.Views--
	b.stats.DeferredBytes += lease.charge
	b.mu.Unlock()
	retirement.viewLease = lease
	return retirement, true, nil
}

// Release delegates the ordinary API to the same physical retirement stages.
func (v *COWView) Release() COWRetirement {
	retirement, _, _ := v.ReleaseWithWork(nil)
	return retirement
}

func (v *COWView) Close() {
	_ = v.CloseAndPutIdleReadScratch3(nil, [3]*COWExternalLease{})
}

// CloseAndPutIdleReadScratch3 closes the view and, when value is non-nil,
// attempts to park caller-owned read scratch under the same budget mutex
// acquisition. It always closes the view; when it returns false, the caller
// retains its original leases and must restore or close them.
func (v *COWView) CloseAndPutIdleReadScratch3(value any, leases [3]*COWExternalLease) bool {
	if v == nil {
		return false
	}
	v.mu.Lock()
	r := v.root
	if r == nil {
		v.mu.Unlock()
		return false
	}
	v.root = nil
	lease := &v.leaseStorage
	v.mu.Unlock()
	v = nil
	retirement := r.Release()
	r = nil
	b := lease.budget
	parked := false
	if retirement.generation == nil {
		b.mu.Lock()
		if value != nil {
			parked = b.putIdleReadScratch3Locked(value, leases)
		}
		if !lease.closed {
			lease.closed = true
			charge := lease.charge
			lease.charge = 0
			b.stats.Views--
			b.stats.TotalBytes -= charge
			b.releaseControlLocked()
		}
		b.mu.Unlock()
		b.releaseMaintenanceReservation()
		return parked
	}
	b.mu.Lock()
	if value != nil {
		parked = b.putIdleReadScratch3Locked(value, leases)
	}
	b.stats.Views--
	b.stats.DeferredBytes += lease.charge
	b.mu.Unlock()
	retirement.viewLease = lease
	retirement.Drain()
	return parked
}

// COWCursor owns its own view, so closing the creating cut/view does not revoke
// it. Record returns immutable strings, avoiding per-entry defensive copies.
// One cursor serializes Next/Seek/Record/Close; independent cursors read together.
type COWCursor struct {
	mu                                    sync.Mutex
	view                                  *COWView
	iter                                  btree.MapIter[string, cowValue]
	ordered                               cowOrderedCursor
	delta                                 []cowDeltaEntry
	l1                                    *cowDeltaL1
	deltaIdx                              int
	l1Chunk, l1Entry                      int
	record                                COWRecord
	rawValid, orderedValid, rawSelected   bool
	deltaValid, l1Valid                   bool
	hasDelta                              bool
	stepL0, stepL1, stepTree              bool
	start, end                            string
	bounded, lowerBounded, valid, reverse bool
}

func (r *COWRoot) Cursor(start, end []byte) (*COWCursor, error) {
	return r.CursorWithExtraBytes(start, end, 0)
}

// Charge every discarded stack backing, not only its retained final capacity.
// Through 32 frames Go doubles capacity. For larger synthetic heights, Go1.26
// nextslicecap grows by at least 1.25 and at most 2; allocator/header rounding
// adds slack. Eight times the rounded 2*height backing bounds the geometric
// sum (the unrounded sum is at most five times the final backing). Degree-32
// trees on supported int cardinalities never need this conservative tail.
func cowCursorStackCharge(height int) (uint64, bool) {
	if height < 1 {
		return 0, true
	}
	frame := 2 * uint64(unsafe.Sizeof(uintptr(0)))
	if height > 32 {
		if uint64(height) > (^uint64(0)-16)/(2*frame) {
			return 0, false
		}
		last := cowAllocation(2*uint64(height)*frame + 16)
		if last > ^uint64(0)/8 {
			return 0, false
		}
		return 8 * last, true
	}
	var sum uint64
	for capacity := 1; ; capacity *= 2 {
		var ok bool
		sum, ok = cowAdd(sum, cowAllocation(uint64(capacity)*frame))
		if !ok {
			return 0, false
		}
		if capacity >= height {
			return sum, true
		}
	}
}

// CursorWithExtraBytes reserves an existing iterator adapter's wrapper and
// copied domain bounds in the same view admission as the cursor itself.
func (r *COWRoot) CursorWithExtraBytes(start, end []byte, extraBytes uint64) (*COWCursor, error) {
	return r.cursorWithExtraBytes(start, end, extraBytes, false)
}

// ReverseCursorWithExtraBytes uses the same admitted immutable tree/view owner
// and bounded traversal stack, moving Last/Prev without materializing records.
func (r *COWRoot) ReverseCursorWithExtraBytes(start, end []byte, extraBytes uint64) (*COWCursor, error) {
	return r.cursorWithExtraBytes(start, end, extraBytes, true)
}
func (r *COWRoot) cursorWithExtraBytes(start, end []byte, extraBytes uint64, reverse bool) (*COWCursor, error) {
	stack, ok := cowCursorStackCharge(r.Height())
	if !ok {
		return nil, ErrCOWCapacity
	}
	if r.ordered != nil {
		orderedStack, fits := cowOrderedCursorStackCharge(r.ordered)
		if !fits {
			return nil, ErrCOWCapacity
		}
		stack, ok = cowAdd(stack, orderedStack)
		if !ok {
			return nil, ErrCOWCapacity
		}
	}
	extra, ok := cowAdd(cowAllocation(uint64(unsafe.Sizeof(COWCursor{}))), stack)
	if !ok {
		return nil, ErrCOWCapacity
	}
	extra, ok = cowAdd(extra, cowAllocation(uint64(len(end))))
	if !ok {
		return nil, ErrCOWCapacity
	}
	if reverse {
		extra, ok = cowAdd(extra, cowAllocation(uint64(len(start))))
		if !ok {
			return nil, ErrCOWCapacity
		}
	}
	extra, ok = cowAdd(extra, extraBytes)
	if !ok {
		return nil, ErrCOWCapacity
	}
	v, err := r.Acquire(extra)
	if err != nil {
		return nil, err
	}
	c := &COWCursor{view: v, iter: r.tree.Iter(), delta: r.delta, l1: r.l1, hasDelta: r.hasDelta(), end: string(end), bounded: end != nil, reverse: reverse}
	if r.ordered != nil {
		c.ordered = r.ordered.cursor()
	}
	if reverse {
		c.start = string(start)
		c.lowerBounded = start != nil
		c.seekReverse(nil)
	} else {
		c.rawValid = cowIteratorSeek(r.tree, &c.iter, start)
		c.orderedValid = c.ordered.seekGE(start)
		if c.hasDelta {
			if len(c.delta) > 0 {
				c.deltaIdx = cowDeltaSeekGE(c.delta, start)
				c.deltaValid = c.deltaIdx < len(c.delta)
			}
			if c.l1 != nil {
				c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.seekGE(start)
			}
		}
		c.selectRecord()
		c.checkEnd()
	}
	return c, nil
}

func (v *COWView) Cursor(start, end []byte) (*COWCursor, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.root == nil {
		return nil, ErrCOWClosed
	}
	return v.root.Cursor(start, end)
}
func (c *COWCursor) checkEnd() {
	if c.valid && ((c.bounded && c.record.Key >= c.end) || (c.reverse && c.lowerBounded && c.record.Key < c.start)) {
		c.valid = false
	}
}

func (c *COWCursor) selectRecord() {
	if c.hasDelta {
		c.orderedValid = false
		c.rawSelected = false
		c.stepL0, c.stepL1, c.stepTree = false, false, false
		c.record = COWRecord{}

		var bestKey string
		var bestVal cowValue
		found := false

		// Evaluate in ascending priority order (tree, then L1, then L0) so
		// ties overwrite with higher-precedence value (L0 > L1 > tree).
		if c.rawValid {
			bestKey = c.iter.Key()
			bestVal = c.iter.Value()
			found = true
			c.stepTree = true
		}
		if c.l1Valid {
			e := c.l1.chunks[c.l1Chunk].entries[c.l1Entry]
			if !found {
				bestKey, bestVal, found = e.key, e.value, true
				c.stepL1 = true
			} else if e.key == bestKey {
				bestVal = e.value
				c.stepL1 = true
			} else if (!c.reverse && e.key < bestKey) || (c.reverse && e.key > bestKey) {
				bestKey, bestVal = e.key, e.value
				c.stepTree = false
				c.stepL1 = true
			}
		}
		if c.deltaValid {
			e := c.delta[c.deltaIdx]
			if !found {
				bestKey, bestVal, found = e.key, e.value, true
				c.stepL0 = true
			} else if e.key == bestKey {
				bestVal = e.value
				c.stepL0 = true
			} else if (!c.reverse && e.key < bestKey) || (c.reverse && e.key > bestKey) {
				bestKey, bestVal = e.key, e.value
				c.stepTree = false
				c.stepL1 = false
				c.stepL0 = true
			}
		}
		c.valid = found
		if found {
			c.record = bestVal.record(bestKey)
		}
		return
	}
	ordered, ord := c.ordered.current()
	c.orderedValid = c.orderedValid && ord
	c.rawSelected = c.rawValid && (!c.orderedValid || (!c.reverse && c.iter.Key() <= ordered.Key) || (c.reverse && c.iter.Key() >= ordered.Key))
	c.valid = c.rawValid || c.orderedValid
	c.record = COWRecord{}
	if c.rawSelected {
		c.record = c.iter.Value().record(c.iter.Key())
	} else if c.orderedValid {
		c.record = ordered
	}
}
func (c *COWCursor) Seek(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil {
		return ErrCOWClosed
	}
	if c.reverse {
		c.seekReverse(key)
		return nil
	}
	c.rawValid = cowIteratorSeek(c.view.root.tree, &c.iter, key)
	c.orderedValid = c.ordered.seekGE(key)
	if c.hasDelta {
		if len(c.delta) > 0 {
			c.deltaIdx = cowDeltaSeekGE(c.delta, key)
			c.deltaValid = c.deltaIdx < len(c.delta)
		}
		if c.l1 != nil {
			c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.seekGE(key)
		}
	}
	c.selectRecord()
	c.checkEnd()
	return nil
}
func (c *COWCursor) Next() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil {
		return ErrCOWClosed
	}
	if c.valid {
		if c.hasDelta {
			if c.stepL0 {
				if c.reverse {
					c.deltaIdx--
					c.deltaValid = c.deltaIdx >= 0
				} else {
					c.deltaIdx++
					c.deltaValid = c.deltaIdx < len(c.delta)
				}
			}
			if c.stepL1 {
				if c.reverse {
					c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.prevPos(c.l1Chunk, c.l1Entry)
				} else {
					c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.nextPos(c.l1Chunk, c.l1Entry)
				}
			}
			if c.stepTree {
				if c.reverse {
					c.rawValid = c.iter.Prev()
				} else {
					c.rawValid = c.iter.Next()
				}
			}
		} else if c.rawSelected {
			if c.reverse {
				c.rawValid = c.iter.Prev()
			} else {
				c.rawValid = c.iter.Next()
			}
		} else {
			if c.reverse {
				c.orderedValid = c.ordered.prev()
			} else {
				c.orderedValid = c.ordered.next()
			}
		}
		c.selectRecord()
		c.checkEnd()
	}
	return nil
}
func (c *COWCursor) seekReverse(key []byte) {
	if key == nil || (c.bounded && cowKeyCompare(c.end, key) <= 0) {
		if c.bounded {
			if c.iter.Seek(c.end) {
				c.rawValid = c.iter.Prev()
			} else {
				c.rawValid = c.iter.Last()
			}
			c.orderedValid = c.ordered.seekLE([]byte(c.end))
			if r, ok := c.ordered.current(); ok && r.Key >= c.end {
				c.orderedValid = c.ordered.prev()
			}
			if c.hasDelta {
				if len(c.delta) > 0 {
					c.deltaIdx = cowDeltaSeekLTString(c.delta, c.end)
					c.deltaValid = c.deltaIdx >= 0
				}
				if c.l1 != nil {
					c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.seekLTString(c.end)
				}
			}
		} else {
			c.rawValid = c.iter.Last()
			c.orderedValid = c.ordered.last()
			if c.hasDelta {
				if len(c.delta) > 0 {
					c.deltaIdx = len(c.delta) - 1
					c.deltaValid = c.deltaIdx >= 0
				}
				if c.l1 != nil {
					c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.seekLT(nil)
				}
			}
		}
	} else if cowIteratorSeek(c.view.root.tree, &c.iter, key) {
		c.rawValid = true
		if cowKeyCompare(c.iter.Key(), key) > 0 {
			c.rawValid = c.iter.Prev()
		}
		c.orderedValid = c.ordered.seekLE(key)
		if c.hasDelta {
			if len(c.delta) > 0 {
				c.deltaIdx = cowDeltaSeekLE(c.delta, key)
				c.deltaValid = c.deltaIdx >= 0
			}
			if c.l1 != nil {
				c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.seekLE(key)
			}
		}
	} else {
		c.rawValid = c.iter.Last()
		c.orderedValid = c.ordered.seekLE(key)
		if c.hasDelta {
			if len(c.delta) > 0 {
				c.deltaIdx = cowDeltaSeekLE(c.delta, key)
				c.deltaValid = c.deltaIdx >= 0
			}
			if c.l1 != nil {
				c.l1Chunk, c.l1Entry, c.l1Valid = c.l1.seekLE(key)
			}
		}
	}
	c.selectRecord()
	c.checkEnd()
}
func (c *COWCursor) Record() (COWRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil {
		return COWRecord{}, false, ErrCOWClosed
	}
	if !c.valid {
		return COWRecord{}, false, nil
	}
	return c.record, true, nil
}
func (c *COWCursor) Close() {
	c.mu.Lock()
	v := c.view
	c.view = nil
	c.valid = false
	c.iter = btree.MapIter[string, cowValue]{}
	c.ordered.clear()
	c.delta = nil
	c.l1 = nil
	c.deltaIdx = 0
	c.l1Chunk, c.l1Entry = 0, 0
	c.deltaValid, c.l1Valid, c.hasDelta = false, false, false
	c.stepL0, c.stepL1, c.stepTree = false, false, false
	c.record = COWRecord{}
	c.end = ""
	c.start = ""
	c.mu.Unlock()
	if v != nil {
		v.Close()
	}
}
