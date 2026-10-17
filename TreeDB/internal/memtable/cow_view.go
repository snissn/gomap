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
	mu    sync.Mutex
	root  *COWRoot
	lease *cowViewReleaseLease
}

func (r *COWRoot) Acquire(extraBytes uint64) (*COWView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		return nil, ErrCOWClosed
	}
	b := r.generation.budget
	charge, ok := cowAdd(cowAllocation(uint64(unsafe.Sizeof(COWView{})))+cowAllocation(uint64(unsafe.Sizeof(cowViewReleaseLease{}))), extraBytes)
	if !ok {
		return nil, ErrCOWCapacity
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrCOWClosed
	}
	if b.stats.Views >= b.limits.MaxViews || !b.addLocked(charge) {
		return nil, ErrCOWCapacity
	}
	b.stats.Views++
	r.refs++
	return &COWView{root: r, lease: &cowViewReleaseLease{budget: b, charge: charge}}, nil
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
	r, lease := v.root, v.lease
	if r == nil {
		v.mu.Unlock()
		return COWRetirement{}, true, nil
	}
	v.root, v.lease = nil, nil
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
	retirement, _, _ := v.ReleaseWithWork(nil)
	v = nil
	retirement.Drain()
}

// COWCursor owns its own view, so closing the creating cut/view does not revoke
// it. Record returns immutable strings, avoiding per-entry defensive copies.
// One cursor serializes Next/Seek/Record/Close; independent cursors read together.
type COWCursor struct {
	mu                                    sync.Mutex
	view                                  *COWView
	iter                                  btree.MapIter[string, cowValue]
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
	c := &COWCursor{view: v, iter: r.tree.Iter(), end: string(end), bounded: end != nil, reverse: reverse}
	if reverse {
		c.start = string(start)
		c.lowerBounded = start != nil
		c.seekReverse(nil)
	} else {
		c.valid = cowIteratorSeek(r.tree, &c.iter, start)
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
	if c.valid && ((c.bounded && c.iter.Key() >= c.end) || (c.reverse && c.lowerBounded && c.iter.Key() < c.start)) {
		c.valid = false
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
	c.valid = cowIteratorSeek(c.view.root.tree, &c.iter, key)
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
		if c.reverse {
			c.valid = c.iter.Prev()
		} else {
			c.valid = c.iter.Next()
		}
		c.checkEnd()
	}
	return nil
}
func (c *COWCursor) seekReverse(key []byte) {
	if key == nil || (c.bounded && string(key) >= c.end) {
		if c.bounded {
			if c.iter.Seek(c.end) {
				c.valid = c.iter.Prev()
			} else {
				c.valid = c.iter.Last()
			}
		} else {
			c.valid = c.iter.Last()
		}
	} else if cowIteratorSeek(c.view.root.tree, &c.iter, key) {
		c.valid = true
		if c.iter.Key() > string(key) {
			c.valid = c.iter.Prev()
		}
	} else {
		c.valid = c.iter.Last()
	}
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
	v := c.iter.Value()
	return COWRecord{Key: c.iter.Key(), Value: v.value, Ptr: v.ptr, Flags: v.flags, Revision: v.revision}, true, nil
}
func (c *COWCursor) Close() {
	c.mu.Lock()
	v := c.view
	c.view = nil
	c.valid = false
	c.iter = btree.MapIter[string, cowValue]{}
	c.end = ""
	c.start = ""
	c.mu.Unlock()
	if v != nil {
		v.Close()
	}
}
