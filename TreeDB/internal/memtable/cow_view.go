package memtable

import (
	"sync"
	"unsafe"

	"github.com/tidwall/btree"
)

// COWView is an independently closable pin. Close races safely with Get/Seek
// and cursor traversal. No published-header Copy or mutable traversal occurs.
type COWView struct {
	mu     sync.Mutex
	root   *COWRoot
	charge uint64
}

func (r *COWRoot) Acquire(extraBytes uint64) (*COWView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		return nil, ErrCOWClosed
	}
	b := r.generation.budget
	charge, ok := cowAdd(cowAllocation(uint64(unsafe.Sizeof(COWView{}))), extraBytes)
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
	return &COWView{root: r, charge: charge}, nil
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

func (v *COWView) Close() {
	v.mu.Lock()
	r := v.root
	if r == nil {
		v.mu.Unlock()
		return
	}
	v.root = nil
	b := r.generation.budget
	b.mu.Lock()
	b.stats.Views--
	b.stats.TotalBytes -= v.charge
	b.releaseControlLocked()
	b.mu.Unlock()
	v.mu.Unlock()
	retirement := r.Release()
	retirement.Drain()
}

// COWCursor owns its own view, so closing the creating cut/view does not revoke
// it. Record returns immutable strings, avoiding per-entry defensive copies.
// One cursor serializes Next/Seek/Record/Close; independent cursors read together.
type COWCursor struct {
	mu             sync.Mutex
	view           *COWView
	iter           btree.MapIter[string, cowValue]
	end            string
	bounded, valid bool
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
	extra, ok = cowAdd(extra, extraBytes)
	if !ok {
		return nil, ErrCOWCapacity
	}
	v, err := r.Acquire(extra)
	if err != nil {
		return nil, err
	}
	c := &COWCursor{view: v, iter: r.tree.Iter(), end: string(end), bounded: end != nil}
	c.valid = cowIteratorSeek(r.tree, &c.iter, start)
	c.checkEnd()
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
	if c.valid && c.bounded && c.iter.Key() >= c.end {
		c.valid = false
	}
}
func (c *COWCursor) Seek(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.view == nil {
		return ErrCOWClosed
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
		c.valid = c.iter.Next()
		c.checkEnd()
	}
	return nil
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
	c.mu.Unlock()
	if v != nil {
		v.Close()
	}
}
