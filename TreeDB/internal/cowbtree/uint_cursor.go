package cowbtree

import "unsafe"

// UintCursor traverses the SAME Map nodes without mutation, sorting or rank
// rescans. The Map header/root must be immutable and retained until Close.
// Timestamp comparisons have fixed width and reserve both operands before reading.
// It is single-owner; the enclosing COW adapter serializes it. No unsafe node
// layout access is used. A result is hidden while an operation is paused.
type UintCursor[V any] struct {
	tree                                   *Map[uint64, V]
	stack                                  []mapIterStackItem[uint64, V]
	item                                   mapPair[uint64, V]
	node                                   *mapNode[uint64, V]
	key, probe                             uint64
	lo, hi, pivot, offset, index, child    int
	op                                     Operation
	phase                                  uint8
	direction                              int
	valid, started, atstart, atend, closed bool
}

func UintCursorBytes[V any](depth int) uint64 {
	return uint64(unsafe.Sizeof(UintCursor[V]{})) + uint64(depth)*uint64(unsafe.Sizeof(mapIterStackItem[uint64, V]{}))
}

// NewUintCursor admits initialization before allocating/zeroing its stack.
// depth is a caller-proved upper bound for this immutable root's height.
func NewUintCursor[V any](tree *Map[uint64, V], depth int, work Work) (UintCursor[V], bool) {
	if depth < 0 || depth > 64 || !reserve(work, 1, UintCursorBytes[V](depth)) {
		return UintCursor[V]{}, false
	}
	return UintCursor[V]{tree: tree, stack: make([]mapIterStackItem[uint64, V], 0, depth)}, true
}

func (c *UintCursor[V]) Begin(op Operation, key uint64, w Work) error {
	if c.closed {
		return ErrClosed
	}
	if c.phase != 0 && op != Close {
		return ErrPending
	}
	if op > Close {
		return ErrOperation
	}
	if !reserve(w, 1, uint64(unsafe.Sizeof(*c))) {
		return ErrWork
	}
	c.op = op
	c.key = key
	c.probe = 0
	c.offset = 0
	if op == Close {
		c.valid = false
		c.phase = closePhase
		return nil
	}
	if op == Next || op == Prev {
		if c.valid {
			c.valid = false
			c.phase = movePhase
			return nil
		}
		if op == Next && (!c.started || c.atstart) {
			c.op = First
		} else if op == Prev && c.atend {
			c.op = Last
		} else {
			return nil
		}
	}
	c.valid = false
	c.phase = resetPhase
	return nil
}

// Current borrows completed metadata. Exporting/copying it is caller-charged.
func (c *UintCursor[V]) Ready() bool { return c.valid && c.phase == 0 && !c.closed }
func (c *UintCursor[V]) Current() (uint64, V, bool) {
	if !c.valid || c.phase != 0 || c.closed {
		var zero V
		return 0, zero, false
	}
	return c.item.key, c.item.value, true
}
func (c *UintCursor[V]) Pending() bool { return c.phase != 0 }
func (c *UintCursor[V]) Closed() bool  { return c.closed }

// Step returns done=false only after stopping BEFORE an unaffordable action.
// It retains binary-search bounds, numeric probe and traversal stack.
// No successor is loaded after the current operation's publication boundary.
func (c *UintCursor[V]) Step(w Work) (done bool, err error) {
	frameBytes := uint64(unsafe.Sizeof(mapIterStackItem[uint64, V]{}))
	nodeBytes := uint64(unsafe.Sizeof(mapNode[uint64, V]{}))
	for c.phase != 0 {
		switch c.phase {
		case resetPhase:
			if len(c.stack) > 0 {
				if !reserve(w, 1, frameBytes) {
					return false, nil
				}
				last := len(c.stack) - 1
				c.stack[last] = mapIterStackItem[uint64, V]{}
				c.stack = c.stack[:last]
			} else {
				c.phase = rootPhase
			}
		case rootPhase:
			if !reserve(w, 1, 2*uint64(unsafe.Sizeof(uintptr(0)))) {
				return false, nil
			}
			c.started = true
			c.atstart = false
			c.atend = false
			if c.tree == nil {
				c.node = nil
			} else {
				c.node = c.tree.root
			}
			c.direction = 1
			if c.op == Last || c.op == SeekLE {
				c.direction = -1
			}
			if c.node == nil {
				c.finishEmpty()
			} else {
				c.phase = enterPhase
			}
		case enterPhase:
			if !reserve(w, 1, nodeBytes) {
				return false, nil
			}
			if c.op == SeekGE || c.op == SeekLE {
				c.lo = 0
				c.hi = len(c.node.items)
				c.phase = probePhase
			} else {
				c.child = 0
				c.index = 0
				if c.direction < 0 {
					c.child = len(c.node.items)
					c.index = len(c.node.items) - 1
				}
				c.phase = pushPhase
			}
		case probePhase:
			if c.lo == c.hi {
				c.child = c.lo
				c.index = c.lo
				if c.direction < 0 {
					c.index--
				}
				c.phase = pushPhase
				continue
			}
			if !reserve(w, 1, 2*uint64(unsafe.Sizeof(uint64(0)))) {
				return false, nil
			}
			c.pivot = c.lo + (c.hi-c.lo)/2
			c.probe = c.node.items[c.pivot].key
			c.offset = 0
			c.phase = comparePhase
		case comparePhase:
			if !reserve(w, 1, 2*uint64(unsafe.Sizeof(uint64(0)))) {
				return false, nil
			}
			if c.probe < c.key || (c.probe == c.key && c.direction < 0) {
				c.lo = c.pivot + 1
			} else {
				c.hi = c.pivot
			}
			c.probe = 0
			c.phase = probePhase
		case pushPhase:
			if len(c.stack) == cap(c.stack) {
				return false, ErrStack
			}
			if !reserve(w, 1, frameBytes) {
				return false, nil
			}
			c.stack = append(c.stack, mapIterStackItem[uint64, V]{c.node, c.index})
			if c.node.leaf() {
				c.phase = candidatePhase
			} else {
				c.phase = childPhase
			}
		case childPhase:
			if !reserve(w, 1, 2*uint64(unsafe.Sizeof(uintptr(0)))) {
				return false, nil
			}
			c.node = (*c.node.children)[c.child]
			c.phase = enterPhase
		case candidatePhase:
			if !reserve(w, 1, frameBytes+nodeBytes) {
				return false, nil
			}
			s := c.stack[len(c.stack)-1]
			if s.i >= 0 && s.i < len(s.n.items) {
				c.phase = publishPhase
			} else {
				c.phase = popPhase
			}
		case popPhase:
			if !reserve(w, 1, frameBytes) {
				return false, nil
			}
			last := len(c.stack) - 1
			c.stack[last] = mapIterStackItem[uint64, V]{}
			c.stack = c.stack[:last]
			if len(c.stack) == 0 {
				c.finishEmpty()
			} else {
				c.phase = candidatePhase
			}
		case movePhase:
			if !reserve(w, 1, 2*frameBytes+nodeBytes) {
				return false, nil
			}
			c.direction = 1
			if c.op == Prev {
				c.direction = -1
			}
			s := &c.stack[len(c.stack)-1]
			c.node = s.n
			if s.n.leaf() {
				s.i += c.direction
				c.phase = candidatePhase
			} else {
				c.child = s.i
				if c.direction > 0 {
					c.child++
				}
				s.i += c.direction
				// Endpoint descent within the selected child, not another root seek.
				if c.direction > 0 {
					c.op = First
				} else {
					c.op = Last
				}
				c.phase = childPhase
			}
		case publishPhase:
			if !reserve(w, 1, uint64(unsafe.Sizeof(c.item))) {
				return false, nil
			}
			s := c.stack[len(c.stack)-1]
			c.item = s.n.items[s.i]
			c.valid = true
			c.key = 0
			c.probe = 0
			c.node = nil
			c.phase = 0
		case emptyPhase:
			if !reserve(w, 1, uint64(unsafe.Sizeof(*c))) {
				return false, nil
			}
			c.completeEmpty()
		case closePhase:
			if len(c.stack) > 0 {
				if !reserve(w, 1, frameBytes) {
					return false, nil
				}
				last := len(c.stack) - 1
				c.stack[last] = mapIterStackItem[uint64, V]{}
				c.stack = c.stack[:last]
			} else {
				if !reserve(w, 1, uint64(unsafe.Sizeof(*c))) {
					return false, nil
				}
				*c = UintCursor[V]{closed: true}
			}
		}
	}
	return true, nil
}
func (c *UintCursor[V]) finishEmpty() { c.phase = emptyPhase }
func (c *UintCursor[V]) completeEmpty() {
	c.valid = false
	c.atstart = c.direction < 0
	c.atend = c.direction > 0
	c.item = mapPair[uint64, V]{}
	c.node = nil
	c.key = 0
	c.probe = 0
	c.phase = 0
}

// BeginAt reuses the admitted stack for a different immutable history. Old
// stack edges are cleared by the ordinary resumable reset phase. The enclosing
// root must own both the old and new history until the request completes.
func (c *UintCursor[V]) BeginAt(tree *Map[uint64, V], op Operation, key uint64, w Work) error {
	if op != First && op != Last && op != SeekGE && op != SeekLE {
		return ErrOperation
	}
	if err := c.Begin(op, key, w); err != nil {
		return err
	}
	c.tree = tree
	return nil
}
