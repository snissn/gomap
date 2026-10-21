package memtable

import (
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

const (
	cowOrderedPrimaryBegin uint8 = iota + 1
	cowOrderedPrimaryStep
	cowOrderedInspect
	cowOrderedPrefixCompare
	cowOrderedSuffixRead
	cowOrderedHistoryBegin
	cowOrderedHistoryStep
	cowOrderedPublish
	cowOrderedEmpty
	cowOrderedClosePrimaryBegin
	cowOrderedClosePrimaryStep
	cowOrderedCloseHistoryBegin
	cowOrderedCloseHistoryStep
	cowOrderedCloseFinish
)

// cowOrderedTraversal borrows its enclosing COWTraversal's exact root owner.
// Both traversal stacks are admitted once; moving to a new logical key reuses
// the numeric stack. Every probe, compared byte and cleared frame uses the
// caller's original work allowance, including suspended seeks and close.
type cowOrderedTraversal struct {
	primary                                                   btree.StringCursor[*cowOrderedHistory]
	history                                                   btree.UintCursor[*cowOrderedValue]
	h                                                         *cowOrderedHistory
	value                                                     cowOrderedValue
	key                                                       string
	prefix                                                    string
	offset                                                    int
	encoded                                                   uint64
	target                                                    uint64
	op                                                        COWTraversalOperation
	primaryOp                                                 COWTraversalOperation
	historyOp                                                 COWTraversalOperation
	phase                                                     uint8
	direction                                                 int
	seekFloor, rebind, valid, started, atstart, atend, closed bool
}

func cowOrderedTraversalBytes(t *cowOrderedTree) uint64 {
	if t == nil {
		return 0
	}
	primaryDepth := cowMaximumHeight(t.keys.Len())
	historyDepth := cowMaximumHeight(t.maxHistory)
	return uint64(unsafe.Sizeof(cowOrderedTraversal{})) +
		btree.StringCursorBytes[*cowOrderedHistory](primaryDepth) - uint64(unsafe.Sizeof(btree.StringCursor[*cowOrderedHistory]{})) +
		btree.UintCursorBytes[*cowOrderedValue](historyDepth) - uint64(unsafe.Sizeof(btree.UintCursor[*cowOrderedValue]{}))
}

func newCOWOrderedTraversal(t *cowOrderedTree, work *COWTraversalWork) (cowOrderedTraversal, bool) {
	if t == nil || work == nil {
		return cowOrderedTraversal{}, false
	}
	primaryDepth := cowMaximumHeight(t.keys.Len())
	historyDepth := cowMaximumHeight(t.maxHistory)
	// Prove all constructor work fits before either stack is allocated.
	ownBytes := uint64(unsafe.Sizeof(cowOrderedTraversal{})) - uint64(unsafe.Sizeof(btree.StringCursor[*cowOrderedHistory]{})) - uint64(unsafe.Sizeof(btree.UintCursor[*cowOrderedValue]{}))
	constructorBytes := ownBytes + btree.StringCursorBytes[*cowOrderedHistory](primaryDepth) + btree.UintCursorBytes[*cowOrderedValue](historyDepth)
	if work.Records > work.RecordLimit || work.RecordLimit-work.Records < 3 || work.Bytes > work.ByteLimit || constructorBytes > work.ByteLimit-work.Bytes {
		return cowOrderedTraversal{}, false
	}
	if !work.Reserve(1, ownBytes) {
		return cowOrderedTraversal{}, false
	}
	primary, ok := btree.NewStringCursor(t.keys, primaryDepth, work)
	if !ok {
		panic("pre-admitted ordered primary cursor refused")
	}
	history, ok := btree.NewUintCursor[*cowOrderedValue](nil, historyDepth, work)
	if !ok {
		panic("pre-admitted ordered history cursor refused")
	}
	return cowOrderedTraversal{primary: primary, history: history}, true
}

func (c *cowOrderedTraversal) Ready() bool   { return c.valid && c.phase == 0 && !c.closed }
func (c *cowOrderedTraversal) Pending() bool { return c.phase != 0 }
func (c *cowOrderedTraversal) Closed() bool  { return c.closed }
func (c *cowOrderedTraversal) Current() (COWRecord, bool) {
	if !c.Ready() {
		return COWRecord{}, false
	}
	return c.value.value.record(c.value.key), true
}

func (c *cowOrderedTraversal) Begin(op COWTraversalOperation, key string, work *COWTraversalWork) error {
	if c.closed {
		return btree.ErrClosed
	}
	if c.phase != 0 && op != btree.Close {
		return btree.ErrPending
	}
	if op > btree.Close {
		return btree.ErrOperation
	}
	if work == nil || !work.Reserve(1, uint64(unsafe.Sizeof(*c))) {
		return btree.ErrWork
	}
	wasValid := c.valid
	c.op, c.key, c.valid, c.seekFloor = op, key, false, false
	c.offset, c.encoded = 0, 0
	if op == btree.Close {
		c.phase = cowOrderedClosePrimaryBegin
		return nil
	}
	c.direction = 1
	if op == btree.Last || op == btree.SeekLE || op == btree.Prev {
		c.direction = -1
	}
	if op == btree.Next || op == btree.Prev {
		if !wasValid {
			if op == btree.Next && (!c.started || c.atstart) {
				c.primaryOp = btree.First
			} else if op == btree.Prev && c.atend {
				c.primaryOp = btree.Last
			} else {
				return nil
			}
		} else if c.h.versions != nil {
			c.rebind = false
			c.historyOp = btree.Prev
			if op == btree.Prev {
				c.historyOp = btree.Next
			}
			c.phase = cowOrderedHistoryBegin
			return nil
		} else {
			c.primaryOp = op
		}
	} else if op == btree.SeekGE || op == btree.SeekLE {
		// The nearest prefix below the physical bound may contain its result.
		c.primaryOp, c.seekFloor = btree.SeekLE, true
	} else {
		c.primaryOp = op
	}
	c.phase = cowOrderedPrimaryBegin
	return nil
}

func (c *cowOrderedTraversal) advancePrimary() {
	c.seekFloor = false
	c.primaryOp = btree.Next
	if c.direction < 0 {
		c.primaryOp = btree.Prev
	}
	c.phase = cowOrderedPrimaryBegin
}

func (c *cowOrderedTraversal) enterHistory() {
	c.rebind = true
	c.historyOp = btree.Last
	if c.direction < 0 {
		c.historyOp = btree.First
	}
	if c.h.versions == nil {
		c.value = c.h.latest
		c.phase = cowOrderedPublish
	} else {
		c.phase = cowOrderedHistoryBegin
	}
}

func (c *cowOrderedTraversal) Step(work *COWTraversalWork) (bool, error) {
	if work == nil {
		return false, btree.ErrWork
	}
	for c.phase != 0 {
		switch c.phase {
		case cowOrderedPrimaryBegin:
			key := ""
			if c.seekFloor {
				key = c.key
			}
			if err := c.primary.Begin(c.primaryOp, key, work); err != nil {
				if err == btree.ErrWork {
					return false, nil
				}
				return false, err
			}
			c.phase = cowOrderedPrimaryStep
		case cowOrderedPrimaryStep:
			done, err := c.primary.Step(work)
			if !done || err != nil {
				return done, err
			}
			c.phase = cowOrderedInspect
		case cowOrderedInspect:
			if !work.Reserve(1, 2*uint64(unsafe.Sizeof(""))+uint64(unsafe.Sizeof(cowOrderedHistory{}))) {
				return false, nil
			}
			prefix, h, valid := c.primary.Current()
			if !valid {
				if c.seekFloor && c.direction > 0 {
					c.seekFloor, c.primaryOp, c.phase = false, btree.First, cowOrderedPrimaryBegin
				} else {
					c.phase = cowOrderedEmpty
				}
				continue
			}
			c.prefix, c.h = prefix, h
			if c.seekFloor {
				c.offset = 0
				c.phase = cowOrderedPrefixCompare
			} else {
				c.enterHistory()
			}
		case cowOrderedPrefixCompare:
			mismatch := len(c.prefix) > len(c.key)
			for !mismatch && c.offset < len(c.prefix) {
				if !work.Reserve(0, 2) {
					return false, nil
				}
				mismatch = c.prefix[c.offset] != c.key[c.offset]
				c.offset++
			}
			if mismatch {
				if c.direction > 0 {
					c.advancePrimary()
				} else {
					c.enterHistory()
				}
			} else {
				c.offset, c.encoded = 0, 0
				c.phase = cowOrderedSuffixRead
			}
		case cowOrderedSuffixRead:
			suffixLen := len(c.key) - len(c.prefix)
			for c.offset < 8 {
				if !work.Reserve(0, 1) {
					return false, nil
				}
				c.encoded <<= 8
				if c.offset < suffixLen {
					c.encoded |= uint64(c.key[len(c.prefix)+c.offset])
				}
				c.offset++
			}
			c.rebind = true
			if c.direction > 0 {
				if suffixLen > 8 {
					if c.encoded == ^uint64(0) {
						c.advancePrimary()
						continue
					}
					c.encoded++
				}
				c.target, c.historyOp = ^c.encoded, btree.SeekLE
			} else {
				if suffixLen < 8 {
					if c.encoded == 0 {
						c.advancePrimary()
						continue
					}
					c.encoded--
				}
				c.target, c.historyOp = ^c.encoded, btree.SeekGE
			}
			if c.h.versions == nil {
				matches := (c.direction > 0 && c.h.maximum <= c.target) || (c.direction < 0 && c.h.maximum >= c.target)
				if matches {
					c.value, c.phase = c.h.latest, cowOrderedPublish
				} else {
					c.advancePrimary()
				}
			} else {
				c.phase = cowOrderedHistoryBegin
			}
		case cowOrderedHistoryBegin:
			var err error
			if c.rebind {
				err = c.history.BeginAt(c.h.versions, c.historyOp, c.target, work)
			} else {
				err = c.history.Begin(c.historyOp, 0, work)
			}
			if err != nil {
				if err == btree.ErrWork {
					return false, nil
				}
				return false, err
			}
			c.phase = cowOrderedHistoryStep
		case cowOrderedHistoryStep:
			done, err := c.history.Step(work)
			if !done || err != nil {
				return done, err
			}
			// Current exports a timestamp and descriptor pointer, then this layer
			// copies the immutable descriptor into its borrowed current value.
			if !work.Reserve(1, 8+uint64(unsafe.Sizeof((*cowOrderedValue)(nil)))+uint64(unsafe.Sizeof(cowOrderedValue{}))) {
				return false, nil
			}
			_, value, valid := c.history.Current()
			if valid {
				c.value, c.phase = *value, cowOrderedPublish
			} else {
				c.advancePrimary()
			}
		case cowOrderedPublish:
			if !work.Reserve(1, uint64(unsafe.Sizeof(*c))) {
				return false, nil
			}
			c.valid, c.started, c.atstart, c.atend = true, true, false, false
			c.key, c.prefix, c.phase = "", "", 0
		case cowOrderedEmpty:
			if !work.Reserve(1, uint64(unsafe.Sizeof(*c))) {
				return false, nil
			}
			c.valid, c.started, c.atstart, c.atend = false, true, c.direction < 0, c.direction > 0
			c.key, c.prefix, c.h, c.value, c.phase = "", "", nil, cowOrderedValue{}, 0
		case cowOrderedClosePrimaryBegin:
			if err := c.primary.Begin(btree.Close, "", work); err != nil {
				if err == btree.ErrWork {
					return false, nil
				}
				return false, err
			}
			c.phase = cowOrderedClosePrimaryStep
		case cowOrderedClosePrimaryStep:
			done, err := c.primary.Step(work)
			if !done || err != nil {
				return done, err
			}
			c.phase = cowOrderedCloseHistoryBegin
		case cowOrderedCloseHistoryBegin:
			if err := c.history.Begin(btree.Close, 0, work); err != nil {
				if err == btree.ErrWork {
					return false, nil
				}
				return false, err
			}
			c.phase = cowOrderedCloseHistoryStep
		case cowOrderedCloseHistoryStep:
			done, err := c.history.Step(work)
			if !done || err != nil {
				return done, err
			}
			c.phase = cowOrderedCloseFinish
		case cowOrderedCloseFinish:
			if !work.Reserve(1, uint64(unsafe.Sizeof(*c))) {
				return false, nil
			}
			*c = cowOrderedTraversal{closed: true}
		}
	}
	return true, nil
}
