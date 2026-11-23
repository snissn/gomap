package memtable

import (
	"errors"
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

// stepMerged keeps the two immutable projections at their current positions.
// Only the selected stream moves on Next/Prev; physical seeks position both.
// Every child action and comparison can stop before consuming unavailable work.
func (c *COWTraversal) stepMerged(work *COWTraversalWork) (bool, error) {
	for c.pending {
		switch c.phase {
		case 1:
			if c.runRaw {
				if err := c.cursor.Begin(c.rawOp, c.rawKey, work); err != nil {
					if errors.Is(err, btree.ErrWork) {
						return false, nil
					}
					return false, err
				}
			}
			c.phase = 2
		case 2:
			if c.runRaw {
				if done, err := c.cursor.Step(work); err != nil || !done {
					return false, err
				}
			}
			c.phase = 3
		case 3:
			if c.runOrdered {
				var err error
				if c.hasDelta {
					err = c.delta.Begin(c.orderedOp, c.orderedKey, work)
				} else {
					err = c.ordered.Begin(c.orderedOp, c.orderedKey, work)
				}
				if err != nil {
					if errors.Is(err, btree.ErrWork) {
						return false, nil
					}
					return false, err
				}
			}
			c.phase = 4
		case 4:
			if c.runOrdered {
				var done bool
				var err error
				if c.hasDelta {
					done, err = c.delta.Step(work)
				} else {
					done, err = c.ordered.Step(work)
				}
				if err != nil || !done {
					return false, err
				}
			}
			c.phase = 5
		case 5:
			// Both streams can decode a packed native value here. Cover each
			// source's headers and metadata before reading either projection.
			decodeBytes := uint64(unsafe.Sizeof(COWRecord{})) + 2*uint64(unsafe.Sizeof(cowValue(""))) + 2*uint64(unsafe.Sizeof("")) + cowValueMetadataBytes
			if !work.Reserve(1, 2*decodeBytes) {
				return false, nil
			}
			key, v, raw := c.cursor.Current()
			c.rawRecord = COWRecord{}
			if raw {
				c.rawRecord = v.record(key)
			}
			if c.hasDelta {
				c.orderedRecord, _ = c.delta.Current()
			} else {
				c.orderedRecord, _ = c.ordered.Current()
			}
			c.compareOffset = 0
			if c.skipRawEqual && raw {
				c.phase = 7
			} else {
				c.skipRawEqual, c.skipRawKey = false, ""
				c.phase = 6
			}
		case 7:
			// On delta direction reversal, skip any shadowed raw tree key equal
			// to the previously emitted delta key before merging.
			a, b := c.rawRecord.Key, c.skipRawKey
			equal := len(a) == len(b)
			if equal {
				for c.compareOffset < len(a) {
					if !work.Reserve(0, 2) {
						return false, nil
					}
					if a[c.compareOffset] != b[c.compareOffset] {
						equal = false
						break
					}
					c.compareOffset++
				}
			}
			c.skipRawEqual, c.skipRawKey = false, ""
			c.compareOffset = 0
			if equal {
				c.runRaw, c.runOrdered = true, false
				c.rawOp, c.rawKey = c.op, ""
				c.phase = 1
			} else {
				c.phase = 6
			}
		case 6:
			raw := c.cursor.Ready()
			ordered := c.ordered.Ready()
			if c.hasDelta {
				ordered = c.delta.Ready()
			}
			cmp := 0
			if raw && ordered {
				a, b := c.rawRecord.Key, c.orderedRecord.Key
				for c.compareOffset < len(a) && c.compareOffset < len(b) {
					if !work.Reserve(0, 2) {
						return false, nil
					}
					x, y := a[c.compareOffset], b[c.compareOffset]
					c.compareOffset++
					if x < y {
						cmp = -1
						break
					}
					if x > y {
						cmp = 1
						break
					}
				}
				if cmp == 0 {
					if len(a) < len(b) {
						cmp = -1
					} else if len(a) > len(b) {
						cmp = 1
					}
				}
			}
			reverse := c.op == btree.Prev || c.op == btree.Last || c.op == btree.SeekLE
			c.direction = 1
			if reverse {
				c.direction = -1
			}
			if c.hasDelta && raw && ordered && cmp == 0 {
				c.equalTreeDelta = true
				c.rawSelected = false
			} else {
				c.equalTreeDelta = false
				c.rawSelected = raw && (!ordered || (!reverse && cmp <= 0) || (reverse && cmp >= 0))
			}
			c.valid = raw || ordered
			c.record = COWRecord{}
			if c.rawSelected {
				c.record = c.rawRecord
			} else if ordered {
				c.record = c.orderedRecord
			}
			c.pending, c.key, c.phase = false, "", 0
			c.rawKey, c.orderedKey = "", ""
		}
	}
	return true, nil
}

func (c *COWTraversal) closeMerged(work *COWTraversalWork) (bool, COWRetirement, error) {
	if !c.closing {
		if !work.Reserve(1, uint64(unsafe.Sizeof(*c))) {
			return false, COWRetirement{}, nil
		}
		c.closing, c.valid, c.pending, c.phase, c.key = true, false, false, 1, ""
		c.rawKey, c.orderedKey, c.skipRawKey = "", "", ""
		c.equalTreeDelta, c.skipRawEqual = false, false
		c.record, c.rawRecord, c.orderedRecord = COWRecord{}, COWRecord{}, COWRecord{}
	}
	for {
		switch c.phase {
		case 1:
			if err := c.cursor.Begin(btree.Close, "", work); err != nil {
				if errors.Is(err, btree.ErrWork) {
					return false, COWRetirement{}, nil
				}
				return false, COWRetirement{}, err
			}
			c.phase = 2
		case 2:
			if done, err := c.cursor.Step(work); err != nil || !done {
				return false, COWRetirement{}, err
			}
			c.phase = 3
		case 3:
			var err error
			if c.hasDelta {
				err = c.delta.Begin(btree.Close, "", work)
			} else {
				err = c.ordered.Begin(btree.Close, "", work)
			}
			if err != nil {
				if errors.Is(err, btree.ErrWork) {
					return false, COWRetirement{}, nil
				}
				return false, COWRetirement{}, err
			}
			c.phase = 4
		case 4:
			var done bool
			var err error
			if c.hasDelta {
				done, err = c.delta.Step(work)
			} else {
				done, err = c.ordered.Step(work)
			}
			if err != nil || !done {
				return false, COWRetirement{}, err
			}
			c.phase = 5
		case 5:
			retirement, ready, err := c.view.ReleaseWithWork(work)
			if !ready || err != nil {
				return false, COWRetirement{}, err
			}
			c.view = nil
			c.cursor = btree.StringCursor[cowValue]{}
			c.ordered = cowOrderedTraversal{}
			c.delta = cowDeltaTraversal{}
			return true, retirement, nil
		}
	}
}
