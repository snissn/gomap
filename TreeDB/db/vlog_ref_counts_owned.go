package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"unsafe"
)

// The original collector, candidate and tracker move this same backing once.
// fileID zero is empty; zero plus MaxUint64 is a tombstone. Actual ValueLog
// file IDs include page's ValueLog marker, including encoded segment zero.
type valueLogRefCountSlot struct {
	fileID uint32
	count  uint64
}
type valueLogRefCounts struct {
	slots            []valueLogRefCountSlot
	used, tombstones int
	metadata         *retainedalloc.Owner
	charge           uint64
}

func refCountHash(id uint32) uint32 {
	id ^= id >> 16
	id *= 0x7feb352d
	id ^= id >> 15
	id *= 0x846ca68b
	return id ^ (id >> 16)
}
func (c *valueLogRefCounts) find(id uint32) (int, bool) {
	if id == 0 || len(c.slots) == 0 {
		return -1, false
	}
	mask := len(c.slots) - 1
	first := -1
	for n, at := 0, int(refCountHash(id))&mask; n < len(c.slots); n, at = n+1, (at+1)&mask {
		s := c.slots[at]
		if s.fileID == id {
			return at, true
		}
		if s.fileID == 0 {
			if s.count == 0 {
				if first >= 0 {
					return first, false
				}
				return at, false
			}
			if first < 0 {
				first = at
			}
		}
	}
	return first, false
}
func (c *valueLogRefCounts) get(id uint32) uint64 {
	at, ok := c.find(id)
	if !ok {
		return 0
	}
	return c.slots[at].count
}
func (c *valueLogRefCounts) reserve(entries int) error {
	if entries < 0 || entries > int(^uint(0)>>1)/2 {
		return retainedalloc.ErrCapacity
	}
	capacity := 8
	for capacity/2 < entries {
		if capacity > int(^uint(0)>>1)/2 {
			return retainedalloc.ErrCapacity
		}
		capacity *= 2
	}
	if len(c.slots) >= capacity && c.used+c.tombstones < len(c.slots)-len(c.slots)/4 {
		return nil
	}
	if len(c.slots) > capacity {
		capacity = len(c.slots)
	}
	if uint64(capacity) > math.MaxUint64/uint64(unsafe.Sizeof(valueLogRefCountSlot{})) {
		return retainedalloc.ErrCapacity
	}
	charge := retainedalloc.AllocationCharge(uint64(capacity) * uint64(unsafe.Sizeof(valueLogRefCountSlot{})))
	if c.metadata != nil {
		if err := c.metadata.AddPending(charge); err != nil {
			return err
		}
	}
	next := valueLogRefCounts{slots: make([]valueLogRefCountSlot, capacity), metadata: c.metadata, charge: charge}
	for _, s := range c.slots {
		if s.fileID != 0 {
			at, _ := next.find(s.fileID)
			next.slots[at] = s
			next.used++
		}
	}
	// Scrub old backing/aliases before refund, with both arrays admitted during copy.
	old := c.take()
	*c = next
	old.close()
	return nil
}
func (c *valueLogRefCounts) set(id uint32, count uint64) error {
	if id == 0 {
		return errors.New("zero value-log count file ID")
	}
	at, exists := c.find(id)
	if count == 0 {
		if exists {
			c.slots[at] = valueLogRefCountSlot{count: math.MaxUint64}
			c.used--
			c.tombstones++
		}
		return nil
	}
	if !exists {
		if err := c.reserve(c.used + 1); err != nil {
			return err
		}
		at, exists = c.find(id)
		if at < 0 {
			return retainedalloc.ErrCapacity
		}
		if c.slots[at].count != 0 {
			c.tombstones--
		}
		c.used++
	}
	c.slots[at] = valueLogRefCountSlot{fileID: id, count: count}
	return nil
}
func (c *valueLogRefCounts) take() valueLogRefCounts { out := *c; *c = valueLogRefCounts{}; return out }
func (c *valueLogRefCounts) close() {
	owner, charge := c.metadata, c.charge
	clear(c.slots)
	*c = valueLogRefCounts{}
	if owner != nil && charge != 0 {
		owner.RemovePending(charge)
	}
}
func (c *valueLogRefCounts) clone() (valueLogRefCounts, error) {
	out := valueLogRefCounts{metadata: c.metadata}
	if c.used == 0 {
		return out, nil
	}
	if err := out.reserve(c.used); err != nil {
		return valueLogRefCounts{}, err
	}
	for _, s := range c.slots {
		if s.fileID != 0 {
			at, _ := out.find(s.fileID)
			out.slots[at] = s
			out.used++
		}
	}
	return out, nil
}

// Diagnostic/output projections are transient; production replacement moves the
// original records and never clones a map into retained cache storage.
func (c *valueLogRefCounts) toMap() map[uint32]uint64 {
	out := make(map[uint32]uint64, c.used)
	for _, s := range c.slots {
		if s.fileID != 0 {
			out[s.fileID] = s.count
		}
	}
	return out
}
func (c *valueLogRefCounts) referencedSet() map[uint32]struct{} {
	out := make(map[uint32]struct{}, c.used)
	for _, s := range c.slots {
		if s.fileID != 0 {
			out[s.fileID] = struct{}{}
		}
	}
	return out
}
func valueLogRefCountsFromMap(owner *retainedalloc.Owner, counts map[uint32]uint64) (valueLogRefCounts, error) {
	out := valueLogRefCounts{metadata: owner}
	if len(counts) != 0 {
		if err := out.reserve(len(counts)); err != nil {
			return valueLogRefCounts{}, err
		}
	}
	for id, n := range counts {
		if err := out.set(id, n); err != nil {
			out.close()
			return valueLogRefCounts{}, err
		}
	}
	return out, nil
}
