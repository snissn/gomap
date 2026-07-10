package zipper

const pointerRefCountsInlineCapacity = 8

type pointerRefCountEntry struct {
	fileID uint32
	count  uint64
}

// PointerRefCounts is an apply-attempt-local count of pointer references by
// file ID. Typical batches stay in the inline representation; unusually wide
// batches promote to a map.
type PointerRefCounts struct {
	inline  [pointerRefCountsInlineCapacity]pointerRefCountEntry
	inlineN uint8
	counts  map[uint32]uint64
}

func (c *PointerRefCounts) add(fileID uint32, count uint64) {
	if c == nil || fileID == 0 || count == 0 {
		return
	}
	if c.counts == nil {
		for i := uint8(0); i < c.inlineN; i++ {
			if c.inline[i].fileID == fileID {
				c.inline[i].count += count
				return
			}
		}
		if int(c.inlineN) < len(c.inline) {
			c.inline[c.inlineN] = pointerRefCountEntry{fileID: fileID, count: count}
			c.inlineN++
			return
		}
		c.counts = make(map[uint32]uint64, pointerRefCountsInlineCapacity*2)
		for i := uint8(0); i < c.inlineN; i++ {
			entry := c.inline[i]
			c.counts[entry.fileID] = entry.count
			c.inline[i] = pointerRefCountEntry{}
		}
		c.inlineN = 0
	}
	c.counts[fileID] += count
}

func (c *PointerRefCounts) merge(src *PointerRefCounts) {
	if c == nil || src == nil {
		return
	}
	src.ForEach(func(fileID uint32, count uint64) bool {
		c.add(fileID, count)
		return true
	})
}

// Count returns the number of removed pointer references for fileID.
func (c *PointerRefCounts) Count(fileID uint32) uint64 {
	if c == nil || fileID == 0 {
		return 0
	}
	if c.counts != nil {
		return c.counts[fileID]
	}
	for i := uint8(0); i < c.inlineN; i++ {
		if c.inline[i].fileID == fileID {
			return c.inline[i].count
		}
	}
	return 0
}

// ForEach visits each non-zero file count until fn returns false.
func (c *PointerRefCounts) ForEach(fn func(fileID uint32, count uint64) bool) {
	if c == nil || fn == nil {
		return
	}
	if c.counts != nil {
		for fileID, count := range c.counts {
			if count != 0 && !fn(fileID, count) {
				return
			}
		}
		return
	}
	for i := uint8(0); i < c.inlineN; i++ {
		entry := c.inline[i]
		if entry.count != 0 && !fn(entry.fileID, entry.count) {
			return
		}
	}
}
