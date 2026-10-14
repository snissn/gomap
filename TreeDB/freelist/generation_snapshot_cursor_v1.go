package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// SnapshotPageClassifierV1 resumes classification of one exact immutable DATA
// generation. The caller owns a registry pin and generation lease throughout;
// this cursor supplies neither reuse nor mutable-current publication authority.
type SnapshotPageClassifierV1 struct {
	generation    *FreelistGenerationV1
	node          *stateNode
	id, oldest    uint64
	depth, lo, hi int
	phase         uint8
	unused        bool
}

func (c *SnapshotPageClassifierV1) Reset(g *FreelistGenerationV1, id, oldest uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(FreelistGenerationV1{}))) {
		return false, nil
	}
	if g == nil || oldest == 0 || id < 2 || id >= g.highWater {
		return false, ErrGenerationFormat
	}
	*c = SnapshotPageClassifierV1{generation: g, node: g.root, id: id, oldest: oldest}
	return true, nil
}

// Step inspects at most one trie, chunk, extent-table or binary extent operand.
// Ready results have exactly the same meaning as SnapshotPageUnusedWithWorkV1.
func (c *SnapshotPageClassifierV1) Step(w *iterator.OrdinalScanWork) (ready, progressed, unused bool, err error) {
	if c == nil || c.generation == nil {
		return false, false, false, ErrGenerationFormat
	}
	if c.phase == 4 {
		return true, false, c.unused, nil
	}
	cost := 2 * uint64(unsafe.Sizeof(*c))
	switch c.phase {
	case 0:
		if c.node == nil || c.depth == chunkTrieDepth {
			if w != nil && !w.Reserve(1, cost) {
				return false, false, false, nil
			}
			c.phase = 1
			return false, true, false, nil
		}
		cost += uint64(unsafe.Sizeof(stateNode{}))
		if w != nil && !w.Reserve(1, cost) {
			return false, false, false, nil
		}
		c.node = c.node.child[chunkNibble(c.id>>freelistChunkShift, c.depth)]
		c.depth++
	case 1:
		cost += uint64(unsafe.Sizeof(stateNode{})) + uint64(unsafe.Sizeof(stateChunk{}))
		if w != nil && !w.Reserve(1, cost) {
			return false, false, false, nil
		}
		if c.node != nil && c.node.chunk != nil {
			chunk := c.node.chunk
			offset := c.id & (freelistChunkSize - 1)
			if chunk.isFree(offset) || (chunk.retired[offset] != 0 && chunk.retired[offset] < c.oldest) {
				c.unused = true
				c.phase = 4
				return true, true, true, nil
			}
		}
		c.phase = 2
	case 2:
		cost += 2 * uint64(unsafe.Sizeof(c.generation.record))
		if w != nil && !w.Reserve(1, cost) {
			return false, false, false, nil
		}
		c.lo = 0
		c.hi = len(c.generation.record.Extents)
		c.phase = 3
	case 3:
		cost += uint64(unsafe.Sizeof(ReservationExtentV1{}))
		if w != nil && !w.Reserve(1, cost) {
			return false, false, false, nil
		}
		if c.lo < c.hi {
			mid := c.lo + (c.hi-c.lo)/2
			if c.generation.record.Extents[mid].StartPageID > c.id {
				c.hi = mid
			} else {
				c.lo = mid + 1
			}
		} else {
			if c.lo > 0 {
				extent := c.generation.record.Extents[c.lo-1]
				if c.id-extent.StartPageID < uint64(extent.Count) && extent.Kind == ReservationPendingMetadataRetirement {
					c.unused = extent.LastReachableCommitSeq != 0 && extent.LastReachableCommitSeq < c.oldest
				}
			}
			c.phase = 4
			return true, true, c.unused, nil
		}
	default:
		return false, false, false, ErrGenerationFormat
	}
	return false, true, false, nil
}
