package freelist

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
)

var (
	ErrGenerationChecksum = errors.New("freelist generation checksum mismatch")
	ErrGenerationDigest   = errors.New("freelist generation digest mismatch")
	ErrGenerationFormat   = errors.New("invalid freelist generation format")
	ErrGenerationParent   = errors.New("stale freelist generation parent")
	ErrPageReserved       = errors.New("freelist page is reserved by a visible candidate")
	ErrNoAllocatablePage  = errors.New("no allocatable freelist page")
	ErrCandidateConsumed  = errors.New("freelist candidate transaction already materialized")
)

const (
	freelistChunkShift = 8
	freelistChunkSize  = 1 << freelistChunkShift
	chunkTrieDepth     = (64 - freelistChunkShift) / 4
	freelistRegionSize = 8192
)

type retiredPage struct {
	id, lastReachableCommitSeq uint64
}

type stateChunk struct {
	pageID   uint64
	checksum uint32
	chunkNo  uint64
	free     [4]uint64
	retired  [freelistChunkSize]uint64
}

func (c *stateChunk) clone() *stateChunk {
	if c == nil {
		return &stateChunk{}
	}
	out := *c
	out.pageID = 0
	out.checksum = 0
	return &out
}

func (c *stateChunk) freeCount() uint64 {
	if c == nil {
		return 0
	}
	return uint64(bits.OnesCount64(c.free[0]) + bits.OnesCount64(c.free[1]) + bits.OnesCount64(c.free[2]) + bits.OnesCount64(c.free[3]))
}

func (c *stateChunk) retiredSummary() (uint64, uint64) {
	var count, minSeq uint64
	if c == nil {
		return 0, 0
	}
	for _, seq := range c.retired {
		if seq == 0 {
			continue
		}
		count++
		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}
	}
	return count, minSeq
}

func (c *stateChunk) isFree(offset uint64) bool {
	return c != nil && c.free[offset/64]&(uint64(1)<<(offset%64)) != 0
}

func (c *stateChunk) setFree(offset uint64, free bool) {
	mask := uint64(1) << (offset % 64)
	if free {
		c.free[offset/64] |= mask
		c.retired[offset] = 0
	} else {
		c.free[offset/64] &^= mask
	}
}

func (c *stateChunk) highestFree() (uint64, bool) {
	for word := len(c.free) - 1; word >= 0; word-- {
		if c.free[word] == 0 {
			continue
		}
		return uint64(word*64 + 63 - bits.LeadingZeros64(c.free[word])), true
	}
	return 0, false
}

func (c *stateChunk) highestFreeUnreserved(ledger *ReservationLedger) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	for word := len(c.free) - 1; word >= 0; word-- {
		bitsLeft := c.free[word]
		for bitsLeft != 0 {
			offset := uint64(word*64 + 63 - bits.LeadingZeros64(bitsLeft))
			id := c.chunkNo<<freelistChunkShift | offset
			if ledger == nil || !ledger.Reserved(id) {
				return offset, true
			}
			bitsLeft &^= uint64(1) << (offset % 64)
		}
	}
	return 0, false
}

type stateNode struct {
	pageID                            uint64
	checksum                          uint32
	child                             [16]*stateNode
	chunk                             *stateChunk
	freeCount, retiredCount           uint64
	minRetiredSeq, minChunk, maxChunk uint64
}

func cloneStateNode(n *stateNode) *stateNode {
	out := &stateNode{}
	if n != nil {
		*out = *n
	}
	out.pageID = 0
	out.checksum = 0
	return out
}

// detachUnmaterialized clones only nodes that do not yet have durable page
// identities. Nodes with identities are immutable and safe to share.
func detachUnmaterialized(n *stateNode, depth int) *stateNode {
	if n == nil || n.pageID != 0 {
		return n
	}
	out := *n
	if depth == chunkTrieDepth {
		if n.chunk != nil && n.chunk.pageID == 0 {
			chunk := *n.chunk
			out.chunk = &chunk
		}
		return &out
	}
	for i, child := range n.child {
		out.child[i] = detachUnmaterialized(child, depth+1)
	}
	return &out
}

func chunkNibble(chunkNo uint64, depth int) int {
	return int((chunkNo >> uint((chunkTrieDepth-1-depth)*4)) & 0xf)
}

func recomputeStateNode(n *stateNode, depth int) {
	n.freeCount, n.retiredCount, n.minRetiredSeq = 0, 0, 0
	n.minChunk, n.maxChunk = 0, 0
	if depth == chunkTrieDepth {
		if n.chunk == nil {
			return
		}
		n.freeCount = n.chunk.freeCount()
		n.retiredCount, n.minRetiredSeq = n.chunk.retiredSummary()
		n.minChunk, n.maxChunk = n.chunk.chunkNo, n.chunk.chunkNo
		return
	}
	first := true
	for _, child := range n.child {
		if child == nil || child.freeCount+child.retiredCount == 0 {
			continue
		}
		n.freeCount += child.freeCount
		n.retiredCount += child.retiredCount
		if child.minRetiredSeq != 0 && (n.minRetiredSeq == 0 || child.minRetiredSeq < n.minRetiredSeq) {
			n.minRetiredSeq = child.minRetiredSeq
		}
		if first || child.minChunk < n.minChunk {
			n.minChunk = child.minChunk
		}
		if first || child.maxChunk > n.maxChunk {
			n.maxChunk = child.maxChunk
		}
		first = false
	}
}

func mutateChunk(n *stateNode, chunkNo uint64, depth int, f func(*stateChunk)) *stateNode {
	out := cloneStateNode(n)
	if depth == chunkTrieDepth {
		chunk := out.chunk.clone()
		chunk.chunkNo = chunkNo
		f(chunk)
		if chunk.freeCount() == 0 {
			retired, _ := chunk.retiredSummary()
			if retired == 0 {
				out.chunk = nil
				recomputeStateNode(out, depth)
				return out
			}
		}
		out.chunk = chunk
		recomputeStateNode(out, depth)
		return out
	}
	i := chunkNibble(chunkNo, depth)
	out.child[i] = mutateChunk(out.child[i], chunkNo, depth+1, f)
	recomputeStateNode(out, depth)
	return out
}

func lookupChunk(n *stateNode, chunkNo uint64) *stateChunk {
	for depth := 0; n != nil && depth < chunkTrieDepth; depth++ {
		n = n.child[chunkNibble(chunkNo, depth)]
	}
	if n == nil {
		return nil
	}
	return n.chunk
}

func rightmostFree(n *stateNode, depth int, visits *uint64) *stateChunk {
	if n == nil || n.freeCount == 0 {
		return nil
	}
	*visits++
	if depth == chunkTrieDepth {
		return n.chunk
	}
	for i := len(n.child) - 1; i >= 0; i-- {
		if c := rightmostFree(n.child[i], depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}

func leftmostFree(n *stateNode, depth int, visits *uint64) *stateChunk {
	if n == nil || n.freeCount == 0 {
		return nil
	}
	*visits++
	if depth == chunkTrieDepth {
		return n.chunk
	}
	for i := 0; i < len(n.child); i++ {
		if c := leftmostFree(n.child[i], depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}

func findFreeLE(n *stateNode, target uint64, depth int, visits *uint64) *stateChunk {
	if n == nil || n.freeCount == 0 || n.minChunk > target {
		return nil
	}
	*visits++
	if n.maxChunk <= target {
		return rightmostFree(n, depth, visits)
	}
	if depth == chunkTrieDepth {
		return n.chunk
	}
	for i := len(n.child) - 1; i >= 0; i-- {
		if c := findFreeLE(n.child[i], target, depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}

func findFreeGE(n *stateNode, target uint64, depth int, visits *uint64) *stateChunk {
	if n == nil || n.freeCount == 0 || n.maxChunk < target {
		return nil
	}
	*visits++
	if n.minChunk >= target {
		return leftmostFree(n, depth, visits)
	}
	if depth == chunkTrieDepth {
		return n.chunk
	}
	for i := 0; i < len(n.child); i++ {
		if c := findFreeGE(n.child[i], target, depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}

func chooseFreeChunk(root *stateNode, pageHint uint64, visits *uint64) *stateChunk {
	chunkHint := pageHint >> freelistChunkShift
	lo, hi := findFreeLE(root, chunkHint, 0, visits), findFreeGE(root, chunkHint, 0, visits)
	if lo == nil {
		return hi
	}
	if hi == nil {
		return lo
	}
	loRegion, hiRegion, hintRegion := (lo.chunkNo<<freelistChunkShift)/freelistRegionSize, (hi.chunkNo<<freelistChunkShift)/freelistRegionSize, pageHint/freelistRegionSize
	loDistance, hiDistance := absDiff(loRegion, hintRegion), absDiff(hiRegion, hintRegion)
	if hiDistance < loDistance || (hiDistance == loDistance && hi.chunkNo >= lo.chunkNo) {
		return hi
	}
	return lo
}

func chooseUnreservedFreePage(root *stateNode, pageHint uint64, ledger *ReservationLedger, visits *uint64) (uint64, bool) {
	preferred := chooseFreeChunk(root, pageHint, visits)
	if offset, ok := preferred.highestFreeUnreserved(ledger); ok {
		return preferred.chunkNo<<freelistChunkShift | offset, true
	}

	hintRegion := pageHint / freelistRegionSize
	var best *stateChunk
	var bestOffset, bestDistance uint64
	_ = walkState(root, 0, func(chunk *stateChunk) error {
		if chunk == preferred {
			return nil
		}
		offset, ok := chunk.highestFreeUnreserved(ledger)
		if !ok {
			return nil
		}
		*visits++
		distance := absDiff((chunk.chunkNo<<freelistChunkShift)/freelistRegionSize, hintRegion)
		if best == nil || distance < bestDistance || (distance == bestDistance && chunk.chunkNo > best.chunkNo) {
			best, bestOffset, bestDistance = chunk, offset, distance
		}
		return nil
	})
	if best == nil {
		return 0, false
	}
	return best.chunkNo<<freelistChunkShift | bestOffset, true
}

func walkState(n *stateNode, depth int, f func(*stateChunk) error) error {
	if n == nil {
		return nil
	}
	if depth == chunkTrieDepth {
		if n.chunk != nil {
			return f(n.chunk)
		}
		return nil
	}
	for _, child := range n.child {
		if err := walkState(child, depth+1, f); err != nil {
			return err
		}
	}
	return nil
}

func absDiff(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}

func safeIncrement(v uint64) (uint64, error) {
	if v == math.MaxUint64 {
		return 0, ErrNoAllocatablePage
	}
	return v + 1, nil
}

func validateManagedPageID(id, highWater uint64) error {
	if id < 2 || id >= highWater {
		return fmt.Errorf("%w: page %d outside [2,%d)", ErrGenerationFormat, id, highWater)
	}
	return nil
}
