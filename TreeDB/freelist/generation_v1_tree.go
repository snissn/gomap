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
	depth                             uint8
	prefix                            uint64 // first depth nibbles, right aligned
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
func detachUnmaterialized(n *stateNode, _ int) *stateNode {
	var work stateDetachWorkV1
	work.begin(n)
	_, err := work.step(nil)
	if err != nil {
		panic(err)
	}
	return work.result
}

func chunkNibble(chunkNo uint64, depth int) int {
	return int((chunkNo >> uint((chunkTrieDepth-1-depth)*4)) & 0xf)
}
func chunkPrefixV1(key uint64, depth int) uint64 {
	return key >> uint(4*(chunkTrieDepth-depth))
}
func firstDifferingDepthV1(a, b uint64) int {
	if a == b {
		return chunkTrieDepth
	}
	return chunkTrieDepth - (bits.Len64(a^b)+3)/4
}
func recomputeStateNode(n *stateNode, _ int) {
	work := stateSummaryWorkV1{node: n}
	_, err := work.step(nil)
	if err != nil {
		panic(err)
	}
}

func mutateChunk(n *stateNode, key uint64, depth int, f func(*stateChunk)) *stateNode {
	return mutateChunkMode(n, key, depth, f, false, nil)
}

// mutatePrivateChunk may reuse unpublished nodes only when the caller owns
// the complete dirty graph exclusively. PageID zero alone grants no permission.
func mutatePrivateChunk(n *stateNode, key uint64, depth int, f func(*stateChunk)) *stateNode {
	return mutateChunkMode(n, key, depth, f, true, nil)
}
func mutatePrivateChunkWithStats(n *stateNode, key uint64, depth int, f func(*stateChunk), stats *FreelistTxnStats) *stateNode {
	return mutateChunkMode(n, key, depth, f, true, stats)
}
func mutateChunkMode(n *stateNode, key uint64, _ int, f func(*stateChunk), private bool, stats *FreelistTxnStats) *stateNode {
	var work stateMutationWorkV1
	work.begin(n, key, private, stats, f, nil)
	_, err := work.step(nil)
	if err != nil {
		panic(err)
	}
	return work.result
}

func lookupChunk(n *stateNode, key uint64) *stateChunk {
	for n != nil {
		if n.chunk != nil {
			if n.chunk.chunkNo == key {
				return n.chunk
			}
			return nil
		}
		if n.freeCount+n.retiredCount == 0 || chunkPrefixV1(key, int(n.depth)) != n.prefix {
			return nil
		}
		n = n.child[chunkNibble(key, int(n.depth))]
	}
	return nil
}

// validateCanonicalStateV2 independently checks shape and cached summaries.
func validateCanonicalStateV2(n *stateNode, root bool) error {
	if n == nil {
		return ErrGenerationFormat
	}
	if n.chunk != nil {
		if n.chunk.chunkNo >= 1<<56 || n.pageID != n.chunk.pageID || n.checksum != n.chunk.checksum {
			return ErrGenerationFormat
		}
		for _, child := range n.child {
			if child != nil {
				return ErrGenerationFormat
			}
		}
		if n.freeCount+n.retiredCount == 0 {
			return ErrGenerationFormat
		}
	} else {
		depth := int(n.depth)
		if depth >= chunkTrieDepth || n.prefix>>uint(4*depth) != 0 {
			return ErrGenerationFormat
		}
		count := 0
		for slot, child := range n.child {
			if child == nil {
				continue
			}
			count++
			if err := validateCanonicalStateV2(child, false); err != nil {
				return err
			}
			if chunkPrefixV1(child.minChunk, depth) != n.prefix || chunkPrefixV1(child.maxChunk, depth) != n.prefix ||
				chunkNibble(child.minChunk, depth) != slot || chunkNibble(child.maxChunk, depth) != slot ||
				child.chunk == nil && int(child.depth) <= depth {
				return ErrGenerationFormat
			}
		}
		if count == 0 {
			if !root || depth != 0 || n.prefix != 0 {
				return ErrGenerationFormat
			}
		} else if count < 2 || firstDifferingDepthV1(n.minChunk, n.maxChunk) != depth {
			return ErrGenerationFormat
		}
	}
	summary := *n
	recomputeStateNode(&summary, 0)
	if summary.freeCount != n.freeCount || summary.retiredCount != n.retiredCount ||
		summary.minRetiredSeq != n.minRetiredSeq || summary.minChunk != n.minChunk || summary.maxChunk != n.maxChunk {
		return ErrGenerationFormat
	}
	return nil
}

func rightmostFree(n *stateNode, depth int, visits *uint64) *stateChunk {
	if n == nil || n.freeCount == 0 {
		return nil
	}
	*visits++
	if n.chunk != nil {
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
	if n.chunk != nil {
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
	if n.chunk != nil {
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
	if n.chunk != nil {
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
	if n.chunk != nil {
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
