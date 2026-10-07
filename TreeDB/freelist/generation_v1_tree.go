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
	// Conservative allocation-class capacities; structural tests cover their
	// field-layout bounds. Ownership needs no map, token, or per-node field.
	stateNodeCopyCapacityV1  = 208
	stateChunkCopyCapacityV1 = 2304
	freelistRegionSize       = 8192
)

type retiredPage struct {
	id, lastReachableCommitSeq uint64
}

type stateChunk struct {
	ownedRefs uint64
	creator   *allocationCreditLeaseV1
	pageID    uint64
	checksum  uint32
	chunkNo   uint64
	free      [4]uint64
	retired   [freelistChunkSize]uint64
}

func (c *stateChunk) clone() *stateChunk {
	out, err := cloneStateChunkOwnedV1(c, false, nil)
	if err != nil {
		panic(err)
	}
	return out
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
	ownedRefs                         uint64
	creator                           *allocationCreditLeaseV1
	pageID                            uint64
	checksum                          uint32
	child                             [16]*stateNode
	chunk                             *stateChunk
	freeCount, retiredCount           uint64
	minRetiredSeq, minChunk, maxChunk uint64
}

func cloneStateNode(n *stateNode) *stateNode {
	out, err := cloneStateNodeOwnedV1(n, false, nil)
	if err != nil {
		panic(err)
	}
	return out
}

// detachUnmaterialized clones only nodes that do not yet have durable page
// identities. Durable nodes may hide zero-ID descendants; sharing remains
// safe only while they are immutable. A private first copy isolates those
// descendants before exposing the copied node for mutation.
func detachUnmaterialized(n *stateNode, depth int) *stateNode {
	return detachUnmaterializedWithStats(n, depth, nil)
}

// Count the work of isolating aliases before giving a preparation exclusive
// ownership of reachable unmaterialized objects. Durable identities stop this
// traversal; each later durable-to-private first copy repeats isolation at its
// child boundary. No ownership flag enters the resulting generation tree.
func detachUnmaterializedWithStats(n *stateNode, depth int, stats *FreelistTxnStats) *stateNode {
	plan := dirtyStateBirthPlanV1(n, depth)
	operation, err := admitAllocationOperationV1(nil, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		panic(err)
	}
	defer operation.close()
	out, err := detachStateOwnedOperationV1(n, depth, stats, &operation)
	if err != nil {
		panic(err)
	}
	return out
}

// A shallow copy of a durable node still shares its zero-ID descendants.
// Isolate immediate-child subtrees through the next durable boundary, even
// when an empty summary caused emission to skip assigning their identities.
// The same rule covers a zero-ID chunk retained under a durable leaf.
func detachStateNodeChildrenWithStats(out *stateNode, depth int, stats *FreelistTxnStats) {
	plan := stateBirthPlanV1{}
	if depth == chunkTrieDepth {
		if out.chunk != nil && out.chunk.pageID == 0 {
			plan.chunks++
		}
	} else {
		for _, child := range out.child {
			plan.add(dirtyStateBirthPlanV1(child, depth+1))
		}
	}
	operation, err := admitAllocationOperationV1(nil, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		panic(err)
	}
	defer operation.close()
	if err = detachChildrenOwnedOperationV1(out, depth, stats, &operation); err != nil {
		panic(err)
	}
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

// mutateChunk retains the persistent mutation semantics for ordinary callers
// and the preparation reference oracle.
func mutateChunk(n *stateNode, chunkNo uint64, depth int, f func(*stateChunk)) *stateNode {
	return mutateChunkForPreparation(n, chunkNo, depth, f, false, nil)
}

// private permits in-place edits only after isolation at the staging boundary
// or the nearest durable-to-private first copy. Zero identity alone is not
// proof: durable nodes can retain shared, empty zero-ID descendants.
func mutateChunkForPreparation(n *stateNode, chunkNo uint64, depth int, f func(*stateChunk), private bool, stats *FreelistTxnStats) *stateNode {
	plan := mutationStateBirthPlanV1(n, chunkNo, depth, private, false, false)
	operation, err := admitAllocationOperationV1(nil, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		panic(err)
	}
	defer operation.close()
	out, err := mutateStateOwnedOperationV1(n, chunkNo, depth, f, private, stats, &operation)
	if err != nil {
		panic(err)
	}
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
