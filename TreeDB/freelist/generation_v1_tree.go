package freelist

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync/atomic"
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
	stateNodeCopyCapacityV1  = 352
	stateChunkCopyCapacityV1 = 2304
	freelistRegionSize       = 8192
)

type retiredPage struct {
	id, lastReachableCommitSeq uint64
}

type stateChunk struct {
	ownedRefs            uint64
	creator              *allocationCreditLeaseV1
	pageID               uint64
	checksum             uint32
	chunkNo              uint64
	retiredPages, minSeq uint64
	free                 [4]uint64
	retired              [freelistChunkSize]uint64
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

// stateRefV1 is a value owning edge. Exactly one pointer is present; no heap
// wrapper or leaf index stands between the edge and its branch/chunk.
type stateRefV1 struct {
	branch *stateNode
	chunk  *stateChunk
}
type stateNode struct {
	ownedRefs                                            uint64
	creator                                              *allocationCreditLeaseV1
	pageID                                               uint64
	checksum                                             uint32
	depth                                                uint8
	prefix                                               uint64 // first depth nibbles, right aligned; all other bits zero
	child                                                [16]stateRefV1
	freePages, retiredPages, minSeq, lowChunk, highChunk uint64
}

func (r stateRefV1) zero() bool      { return r.branch == nil && r.chunk == nil }
func (r stateRefV1) validKind() bool { return r.branch == nil || r.chunk == nil }
func (r stateRefV1) pageID() uint64 {
	if r.branch != nil {
		return r.branch.pageID
	}
	if r.chunk != nil {
		return r.chunk.pageID
	}
	return 0
}
func (r stateRefV1) checksum() uint32 {
	if r.branch != nil {
		return r.branch.checksum
	}
	if r.chunk != nil {
		return r.chunk.checksum
	}
	return 0
}
func (r stateRefV1) setIdentity(id uint64, crc uint32) {
	if r.branch != nil {
		r.branch.pageID, r.branch.checksum = id, crc
	} else if r.chunk != nil {
		r.chunk.pageID, r.chunk.checksum = id, crc
	}
}
func (r stateRefV1) freeCount() uint64 {
	if r.branch != nil {
		return r.branch.freePages
	}
	if r.chunk != nil {
		return r.chunk.freeCount()
	}
	return 0
}
func (r stateRefV1) retiredCount() uint64 {
	if r.branch != nil {
		return r.branch.retiredPages
	}
	if r.chunk != nil {
		return r.chunk.retiredPages
	}
	return 0
}
func (r stateRefV1) minRetiredSeq() uint64 {
	if r.branch != nil {
		return r.branch.minSeq
	}
	if r.chunk != nil {
		return r.chunk.minSeq
	}
	return 0
}
func (r stateRefV1) minChunk() uint64 {
	if r.branch != nil {
		return r.branch.lowChunk
	}
	if r.chunk != nil {
		return r.chunk.chunkNo
	}
	return 0
}
func (r stateRefV1) maxChunk() uint64 {
	if r.branch != nil {
		return r.branch.highChunk
	}
	if r.chunk != nil {
		return r.chunk.chunkNo
	}
	return 0
}
func (r stateRefV1) creator() *allocationCreditLeaseV1 {
	if r.branch != nil {
		return r.branch.creator
	}
	if r.chunk != nil {
		return r.chunk.creator
	}
	return nil
}
func (r stateRefV1) ownedRefs() uint64 {
	if r.branch != nil {
		return atomic.LoadUint64(&r.branch.ownedRefs)
	}
	if r.chunk != nil {
		return atomic.LoadUint64(&r.chunk.ownedRefs)
	}
	return 0
}
func (r stateRefV1) class() uint64 {
	if r.chunk != nil {
		return stateChunkCopyCapacityV1
	}
	return stateNodeCopyCapacityV1
}
func (r stateRefV1) kind() byte {
	if r.chunk != nil {
		return 1
	}
	return 0
}
func (r stateRefV1) containsChunk(key uint64) bool {
	if r.chunk != nil {
		return r.chunk.chunkNo == key
	}
	if r.branch == nil || r.freeCount()+r.retiredCount() == 0 {
		return false
	}
	return key>>(4*(chunkTrieDepth-int(r.branch.depth))) == r.branch.prefix
}
func firstDifferingDepthV1(a, b uint64) int {
	if a == b {
		return chunkTrieDepth
	}
	return chunkTrieDepth - (bits.Len64(a^b)+3)/4
}
func chunkPrefixV1(key uint64, depth int) uint64 { return key >> uint(4*(chunkTrieDepth-depth)) }
func chunkNibble(chunkNo uint64, depth int) int {
	return int(chunkNo >> uint((chunkTrieDepth-1-depth)*4) & 15)
}
func refreshChunkSummaryV1(c *stateChunk) { c.retiredPages, c.minSeq = c.retiredSummary() }
func recomputeStateNode(n *stateNode, _ int) {
	n.freePages, n.retiredPages, n.minSeq, n.lowChunk, n.highChunk = 0, 0, 0, 0, 0
	first := true
	for _, child := range n.child {
		if child.zero() {
			continue
		}
		n.freePages += child.freeCount()
		n.retiredPages += child.retiredCount()
		if seq := child.minRetiredSeq(); seq != 0 && (n.minSeq == 0 || seq < n.minSeq) {
			n.minSeq = seq
		}
		if first || child.minChunk() < n.lowChunk {
			n.lowChunk = child.minChunk()
		}
		if first || child.maxChunk() > n.highChunk {
			n.highChunk = child.maxChunk()
		}
		first = false
	}
}
func cloneStateNode(r stateRefV1) stateRefV1 {
	out, err := cloneStateRefOwnedV1(r, false, nil)
	if err != nil {
		panic(err)
	}
	return out
}
func detachUnmaterialized(r stateRefV1, depth int) stateRefV1 {
	return detachUnmaterializedWithStats(r, depth, nil)
}
func detachUnmaterializedWithStats(r stateRefV1, depth int, stats *FreelistTxnStats) stateRefV1 {
	out, err := detachUnmaterializedOwnedV1(r, depth, stats, nil)
	if err != nil {
		panic(err)
	}
	return out
}

// The creating facet is supplied by the current serialized builder, never
// inferred from a retained generation whose request may already have retired.
func detachUnmaterializedOwnedV1(r stateRefV1, depth int, stats *FreelistTxnStats, creator *allocationCreditLeaseV1) (stateRefV1, error) {
	plan := dirtyStateBirthPlanV1(r, depth)
	op, err := admitAllocationOperationV1(creator, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		return stateRefV1{}, err
	}
	defer op.close()
	return detachStateOwnedOperationV1(r, depth, stats, &op)
}
func mutateChunk(r stateRefV1, key uint64, depth int, f func(*stateChunk)) stateRefV1 {
	return mutateChunkForPreparation(r, key, depth, f, false, nil)
}
func mutateChunkForPreparation(r stateRefV1, key uint64, depth int, f func(*stateChunk), private bool, stats *FreelistTxnStats) stateRefV1 {
	plan := mutationStateBirthPlanV1(r, key, depth, private, false, false)
	op, err := admitAllocationOperationV1(nil, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		panic(err)
	}
	defer op.close()
	out, err := mutateStateOwnedOperationV1(r, key, depth, f, private, stats, &op)
	if err != nil {
		panic(err)
	}
	return out
}
func lookupChunk(r stateRefV1, key uint64) *stateChunk {
	for !r.zero() {
		if r.chunk != nil {
			if r.chunk.chunkNo == key {
				return r.chunk
			}
			return nil
		}
		if !r.containsChunk(key) {
			return nil
		}
		r = r.branch.child[chunkNibble(key, int(r.branch.depth))]
	}
	return nil
}
func rightmostFree(r stateRefV1, depth int, visits *uint64) *stateChunk {
	if r.zero() || r.freeCount() == 0 {
		return nil
	}
	*visits++
	if r.chunk != nil {
		return r.chunk
	}
	for i := 15; i >= 0; i-- {
		if c := rightmostFree(r.branch.child[i], depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}
func leftmostFree(r stateRefV1, depth int, visits *uint64) *stateChunk {
	if r.zero() || r.freeCount() == 0 {
		return nil
	}
	*visits++
	if r.chunk != nil {
		return r.chunk
	}
	for i := 0; i < 16; i++ {
		if c := leftmostFree(r.branch.child[i], depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}
func findFreeLE(r stateRefV1, target uint64, depth int, visits *uint64) *stateChunk {
	if r.zero() || r.freeCount() == 0 || r.minChunk() > target {
		return nil
	}
	*visits++
	if r.maxChunk() <= target {
		return rightmostFree(r, depth, visits)
	}
	if r.chunk != nil {
		return r.chunk
	}
	for i := 15; i >= 0; i-- {
		if c := findFreeLE(r.branch.child[i], target, depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}
func findFreeGE(r stateRefV1, target uint64, depth int, visits *uint64) *stateChunk {
	if r.zero() || r.freeCount() == 0 || r.maxChunk() < target {
		return nil
	}
	*visits++
	if r.minChunk() >= target {
		return leftmostFree(r, depth, visits)
	}
	if r.chunk != nil {
		return r.chunk
	}
	for i := 0; i < 16; i++ {
		if c := findFreeGE(r.branch.child[i], target, depth+1, visits); c != nil {
			return c
		}
	}
	return nil
}
func chooseFreeChunk(root stateRefV1, pageHint uint64, visits *uint64) *stateChunk {
	hint := pageHint >> freelistChunkShift
	lo, hi := findFreeLE(root, hint, 0, visits), findFreeGE(root, hint, 0, visits)
	if lo == nil {
		return hi
	}
	if hi == nil {
		return lo
	}
	loRegion, hiRegion, hintRegion := (lo.chunkNo<<freelistChunkShift)/freelistRegionSize, (hi.chunkNo<<freelistChunkShift)/freelistRegionSize, pageHint/freelistRegionSize
	ld, hd := absDiff(loRegion, hintRegion), absDiff(hiRegion, hintRegion)
	if hd < ld || (hd == ld && hi.chunkNo >= lo.chunkNo) {
		return hi
	}
	return lo
}
func chooseUnreservedFreePage(root stateRefV1, pageHint uint64, ledger *ReservationLedger, visits *uint64) (uint64, bool) {
	preferred := chooseFreeChunk(root, pageHint, visits)
	if offset, ok := preferred.highestFreeUnreserved(ledger); ok {
		return preferred.chunkNo<<freelistChunkShift | offset, true
	}
	hint := pageHint / freelistRegionSize
	var best *stateChunk
	var bestOffset, bestDistance uint64
	_ = walkState(root, 0, func(c *stateChunk) error {
		if c == preferred {
			return nil
		}
		offset, ok := c.highestFreeUnreserved(ledger)
		if !ok {
			return nil
		}
		*visits++
		distance := absDiff((c.chunkNo<<freelistChunkShift)/freelistRegionSize, hint)
		if best == nil || distance < bestDistance || (distance == bestDistance && c.chunkNo > best.chunkNo) {
			best, bestOffset, bestDistance = c, offset, distance
		}
		return nil
	})
	if best == nil {
		return 0, false
	}
	return best.chunkNo<<freelistChunkShift | bestOffset, true
}
func walkState(r stateRefV1, depth int, f func(*stateChunk) error) error {
	if r.zero() {
		return nil
	}
	if r.chunk != nil {
		return f(r.chunk)
	}
	for _, child := range r.branch.child {
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

// validateCanonicalStateV2 checks the mutable shape independently of emission.
func validateCanonicalStateV2(r stateRefV1, root bool) error {
	if !r.validKind() {
		return ErrGenerationFormat
	}
	if r.zero() {
		if root {
			return nil
		}
		return ErrGenerationFormat
	}
	if r.chunk != nil {
		retired, minimum := r.chunk.retiredSummary()
		if r.chunk.chunkNo >= 1<<56 || r.chunk.freeCount()+retired == 0 ||
			r.chunk.retiredPages != retired || r.chunk.minSeq != minimum {
			return ErrGenerationFormat
		}
		return nil
	}
	n := r.branch
	depth := int(n.depth)
	if depth >= chunkTrieDepth || n.prefix>>uint(4*depth) != 0 {
		return ErrGenerationFormat
	}
	count := 0
	for slot, child := range n.child {
		if child.zero() {
			continue
		}
		count++
		if err := validateCanonicalStateV2(child, false); err != nil {
			return err
		}
		if chunkPrefixV1(child.minChunk(), depth) != n.prefix || chunkPrefixV1(child.maxChunk(), depth) != n.prefix ||
			chunkNibble(child.minChunk(), depth) != slot || chunkNibble(child.maxChunk(), depth) != slot ||
			child.branch != nil && int(child.branch.depth) <= depth {
			return ErrGenerationFormat
		}
	}
	summary := stateNode{child: n.child}
	recomputeStateNode(&summary, depth)
	if n.freePages != summary.freePages || n.retiredPages != summary.retiredPages || n.minSeq != summary.minSeq ||
		n.lowChunk != summary.lowChunk || n.highChunk != summary.highChunk {
		return ErrGenerationFormat
	}
	if count == 0 {
		if !root || depth != 0 || n.prefix != 0 {
			return ErrGenerationFormat
		}
	} else if count < 2 || firstDifferingDepthV1(n.lowChunk, n.highChunk) != depth {
		return ErrGenerationFormat
	}
	return nil
}
