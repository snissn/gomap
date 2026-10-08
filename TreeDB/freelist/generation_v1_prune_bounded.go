package freelist

import (
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// BoundedPruneStats counts whole work, including trie lookups, bit inspection,
// and path cloning. Credits are checked before traversal or mutation.
type BoundedPruneStats struct {
	NodeVisits, EntriesExamined, PromotedPages, MutationPaths, MutationItems uint64
	PageCredits, ByteCredits                                                 uint64
	Wrapped                                                                  bool
}

// A lower-bound seek can visit at most two depth-14 paths plus their
// sixteen sibling summaries: 2*14*16+1 < 512.
const BoundedPruneMaxNodeVisits = 512
const BoundedPruneMaxEntries = 64
const BoundedPruneMaxPromotions = 16
const boundedPruneMutationPages = 2 * (chunkTrieDepth + 2)

// Budget includes seek, old-path identity visits, COW nodes/chunk, full chunk
// summary inspection and each internal summary recomputation.
const BoundedPruneMaxPageCredits = BoundedPruneMaxNodeVisits + boundedPruneMutationPages
const BoundedPruneMaxByteCredits = BoundedPruneMaxPageCredits*page.PageSize + freelistChunkSize*8 + chunkTrieDepth*16*8

func findPrunableChunkGE(n stateRefV1, depth int, target uint64, cap ReuseCapability, work *BoundedPruneStats) *stateChunk {
	if n.zero() || work.NodeVisits == BoundedPruneMaxNodeVisits {
		return nil
	}
	work.NodeVisits++
	if n.retiredCount() == 0 || n.maxChunk() < target || !cap.permits(n.minRetiredSeq()) {
		return nil
	}
	if n.chunk != nil {
		return n.chunk
	}
	for _, child := range n.branch.child {
		if result := findPrunableChunkGE(child, depth+1, target, cap, work); result != nil {
			return result
		}
	}
	return nil
}

// A bounded plan is transient under the transaction/allocator serializer. It
// owns no retained callback and cannot outlive a mutation of the planned tree.
// Admission precedes cursor/stat changes and shared publication transitions.
type boundedPrunePlanV1 struct {
	work                          BoundedPruneStats
	cap                           ReuseCapability
	chunkNo, nextCursor, reuseLag uint64
	promote                       [BoundedPruneMaxPromotions]uint64
	mutation                      transactionMutationPlanV1
	operation                     allocationOperationV1
}

func (t *FreelistTxn) prepareBoundedPruneV1(cap ReuseCapability) (boundedPrunePlanV1, error) {
	if t == nil || t.consumed || cap.oldestRecoverableCommitSeq == 0 {
		return boundedPrunePlanV1{}, nil
	}
	if t.allocationErr != nil {
		return boundedPrunePlanV1{}, t.allocationErr
	}
	// Pay the full scratch/control class even if the compiler keeps it on stack.
	if err := reserveMaterializationBackingV1(t.buildCreator, 1, uint64(unsafe.Sizeof(boundedPrunePlanV1{})), true); err != nil {
		return boundedPrunePlanV1{}, err
	}
	plan := boundedPrunePlanV1{cap: cap, reuseLag: t.stats.ReuseLag}
	work := &plan.work
	work.PageCredits = BoundedPruneMaxPageCredits
	work.ByteCredits = BoundedPruneMaxByteCredits
	chunk := findPrunableChunkGE(t.root, 0, t.pruneCursor>>freelistChunkShift, cap, work)
	if chunk == nil {
		work.Wrapped = true
		return plan, nil
	}
	plan.chunkNo = chunk.chunkNo
	offset := uint64(0)
	if chunk.chunkNo == t.pruneCursor>>freelistChunkShift {
		offset = t.pruneCursor & (freelistChunkSize - 1)
	}
	for offset < freelistChunkSize && work.EntriesExamined < BoundedPruneMaxEntries && work.PromotedPages < BoundedPruneMaxPromotions {
		work.EntriesExamined++
		seq := chunk.retired[offset]
		if seq != 0 && cap.permits(seq) {
			plan.promote[work.PromotedPages] = offset
			work.PromotedPages++
			if lag := cap.oldestRecoverableCommitSeq - seq; lag > plan.reuseLag {
				plan.reuseLag = lag
			}
		}
		offset++
	}
	plan.nextCursor = (chunk.chunkNo << freelistChunkShift) + offset
	if plan.nextCursor >= t.highWater {
		plan.nextCursor = 0
		work.Wrapped = true
	}
	if work.PromotedPages != 0 {
		plan.mutation = t.planMutationV1(chunk.chunkNo, 0)
		if plan.mutation.err != nil {
			return boundedPrunePlanV1{}, plan.mutation.err
		}
		operation, err := admitAllocationOperationV1(t.buildCreator, plan.mutation.bytes, plan.mutation.refs)
		if err != nil {
			return boundedPrunePlanV1{}, err
		}
		plan.operation = operation
	}
	return plan, nil
}

// No facet callback occurs during application of an admitted plan. The caller
// closes its operation after success or rejection of the shared transition.
func (t *FreelistTxn) applyBoundedPruneV1(plan *boundedPrunePlanV1) (BoundedPruneStats, error) {
	work := plan.work
	if work.PageCredits == 0 {
		return work, nil
	}
	if work.PromotedPages != 0 {
		err := t.applyMutationV1(plan.chunkNo, work.PromotedPages, func(c *stateChunk) {
			for _, off := range plan.promote[:work.PromotedPages] {
				c.retired[off] = 0
				c.setFree(off, true)
			}
		}, plan.mutation, &plan.operation)
		if err != nil {
			return BoundedPruneStats{}, err
		}
		work.MutationPaths = 1
		work.MutationItems = work.PromotedPages
	}
	t.stats.OldestRecoverableCommitSeq = plan.cap.oldestRecoverableCommitSeq
	t.stats.MinPinnedSnapshotCommitSeq = plan.cap.minPinnedSnapshotCommitSeq
	t.stats.HistoryFloorCommitSeq = plan.cap.historyFloorCommitSeq
	t.stats.PageVisits += work.NodeVisits
	t.stats.ReuseLag = plan.reuseLag
	t.pruneCursor = plan.nextCursor
	return work, nil
}

// PruneWithCapabilityBounded makes at most one bounded chunk mutation. Cursor
// omission only delays reclamation; every candidate is checked against the
// fresh opaque capability supplied by the existing root/pin owner.
func (t *FreelistTxn) PruneWithCapabilityBounded(cap ReuseCapability) BoundedPruneStats {
	plan, err := t.prepareBoundedPruneV1(cap)
	if err != nil {
		if t != nil {
			t.allocationErr = err
		}
		return BoundedPruneStats{}
	}
	defer plan.operation.close()
	work, err := t.applyBoundedPruneV1(&plan)
	if err != nil {
		t.allocationErr = err
	}
	return work
}
