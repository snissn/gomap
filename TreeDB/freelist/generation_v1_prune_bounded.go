package freelist

import "github.com/snissn/gomap/TreeDB/page"

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

func findPrunableChunkGE(n *stateNode, depth int, target uint64, cap ReuseCapability, work *BoundedPruneStats) *stateChunk {
	if n == nil || work.NodeVisits == BoundedPruneMaxNodeVisits {
		return nil
	}
	work.NodeVisits++
	if n.retiredCount == 0 || n.maxChunk < target || !cap.permits(n.minRetiredSeq) {
		return nil
	}
	if depth == chunkTrieDepth {
		return n.chunk
	}
	for _, child := range n.child {
		if result := findPrunableChunkGE(child, depth+1, target, cap, work); result != nil {
			return result
		}
	}
	return nil
}

// PruneWithCapabilityBounded makes at most one bounded chunk mutation. Cursor
// omission only delays reclamation; every candidate is checked against the
// fresh opaque capability supplied by the existing root/pin owner.
func (t *FreelistTxn) PruneWithCapabilityBounded(cap ReuseCapability) BoundedPruneStats {
	var work BoundedPruneStats
	if t == nil || t.consumed || cap.oldestRecoverableCommitSeq == 0 {
		return work
	}
	// Charge a finite upper bound before any traversal/load or path mutation.
	work.PageCredits = BoundedPruneMaxPageCredits
	work.ByteCredits = BoundedPruneMaxByteCredits
	t.stats.OldestRecoverableCommitSeq = cap.oldestRecoverableCommitSeq
	t.stats.MinPinnedSnapshotCommitSeq = cap.minPinnedSnapshotCommitSeq
	t.stats.HistoryFloorCommitSeq = cap.historyFloorCommitSeq
	chunk := findPrunableChunkGE(t.root, 0, t.pruneCursor>>freelistChunkShift, cap, &work)
	t.stats.PageVisits += work.NodeVisits
	if chunk == nil {
		t.pruneCursor = 0
		work.Wrapped = true
		return work
	}
	offset := uint64(0)
	if chunk.chunkNo == t.pruneCursor>>freelistChunkShift {
		offset = t.pruneCursor & (freelistChunkSize - 1)
	}
	var promote [BoundedPruneMaxPromotions]uint64
	for offset < freelistChunkSize && work.EntriesExamined < BoundedPruneMaxEntries && work.PromotedPages < BoundedPruneMaxPromotions {
		work.EntriesExamined++
		seq := chunk.retired[offset]
		if seq != 0 && cap.permits(seq) {
			promote[work.PromotedPages] = offset
			work.PromotedPages++
			if lag := cap.oldestRecoverableCommitSeq - seq; lag > t.stats.ReuseLag {
				t.stats.ReuseLag = lag
			}
		}
		offset++
	}
	t.pruneCursor = (chunk.chunkNo << freelistChunkShift) + offset
	// Offset 256 naturally advances to the following chunk.
	if work.PromotedPages != 0 {
		t.mutateMany(chunk.chunkNo, work.PromotedPages, func(c *stateChunk) {
			for _, off := range promote[:work.PromotedPages] {
				c.retired[off] = 0
				c.setFree(off, true)
			}
		})
		work.MutationPaths = 1
		work.MutationItems = work.PromotedPages
	}
	if t.pruneCursor >= t.highWater {
		t.pruneCursor = 0
		work.Wrapped = true
	}
	return work
}
