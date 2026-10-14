package freelist

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"unsafe"
)

// SnapshotPageUnusedV1 classifies bytes that an export must not read from a
// live index. The caller must retain a snapshot registry pin at oldestCommit,
// and bind this immutable generation to the newest fully published root. A
// retired page at the pinned boundary remains readable; only strictly older
// retirement can be reused. This is classification, not reuse authority.
func (g *FreelistGenerationV1) SnapshotPageUnusedV1(id, oldestCommit uint64) (bool, error) {
	unused, _, err := g.SnapshotPageUnusedWithWorkV1(id, oldestCommit, nil)
	return unused, err
}

// SnapshotPageUnusedWithWorkV1 charges every immutable trie node and canonical
// reservation-extent operand actually inspected. It never hides a full metadata
// classifier beneath a scalar reservation. Under-admission changes no state.
func (g *FreelistGenerationV1) SnapshotPageUnusedWithWorkV1(id, oldestCommit uint64, w *iterator.OrdinalScanWork) (bool, bool, error) {
	if w != nil && !w.Reserve(1, 2*uint64(unsafe.Sizeof(FreelistGenerationV1{}))) {
		return false, false, nil
	}
	if g == nil || oldestCommit == 0 || id < 2 || id >= g.highWater {
		return false, false, ErrGenerationFormat
	}
	n := g.root
	for depth := 0; n != nil && depth < chunkTrieDepth; depth++ {
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(stateNode{}))) {
			return false, false, nil
		}
		n = n.child[chunkNibble(id>>freelistChunkShift, depth)]
	}
	if n != nil {
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(stateNode{}))+uint64(unsafe.Sizeof(stateChunk{}))) {
			return false, false, nil
		}
		chunk := n.chunk
		if chunk != nil {
			offset := id & (freelistChunkSize - 1)
			if chunk.isFree(offset) || (chunk.retired[offset] != 0 && chunk.retired[offset] < oldestCommit) {
				return true, true, nil
			}
		}
	}
	lo, hi := 0, len(g.record.Extents)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(ReservationExtentV1{}))) {
			return false, false, nil
		}
		if g.record.Extents[mid].StartPageID > id {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	if lo > 0 {
		if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(ReservationExtentV1{}))) {
			return false, false, nil
		}
		extent := g.record.Extents[lo-1]
		if id-extent.StartPageID < uint64(extent.Count) && extent.Kind == ReservationPendingMetadataRetirement {
			return extent.LastReachableCommitSeq != 0 && extent.LastReachableCommitSeq < oldestCommit, true, nil
		}
	}
	return false, true, nil
}

// PublishedSnapshotGenerationV1 refuses allocation/publication work that could
// have acquired reuse authority before the export's registry pin. The DB must
// hold writer and durable-publication admission while acquiring this value.
func (a *Allocator) PublishedSnapshotGenerationV1(expected GenerationRefV1) (*FreelistGenerationV1, error) {
	if a == nil {
		return nil, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.generation == nil || a.cow.generation.ref != expected || a.cow.prepared != nil || len(a.cow.activated) != 0 || a.cow.waitErr != nil {
		return nil, ErrCOWCandidatePrepared
	}
	if txn := a.cow.txn; txn != nil && (len(txn.allocated) != 0 || len(txn.abandonedAppends) != 0) {
		return nil, ErrCOWCandidatePrepared
	}
	return a.cow.generation, nil
}
