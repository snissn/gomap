package freelist

import "sort"

// SnapshotPageUnusedV1 classifies bytes that an export must not read from a
// live index. The caller must retain a snapshot registry pin at oldestCommit,
// and bind this immutable generation to the newest fully published root. A
// retired page at the pinned boundary remains readable; only strictly older
// retirement can be reused. This is classification, not reuse authority.
func (g *FreelistGenerationV1) SnapshotPageUnusedV1(id, oldestCommit uint64) (bool, error) {
	if g == nil || oldestCommit == 0 || id < 2 || id >= g.highWater {
		return false, ErrGenerationFormat
	}
	chunk := lookupChunk(g.root, id>>freelistChunkShift)
	if chunk != nil {
		offset := id & (freelistChunkSize - 1)
		if chunk.isFree(offset) || (chunk.retired[offset] != 0 && chunk.retired[offset] < oldestCommit) {
			return true, nil
		}
	}
	// BeginCandidate applies these deferred retirements before allocating.
	// The canonical reservation extents are sorted and non-overlapping, so
	// check the containing extent without expanding it into per-page state.
	i := sort.Search(len(g.record.Extents), func(i int) bool { return g.record.Extents[i].StartPageID > id }) - 1
	if i >= 0 {
		extent := g.record.Extents[i]
		if id-extent.StartPageID < uint64(extent.Count) && extent.Kind == ReservationPendingMetadataRetirement {
			return extent.LastReachableCommitSeq != 0 && extent.LastReachableCommitSeq < oldestCommit, nil
		}
	}
	return false, nil
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
	if a.closed {
		return nil, ErrCandidateConsumed
	}
	g, err := a.publishedSnapshotGenerationLockedV1(expected)
	if err != nil {
		return nil, err
	}
	if g.hasFiniteBackingV1() {
		return nil, ErrFiniteAllocationExportV1
	}
	g.markOrdinaryEscapeV1()
	a.cow.rawGenerationEscaped = true
	return g, nil
}

func (a *Allocator) publishedSnapshotGenerationLockedV1(expected GenerationRefV1) (*FreelistGenerationV1, error) {
	if a.closed {
		return nil, ErrCandidateConsumed
	}
	if a.cow == nil || a.cow.generation == nil || a.cow.generation.ref != expected || a.cow.packet != nil || a.cow.prepared != nil || len(a.cow.activated) != 0 || a.cow.waitErr != nil {
		return nil, ErrCOWCandidatePrepared
	}
	if txn := a.cow.txn; txn != nil && (len(txn.allocated) != 0 || len(txn.abandonedAppends) != 0) {
		return nil, ErrCOWCandidatePrepared
	}
	return a.cow.generation, nil
}
