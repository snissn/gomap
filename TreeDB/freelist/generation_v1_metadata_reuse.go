package freelist

// A bounded placement attempt, not a full free-set search. Each visited chunk
// has 256 bits; larger/fragmented requests retain the existing tail fallback.
const metadataReuseChunkAttempts = 4

func (t *FreelistTxn) reusableChunkRun(chunk *stateChunk) (start, count uint64) {
	t.ledger.mu.Lock()
	defer t.ledger.mu.Unlock()
	var run, runStart uint64
	for offset := uint64(0); offset < freelistChunkSize; offset++ {
		id := chunk.chunkNo<<freelistChunkShift | offset
		if chunk.isFree(offset) && !t.ledger.reservedLocked(id) {
			if run == 0 {
				runStart = id
			}
			run++
			if run > count {
				start, count = runStart, run
			}
		} else {
			run = 0
		}
	}
	return start, count
}

// claimReusedMetadata atomically owns data plus the metadata interval. Even
// this candidate's own data reservations cannot overlap its metadata.
func (l *ReservationLedger) claimReusedMetadata(candidate CandidateIDV1, start, count, highWater uint64, data []allocatedPage, abandoned []ReservationExtentV1) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	// A failed physical tail write may be ahead of this transaction. Let the
	// existing tail reservation encode and discharge that abandoned coverage.
	// Check under the claim lock, not merely during speculative placement.
	for _, burned := range l.burnedTails {
		if burned.start >= highWater || burned.count > highWater-burned.start {
			return false
		}
	}
	r := l.candidates[candidate]
	if count == 0 || start < 2 || start+count < start || (r != nil && (r.state != CandidatePreVisible || r.tailReserved)) {
		return false
	}
	for _, allocation := range data {
		if allocation.id >= start && allocation.id-start < count || l.reservedByOtherLocked(candidate, allocation.id) {
			return false
		}
	}
	for id := start; id < start+count; id++ {
		if l.reservedLocked(id) {
			return false
		}
	}
	if r == nil {
		r = &reservation{state: CandidatePreVisible}
		l.candidates[candidate] = r
	}
	for _, allocation := range data {
		if _, exists := l.owners[allocation.id]; !exists {
			l.owners[allocation.id] = candidate
			r.ids = append(r.ids, allocation.id)
		}
	}
	r.tailReserved, r.reusedMetadata, r.tailStart, r.tailCount = true, true, start, count
	for _, extent := range abandoned {
		if extent.Kind == ReservationAbandonedAppend {
			r.abandonedCoverage = append(r.abandonedCoverage, reservationInterval{start: extent.StartPageID, count: uint64(extent.Count)})
		}
	}
	return true
}

func (t *FreelistTxn) tryReusedMetadata(candidate CandidateIDV1) (uint64, uint64, []ReservationExtentV1, bool) {
	var lower uint64
	for attempt := 0; attempt < metadataReuseChunkAttempts; attempt++ {
		chunk := findFreeGE(t.root, lower, 0, &t.stats.PageVisits)
		if chunk == nil {
			break
		}
		lower = chunk.chunkNo + 1
		start, run := t.reusableChunkRun(chunk)
		// Keep the chunk alive throughout sizing and final mutation.
		if run < 3 || chunk.freeCount() < 4 {
			continue
		}
		planned, err := t.cloneForAllocatorPrepare()
		if err != nil {
			return 0, 0, nil, false
		}
		// Count the exact additional dirty path without cloning it for sizing.
		// The selected chunk stays nonempty, so the eventual mutation neither
		// creates nor removes a node; already dirty nodes are counted once.
		statePages := countUnmaterializedStatePages(planned.root, 0)
		for n, depth := planned.root, 0; n != nil; depth++ {
			if n.pageID != 0 {
				statePages++
			}
			if depth == chunkTrieDepth {
				if n.chunk.pageID != 0 {
					statePages++
				}
				break
			}
			n = n.child[chunkNibble(chunk.chunkNo, depth)]
		}
		planned.markReplacedPath(chunk.chunkNo)
		extents, err := planned.reservationExtents()
		if err != nil {
			return 0, 0, nil, false
		}
		count := statePages + reservationPagesForEntries(uint64(len(extents))+1) + 1
		if count > run || count >= chunk.freeCount() {
			continue
		}
		planned.mutate(chunk.chunkNo, func(c *stateChunk) {
			for id := start; id < start+count; id++ {
				c.setFree(id&(freelistChunkSize-1), false)
			}
		})
		if !t.ledger.claimReusedMetadata(candidate, start, count, planned.highWater, planned.allocated, planned.abandonedAppends) {
			continue
		}
		*t = *planned
		return start, count, extents, true
	}
	return 0, 0, nil, false
}

func (t *FreelistTxn) allocateReusedRange(count int) ([]uint64, bool) {
	if count <= 0 || count > freelistChunkSize {
		return nil, false
	}
	var lower uint64
	for attempt := 0; attempt < metadataReuseChunkAttempts; attempt++ {
		chunk := findFreeGE(t.root, lower, 0, &t.stats.PageVisits)
		if chunk == nil {
			break
		}
		lower = chunk.chunkNo + 1
		start, run := t.reusableChunkRun(chunk)
		if uint64(count) > run {
			continue
		}
		ids := make([]uint64, count)
		t.mutate(chunk.chunkNo, func(c *stateChunk) {
			for i := range ids {
				ids[i] = start + uint64(i)
				c.setFree(ids[i]&(freelistChunkSize-1), false)
			}
		})
		for _, id := range ids {
			t.allocated = append(t.allocated, allocatedPage{id, ReservationReusedData})
		}
		t.stats.ReuseAllocations += uint64(count)
		return ids, true
	}
	return nil, false
}
