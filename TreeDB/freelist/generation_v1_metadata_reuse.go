package freelist

import "maps"

// detachMetadataSiblings is called immediately after successful metadata
// mutation copied the selected path. Other dirty branches still need isolation.
func detachMetadataSiblings(n *stateNode, _ int, chunkNo uint64) *stateNode {
	if n == nil || n.chunk != nil {
		return n
	}
	selected := chunkNibble(chunkNo, int(n.depth))
	for i, child := range n.child {
		if i == selected {
			n.child[i] = detachMetadataSiblings(child, 0, chunkNo)
		} else {
			n.child[i] = detachUnmaterialized(child, 0)
		}
	}
	return n
}

// A bounded placement attempt, not a full free-set search. Each visited chunk
// has 256 bits; larger/fragmented requests retain the existing tail fallback.
const metadataReuseChunkAttempts = 4

type metadataChunkSearch struct {
	initial, lower, last uint64
	wrapped              bool
	attempts             int
}

func (t *FreelistTxn) metadataChunkSearch() metadataChunkSearch {
	t.ledger.mu.Lock()
	hint := t.ledger.nextReuseChunk
	t.ledger.mu.Unlock()
	return metadataChunkSearch{initial: hint, lower: hint}
}

func (s *metadataChunkSearch) next(t *FreelistTxn) *stateChunk {
	if s.attempts >= metadataReuseChunkAttempts {
		return nil
	}
	chunk := findFreeGE(t.root, s.lower, 0, &t.stats.PageVisits)
	if chunk == nil && !s.wrapped {
		s.wrapped, s.lower = true, 0
		chunk = findFreeGE(t.root, 0, 0, &t.stats.PageVisits)
	}
	if chunk == nil || s.wrapped && chunk.chunkNo >= s.initial {
		return nil
	}
	// Chunk IDs use at most 56 bits, so the successor sentinel fits uint64.
	s.lower = chunk.chunkNo + 1
	s.last = chunk.chunkNo
	s.attempts++
	return chunk
}

func (s *metadataChunkSearch) finish(t *FreelistTxn, success bool) {
	if s.attempts == 0 {
		return
	}
	hint := s.last
	if !success {
		hint++
	}
	t.ledger.mu.Lock()
	defer t.ledger.mu.Unlock()
	// Advisory compare/update: preserve another search's changed hint.
	// Numeric ABA only affects placement, never reservation authority.
	if t.ledger.nextReuseChunk == s.initial {
		t.ledger.nextReuseChunk = hint
	}
}

func (t *FreelistTxn) reusableChunkRun(chunk *stateChunk) (start, count uint64) {
	t.ledger.mu.Lock()
	defer t.ledger.mu.Unlock()
	free := chunk.free
	base := chunk.chunkNo << freelistChunkShift
	for _, burned := range t.ledger.burnedTails {
		clearReservedChunkRange(&free, base, burned.start, burned.count)
	}
	for _, candidate := range t.ledger.candidates {
		if candidate.tailReserved {
			clearReservedChunkRange(&free, base, candidate.tailStart, candidate.tailCount)
		}
	}
	var run, runStart uint64
	for offset := uint64(0); offset < freelistChunkSize; offset++ {
		if free[offset/64]&(uint64(1)<<(offset%64)) == 0 {
			run = 0
			continue
		}
		id := base | offset
		_, owned := t.ledger.owners[id]
		if !owned {
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

// Clip by subtraction, preserving reservedLocked's semantics even when an
// interval's absolute end would overflow. Only this chunk's four words change.
func clearReservedChunkRange(free *[4]uint64, base, start, count uint64) {
	var offset uint64
	if start < base {
		skipped := base - start
		if skipped >= count {
			return
		}
		count -= skipped
	} else {
		offset = start - base
		if offset >= freelistChunkSize {
			return
		}
	}
	end := offset + min(count, freelistChunkSize-offset)
	for offset < end {
		next := min(end, (offset/64+1)*64)
		mask := (^uint64(0) >> (64 - (next - offset))) << (offset % 64)
		free[offset/64] &^= mask
		offset = next
	}
}

// claimReusedMetadata atomically owns data plus the metadata interval. Even
// this candidate's own data reservations cannot overlap its metadata.
func (l *ReservationLedger) claimReusedMetadata(candidate CandidateIDV1, start, count, highWater uint64, data []allocatedPage, abandoned []ReservationExtentV1) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.claimReusedMetadataLocked(candidate, start, count, highWater, data, abandoned)
}

func (l *ReservationLedger) claimReusedMetadataLocked(candidate CandidateIDV1, start, count, highWater uint64, data []allocatedPage, abandoned []ReservationExtentV1) bool {
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
	// Keep the original synchronous loop's borrowed stack controls. The finite
	// controller uses the same search, sizing, inventory and original claim,
	// without letting persistent operand bindings escape this ordinary driver.
	search := t.metadataChunkSearch()
	success := false
	defer func() { search.finish(t, success) }()
	for chunk := search.next(t); chunk != nil; chunk = search.next(t) {
		start, run := t.reusableChunkRun(chunk)
		if run < 3 || chunk.freeCount() < 4 {
			continue
		}
		statePages := countUnmaterializedStatePages(t.root, 0)
		for n := t.root; n != nil; {
			if n.pageID != 0 {
				statePages++
			}
			if n.chunk != nil {
				break
			}
			n = n.child[chunkNibble(chunk.chunkNo, int(n.depth))]
		}
		if minimum := statePages + 2; minimum > run || minimum >= chunk.freeCount() {
			continue
		}
		planned := *t
		planned.replacedMetadata = maps.Clone(t.replacedMetadata)
		planned.markReplacedPath(chunk.chunkNo)
		extents, err := planned.reservationExtents()
		if err != nil {
			return 0, 0, nil, false
		}
		count := statePages + reservationPagesForEntries(uint64(len(extents))+1) + 1
		if count > run || count >= chunk.freeCount() {
			continue
		}
		if !t.ledger.claimReusedMetadata(candidate, start, count, t.highWater, t.allocated, t.abandonedAppends) {
			continue
		}
		t.mutate(chunk.chunkNo, func(c *stateChunk) {
			for id := start; id < start+count; id++ {
				c.setFree(id&(freelistChunkSize-1), false)
			}
		})
		success = true
		return start, count, extents, true
	}
	return 0, 0, nil, false
}

func (t *FreelistTxn) allocateReusedRange(count int) ([]uint64, bool) {
	if count <= 0 || count > freelistChunkSize {
		return nil, false
	}
	search := t.metadataChunkSearch()
	success := false
	defer func() { search.finish(t, success) }()
	for chunk := search.next(t); chunk != nil; chunk = search.next(t) {
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
		success = true
		return ids, true
	}
	return nil, false
}
