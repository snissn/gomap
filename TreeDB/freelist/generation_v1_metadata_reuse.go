package freelist

import "unsafe"

// detachMetadataSiblings is called immediately after successful metadata
// persistent mutation copied the selected path. Other dirty branches still
// need isolation. A private preparation already isolated all dirty aliases and
// bypasses this helper when materialization consumes its builder.
func detachMetadataSiblings(r stateRefV1, depth int, chunkNo uint64) stateRefV1 {
	return detachMetadataSiblingsWithStats(r, depth, chunkNo, nil)
}
func detachMetadataSiblingsWithStats(r stateRefV1, depth int, chunkNo uint64, stats *FreelistTxnStats) stateRefV1 {
	out, err := detachMetadataSiblingsOwnedV1(r, depth, chunkNo, stats, nil)
	if err != nil {
		panic(err)
	}
	return out
}
func metadataSiblingBirthPlanV1(r stateRefV1, chunkNo uint64) stateBirthPlanV1 {
	var plan stateBirthPlanV1
	if r.branch == nil {
		return plan
	}
	selected := chunkNibble(chunkNo, int(r.branch.depth))
	for i, child := range r.branch.child {
		if i == selected {
			plan.add(metadataSiblingBirthPlanV1(child, chunkNo))
		} else {
			plan.add(dirtyStateBirthPlanV1(child, 0))
		}
	}
	return plan
}
func detachMetadataSiblingsOwnedV1(r stateRefV1, _ int, chunkNo uint64, stats *FreelistTxnStats, creator *allocationCreditLeaseV1) (stateRefV1, error) {
	plan := metadataSiblingBirthPlanV1(r, chunkNo)
	operation, err := admitAllocationOperationV1(creator, plan.nodes*stateNodeCopyCapacityV1+plan.chunks*stateChunkCopyCapacityV1, plan.nodes+plan.chunks)
	if err != nil {
		return stateRefV1{}, err
	}
	defer operation.close()
	return detachMetadataSiblingsOperationV1(r, chunkNo, stats, &operation)
}
func detachMetadataSiblingsOperationV1(r stateRefV1, chunkNo uint64, stats *FreelistTxnStats, operation *allocationOperationV1) (stateRefV1, error) {
	owned := retainStateNodeV1(r)
	if stats != nil {
		stats.StateIsolationVisits++
	}
	if r.branch == nil {
		return owned, nil
	}
	selected := chunkNibble(chunkNo, int(r.branch.depth))
	for i, child := range r.branch.child {
		var next stateRefV1
		var err error
		if i == selected {
			next, err = detachMetadataSiblingsOperationV1(child, chunkNo, stats, operation)
		} else {
			next, err = detachStateOwnedOperationV1(child, 0, stats, operation)
		}
		if err != nil {
			releaseStateNodeV1(owned)
			return stateRefV1{}, err
		}
		replaceStateNodeV1(&r.branch.child[i], next)
	}
	return owned, nil
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
	t.ledger.candidates.Range(func(_ CandidateIDV1, candidate *reservation) bool {
		if candidate.tailReserved {
			clearReservedChunkRange(&free, base, candidate.tailStart, candidate.tailCount)
		}
		return true
	})
	var run, runStart uint64
	for offset := uint64(0); offset < freelistChunkSize; offset++ {
		if free[offset/64]&(uint64(1)<<(offset%64)) == 0 {
			run = 0
			continue
		}
		id := base | offset
		_, owned := t.ledger.owners.Get(id)
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
func (l *ReservationLedger) claimReusedMetadata(candidate CandidateIDV1, start, count, highWater uint64, data []allocatedPage, abandoned []ReservationExtentV1, creators ...*allocationCreditLeaseV1) bool {
	var creator *allocationCreditLeaseV1
	if len(creators) != 0 {
		creator = creators[0]
	}
	operation, ok, _ := l.claimReusedMetadataAdmittedV1(candidate, start, count, highWater, data, abandoned, creator, allocationOperationV1{})
	operation.close()
	return ok
}

// The combined reserve precedes the first ownership edit and includes the
// caller's tree and output births. A successful claim lends its unused receipt.
func (l *ReservationLedger) claimReusedMetadataAdmittedV1(candidate CandidateIDV1, start, count, highWater uint64, data []allocatedPage, abandoned []ReservationExtentV1, creator *allocationCreditLeaseV1, extra allocationOperationV1) (allocationOperationV1, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// A failed physical tail write may be ahead of this transaction. Let the
	// existing tail reservation encode and discharge that abandoned coverage.
	// Check under the claim lock, not merely during speculative placement.
	for _, burned := range l.burnedTails {
		if burned.start >= highWater || burned.count > highWater-burned.start {
			return allocationOperationV1{}, false, nil
		}
	}
	r := l.candidates.Value(candidate)
	if count == 0 || start < 2 || start+count < start || (r != nil && (r.state != CandidatePreVisible || r.tailReserved)) {
		return allocationOperationV1{}, false, nil
	}
	for _, allocation := range data {
		if allocation.id >= start && allocation.id-start < count || l.reservedByOtherLocked(candidate, allocation.id) {
			return allocationOperationV1{}, false, nil
		}
	}
	for id := start; id < start+count; id++ {
		if l.reservedLocked(id) {
			return allocationOperationV1{}, false, nil
		}
	}
	newOwners := 0
	for _, allocation := range data {
		if _, exists := l.owners.Get(allocation.id); !exists {
			newOwners++
		}
	}
	oldIDs, oldCoverage := 0, 0
	if r != nil {
		oldIDs, oldCoverage = len(r.ids), len(r.abandonedCoverage)
	}
	coverage := 0
	for _, extent := range abandoned {
		if extent.Kind == ReservationAbandonedAppend {
			coverage++
		}
	}
	if oldIDs > int(^uint(0)>>1)-newOwners || oldCoverage > int(^uint(0)>>1)-coverage {
		return allocationOperationV1{}, false, ErrNoAllocatablePage
	}
	plan, err := l.admitReservationV1(candidate, r, oldIDs+newOwners, newOwners, oldCoverage+coverage, creator, extra)
	if err != nil {
		return allocationOperationV1{}, false, err
	}
	accepted := false
	defer func() {
		if !accepted {
			plan.operation.close()
		}
	}()
	r, err = plan.prepare(r)
	if err != nil {
		return allocationOperationV1{}, false, err
	}
	if plan.newReservation {
		if err = l.candidates.putAdmittedV1(candidate, r, creator, &plan.operation); err != nil {
			return allocationOperationV1{}, false, err
		}
	}
	for _, allocation := range data {
		if _, exists := l.owners.Get(allocation.id); !exists {
			if err = l.owners.putAdmittedV1(allocation.id, candidate, creator, &plan.operation); err != nil {
				return allocationOperationV1{}, false, err
			}
			r.ids = append(r.ids, allocation.id)
		}
	}

	r.tailReserved, r.reusedMetadata, r.tailStart, r.tailCount = true, true, start, count
	for _, extent := range abandoned {
		if extent.Kind == ReservationAbandonedAppend {
			r.abandonedCoverage = append(r.abandonedCoverage, reservationInterval{start: extent.StartPageID, count: uint64(extent.Count)})
		}
	}
	accepted = true
	return plan.operation, true, nil
}

func (t *FreelistTxn) tryReusedMetadata(candidate CandidateIDV1) (uint64, uint64, []ReservationExtentV1, bool) {
	search := t.metadataChunkSearch()
	success := false
	var baseExtents []ReservationExtentV1
	baseReady := false
	defer func() { search.finish(t, success) }()
	for chunk := search.next(t); chunk != nil; chunk = search.next(t) {
		start, run := t.reusableChunkRun(chunk)
		// Keep the chunk alive throughout sizing and final mutation.
		if run < 3 || chunk.freeCount() < 4 {
			continue
		}
		// Count the exact additional dirty path without cloning it for sizing.
		// The selected chunk stays nonempty, so the eventual mutation neither
		// creates nor removes a node; already dirty nodes are counted once.
		statePages := countUnmaterializedStatePages(t.root, 0)
		for r := t.root; !r.zero() && r.containsChunk(chunk.chunkNo); {
			if r.pageID() != 0 {
				statePages++
			}
			if r.chunk != nil {
				break
			}
			r = r.branch.child[chunkNibble(chunk.chunkNo, int(r.branch.depth))]
		}
		// Even one reservation page plus the header cannot fit. Reject before
		// allocating a private plan or constructing its reservation extents.
		if minimum := statePages + 2; minimum > run || minimum >= chunk.freeCount() {
			continue
		}
		// Build the normalized base once. Each attempt overlays at most one
		// fixed path, without cloning the radix set or rescanning data IDs.
		if !baseReady {
			var err error
			baseExtents, err = t.reservationExtents()
			if err != nil {
				t.allocationErr = err
				return 0, 0, nil, false
			}
			baseReady = true
		}
		var overlay [chunkTrieDepth + 2]ReservationExtentV1
		overlayCount := 0
		add := func(id uint64) {
			if id == 0 {
				return
			}
			if _, exists := t.replacedMetadata.Get(id); exists {
				return
			}
			for i := 0; i < overlayCount; i++ {
				if overlay[i].StartPageID == id {
					return
				}
			}
			overlay[overlayCount] = ReservationExtentV1{StartPageID: id, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: t.base.commitSeq}
			overlayCount++
		}
		for r := t.root; !r.zero() && r.containsChunk(chunk.chunkNo); {
			add(r.pageID())
			if r.chunk != nil {
				break
			}
			r = r.branch.child[chunkNibble(chunk.chunkNo, int(r.branch.depth))]
		}
		for i := 1; i < overlayCount; i++ {
			for j := i; j > 0 && overlay[j].StartPageID < overlay[j-1].StartPageID; j-- {
				overlay[j], overlay[j-1] = overlay[j-1], overlay[j]
			}
		}
		extentCount, err := mergeReservationPlanV1(baseExtents, overlay[:overlayCount], nil)
		if err != nil {
			return 0, 0, nil, false
		}
		count := statePages + reservationPagesForEntries(uint64(extentCount)+1) + 1
		if count > run || count >= chunk.freeCount() {
			continue
		}
		mutation := t.planMutationV1(chunk.chunkNo, 0)
		if mutation.err != nil {
			t.allocationErr = mutation.err
			return 0, 0, nil, false
		}
		outputBytes := allocationClassV1(uint64(extentCount)*uint64(unsafe.Sizeof(ReservationExtentV1{})), false)
		extra := allocationOperationV1{bytes: cowSaturatingAddV1(mutation.bytes, outputBytes), refs: mutation.refs}
		operation, claimed, err := t.ledger.claimReusedMetadataAdmittedV1(candidate, start, count, t.highWater, t.allocated, t.abandonedAppends, t.buildCreator, extra)
		if err != nil {
			t.allocationErr = err
			return 0, 0, nil, false
		}
		if !claimed {
			continue
		}
		// No facet callback follows the claim: all births consume this receipt.
		if err = operation.take(outputBytes, 0); err != nil {
			operation.close()
			t.allocationErr = err
			return 0, 0, nil, false
		}
		extents := make([]ReservationExtentV1, extentCount)
		if _, err = mergeReservationPlanV1(baseExtents, overlay[:overlayCount], extents); err != nil {
			operation.close()
			t.allocationErr = err
			return 0, 0, nil, false
		}
		err = t.applyMutationV1(chunk.chunkNo, 1, func(c *stateChunk) {
			for id := start; id < start+count; id++ {
				c.setFree(id&(freelistChunkSize-1), false)
			}
		}, mutation, &operation)
		operation.close()
		if err != nil {
			t.allocationErr = err
			return 0, 0, nil, false
		}

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
		mutation := t.planMutationV1(chunk.chunkNo, count)
		if mutation.err != nil {
			t.allocationErr = mutation.err
			return nil, false
		}
		outputBytes := allocationClassV1(uint64(count)*8, false)
		operation, err := admitAllocationOperationV1(t.buildCreator, cowSaturatingAddV1(mutation.bytes, outputBytes), mutation.refs)
		if err != nil {
			t.allocationErr = err
			return nil, false
		}
		if err = operation.take(outputBytes, 0); err != nil {
			operation.close()
			t.allocationErr = err
			return nil, false
		}
		ids := make([]uint64, count)
		for i := range ids {
			ids[i] = start + uint64(i)
		}
		err = t.applyMutationV1(chunk.chunkNo, uint64(count), func(c *stateChunk) {
			for _, id := range ids {
				c.setFree(id&(freelistChunkSize-1), false)
			}
		}, mutation, &operation)
		operation.close()
		if err != nil {
			t.allocationErr = err
			return nil, false
		}
		for _, id := range ids {
			t.allocated = append(t.allocated, allocatedPage{id, ReservationReusedData})
		}
		t.stats.ReuseAllocations += uint64(count)
		success = true
		return ids, true
	}
	return nil, false
}

// mergeReservationPlanV1 visits two sorted, owned value runs. A nil output is
// allocation-free sizing; an exact output receives the same normalized values.
func mergeReservationPlanV1(base, overlay, out []ReservationExtentV1) (int, error) {
	i, j, count := 0, 0, 0
	var last ReservationExtentV1
	flush := func() {
		if last.Count != 0 {
			if out != nil {
				out[count] = last
			}
			count++
		}
	}
	for i < len(base) || j < len(overlay) {
		var next ReservationExtentV1
		if j == len(overlay) || i < len(base) && base[i].StartPageID < overlay[j].StartPageID {
			next = base[i]
			i++
		} else {
			next = overlay[j]
			j++
		}
		if next.StartPageID < 2 || next.Count == 0 || next.StartPageID+uint64(next.Count) < next.StartPageID {
			return 0, ErrGenerationFormat
		}
		if last.Count != 0 {
			end := last.StartPageID + uint64(last.Count)
			if end > next.StartPageID {
				return 0, ErrGenerationFormat
			}
			if end == next.StartPageID && last.Kind == next.Kind && last.LastReachableCommitSeq == next.LastReachableCommitSeq && uint64(last.Count)+uint64(next.Count) <= uint64(^uint32(0)) {
				last.Count += next.Count
				continue
			}
			flush()
		}
		last = next
	}
	flush()
	return count, nil
}
