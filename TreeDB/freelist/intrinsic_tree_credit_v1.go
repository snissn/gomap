package freelist

import "sync/atomic"

// Value refs are the sole owning tree edges; borrowed refs do not retain.
func retainStateNodeV1(r stateRefV1) stateRefV1 {
	if r.branch != nil {
		if atomic.LoadUint64(&r.branch.ownedRefs) != 0 {
			atomic.AddUint64(&r.branch.ownedRefs, 1)
		}
	}
	if r.chunk != nil {
		retainStateChunkV1(r.chunk)
	}
	return r
}
func retainStateChunkV1(c *stateChunk) *stateChunk {
	if c != nil && atomic.LoadUint64(&c.ownedRefs) != 0 {
		atomic.AddUint64(&c.ownedRefs, 1)
	}
	return c
}
func replaceStateNodeV1(slot *stateRefV1, owned stateRefV1) {
	old := *slot
	*slot = owned
	releaseStateNodeV1(old)
}
func replaceStateChunkV1(slot **stateChunk, owned *stateChunk) {
	old := *slot
	*slot = owned
	releaseStateChunkV1(old)
}
func releaseStateNodeV1(r stateRefV1) {
	if r.chunk != nil {
		releaseStateChunkV1(r.chunk)
	}
	n := r.branch
	if n == nil || atomic.LoadUint64(&n.ownedRefs) == 0 {
		return
	}
	if atomic.AddUint64(&n.ownedRefs, ^uint64(0)) != 0 {
		return
	}
	children, creator := n.child, n.creator
	n.child = [16]stateRefV1{}
	n.creator = nil
	n.pageID, n.checksum, n.depth, n.prefix, n.freePages, n.retiredPages, n.minSeq, n.lowChunk, n.highChunk = 0, 0, 0, 0, 0, 0, 0, 0, 0
	for _, child := range children {
		releaseStateNodeV1(child)
	}
	creator.release()
}
func releaseStateChunkV1(c *stateChunk) {
	if c == nil || atomic.LoadUint64(&c.ownedRefs) == 0 {
		return
	}
	if atomic.AddUint64(&c.ownedRefs, ^uint64(0)) != 0 {
		return
	}
	creator := c.creator
	c.creator = nil
	c.pageID, c.checksum, c.chunkNo, c.retiredPages, c.minSeq = 0, 0, 0, 0, 0
	c.free = [4]uint64{}
	c.retired = [freelistChunkSize]uint64{}
	creator.release()
}
func cloneStateRefOwnedV1(request AllocationRequestCreditV1, r stateRefV1, preserve bool, creator *allocationCreditLeaseV1, ops ...*allocationOperationV1) (stateRefV1, error) {
	if r.chunk != nil {
		c, err := cloneStateChunkOwnedV1(request, r.chunk, preserve, creator, ops...)
		return stateRefV1{chunk: c}, err
	}
	n, err := cloneStateBranchOwnedV1(request, r.branch, preserve, creator, ops...)
	return stateRefV1{branch: n}, err
}
func cloneStateBranchOwnedV1(request AllocationRequestCreditV1, n *stateNode, preserve bool, creator *allocationCreditLeaseV1, ops ...*allocationOperationV1) (*stateNode, error) {
	if err := reserveBirthV1(request, creator, stateNodeCopyCapacityV1, 1, ops...); err != nil {
		return nil, err
	}
	out := &stateNode{ownedRefs: 1, creator: creator}
	if n != nil {
		out.pageID, out.checksum, out.depth, out.prefix = n.pageID, n.checksum, n.depth, n.prefix
		out.freePages, out.retiredPages, out.minSeq, out.lowChunk, out.highChunk = n.freePages, n.retiredPages, n.minSeq, n.lowChunk, n.highChunk
		for i, child := range n.child {
			out.child[i] = retainStateNodeV1(child)
		}
	}
	if !preserve {
		out.pageID, out.checksum = 0, 0
	}
	return out, nil
}
func cloneStateChunkOwnedV1(request AllocationRequestCreditV1, c *stateChunk, preserve bool, creator *allocationCreditLeaseV1, ops ...*allocationOperationV1) (*stateChunk, error) {
	if err := reserveBirthV1(request, creator, stateChunkCopyCapacityV1, 1, ops...); err != nil {
		return nil, err
	}
	out := &stateChunk{ownedRefs: 1, creator: creator}
	if c != nil {
		out.pageID, out.checksum, out.chunkNo, out.free, out.retired, out.retiredPages, out.minSeq = c.pageID, c.checksum, c.chunkNo, c.free, c.retired, c.retiredPages, c.minSeq
	}
	if !preserve {
		out.pageID, out.checksum = 0, 0
	}
	return out, nil
}
func stateTreeFiniteV1(r stateRefV1) bool {
	if r.creator() != nil {
		return true
	}
	if r.branch != nil {
		for _, child := range r.branch.child {
			if stateTreeFiniteV1(child) {
				return true
			}
		}
	}
	return false
}
func retainGenerationV1(g *FreelistGenerationV1) *FreelistGenerationV1 {
	if g != nil && atomic.LoadUint64(&g.ownedRefs) != 0 {
		atomic.AddUint64(&g.ownedRefs, 1)
	}
	return g
}
func releaseGenerationV1(g *FreelistGenerationV1) {
	if g == nil || atomic.LoadUint64(&g.ownedRefs) == 0 {
		return
	}
	if atomic.AddUint64(&g.ownedRefs, ^uint64(0)) != 0 {
		return
	}
	root, creator := g.root, g.creator
	g.root, g.creator = stateRefV1{}, nil
	clear(g.record.Extents[:cap(g.record.Extents)])
	clear(g.record.pageIDs[:cap(g.record.pageIDs)])
	clear(g.metadataPages[:cap(g.metadataPages)])
	g.record = ReservationRecordV1{}
	g.metadataPages = nil
	releaseStateNodeV1(root)
	creator.release()
}
func (g *FreelistGenerationV1) hasFiniteBackingV1() bool {
	return g != nil && (g.creator != nil || stateTreeFiniteV1(g.root))
}
func (g *FreelistGenerationV1) markOrdinaryEscapeV1() {
	if g != nil && !g.hasFiniteBackingV1() && atomic.CompareAndSwapUint32(&g.escaped, 0, 1) {
		retainGenerationV1(g)
	}
}
func (c *FreelistCandidateV1) hasFiniteBackingV1() bool {
	return c != nil && (c.creator != nil || c.generation.hasFiniteBackingV1())
}
func (c *FreelistCandidateV1) markOrdinaryEscapeV1() {
	if c != nil {
		c.generation.markOrdinaryEscapeV1()
	}
}
func releaseTxnV1(t *FreelistTxn) {
	if t == nil {
		return
	}
	root, base, creator, buildCreator := t.root, t.base, t.creator, t.buildCreator
	ledger, ledgerOwned := t.ledger, t.ledgerOwned
	t.ledger, t.ledgerOwned = nil, false
	t.root, t.base, t.creator, t.buildCreator = stateRefV1{}, nil, nil, nil
	t.privatePreparation = false
	allocatedCreator, abandonedCreator := t.allocatedCreator, t.abandonedCreator
	clear(t.allocated[:cap(t.allocated)])
	clear(t.abandonedAppends[:cap(t.abandonedAppends)])
	t.allocated, t.abandonedAppends = nil, nil
	t.allocatedCreator, t.abandonedCreator = nil, nil
	t.changedChunks.Clear()
	t.replacedMetadata.Clear()
	t.changedChunks, t.replacedMetadata = nil, nil
	releaseStateNodeV1(root)
	releaseGenerationV1(base)
	creator.release()
	buildCreator.release()
	allocatedCreator.release()
	abandonedCreator.release()
	if ledgerOwned {
		releaseReservationLedgerV1(ledger)
	}
}

// A not-yet-exposed owned materialization has no callback or external view edge.
// Failure scrubs its full vectors before dropping its exact generation creator.
func releaseCandidateOwnedBackingV1(candidate *FreelistCandidateV1) {
	if candidate == nil {
		return
	}
	generation, creator := candidate.generation, candidate.creator
	clear(candidate.pages[:cap(candidate.pages)])
	clear(candidate.dirtyIDs[:cap(candidate.dirtyIDs)])
	candidate.pages, candidate.dirtyIDs, candidate.generation, candidate.creator = nil, nil, nil, nil
	releaseGenerationV1(generation)
	creator.release()
}
