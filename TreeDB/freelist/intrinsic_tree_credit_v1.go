package freelist

import "sync/atomic"

// Each allocation retains one creator reference. Intrusive references count
// actual owning root/tree edges; stack borrows never become lifetime authority.
func retainStateNodeV1(n *stateNode) *stateNode {
	if n != nil && atomic.LoadUint64(&n.ownedRefs) != 0 {
		atomic.AddUint64(&n.ownedRefs, 1)
	}
	return n
}
func retainStateChunkV1(c *stateChunk) *stateChunk {
	if c != nil && atomic.LoadUint64(&c.ownedRefs) != 0 {
		atomic.AddUint64(&c.ownedRefs, 1)
	}
	return c
}
func replaceStateNodeV1(slot **stateNode, owned *stateNode) {
	old := *slot
	*slot = owned
	releaseStateNodeV1(old)
}
func replaceStateChunkV1(slot **stateChunk, owned *stateChunk) {
	old := *slot
	*slot = owned
	releaseStateChunkV1(old)
}
func releaseStateNodeV1(n *stateNode) {
	if n == nil || atomic.LoadUint64(&n.ownedRefs) == 0 {
		return
	}
	if atomic.AddUint64(&n.ownedRefs, ^uint64(0)) != 0 {
		return
	}
	children, chunk, creator := n.child, n.chunk, n.creator
	n.child = [16]*stateNode{}
	n.chunk, n.creator = nil, nil
	n.pageID, n.freeCount, n.retiredCount, n.minRetiredSeq, n.minChunk, n.maxChunk, n.checksum = 0, 0, 0, 0, 0, 0, 0
	for _, child := range children {
		releaseStateNodeV1(child)
	}
	releaseStateChunkV1(chunk)
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
	c.pageID, c.checksum, c.chunkNo = 0, 0, 0
	c.free = [4]uint64{}
	c.retired = [freelistChunkSize]uint64{}
	creator.release()
}
func cloneStateNodeOwnedV1(n *stateNode, preserveIdentity bool, creator *allocationCreditLeaseV1, operations ...*allocationOperationV1) (*stateNode, error) {
	if err := reserveBirthV1(creator, stateNodeCopyCapacityV1, 1, operations...); err != nil {
		return nil, err
	}
	out := &stateNode{ownedRefs: 1, creator: creator}
	if n != nil {
		out.pageID, out.checksum = n.pageID, n.checksum
		out.freeCount, out.retiredCount, out.minRetiredSeq, out.minChunk, out.maxChunk = n.freeCount, n.retiredCount, n.minRetiredSeq, n.minChunk, n.maxChunk
		for i, child := range n.child {
			out.child[i] = retainStateNodeV1(child)
		}
		out.chunk = retainStateChunkV1(n.chunk)
	}
	if !preserveIdentity {
		out.pageID, out.checksum = 0, 0
	}
	return out, nil
}
func cloneStateChunkOwnedV1(c *stateChunk, preserveIdentity bool, creator *allocationCreditLeaseV1, operations ...*allocationOperationV1) (*stateChunk, error) {
	if err := reserveBirthV1(creator, stateChunkCopyCapacityV1, 1, operations...); err != nil {
		return nil, err
	}
	out := &stateChunk{ownedRefs: 1, creator: creator}
	if c != nil {
		out.pageID, out.checksum, out.chunkNo, out.free, out.retired = c.pageID, c.checksum, c.chunkNo, c.free, c.retired
	}
	if !preserveIdentity {
		out.pageID, out.checksum = 0, 0
	}
	return out, nil
}
func stateTreeFiniteV1(n *stateNode) bool {
	if n == nil {
		return false
	}
	if n.creator != nil || n.chunk != nil && n.chunk.creator != nil {
		return true
	}
	for _, child := range n.child {
		if stateTreeFiniteV1(child) {
			return true
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
	g.root, g.creator = nil, nil
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
	return c != nil && (c.creator != nil || c.allocationCredit != nil || c.generation.hasFiniteBackingV1())
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
	t.root, t.base, t.creator, t.buildCreator = nil, nil, nil, nil
	t.allocationCredit = nil
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
