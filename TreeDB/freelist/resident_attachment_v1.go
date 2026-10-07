package freelist

import (
	"sync/atomic"
	"unsafe"
)

// residentAttachmentV1 contains only scalar accounting and a nonowning borrow
// of the existing facet wrapper. Census charges actual allocation classes and
// full capacities; shared representations may be conservatively counted twice.
type residentAttachmentV1 struct {
	creator     *allocationCreditLeaseV1
	bytes, refs uint64
	attach      bool
}

func (v *residentAttachmentV1) claim(slot **allocationCreditLeaseV1, bytes uint64) {
	if bytes == 0 {
		return
	}
	if !v.attach {
		v.bytes = cowSaturatingAddV1(v.bytes, bytes)
		// A historical creating wrapper is actual retained backing. Charging it
		// per intrinsic edge conservatively overcounts sharing without a registry.
		if *slot != nil {
			v.bytes = cowSaturatingAddV1(v.bytes, allocationClassV1(uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), true))
		}
		if *slot == nil {
			v.refs = cowSaturatingAddV1(v.refs, 1)
		}
		return
	}
	if *slot == nil {
		*slot = v.creator
		v.refs--
	}
}
func (v *residentAttachmentV1) tree(n *stateNode) {
	if n == nil {
		return
	}
	v.claim(&n.creator, allocationClassV1(uint64(unsafe.Sizeof(*n)), true))
	if n.chunk != nil {
		v.claim(&n.chunk.creator, allocationClassV1(uint64(unsafe.Sizeof(*n.chunk)), true))
	}
	for _, child := range n.child {
		v.tree(child)
	}
}
func (v *residentAttachmentV1) generation(g *FreelistGenerationV1) {
	if g == nil {
		return
	}
	bytes := allocationClassV1(uint64(unsafe.Sizeof(*g)), true)
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(g.record.Extents)), uint64(unsafe.Sizeof(ReservationExtentV1{}))), false))
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(g.record.pageIDs)), 8), false))
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(g.metadataPages)), 8), false))
	v.claim(&g.creator, bytes)
	v.tree(g.root)
}
func (v *residentAttachmentV1) transaction(t *FreelistTxn) {
	if t == nil {
		return
	}
	v.claim(&t.creator, allocationClassV1(uint64(unsafe.Sizeof(*t)), true))
	v.claim(&t.allocatedCreator, allocationClassV1(cowSaturatingMulV1(uint64(cap(t.allocated)), uint64(unsafe.Sizeof(allocatedPage{}))), false))
	v.claim(&t.abandonedCreator, allocationClassV1(cowSaturatingMulV1(uint64(cap(t.abandonedAppends)), uint64(unsafe.Sizeof(ReservationExtentV1{}))), false))
	residentRadixAttachmentV1(t.changedChunks, v)
	residentRadixAttachmentV1(t.replacedMetadata, v)
	v.generation(t.base)
	v.tree(t.root)
}
func residentRadixAttachmentV1[K comparable, V comparable](m *numericRadixV1[K, V], v *residentAttachmentV1) {
	if m == nil {
		return
	}
	v.claim(&m.credit, allocationClassV1(uint64(unsafe.Sizeof(*m)), true))
	for c := m.head; c != nil; c = c.next {
		v.claim(&c.credit, numericRadixChunkClassV1[K, V]())
	}
}

func (v *residentAttachmentV1) prepared(p *PreparedCOWCandidateV1) {
	if p == nil {
		return
	}
	bytes := allocationClassV1(uint64(unsafe.Sizeof(*p)), true)
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(p.auxiliary)), 8), false))
	v.claim(&p.creator, bytes)
	v.transaction(p.rollbackTxn)
	c := p.candidate
	if c == nil {
		return
	}
	bytes = allocationClassV1(uint64(unsafe.Sizeof(*c)), true)
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(c.pages)), uint64(unsafe.Sizeof(candidatePageV1{}))), true))
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(c.dirtyIDs)), 8), false))
	for _, page := range c.pages[:cap(c.pages)] {
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(cap(page.view.data)), false))
		if page.view.index != nil {
			bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*page.view.index)), false))
		}
	}
	v.claim(&c.creator, bytes)
	v.generation(c.generation)
}
func (v *residentAttachmentV1) ledger(l *ReservationLedger) {
	v.claim(&l.creator, allocationClassV1(uint64(unsafe.Sizeof(*l)), true))
	v.claim(&l.burnedCreator, allocationClassV1(cowSaturatingMulV1(uint64(cap(l.burnedTails)), uint64(unsafe.Sizeof(reservationInterval{}))), false))
	residentRadixAttachmentV1(l.owners, v)
	residentRadixAttachmentV1(l.candidates, v)
	l.candidates.Range(func(_ CandidateIDV1, r *reservation) bool {
		if r == nil {
			return true
		}
		v.claim(&r.creator, allocationClassV1(uint64(unsafe.Sizeof(*r)), true))
		v.claim(&r.idsCreator, allocationClassV1(cowSaturatingMulV1(uint64(cap(r.ids)), 8), false))
		v.claim(&r.coverageCreator, allocationClassV1(cowSaturatingMulV1(uint64(cap(r.abandonedCoverage)), uint64(unsafe.Sizeof(reservationInterval{}))), false))
		return true
	})
}
func (v *residentAttachmentV1) allocator(a *Allocator) {
	state := a.cow
	bytes := allocationClassV1(uint64(unsafe.Sizeof(*a)), true)
	bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*state)), true))
	if state.ready != nil {
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*state.ready)), true))
	}
	if a.writerAuthority != nil {
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*a.writerAuthority)), false))
	}
	v.claim(&state.creator, bytes)
	v.claim(&state.activatedCreator, allocationClassV1(cowSaturatingMulV1(uint64(cap(state.activated)), 8), true))
	v.generation(state.generation)
	v.transaction(state.txn)
	for p := state.ownedCandidates; p != nil; p = p.ownedNext {
		v.prepared(p)
	}
	for lease := state.generationLeases; lease != nil; lease = lease.next {
		v.claim(&lease.creator, allocationClassV1(uint64(unsafe.Sizeof(*lease)), true))
		v.generation(lease.generation)
	}
	v.ledger(state.ledger)
}

// managedGenerationEdgesLockedV1 counts actual direct generation owners. It
// deliberately uses pointer identity, never a reused page ID or logical ref.
func (a *Allocator) managedGenerationEdgesLockedV1(g *FreelistGenerationV1) uint64 {
	var edges uint64
	if a.cow.generation == g {
		edges++
	}
	if a.cow.txn != nil && a.cow.txn.base == g {
		edges++
	}
	for p := a.cow.ownedCandidates; p != nil; p = p.ownedNext {
		if p.rollbackTxn != nil && p.rollbackTxn.base == g {
			edges++
		}
		if p.candidate != nil && p.candidate.generation == g {
			edges++
		}
	}
	for lease := a.cow.generationLeases; lease != nil; lease = lease.next {
		if lease.generation == g {
			edges++
		}
	}
	return edges
}
func (a *Allocator) residentProvenanceLockedV1() error {
	state := a.cow
	if a.writerAuthority == nil || a.writerDetached || a.rawWriterEscaped || state.rawGenerationEscaped || state.ledger.rawEscaped {
		return ErrFiniteAllocationExportV1
	}
	check := func(g *FreelistGenerationV1) bool {
		return g != nil && atomic.LoadUint32(&g.escaped) == 0 && atomic.LoadUint64(&g.ownedRefs) == a.managedGenerationEdgesLockedV1(g)
	}
	if !check(state.generation) || state.txn == nil || !check(state.txn.base) {
		return ErrFiniteAllocationExportV1
	}
	edges := uint64(1)
	if state.txn.ledger != state.ledger || !state.txn.ledgerOwned {
		return ErrFiniteAllocationExportV1
	}
	edges++
	for p := state.ownedCandidates; p != nil; p = p.ownedNext {
		if !p.ownedBacking || p.allocator != a || p.candidate == nil || !check(p.candidate.generation) {
			return ErrFiniteAllocationExportV1
		}
		if t := p.rollbackTxn; t != nil {
			if !check(t.base) || t.ledger != state.ledger || !t.ledgerOwned {
				return ErrFiniteAllocationExportV1
			}
			edges++
		}
	}
	for lease := state.generationLeases; lease != nil; lease = lease.next {
		if lease.allocator != a || !check(lease.generation) {
			return ErrFiniteAllocationExportV1
		}
	}
	if state.ledger.ownedRefs != edges {
		return ErrFiniteAllocationExportV1
	}
	return nil
}

// admitResidentAllocationCreditV1 is the guarded resident attachment component,
// not a public finite gate. The caller must already hold the trusted request's
// admission ownership; limits copies cannot own this edge. The public prepare
// and DB pre-WAL guards remain closed until the whole caller/slot/output proof.
//
// Lock order: allocator -> every actual prepared backing -> candidate writer
// -> shared ledger -> trusted independent account. Facet callbacks execute
// inline under engine locks, allocate nothing and never reenter any engine.
// Cut readers touch immutable scalars/tree contents; Close waits on allocator.
// Opaque managed constructors transfer a unique graph. Permanent raw escape
// plus exact generation/ledger refs rejects every unknown external owner before
// debit. Pass one reserves ALL old backing capacities and all new creator refs;
// pass two only assigns nil intrinsic creators. Debit is cumulative.
func (a *Allocator) admitResidentAllocationCreditV1(creator *allocationCreditLeaseV1, epoch uint64) (uint64, error) {
	if a == nil || creator == nil || epoch == 0 {
		return 0, ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.cow
	if a.closed {
		return 0, ErrCandidateConsumed
	}
	if state == nil || state.closed || state.ledger == nil || state.txn == nil {
		return 0, ErrGenerationFormat
	}
	if epoch == state.residentAdmissionEpoch {
		if state.residentAdmissionCreator == creator && state.txn.buildCreator == creator {
			return state.residentAdmissionBytes, nil
		}
		return 0, ErrCandidateConsumed
	}
	if epoch < state.residentAdmissionEpoch || state.txn.buildCreator != nil {
		return 0, ErrCandidateConsumed
	}
	if state.rawGenerationEscaped || state.prepared != nil && !state.prepared.ownedBacking {
		return 0, ErrFiniteAllocationExportV1
	}
	for _, p := range state.activated {
		if !p.ownedBacking {
			return 0, ErrFiniteAllocationExportV1
		}
	}
	// Ordinary generic callbacks taint the lineage before invoking user code.
	// Concrete pager writers acquire backing then writeMu and never reenter a.
	for p := state.ownedCandidates; p != nil; p = p.ownedNext {
		p.backingMu.Lock()
		if p.candidate != nil {
			p.candidate.writeMu.Lock()
		}
	}
	defer func() {
		for p := state.ownedCandidates; p != nil; p = p.ownedNext {
			if p.candidate != nil {
				p.candidate.writeMu.Unlock()
			}
			p.backingMu.Unlock()
		}
	}()
	state.ledger.mu.Lock()
	defer state.ledger.mu.Unlock()
	if err := a.residentProvenanceLockedV1(); err != nil {
		return 0, err
	}
	if state.txn.buildCreator != nil && state.txn.buildCreator != creator {
		return 0, ErrCandidateConsumed
	}
	census := residentAttachmentV1{creator: creator}
	census.allocator(a)
	if state.txn.buildCreator == nil {
		census.refs = cowSaturatingAddV1(census.refs, 1)
	}
	operation, err := admitAllocationOperationV1(creator, census.bytes, census.refs)
	if err != nil {
		return 0, err
	}
	// No fallible engine action may follow the first creator assignment.
	attached := residentAttachmentV1{creator: creator, attach: true, refs: census.refs}
	attached.allocator(a)
	if state.txn.buildCreator == nil {
		state.txn.buildCreator = creator
		creator.mu.Lock()
		state.txn.allocationCredit = creator.facet
		creator.mu.Unlock()
		attached.refs--
	}
	state.txn.privatePreparation = false
	state.residentAdmissionEpoch, state.residentAdmissionBytes, state.residentAdmissionCreator = epoch, census.bytes, creator
	operation.refs = attached.refs
	operation.close()
	return census.bytes, nil
}

// endResidentAllocationCreditV1 revokes the one mutable admission edge under
// the same existing allocator control. Copies of limits retain no ownership.
// The epoch is monotonic; a terminal epoch cannot be admitted again.
func (a *Allocator) endResidentAllocationCreditV1(creator *allocationCreditLeaseV1, epoch uint64) error {
	if a == nil || creator == nil || epoch == 0 {
		return ErrGenerationFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.cow
	if state == nil || state.closed || epoch != state.residentAdmissionEpoch {
		return ErrCandidateConsumed
	}
	if state.residentAdmissionCreator == nil {
		return nil
	}
	if state.residentAdmissionCreator != creator || state.txn == nil || state.txn.buildCreator != creator {
		return ErrCandidateConsumed
	}
	if err := state.txn.endBuildCreatorV1(creator); err != nil {
		return err
	}
	state.residentAdmissionCreator = nil
	return nil
}

// growActivatedBackingLockedV1 admits the replacement capacity before the
// activation creates a builder or mutates ledger visibility. The replaced
// buffer loses all aliases before its historical creating edge is released.
func (a *Allocator) growActivatedBackingLockedV1(creator *allocationCreditLeaseV1) error {
	state := a.cow
	if len(state.activated) < cap(state.activated) {
		return nil
	}
	capacity := grownCapacityV1(cap(state.activated), len(state.activated)+1)
	bytes := allocationClassV1(uint64(capacity)*8, true)
	operation, err := admitAllocationOperationV1(creator, bytes, 1)
	if err != nil {
		return err
	}
	defer operation.close()
	if err = operation.take(bytes, 1); err != nil {
		return err
	}
	next := make([]*PreparedCOWCandidateV1, len(state.activated), capacity)
	copy(next, state.activated)
	oldCreator := state.activatedCreator
	clear(state.activated[:cap(state.activated)])
	state.activated, state.activatedCreator = next, creator
	oldCreator.release()
	return nil
}
