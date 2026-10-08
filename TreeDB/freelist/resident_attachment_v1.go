package freelist

import (
	"sync/atomic"
	"unsafe"
)

// residentAttachmentV1 contains only scalar accounting and a nonowning borrow
// of the existing facet wrapper. Census charges actual allocation classes and
// full capacities; shared representations may be conservatively counted twice.
type residentAttachmentV1 struct {
	creator             *allocationCreditLeaseV1
	bytes, births, refs uint64
	attach              bool
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
			v.births = cowSaturatingAddV1(v.births, bytes)
			v.refs = cowSaturatingAddV1(v.refs, 1)
		}
		return
	}
	if *slot == nil {
		*slot = v.creator
		v.refs--
	}
}
func (v *residentAttachmentV1) tree(r stateRefV1) {
	if r.zero() {
		return
	}
	if r.chunk != nil {
		v.claim(&r.chunk.creator, allocationClassV1(uint64(unsafe.Sizeof(*r.chunk)), true))
		return
	}
	v.claim(&r.branch.creator, allocationClassV1(uint64(unsafe.Sizeof(*r.branch)), true))
	for _, child := range r.branch.child {
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
	v.transaction(p.activationTxn)
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
	if packet := state.packet; packet != nil {
		v.claim(&packet.creator, packet.retainedControlBytesV1())
		for _, txn := range packet.packetOwnedTransactionsV1() {
			v.transaction(txn)
		}
	}
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
	if packet := a.cow.packet; packet != nil {
		for _, txn := range packet.packetOwnedTransactionsV1() {
			if txn != nil && txn.base == g {
				edges++
			}
		}
	}
	for p := a.cow.ownedCandidates; p != nil; p = p.ownedNext {
		if p.activationTxn != nil && p.activationTxn.base == g {
			edges++
		}
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
	if packet := state.packet; packet != nil {
		if packet.allocator.Load() != a || packet.phase > 2 {
			return ErrFiniteAllocationExportV1
		}
		for _, t := range packet.packetOwnedTransactionsV1() {
			if t == nil {
				continue
			}
			if !check(t.base) || t.ledger != state.ledger || !t.ledgerOwned {
				return ErrFiniteAllocationExportV1
			}
			edges++
		}
	}
	for p := state.ownedCandidates; p != nil; p = p.ownedNext {
		if !p.ownedBacking || p.allocator != a || p.candidate == nil || !check(p.candidate.generation) {
			return ErrFiniteAllocationExportV1
		}
		if t := p.activationTxn; t != nil {
			if !check(t.base) || t.ledger != state.ledger || !t.ledgerOwned {
				return ErrFiniteAllocationExportV1
			}
			edges++
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

// mutableCOWTransactionsV1 enumerates legacy and EVERY private packet future.
// Slots are exact owning roles; identity deduplication excludes phase borrows.
// Deduplication uses object identity and fixed stack storage, never tree census.
func mutableCOWTransactionsV1(state *allocatorCOWStateV1) [9]*FreelistTxn {
	var roles [9]*FreelistTxn
	if state == nil {
		return roles
	}
	roles[0] = state.txn
	if state.prepared != nil {
		roles[1], roles[2] = state.prepared.rollbackTxn, state.prepared.activationTxn
	}
	if state.packet != nil {
		owned := state.packet.packetOwnedTransactionsV1()
		copy(roles[3:], owned[:])
	}
	for i := range roles {
		for j := 0; j < i; j++ {
			if roles[i] == roles[j] {
				roles[i] = nil
				break
			}
		}
	}
	return roles
}

// AdmitManagedResidentAllocationV1 exposes the existing managed attachment
// operation, not a finite admission certificate. The installed DB must supply
// its exact concrete creator from the SAME native resident owner and an epoch
// held by the actual serializer/admission lifetime. Interface satisfaction is
// not provenance. The request is borrowed only on this synchronous call stack.
func (a *Allocator) AdmitManagedResidentAllocationV1(request AllocationRequestCreditV1, creator *AllocationCreatorV1, epoch uint64) (uint64, error) {
	if request == nil {
		return 0, ErrAllocationCertificateIncompleteV1
	}
	return a.admitResidentAllocationCreditV1(request, creator, epoch)
}

// EndManagedResidentAllocationV1 revokes the epoch's future-birth edges only.
// Historical intrinsic creators survive until their own exact last edge.
func (a *Allocator) EndManagedResidentAllocationV1(creator *AllocationCreatorV1, epoch uint64) error {
	return a.endResidentAllocationCreditV1(creator, epoch)
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
func (a *Allocator) admitResidentAllocationCreditV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1, epoch uint64) (uint64, error) {
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
	roles := mutableCOWTransactionsV1(state)
	if epoch == state.residentAdmissionEpoch {
		if state.residentAdmissionCreator == creator {
			for _, role := range roles {
				if role != nil && role.buildCreator != creator {
					return 0, ErrCandidateConsumed
				}
			}
			return state.residentAdmissionBytes, nil
		}
		return 0, ErrCandidateConsumed
	}
	if epoch < state.residentAdmissionEpoch {
		return 0, ErrCandidateConsumed
	}
	for _, role := range roles {
		if role != nil && role.buildCreator != nil {
			return 0, ErrCandidateConsumed
		}
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
	for _, role := range roles {
		if role != nil && role.buildCreator != nil && role.buildCreator != creator {
			return 0, ErrCandidateConsumed
		}
	}
	census := residentAttachmentV1{creator: creator}
	census.allocator(a)
	for _, role := range roles {
		if role != nil && role.buildCreator == nil {
			census.refs = cowSaturatingAddV1(census.refs, 1)
		}
	}
	// The current request pays the complete resident loan, including old cuts.
	// Resident preparation pays ONLY unowned backing; historical creator scopes
	// already own their classes and are never rebound or charged a second time.
	operation, err := admitAllocationLoanOperationV1(request, creator, census.bytes, census.births, census.refs)
	if err != nil {
		return 0, err
	}
	// No fallible engine action may follow the first creator assignment.
	attached := residentAttachmentV1{creator: creator, attach: true, refs: census.refs}
	attached.allocator(a)
	for _, role := range roles {
		if role == nil {
			continue
		}
		if role.buildCreator == nil {
			role.buildCreator = creator
			attached.refs--
		}
		role.allocationRequired = true
		role.privatePreparation = false
	}
	state.residentAdmissionEpoch, state.residentAdmissionBytes, state.residentAdmissionCreator = epoch, census.bytes, creator
	operation.refs = attached.refs
	operation.close()
	return census.bytes, nil
}

// endResidentAllocationCreditV1 revokes every exact mutable role edge under
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
	// Validate every mutable role before releasing the first edge. Rollback
	// isolation is a future birth too; retaining its old epoch would bypass
	// current-request/destination re-admission after Abort.
	roles := mutableCOWTransactionsV1(state)
	for _, role := range roles {
		if role != nil && role.buildCreator != nil && role.buildCreator != creator {
			return ErrCandidateConsumed
		}
	}
	for _, role := range roles {
		if role != nil {
			if err := role.endBuildCreatorV1(creator); err != nil {
				return err
			}
		}
	}
	state.residentAdmissionCreator = nil
	return nil
}

// growActivatedBackingLockedV1 admits the replacement capacity before the
// activation creates a builder or mutates ledger visibility. The replaced
// buffer loses all aliases before its historical creating edge is released.
func (a *Allocator) growActivatedBackingLockedV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1) error {
	return a.growActivatedCapacityLockedV1(request, creator, 1)
}
func (a *Allocator) growActivatedCapacityLockedV1(request AllocationRequestCreditV1, creator *allocationCreditLeaseV1, additional int) error {
	state := a.cow
	if additional < 0 || additional > int(^uint(0)>>1)-len(state.activated) {
		return ErrNoAllocatablePage
	}
	required := len(state.activated) + additional
	if required <= cap(state.activated) {
		return nil
	}
	maximum := int(^uint(0) >> 1)
	if len(state.activated) == maximum {
		return ErrNoAllocatablePage
	}
	capacity := grownCapacityV1(cap(state.activated), required)
	if capacity > maximum/8 {
		return ErrNoAllocatablePage
	}
	bytes := allocationClassV1(uint64(capacity)*8, true)
	operation, err := admitAllocationOperationV1(request, creator, bytes, 1)
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
