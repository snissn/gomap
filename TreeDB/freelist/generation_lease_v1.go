package freelist

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// PublishedGenerationLeaseV1 makes real physical-cut generation ownership
// visible to subsequent admission, including cuts captured in ordinary mode.
// Closing the source DB does not revoke this lease.
type PublishedGenerationLeaseV1 struct {
	mu         sync.Mutex
	creator    *allocationCreditLeaseV1
	allocator  *Allocator
	generation *FreelistGenerationV1
	ref        GenerationRefV1
	next       *PublishedGenerationLeaseV1
	closed     bool
}

func (a *Allocator) AcquirePublishedGenerationLeaseV1(expected GenerationRefV1) (*PublishedGenerationLeaseV1, error) {
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
	// Ordinary acquisition preserves its contract. A future finite capture must
	// admit its wrapper/directory/pins before calling this constructor.
	if a.cow.txn != nil && a.cow.txn.allocationCredit != nil {
		return nil, ErrAllocationCertificateIncompleteV1
	}
	lease := &PublishedGenerationLeaseV1{allocator: a, generation: retainGenerationV1(g), ref: g.ref, next: a.cow.generationLeases}
	a.cow.generationLeases = lease
	return lease, nil
}

func (lease *PublishedGenerationLeaseV1) GenerationRefV1() GenerationRefV1 {
	if lease == nil {
		return GenerationRefV1{}
	}
	return lease.ref
}

func (lease *PublishedGenerationLeaseV1) SnapshotPageUnusedV1(id, oldestCommit uint64) (bool, error) {
	if lease == nil {
		return false, ErrGenerationFormat
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || lease.generation == nil {
		return false, ErrFiniteAllocationExportV1
	}
	return lease.generation.SnapshotPageUnusedV1(id, oldestCommit)
}

func (lease *PublishedGenerationLeaseV1) Close() {
	if lease == nil {
		return
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return
	}
	a := lease.allocator
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow != nil {
		link := &a.cow.generationLeases
		for *link != nil {
			if *link == lease {
				*link = lease.next
				break
			}
			link = &(*link).next
		}
	}
	lease.closed = true
	generation, creator := lease.generation, lease.creator
	lease.generation, lease.allocator, lease.next, lease.creator = nil, nil, nil, nil
	releaseGenerationV1(generation)
	creator.release()
	a.releaseClosedCOWStateCreatorLockedV1()
}

// ResidentGenerationProfileV1 is an allocation-free census of real generation
// owners. Distinct lease roots are counted by instance, never reused page ID.
type ResidentGenerationProfileV1 struct {
	PhysicalCutLeases, DistinctPhysicalCutGenerations                                                                       uint64
	RetainedHighWaterSum, ReservationExtentCapacity, MetadataIDCapacity                                                     uint64
	RawWriterEscaped                                                                                                        bool
	RawLedgerEscaped                                                                                                        bool
	RawGenerationEscaped                                                                                                    bool
	RepresentationCount, TransactionCount, CandidateCount                                                                   uint64
	StateNodeCount, StateChunkCount, CandidatePageCount                                                                     uint64
	GenerationBytes, TransactionBytes, CandidateBytes, PhysicalCutLeaseBytes, SharedLedgerBytes, AllocatorBytes, TotalBytes uint64
}

func (a *Allocator) ResidentGenerationProfileV1() ResidentGenerationProfileV1 {
	if a == nil {
		return ResidentGenerationProfileV1{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.residentGenerationProfileLockedV1()
}

func (a *Allocator) residentGenerationProfileLockedV1() ResidentGenerationProfileV1 {
	var profile ResidentGenerationProfileV1
	if a.cow == nil {
		return profile
	}
	profile.RawWriterEscaped = a.rawWriterEscaped
	profile.RawGenerationEscaped = a.cow.rawGenerationEscaped
	profile.AllocatorBytes = allocationClassV1(uint64(unsafe.Sizeof(*a)), true)
	profile.AllocatorBytes = cowSaturatingAddV1(profile.AllocatorBytes, allocationClassV1(uint64(unsafe.Sizeof(*a.cow)), true))
	if a.cow.ready != nil {
		profile.AllocatorBytes = cowSaturatingAddV1(profile.AllocatorBytes, allocationClassV1(uint64(unsafe.Sizeof(*a.cow.ready)), true))
	}
	if a.writerAuthority != nil {
		profile.AllocatorBytes = cowSaturatingAddV1(profile.AllocatorBytes, allocationClassV1(uint64(unsafe.Sizeof(*a.writerAuthority)), false))
	}
	profile.AllocatorBytes = cowSaturatingAddV1(profile.AllocatorBytes, allocationClassV1(uint64(cap(a.cow.activated))*8, true))
	for lease := a.cow.generationLeases; lease != nil; lease = lease.next {
		profile.PhysicalCutLeases++
		seen := false
		for prior := a.cow.generationLeases; prior != lease; prior = prior.next {
			if prior.generation == lease.generation {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		profile.DistinctPhysicalCutGenerations++
		g := lease.generation
		profile.addGeneration(g)
		profile.RetainedHighWaterSum = cowSaturatingAddV1(profile.RetainedHighWaterSum, g.highWater)
		profile.ReservationExtentCapacity = cowSaturatingAddV1(profile.ReservationExtentCapacity, uint64(cap(g.record.Extents)))
		profile.MetadataIDCapacity = cowSaturatingAddV1(profile.MetadataIDCapacity, uint64(cap(g.metadataPages)+cap(g.record.pageIDs)))
	}
	profile.addGeneration(a.cow.generation)
	profile.addTransaction(a.cow.txn)
	for prepared := a.cow.ownedCandidates; prepared != nil; prepared = prepared.ownedNext {
		profile.addPrepared(prepared)
	}
	if prepared := a.cow.prepared; prepared != nil && !prepared.ownedBacking {
		profile.addPrepared(prepared)
	}
	for _, prepared := range a.cow.activated {
		if !prepared.ownedBacking {
			profile.addPrepared(prepared)
		}
	}
	profile.PhysicalCutLeaseBytes = cowSaturatingMulV1(profile.PhysicalCutLeases, allocationClassV1(uint64(unsafe.Sizeof(PublishedGenerationLeaseV1{})), true))
	if ledger := a.cow.ledger; ledger != nil {
		ledger.mu.Lock()
		profile.RawLedgerEscaped = ledger.rawEscaped
		bytes := allocationClassV1(uint64(unsafe.Sizeof(*ledger)), true)
		bytes = cowSaturatingAddV1(bytes, ledger.owners.residentBytesV1())
		bytes = cowSaturatingAddV1(bytes, ledger.candidates.residentBytesV1())
		bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(ledger.burnedTails)), uint64(unsafe.Sizeof(reservationInterval{}))), false))
		ledger.candidates.Range(func(_ CandidateIDV1, r *reservation) bool {
			bytes = cowSaturatingAddV1(bytes, allocationClassV1(uint64(unsafe.Sizeof(*r)), true))
			bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(r.ids)), 8), false))
			bytes = cowSaturatingAddV1(bytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(r.abandonedCoverage)), uint64(unsafe.Sizeof(reservationInterval{}))), false))
			return true
		})
		ledger.mu.Unlock()
		profile.SharedLedgerBytes = bytes
	}
	profile.TotalBytes = cowSaturatingAddV1(profile.GenerationBytes, profile.TransactionBytes)
	profile.TotalBytes = cowSaturatingAddV1(profile.TotalBytes, profile.CandidateBytes)
	profile.TotalBytes = cowSaturatingAddV1(profile.TotalBytes, profile.PhysicalCutLeaseBytes)
	profile.TotalBytes = cowSaturatingAddV1(profile.TotalBytes, profile.SharedLedgerBytes)
	profile.TotalBytes = cowSaturatingAddV1(profile.TotalBytes, profile.AllocatorBytes)
	return profile
}

func cowSaturatingAddV1(a, b uint64) uint64 {
	if a > ^uint64(0)-b {
		return ^uint64(0)
	}
	return a + b
}

func (profile *ResidentGenerationProfileV1) addTree(root stateRefV1) {
	if root.zero() {
		return
	}
	if root.creator() != nil {
		profile.GenerationBytes = cowSaturatingAddV1(profile.GenerationBytes, allocationClassV1(uint64(unsafe.Sizeof(allocationCreditLeaseV1{})), true))
	}
	if root.chunk != nil {
		profile.StateChunkCount++
		profile.GenerationBytes = cowSaturatingAddV1(profile.GenerationBytes, allocationClassV1(uint64(unsafe.Sizeof(*root.chunk)), true))
		return
	}
	profile.StateNodeCount++
	profile.GenerationBytes = cowSaturatingAddV1(profile.GenerationBytes, allocationClassV1(uint64(unsafe.Sizeof(*root.branch)), true))
	for _, child := range root.branch.child {
		profile.addTree(child)
	}
}
func (profile *ResidentGenerationProfileV1) addGeneration(g *FreelistGenerationV1) {
	if g == nil {
		return
	}
	if atomic.LoadUint32(&g.escaped) != 0 {
		profile.RawGenerationEscaped = true
	}
	profile.RepresentationCount++
	profile.GenerationBytes = cowSaturatingAddV1(profile.GenerationBytes, allocationClassV1(uint64(unsafe.Sizeof(*g)), true))
	profile.GenerationBytes = cowSaturatingAddV1(profile.GenerationBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(g.record.Extents)), uint64(unsafe.Sizeof(ReservationExtentV1{}))), false))
	profile.GenerationBytes = cowSaturatingAddV1(profile.GenerationBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(g.record.pageIDs)), 8), false))
	profile.GenerationBytes = cowSaturatingAddV1(profile.GenerationBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(g.metadataPages)), 8), false))
	profile.addTree(g.root)
}
func (profile *ResidentGenerationProfileV1) addTransaction(txn *FreelistTxn) {
	if txn == nil {
		return
	}
	profile.TransactionCount++
	profile.TransactionBytes = cowSaturatingAddV1(profile.TransactionBytes, allocationClassV1(uint64(unsafe.Sizeof(*txn)), true))
	profile.TransactionBytes = cowSaturatingAddV1(profile.TransactionBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(txn.allocated)), uint64(unsafe.Sizeof(allocatedPage{}))), false))
	profile.TransactionBytes = cowSaturatingAddV1(profile.TransactionBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(txn.abandonedAppends)), uint64(unsafe.Sizeof(ReservationExtentV1{}))), false))
	profile.TransactionBytes = cowSaturatingAddV1(profile.TransactionBytes, txn.changedChunks.residentBytesV1())
	profile.TransactionBytes = cowSaturatingAddV1(profile.TransactionBytes, txn.replacedMetadata.residentBytesV1())
	profile.addGeneration(txn.base)
	profile.addTree(txn.root)
}
func (profile *ResidentGenerationProfileV1) addPrepared(prepared *PreparedCOWCandidateV1) {
	if prepared == nil {
		return
	}
	prepared.backingMu.RLock()
	defer prepared.backingMu.RUnlock()
	if prepared == nil {
		return
	}
	profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(uint64(unsafe.Sizeof(*prepared)), true))
	profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(prepared.auxiliary)), 8), false))
	profile.addTransaction(prepared.rollbackTxn)
	c := prepared.candidate
	if c == nil {
		return
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	profile.CandidateCount++
	profile.CandidatePageCount = cowSaturatingAddV1(profile.CandidatePageCount, uint64(len(c.pages)))
	profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(uint64(unsafe.Sizeof(*c)), true))
	profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(c.pages)), uint64(unsafe.Sizeof(candidatePageV1{}))), true))
	profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(cowSaturatingMulV1(uint64(cap(c.dirtyIDs)), 8), false))
	if c.creator != nil {
		profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(uint64(unsafe.Sizeof(*c.creator)), true))
	}
	for _, page := range c.pages[:cap(c.pages)] {
		profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(uint64(cap(page.view.data)), false))
		if page.view.index != nil {
			profile.CandidateBytes = cowSaturatingAddV1(profile.CandidateBytes, allocationClassV1(uint64(unsafe.Sizeof(*page.view.index)), false))
		}
	}
	profile.addGeneration(c.generation)
}
func cowSaturatingMulV1(a, b uint64) uint64 {
	if b != 0 && a > ^uint64(0)/b {
		return ^uint64(0)
	}
	return a * b
}

func (lease *PublishedGenerationLeaseV1) HighWater() uint64 {
	if lease == nil {
		return 0
	}
	return lease.ref.HighWater
}
func (lease *PublishedGenerationLeaseV1) Allocatable(id uint64) bool {
	if lease == nil {
		return false
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	return !lease.closed && lease.generation != nil && lease.generation.Allocatable(id)
}
