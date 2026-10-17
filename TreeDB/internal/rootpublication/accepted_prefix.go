package rootpublication

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"reflect"
	"sync"
	"unsafe"
)

var (
	ErrAcceptedCapacity = errors.New("accepted publication prefix capacity exhausted")
	ErrAcceptedReceipt  = errors.New("accepted publication receipt mismatch")
)

// ParticipantProjection borrows an immutable native plan and its retained
// assets from the descriptor's original custodian. Domain and Incarnation are
// logical identities, never physical page or value-log file identities.
// The slice is sparse: an unchanged participant need not be copied into it.
type ParticipantProjection struct {
	Domain, Incarnation uint64
	Plan, Assets        any
}

// LogicalRootSet is one complete immutable version, captured and installed at
// the producer's existing engine cut latch. Raw and Semantic are opaque native
// projections; this package neither interprets them nor discovers participants
// from a second current registry.
type LogicalRootSet struct{ Raw, Semantic any }

// AcceptedCustody consumes a reference transferred from the original native
// version owner. Queue removal invokes it outside publication/build/read gates.
// There is no replacement asset allocator or retirement registry.
type AcceptedCustody interface{ ReleaseAccepted() error }

// AcceptedDescriptor is pre-admitted before assignment. Install transfers its
// original version/plan/asset custody into one bounded FIFO slot. Sequence is
// supplied by the engine's existing clock; this owner never assigns a second
// logical sequence. Canonical LSN remains optional for NoWAL publications.
type AcceptedDescriptor struct {
	Sequence                    uint64
	RootNeutral                 bool
	Projections                 []ParticipantProjection
	Custody                     AcceptedCustody
	AdmittedBytes, CleanupBytes uint64
}

// AcceptedReceipt is an observation of four distinct completion boundaries.
// Assigned/Uncertain survive an append error. Installed proves this exact
// logical version; WALCompleted/Stable concern the actual canonical WAL only.
// Sealed names the exact installed root receipt, never a sampled maximum.
type AcceptedReceipt struct {
	LogicalSequence, CanonicalLSN                                   uint64
	Assigned, Uncertain, Installed, WALCompleted, WALStable, Sealed bool
}

type SealedPrefix struct{ LogicalSequence, CanonicalLSN uint64 }

type LogicalFrontiers struct {
	V, C, W uint64
	S       SealedPrefix
}

// AcceptedTicket is a by-value preaccept capability. A nonce prevents cancelled
// slot reuse from validating a stale preparer's Install or Cancel.
type AcceptedTicket struct {
	owner *Coordinator
	slot  int
	nonce uint64
}

type acceptedEntry struct {
	nonce                          uint64
	reserved, ordered, maintenance bool
	descriptor                     AcceptedDescriptor
	receipt                        AcceptedReceipt
	seal                           PhysicalSealReceipt
	handedOff                      bool
	retiring, released             bool
	releaseErr                     error
}

// acceptedPrefixState is embedded in the existing Coordinator. A replacement
// physical runtime borrows the original coordinator as its logical owner.
// It does not create another scheduler or assign another publication clock.
type acceptedPrefixState struct {
	mu                     sync.Mutex
	changed                *sync.Cond
	entries                []acceptedEntry
	order, free            []int
	head, count, freeCount int
	nonce                  uint64
	maintenanceUsed        bool
	frontiers              LogicalFrontiers
	poison                 error
	closed                 bool
	closing, closeDone     bool
	closeErr               error
	cutNonce, activeCut    uint64
	cutCount               int
	cutThrough             SealedPrefix
	cutRootSet             LogicalRootSet
	retiring               int
}

// AcceptedPrefixOwner returns the original logical authority across a physical
// index/runtime replacement. It grants no producer authority by itself.
func (c *Coordinator) AcceptedPrefixOwner() *Coordinator {
	if c == nil {
		return nil
	}
	if c.acceptedOwner != nil {
		return c.acceptedOwner
	}
	return c
}

// ConfigureAcceptedPrefix admits a fixed queue and one separately reserved
// maintenance slot before producer exposure. The caller charges these arrays
// and the containing coordinator control to its existing finite budget.
func (c *Coordinator) ConfigureAcceptedPrefix(capacity int, initial LogicalFrontiers) error {
	c = c.AcceptedPrefixOwner()
	if c == nil || capacity <= 0 || initial.S.LogicalSequence > initial.V || initial.S.CanonicalLSN > initial.C {
		return ErrAcceptedReceipt
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.entries != nil || p.closed {
		return ErrAcceptedReceipt
	}
	p.entries = make([]acceptedEntry, capacity+1)
	p.order = make([]int, capacity+1)
	p.free = make([]int, capacity)
	for i := range p.free {
		p.free[i] = i
	}
	p.freeCount = capacity
	p.frontiers = initial
	p.changed = sync.NewCond(&p.mu)
	return nil
}

// ReserveAccepted performs bounded preaccept admission. Ordinary producers
// cannot consume the one maintenance/barrier slot when their queue is full.
func (c *Coordinator) ReserveAccepted(maintenance bool) (AcceptedTicket, error) {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return AcceptedTicket{}, ErrAcceptedReceipt
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poison != nil {
		return AcceptedTicket{}, p.poison
	}
	if p.closed || len(p.entries) == 0 {
		return AcceptedTicket{}, ErrAcceptedReceipt
	}
	slot := len(p.entries) - 1
	if maintenance {
		if p.maintenanceUsed {
			return AcceptedTicket{}, ErrAcceptedCapacity
		}
		p.maintenanceUsed = true
	} else {
		if p.freeCount == 0 {
			return AcceptedTicket{}, ErrAcceptedCapacity
		}
		p.freeCount--
		slot = p.free[p.freeCount]
	}
	p.nonce++
	if p.nonce == 0 {
		panic("accepted publication nonce exhausted")
	}
	p.entries[slot] = acceptedEntry{nonce: p.nonce, reserved: true, maintenance: maintenance}
	return AcceptedTicket{owner: c, slot: slot, nonce: p.nonce}, nil
}

func (ticket AcceptedTicket) entryLocked() (*acceptedPrefixState, *acceptedEntry) {
	if ticket.owner == nil {
		return nil, nil
	}
	p := &ticket.owner.accepted
	if ticket.slot < 0 || ticket.slot >= len(p.entries) {
		return p, nil
	}
	e := &p.entries[ticket.slot]
	if !e.reserved || e.nonce != ticket.nonce {
		return p, nil
	}
	return p, e
}

// RecordAssigned records the actual canonical append result in canonical
// sequencer order. A nonzero identity is irreversible, even with append error.
func (ticket AcceptedTicket) RecordAssigned(lsn uint64, appendErr error, stable bool) error {
	if ticket.owner == nil {
		return ErrAcceptedReceipt
	}
	p := &ticket.owner.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	_, e := ticket.entryLocked()
	if e == nil || e.receipt.Assigned || e.receipt.Installed {
		return ErrAcceptedReceipt
	}
	e.receipt.CanonicalLSN, e.receipt.Assigned, e.receipt.Uncertain = lsn, lsn != 0, lsn != 0 && appendErr != nil
	e.receipt.WALCompleted, e.receipt.WALStable = lsn != 0 && appendErr == nil, lsn != 0 && appendErr == nil && stable
	if lsn != 0 {
		p.orderEntryLocked(ticket.slot, e)
	}
	if e.receipt.Uncertain {
		if p.poison == nil {
			p.poison = appendErr
		}
		p.changed.Broadcast()
	}
	return p.poison
}

func (p *acceptedPrefixState) orderEntryLocked(slot int, e *acceptedEntry) {
	if e.ordered {
		return
	}
	p.order[(p.head+p.count)%len(p.order)] = slot
	p.count++
	e.ordered = true
}

// InstallAccepted belongs inside the SAME engine cut installation. The native
// producer has already retained descriptor.Custody; success consumes that edge.
// No allocation or callback runs here. An assigned but unmaterializable entry
// remains in FIFO custody and poisons continuation instead of being skipped.
// AcceptedInstall holds only the existing logical kernel mutex across the
// producer's nonfallible native root swaps. It is not outer disk D. All native
// preparation/admission precedes BeginInstall; no IO, release or wait belongs
// between BeginInstall and Commit. Commit runs at the same engine cut latch.
// This by-value guard adds no per-write allocation or ownership registry.
type AcceptedInstall struct {
	ticket     AcceptedTicket
	descriptor AcceptedDescriptor
	done       bool
}

func (ticket AcceptedTicket) BeginInstall(descriptor AcceptedDescriptor) (AcceptedInstall, error) {
	if ticket.owner == nil {
		return AcceptedInstall{}, ErrAcceptedReceipt
	}
	p := &ticket.owner.accepted
	p.mu.Lock()
	_, e := ticket.entryLocked()
	if p.poison != nil {
		err := p.poison
		p.mu.Unlock()
		return AcceptedInstall{}, err
	}
	invalid := p.closed || e == nil || e.receipt.Installed || (descriptor.Sequence != p.frontiers.V+1 && !((descriptor.RootNeutral || e.maintenance) && descriptor.Sequence == p.frontiers.V)) || e.receipt.Uncertain || (e.receipt.CanonicalLSN != 0 && e.receipt.CanonicalLSN != p.frontiers.C+1) || (len(descriptor.Projections) != 0 && descriptor.Custody == nil)
	if invalid {
		err := error(ErrAcceptedReceipt)
		if p.closed {
			err = ErrClosed
		}
		if e != nil && e.receipt.Assigned && p.poison == nil {
			p.poison = err
			p.changed.Broadcast()
		}
		p.mu.Unlock()
		return AcceptedInstall{}, err
	}
	return AcceptedInstall{ticket: ticket, descriptor: descriptor}, nil
}

func (install *AcceptedInstall) Commit() {
	if install == nil || install.done || install.ticket.owner == nil {
		panic("invalid accepted installation")
	}
	p, e := install.ticket.entryLocked()
	e.descriptor = install.descriptor
	e.receipt.LogicalSequence, e.receipt.Installed = install.descriptor.Sequence, true
	p.orderEntryLocked(install.ticket.slot, e)
	p.frontiers.V = install.descriptor.Sequence
	if e.receipt.CanonicalLSN != 0 {
		p.frontiers.C = e.receipt.CanonicalLSN
	}
	install.done = true
	p.changed.Broadcast()
	p.mu.Unlock()
}

// Abort is a deferred safeguard for a native publication that did not reach
// its nonfallible cut swap. The assigned receipt remains terminally poisoned.
func (install *AcceptedInstall) Abort() {
	if install == nil || install.done || install.ticket.owner == nil {
		return
	}
	p := &install.ticket.owner.accepted
	if p.poison == nil {
		p.poison = ErrAcceptedReceipt
	}
	install.done = true
	p.changed.Broadcast()
	p.mu.Unlock()
}

func (ticket AcceptedTicket) InstallAccepted(descriptor AcceptedDescriptor) error {
	install, err := ticket.BeginInstall(descriptor)
	if err != nil {
		return err
	}
	install.Commit()
	return nil
}

func (ticket AcceptedTicket) Valid() bool { return ticket.owner != nil && ticket.nonce != 0 }

// CancelAccepted cancels only an unassigned private reservation. Its caller
// still owns native private preparation and disposes it through its own path.
func (ticket AcceptedTicket) CancelAccepted() error {
	if ticket.owner == nil {
		return nil
	}
	p := &ticket.owner.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	_, e := ticket.entryLocked()
	if e == nil {
		return ErrAcceptedReceipt
	}
	if e.receipt.Assigned || e.receipt.Installed {
		return ErrAcceptedReceipt
	}
	p.recycleLocked(ticket.slot, e)
	p.changed.Broadcast()
	return nil
}

func (p *acceptedPrefixState) recycleLocked(slot int, e *acceptedEntry) {
	maintenance := e.maintenance
	*e = acceptedEntry{}
	if maintenance {
		p.maintenanceUsed = false
	} else {
		p.free[p.freeCount] = slot
		p.freeCount++
	}
}

func (c *Coordinator) LogicalFrontiers() LogicalFrontiers {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return LogicalFrontiers{}
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.frontiers
}

// ObserveStableWAL accepts only the canonical WAL owner's completed sync proof.
// Physical root sealing must never call this method to invent W.
func (c *Coordinator) ObserveStableWAL(lsn uint64) {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	if lsn > p.frontiers.W {
		p.frontiers.W = lsn
	}
	if p.changed != nil {
		p.changed.Broadcast()
	}
}

func (c *Coordinator) PoisonAccepted(err error) {
	c = c.AcceptedPrefixOwner()
	if c == nil || err == nil {
		return
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poison == nil {
		p.poison = err
	}
	if p.changed != nil {
		p.changed.Broadcast()
	}
}

// AcceptedPrefixAllocationBytes covers fixed queue storage and added logical
// coordinator control. It is admitted once, outside ordinary publication.
func AcceptedPrefixAllocationBytes(capacity int) (uint64, error) {
	if capacity <= 0 || uint64(capacity) > ^uint64(0)/uint64(unsafe.Sizeof(acceptedEntry{}))-1 {
		return 0, ErrAcceptedCapacity
	}
	n := uint64(capacity)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(acceptedPrefixState{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(sync.Cond{})))
	for _, raw := range [3]uint64{(n + 1) * uint64(unsafe.Sizeof(acceptedEntry{})), (n + 1) * uint64(unsafe.Sizeof(int(0))), n * uint64(unsafe.Sizeof(int(0)))} {
		added := retainedalloc.AllocationCharge(raw)
		if added > ^uint64(0)-charge {
			return 0, ErrAcceptedCapacity
		}
		charge += added
	}
	return charge, nil
}

// AcceptedCut is one finite complete prefix. New acceptances append behind it;
// the original descriptor/plan/assets remain in their slots through handoff.
// Only the outer disk coordinator may have a cut active at a time.
type AcceptedCut struct {
	owner        *Coordinator
	nonce        uint64
	start, count int
	through      SealedPrefix
	rootSet      LogicalRootSet
}

// Through and RootSet expose by-value observations without allowing a caller
// to rewrite the authority stored in the original capture capability.
func (cut AcceptedCut) Through() SealedPrefix   { return cut.through }
func (cut AcceptedCut) RootSet() LogicalRootSet { return cut.rootSet }
func (cut AcceptedCut) Valid() bool             { return cut.owner != nil && cut.nonce != 0 }

// CaptureAcceptedCut binds a root-set retained once by the outer disk owner.
// The producer captures through and roots coherently under its existing engine
// lease/latch, then calls here after releasing gates needed by retirement.
// Entries retain sparse native plans/assets only; raw effects stay in their
// existing COW sources. No per-write whole-cut reader is created.
func (c *Coordinator) CaptureAcceptedCut(through SealedPrefix, roots LogicalRootSet) (AcceptedCut, error) {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return AcceptedCut{}, ErrAcceptedReceipt
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.retiring != 0 && p.poison == nil && !p.closed {
		p.changed.Wait()
	}
	if p.poison != nil {
		return AcceptedCut{}, p.poison
	}
	if !sameLogicalRootSet(roots, roots) || p.closed || p.changed == nil || p.activeCut != 0 || through.LogicalSequence < p.frontiers.S.LogicalSequence || through.CanonicalLSN < p.frontiers.S.CanonicalLSN || through.LogicalSequence > p.frontiers.V || through.CanonicalLSN > p.frontiers.C {
		return AcceptedCut{}, ErrAcceptedReceipt
	}
	count, start := 0, 0
	reached := p.frontiers.S
	for ; count < p.count; count++ {
		e := &p.entries[p.order[(p.head+count)%len(p.order)]]
		if e.receipt.LogicalSequence > through.LogicalSequence || e.receipt.CanonicalLSN > through.CanonicalLSN {
			break
		}
		if !e.receipt.Installed || e.receipt.Uncertain {
			return AcceptedCut{}, ErrAcceptedReceipt
		}
		reached.LogicalSequence = e.receipt.LogicalSequence
		if e.receipt.CanonicalLSN != 0 {
			reached.CanonicalLSN = e.receipt.CanonicalLSN
		}
		if e.handedOff && start == count {
			start = count + 1
		}
	}
	if reached != through {
		return AcceptedCut{}, ErrAcceptedReceipt
	}
	p.cutNonce++
	if p.cutNonce == 0 {
		panic("accepted cut nonce exhausted")
	}
	p.activeCut, p.cutCount = p.cutNonce, count
	p.cutThrough, p.cutRootSet = through, roots
	return AcceptedCut{owner: c, nonce: p.cutNonce, start: start, count: count, through: through, rootSet: roots}, nil
}

// Root projections are opaque comparable native identities (normally pointers).
// Reject noncomparable containers rather than comparing mutable contents.
func sameLogicalRootSet(a, b LogicalRootSet) bool {
	same := func(x, y any) bool {
		if x == nil || y == nil {
			return x == nil && y == nil
		}
		t := reflect.TypeOf(x)
		return t == reflect.TypeOf(y) && t.Comparable() && x == y
	}
	return same(a.Raw, b.Raw) && same(a.Semantic, b.Semantic)
}

// validatePhysicalCandidate runs before EnqueueBuilt's final fallible step.
// Native materialization belongs to the producer/group bound to this cut;
// canonical coverage and physical acceptance identity are checked here.
func (cut AcceptedCut) validatePhysicalCandidate(c *Coordinator, candidate *PreparedRootCandidate) error {
	if cut.owner == nil || c.AcceptedPrefixOwner() != cut.owner || candidate == nil || candidate.frontier.appliedCommandLSN != cut.through.CanonicalLSN {
		return ErrAcceptedReceipt
	}
	p := &cut.owner.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poison != nil {
		return p.poison
	}
	if p.closed || p.activeCut != cut.nonce || p.cutCount != cut.count || p.cutThrough != cut.through || !sameLogicalRootSet(p.cutRootSet, cut.rootSet) {
		return ErrAcceptedReceipt
	}
	return nil
}

// Range visits the captured sparse native plans without building a second
// projection table. Its callback runs outside kernel and engine read latches.
func (cut AcceptedCut) Range(visit func(AcceptedDescriptor, AcceptedReceipt) error) error {
	if cut.owner == nil || visit == nil {
		return ErrAcceptedReceipt
	}
	p := &cut.owner.accepted
	for i := cut.start; i < cut.count; i++ {
		p.mu.Lock()
		if p.activeCut != cut.nonce || p.cutCount != cut.count || p.cutThrough != cut.through || !sameLogicalRootSet(p.cutRootSet, cut.rootSet) {
			p.mu.Unlock()
			return ErrAcceptedReceipt
		}
		e := &p.entries[p.order[(p.head+i)%len(p.order)]]
		descriptor, receipt := e.descriptor, e.receipt
		p.mu.Unlock()
		if err := visit(descriptor, receipt); err != nil {
			return err
		}
	}
	return nil
}

// CompleteHandoff maps exactly this captured prefix to the actual accepted
// disk generation. It follows the cache's matching-generation installation.
// No later current root or sampled command maximum participates in this proof.
func (cut AcceptedCut) CompleteHandoff(seal PhysicalSealReceipt) error {
	if cut.owner == nil || !seal.Valid() || seal.logicalOwner != cut.owner || seal.cutNonce != cut.nonce || seal.through != cut.through {
		return ErrAcceptedReceipt
	}
	p := &cut.owner.accepted
	p.mu.Lock()
	if p.activeCut != cut.nonce || p.cutCount != cut.count || p.cutThrough != cut.through || !sameLogicalRootSet(p.cutRootSet, cut.rootSet) {
		p.mu.Unlock()
		return ErrAcceptedReceipt
	}
	for i := cut.start; i < cut.count; i++ {
		e := &p.entries[p.order[(p.head+i)%len(p.order)]]
		if e.handedOff {
			p.mu.Unlock()
			return ErrAcceptedReceipt
		}
		e.seal, e.handedOff = seal, true
	}
	p.activeCut, p.cutCount = 0, 0
	p.cutThrough, p.cutRootSet = SealedPrefix{}, LogicalRootSet{}
	p.changed.Broadcast()
	p.mu.Unlock()
	if seal.Sealed() {
		cut.owner.observeAcceptedPhysicalSeal(seal.coordinator, seal.sequence)
	}
	return cut.owner.drainSealedAccepted()
}

// Abandon ends only private disk construction. Accepted source custody stays
// in the queue; a later disk pass may capture and materialize the same prefix.
func (cut AcceptedCut) Abandon() error {
	if cut.owner == nil {
		return nil
	}
	p := &cut.owner.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeCut != cut.nonce || p.cutCount != cut.count || p.cutThrough != cut.through || !sameLogicalRootSet(p.cutRootSet, cut.rootSet) {
		return ErrAcceptedReceipt
	}
	p.activeCut, p.cutCount = 0, 0
	p.cutThrough, p.cutRootSet = SealedPrefix{}, LogicalRootSet{}
	p.changed.Broadcast()
	return nil
}

func (c *Coordinator) observeAcceptedPhysicalSeal(physical *Coordinator, sequence uint64) {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < p.count; i++ {
		e := &p.entries[p.order[(p.head+i)%len(p.order)]]
		if e.handedOff && e.seal.coordinator == physical && e.seal.sequence <= sequence {
			e.receipt.Sealed = true
		}
	}
	if p.changed != nil {
		p.changed.Broadcast()
	}
}

// drainSealedAccepted consumes original native references outside all kernel
// and physical coordinator locks. A refused release is never replayed; its
// original error poisons the prefix and the original finalizer retains debt.
func (c *Coordinator) drainSealedAccepted() error {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return nil
	}
	p := &c.accepted
	for {
		p.mu.Lock()
		if p.poison != nil {
			err := p.poison
			p.mu.Unlock()
			return err
		}
		if p.count == 0 || p.activeCut != 0 {
			p.mu.Unlock()
			return nil
		}
		slot := p.order[p.head]
		e := &p.entries[slot]
		if !e.handedOff || !e.receipt.Sealed || e.retiring {
			p.mu.Unlock()
			return nil
		}
		e.retiring = true
		p.retiring++
		custody := e.descriptor.Custody
		p.mu.Unlock()
		var err error
		if custody != nil {
			err = custody.ReleaseAccepted()
		}
		custody = nil
		p.mu.Lock()
		e = &p.entries[slot]
		e.retiring = false
		p.retiring--
		e.released = true
		e.releaseErr = err
		if err != nil {
			p.poison = err
			p.changed.Broadcast()
			p.mu.Unlock()
			return err
		}
		p.frontiers.S.LogicalSequence = e.receipt.LogicalSequence
		if e.receipt.CanonicalLSN != 0 {
			p.frontiers.S.CanonicalLSN = e.receipt.CanonicalLSN
		}
		p.head = (p.head + 1) % len(p.order)
		p.count--
		p.recycleLocked(slot, e)
		p.changed.Broadcast()
		p.mu.Unlock()
	}
}

// WaitAccepted joins the requested logical/canonical seal after the caller has
// ended its in-flight publication and released every gate needed by the drain.
func (c *Coordinator) WaitAccepted(ctx context.Context, through SealedPrefix) error {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return ErrAcceptedReceipt
	}
	p := &c.accepted
	p.mu.Lock()
	if p.changed == nil {
		p.mu.Unlock()
		return ErrAcceptedReceipt
	}
	stop := context.AfterFunc(ctx, func() { p.mu.Lock(); p.changed.Broadcast(); p.mu.Unlock() })
	defer stop()
	defer p.mu.Unlock()
	for {
		if p.poison != nil {
			return p.poison
		}
		if p.frontiers.S.LogicalSequence >= through.LogicalSequence && p.frontiers.S.CanonicalLSN >= through.CanonicalLSN {
			return nil
		}
		if p.closed {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		p.changed.Wait()
	}
}

// AcceptedError is the original terminal prefix refusal, if any.
func (c *Coordinator) AcceptedError() error {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return nil
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.poison
}

// CloseAccepted follows producer withdrawal and the last disk drain. It joins
// an original release already in flight without holding gates it needs, then
// consumes each remaining original reference once, preserving terminal poison.
func (c *Coordinator) CloseAccepted() error {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return nil
	}
	p := &c.accepted
	p.mu.Lock()
	if p.changed == nil {
		p.mu.Unlock()
		return nil
	}
	if p.closing {
		for !p.closeDone {
			p.changed.Wait()
		}
		err := p.closeErr
		p.mu.Unlock()
		return err
	}
	p.closing = true
	p.closed = true
	p.changed.Broadcast()
	for {
		busy := false
		for i := range p.entries {
			if p.entries[i].retiring {
				busy = true
				break
			}
		}
		if !busy {
			break
		}
		p.changed.Wait()
	}
	originalErr := p.poison
	p.activeCut, p.cutCount = 0, 0
	p.cutThrough, p.cutRootSet = SealedPrefix{}, LogicalRootSet{}
	for i := range p.entries {
		e := &p.entries[i]
		if !e.reserved || e.released || e.descriptor.Custody == nil {
			continue
		}
		e.retiring = true
		custody := e.descriptor.Custody
		p.mu.Unlock()
		var err error
		if custody != nil {
			err = custody.ReleaseAccepted()
		}
		custody = nil
		p.mu.Lock()
		e = &p.entries[i]
		e.retiring = false
		e.released = true
		e.releaseErr = err
		if err == nil {
			e.descriptor = AcceptedDescriptor{}
		}
		if err != nil {
			originalErr = errors.Join(originalErr, err)
			if p.poison == nil {
				p.poison = err
			}
		}
		p.changed.Broadcast()
	}
	// Failed original entries keep their exact Custody, first error and admitted
	// containing arrays. Successful siblings may be scrubbed/refunded once.
	failed := false
	for i := range p.entries {
		if p.entries[i].releaseErr != nil {
			failed = true
			break
		}
	}
	if !failed {
		clear(p.entries)
		clear(p.order)
		clear(p.free)
		p.entries, p.order, p.free = nil, nil, nil
	}
	p.count, p.freeCount, p.head = 0, 0, 0
	p.closeErr, p.closeDone = originalErr, true
	p.changed.Broadcast()
	p.mu.Unlock()
	return originalErr
}

// AcceptedCleanupFailed reports terminal original custody still retained in
// the original queue allocation. The caller must retain its containing lease.
func (c *Coordinator) AcceptedCleanupFailed() bool {
	c = c.AcceptedPrefixOwner()
	if c == nil {
		return false
	}
	p := &c.accepted
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.entries {
		if p.entries[i].releaseErr != nil {
			return true
		}
	}
	return false
}
