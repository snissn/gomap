package freelist

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// PublishActivatedCOWPrefixWithWorkV6 consumes the SAME original ledger prefix
// exactly once. Unlike preparation, publication cannot yield after its first
// ledger effect. The actual whole prefix/ledger geometry is therefore admitted
// under the allocator and ledger locks before canonical publication begins.
// An unsupported geometry refuses without changing the ledger or allocator.
// This is an explicit finite geometry limit, not history-independent progress.
//
// Eager mutation of the live successor is unnecessary at this fence. Both
// ordinary PrepareCOWCandidateRetiringWithLimitsV1 and COWPreparationWorkV6
// already prune their own staged transaction with the current durable reuse
// capability before materialization. Keeping retired bits until that existing
// stage delays reuse conservatively; no mutable-txn callback enters Finish and
// no native continuation is delegated to an ordinary writer.
// COWPublicationScopeV6 is call-local lock custody. It must be closed in the
// SAME nonyielding authority call, before disposition, Consume or Finish. Its
// identity guard rejects copying the scope while its original locks are held.
type COWPublicationScopeV6 struct {
	allocator *Allocator
	self      *COWPublicationScopeV6
	prefix    []*PreparedCOWCandidateV1
	active    bool
}

func (a *Allocator) PublishActivatedCOWPrefixWithWorkV6(prefix []*PreparedCOWCandidateV1, nextCapability ReuseCapability, w *iterator.OrdinalScanWork) (bool, error) {
	var scope COWPublicationScopeV6
	ready, err := a.BeginCOWPublicationScopeV6(&scope, prefix, w)
	if !ready || err != nil {
		return false, err
	}
	defer scope.Close()
	err = scope.Commit(nextCapability)
	return err == nil, err
}

// BeginCOWPublicationScopeV6 admits the current canonical publication geometry
// while BOTH original locks exclude private rollback and ordinary edits. False
// returns release every lock without a ledger or allocator mutation. A true
// return transfers these call-local locks to scope; callers must not yield.
func (a *Allocator) BeginCOWPublicationScopeV6(scope *COWPublicationScopeV6, prefix []*PreparedCOWCandidateV1, w *iterator.OrdinalScanWork) (ready bool, err error) {
	if a == nil || scope == nil || scope.active || scope.self != nil || len(prefix) == 0 || w == nil {
		return false, ErrGenerationFormat
	}
	a.mu.Lock()
	locked := true
	defer func() {
		if locked {
			a.mu.Unlock()
		}
	}()
	if a.cow == nil || a.cow.ledger == nil || len(prefix) > len(a.cow.activated) {
		return false, ErrCandidateConsumed
	}
	if a.cow.waitErr != nil {
		return false, a.cow.waitErr
	}
	if ok, e := appendReserve(w, 1+uint64(len(prefix)), 2*uint64(unsafe.Sizeof(*scope))+uint64(len(prefix))*(uint64(unsafe.Sizeof(PreparedCOWCandidateV1{}))+32)); !ok || e != nil {
		return false, e
	}
	l := a.cow.ledger
	l.mu.Lock()
	ledgerLocked := true
	defer func() {
		if ledgerLocked {
			l.mu.Unlock()
		}
	}()
	// Eight visits per prefix cover validation, ID construction, map lookup and
	// insertion, actual ledger publication, and final marking. Map bytes are an
	// explicit conservative work bound, not exact Go heap allocation admission.
	records, bytes := a.cowPublicationBaseAdmissionV6(uint64(len(prefix)), uint64(len(prefix)))
	add := func(r, b uint64) bool {
		if r > ^uint64(0)-records || b > ^uint64(0)-bytes {
			return false
		}
		records += r
		bytes += b
		return true
	}
	for i, prepared := range prefix {
		if prepared == nil || prepared.candidate == nil || !prepared.activated || prepared.published || a.cow.activated[i] != prepared {
			return false, ErrCandidateConsumed
		}
		r := l.candidates[prepared.candidateID]
		if r == nil {
			return false, ErrGenerationFormat
		}
		n := uint64(len(r.ids))
		if n > ^uint64(0)/32 || !add(n, n*32) {
			return false, ErrGenerationFormat
		}
		if len(r.abandonedCoverage) != 0 {
			burned, coverage := uint64(len(l.burnedTails)), uint64(len(r.abandonedCoverage))
			width := uint64(unsafe.Sizeof(reservationInterval{}))
			if coverage > ^uint64(0)/width-2 || burned != 0 && (2+coverage > ^uint64(0)/burned || (2+coverage)*width > ^uint64(0)/burned) {
				return false, ErrGenerationFormat
			}
			if !add(burned*(2+coverage), burned*(2+coverage)*width) {
				return false, ErrGenerationFormat
			}
		}
	}
	generation := prefix[len(prefix)-1].candidate.Generation()
	if generation == nil {
		return false, ErrGenerationFormat
	}
	if err := a.validateCOWPhysicalTailLockedV1(generation.HighWater()); err != nil {
		return false, err
	}
	if ok, e := appendReserve(w, records, bytes); !ok || e != nil {
		return false, e
	}
	scope.allocator, scope.self, scope.prefix, scope.active = a, scope, prefix, true
	locked, ledgerLocked = false, false
	return true, nil
}

func (a *Allocator) cowPublicationBaseAdmissionV6(prefix, activated uint64) (uint64, uint64) {
	return 3 + 8*prefix + activated, uint64(unsafe.Sizeof(*a.cow)) + 256 + prefix*(uint64(unsafe.Sizeof(CandidateIDV1{}))+128) + activated*16
}

// Commit consumes the original canonical ledger exactly once without further
// work admission or lock acquisition. The scope remains locked until Close so
// the enclosing SAME durable install can complete before exposing successors.
func (scope *COWPublicationScopeV6) Commit(nextCapability ReuseCapability) error {
	if scope == nil || scope.self != scope || !scope.active || scope.allocator == nil || len(scope.prefix) == 0 {
		return ErrCandidateConsumed
	}
	a := scope.allocator
	err := a.publishActivatedCOWPrefixPolicyLockedV1(scope.prefix, nextCapability, false, true)
	if err == nil {
		scope.prefix = nil
	}
	return err
}

func (scope *COWPublicationScopeV6) Close() {
	if scope == nil || scope.self != scope || !scope.active {
		return
	}
	a := scope.allocator
	scope.allocator, scope.self, scope.prefix, scope.active = nil, nil, nil, false
	a.cow.ledger.mu.Unlock()
	a.mu.Unlock()
}

// Scalar-only pre-effect test observation of this SAME private preparation.
// No reservation, page, root, allocator or callback alias escapes.
type NativeCOWGeometryForTestV6 struct {
	SourceMatches, PreVisible, TailWritten, Append               bool
	Owners, Candidates, IDs, Data, NewIDs, Burned, Coverage      uint64
	TailStart, TailCount, LiveHighWater, RequiredHighWater       uint64
	Activated, ActivatedCapacity, MetadataPages                  uint64
	MetadataFirst, MetadataLast                                  uint64
	ClaimRecords, ClaimBytes, ActivationRecords, ActivationBytes uint64
	PublicationRecords, PublicationBytes                         uint64
	PageSizesValid                                               bool
}

func (s *COWPreparationWorkV6) MaintenanceNativeGeometryForTestV6(w *iterator.OrdinalScanWork) (g NativeCOWGeometryForTestV6, ready bool) {
	if s == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(g))) {
		return g, false
	}
	a := s.allocator
	if a == nil {
		return g, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*a))+2*uint64(unsafe.Sizeof(FreelistTxn{}))) {
		return g, false
	}
	if a.cow == nil || a.cow.txn == nil || s.ledger == nil || s.private == nil || s.result == nil {
		return g, false
	}
	g.SourceMatches = s.sourceLocked(a)
	g.LiveHighWater, g.RequiredHighWater = a.cow.txn.highWater, s.result.Generation().HighWater()
	g.Activated, g.ActivatedCapacity = uint64(len(a.cow.activated)), uint64(cap(a.cow.activated))
	g.ActivationRecords, g.ActivationBytes = a.activationAdmissionV6()
	l := s.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*l))+2*uint64(unsafe.Sizeof(reservation{}))) {
		return g, false
	}
	r := l.candidates[s.candidateID]
	if r == nil {
		return g, false
	}
	g.PreVisible = r.state == CandidatePreVisible
	g.Owners, g.Candidates, g.IDs, g.Data = uint64(len(l.owners)), uint64(len(l.candidates)), uint64(len(r.ids)), uint64(len(s.private.allocated))
	g.Burned, g.Coverage = uint64(len(l.burnedTails)), uint64(len(r.abandonedCoverage))
	g.TailStart, g.TailCount, g.TailWritten = r.tailStart, r.tailCount, r.tailWriteAttempted
	g.Append = r.tailReserved && !r.reusedMetadata
	g.ClaimRecords, g.ClaimBytes = s.claimDataAdmissionLockedV6(g.ActivationRecords+1, g.ActivationBytes+48)
	for i := range s.private.allocated {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(allocatedPage{}))+32) {
			return g, false
		}
		id := s.private.allocated[i].id
		if _, exists := l.owners[id]; exists {
			continue
		}
		unique := true
		for j := 0; j < i; j++ {
			if !w.Reserve(1, uint64(unsafe.Sizeof(allocatedPage{}))) {
				return g, false
			}
			if s.private.allocated[j].id == id {
				unique = false
				break
			}
		}
		if unique {
			g.NewIDs++
		}
	}
	g.MetadataPages, g.PageSizesValid = uint64(len(s.result.pages)), true
	for i := range s.result.pages {
		if !w.Reserve(1, uint64(unsafe.Sizeof(PageImageV1{}))) {
			return g, false
		}
		g.PageSizesValid = g.PageSizesValid && len(s.result.pages[i].Data) == 4096
		id := s.result.pages[i].PageID
		if i == 0 || id < g.MetadataFirst {
			g.MetadataFirst = id
		}
		if id > g.MetadataLast {
			g.MetadataLast = id
		}
	}
	// The same single prepared member is appended by its original activation.
	// This is the existing scope formula, not a lower-cost publication plan.
	n := g.IDs + g.NewIDs
	coverage := uint64(0)
	if g.Coverage != 0 {
		coverage = reservationBoundMulV1(g.Burned, 2+g.Coverage)
	}
	g.PublicationRecords, g.PublicationBytes = a.cowPublicationBaseAdmissionV6(1, 1)
	g.PublicationRecords += 2 + n + coverage
	g.PublicationBytes += 2*uint64(unsafe.Sizeof(COWPublicationScopeV6{})) + uint64(unsafe.Sizeof(PreparedCOWCandidateV1{})) + 32 + 32*n + coverage*uint64(unsafe.Sizeof(reservationInterval{}))
	return g, true
}
