package freelist

import (
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// ErrCOWPreparationStaleV6 is a previsibility refusal. The private candidate
// still owns its original reservation until Cancel completes; it must never be
// installed over the newer live transaction.
var ErrCOWPreparationStaleV6 = errors.New("freelist: private COW preparation source changed")

// COWPreparationWorkV6 prepares a DATA candidate outside the allocator's sole
// prepared slot. Its containing producer owns and charges this control and the
// borrowed immutable retirement input. No live FreelistTxn pointer is retained
// between calls. The original allocator is touched again only for source
// validation, final adoption, and cancellation of the original ledger claim.
//
// This entry prepares no auxiliary pages. Root-record/capsule construction is a
// separate producer; it cannot silently request unbounded contiguous allocation
// through the ordinary constructor.
type COWPreparationWorkV6 struct {
	allocator                                                   *Allocator
	base                                                        *FreelistGenerationV1
	root                                                        *stateNode
	ledger                                                      *ReservationLedger
	highWater                                                   uint64
	sourceStats                                                 FreelistTxnStats
	sourceRequires                                              bool
	allocatedCount, abandonedCount, changedCount, replacedCount int
	private                                                     *FreelistTxn
	generationID, commitSeq                                     uint64
	candidateID                                                 CandidateIDV1
	capability                                                  ReuseCapability
	retirements                                                 []COWRetirementV1
	retirementIdentity                                          *COWRetirementV1
	sink                                                        *CandidatePageSinkV1
	detach                                                      stateDetachWorkV1
	retire                                                      privateRetirementWorkV6
	prune                                                       txnPruneWorkV1
	materialize                                                 CandidateMaterializationWorkV1
	result                                                      *FreelistCandidateV1
	prepared                                                    *PreparedCOWCandidateV1
	retiredCount                                                uint64
	index, retirementIndex, pageIndex                           int
	phase                                                       uint8
	adopted, cancelled                                          bool
	firstErr                                                    error
}

// BindRetainedOwnerV6 binds finite-only scratch to the same producer that owns
// this embedded control. Canonical DATA images retain the ordinary allocator
// contract; this owner covers newly introduced retained sorting storage.
// Native constructors must bind before Step. The allocator-only engine may be
// exercised independently without introducing a second allocation authority.
func (s *COWPreparationWorkV6) BindRetainedOwnerV6(owner *retainedalloc.Owner) error {
	if s == nil || owner == nil || s.sink != nil || s.cancelled || s.adopted {
		return ErrGenerationFormat
	}
	if s.materialize.inventory.normalized.metadata != nil && s.materialize.inventory.normalized.metadata != owner {
		return ErrGenerationFormat
	}
	s.materialize.inventory.normalized.metadata = owner
	s.materialize.reuse.inventory.normalized.metadata = owner
	return nil
}

// sourceLocked compares the original constructor operands, not just collection
// lengths. Dirty tree identity plus mutation counters prevents an ABA-shaped
// allocation/retirement from authorizing stale installation.
func (s *COWPreparationWorkV6) sourceLocked(a *Allocator) bool {
	if a.cow == nil || a.cow.txn == nil || a.cow.prepared != nil || a.cow.generation != s.base || a.cow.ledger != s.ledger {
		return false
	}
	t := a.cow.txn
	return !t.consumed && t.base == s.base && t.root == s.root && t.highWater == s.highWater &&
		t.stats == s.sourceStats && t.requiresGeneration == s.sourceRequires &&
		len(t.allocated) == s.allocatedCount && len(t.abandonedAppends) == s.abandonedCount &&
		len(t.changedChunks) == s.changedCount && len(t.replacedMetadata) == s.replacedCount
}

func (s *COWPreparationWorkV6) capture(a *Allocator, w *iterator.OrdinalScanWork) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.txn == nil || a.cow.generation == nil || a.cow.ledger == nil {
		return false, ErrGenerationFormat
	}
	if a.cow.waitErr != nil {
		return false, a.cow.waitErr
	}
	if a.cow.prepared != nil {
		return false, ErrCOWCandidatePrepared
	}
	t := a.cow.txn
	if err := t.valid(); err != nil {
		return false, err
	}
	if s.phase == 0 {
		bytes := uint64(unsafe.Sizeof(FreelistTxn{})) + uint64(len(t.allocated))*uint64(unsafe.Sizeof(allocatedPage{})) + uint64(len(t.abandonedAppends))*uint64(unsafe.Sizeof(ReservationExtentV1{}))
		if ok, e := appendReserve(w, 1, bytes); !ok || e != nil {
			return false, e
		}
		// Revocation makes every later ordinary edit persistent. The retained
		// root is immutable even if the live writer immediately replaces it.
		t.privatePreparation = false
		s.allocator, s.base, s.root, s.ledger, s.highWater = a, a.cow.generation, t.root, t.ledger, t.highWater
		s.sourceStats, s.sourceRequires = t.stats, t.requiresGeneration
		s.allocatedCount, s.abandonedCount = len(t.allocated), len(t.abandonedAppends)
		s.changedCount, s.replacedCount = len(t.changedChunks), len(t.replacedMetadata)
		clone := *t
		clone.allocated = make([]allocatedPage, len(t.allocated))
		clone.abandonedAppends = make([]ReservationExtentV1, len(t.abandonedAppends))
		clone.changedChunks, clone.replacedMetadata = nil, nil
		s.private = &clone
		s.phase = 1
	}
	if !s.sourceLocked(a) {
		return false, ErrCOWPreparationStaleV6
	}
	for s.phase < 5 {
		switch s.phase {
		case 1:
			n, e := stateEncodeEntriesV1(w, len(t.allocated)-s.index, 2*uint64(unsafe.Sizeof(allocatedPage{})))
			if e != nil || n == 0 && s.index < len(t.allocated) {
				return false, e
			}
			copy(s.private.allocated[s.index:s.index+n], t.allocated[s.index:s.index+n])
			s.index += n
			if s.index < len(t.allocated) {
				return false, nil
			}
			s.index, s.phase = 0, 2
		case 2:
			n, e := stateEncodeEntriesV1(w, len(t.abandonedAppends)-s.index, 2*uint64(unsafe.Sizeof(ReservationExtentV1{})))
			if e != nil || n == 0 && s.index < len(t.abandonedAppends) {
				return false, e
			}
			copy(s.private.abandonedAppends[s.index:s.index+n], t.abandonedAppends[s.index:s.index+n])
			s.index += n
			if s.index < len(t.abandonedAppends) {
				return false, nil
			}
			s.index, s.phase = 0, 3
		case 3:
			// Go maps cannot expose stable incremental iterators while another
			// writer may mutate them. Admit this actual complete locked copy.
			if ok, e := appendReserve(w, 1+uint64(len(t.changedChunks)), 256+128*uint64(len(t.changedChunks))); !ok || e != nil {
				return false, e
			}
			s.private.changedChunks = make(map[uint64]struct{}, len(t.changedChunks))
			for key := range t.changedChunks {
				s.private.changedChunks[key] = struct{}{}
			}
			s.phase = 4
		case 4:
			if ok, e := appendReserve(w, 1+uint64(len(t.replacedMetadata)), 256+128*uint64(len(t.replacedMetadata))); !ok || e != nil {
				return false, e
			}
			s.private.replacedMetadata = make(map[uint64]struct{}, len(t.replacedMetadata))
			for key := range t.replacedMetadata {
				s.private.replacedMetadata[key] = struct{}{}
			}
			s.detach.begin(s.root)
			s.phase = 5
		}
	}
	return true, nil
}

// Step produces immutable canonical candidate images and the original ledger
// reservation, but deliberately does not install a.cow.prepared. Callers may
// run real ordinary writers between every false,nil return. A changed source
// refuses at Adopt and keeps cleanup custody; it cannot become a stale root.
func (s *COWPreparationWorkV6) Step(a *Allocator, generationID, commitSeq uint64, candidate CandidateIDV1, capability ReuseCapability, retirements []COWRetirementV1, sink *CandidatePageSinkV1, w *iterator.OrdinalScanWork) (*FreelistCandidateV1, bool, error) {
	if s == nil || a == nil || sink == nil || w == nil || generationID == 0 || commitSeq == 0 || candidate == (CandidateIDV1{}) || s.cancelled || s.adopted {
		return nil, false, ErrGenerationFormat
	}
	if s.firstErr != nil {
		return nil, false, s.firstErr
	}
	var identity *COWRetirementV1
	if len(retirements) != 0 {
		identity = &retirements[0]
	}
	if s.sink == nil {
		if ok, e := appendReserve(w, 1, 96); !ok || e != nil {
			return nil, false, e
		}
		s.generationID, s.commitSeq, s.candidateID, s.capability = generationID, commitSeq, candidate, capability
		s.retirements, s.retirementIdentity, s.sink = retirements, identity, sink
	} else if s.generationID != generationID || s.commitSeq != commitSeq || s.candidateID != candidate || s.capability != capability || s.sink != sink || s.retirementIdentity != identity || len(s.retirements) != len(retirements) || s.allocator != nil && s.allocator != a {
		return nil, false, ErrGenerationFormat
	}
	ready, err := s.advance(a, w)
	if err != nil {
		s.firstErr = err
	}
	return s.result, ready, err
}

func (s *COWPreparationWorkV6) advance(a *Allocator, w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase < 5 {
		ready, err := s.capture(a, w)
		if !ready || err != nil {
			return false, err
		}
	}
	for {
		switch s.phase {
		case 5:
			ready, err := s.detach.step(w)
			if !ready || err != nil {
				return false, err
			}
			s.private.root, s.private.privatePreparation = s.detach.result, true
			s.phase = 6
		case 6:
			for s.retirementIndex < len(s.retirements) {
				r := &s.retirements[s.retirementIndex]
				if s.pageIndex == len(r.PageIDs) {
					if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*r))); !ok || e != nil {
						return false, e
					}
					s.retirementIndex++
					s.pageIndex = 0
					continue
				}
				ready, e := s.retire.step(s.private, r.PageIDs[s.pageIndex], r.LastReachableCommitSeq, w)
				if !ready || e != nil {
					return false, e
				}
				s.retiredCount++
				s.pageIndex++
			}
			s.phase = 7
		case 7:
			ready, e := s.prune.step(s.private, s.capability, w)
			if !ready || e != nil {
				return false, e
			}
			s.phase = 8
		case 8:
			// Original live DATA allocation remains available to a newer ordinary
			// candidate while private metadata has its own original ledger claim.
			// Exact-source adoption claims DATA atomically before visibility.
			s.materialize.deferDataClaimV6 = true
			result, ready, e := s.materialize.Step(s.private, s.generationID, s.commitSeq, s.candidateID, s.sink, w)
			if !ready || e != nil {
				return false, e
			}
			s.result, s.phase = result, 9
		case 9:
			if s.prepared == nil {
				if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(PreparedCOWCandidateV1{}))); !ok || e != nil {
					return false, e
				}
				s.prepared = &PreparedCOWCandidateV1{candidate: s.result, candidateID: s.candidateID, privatePrepared: true}
			}
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}

// PreparedForConstructionV6 returns the SAME canonical descriptor that Adopt
// will install. It permits immutable membership/certificate construction only;
// allocator activation still requires this exact object in the original slot.
func (s *COWPreparationWorkV6) PreparedForConstructionV6() *PreparedCOWCandidateV1 {
	if s == nil || s.phase != 9 || s.cancelled || s.firstErr != nil {
		return nil
	}
	return s.prepared
}

// ValidateSourceWithWorkV6 checks the original live operands without taking
// the prepared slot or consuming any ledger claim. Adoption repeats this exact
// check under its mutation lock; a successful observation grants no authority.
func (s *COWPreparationWorkV6) ValidateSourceWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || s.allocator == nil || s.cancelled || s.adopted || w == nil {
		return false, ErrGenerationFormat
	}
	a := s.allocator
	a.mu.Lock()
	defer a.mu.Unlock()
	if ok, err := appendReserve(w, 1, 256); !ok || err != nil {
		return false, err
	}
	if !s.sourceLocked(a) {
		return false, ErrCOWPreparationStaleV6
	}
	return true, nil
}

// Adopt is the final allocator-cut operation. After true, the caller must
// activate or abort this SAME prepared candidate in this public call; it may
// not yield with the prepared slot installed. PrepareCOWActivationWithWorkV1
// must already have completed against Result before this method is invoked.
func (s *COWPreparationWorkV6) Adopt(w *iterator.OrdinalScanWork) (*PreparedCOWCandidateV1, bool, error) {
	if s == nil || s.phase != 9 || s.result == nil || s.prepared == nil || s.adopted || s.cancelled || s.firstErr != nil || w == nil {
		return nil, false, ErrGenerationFormat
	}
	a := s.allocator
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow.waitErr != nil {
		return nil, false, a.cow.waitErr
	}
	if !s.sourceLocked(a) {
		return nil, false, ErrCOWPreparationStaleV6
	}
	s.ledger.mu.Lock()
	defer s.ledger.mu.Unlock()
	if ready, err := s.claimDataLockedV6(1, 6*8, w); !ready || err != nil {
		return nil, false, err
	}
	return s.adoptLockedV6(a), true, nil
}

// adoptLockedV6 transfers the original private candidate into the sole slot.
// Source validation and all admission must precede this nonfallible operation.
func (s *COWPreparationWorkV6) adoptLockedV6(a *Allocator) *PreparedCOWCandidateV1 {
	prepared := s.prepared
	prepared.rollbackTxn, prepared.rollbackStats, prepared.privatePrepared = a.cow.txn, a.stats, false
	a.cow.txn = s.private
	a.cow.prepared = prepared
	a.stats.FreePages += s.retiredCount
	s.adopted = true
	return prepared
}

// AdoptAndActivateV6 performs the final private adoption and the original
// activation in one admitted allocator/ledger cut. A refusal leaves the live
// transaction, original prepared slot, and ledger state unchanged. No lock or
// installed prepared slot survives a return, and no new publication authority
// is introduced. The successor must have been prepared outside this cut.
func (s *COWPreparationWorkV6) AdoptAndActivateV6(activation *COWActivationWorkV1, w *iterator.OrdinalScanWork) (*PreparedCOWCandidateV1, bool, error) {
	if s == nil || s.phase != 9 || s.result == nil || s.prepared == nil || s.adopted || s.cancelled || s.firstErr != nil || w == nil {
		return nil, false, ErrGenerationFormat
	}
	a := s.allocator
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cow == nil || a.cow.waitErr != nil {
		if a.cow != nil {
			return nil, false, a.cow.waitErr
		}
		return nil, false, ErrGenerationFormat
	}
	if !s.sourceLocked(a) {
		return nil, false, ErrCOWPreparationStaleV6
	}
	if activation == nil || !activation.ready || activation.allocator != a || activation.candidate != s.result || activation.ledger != a.cow.ledger || activation.successor == nil ||
		s.prepared.candidate != s.result || !s.prepared.privatePrepared || s.prepared.activated || s.prepared.published {
		return nil, false, ErrGenerationFormat
	}
	ledger := a.cow.ledger
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	// The locked original transition checks the exact claim before effects;
	// cancellation/rollback cannot race its state between validation/install.
	claim := ledger.candidates[s.candidateID]
	if claim == nil || claim.state != CandidatePreVisible {
		return nil, false, ErrCandidateConsumed
	}
	records, bytes := a.activationAdmissionV6()
	if ready, err := s.claimDataLockedV6(records+1, bytes+6*8, w); !ready || err != nil {
		return nil, false, err
	}
	if err := ledger.transitionLockedV6(s.candidateID, CandidateVisible); err != nil {
		return nil, false, err
	}
	prepared := s.adoptLockedV6(a)
	a.installActivatedCandidateV6(prepared, activation)
	return prepared, true, nil
}

// claimDataLockedV6 finishes the SAME metadata reservation at the original
// adoption cut. Caller holds allocator then ledger locks and has already
// validated the exact live transaction. All DATA checks/storage are admitted
// before any claim effect, alongside the caller's entire remaining activation.
// Previsible preparation never monopolizes DATA allocated on the shared live
// transaction, so ordinary writers can supersede it without assisting its work.
func (s *COWPreparationWorkV6) claimDataLockedV6(extraRecords, extraBytes uint64, w *iterator.OrdinalScanWork) (bool, error) {
	l := s.ledger
	r := l.candidates[s.candidateID]
	if r == nil || r.state != CandidatePreVisible {
		return false, ErrCandidateConsumed
	}
	records, bytes := s.claimDataAdmissionLockedV6(extraRecords, extraBytes)
	if ok, err := appendReserve(w, records, bytes); !ok || err != nil {
		return false, err
	}
	for _, allocation := range s.private.allocated {
		if l.reservedByOtherLocked(s.candidateID, allocation.id) {
			return false, ErrPageReserved
		}
	}
	for _, allocation := range s.private.allocated {
		if _, exists := l.owners[allocation.id]; !exists {
			l.owners[allocation.id] = s.candidateID
			r.ids = append(r.ids, allocation.id)
		}
	}
	return true, nil
}

// Original read-only pricing shared by the real claim and its pre-effect probe.
// The caller holds the original allocator and ledger locks.
func (s *COWPreparationWorkV6) claimDataAdmissionLockedV6(extraRecords, extraBytes uint64) (uint64, uint64) {
	l, n := s.ledger, uint64(len(s.private.allocated))
	records, bytes := l.claimStorageBoundLockedV1(s.candidateID, n, 0)
	records = reservationBoundAddV1(records, reservationBoundMulV1(n, 1+uint64(len(l.burnedTails))+uint64(len(l.candidates))))
	return reservationBoundAddV1(records, extraRecords), reservationBoundAddV1(bytes, extraBytes)
}

// Cancel consumes only this constructor's original previsible ledger claim.
// Its whole actual collection bound is admitted under the same ledger lock;
// written tails remain burned exactly as in ordinary rollback.
func (s *COWPreparationWorkV6) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || s.cancelled {
		return true, nil
	}
	if s.adopted {
		return false, ErrCandidateConsumed
	}
	for _, normalize := range [2]*reservationNormalizeWorkV1{&s.materialize.inventory.normalized, &s.materialize.reuse.inventory.normalized} {
		ready, err := normalize.releaseScratch(w)
		if !ready || err != nil {
			return false, err
		}
	}
	if s.ledger != nil {
		ready, err := s.ledger.rollbackPreVisibleWithWorkV6(s.candidateID, w)
		if !ready || err != nil {
			return false, err
		}
	}
	if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || e != nil {
		return false, e
	}
	if s.prepared != nil {
		s.prepared.privatePrepared = false
	}
	*s = COWPreparationWorkV6{cancelled: true}
	return true, nil
}

// privateRetirementWorkV6 edits only a constructor-owned transaction. Both
// ordinary and finite paths use the same Patricia path/summary primitives.
type privateRetirementWorkV6 struct {
	marker   candidateBeginWorkV1
	mutation stateMutationWorkV1
	extent   [1]ReservationExtentV1
	phase    uint8
}

func (s *privateRetirementWorkV6) step(t *FreelistTxn, id, seq uint64, w *iterator.OrdinalScanWork) (bool, error) {
	if id < 2 || id >= t.highWater || seq == 0 {
		return false, ErrGenerationFormat
	}
	key := id >> freelistChunkShift
	if s.phase == 0 {
		if ok, e := appendReserve(w, 1, 64); !ok || e != nil {
			return false, e
		}
		s.marker.txn, s.marker.markNode, s.marker.chunkNo = t, t.root, key
		s.extent[0] = ReservationExtentV1{StartPageID: id, Count: 1, Kind: ReservationPendingMetadataRetirement, LastReachableCommitSeq: seq}
		s.phase = 1
	}
	if s.phase == 1 {
		ready, e := s.marker.markPath(w)
		if !ready || e != nil {
			return false, e
		}
		s.mutation.begin(t.root, key, t.privatePreparation, &t.stats, nil, nil)
		s.mutation.pendingEnabled = true
		s.mutation.pending = pendingRetirementCursorV1{extents: s.extent[:], highWater: t.highWater, chunkNo: key}
		s.phase = 2
	}
	if s.phase == 2 {
		ready, e := s.mutation.step(w)
		if !ready || e != nil {
			return false, e
		}
		t.root, t.requiresGeneration = s.mutation.result, true
		s.phase = 3
	}
	if s.phase == 3 {
		ready, e := s.marker.mapInsert.step(&t.changedChunks, key, w)
		if !ready || e != nil {
			return false, e
		}
		t.stats.StateMutationPaths++
		t.stats.StateMutationItems++
		if t.privatePreparation {
			t.stats.PrivateMutationPaths++
		}
		s.phase = 4
	}
	if ok, e := appendReserve(w, 1, uint64(unsafe.Sizeof(*s))); !ok || e != nil {
		return false, e
	}
	*s = privateRetirementWorkV6{}
	return true, nil
}
