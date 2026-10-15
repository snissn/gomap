package db

import (
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// nativeMVCCAllocatorPreparationV6 is containing storage on the original
// selected-delete producer. Page observation never takes its AppendWork rollback
// edges; those remain on output until the SAME visible install accepts them.
type nativeMVCCAllocatorPreparationV6 struct {
	preparation           freelist.COWPreparationWorkV6
	activation            freelist.COWActivationWorkV1
	sink                  freelist.CandidatePageSinkV1
	countVisit, fillVisit zipper.MaterializedPageVisitWorkV6
	retired               []uint64
	retirements           [1]freelist.COWRetirementV1
	owner                 *retainedalloc.Owner
	charge                uint64
	count, index          int
	generationID          uint64
	candidateID           freelist.CandidateIDV1
	capability            freelist.ReuseCapability
	minimum               nativeMVCCSnapshotMinimumV6
	primary               *rootpublication.PreparedPrimaryRootV5
	phase                 uint8
}

// nativeMVCCSnapshotMinimumV6 admits one original registry at a time. A changed
// acquisition epoch restarts the observation, exactly as ordinary minimum does.
// Removed generations cannot lower the minimum without a new acquisition.
// No index-list slice, physical generation or registry pointer escapes a call.
type nativeMVCCSnapshotMinimumV6 struct {
	epoch, minimum uint64
	index          uint64
	begun          bool
	currentSeen    bool
}

func (s *nativeMVCCSnapshotMinimumV6) step(db *DB, w *iterator.OrdinalScanWork) (uint64, bool, error) {
	if !w.Reserve(1, uint64(unsafe.Sizeof(*s))+32) {
		return 0, false, nil
	}
	if db.snapshotAcquireInFlight() != 0 {
		return 0, false, nil
	}
	epoch := db.snapshotAcquireEpoch.Load()
	if !s.begun || s.epoch != epoch {
		*s = nativeMVCCSnapshotMinimumV6{epoch: epoch, minimum: math.MaxUint64, begun: true}
	}
	db.idxMu.Lock()
	defer db.idxMu.Unlock()
	// Admit the actual current map walk before observing it. Select the next
	// stable generation ID without retaining a mutable map iterator or slice.
	if !w.Reserve(1+uint64(len(db.idxAll)), 32*uint64(len(db.idxAll))) {
		if 1+uint64(len(db.idxAll)) > w.RecordLimit {
			return 0, false, &iterator.OrdinalUnitTooLarge{Records: 1 + uint64(len(db.idxAll)), Bytes: 32 * uint64(len(db.idxAll))}
		}
		return 0, false, nil
	}
	var gen *indexGen
	nextID := uint64(math.MaxUint64)
	found := false
	for id, observed := range db.idxAll {
		if id > s.index && (!found || id < nextID) {
			nextID, gen, found = id, observed, true
		}
	}
	if !found {
		if !s.currentSeen {
			gen = db.idx.Load()
		} else {
			if db.snapshotAcquireInFlight() != 0 || db.snapshotAcquireEpoch.Load() != s.epoch {
				s.begun = false
				return 0, false, nil
			}
			return s.minimum, true, nil
		}
	}
	if gen != nil && gen.registry != nil {
		seq, ready, err := gen.registry.MinPinnedSeqWithWorkV6(w)
		if !ready || err != nil {
			return 0, false, err
		}
		if seq < s.minimum {
			s.minimum = seq
		}
	}
	if found {
		s.index = nextID
	} else {
		s.currentSeen = true
	}
	return 0, false, nil
}

// StepNativeMVCCAllocatorV6 creates only the original allocator's immutable
// private descriptor. It exposes no prepared slot and leaves all physical
// installed-page rollback custody with this SAME producer across every yield.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCAllocatorV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.phase != 4 || c.rootPreparation.phase != 7 || c.transaction == nil || c.output == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	s := &c.allocatorPreparation
	root := &c.rootPreparation
	a := c.snapshot.idx.allocator
	for {
		switch s.phase {
		case 0:
			ready, err := c.output.VisitPagesWithWorkV6(&s.countVisit, func(_, old uint64, _ *iterator.OrdinalScanWork) (bool, error) {
				if old != 0 {
					s.count++
				}
				return true, nil
			}, w)
			if !ready || err != nil {
				return false, err
			}
			s.count += c.outputInput.RetiredCount
			s.countVisit = zipper.MaterializedPageVisitWorkV6{}
			s.phase = 1
		case 1:
			if !w.Reserve(1, 2*uint64(s.count)*8+2*uint64(unsafe.Sizeof(*s))) {
				return false, nil
			}
			owner := c.snapshot.idx.primary.MetadataOwner()
			charge := retainedalloc.AllocationCharge(uint64(s.count) * 8)
			if s.count != 0 {
				if err := owner.AddPending(charge); err != nil {
					return false, err
				}
				s.retired = make([]uint64, s.count)
				s.owner, s.charge = owner, charge
			}
			if err := s.preparation.BindRetainedOwnerV6(owner); err != nil {
				return false, err
			}
			s.phase = 2
		case 2:
			ready, err := c.output.VisitPagesWithWorkV6(&s.fillVisit, func(_, old uint64, w *iterator.OrdinalScanWork) (bool, error) {
				if old != 0 {
					if !w.Reserve(1, 16) {
						return false, nil
					}
					s.retired[s.index] = old
					s.index++
				}
				return true, nil
			}, w)
			if !ready || err != nil {
				return false, err
			}
			s.fillVisit = zipper.MaterializedPageVisitWorkV6{}
			s.phase = 3
		case 3:
			for s.index < s.count {
				if !w.Reserve(1, 16) {
					return false, nil
				}
				inputIndex := c.outputInput.RetiredCount - (s.count - s.index)
				s.retired[s.index] = c.outputInput.Retired[inputIndex]
				s.index++
			}
			s.retirements[0] = freelist.COWRetirementV1{PageIDs: s.retired, LastReachableCommitSeq: root.expectedSequence}
			s.phase = 4
		case 4:
			minimum, ready, err := s.minimum.step(c.db, w)
			if !ready || err != nil {
				return false, err
			}
			if !w.Reserve(1, 256) {
				return false, nil
			}
			// Only original fixed two-slot state is copied under its existing exclusion.
			c.db.durablePublishMu.Lock()
			c.db.rootReuseMu.Lock()
			oldest, err := oldestRecoverableSlotCommitV1(c.db.durableRoot.slotCommit)
			if err == nil && c.db.durableRoot.primary != nil {
				for _, seq := range c.db.durableRoot.primary.dataFloor {
					if seq != 0 && seq < oldest {
						oldest = seq
					}
				}
			}
			c.db.rootReuseMu.Unlock()
			c.db.durablePublishMu.Unlock()
			if err != nil {
				return false, err
			}
			s.capability, err = freelist.NewReuseCapability(oldest, minimum, 0)
			if err != nil {
				return false, err
			}
			live := a.COWGenerationV1()
			if live == nil || live.GenerationID() == math.MaxUint64 {
				return false, ErrNativeMVCCSnapshotWork
			}
			s.generationID = live.GenerationID() + 1
			s.candidateID = rootPublicationLogicalCandidateIDV1(c.db.rootPublication.lineage, s.generationID, root.next)
			s.phase = 5
		case 5:
			candidate, ready, err := s.preparation.Step(a, s.generationID, root.next.CommitSeq, s.candidateID, s.capability, s.retirements[:], &s.sink, w)
			if !ready || err != nil {
				return false, err
			}
			root.next.TotalPages = candidate.Generation().HighWater()
			root.next.FreelistHeadID = 0
			s.phase = 6
		case 6:
			prepared := s.preparation.PreparedForConstructionV6()
			if prepared == nil {
				return false, rootpublication.ErrDurableRootOwnership
			}
			ready, err := a.PrepareCOWActivationWithWorkV1(prepared.Candidate(), &s.activation, w)
			if !ready || err != nil {
				return false, err
			}
			s.phase = 7
		case 7:
			runtime := c.db.rootPublication
			if runtime == nil || runtime.idx != c.snapshot.idx {
				return false, rootpublication.ErrDurableRootOwnership
			}
			// Do not publish a cache borrowed from a fallible private preparation. The
			// SAME successful visible activation owns any future runtime cache update.
			runtime.mu.Lock()
			previous := runtime.primaryDataCertificateV6
			runtime.mu.Unlock()
			prepared := s.preparation.PreparedForConstructionV6()
			primary, ready, err := rootpublication.NewPreparedPrimaryReadRootCertificateV6(c.transaction.PrimaryConstructionCertificateV6(), prepared.Candidate().Generation(), prepared, rootPublicationFrontierV1(root.next), c.snapshot.idx.pager, previous, w)
			if !ready || err != nil {
				return false, err
			}
			s.primary = primary
			s.phase = 8
		case 8:
			return s.preparation.ValidateSourceWithWorkV6(w)
		default:
			return false, ErrNativeMVCCSnapshotWork
		}
	}
}

func (s *nativeMVCCAllocatorPreparationV6) closeStep(w *iterator.OrdinalScanWork) (bool, error) {
	ready, err := s.preparation.Cancel(w)
	if !ready || err != nil {
		return false, err
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))+uint64(len(s.retired))*8) {
		return false, nil
	}
	owner, charge := s.owner, s.charge
	clear(s.retired)
	*s = nativeMVCCAllocatorPreparationV6{}
	if owner != nil {
		owner.RemovePending(charge)
	}
	return true, nil
}
