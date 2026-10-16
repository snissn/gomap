package db

import (
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This is containing storage on the original selected-delete producer. Each
// separately allocated member/install/callback is the existing publication
// descriptor, admitted before allocation and transferred into the SAME txn.
type nativeMVCCCandidatePreparationV6 struct {
	member        *rootPublicationVisibleMemberV1
	resources     *rootpublication.StableResourceSet
	candidate     *rootpublication.PreparedRootCandidate
	spec          rootpublication.DurableRootTransactionSpec
	metadata      *retainedalloc.Owner
	charge        uint64
	bound         bool
	phase         uint8
	capacityPhase uint8
	backingRefund primaryBackingRefundV6
	refund        retainedalloc.RemovalWork
	completion    rootpublication.StableResourceTokenReleaseCompletionV6
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeCandidateScalarsForTest() (phase uint8, bound, candidate bool) {
	if c == nil {
		return
	}
	s := &c.candidatePreparation
	return s.phase, s.bound, s.candidate != nil
}

func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCCandidateV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.phase != 4 || c.allocatorPreparation.phase != 8 || c.transaction == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	s, db, root, allocator := &c.candidatePreparation, c.db, &c.rootPreparation, &c.allocatorPreparation
	if ready, err := s.backingRefund.complete(w); !ready || err != nil {
		return false, err
	}
	runtime := db.rootPublication
	if runtime == nil || runtime.idx != c.snapshot.idx {
		return false, ErrNativeMVCCSnapshotWork
	}
	for {
		switch s.phase {
		case 0:
			ready, err := c.request.Validate(w)
			if !ready || err != nil {
				return false, err
			}
			if !w.Reserve(3, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(rootPublicationVisibleMemberV1{}))+2*uint64(unsafe.Sizeof(rootPublicationVisibleInstallV1{}))) {
				return false, nil
			}
			if !db.writeMu.TryLock() {
				return false, nil
			}
			if !db.durablePublishMu.TryLock() {
				db.writeMu.Unlock()
				return false, nil
			}
			if !db.rootReuseMu.TryRLock() {
				db.durablePublishMu.Unlock()
				db.writeMu.Unlock()
				return false, nil
			}
			db.mu.RLock()
			err = func() error {
				if db.closing.Load() || db.idx.Load() != runtime.idx || db.meta.CommitSeq != root.expectedSequence {
					return errDurableRootCandidateStale
				}
				// Native outer-leaf pending manifests require their original retained
				// constructor before accepting this member. No silent ordinary scan.
				if db.leafPageLog != nil {
					return fmt.Errorf("%w: outer-leaf pending capture", rootpublication.ErrNativeMVCCRootWorkRequiredV6)
				}
				db.conditionalOracle.mu.Lock()
				active := len(db.conditionalOracle.active) != 0
				db.conditionalOracle.mu.Unlock()
				if active {
					return fmt.Errorf("%w: active conditional transaction", rootpublication.ErrNativeMVCCRootWorkRequiredV6)
				}
				runtime.mu.Lock()
				defer runtime.mu.Unlock()
				if runtime.poison != nil {
					return runtime.poison
				}
				resources, ready, err := rootpublication.PrepareEmptyStableResourceSetWithOrdinalWorkV6(runtime.idx.primary.MetadataOwner(), runtime.visibleResources, w)
				if !ready || err != nil {
					return err
				}
				s.resources = resources
				s.phase = 1
				return nil
			}()
			db.mu.RUnlock()
			db.rootReuseMu.RUnlock()
			db.durablePublishMu.Unlock()
			db.writeMu.Unlock()
			if err != nil || s.phase == 0 {
				return false, err
			}
		case 1:
			charge := primaryVisibleMetadataV6() + 5*retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryVisibilityCallbackEnvironmentV6{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootPublicationVisibleInstallV1{})))
			if !w.Reserve(3, 2*uint64(unsafe.Sizeof(rootPublicationVisibleMemberV1{}))+2*uint64(unsafe.Sizeof(rootPublicationVisibleInstallV1{}))+10*uint64(unsafe.Sizeof(primaryVisibilityCallbackEnvironmentV6{}))) {
				return false, nil
			}
			owner := runtime.idx.primary.MetadataOwner()
			if ready, err := retainedalloc.AddPendingWithOrdinalWork(owner, charge, w); !ready || err != nil {
				return false, err
			}
			s.metadata, s.charge = owner, charge
			install := &rootPublicationVisibleInstallV1{metadata: owner, db: db, idx: runtime.idx, next: root.next, oldUserRootID: c.snapshot.state.RootPageID, newUserRootID: root.next.UserRootPageID, conditionalMutation: conditionalCommitMutation{physicalOnly: true}}
			install.post.commitSeq, install.post.vlogRefIndexID, install.post.sync = root.next.CommitSeq, runtime.idx.id, true
			s.member = &rootPublicationVisibleMemberV1{sequence: root.next.CommitSeq, next: root.next, prepared: allocator.preparation.PreparedForConstructionV6(), primary: allocator.primary, install: install, allocatorPreparation: &allocator.preparation, allocatorActivation: &allocator.activation, retirementReleaseSet: releasePrimaryResourceSetWithWorkV6}
			s.phase = 2
		case 2:
			resources, ready, err := rootpublication.CloneAdmittedStableResourceSetWithOrdinalWorkV6(s.resources, w)
			if !ready || err != nil {
				return false, err
			}
			s.member.resources = resources
			s.phase = 3
		case 3:
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*s.member.install))+128) {
				return false, nil
			}
			db.rootReuseMu.RLock()
			db.mu.RLock()
			if db.closing.Load() || db.idx.Load() != runtime.idx || db.meta.CommitSeq != root.expectedSequence {
				db.mu.RUnlock()
				db.rootReuseMu.RUnlock()
				return false, errDurableRootCandidateStale
			}
			install, state := s.member.install, db.state.Load()
			if state == nil {
				db.mu.RUnlock()
				db.rootReuseMu.RUnlock()
				return false, ErrClosed
			}
			sizes, known := state.ValueLogSet.RetentionSizes()
			if !known || sizes.FileCount > 17 || state.ValueLogSet != nil && db.valueLogManager == nil {
				db.mu.RUnlock()
				db.rootReuseMu.RUnlock()
				return false, ErrNativeMVCCSnapshotWork
			}
			install.valueLogSet = state.ValueLogSet
			if state.ValueLogSet != nil {
				db.valueLogManager.Acquire(state.ValueLogSet)
			}
			install.leafGenerationView = state.LeafGenerations
			db.mu.RUnlock()
			db.rootReuseMu.RUnlock()
			s.phase = 4
		case 4:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primaryStateRootV5{}))) {
				return false, nil
			}
			cert := allocator.primary.ReadCertificateV6()
			if err := runtime.idx.primary.AdmitRootMetadataWithWorkV6(cert.Reference(), retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{}))), w); err != nil {
				if err == retainedalloc.ErrWorkBudget {
					return false, nil
				}
				return false, err
			}
			s.member.install.primaryRoot = &primaryStateRootV5{arena: runtime.idx.primary, ref: cert.Reference(), dataCommit: s.member.prepared.Candidate().Generation().CommitSeq(), certificate: cert}
			s.phase = 5
		case 5:
			if ready, err := s.backingRefund.complete(w); !ready || err != nil {
				return false, err
			}
			runtime.mu.Lock()
			ready, err := runtime.reserveNativeVisibleCapacityV6(&s.capacityPhase, &s.backingRefund, w)
			runtime.mu.Unlock()
			if !ready || err != nil {
				return false, err
			}
			if ready, err := s.backingRefund.complete(w); !ready || err != nil {
				return false, err
			}

			// All environments are allocated once in the original admitted spec.
			s.spec = runtime.visibleTransactionSpecV6(s.member, s.charge, true)
			s.phase = 6
		case 6:
			ready, err := c.transaction.BindPrimaryVisibilityV6(s.spec, s.resources, w)
			if !ready || err != nil {
				return false, err
			}
			s.member.transaction = c.transaction
			s.spec = rootpublication.DurableRootTransactionSpec{}
			s.metadata, s.charge, s.bound = nil, 0, true
			s.phase = 7
		case 7:
			candidate, ready, err := rootpublication.NewPreparedPrimaryEmptyCandidateWithWorkV6(rootpublication.CandidateSpec{Frontier: rootPublicationFrontierV1(root.next), TotalPages: root.next.TotalPages, IndexBytes: uint64(s.member.prepared.Candidate().PageCount()) * 4096, ResourceSet: s.resources, DurableRoot: c.transaction}, w)
			if !ready || err != nil {
				return false, err
			}
			s.candidate = candidate
			s.phase = 8
		case 8:
			return true, nil
		default:
			return false, ErrNativeMVCCSnapshotWork
		}
	}
}

func (s *nativeMVCCCandidatePreparationV6) closeStep(w *iterator.OrdinalScanWork, refundOwner **retainedalloc.Owner, refundCharge *uint64) (bool, error) {
	if ready, err := s.backingRefund.complete(w); !ready || err != nil {
		return false, err
	}
	if ready, err := s.completion.CompleteWithWorkV6(&s.refund, w); !ready || err != nil {
		return false, err
	}
	if s.phase == 0 {
		return true, nil
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	if s.bound {
		// The exact original transaction and external release receipt own all
		// remaining operands. Clear every producer-side candidate/Set alias
		// before that receipt can physically refund their containing storage.
		*s = nativeMVCCCandidatePreparationV6{}
		return true, nil
	}
	if m := s.member; m != nil {
		m.primary = nil
		m.install.primaryRoot = nil
		if m.resources != nil {
			completion, ready, err := releasePrimaryResourceSetWithWorkV6(m.resources, w)
			if !ready || err != nil {
				return false, err
			}
			m.resources = nil
			s.completion = completion
			if ready, err := s.completion.CompleteWithWorkV6(&s.refund, w); !ready || err != nil {
				return false, err
			}
		}
		if m.install.valueLogSet != nil {
			ready, err := m.install.db.valueLogManager.ReleaseNativeMVCCSetStep(m.install.valueLogSet, w)
			if !ready || err != nil {
				return false, err
			}
			m.install.valueLogSet = nil
		}
	}
	if s.resources != nil {
		completion, ready, err := releasePrimaryResourceSetWithWorkV6(s.resources, w)
		if !ready || err != nil {
			return false, err
		}
		s.resources = nil
		s.completion = completion
		if ready, err := s.completion.CompleteWithWorkV6(&s.refund, w); !ready || err != nil {
			return false, err
		}
	}
	owner, charge := s.metadata, s.charge
	*s = nativeMVCCCandidatePreparationV6{}
	*refundOwner, *refundCharge = owner, charge
	return true, nil
}
