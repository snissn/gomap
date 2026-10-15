package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// These test observations are fixed by-value scalars on the existing phase26
// producer. Every call has its own ordinary Q32 grant; none grants publication
// authority or retains an observation cursor/physical operand.
type NativeBasisGeometryForTestV6 struct {
	CandidatePhase, SealPhase                                                                                      uint8
	SameIndex, SameExpectedSequence, SameParent, SameRecord, HeadersReady                                          bool
	LeafGenerations, ConditionalActive                                                                             bool
	Prefix, Debt, DebtCapacity, Seals, SealCapacity, Visible, VisibleCapacity                                      uint64
	RuntimeScopeRecords, RuntimeScopeBytes, ActivationDispatchBytes, ActivationMemberBytes, ActivationInstallBytes uint64
	PublisherRecords, PublisherBytes, ConsumeRecords, ConsumeBytes, TurnReleaseBytes, CapsuleRecords, CapsuleBytes uint64
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeBasisGeometryForTestV6(w *iterator.OrdinalScanWork) (g NativeBasisGeometryForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(6, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}))+4*uint64(unsafe.Sizeof(durableRootRuntimeV1{}))) {
		return g, false
	}
	db, s := c.db, &c.sealPreparation
	if db == nil || s.seal == nil || c.candidatePreparation.member == nil {
		return g, false
	}
	runtime, member := db.rootPublication, c.candidatePreparation.member
	if runtime == nil {
		return g, false
	}
	db.rootReuseMu.RLock()
	defer db.rootReuseMu.RUnlock()
	db.mu.RLock()
	defer db.mu.RUnlock()
	db.conditionalOracle.mu.Lock()
	g.ConditionalActive = len(db.conditionalOracle.active) != 0
	db.conditionalOracle.mu.Unlock()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	g.CandidatePhase, g.SealPhase = c.candidatePreparation.phase, s.phase
	g.SameIndex, g.SameExpectedSequence = db.idx.Load() == runtime.idx && c.snapshot.idx == runtime.idx, db.meta.CommitSeq == c.rootPreparation.expectedSequence
	base := db.durableRoot
	g.SameRecord = base.slot == s.parentSlot && base.record == s.parentRecord && base.meta == s.parentMeta
	g.SameParent = base.primary != nil && base.primary.records[base.slot] == s.parentRoot && s.seal.primary.promotion != nil && s.seal.primary.promotion.Parent == s.parentRoot
	g.HeadersReady = member.install.activationState != nil && member.install.activationSnapshot != nil
	g.LeafGenerations = member.install.leafGenerationView != nil
	g.Prefix, g.Debt, g.DebtCapacity = uint64(len(s.seal.prefix)), uint64(len(runtime.debt)), uint64(cap(runtime.debt))
	g.Seals, g.SealCapacity = uint64(len(runtime.seals)), uint64(cap(runtime.seals))
	g.Visible, g.VisibleCapacity = uint64(len(runtime.visibleMembers)-runtime.visibleHead), uint64(cap(runtime.visibleMembers))
	// One SAME member/allocator debt/seal is installed in the proposed turn.
	g.RuntimeScopeRecords, g.RuntimeScopeBytes = nativeAuthorityAdmissionV6(g.Prefix, g.Debt+1, g.Seals+1, g.Visible+1)
	input := rootpublication.DurableRootCallbackInput{}
	g.ActivationDispatchBytes = 2*uint64(unsafe.Sizeof(input)) + 2*uint64(unsafe.Sizeof(uint8(0))) + 2*uint64(unsafe.Sizeof(rootpublication.ResourceOwnerState(0)))
	g.ActivationMemberBytes = 2*uint64(unsafe.Sizeof(input)) + 12*uint64(unsafe.Sizeof(member))
	g.ActivationInstallBytes = 2*uint64(unsafe.Sizeof(page.MetaPageBody{})) + uint64(unsafe.Sizeof(DBState{})) + uint64(unsafe.Sizeof(snapshotView{}))
	g.PublisherRecords, g.PublisherBytes = 3+g.Visible, uint64(unsafe.Sizeof(rootPublicationSealV1{}))+128+16*(g.Visible+1)
	g.ConsumeRecords, g.ConsumeBytes = 4, g.ActivationDispatchBytes+2*uint64(unsafe.Sizeof(input))+6*uint64(unsafe.Sizeof(member))+32
	g.TurnReleaseBytes = uint64(unsafe.Sizeof(rootpublication.NativePublicationV6{})) + 128
	g.CapsuleRecords, g.CapsuleBytes = 7, 29696
	return g, true
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeAllocatorGeometryForTestV6(w *iterator.OrdinalScanWork) (freelist.NativeCOWGeometryForTestV6, bool) {
	if c == nil || w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))) {
		return freelist.NativeCOWGeometryForTestV6{}, false
	}
	return c.allocatorPreparation.preparation.MaintenanceNativeGeometryForTestV6(w)
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeCoordinatorGeometryForTestV6(w *iterator.OrdinalScanWork) (rootpublication.NativeCoordinatorGeometryForTestV6, bool) {
	if c == nil || w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))) || c.db == nil || c.db.rootPublication == nil {
		return rootpublication.NativeCoordinatorGeometryForTestV6{}, false
	}
	return c.db.rootPublication.coordinator.MaintenanceNativeGeometryForTestV6(w)
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeResourcesGeometryForTestV6(w *iterator.OrdinalScanWork) (rootpublication.NativeEmptySetGeometryForTestV6, bool) {
	if c == nil || w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*c))) || c.candidatePreparation.member == nil {
		return rootpublication.NativeEmptySetGeometryForTestV6{}, false
	}
	return c.candidatePreparation.resources.MaintenanceNativeEmptyGeometryForTestV6(c.candidatePreparation.member.resources, w)
}

type NativeIndexGeometryForTestV6 struct {
	Token                            rootpublication.NativeIndexTokenGeometryForTestV6
	Pager                            pager.NativeMaterializationGeometryForTestV6
	SameSnapshot, SameNativeCallback bool
	OperationRefs                    uint64
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeIndexGeometryForTestV6(index int, target, first, last, pages uint64, w *iterator.OrdinalScanWork) (g NativeIndexGeometryForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(2, uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(Snapshot{}))+2*uint64(unsafe.Sizeof(stablePagerOwnedOperations{}))+2*uint64(unsafe.Sizeof(g))) || index < 0 || index > 1 || c.sealPreparation.seal == nil {
		return g, false
	}
	capture, seal := &c.sealPreparation.indexCaptures[index], c.sealPreparation.seal
	token := seal.token
	if index == 1 {
		token = seal.primary.token
	}
	var ok bool
	g.Token, ok = token.MaintenanceNativeIndexGeometryForTestV6(w)
	if !ok || capture.snapshot == nil {
		return g, false
	}
	snapshot := capture.snapshot
	snapshot.iteratorMu.Lock()
	operations := snapshot.stablePagerCompletion
	if operations == nil {
		snapshot.iteratorMu.Unlock()
		return g, false
	}
	g.SameSnapshot = operations.snapshot == snapshot && snapshot.idx == c.db.rootPublication.idx && snapshot.stableIndexCaptureTransferred
	g.SameNativeCallback, g.OperationRefs = operations.native == capture, uint64(operations.refs.Load())
	p, pathBytes := operations.pager, uint64(len(operations.syncPath))
	snapshot.iteratorMu.Unlock()
	g.Pager, ok = p.MaintenanceNativeGeometryForTestV6(target, first, last, pages, pathBytes, w)
	return g, ok
}

// Containing storage belongs to the SAME admitted selected-delete producer.
// A copied immutable parent is evidence, never a current-root/slot authority.
// The original transaction owns any accepted promotion and its abort queue.
type nativeMVCCSealPreparationV6 struct {
	claimRefundOwner          *retainedalloc.Owner
	claimRefundCharge         uint64
	resources, proofResources *rootpublication.StableResourceSet
	manifest                  *rootpublication.DependencyManifestV1
	banks                     *primaryBankConstructionV5
	claim                     *primaryarena.Claim
	parent                    rootpublication.PrimaryCapsuleViewV6
	parentRoot                primaryarena.Ref
	parentRecord              rootpublication.DurableRootRecordV1
	parentMeta                page.DurableMetaV1
	parentSlot, target        uint64
	record                    rootpublication.DurableRootRecordV1
	projection                rootpublication.PrimaryProjectionV5
	manifestRef               rootpublication.DependencyManifestRefV1
	parentImage               [rootpublication.PrimaryCapsuleSizeV6]byte
	manifestImage, scratch    [page.PageSize]byte
	indexCaptures             [2]nativeStablePagerCaptureV6
	seal                      *rootPublicationSealV1
	phase                     uint8
	promoted                  bool
	refund                    retainedalloc.RemovalWork
	completion                rootpublication.StableResourceTokenReleaseCompletionV6
	childCompletion           primaryRuntimeDisposalCompletionV6
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeSealScalarsForTest() (phase uint8, promoted bool) {
	if c == nil {
		return
	}
	return c.sealPreparation.phase, c.sealPreparation.promoted
}

// MaintenanceNativeIndexCaptureScalarsForTest reports only original operand
// identity/custody, never an alternate publication or cleanup handle.
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeIndexCaptureScalarsForTest() (count int, sameGeneration bool) {
	if c == nil || c.db == nil || c.db.rootPublication == nil {
		return
	}
	sameGeneration = true
	for i := range c.sealPreparation.indexCaptures {
		s := &c.sealPreparation.indexCaptures[i]
		if s.phase == 8 && (s.token != nil || c.sealPreparation.seal != nil && (i == 0 && c.sealPreparation.seal.token != nil || i == 1 && c.sealPreparation.seal.primary != nil && c.sealPreparation.seal.primary.token != nil)) {
			count++
			sameGeneration = sameGeneration && s.snapshot != nil && s.snapshot.idx == c.db.rootPublication.idx && s.snapshot.stableIndexCaptureTransferred
		}
	}
	return
}

// StepNativeMVCCSealPreparationV6 prepares the original publication's immutable
// operands. No prepared allocator slot, DB state, coordinator frontier or disk
// authority is installed. Every return releases the original exclusion locks.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCSealPreparationV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.candidatePreparation.phase != 8 || c.transaction == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	s, db, member := &c.sealPreparation, c.db, c.candidatePreparation.member
	if ready, err := s.refund.CompleteRemovalWork(&s.claimRefundOwner, &s.claimRefundCharge, w); !ready || err != nil {
		return false, err
	}
	runtime := db.rootPublication
	for {
		switch s.phase {
		case 0:
			ready, err := c.request.Validate(w)
			if !ready || err != nil {
				return false, err
			}
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(rootpublication.PrimaryCapsuleViewV6{}))+2*uint64(unsafe.Sizeof(rootpublication.DurableRootRecordV1{}))+128) {
				return false, nil
			}
			db.rootReuseMu.RLock()
			db.mu.RLock()
			runtime.mu.Lock()
			base := db.durableRoot
			err = func() error {
				if db.closing.Load() || db.idx.Load() != runtime.idx || db.meta.CommitSeq != c.rootPreparation.expectedSequence || base.primary == nil || base.record.CommitSeq >= member.sequence || !runtime.idx.primary.CapsuleFormatV6() {
					return errDurableRootCandidateStale
				}
				parent := base.primary.capsuleViews[base.slot]
				if parent.Current().Record != base.record || parent.Slot() != base.slot {
					return rootpublication.ErrDurableRootOwnership
				}
				copied, ready, e := parent.CopyIntoWithWorkV6(s.parentImage[:], w)
				if !ready || e != nil {
					return e
				}
				proof, ready, e := rootpublication.PrepareEmptyStableResourceSetWithOrdinalWorkV6(runtime.idx.primary.MetadataOwner(), base.slotResources[base.slot], w)
				if !ready || e != nil {
					return e
				}
				s.parent, s.parentRoot, s.parentRecord, s.parentMeta, s.parentSlot = copied, base.primary.records[base.slot], base.record, base.meta, base.slot
				s.target, s.proofResources, s.phase = 1-base.slot, proof, 1
				return nil
			}()
			runtime.mu.Unlock()
			db.mu.RUnlock()
			db.rootReuseMu.RUnlock()
			if err != nil || s.phase == 0 {
				return false, err
			}
		case 1:
			resources, ready, err := rootpublication.CloneAdmittedStableResourceSetWithOrdinalWorkV6(member.resources, w)
			if !ready || err != nil {
				return false, err
			}
			s.resources, s.phase = resources, 2
		case 2:
			manifest, ready, err := rootpublication.EmptyDependencyManifestWithWorkV6(s.resources, w)
			if !ready || err != nil {
				return false, err
			}
			if manifest.PageCount() != 1 {
				return false, rootpublication.ErrDurableRootOwnership
			}
			s.manifest, s.phase = manifest, 3
		case 3:
			if !w.Reserve(3, 2*uint64(unsafe.Sizeof(primaryBankConstructionV5{}))+2*uint64(unsafe.Sizeof(primaryarena.Ref{}))+128) {
				return false, nil
			}
			if s.banks == nil {
				banks, ready, err := newNativePrimaryBankConstructionV6(runtime.idx, w)
				if !ready || err != nil {
					return false, err
				}
				s.banks = banks
			}
			if ready, err := s.banks.ensureNativeCapacityV6(1, w); !ready || err != nil {
				return false, err
			}
			s.phase = 4
		case 4:
			if s.claim == nil {
				claim, ready, err := runtime.idx.primary.PrepareNativeClaimV6(primaryarena.Manifest, w)
				if !ready || err != nil {
					return false, err
				}
				s.claim = claim
			}
			ref, ready, err := s.claim.Step(w)
			if !ready || err != nil {
				return false, err
			}
			_ = ref // The original claim identity is consumed by the helper.
			if ready, err := s.finishClaimV6(false, w); !ready || err != nil {
				return false, err
			}
			s.phase = 5
		case 5:
			if !w.Reserve(3, 6*page.PageSize+2*uint64(unsafe.Sizeof(primaryManifestImagesV5{}))) {
				return false, nil
			}
			sink := primaryManifestImagesV5{first: s.banks.refs[0].PageID, images: s.manifestImage[:]}
			ref, err := s.manifest.MaterializeWithScratchV1(sink.first, &sink, s.scratch[:])
			if err != nil {
				return false, err
			}
			s.manifestRef, s.phase = ref, 6
		case 6:
			ready, err := runtime.idx.primary.SealMetadata(s.banks.refs[0], s.manifestImage[:], w)
			if !ready || err != nil {
				return false, err
			}
			s.phase = 7
		case 7:
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(s.record))+2*uint64(unsafe.Sizeof(s.projection))+128) {
				return false, nil
			}
			next, generation := member.next, member.primary.Generation()
			s.projection = member.primary.Projection()
			s.projection.ArenaHighWater = runtime.idx.primary.Pager().PageCount()
			s.record = rootpublication.DurableRootRecordV1{CommitSeq: next.CommitSeq, DurableSeq: s.parentRecord.DurableSeq + 1, UserRootPageID: next.UserRootPageID, SystemRootPageID: next.SystemRootPageID, TotalPages: generation.HighWater(), AppliedCommandLSN: next.AppliedCommandLSN, LastCommitHeight: next.LastCommitHeight, MaxEntryRevision: next.MaxEntryRevision, Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(), Manifest: s.manifestRef, ParentRecordPageID: s.parentMeta.RootRecordPageID, ParentCommitSeq: s.parentMeta.CommitSeq, ParentRecordDigest: s.parentMeta.RootRecordDigest, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(next.CommitSeq, s.parentRecord.DurableSeq+1, rootpublication.PrimaryCapsulePageV6(s.target))}
			s.phase = 8
		case 8:
			// The copied parent grants no arena edge. A foreground writer may
			// replace both slots while this frame yields. Revalidate the exact
			// generation-tagged basis under its original exclusion before asking
			// the SAME constructor to acquire any promotion edge.
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(s.parentRecord))+2*uint64(unsafe.Sizeof(s.parentMeta))+128) {
				return false, nil
			}
			db.rootReuseMu.RLock()
			db.mu.RLock()
			runtime.mu.Lock()
			base := db.durableRoot
			if db.closing.Load() || db.idx.Load() != runtime.idx || db.meta.CommitSeq != c.rootPreparation.expectedSequence || base.primary == nil || base.slot != s.parentSlot || base.record != s.parentRecord || base.meta != s.parentMeta || base.primary.records[base.slot] != s.parentRoot || base.primary.capsuleViews[base.slot].Current().Record != s.parentRecord {
				runtime.mu.Unlock()
				db.mu.RUnlock()
				db.rootReuseMu.RUnlock()
				return false, errDurableRootCandidateStale
			}
			// The transaction retains its original image and all four acquired
			// edges. This frame keeps only scalar completion state.
			_, ready, err := c.transaction.PreparePrivatePrimaryPromotionV6(s.record, s.projection, &s.parent, s.parentRoot, w)
			runtime.mu.Unlock()
			db.mu.RUnlock()
			db.rootReuseMu.RUnlock()
			if !ready || err != nil {
				return false, err
			}
			s.promoted, s.phase = true, 9
		case 9, 10:
			ready, err := s.indexCaptures[s.phase-9].step(c, s.phase == 10, w)
			if !ready || err != nil {
				return false, err
			}
			s.phase++
		case 11:
			ready, err := c.prepareNativePrivateSealDescriptorV6(w)
			if !ready || err != nil {
				return false, err
			}
			s.phase = 12
		case 12:
			runtime.mu.Lock()
			capacityReady, capacityErr := growPrimaryRuntimeSliceWithWorkV6(runtime, &runtime.seals, len(runtime.seals)+1, w)
			if !capacityReady || capacityErr != nil {
				runtime.mu.Unlock()
				return false, capacityErr
			}
			ready, err := preparePrimarySealCommitStorageLockedV6(s.seal, runtime.idx.primary, len(runtime.seals)+1, w)
			runtime.mu.Unlock()
			if !ready || err != nil {
				return false, err
			}
			s.phase = 13
		case 13:
			return true, nil
		default:
			return false, ErrNativeMVCCSnapshotWork
		}
	}
}

// A late wrapper-detach refusal can follow an installed bank. Preserve that
// original edge in the SAME bank construction before disposing the wrapper;
// cancellation drops the bank through its existing queue, never Claim.Cancel.
func (s *nativeMVCCSealPreparationV6) finishClaimV6(cancel bool, w *iterator.OrdinalScanWork) (bool, error) {
	if s.claim == nil {
		return true, nil
	}
	ref, installed, ready, err := s.claim.NativeInstalledWithWorkV6(w)
	if !ready || err != nil {
		return false, err
	}
	if installed {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(primaryBankConstructionV5{}))+2*uint64(unsafe.Sizeof(ref))) {
			return false, nil
		}
		if s.banks == nil || cap(s.banks.refs) < 1 || (len(s.banks.refs) > 0 && s.banks.refs[0] != ref) {
			return false, rootpublication.ErrDurableRootOwnership
		}
		s.banks.refs = s.banks.refs[:1]
		s.banks.refs[0] = ref
	} else if cancel {
		ready, err = s.claim.Cancel(w)
		if !ready || err != nil {
			return false, err
		}
	} else {
		return false, rootpublication.ErrDurableRootOwnership
	}
	owner, charge, ready, err := s.claim.DetachNativeAccountingV6(w)
	if !ready || err != nil {
		return false, err
	}
	s.claim = nil
	s.claimRefundOwner, s.claimRefundCharge = owner, charge
	return s.refund.CompleteRemovalWork(&s.claimRefundOwner, &s.claimRefundCharge, w)
}

func (s *nativeMVCCSealPreparationV6) closeStep(w *iterator.OrdinalScanWork) (bool, error) {
	if ready, err := s.refund.CompleteRemovalWork(&s.claimRefundOwner, &s.claimRefundCharge, w); !ready || err != nil {
		return false, err
	}
	if ready, err := s.completion.CompleteWithWorkV6(&s.refund, w); !ready || err != nil {
		return false, err
	}
	if ready, err := s.childCompletion.completeWithWork(&s.refund, w); !ready || err != nil {
		return false, err
	}
	if s.phase == 0 && s.claim == nil {
		// A declined proof clone may have copied only evidence bytes. Clear
		// those bytes before the containing producer charge is withdrawn.
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		*s = nativeMVCCSealPreparationV6{}
		return true, nil
	}
	if s.seal != nil {
		completion, ready, err := continueRootPublicationSealReleaseV6(s.seal, w, releasePrimaryResourceSetWithWorkV6)
		if !ready || err != nil {
			return false, err
		}
		s.seal = nil
		s.childCompletion = completion
		if ready, err := s.childCompletion.completeWithWork(&s.refund, w); !ready || err != nil {
			return false, err
		}
	}
	for i := range s.indexCaptures {
		if ready, err := s.indexCaptures[i].closeStep(w); !ready || err != nil {
			return false, err
		}
	}
	if ready, err := s.finishClaimV6(true, w); !ready || err != nil {
		return false, err
	}
	if s.banks != nil {
		completion, ready, err := continuePrimaryBankConstructionReleaseV6(s.banks, w)
		if !ready || err != nil {
			return false, err
		}
		s.banks = nil
		s.childCompletion = completion
		if ready, err := s.childCompletion.completeWithWork(&s.refund, w); !ready || err != nil {
			return false, err
		}
	}
	if s.manifest != nil {
		completion, ready, err := s.manifest.ReleaseOwnedMetadataWithWorkV6(w)
		if !ready || err != nil {
			return false, err
		}
		s.manifest = nil
		s.completion = completion
		if ready, err := s.completion.CompleteWithWorkV6(&s.refund, w); !ready || err != nil {
			return false, err
		}
	}
	for _, set := range [2]**rootpublication.StableResourceSet{&s.resources, &s.proofResources} {
		if *set != nil {
			completion, ready, err := releasePrimaryResourceSetWithWorkV6(*set, w)
			if !ready || err != nil {
				return false, err
			}
			*set = nil
			s.completion = completion
			if ready, err := s.completion.CompleteWithWorkV6(&s.refund, w); !ready || err != nil {
				return false, err
			}
		}
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	*s = nativeMVCCSealPreparationV6{}
	return true, nil
}

// The retained caller captures immutable operands on its SAME original seal.
// No activeSeal/debt/coordinator/allocator authority changes here. A newer
// writer can invalidate this private basis; final handoff must validate again.
func (c *NativeMVCCCurrentDeleteV6) prepareNativePrivateSealDescriptorV6(w *iterator.OrdinalScanWork) (bool, error) {
	s, db, member := &c.sealPreparation, c.db, c.candidatePreparation.member
	runtime := db.rootPublication
	db.rootReuseMu.RLock()
	db.mu.RLock()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	defer db.mu.RUnlock()
	defer db.rootReuseMu.RUnlock()
	// Admit the fixed basis copy/comparison before reading the current record
	// or copied capsule view. This is repeated at every retained attempt.
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(s.parentRecord))+2*uint64(unsafe.Sizeof(s.parentMeta))+128) {
		return false, nil
	}
	base := db.durableRoot
	if db.closing.Load() || db.idx.Load() != runtime.idx || db.meta.CommitSeq != c.rootPreparation.expectedSequence || base.primary == nil || base.slot != s.parentSlot || base.record != s.parentRecord || base.meta != s.parentMeta || base.primary.records[base.slot] != s.parentRoot || base.primary.capsuleViews[base.slot].Current().Record != s.parentRecord {
		return false, errDurableRootCandidateStale
	}
	count := len(runtime.debt) + 1
	bytes := 2*uint64(unsafe.Sizeof(rootPublicationSealV1{})) + 2*uint64(unsafe.Sizeof(primaryPublicationSealV5{})) + 3*uint64(count)*uint64(unsafe.Sizeof((*freelist.PreparedCOWCandidateV1)(nil)))
	if !w.Reserve(3+uint64(count), bytes) {
		if 3+uint64(count) > w.RecordLimit || bytes > w.ByteLimit {
			return false, &iterator.OrdinalUnitTooLarge{Records: 3 + uint64(count), Bytes: bytes}
		}
		return false, nil
	}
	promotion := c.transaction.PrimaryPromotionV6()
	// The SAME capsule encoder replaces the logical directory page with its
	// fixed slot directory and embeds the parent by value. Compare that exact
	// canonical record, not its unencoded logical constructor input.
	expectedRecord := s.record
	expectedRecord.UserRootPageID = rootpublication.PrimaryCapsulePageV6(s.target) + 1
	expectedRecord.ParentRecordPageID, expectedRecord.ParentCommitSeq = 0, 0
	expectedRecord.ParentRecordDigest = [32]byte{}
	if promotion == nil || promotion.Root == (primaryarena.Ref{}) || promotion.Parent != s.parentRoot || promotion.View.Slot() != s.target || promotion.View.Current().Record != expectedRecord || s.indexCaptures[0].token == nil || s.indexCaptures[1].token == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	var digest [32]byte
	copy(digest[:], promotion.Image[48:80])
	meta, err := page.NewDurableMetaV1(s.record.CommitSeq, s.record.DurableSeq, rootpublication.PrimaryCapsulePageV6(s.target), digest)
	if err != nil {
		return false, err
	}
	charge := primarySealMetadataV6(count)
	owner := runtime.idx.primary.MetadataOwner()
	if err := retainedalloc.AddPendingWithOrdinalWork(owner, charge, w); err != nil {
		return false, err
	}
	prefix := make([]*freelist.PreparedCOWCandidateV1, count)
	copy(prefix, runtime.debt)
	prefix[count-1] = member.prepared
	s.seal = newPrimaryCapsuleSealDescriptorV6(rootPublicationSealV1{metadata: owner, metadataCharge: charge, latestSequence: member.sequence, groupLength: 1, idx: runtime.idx, next: member.next, resources: s.resources, manifest: s.manifest, prefix: prefix, token: s.indexCaptures[0].token, record: promotion.View.Current().Record, meta: meta, target: s.target, base: rootPublicationSealBaseV1{slotCommit: base.slotCommit, slotResources: base.slotResources, slotMeta: base.slotMeta, slotRecord: base.slotRecord}}, primaryPublicationSealV5{prepared: member.primary, projection: promotion.View.Current().Primary, manifestBanks: s.banks, token: s.indexCaptures[1].token, proofResources: s.proofResources, promotion: promotion})
	// The actual callbacks still point into these original containing frames.
	// Transfer only the original disposing-parent aliases, never the controls.
	s.resources, s.manifest, s.banks, s.proofResources = nil, nil, nil, nil
	s.indexCaptures[0].token, s.indexCaptures[1].token = nil, nil
	return true, nil
}
