package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// Containing storage belongs to the SAME admitted selected-delete producer.
// A copied immutable parent is evidence, never a current-root/slot authority.
// The original transaction owns any accepted promotion and its abort queue.
type nativeMVCCSealPreparationV6 struct {
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
			banks, err := newPrimaryBankConstructionV5(runtime.idx)
			if err != nil {
				return false, err
			}
			s.banks, s.phase = banks, 4
			if err := banks.ensureCapacity(1); err != nil {
				return false, err
			}
		case 4:
			if s.claim == nil {
				claim, ready, err := runtime.idx.primary.PrepareClaim(primaryarena.Manifest, w)
				if !ready || err != nil {
					return false, err
				}
				s.claim = claim
			}
			ref, ready, err := s.claim.Step(w)
			if !ready || err != nil {
				return false, err
			}
			s.claim = nil
			s.banks.refs = s.banks.refs[:1]
			s.banks.refs[0] = ref
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

func (s *nativeMVCCSealPreparationV6) closeStep(w *iterator.OrdinalScanWork) (bool, error) {
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
		completion.complete()
	}
	for i := range s.indexCaptures {
		if ready, err := s.indexCaptures[i].closeStep(w); !ready || err != nil {
			return false, err
		}
	}
	if s.claim != nil {
		ready, err := s.claim.Cancel(w)
		if !ready || err != nil {
			return false, err
		}
		s.claim = nil
	}
	if s.banks != nil {
		completion, ready, err := continuePrimaryBankConstructionReleaseV6(s.banks, w)
		if !ready || err != nil {
			return false, err
		}
		s.banks = nil
		completion.complete()
	}
	if s.manifest != nil {
		completion, ready, err := s.manifest.ReleaseOwnedMetadataWithWorkV6(w)
		if !ready || err != nil {
			return false, err
		}
		s.manifest = nil
		completion.Complete()
	}
	for _, set := range [2]**rootpublication.StableResourceSet{&s.resources, &s.proofResources} {
		if *set != nil {
			completion, ready, err := releasePrimaryResourceSetWithWorkV6(*set, w)
			if !ready || err != nil {
				return false, err
			}
			*set = nil
			completion.Complete()
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
	if err := owner.AddPending(charge); err != nil {
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
