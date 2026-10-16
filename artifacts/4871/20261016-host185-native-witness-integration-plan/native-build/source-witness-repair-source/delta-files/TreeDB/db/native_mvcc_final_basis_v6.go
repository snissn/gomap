package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Invocation-local preparation of the SAME original seal installation. It is
// never attached to a cursor and cannot survive a return or release of the
// original write/durable/rootReuse build exclusion. It owns no extra edge.
type nativeSealInstallationV6 struct {
	runtime  *rootPublicationRuntimeV1
	seal     *rootPublicationSealV1
	member   *rootPublicationVisibleMemberV1
	admitted bool
}

// Caller holds writeMu -> durablePublishMu -> rootReuseMu. The short db/runtime
// read scopes below are released before enqueue's coordinator callback. The
// full basis, including the original generation-tagged parent Ref, is checked
// before any enqueue/activation/slot effect, without restoring a zeroed scalar
// ParentCommitSeq as authority.
func (c *NativeMVCCCurrentDeleteV6) prepareNativeSealInstallationV6(w *iterator.OrdinalScanWork) (nativeSealInstallationV6, bool, error) {
	var none nativeSealInstallationV6
	if c == nil || w == nil {
		return none, false, rootpublication.ErrDurableRootOwnership
	}
	parentRecords, parentBytes := nativeCapsuleParentRecordWorkV6()
	witnessRecords, witnessBytes := nativeRootSourceWitnessWorkV6()
	if !w.Reserve(4+parentRecords+witnessRecords, parentBytes+witnessBytes+uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(rootPublicationSealV1{}))+2*uint64(unsafe.Sizeof(rootpublication.DurableRootRecordV1{}))+2*uint64(unsafe.Sizeof(page.DurableMetaV1{}))+2*uint64(unsafe.Sizeof(primaryStateRootV5{}))+uint64(unsafe.Sizeof(DBState{}))) {
		return none, false, nil
	}
	if c.db == nil || c.snapshot == nil || c.candidatePreparation.phase != 8 || c.sealPreparation.phase != 13 || c.rootPreparation.phase != 7 {
		return none, false, rootpublication.ErrDurableRootOwnership
	}
	if ready, err := c.request.Validate(w); !ready || err != nil {
		return none, false, err
	}
	db, member, s := c.db, c.candidatePreparation.member, &c.sealPreparation
	runtime := db.rootPublication
	if runtime == nil {
		return none, false, rootpublication.ErrDurableRootOwnership
	}
	// Both SAME retained callbacks and the authority pager must support the
	// concrete single-attempt barrier BEFORE enqueue/activation. These reads
	// borrow existing admitted capture frames, never a replacement token/FD.
	for i := range s.indexCaptures {
		if !w.Reserve(3, 2*uint64(unsafe.Sizeof(nativeStablePagerCaptureV6{}))+2*uint64(unsafe.Sizeof(stablePagerOwnedOperations{}))+2*uint64(unsafe.Sizeof(Snapshot{}))+uint64(unsafe.Sizeof(indexGen{}))) {
			return none, false, nil
		}
		capture := &s.indexCaptures[i]
		if capture.phase != 8 || capture.snapshot == nil || runtime.idx == nil || runtime.idx.primary == nil {
			return none, false, rootpublication.ErrDurableRootOwnership
		}
		// Construction has dropped its independent operations edge by phase 8.
		// Borrow the SAME callback retained by the original Snapshot/token; do
		// not infer that its deliberately cleared builder alias is an owner.
		capture.snapshot.iteratorMu.Lock()
		operations := capture.snapshot.stablePagerCompletion
		valid := operations != nil && operations.native == capture && operations.snapshot == capture.snapshot && operations.pager != nil && operations.refs.Load() > 0 && !operations.disposalConsumed.Load()
		expected := runtime.idx.pager
		if i == 1 {
			expected = runtime.idx.primary.Pager()
		}
		valid = valid && operations.pager == expected
		capture.snapshot.iteratorMu.Unlock()
		if !valid {
			return none, false, rootpublication.ErrDurableRootOwnership
		}
		if ready, err := expected.ValidateNativeFileFenceWithWorkV6(w); !ready || err != nil {
			return none, false, err
		}
	}
	db.mu.RLock()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	defer db.mu.RUnlock()
	base, seal := &db.durableRoot, s.seal
	state := db.state.Load()
	// View access returns an operand by value; the original fixed header reserve
	// already covers that existing read. The projection has its own admission.
	var parentRecord rootpublication.DurableRootRecordV1
	if seal != nil && seal.primary != nil && seal.primary.promotion != nil {
		parentRecord = seal.primary.promotion.View.Parent().Record
	}
	if db.closing.Load() || runtime.poison != nil || db.idx.Load() != runtime.idx || c.snapshot.idx != runtime.idx || db.meta.CommitSeq != c.rootPreparation.expectedSequence ||
		c.source != nil || state == nil || !c.rootPreparation.matchesCurrentRoot(state.primaryRoot) ||
		member == nil || member.install == nil || member.install.valueLogSet != state.ValueLogSet || member.install.activated || member.transaction != c.transaction || member.primary == nil ||
		seal == nil || seal.idx != runtime.idx || runtime.activeSeal != nil || len(runtime.visibleMembers) != runtime.visibleHead ||
		base.primary == nil || base.slot != s.parentSlot || base.meta != s.parentMeta || base.record != s.parentRecord ||
		base.primary.records[base.slot] != s.parentRoot || s.parentRoot == (primaryarena.Ref{}) ||
		base.primary.capsuleViews[base.slot].Current().Record != s.parentRecord || seal.target != 1-base.slot ||
		seal.primary == nil || seal.primary.prepared != member.primary || seal.primary.promotion == nil ||
		seal.primary.promotion.Parent != s.parentRoot || !nativeCapsuleParentRecordMatchesV6(&base.record, &parentRecord, seal.target) ||
		seal.primary.promotion.View.Slot() != seal.target || seal.primary.promotion.View.Current().Record != seal.record ||
		c.transaction.PrimaryPromotionV6() != seal.primary.promotion || !seal.commitStorageReady || seal.commitRuntime == nil ||
		cap(seal.commitRetired) < len(runtime.seals)+1 || len(runtime.seals) == cap(runtime.seals) ||
		len(seal.prefix) != len(runtime.debt)+1 || seal.prefix[len(seal.prefix)-1] != member.prepared {
		return none, false, errDurableRootCandidateStale
	}
	for i := range runtime.debt {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(runtime.debt[i]))) {
			return none, false, nil
		}
		if seal.prefix[i] != runtime.debt[i] {
			return none, false, errDurableRootCandidateStale
		}
	}
	// Actual two slice/pointer installs happen after original enqueue activation.
	// Their capacity and all mutable operands remain protected until commit.
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(runtime.seals))+2*uint64(unsafe.Sizeof(runtime.activeSeal))+uint64(unsafe.Sizeof(seal))) {
		return none, false, nil
	}
	return nativeSealInstallationV6{runtime: runtime, seal: seal, member: member, admitted: true}, true, nil
}

func (p *nativeSealInstallationV6) commit() {
	if !p.admitted {
		panic("native seal installation without original admission")
	}
	p.admitted = false
	p.runtime.mu.Lock()
	p.runtime.seals = append(p.runtime.seals, p.seal)
	p.runtime.activeSeal = p.seal
	p.runtime.mu.Unlock()
}

// observeNativePublicationCutV6 consumes only actual selected deletion keys.
// The retained selected+anchor proof and exact current base above are separate
// prerequisites. The caller's stack Cut is passed to the SAME visible install
// and always aborted on an unsuccessful enqueue; no Cut survives a return/I/O.
func (c *NativeMVCCCurrentDeleteV6) observeNativePublicationCutV6(cut *mvccadmission.Cut, w *iterator.OrdinalScanWork) (bool, error) {
	if cut == nil || w == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*cut))+2*uint64(unsafe.Sizeof(mvccadmission.Authority{}))+uint64(unsafe.Sizeof(c.records))) {
		return false, nil
	}
	records := c.records
	if len(records) == 0 {
		records = unsafe.Slice(&c.record, 1)
	}
	*cut = c.db.mvccAdmission.Begin()
	for i := range records {
		if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))) {
			cut.Abort()
			return false, nil
		}
		if ready, err := cut.ObservePrune(c.request, records[i].Key, w); !ready || err != nil {
			cut.Abort()
			return false, err
		}
	}
	return true, nil
}
