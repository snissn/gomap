package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// nativeAuthorityScopeV6 is call-local custody over the original authority
// mutation. It never escapes the original producer
// call to publishNativeAuthorityWithWorkV6. Dependencies, namespaces,
// materialization and index sync run before this scope. Only the fixed actual
// authority write and original post-fence install run under these locks.
type nativeAuthorityScopeV6 struct {
	runtime         *rootPublicationRuntimeV1
	seal            *rootPublicationSealV1
	allocator       freelist.COWPublicationScopeV6
	active          bool
	work            *iterator.OrdinalScanWork
	failureAdmitted bool
	hardIO          bool // by-value disposition; no error/resource alias
}

func (s *nativeAuthorityScopeV6) begin(runtime *rootPublicationRuntimeV1, seal *rootPublicationSealV1, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || s.active || runtime == nil || seal == nil || w == nil {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	db := runtime.db
	db.durablePublishMu.Lock()
	db.rootReuseMu.Lock()
	runtime.mu.Lock()
	s.runtime, s.seal, s.active = runtime, seal, true
	ready := false
	defer func() {
		if !ready {
			s.Close()
		}
	}()
	// All current validation and compaction shapes are admitted before a
	// variable traversal. These bounds cover prefix validation, debt copy and
	// clear, seal partition and clear, and fixed two-slot ownership transfers.
	records, bytes, fits := checkedNativeAuthorityAdmissionV6(uint64(len(seal.prefix)), uint64(len(seal.prefix)), uint64(len(runtime.seals)), 1)
	if !fits {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(records, bytes) {
		if records > w.RecordLimit || bytes > w.ByteLimit {
			return false, &iterator.OrdinalUnitTooLarge{Records: records, Bytes: bytes}
		}
		return false, nil
	}
	var member *rootPublicationVisibleMemberV1
	if runtime.visibleHead < len(runtime.visibleMembers) {
		member = runtime.visibleMembers[runtime.visibleHead]
	}
	if member != nil && member.sequence != seal.latestSequence {
		member = nil
	}
	// Canonical capsules embed their parent by value and zero ParentCommitSeq.
	// Bind authority to the complete captured original slot and generation ref.
	if seal.target > 1 || seal.primary == nil || seal.primary.promotion == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	baseSlot := uint64(1) - seal.target
	if runtime.activeSeal != seal || runtime.poison != nil || member == nil || member.transaction == nil ||
		!member.transaction.RequiresNativeCallbackWorkV6() || seal.primary == nil ||
		member.primary != seal.primary.prepared || member.primaryRecordTransferred ||
		db.durableRoot.primary == nil || db.durableRoot.slot != baseSlot ||
		db.durableRoot.meta != seal.base.slotMeta[baseSlot] || db.durableRoot.record != seal.base.slotRecord[baseSlot] ||
		db.durableRoot.primary.records[baseSlot] != seal.primary.promotion.Parent ||
		seal.primary.promotion.View.Parent().Record != db.durableRoot.record ||
		!seal.commitStorageReady || seal.commitRuntime == nil || cap(seal.commitRetired) < len(runtime.seals) ||
		len(runtime.debt) < len(seal.prefix) {
		return false, rootpublication.ErrDurableRootOwnership
	}
	for i, prepared := range seal.prefix {
		if runtime.debt[i] != prepared {
			return false, rootpublication.ErrDurableRootOwnership
		}
	}
	if len(seal.prefix) != 0 {
		ok, err := seal.idx.allocator.BeginCOWPublicationScopeV6(&s.allocator, seal.prefix, w)
		if !ok || err != nil {
			return false, err
		}
	}
	// Fixed original error disposition is fully admitted before the authority
	// write can become ambiguous. Later unlocking cannot change its geometry:
	// exact allocator-state membership uses the candidate's original backlink.
	if !w.Reserve(3, 2*uint64(unsafe.Sizeof(runtime.nativePoison))+2*uint64(unsafe.Sizeof(runtime.poison))+2*uint64(unsafe.Sizeof(db.publicationPoisoned))) {
		return false, nil
	}
	if admitted, err := freelist.AdmitCOWFailureV6(w); !admitted || err != nil {
		return false, err
	}
	s.failureAdmitted = true
	ready = true
	return true, nil
}

// Scope locks are already unwound here. Admission precedes the actual write;
// these original fixed state operations cannot later refuse for lack of work.
func (s *nativeAuthorityScopeV6) failAmbiguousAdmittedV6(runtime *rootPublicationRuntimeV1, seal *rootPublicationSealV1, cause error) {
	if !s.failureAdmitted || s.active {
		panic("native ambiguous disposition without original admission/unlock")
	}
	s.failureAdmitted = false
	runtime.mu.Lock()
	if runtime.poison == nil {
		runtime.nativePoison.causes[0], runtime.nativePoison.causes[1] = cause, ErrRecoveryRequired
		runtime.poison = &runtime.nativePoison
	}
	poison := runtime.poison
	runtime.mu.Unlock()
	runtime.db.publicationPoisoned.Store(true)
	if seal.prepared != nil {
		_ = seal.idx.allocator.FailCOWCandidateAdmittedV6(seal.prepared, poison)
	}
}

func nativeAuthorityAdmissionV6(prefix, debt, seals, visible uint64) (uint64, uint64) {
	return 11 + 2*prefix + 4*debt + 3*seals + 2*visible,
		2*uint64(unsafe.Sizeof(nativeAuthorityScopeV6{})) + 4*uint64(unsafe.Sizeof(primaryDurableRuntimeV5{})) + 16*prefix + 32*debt + 32*seals + 32*visible
}

// checkedNativeAuthorityAdmissionV6 evaluates the same concrete visit/header
// formula without wrapping a large mutable geometry into a smaller grant.
func checkedNativeAuthorityAdmissionV6(prefix, debt, seals, visible uint64) (uint64, uint64, bool) {
	p := nativePublicationRemainderV6{}
	if !p.add(11, 2*uint64(unsafe.Sizeof(nativeAuthorityScopeV6{}))+4*uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))) ||
		!p.addN(prefix, 2, 16) || !p.addN(debt, 4, 32) || !p.addN(seals, 3, 32) || !p.addN(visible, 2, 32) {
		return 0, 0, false
	}
	return p.records, p.bytes, true
}

func (s *nativeAuthorityScopeV6) Close() {
	if s == nil || !s.active {
		return
	}
	runtime := s.runtime
	s.allocator.Close()
	s.runtime, s.seal, s.active = nil, nil, false
	runtime.mu.Unlock()
	runtime.db.rootReuseMu.Unlock()
	runtime.db.durablePublishMu.Unlock()
}
