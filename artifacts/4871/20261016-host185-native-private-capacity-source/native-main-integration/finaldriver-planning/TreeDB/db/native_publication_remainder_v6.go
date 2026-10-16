package db

import (
	"context"
	"os"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
)

// This is an invocation-local forecast of the actual SAME operations AFTER
// enqueue activation. It carries neither replacement work nor authority.
type nativePublicationRemainderV6 struct{ records, bytes uint64 }

func (p *nativePublicationRemainderV6) add(r, b uint64) bool {
	if r > ^uint64(0)-p.records || b > ^uint64(0)-p.bytes {
		return false
	}
	p.records, p.bytes = p.records+r, p.bytes+b
	return true
}

func (p *nativePublicationRemainderV6) addN(n, r, b uint64) bool {
	if n != 0 && (r > ^uint64(0)/n || b > ^uint64(0)/n) {
		return false
	}
	return p.add(n*r, n*b)
}

// Caller retains the original write/durable/rootReuse build exclusion. The
// runtime scope is never reacquired while allocator/ledger locks are held.
// Private claim geometry can still change: the actual claim must reserve on
// the reduced SAME work grant before its first effect, preserving this floor.
func (c *NativeMVCCCurrentDeleteV6) nativePublicationRemainderWithWorkV6(w *iterator.OrdinalScanWork) (p nativePublicationRemainderV6, ready bool, err error) {
	if w == nil || !w.Reserve(6, 2*uint64(unsafe.Sizeof(p))+2*uint64(unsafe.Sizeof(*c))+uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}))+uint64(unsafe.Sizeof(freelist.NativePublicationBoundV6{}))) {
		return p, false, nil
	}
	runtime, seal := c.db.rootPublication, c.sealPreparation.seal
	if runtime == nil || seal == nil || seal.idx == nil || seal.idx.pager == nil {
		return p, false, rootpublication.ErrDurableRootOwnership
	}
	runtime.mu.Lock()
	g, ready, err := c.allocatorPreparation.preparation.NativePublicationBoundWithWorkV6(seal.prefix, w)
	seals := uint64(len(runtime.seals))
	runtime.mu.Unlock()
	if !ready || err != nil {
		return p, false, err
	}
	if seals == ^uint64(0) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	seals++ // the SAME preadmitted seal installation follows successful enqueue
	if ready, err := seal.idx.pager.ValidateNativeCountCapacityWithWorkV6(g.HighWater, w); !ready || err != nil {
		return p, false, err
	}
	return nativePublicationRemainderForGeometryV6(g, seals)
}

// Pure evaluation of the SAME actual operation calculators. Numeric geometry
// does not grant authority and must have been observed/revalidated by the
// original locked caller before any effect.
func nativePublicationRemainderForGeometryV6(g freelist.NativePublicationBoundV6, seals uint64) (p nativePublicationRemainderV6, ready bool, err error) {
	add := func(r, b uint64) bool { return p.add(r, b) }
	r, b := nativeOrderedPostWorkV6()
	if !add(r, b) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	// Publisher result/context, prepared-seal lookup, exact visible head.
	if !add(6, 2*uint64(unsafe.Sizeof(rootpublication.PublishResult{}))+2*uint64(unsafe.Sizeof((context.Context)(nil)))+2*uint64(unsafe.Sizeof(rootPublicationSealV1{}))+uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}))+uint64(unsafe.Sizeof(rootpublication.PreparedRootCandidate{}))+2*uint64(unsafe.Sizeof(rootPublicationVisibleMemberV1{}))+192) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	// Native executor wrapper, original storage-transaction header, both empty
	// dependency operations, original observation and materialize dispatch.
	if !add(4, 4*uint64(unsafe.Sizeof(nativeAuthorityScopeV6{}))+uint64(unsafe.Sizeof(durableRootStorageTransactionV1{}))+uint64(unsafe.Sizeof((*rootpublication.StableResourceSet)(nil)))+uint64(unsafe.Sizeof(""))) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	r, b = rootpublication.NativeEmptyResourceOperationWorkV6()
	if !p.addN(2, r, b) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	// Six original materialization headers, canonical metadata/physical-tail
	// validation, capacity-covered logical count install and recorded page writes.
	if !add(6, 2*uint64(unsafe.Sizeof(rootPublicationSealV1{}))+uint64(unsafe.Sizeof(primaryPublicationSealV5{}))+uint64(unsafe.Sizeof(rootpublication.PreparedPrimaryRootV5{}))+uint64(unsafe.Sizeof(freelist.FreelistGenerationV1{}))+uint64(unsafe.Sizeof(rootpublication.PrimaryPromotionV6{}))+uint64(unsafe.Sizeof(indexGen{}))) || !add(g.MaterializationRecords, g.MaterializationBytes) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	r, b = pager.NativeCapacityCountWorkV6()
	if !add(r, b) || !p.addN(g.Prefix, 1, uint64(unsafe.Sizeof(freelist.PreparedCOWCandidateV1{}))+uint64(unsafe.Sizeof((*freelist.PreparedCOWCandidateV1)(nil)))) || !add(g.PageRecords, g.PageBytes) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	// BOTH captured native index callbacks, PRIMARY namespace, and dispatch.
	if !add(1, 2*uint64(unsafe.Sizeof(nativeAuthorityScopeV6{}))+uint64(unsafe.Sizeof(rootPublicationSealV1{}))+uint64(unsafe.Sizeof(primaryPublicationSealV5{}))) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	r, b = rootpublication.NativeIndexSyncWorkV6()
	if !p.addN(2, r, b) || !p.addN(2, 1, uint64(unsafe.Sizeof(stablePagerOwnedOperations{}))+uint64(unsafe.Sizeof((*os.File)(nil)))) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	r, b = pager.NativeFileFenceWorkV6()
	if !p.addN(2, r, b) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	r, b = rootpublication.NativeStableNamespaceWorkV6()
	if !add(r, b) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	// Actual authority wrapper, SAME locked runtime/allocator scopes, BOTH
	// possible error dispositions and the original three-page typed capsule.
	if !add(2, 2*uint64(unsafe.Sizeof(nativeAuthorityScopeV6{}))+2*uint64(unsafe.Sizeof(rootpublication.PublishResult{}))+32) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	var fits bool
	r, b, fits = checkedNativeAuthorityAdmissionV6(g.Prefix, g.Prefix, seals, 1)
	if !fits {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if !add(r, b) || !add(g.PublicationRecords, g.PublicationBytes) || !add(3, 2*uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}.nativePoison))+2*uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}.poison))+2*uint64(unsafe.Sizeof(DB{}.publicationPoisoned))) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	r, b = freelist.COWFailureWorkV6()
	if !add(r, b) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	r, b = pager.NativeCapsuleWriteWorkV6()
	if !add(r, b) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	// Original Set logical handoff, transaction dispatch and concrete member
	// Consume. Completion/timer/notify/clock have ALREADY been admitted by the
	// invocation-local Coordinator plan; they must not be charged twice here.
	r, b = rootpublication.NativePublicationConsumeWorkV6()
	if !add(r, b) || !add(3, 2*uint64(unsafe.Sizeof(rootpublication.DurableRootCallbackInput{}))+6*uint64(unsafe.Sizeof((*rootPublicationVisibleMemberV1)(nil)))+32) {
		return p, false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	return p, true, nil
}

// Withhold/restore touches only invocation-local limits. Actual operations
// still Reserve once against this SAME w; no scalar credit funds a callback.
// The caller must defer restore immediately, including refusal/error paths.
type nativePublicationWorkFloorV6 struct {
	work           *iterator.OrdinalScanWork
	records, bytes uint64
}

func (p nativePublicationRemainderV6) withhold(w *iterator.OrdinalScanWork) (nativePublicationWorkFloorV6, bool) {
	var floor nativePublicationWorkFloorV6
	if w == nil || !w.Reserve(4, 2*uint64(unsafe.Sizeof(floor))+2*uint64(unsafe.Sizeof(*w))+2*uint64(unsafe.Sizeof(p))+32) {
		return floor, false
	}
	if w.Records > w.RecordLimit || w.Bytes > w.ByteLimit || p.records > w.RecordLimit-w.Records || p.bytes > w.ByteLimit-w.Bytes {
		return floor, false
	}
	floor = nativePublicationWorkFloorV6{w, w.RecordLimit, w.ByteLimit}
	w.RecordLimit -= p.records
	w.ByteLimit -= p.bytes
	return floor, true
}

func (f *nativePublicationWorkFloorV6) restore() {
	if f.work != nil {
		f.work.RecordLimit, f.work.ByteLimit = f.records, f.bytes
		f.work = nil
	}
}
