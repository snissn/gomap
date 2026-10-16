package db

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"unsafe"
)

// A row is only checked arithmetic over separately observed operands. It does
// not validate them as one atomic basis or reserve any future authority.
type NativeFRowForTestV6 struct {
	Records, Bytes     uint64
	Complete, Overflow bool
}

// role order mirrors the complete F291 source table. Variable Z,V,A and R
// use their original separate observers. K/keyBytes come from individually
// preadmitted immutable selected records, never count-then-reserve.
func (c *NativeMVCCCurrentDeleteV6) MaintenanceNativeFRowForTestV6(role uint8, debt, k, keyBytes, requestBytes uint64, r NativeRemainderGeometryForTestV6, w *iterator.OrdinalScanWork) (g NativeFRowForTestV6, ready bool) {
	if c == nil || w == nil || !w.Reserve(2, 2*uint64(unsafe.Sizeof(g))+2*uint64(unsafe.Sizeof(r))+128) {
		return g, false
	}
	p := nativePublicationRemainderV6{}
	add := func(n, b uint64) bool { return p.add(n, b) }
	ok := true
	switch role {
	case 0:
		ok = add(6, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(nativeSealInstallationV6{}))+2*uint64(unsafe.Sizeof(mvccadmission.Cut{}))+2*uint64(unsafe.Sizeof(rootpublication.NativePublicationCompletionPlanV6{}))+2*uint64(unsafe.Sizeof(rootpublication.NativeBuilderReleasePlanV6{}))+2*uint64(unsafe.Sizeof(rootpublication.PublishResult{}))+64)
	case 1:
		ok = add(2, uint64(unsafe.Sizeof(c.publicationCoordinator))+uint64(unsafe.Sizeof(c.publicationBound))+uint64(unsafe.Sizeof(c.publication)))
	case 2:
		parentRecords, parentBytes := nativeCapsuleParentRecordWorkV6()
		witnessRecords, witnessBytes := nativeRootSourceWitnessWorkV6()
		ok = add(4+parentRecords+witnessRecords, parentBytes+witnessBytes+uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(rootPublicationSealV1{}))+2*uint64(unsafe.Sizeof(rootpublication.DurableRootRecordV1{}))+2*uint64(unsafe.Sizeof(page.DurableMetaV1{}))+2*uint64(unsafe.Sizeof(primaryStateRootV5{}))+uint64(unsafe.Sizeof(DBState{}))) && add(1, requestBytes)
		ok = ok && p.addN(2, 3, 2*uint64(unsafe.Sizeof(nativeStablePagerCaptureV6{}))+2*uint64(unsafe.Sizeof(stablePagerOwnedOperations{}))+2*uint64(unsafe.Sizeof(Snapshot{}))+uint64(unsafe.Sizeof(indexGen{}))) && p.addN(2, 1, 2*uint64(unsafe.Sizeof(pager.Pager{}))+16)
		ok = ok && p.addN(debt, 1, 2*uint64(unsafe.Sizeof((*freelist.PreparedCOWCandidateV1)(nil)))) && add(2, 2*uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}.seals))+2*uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}.activeSeal))+uint64(unsafe.Sizeof(rootPublicationSealV1{})))
	case 3:
		ok = add(2, 2*uint64(unsafe.Sizeof(mvccadmission.Cut{}))+2*uint64(unsafe.Sizeof(mvccadmission.Authority{}))+uint64(unsafe.Sizeof(c.records))) && p.addN(k, 2, uint64(unsafe.Sizeof(node.LeafEntry{}))+requestBytes) && add(0, keyBytes)
	case 4:
		if !r.Complete {
			return g, true
		}
		rr, bb, valid := freelist.NativePublicationObservationPriceForTestV6(r.Bound.Prefix, r.Bound.MetadataPages)
		cr, cb := pager.NativeCapacityCountWorkV6()
		ok = valid && add(6, 2*uint64(unsafe.Sizeof(nativePublicationRemainderV6{}))+2*uint64(unsafe.Sizeof(*c))+uint64(unsafe.Sizeof(rootPublicationRuntimeV1{}))+uint64(unsafe.Sizeof(freelist.NativePublicationBoundV6{}))) && add(rr, bb) && add(cr, cb)
	case 5:
		ok = add(4, 2*uint64(unsafe.Sizeof(nativePublicationWorkFloorV6{}))+2*uint64(unsafe.Sizeof(iterator.OrdinalScanWork{}))+2*uint64(unsafe.Sizeof(nativePublicationRemainderV6{}))+32)
	case 6:
		// Member/post validation (2+1), member in-memory tail (2), output
		// allocations (2), original install/init (4). Z,V,A are separate rows.
		ok = add(3, uint64(unsafe.Sizeof(rootPublicationVisibleInstallV1{}))+uint64(unsafe.Sizeof(finalizeCommitPost{}))+uint64(unsafe.Sizeof(pruneWorker{}))+uint64(unsafe.Sizeof(lifecycle.Graveyard{}))) && add(2, 2*uint64(unsafe.Sizeof(rootpublication.DurableRootCallbackInput{}))+12*uint64(unsafe.Sizeof((*rootPublicationVisibleMemberV1)(nil)))) && add(2, uint64(unsafe.Sizeof(DBState{}))+uint64(unsafe.Sizeof(snapshotView{}))) && add(4, 2*uint64(unsafe.Sizeof(page.MetaPageBody{}))+uint64(unsafe.Sizeof(DBState{}))+uint64(unsafe.Sizeof(snapshotView{})))
	default:
		return g, false
	}
	g.Records, g.Bytes, g.Complete, g.Overflow = p.records, p.bytes, ok, !ok
	return g, true
}
