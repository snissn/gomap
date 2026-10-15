package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// preparePrimarySealCommitStorageV6 allocates only blank commit storage. No
// predecessor reference is independently acquired or physically disposed here.
// At the fence the existing commit transfers its exact original operands into
// this storage and the SAME member's Finish. The seal owns unused capacity on
// cancellation, including its real pending metadata charge.
func (runtime *rootPublicationRuntimeV1) preparePrimarySealCommitStorageV6(seal *rootPublicationSealV1, w *iterator.OrdinalScanWork) (bool, error) {
	if runtime == nil || seal == nil || seal.primary == nil || seal.idx != runtime.idx || seal.metadata == nil || w == nil {
		return false, rootpublication.ErrDurableRootOwnership
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeSeal != seal || runtime.poison != nil || seal.released {
		return false, rootpublication.ErrDurableRootOwnership
	}
	return preparePrimarySealCommitStorageLockedV6(seal, runtime.idx.primary, len(runtime.seals), w)
}

// Caller holds runtime.mu. Private preparation reserves for its future SAME
// seal as well; later activation/fence validates that exact capacity again.
func preparePrimarySealCommitStorageLockedV6(seal *rootPublicationSealV1, arena *primaryarena.Arena, sealCount int, w *iterator.OrdinalScanWork) (bool, error) {
	if seal.commitRuntime == nil {
		bytes := uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))
		if !w.Reserve(1, bytes) {
			return false, nil
		}
		charge := primaryRuntimeDescriptorChargeV5()
		if ready, err := retainedalloc.AddPendingWithOrdinalWork(seal.metadata, charge, w); !ready || err != nil {
			return false, err
		}
		seal.commitRuntime = &primaryDurableRuntimeV5{}
		bindReservedPrimaryRuntimeV5(seal.commitRuntime, arena)
	}
	if seal.commitRetired == nil {
		count := sealCount
		if !w.Reserve(1, uint64(count)*uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil)))) {
			return false, nil
		}
		charge := retainedalloc.AllocationCharge(uint64(count) * uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil))))
		if ready, err := retainedalloc.AddPendingWithOrdinalWork(seal.metadata, charge, w); !ready || err != nil {
			return false, err
		}
		seal.commitRetired = make([]*rootPublicationSealV1, 0, count)
		seal.commitRetiredCharge = charge
	}
	// A later seal may join while this producer yields. Refuse before the
	// durable fence rather than reallocating or hiding a variable scan there.
	if cap(seal.commitRetired) < sealCount {
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	seal.commitStorageReady = true
	return true, nil
}

// abandonPrimarySealCommitStorageV6 detaches blank precommit storage only.
// Commit always removes these aliases before they own any physical operand.
func abandonPrimarySealCommitStorageV6(seal *rootPublicationSealV1, w *iterator.OrdinalScanWork) (bool, error) {
	if w != nil {
		if ready, err := seal.disposalCompletion.CompleteWithWorkV6(&seal.disposalRefund, w); !ready || err != nil {
			return false, err
		}
		if ready, err := seal.disposalRefund.CompletePendingWork(&seal.abandonedCommitOwner, &seal.abandonedCommitCharge, w); !ready || err != nil {
			return false, err
		}
	}
	if seal.commitRuntime == nil && seal.commitRetired == nil {
		return true, nil
	}
	if w != nil && !w.Reserve(1, uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))+uint64(cap(seal.commitRetired))*uint64(unsafe.Sizeof((*rootPublicationSealV1)(nil)))) {
		return false, nil
	}
	charge := seal.commitRetiredCharge
	if seal.commitRuntime != nil {
		// No record/resource has been installed into this blank descriptor.
		if seal.commitRuntime.current != nil || seal.commitRuntime.records != [2]primaryarena.Ref{} {
			return false, rootpublication.ErrDurableRootOwnership
		}
		charge += seal.commitRuntime.metadataCharge
		seal.commitRuntime.metadata, seal.commitRuntime.metadataCharge = nil, 0
		seal.commitRuntime = nil
	}
	clear(seal.commitRetired)
	seal.commitRetired, seal.commitRetiredCharge, seal.commitStorageReady = nil, 0, false
	if w == nil {
		seal.metadata.RemovePending(charge)
		return true, nil
	}
	seal.abandonedCommitOwner, seal.abandonedCommitCharge = seal.metadata, charge
	return seal.disposalRefund.CompletePendingWork(&seal.abandonedCommitOwner, &seal.abandonedCommitCharge, w)
}
