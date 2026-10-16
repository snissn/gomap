package db

import (
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// consumeNativeVisibleMemberV6 removes only the exact current head and moves
// its original record edge to the SAME member's Finish. No root/bank/resource
// physical release can run in the fenced coordinator turn.
func (runtime *rootPublicationRuntimeV1) consumeNativeVisibleMemberV6(
	member *rootPublicationVisibleMemberV1, cow *freelist.PreparedCOWCandidateV1,
	input rootpublication.DurableRootCallbackInput, w *iterator.OrdinalScanWork,
) (bool, error) {
	if runtime == nil || member == nil || w == nil || input.PreparedCOW != cow ||
		input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
		return false, rootpublication.ErrDurableRootOwnership
	}
	// Exact input comparisons, head probes, old/new pointer slots and scalar
	// frontier writes are all admitted BEFORE consuming any original edge.
	if !w.Reserve(3, 2*uint64(unsafe.Sizeof(input))+6*uint64(unsafe.Sizeof(member))+4*8) {
		return false, nil
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.visibleHead >= len(runtime.visibleMembers) ||
		runtime.visibleMembers[runtime.visibleHead] != member ||
		member.retiredRecord != (primaryarena.Ref{}) || member.retiredRecordQueueInitialized {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if member.primary != nil && !member.primaryRecordTransferred {
		member.retiredRecord = member.primary.Bundle().Record
	}
	// Keep ordinary non-head compaction out of this native fixed-work transition.
	runtime.visibleMembers[runtime.visibleHead] = nil
	runtime.visibleHead++
	return true, nil
}

// activateNativeVisibleMemberV6 is the work-bearing form of the same original
// member callback. Capacity/header preparation belongs before enqueue. The
// caller must admit this whole activation/fence/Consume/turn-release window:
// no false,nil from this function authorizes a public yield after enqueue.
func (runtime *rootPublicationRuntimeV1) activateNativeVisibleMemberV6(
	member *rootPublicationVisibleMemberV1, cow *freelist.PreparedCOWCandidateV1,
	input rootpublication.DurableRootCallbackInput, w *iterator.OrdinalScanWork,
) (bool, error) {
	if runtime == nil || member == nil || member.install == nil || w == nil ||
		input.PreparedCOW != cow || input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
		return false, rootpublication.ErrDurableRootOwnership
	}
	if ready, err := member.install.validateNativePostActivationV6(w); !ready || err != nil {
		return false, err
	}
	runtime.mu.Lock()
	if member.retiredVisibleResources != nil ||
		len(runtime.visibleMembers) == cap(runtime.visibleMembers) ||
		(member.prepared != nil && len(runtime.debt) == cap(runtime.debt)) {
		runtime.mu.Unlock()
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	if member.preparedLimits != nil &&
		(len(runtime.visibleMembers)-runtime.visibleHead >= member.preparedLimits.MaxVisibleMembers ||
			len(runtime.debt) >= member.preparedLimits.MaxAllocatorDebt) {
		runtime.mu.Unlock()
		return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
	}
	runtime.mu.Unlock()
	// No append allocation or resource release remains after the original
	// install crosses allocator/DB visibility. This reservation includes exact
	// original pointer slots and slice/scalar header updates.
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(input))+12*uint64(unsafe.Sizeof(member))) {
		return false, nil
	}
	// The accepted borrowed proof is published only after successful visibility.
	// Debit its exact fixed header/slot traffic before adoption can take effect.
	r, b := nativeAcceptedDataCertificateWorkV6()
	if !w.Reserve(r, b) {
		return false, nil
	}
	ready, err := member.install.activateWithWorkV6(w, func(work *iterator.OrdinalScanWork) (bool, error) {
		if member.prepared == nil {
			return true, nil
		}
		if member.allocatorActivation == nil {
			return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
		}
		if member.allocatorPreparation != nil {
			prepared, ready, err := member.allocatorPreparation.AdoptAndActivateV6(member.allocatorActivation, work)
			if ready && err == nil && prepared != member.prepared {
				return false, rootpublication.ErrDurableRootOwnership
			}
			return ready, err
		}
		return runtime.idx.allocator.ActivateCOWCandidateWithWorkV1(member.prepared, member.allocatorActivation, work)
	})
	if !ready || err != nil {
		return false, err
	}
	runtime.mu.Lock()
	if member.primary != nil && member.primary.ReadCertificateV6() != nil {
		runtime.primaryDataCertificateV6 = member.primary.DataCertificateV6()
	}
	member.retiredVisibleResources = runtime.visibleResources
	runtime.visibleResources = member.resources
	runtime.visibleMembers = append(runtime.visibleMembers, member)
	if member.prepared != nil {
		runtime.debt = append(runtime.debt, member.prepared)
	}
	runtime.mu.Unlock()
	return true, nil
}

// Two fixed operations: validate the accepted read-certificate operand, then
// copy its borrowed DATA certificate to the runtime cache. The original root
// owns that proof; no independent retained header or refund is introduced.
func nativeAcceptedDataCertificateWorkV6() (records, bytes uint64) {
	return 2, 2*uint64(unsafe.Sizeof(rootpublication.PreparedPrimaryRootV5{})) + 3*uint64(unsafe.Sizeof((*rootpublication.PrimaryDataCertificateV6)(nil)))
}
