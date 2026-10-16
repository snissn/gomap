package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// Match the original physical pager's eligibility before a directory transfers
// its last reference to native cleanup. Ordinary disposal remains available.
func (lease *primaryDependencyLeaseV5) SupportsBoundedDependencyDirectoryReleaseV6() bool {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		// Eligibility only: the directory's cached cleanupConsumed/failure
		// still distinguishes actual consumption from sticky original debt.
		return true
	}
	// The live lease owns an arena edge; another owner borrower cannot close
	// or refund this physical owner while lease.mu protects its transfer.
	return lease.owner != nil && lease.owner.arena != nil && lease.owner.arena.Pager().SupportsBoundedPhysicalClose()
}

// The SAME admitted lease carries original physical progress, then returns its
// containing charge by value after unlocking. Its directory clears the typed
// provider alias before actual external removal.
func (lease *primaryDependencyLeaseV5) ReleaseDependencyDirectoryWorkV6(work *iterator.OrdinalScanWork) (consumed, ready bool, completion rootpublication.DependencyDirectoryReleaseCompletionV6, err error) {
	lease.mu.Lock()
	if lease.closed {
		consumed, err = !lease.failed, lease.cleanupErr
		lease.mu.Unlock()
		return consumed, true, completion, err
	}
	if lease.releasePhase != 0 && work == nil && lease.releaseNative {
		lease.mu.Unlock()
		return false, true, completion, ErrNativeMVCCSnapshotWork
	}
	ordinaryWork := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	w := work
	if w == nil {
		w = &ordinaryWork
	}
	if lease.releasePhase == 0 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*lease))+2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))) {
			lease.mu.Unlock()
			return false, false, completion, nil
		}
		lease.releaseMetadata = lease.owner.arena.MetadataOwner()
		lease.releaseNative = work != nil
		lease.releasePhase = 1
	}
	fail := func(cause error) (bool, bool, rootpublication.DependencyDirectoryReleaseCompletionV6, error) {
		if cause == nil {
			cause = rootpublication.ErrResourceOwnership
		}
		lease.closed = true
		lease.failure.causes = [3]error{cause, rootpublication.ErrResourceOwnership}
		lease.cleanupErr = &lease.failure
		lease.releaseMetadata.CleanupFailed()
		custodyOwner := lease.owner
		if custodyOwner == nil {
			custodyOwner = lease.releaseWork.owner
		}
		lease.owner = custodyOwner
		custodyOwner.retainFailedDependency(lease, lease.cleanupErr)
		result := lease.cleanupErr
		lease.mu.Unlock()
		return false, true, rootpublication.DependencyDirectoryReleaseCompletionV6{}, result
	}
	if lease.releasePhase == 1 {
		if lease.ref.PageID != 0 {
			ready, err := lease.owner.arena.Drop(lease.ref, w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				lease.mu.Unlock()
				return false, false, completion, nil
			}
			lease.ref = primaryarena.Ref{}
		}
		lease.releasePhase = 2
	}
	if lease.releasePhase == 2 {
		ready, err := bindPrimaryArenaRelease(&lease.releaseWork.primaryArenaReleaseState, lease.owner, lease.releaseNative, w)
		if err != nil {
			return fail(err)
		}
		if !ready {
			lease.mu.Unlock()
			return false, false, completion, nil
		}
		lease.ownerReleaseConsumed = true
		// The SAME external physical receipt now owns the last arena edge.
		// Clear the caller alias before its terminal containing refund.
		lease.owner = nil
		lease.releasePhase = 3
	}
	if lease.releasePhase == 3 {
		ready, debt, err := continuePrimaryArenaRelease(&lease.releaseWork.primaryArenaReleaseState, &lease.releaseWork.proof, &lease.releaseWork.parentRefund, &lease.releaseWork.ownerRefund, w)
		if !ready && err == nil {
			lease.mu.Unlock()
			return false, false, completion, nil
		}
		if debt || !ready {
			return fail(err)
		}
		lease.cleanupErr = err
		lease.releasePhase = 4
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*lease))) {
		lease.mu.Unlock()
		return false, false, completion, nil
	}
	completion = rootpublication.DependencyDirectoryReleaseCompletionV6{Metadata: lease.releaseMetadata, Charge: lease.metadataCharge}
	lease.metadataCharge = 0
	lease.releaseMetadata = nil
	lease.owner = nil
	lease.directory = nil
	lease.closed = true
	err = lease.cleanupErr
	lease.mu.Unlock()
	return true, true, completion, err
}
