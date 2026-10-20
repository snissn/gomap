package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Retained by independent bounded physical release helpers after Q32 Store
// capture retirement. This is a real cleanup capability refusal.
var ErrNativeMVCCSnapshotWork = errors.New("native MVCC snapshot requires original work-bearing capture/cleanup")

// The SAME admitted lease disposes its original root and owner edge. The
// directory clears its provider alias before consuming the containing refund.
func (lease *primaryDependencyLeaseV5) ReleaseDependencyDirectoryV6() (consumed bool, completion rootpublication.DependencyDirectoryReleaseCompletionV6, err error) {
	lease.mu.Lock()
	if lease.closed {
		consumed, err = !lease.failed, lease.cleanupErr
		lease.mu.Unlock()
		return consumed, completion, err
	}
	metadata := lease.owner.arena.MetadataOwner()
	fail := func(cause error) (bool, rootpublication.DependencyDirectoryReleaseCompletionV6, error) {
		if cause == nil {
			cause = rootpublication.ErrResourceOwnership
		}
		lease.closed = true
		lease.failure.causes = [3]error{cause, rootpublication.ErrResourceOwnership}
		lease.cleanupErr = &lease.failure
		metadata.CleanupFailed()
		lease.owner.retainFailedDependency(lease, lease.cleanupErr)
		result := lease.cleanupErr
		lease.mu.Unlock()
		return false, rootpublication.DependencyDirectoryReleaseCompletionV6{}, result
	}
	if lease.ref.PageID != 0 {
		ready, err := lease.owner.arena.Drop(lease.ref, nil)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return fail(rootpublication.ErrResourceOwnership)
		}
		lease.ref = primaryarena.Ref{}
	}
	owner := lease.owner
	consumed, debt, ownerCompletion, err := owner.releaseOriginalWithOutcome()
	lease.ownerReleaseConsumed = consumed
	if !consumed || debt {
		return fail(err)
	}
	// End both aliases before refunding the physical owner. The lease remains
	// charged until the containing directory releases its provider alias.
	owner, lease.owner = nil, nil
	lease.cleanupErr = err
	lease.directory = nil
	lease.closed = true
	completion = rootpublication.DependencyDirectoryReleaseCompletionV6{Metadata: metadata, Charge: lease.metadataCharge}
	lease.metadataCharge = 0
	lease.mu.Unlock()
	ownerCompletion.complete()
	return true, completion, err
}
