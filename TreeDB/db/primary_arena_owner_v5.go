package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"sync"
	"unsafe"
)

// primaryArenaOwnerV5 is independent of every DATA generation. References
// retain the actual bank mapping, allocator and namespace proof together.
// A replacement DATA pager never owns, recovers or swaps the companion file.
type primaryArenaOwnerV5 struct {
	mu sync.Mutex
	// Failed dependency custody is held by this same physical owner. A failed
	// lease never relinquishes its existing edge or becomes undiscoverable.
	failedDependencies      *primaryDependencyLeaseV5
	failedBankConstructions *primaryBankConstructionV5
	failedPagerOperations   *stablePagerOwnedOperations
	failedDataGenerations   *indexGen
	failedSnapshots         *Snapshot
	cleanupErr              error
	closeFailure            primaryCleanupFailureV5
	namespaceFailure        primaryCleanupFailureV5
	refs                    uint64
	arena                   *primaryarena.Arena
	namespaceMu             sync.Mutex
	namespaceParent         *os.File
	namespaceProof          *rootpublication.StableNamespaceCreationProof
	metadataCharge          uint64
	namespaceParentCharge   uint64
}

func newPrimaryArenaOwnerV5(a *primaryarena.Arena) (*primaryArenaOwnerV5, error) {
	if a == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryArenaOwnerV5{})))
	if err := a.MetadataOwner().AddPending(charge); err != nil {
		return nil, err
	}
	return &primaryArenaOwnerV5{refs: 1, arena: a, metadataCharge: charge}, nil
}
func (owner *primaryArenaOwnerV5) retain(work *iterator.OrdinalScanWork) (bool, error) {
	if work != nil && !work.Reserve(1, 2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))) {
		return false, nil
	}
	if owner == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.refs == 0 || owner.refs == ^uint64(0) {
		return false, rootpublication.ErrResourceOwnership
	}
	owner.refs++
	return true, nil
}

// primaryCleanupFailureV5 is embedded in the already admitted physical owner
// or lease. Linking error custody needs no failure-time backing allocation.
type primaryCleanupFailureV5 struct{ causes [3]error }

func (failure *primaryCleanupFailureV5) Error() string   { return "primary physical cleanup failed" }
func (failure *primaryCleanupFailureV5) Unwrap() []error { return failure.causes[:] }
func (owner *primaryArenaOwnerV5) release(work *iterator.OrdinalScanWork) (bool, error) {
	consumed, _, err := owner.releaseWithOutcome(work)
	return consumed, err
}

// consumed reports the exact owner-edge decrement independently from failure.
// physicalDebt distinguishes incomplete last physical cleanup from an earlier
// sibling failure reported after this caller's successful nonlast release.
func (owner *primaryArenaOwnerV5) releaseWithOutcome(work *iterator.OrdinalScanWork) (consumed, physicalDebt bool, err error) {
	if work != nil && !work.Reserve(1, 2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))) {
		return false, false, nil
	}
	if owner == nil {
		return false, false, rootpublication.ErrResourceOwnership
	}
	owner.mu.Lock()
	if owner.refs == 0 {
		owner.mu.Unlock()
		return false, false, rootpublication.ErrResourceOwnership
	}
	owner.refs--
	last, prior := owner.refs == 0, owner.cleanupErr
	owner.mu.Unlock()
	if !last {
		return true, false, prior
	}
	arenaErr := owner.arena.Close()
	namespaceErr := owner.clearNamespaceV5()
	if arenaErr == nil && namespaceErr == nil && prior == nil {
		arena, charge := owner.arena, owner.metadataCharge
		owner.arena = nil
		owner.metadataCharge = 0
		arena.MetadataOwner().RemovePending(charge)
		return true, false, nil
	}
	owner.closeFailure.causes = [3]error{arenaErr, namespaceErr, prior}
	owner.arena.MetadataOwner().CleanupFailed()
	return true, true, &owner.closeFailure
}

// clearNamespaceV5 retires only the exact current proof and parent. Failed
// cleanup stays in the existing owner and refuses replacement of that custody.
func (owner *primaryArenaOwnerV5) clearNamespaceV5() error {
	owner.namespaceMu.Lock()
	defer owner.namespaceMu.Unlock()
	var proofErr, parentErr error
	if owner.namespaceProof != nil {
		proofErr = owner.namespaceProof.ReleaseWithError()
		if proofErr == nil {
			owner.namespaceProof = nil
		}
	}
	if owner.namespaceParent != nil {
		closeErr := owner.namespaceParent.Close()
		if closeErr == nil || errors.Is(closeErr, os.ErrClosed) {
			owner.namespaceParent = nil
			owner.arena.MetadataOwner().RemovePending(owner.namespaceParentCharge)
			owner.namespaceParentCharge = 0
		} else {
			parentErr = closeErr
		}
	}
	if proofErr != nil || parentErr != nil {
		owner.arena.MetadataOwner().CleanupFailed()
		owner.namespaceFailure.causes = [3]error{proofErr, parentErr}
		return &owner.namespaceFailure
	}
	return nil
}

func (generation *indexGen) sharePrimaryArenaV5(source *indexGen) error {
	if generation == nil || source == nil || generation.primaryOwner != nil || source.primaryOwner == nil {
		return rootpublication.ErrResourceOwnership
	}
	owner := source.primaryOwner
	ready, err := owner.retain(nil)
	if !ready || err != nil {
		return errors.Join(err, rootpublication.ErrResourceOwnership)
	}
	if err = generation.pager.AttachPrimaryBankPager(owner.arena.Pager()); err != nil {
		_, closeErr := owner.release(nil)
		return errors.Join(err, closeErr)
	}
	generation.primaryOwner = owner
	generation.primary = owner.arena
	generation.zipper.SetPrimaryArena(owner.arena)
	generation.zipper.SetPrimaryConstructorV6(rootpublication.NewPrimaryConstructionTransactionV6)
	return nil
}

// Retain failure on the existing lease edge, with no new allocation or ref.
// The owning DB observes the error through its normal physical-owner release;
// the exact callback/tree/remaining bank custody stays reachable on that owner.
func (owner *primaryArenaOwnerV5) retainFailedDependency(lease *primaryDependencyLeaseV5, err error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !lease.failed {
		lease.failed = true
		lease.failedNext = owner.failedDependencies
		owner.failedDependencies = lease
		lease.failure.causes[2] = owner.cleanupErr
		owner.cleanupErr = err
	}
}

// The intrusive link consumes no failure-time storage and retains the exact
// failed generation rather than only its scalar error/charge.
func (owner *primaryArenaOwnerV5) retainFailedDataGeneration(g *indexGen) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	g.failedNext = owner.failedDataGenerations
	owner.failedDataGenerations = g
	g.closeFailure.causes[2] = owner.cleanupErr
	if g.closeErr != &g.closeFailure {
		g.closeFailure.causes[0] = g.closeErr
	}
	g.closeErr = &g.closeFailure
	owner.cleanupErr = g.closeErr
}

func (owner *primaryArenaOwnerV5) retainFailedSnapshot(snapshot *Snapshot, err error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	snapshot.primaryFailure.causes = [3]error{err, rootpublication.ErrResourceOwnership, owner.cleanupErr}
	snapshot.primaryFailedNext = owner.failedSnapshots
	owner.failedSnapshots = snapshot
	owner.cleanupErr = &snapshot.primaryFailure
	owner.arena.MetadataOwner().CleanupFailed()
}
