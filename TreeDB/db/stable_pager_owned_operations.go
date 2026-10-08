package db

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

// This is the actual private pager/Snapshot callback environment. Descriptor
// clones retain this same backing, rather than copying opaque function values.
// Its physical-owner edge protects cleanup/error custody through DB teardown.
type stablePagerOwnedOperations struct {
	refs              atomic.Int64
	completionMu      sync.Mutex
	snapshotCompleted bool
	disposalClaimed   bool
	transferred       atomic.Bool
	snapshot          *Snapshot
	pager             *pager.Pager
	counter           *atomic.Int64
	owner             *primaryArenaOwnerV5
	metadata          *retainedalloc.Owner
	originalCustody   *rootpublication.OriginalCleanupCustody
	charge            uint64
	failure           primaryCleanupFailureV5
	failedNext        *stablePagerOwnedOperations
}

func newStablePagerOwnedOperations(snapshot *Snapshot, p *pager.Pager, metadata *retainedalloc.Owner) (*stablePagerOwnedOperations, error) {
	if snapshot == nil || snapshot.idx == nil || snapshot.db == nil || metadata == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	owner := snapshot.idx.primaryOwner
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stablePagerOwnedOperations{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	custody, err := rootpublication.NewOriginalCleanupCustody(snapshot.db.StableResourceIdentityPinRegistry(), metadata)
	if err != nil {
		metadata.RemovePending(charge)
		return nil, err
	}
	if owner != nil {
		if ok, err := owner.retain(nil); !ok || err != nil {
			custody.Complete(nil, nil)
			metadata.RemovePending(charge)
			if err == nil {
				err = rootpublication.ErrResourceOwnership
			}
			return nil, err
		}
	}
	operations := &stablePagerOwnedOperations{snapshot: snapshot, pager: p, counter: snapshot.stableIndexCaptureCounter, owner: owner, metadata: metadata, originalCustody: custody, charge: charge}
	operations.refs.Store(1)
	return operations, nil
}
func (operations *stablePagerOwnedOperations) MetadataOwner() *retainedalloc.Owner {
	return operations.metadata
}
func (operations *stablePagerOwnedOperations) Retain() error {
	for n := operations.refs.Load(); n > 0 && n < int64(^uint64(0)>>1); n = operations.refs.Load() {
		if operations.refs.CompareAndSwap(n, n+1) {
			return nil
		}
	}
	return rootpublication.ErrResourceOwnership
}
func (operations *stablePagerOwnedOperations) FlushThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func (operations *stablePagerOwnedOperations) SyncThrough(file *os.File, _ rootpublication.DurableFrontier) error {
	return operations.pager.SyncIndexDataWithStableFile(file)
}
func (operations *stablePagerOwnedOperations) Release() {
	n := operations.refs.Add(-1)
	if n < 0 {
		panic("unbalanced pager callback backing")
	}
	if n != 0 {
		return
	}
	operations.completionMu.Lock()
	if !operations.transferred.Load() && !operations.snapshotCompleted {
		// Failed construction never transferred the caller's Snapshot edge.
		operations.snapshot = nil
		operations.counter = nil
		operations.snapshotCompleted = true
	}
	if operations.snapshotCompleted {
		claimed := !operations.disposalClaimed
		operations.disposalClaimed = true
		operations.completionMu.Unlock()
		if claimed {
			operations.disposeAfterSnapshotCompletion()
		}
		return
	}
	snapshot := operations.snapshot
	operations.completionMu.Unlock()
	// Close only requests retirement. The actual finalizer owns completion,
	// including an overlapping last read and all of its original error outcome.
	_ = snapshot.Close()
}

// Called by the original Snapshot finalizer after successful handle scrub. False retains
// the failed original wrapper with the already admitted failure owner instead
// of returning an alias which still represents cleanup debt to the pool.
func (operations *stablePagerOwnedOperations) snapshotFinalized(err error) bool {
	operations.completionMu.Lock()
	if operations.snapshotCompleted {
		operations.completionMu.Unlock()
		return err == nil
	}
	operations.snapshotCompleted = true
	operations.failure.causes[0] = err
	if err == nil {
		operations.snapshot = nil
	}
	counter := operations.counter
	operations.counter = nil
	operations.transferred.Store(false)
	claimed := operations.refs.Load() == 0 && !operations.disposalClaimed
	if claimed {
		operations.disposalClaimed = true
	}
	operations.completionMu.Unlock()
	if counter != nil {
		counter.Add(-1)
	}
	if claimed {
		operations.disposeAfterSnapshotCompletion()
	}
	return err == nil
}

func (operations *stablePagerOwnedOperations) disposeAfterSnapshotCompletion() {
	owner, metadata, charge := operations.owner, operations.MetadataOwner(), operations.charge
	operations.pager = nil
	if operations.failure.causes[0] != nil {
		if owner != nil {
			owner.retainFailedPagerOperations(operations)
		}
		operations.originalCustody.Complete(operations, &operations.failure)
		metadata.CleanupFailed()
		return
	}
	consumed, debt, err := true, false, error(nil)
	if owner != nil {
		consumed, debt, err = owner.releaseWithOutcome(nil)
	}
	if !consumed || debt {
		operations.failure.causes[0] = err
		if owner != nil {
			owner.retainFailedPagerOperations(operations)
		}
		operations.originalCustody.Complete(operations, &operations.failure)
		metadata.CleanupFailed()
		return
	}
	operations.originalCustody.Complete(nil, nil)
	operations.originalCustody = nil
	operations.owner = nil
	operations.metadata = nil
	operations.charge = 0
	metadata.RemovePending(charge)
}
func (owner *primaryArenaOwnerV5) retainFailedPagerOperations(operations *stablePagerOwnedOperations) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	operations.failedNext = owner.failedPagerOperations
	owner.failedPagerOperations = operations
	operations.failure.causes[2] = owner.cleanupErr
	owner.cleanupErr = &operations.failure
}
