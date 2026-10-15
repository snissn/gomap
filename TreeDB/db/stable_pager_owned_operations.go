package db

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/pager"
)

// This is the actual private pager/Snapshot callback environment. Descriptor
// clones retain this same backing, rather than copying opaque function values.
// Its physical-owner edge protects cleanup/error custody through DB teardown.
type stablePagerOwnedOperations struct {
	refs              atomic.Int64
	completionMu      sync.Mutex
	snapshotCompleted bool
	completionErr     error
	disposalClaimed   bool
	disposalConsumed  atomic.Bool
	transferred       atomic.Bool
	snapshot          *Snapshot
	pager             *pager.Pager
	syncPath          string
	counter           *atomic.Int64
	owner             *primaryArenaOwnerV5
	metadata          *retainedalloc.Owner
	originalCustody   *rootpublication.OriginalCleanupCustody
	charge            uint64
	failure           primaryCleanupFailureV5
	failedNext        *stablePagerOwnedOperations
	// Nil on the ordinary producer. Native controls live in the actual
	// admitted publication frame through this SAME callback's physical Finish.
	native *nativeStablePagerCaptureV6
}

func newStablePagerOwnedOperations(snapshot *Snapshot, p *pager.Pager, metadata *retainedalloc.Owner, syncPath string) (*stablePagerOwnedOperations, error) {
	if snapshot == nil || snapshot.idx == nil || snapshot.db == nil || metadata == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	owner := snapshot.idx.primaryOwner
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(stablePagerOwnedOperations{}))) + retainedalloc.AllocationCharge(uint64(len(syncPath)))
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
	operations := &stablePagerOwnedOperations{snapshot: snapshot, pager: p, syncPath: strings.Clone(syncPath), counter: snapshot.stableIndexCaptureCounter, owner: owner, metadata: metadata, originalCustody: custody, charge: charge}
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
	return operations.pager.SyncIndexDataWithStableFileAtPath(file, operations.syncPath)
}
func (operations *stablePagerOwnedOperations) Release() (consumed bool, err error) {
	// The retained producer owns the last action. Ordinary constructor unwind
	// may drop an independent nonlast edge, but cannot drain native custody.
	if operations.native != nil {
		for n := operations.refs.Load(); ; n = operations.refs.Load() {
			if n <= 1 {
				return false, rootpublication.ErrNativeMVCCRootWorkRequiredV6
			}
			if operations.refs.CompareAndSwap(n, n-1) {
				return true, nil
			}
		}
	}
	n := operations.refs.Add(-1)
	if n < 0 {
		panic("unbalanced pager callback backing")
	}
	if n != 0 {
		return true, nil
	}
	operations.completionMu.Lock()
	if !operations.transferred.Load() && !operations.snapshotCompleted {
		operations.snapshot, operations.counter = nil, nil
		operations.snapshotCompleted = true
	}
	if operations.snapshotCompleted {
		claimed := !operations.disposalClaimed
		operations.disposalClaimed = true
		prior := operations.completionErr
		operations.completionMu.Unlock()
		if claimed {
			err := operations.disposeAfterSnapshotCompletion()
			operations.completionMu.Lock()
			if operations.completionErr == nil {
				operations.completionErr = err
			}
			prior = operations.completionErr
			operations.completionMu.Unlock()
		}
		if !claimed {
			// The descriptor reference is consumed. The SAME Snapshot finalizer and
			// original custody retain the in-flight physical action and its outcome.
			return true, prior
		}
		return operations.disposalConsumed.Load(), prior
	}
	snapshot := operations.snapshot
	operations.completionMu.Unlock()
	// An overlapping read retains the SAME original completion edge. This
	// descriptor consumed its independent reference; actual asynchronous debt
	// remains in that original Snapshot/operations custody.
	err = snapshot.Close()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Called by the original Snapshot finalizer after successful handle scrub.
// Return the actual available disposal outcome; surviving descriptor references
// own their independent later physical disposal without retaining Snapshot aliases.
func (operations *stablePagerOwnedOperations) snapshotFinalized(err error) error {
	operations.completionMu.Lock()
	if operations.snapshotCompleted {
		err := operations.completionErr
		operations.completionMu.Unlock()
		return err
	}
	operations.snapshotCompleted = true
	operations.completionErr = err
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
		if disposalErr := operations.disposeAfterSnapshotCompletion(); err == nil {
			err = disposalErr
		}
	}
	operations.completionMu.Lock()
	operations.completionErr = err
	operations.completionMu.Unlock()
	return err
}

func (operations *stablePagerOwnedOperations) disposeAfterSnapshotCompletion() error {
	owner, metadata, charge := operations.owner, operations.MetadataOwner(), operations.charge
	operations.pager = nil
	operations.syncPath = ""
	if operations.failure.causes[0] != nil {
		if owner != nil {
			owner.retainFailedPagerOperations(operations)
		}
		operations.originalCustody.Complete(operations, &operations.failure)
		metadata.CleanupFailed()
		return &operations.failure
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
		return &operations.failure
	}
	operations.originalCustody.Complete(nil, nil)
	operations.originalCustody = nil
	operations.owner = nil
	operations.disposalConsumed.Store(true)
	operations.metadata = nil
	operations.charge = 0
	metadata.RemovePending(charge)
	return err
}
func (owner *primaryArenaOwnerV5) retainFailedPagerOperations(operations *stablePagerOwnedOperations) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	operations.failedNext = owner.failedPagerOperations
	owner.failedPagerOperations = operations
	operations.failure.causes[2] = owner.cleanupErr
	owner.cleanupErr = &operations.failure
}
