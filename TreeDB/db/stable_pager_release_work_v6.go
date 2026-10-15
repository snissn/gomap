package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// maintenanceOnlyLocked checks the actual producer's Snapshot, rather than its
// index kind or a current refcount. Logical readers retain their existing close
// authority and are not enrolled as new native captures.
func (o *stablePagerOwnedOperations) maintenanceOnlyLocked() bool {
	s := o.snapshot
	return s != nil && s.db != nil && s.idx != nil && s.stableIndexCapture &&
		s.stableIndexCaptureTransferred && s.stablePagerCompletion == o &&
		!s.nativeMVCC.workOwned && s.state == nil && s.primaryRoot == nil &&
		s.primaryOwner == nil && s.primaryMetadata == nil && s.registryID == 0 &&
		s.vlogManager == nil && !s.vlogPinned && !s.vlogWrapperBorrowed &&
		s.foregroundReadEnd == nil && s.cleanupCompletion == nil &&
		len(s.iterators) == 0 && cap(s.rootTrees) == 0 && cap(s.leafGenerationRefs) == 0 &&
		s.leafGenerationPinSet == nil && len(s.leafGenerationIDs) == 0 &&
		len(s.leafGenerationPinnedIDs) == 0
}
func (o *stablePagerOwnedOperations) SupportsReleaseWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if o == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*o))) {
		return false, nil
	}
	o.completionMu.Lock()
	defer o.completionMu.Unlock()
	if o.completionErr != nil {
		return false, o.completionErr
	}
	if o.releasePhase != 0 || o.refs.Load() <= 0 || o.snapshot == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	s := o.snapshot
	s.iteratorMu.Lock()
	defer s.iteratorMu.Unlock()
	if !o.maintenanceOnlyLocked() || s.closed.Load() || s.readState.Load() != 0 {
		return false, ErrNativeMVCCSnapshotWork
	}
	return true, nil
}

// ReleaseWithWorkV6 consumes exactly the caller's original descriptor edge.
// The last edge retains its original Snapshot/owner/custody through all phases.
// Accounting is returned by value only after physical aliases have detached;
// the token clears its producer alias before consuming those bytes.
func (o *stablePagerOwnedOperations) ReleaseWithWorkV6(consumed *bool, w *iterator.OrdinalScanWork) (*retainedalloc.Owner, uint64, bool, error) {
	if o == nil || consumed == nil || w == nil {
		return nil, 0, false, ErrNativeMVCCSnapshotWork
	}
	o.completionMu.Lock()
	defer o.completionMu.Unlock()
	fail := func(err error) (*retainedalloc.Owner, uint64, bool, error) {
		if o.completionErr == nil {
			o.failure.causes[0] = err
			if o.owner != nil {
				o.owner.retainFailedPagerOperations(o)
			}
			o.originalCustody.Complete(o, &o.failure)
			o.metadata.CleanupFailed()
			o.completionErr = &o.failure
		}
		return nil, 0, false, o.completionErr
	}
	if o.completionErr != nil {
		return nil, 0, false, o.completionErr
	}
	if !*consumed {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*o))) {
			return nil, 0, false, nil
		}
		if o.refs.Load() <= 0 || o.releasePhase != 0 {
			return nil, 0, false, rootpublication.ErrResourceOwnership
		}
		n := o.refs.Add(-1)
		*consumed = true
		if n > 0 {
			return nil, 0, true, nil
		}
		o.disposalClaimed = true
		o.releasePhase = 1
	}
	if o.releasePhase == 0 {
		return nil, 0, true, nil
	} // this caller's nonlast edge
	if o.releasePhase == 1 {
		s := o.snapshot
		if s == nil {
			return fail(rootpublication.ErrResourceOwnership)
		}
		s.iteratorMu.Lock()
		if !o.maintenanceOnlyLocked() || s.closed.Load() || s.readState.Load() != 0 {
			s.iteratorMu.Unlock()
			return fail(ErrNativeMVCCSnapshotWork)
		}
		idx, db := s.idx, s.db
		// A stable maintenance capture excludes index replacement until its exact
		// counter is released. Do not silently turn a retired generation into a
		// new supported finalizer or assume a wrapper borrow protects physical close.
		idx.wrapperMu.Lock()
		if idx.physicalCompleted || db.idx.Load() != idx || !s.indexWrapperBorrowed || idx.wrapperBorrows == 0 {
			idx.wrapperMu.Unlock()
			s.iteratorMu.Unlock()
			return fail(ErrNativeMVCCSnapshotWork)
		}
		if !w.Reserve(3, 4*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*o))+128) {
			idx.wrapperMu.Unlock()
			s.iteratorMu.Unlock()
			return nil, 0, false, nil
		}
		s.closed.Store(true)
		s.readState.Store(snapshotReadClosedBit)
		s.finalized.Store(true)
		idx.wrapperBorrows--
		s.indexWrapperBorrowed = false
		idx.wrapperMu.Unlock()
		s.stablePagerCompletion = nil
		s.stableIndexCapture = false
		s.stableIndexCaptureTransferred = false
		s.stableIndexCaptureCounter = nil
		s.iteratorMu.Unlock()
		// This is the same original scrub used by Snapshot.Close. Eligibility above
		// proves its variable-capacity loops are empty, and the full handle is admitted.
		db.snapPool.Put(s)
		s.iteratorMu.Lock()
		s.cleanupFinished = true
		s.cleanupErr = nil
		s.iteratorMu.Unlock()
		o.snapshot = nil
		o.snapshotCompleted = true
		o.transferred.Store(false)
		if o.counter != nil {
			o.counter.Add(-1)
			o.counter = nil
		}
		o.releasePhase = 2
	}
	if o.releasePhase == 2 {
		if o.owner != nil {
			ready, err := o.releaseOwner.Bind(o.owner, w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return nil, 0, false, nil
			}
		}
		o.releasePhase = 3
	}
	if o.releasePhase == 3 {
		if o.owner != nil {
			ready, debt, err := o.releaseOwner.ContinueWork(w)
			if err != nil && debt {
				return fail(err)
			}
			if !ready {
				return nil, 0, false, err
			}
			// Nonlast ordinary sibling debt stays with that original authority.
		}
		o.releasePhase = 4
	}
	if o.releasePhase == 4 {
		// CompleteWithWorkV6 refuses a shutdown requiring unbound registry table
		// disposal before consuming this original custody; ordinary Close owns it.
		completion, ready, err := o.originalCustody.CompleteWithWorkV6(w)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return nil, 0, false, nil
		}
		o.originalCustody = nil
		completion.Complete()
		o.releasePhase = 5
	}
	if o.releasePhase == 5 {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*o))) {
			return nil, 0, false, nil
		}
		owner, charge := o.metadata, o.charge
		o.pager, o.owner, o.metadata = nil, nil, nil
		o.charge = 0
		o.disposalConsumed.Store(true)
		o.releasePhase = 6
		return owner, charge, true, nil
	}
	if o.releasePhase == 6 {
		return nil, 0, true, nil
	}
	return nil, 0, false, rootpublication.ErrResourceOwnership
}
