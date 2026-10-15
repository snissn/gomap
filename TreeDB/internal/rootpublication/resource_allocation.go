package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

// resourceAllocation is embedded in actual immutable resource nodes/backing.
// It owns only that allocation: physical token references are independent.
// Generic (nil-metadata) construction keeps its original diagnostic lifetime.
// Constructors must admit the full rounded containing allocation BEFORE make.
// Selected clones acquire each directly shared allocation edge; children are
// held by the containing node, so root retention requires no graph traversal.
type resourceAllocation struct {
	release resourceReleaseFrameV6
	owner   *retainedalloc.Owner
	charge  uint64
	refs    atomic.Int64
	failure originalReleaseFailure
}

// newResourceAllocation reserves both the containing allocation and this
// exact reference header before either allocation. The caller constructs the
// containing immutable node/backing only after this constructor succeeds.
func newResourceAllocation(owner *retainedalloc.Owner, containingBytes uint64) (*resourceAllocation, error) {
	return newResourceAllocationWithWorkV6(owner, containingBytes, nil)
}

func newResourceAllocationWithWorkV6(owner *retainedalloc.Owner, containingBytes uint64, w *iterator.OrdinalScanWork) (*resourceAllocation, error) {
	if owner == nil {
		return nil, nil
	}
	header := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceAllocation{})))
	if containingBytes > math.MaxUint64-header {
		return nil, retainedalloc.ErrCapacity
	}
	charge := containingBytes + header
	if err := retainedalloc.AddPendingWithOrdinalWork(owner, charge, w); err != nil {
		return nil, err
	}
	return resourceAllocationAdmittedV6(owner, charge), nil
}

// Called only after the exact header+containing charge was accepted on this
// SAME original owner. It creates no reservation or second lifetime authority.
func resourceAllocationAdmittedV6(owner *retainedalloc.Owner, charge uint64) *resourceAllocation {
	allocation := &resourceAllocation{owner: owner, charge: charge}
	allocation.refs.Store(1)
	return allocation
}
func (allocation *resourceAllocation) retainWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if allocation == nil || w == nil {
		return false, ErrResourceOwnership
	}
	for {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(allocation.refs))) {
			return false, nil
		}
		n := allocation.refs.Load()
		if n <= 0 || n == math.MaxInt64 {
			return false, ErrResourceOwnership
		}
		if allocation.refs.CompareAndSwap(n, n+1) {
			return true, nil
		}
	}
}

func (allocation *resourceAllocation) retain() bool {
	if allocation == nil {
		return true
	}
	for n := allocation.refs.Load(); n > 0 && n < math.MaxInt64; n = allocation.refs.Load() {
		if allocation.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
	return false
}

// drop returns the last-allocation edge without refunding it. The existing
// node/backing owner must first clear every outgoing allocation and borrowed
// reference. This separates real metadata alias custody from physical release.
func (allocation *resourceAllocation) drop() bool {
	if allocation == nil {
		return false
	}
	n := allocation.refs.Add(-1)
	if n < 0 {
		panic("unbalanced resource allocation reference")
	}
	return n == 0
}

func (allocation *resourceAllocation) refund() {
	if allocation == nil {
		return
	}
	if allocation.refs.Load() != 0 {
		panic("live resource allocation refund")
	}
	owner, charge := allocation.owner, allocation.charge
	allocation.owner, allocation.charge = nil, 0
	owner.RemovePending(charge)
}

// Embedded in the original admitted allocation. Returning this receipt keeps
// the exact containing payload reachable through its caller's existing error
// authority; it creates no cleanup owner or retry lifecycle.
type originalReleaseFailure struct {
	original any
	mu       sync.Mutex
	causes   [2]error
}

func (f *originalReleaseFailure) Error() string { return "original resource cleanup failed" }
func (f *originalReleaseFailure) Is(target error) bool {
	f.mu.Lock()
	causes := f.causes
	f.mu.Unlock()
	for _, cause := range causes {
		if cause != nil && errors.Is(cause, target) {
			return true
		}
	}
	return false
}
func (f *originalReleaseFailure) As(target any) bool {
	f.mu.Lock()
	causes := f.causes
	f.mu.Unlock()
	for _, cause := range causes {
		if cause != nil && errors.As(cause, target) {
			return true
		}
	}
	return false
}
func (f *originalReleaseFailure) LinkPriorCleanupError(prior error) {
	if prior == nil || prior == f || errors.Is(prior, f) {
		return
	}
	f.TryLinkPriorCleanupError(prior)
}
func (f *originalReleaseFailure) OriginalCleanupPriorAvailable() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.causes[1] == nil
}
func (f *originalReleaseFailure) TryLinkPriorCleanupError(prior error) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.causes[1] != nil || prior == nil || prior == f {
		return false
	}
	f.causes[1] = prior
	return true
}
func (a *resourceAllocation) cleanupResult() error {
	if a == nil {
		return nil
	}
	a.failure.mu.Lock()
	defer a.failure.mu.Unlock()
	if a.failure.causes[0] != nil {
		return &a.failure
	}
	return nil
}
func (a *resourceAllocation) cleanupFailed(original any, err error) error {
	if err == nil {
		return nil
	}
	a.failure.mu.Lock()
	if a.failure.causes[0] == nil {
		a.failure.original, a.failure.causes[0] = original, err
	}
	a.failure.mu.Unlock()
	a.owner.CleanupFailed()
	return &a.failure
}

// OriginalReleaseFailure is the original admitted receipt, embeddable by a
// containing producer that owns its outgoing cleanup operands. It is not a
// registry, cleanup owner or retry mechanism.
type OriginalReleaseFailure = originalReleaseFailure

func (f *originalReleaseFailure) RetainOriginalCleanupFailure(original any, cause error) error {
	if cause == nil {
		return nil
	}
	f.mu.Lock()
	if f.causes[0] == nil {
		f.original, f.causes[0] = original, cause
	}
	f.mu.Unlock()
	return f
}
