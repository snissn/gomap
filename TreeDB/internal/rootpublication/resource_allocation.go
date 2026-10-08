package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
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
	owner  *retainedalloc.Owner
	charge uint64
	refs   atomic.Int64
}

// newResourceAllocation reserves both the containing allocation and this
// exact reference header before either allocation. The caller constructs the
// containing immutable node/backing only after this constructor succeeds.
func newResourceAllocation(owner *retainedalloc.Owner, containingBytes uint64) (*resourceAllocation, error) {
	if owner == nil {
		return nil, nil
	}
	header := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceAllocation{})))
	if containingBytes > math.MaxUint64-header {
		return nil, retainedalloc.ErrCapacity
	}
	charge := containingBytes + header
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	allocation := &resourceAllocation{owner: owner, charge: charge}
	allocation.refs.Store(1)
	return allocation, nil
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
