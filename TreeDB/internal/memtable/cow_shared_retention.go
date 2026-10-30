package memtable

import (
	"sync/atomic"
	"unsafe"
)

// COWSharedRetention owns one non-shrinking, independently admitted charge
// shared by explicitly retained edges. It provides no physical-resource,
// content, publication, or cleanup authority. The caller must establish that
// retainedCharge covers the actual lifetime of the backing it associates with
// this owner, and must release physical resources before its final edge.
// The allocation for this wrapper is admitted before it is constructed.
type COWSharedRetention struct {
	refs  atomic.Uint64
	lease *COWExternalLease
}

// NewCOWSharedRetention admits an already-rounded backing charge plus the
// wrapper's separate allocation. The allocator may be the original bounded
// maintenance allocator or an explicitly reserved child; it is never inferred
// from a borrowed source. On refusal no edge or wrapper is created.
func NewCOWSharedRetention(allocator interface {
	AdmitNativeBytes(uint64) (*COWExternalLease, error)
}, retainedCharge uint64) (*COWSharedRetention, error) {
	if allocator == nil {
		return nil, ErrCOWClosed
	}
	total, ok := cowAdd(retainedCharge, COWAllocationCharge(uint64(unsafe.Sizeof(COWSharedRetention{}))))
	if !ok {
		return nil, ErrCOWCapacity
	}
	lease, err := allocator.AdmitNativeBytes(total)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, ErrCOWClosed
	}
	owner := &COWSharedRetention{lease: lease}
	owner.refs.Store(1)
	return owner, nil
}

// Retain acquires one independent edge while some prior edge is still alive.
// It never resurrects a fully released owner or grows the admitted charge.
func (owner *COWSharedRetention) Retain() bool {
	if owner == nil {
		return false
	}
	for {
		old := owner.refs.Load()
		if old == 0 || old == ^uint64(0) {
			return false
		}
		if owner.refs.CompareAndSwap(old, old+1) {
			return true
		}
	}
}

// Release consumes one caller-owned edge. The final edge refunds the charge,
// including after the budget closes. Each successfully retained edge must be
// released exactly once; Release is not idempotent for a particular edge.
// Clear backing aliases and complete resource cleanup before the final release.
// A failed cleanup retains its original edge and charge, even after Close.
func (owner *COWSharedRetention) Release() {
	if owner == nil {
		return
	}
	for {
		old := owner.refs.Load()
		if old == 0 {
			return
		}
		if owner.refs.CompareAndSwap(old, old-1) {
			if old == 1 {
				owner.lease.Close()
			}
			return
		}
	}
}
