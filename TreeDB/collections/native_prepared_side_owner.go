package collections

import (
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// nativePreparedSideBudget meters a single metadata owner against an already
// admitted, finite side reservation. Resize never calls the COW allocator while
// retainedalloc holds its owner lock. This budget does not govern the original
// registry or producer; doing so could make their subsequent allocations fail.
type nativePreparedSideBudget struct {
	mu     sync.Mutex
	limit  uint64
	used   uint64
	active bool
	closed bool
	lease  nativePreparedSideLease
}

type nativePreparedSideLease struct{ budget *nativePreparedSideBudget }

func (b *nativePreparedSideBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.active || n > b.limit {
		return nil, retainedalloc.ErrCapacity
	}
	b.active, b.used = true, n
	b.lease.budget = b
	return &b.lease, nil
}

func (l *nativePreparedSideLease) Resize(n uint64) error {
	if l == nil || l.budget == nil {
		return retainedalloc.ErrClosed
	}
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.active || b.closed {
		return retainedalloc.ErrClosed
	}
	if n > b.limit {
		return retainedalloc.ErrCapacity
	}
	b.used = n
	return nil
}

func (l *nativePreparedSideLease) Close() {
	if l == nil || l.budget == nil {
		return
	}
	b := l.budget
	b.mu.Lock()
	b.active, b.closed, b.used = false, true, 0
	b.mu.Unlock()
}

// nativePreparedSideOwner is an unconnected ownership building block for an
// observational capture. It provides no physical-resource, snapshot, cut,
// publication or graph authority. The enclosing producer must separately prove
// and charge every input, registry effect and retained operand before use.
// Access to metadata and completion is serialized by the caller; this type is
// not a graph-join or concurrent ownership protocol.
//
// An independent reservation from the caller's COW budget is acquired before
// the containing owner is allocated. It does not split or resize a build lease;
// it still competes for unused maintenance capacity, so connecting it requires
// proof that the original build and registry retain their progress capacity.
// A successful side owner holds the reservation
// until every actual side allocation is gone or a real cleanup failure has
// been transferred to the original registry's preborn custody.
type nativePreparedSideOwner struct {
	metadata   retainedalloc.Owner
	budget     nativePreparedSideBudget
	retention  *memtable.COWSharedRetention
	enrollment *retainedalloc.Enrollment
	custody    *rootpublication.OriginalCleanupCustody
	idle       uint64
	finished   bool
	failure    error

	// A future caller that transfers an actual failed operand here must have
	// independently admitted its full backing before assigning it. The custody
	// retains this containing owner on failure; it is never a generic uncharged
	// payload or an instruction to retry cleanup.
	failedOperand any
}

func newNativePreparedSideOwner(allocator *memtable.COWBudget, registry *rootpublication.IdentityPinRegistry, limit uint64) (*nativePreparedSideOwner, error) {
	ownerCharge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(nativePreparedSideOwner{})))
	if allocator == nil || registry == nil || limit < ownerCharge {
		return nil, retainedalloc.ErrCapacity
	}
	retention, err := memtable.NewCOWSharedRetention(allocator, limit)
	if err != nil {
		return nil, err
	}
	owner := &nativePreparedSideOwner{retention: retention}
	owner.budget.limit = limit
	owner.metadata.Initialize(ownerCharge)
	owner.enrollment, err = owner.metadata.Enroll(&owner.budget)
	if err != nil {
		_ = owner.metadata.Close()
		owner.retention.Release()
		return nil, err
	}
	owner.custody, err = rootpublication.NewOriginalCleanupCustody(registry, &owner.metadata)
	if err != nil {
		owner.enrollment.Close()
		_ = owner.metadata.Close()
		owner.retention.Release()
		return nil, err
	}
	owner.idle = owner.metadata.Bytes()
	return owner, nil
}

// finish is called once after the original operands have actually been
// released. Equal accounting is only an additional check: callers must also
// clear all aliases and finish physical cleanup before a successful finish.
// A real cleanup failure transfers this exact owner and its independently
// admitted operand to the registry, retaining the original charge and error.
func (owner *nativePreparedSideOwner) finish(cleanupErr error) error {
	if owner == nil {
		return nil
	}
	if owner.finished {
		return owner.failure
	}
	if cleanupErr != nil {
		owner.finished, owner.failure = true, cleanupErr
		owner.custody.Complete(owner, cleanupErr)
		owner.custody = nil
		return cleanupErr
	}
	if owner.failedOperand != nil || owner.metadata.Bytes() != owner.idle {
		return rootpublication.ErrResourceOwnership
	}
	if err := owner.metadata.Close(); err != nil {
		owner.finished, owner.failure = true, err
		owner.custody.Complete(owner, err)
		owner.custody = nil
		return err
	}
	owner.custody.Complete(nil, nil)
	owner.custody = nil
	owner.enrollment.Close()
	owner.enrollment = nil
	owner.finished = true
	owner.retention.Release()
	owner.retention = nil
	return nil
}
