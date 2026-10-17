package caching

import (
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// NativeSemanticCapture retains the exact raw+semantic+disk cut, never a new
// backend capture. State and Snapshot are borrowed until Close. This internal
// capability is admitted before allocation and belongs to this public owner.
type NativeSemanticCapture struct {
	cache *cowCache
	cut   *cowReadCut
	lease *memtable.COWExternalLease
}

func (db *DB) CaptureNativeSemanticState(owner *transactionowner.Owner) (*NativeSemanticCapture, error) {
	if db == nil || db.cow == nil || owner == nil || db.cow.txnOwner != owner {
		return nil, ErrCOWUnsupported
	}
	lease, err := db.cow.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(NativeSemanticCapture{}))))
	if err != nil {
		return nil, err
	}
	cut := db.cow.retainCut()
	if cut == nil {
		lease.Close()
		return nil, backenddb.ErrClosed
	}
	return &NativeSemanticCapture{cache: db.cow, cut: cut, lease: lease}, nil
}
func (c *NativeSemanticCapture) State() backenddb.NativeSemanticState { return c.cut.semanticRootSet }
func (c *NativeSemanticCapture) Snapshot() *backenddb.Snapshot        { return c.cut.basis.snapshot }
func (c *NativeSemanticCapture) LogicalSequence() uint64              { return c.cut.logicalSeq }
func (c *NativeSemanticCapture) Close() error {
	if c == nil || c.cut == nil {
		return nil
	}
	cut := c.cut
	c.cut = nil
	err := c.cache.releaseCut(cut)
	c.lease.Close()
	return err
}

// NativeSemanticState borrows the immutable state of this original snapshot.
func (s *Snapshot) NativeSemanticState() backenddb.NativeSemanticState {
	if s == nil || s.cowCut == nil {
		return nil
	}
	return s.cowCut.semanticRootSet
}

// cowNativeOwner is independently admitted before assignment, never reused with
// Batch storage. Accepted custody retains only sparse native plans/assets.
type cowNativeOwner struct {
	cache   *cowCache
	plan    *backenddb.NativePreparedPublication
	custody backenddb.NativeSemanticCustody
	opening *cowReadCut
	lease   *memtable.COWExternalLease
}

func (o *cowNativeOwner) ReleaseAccepted() error {
	if o.custody != nil {
		record := o.custody.ReleaseNativeCustody()
		o.custody = nil
		o.cache.semanticMu.Lock()
		o.cache.enqueueSemanticLocked(record)
		o.cache.semanticMu.Unlock()
		o.cache.wakeSemanticCleanup()
	}
	// All owner aliases to native resources have been detached or transferred.
	o.lease.Close()
	return nil
}

// SetNativeSemanticPublication attaches a preadmitted native composite to the
// ordinary batch authority. On error the caller retains State and Custody. On
// success the batch consumes State/Custody on installation or cancellation;
// Basis stays borrowed from the caller's retained capture until Write returns.
func (b *Batch) SetNativeSemanticPublication(owner *transactionowner.Owner, native *backenddb.NativePreparedPublication) error {
	if b == nil || b.db == nil || b.db.cow == nil || b.cowState == nil || b.closed || b.cowState.err != nil || b.cowState.native != nil || native == nil || native.State == nil || owner == nil || b.db.cow.txnOwner != owner {
		return ErrCOWUnsupported
	}
	if err := b.AuthorizeCOWAppend(owner); err != nil {
		return err
	}
	c := b.db.cow
	lease, err := c.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowNativeOwner{}))))
	if err != nil {
		return err
	}
	opening := c.retainCut()
	if opening == nil {
		lease.Close()
		return backenddb.ErrClosed
	}
	// Raw effects retain ordinary owned/canonical pointer/revision preparation.
	if err = b.cowSetOps(native.Entries); err != nil {
		_ = c.releaseCut(opening)
		lease.Close()
		return err
	}
	b.cowState.native = &cowNativeOwner{cache: c, plan: native, custody: native.Custody, opening: opening, lease: lease}
	native.Custody = nil
	return nil
}

func (b *Batch) releaseNativePublication() {
	owner := b.cowState.native
	if owner == nil {
		return
	}
	b.cowState.native = nil
	if owner.plan != nil {
		native := owner.plan
		owner.plan = nil
		if native.State != nil {
			record := native.State.Release()
			native.State = nil
			owner.cache.semanticMu.Lock()
			owner.cache.enqueueSemanticLocked(record)
			owner.cache.semanticMu.Unlock()
			owner.cache.wakeSemanticCleanup()
		}
	}
	_ = owner.ReleaseAccepted()
	// Private state and sparse custody enqueue BEFORE the opening cut can leave
	// the serialized closed/zero-active-cut terminal predicate.
	_ = owner.cache.releaseCut(owner.opening)
	owner.opening = nil
}
