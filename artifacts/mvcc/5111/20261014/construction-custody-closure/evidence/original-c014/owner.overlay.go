// Package retainedalloc admits added producer-owned metadata before allocation.
// An Owner is embedded in the existing storage owner, not a publication or
// retirement registry. Its budget enrollments share that owner's real lifetime.
package retainedalloc

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

var ErrCapacity = errors.New("retained metadata capacity exhausted")
var ErrClosed = errors.New("retained metadata owner closed")

type Lease interface {
	Resize(uint64) error
	Close()
}
type Budget interface{ AcquireRetention(uint64) (Lease, error) }

// AllocationCharge matches the conservative Go allocator capacity policy used
// by COW. Round separate actual allocations before summing their charges.
func AllocationCharge(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	if n > math.MaxInt64-8192-16 {
		return math.MaxUint64
	}
	ptr := uint64(unsafe.Sizeof(uintptr(0)))
	if n > ptr*ptr*8 && n < 32768 {
		n += 16
	}
	if n <= 32768 {
		c := uint64(16)
		for c < n {
			c *= 2
		}
		return c
	}
	return (n + 8191) &^ uint64(8191)
}

type binding struct {
	budget   Budget
	lease    Lease
	refs     uint64
	producer bool
	next     *binding
}
type Owner struct {
	mu                                 sync.Mutex
	bytes, baseline, controls, pending uint64
	head                               *binding
	closed                             bool
	retired, cleanupFailed             bool
}

// Initialize freezes the actual constructor baseline before any enrollment.
// It must be called once before this owner becomes visible.
func (o *Owner) Initialize(bytes uint64, fixed ...uint64) {
	o.bytes = bytes
	o.baseline = bytes
	if len(fixed) != 0 {
		o.baseline = fixed[0]
	}
}

// Bytes is a producer-maintained capacity total; it never walks retained roots.
func (o *Owner) Bytes() uint64 { o.mu.Lock(); defer o.mu.Unlock(); return o.bytes }

// Add admits exact already-rounded added capacity against every enrolled
// governing budget. If any refuses, all earlier reservations are rolled back
// before returning and the producer must allocate nothing.
func (o *Owner) Add(bytes uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.resizeLocked(bytes, true)
}
func (o *Owner) resizeLocked(bytes uint64, grow bool) error {
	if o.closed && grow {
		return ErrClosed
	}
	target := o.bytes
	if grow {
		if bytes > math.MaxUint64-target {
			return ErrCapacity
		}
		target += bytes
	} else {
		if bytes > target {
			return ErrCapacity
		}
		target -= bytes
	}
	for b := o.head; b != nil; b = b.next {
		if err := b.lease.Resize(target); err != nil {
			// Shrink back to the prior successful admission. Existing leases always
			// permit this even after budget Close; no foreign writer can own this lease.
			for prior := o.head; prior != b; prior = prior.next {
				if rollback := prior.lease.Resize(o.bytes); rollback != nil {
					panic("retained allocation rollback refused")
				}
			}
			return err
		}
	}
	o.bytes = target
	return nil
}

// AddPending admits actual transaction/runtime storage whose existing ownership
// intentionally survives physical arena shutdown. This is capacity accounting,
// not another retirement cursor or callback registry.
func (o *Owner) AddPending(bytes uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.resizeLocked(bytes, true); err != nil {
		return err
	}
	beforePending := o.pending
	o.pending += bytes
	_, file, line, ok := runtime.Caller(1)
	println("COW_PENDING_SITE", o, "AddPending", bytes, beforePending, o.pending, file, line, ok)
	return nil
}
func (o *Owner) RemovePending(bytes uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if bytes > o.pending {
		panic("pending metadata underflow")
	}
	if err := o.resizeLocked(bytes, false); err != nil {
		panic("pending metadata release refused")
	}
	beforePending := o.pending
	o.pending -= bytes
	_, file, line, ok := runtime.Caller(1)
	println("COW_PENDING_SITE", o, "RemovePending", bytes, beforePending, o.pending, file, line, ok)
	if o.closed && o.pending == 0 {
		o.releaseClosedBindingsLocked()
	}
}
func (o *Owner) releaseClosedBindingsLocked() {
	link := &o.head
	for *link != nil {
		b := *link
		if b.refs != 0 || b.producer {
			link = &b.next
			continue
		}
		*link = b.next
		b.next = nil
		b.lease.Close()
		delta := AllocationCharge(uint64(unsafe.Sizeof(binding{})))
		if err := o.resizeLocked(delta, false); err != nil {
			panic("retained cleanup release refused")
		}
		o.controls -= delta
	}
	if o.head == nil {
		o.bytes = 0
		o.baseline = 0
	}
}

// Remove follows actual storage/edge cleanup, never speculative disposal.
func (o *Owner) Remove(bytes uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.resizeLocked(bytes, false); err != nil {
		panic("retained allocation accounting underflow")
	}
}

type Enrollment struct {
	owner     *Owner
	binding   *binding
	closed    atomic.Bool
	companion *Enrollment
}

// Enroll shares one governing lease for this exact budget/owner identity.
// Each returned handle owns one actual capture edge. All enrollment and
// binding storage is included in the aggregate owner before allocation.
func (o *Owner) Enroll(budget Budget) (*Enrollment, error) {
	if budget == nil {
		return nil, ErrCapacity
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, ErrClosed
	}
	var found *binding
	for b := o.head; b != nil; b = b.next {
		if b.budget == budget {
			found = b
			break
		}
	}
	delta := AllocationCharge(uint64(unsafe.Sizeof(Enrollment{})))
	if found == nil {
		delta += AllocationCharge(uint64(unsafe.Sizeof(binding{})))
	}
	if err := o.resizeLocked(delta, true); err != nil {
		return nil, err
	}
	if found == nil {
		lease, err := budget.AcquireRetention(o.bytes)
		if err != nil {
			if rollback := o.resizeLocked(delta, false); rollback != nil {
				panic("retained enrollment rollback refused")
			}
			return nil, err
		}
		found = &binding{budget: budget, lease: lease, next: o.head}
		o.head = found
	}
	if found.refs == math.MaxUint64 {
		panic("retained enrollment reference overflow")
	}
	o.controls += delta
	found.refs++
	return &Enrollment{owner: o, binding: found}, nil
}

// Close follows the capture's real storage cleanup. It drops this exact
// enrollment and releases the governing lease only at its last shared capture.
func (e *Enrollment) Close() {
	if e == nil || !e.closed.CompareAndSwap(false, true) {
		return
	}
	e.companion.Close()
	o, b := e.owner, e.binding
	o.mu.Lock()
	defer o.mu.Unlock()
	if b.refs == 0 {
		panic("retained enrollment underflow")
	}
	b.refs--
	delta := AllocationCharge(uint64(unsafe.Sizeof(Enrollment{})))
	if b.refs == 0 && !b.producer && (!o.closed && !o.retired && !o.cleanupFailed || o.closed && o.pending == 0) {
		link := &o.head
		for *link != b {
			if *link == nil {
				panic("retained enrollment absent")
			}
			link = &(*link).next
		}
		*link = b.next
		b.next = nil
		b.lease.Close()
		delta += AllocationCharge(uint64(unsafe.Sizeof(binding{})))
	}
	if err := o.resizeLocked(delta, false); err != nil {
		panic("retained enrollment release refused")
	}
	o.controls -= delta
	if o.closed && o.head == nil && o.pending == 0 {
		o.bytes = 0
		o.baseline = 0
	}
}

// Close follows actual storage and physical-owner cleanup. Captures can still
// own the constructor baseline and enrollment descriptors until their normal
// last release; closed owners never admit more producer growth.
func (o *Owner) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	retained := o.baseline + o.controls + o.pending
	if o.bytes < retained {
		return ErrCapacity
	}
	if err := o.resizeLocked(o.bytes-retained, false); err != nil {
		return err
	}
	o.closed = true
	for b := o.head; b != nil; b = b.next {
		b.producer = false
	}
	if o.pending == 0 {
		o.releaseClosedBindingsLocked()
	}
	return nil
}

// Retire follows replacement of this exact physical arena. Old captures can
// release normally, but its existing governor remains until actual old-owner
// cleanup, including the existing index ghost lifetime.
func (o *Owner) Retire() { o.mu.Lock(); o.retired = true; o.mu.Unlock() }

// CleanupFailed preserves a governing edge for real remaining storage/debt.
// Successful cleanup in Close, rather than an error label, releases this edge.
func (o *Owner) CleanupFailed() { o.mu.Lock(); o.cleanupFailed = true; o.mu.Unlock() }

func (e *Enrollment) BelongsTo(o *Owner) bool {
	return e != nil && !e.closed.Load() && e.owner == o
}
func (e *Enrollment) CloseAfterCleanup(err error) {
	if e == nil {
		return
	}
	if err != nil {
		e.owner.CleanupFailed()
		if e.companion != nil {
			e.companion.owner.CleanupFailed()
		}
	}
	e.Close()
}

// PhysicalClosed distinguishes actual storage teardown from a failed close.
func (o *Owner) PhysicalClosed() bool { o.mu.Lock(); defer o.mu.Unlock(); return o.closed }

// TransferPending preserves total admission while an immutable allocation moves
// from the constructing transaction to an actual durable root bank.
func (o *Owner) TransferPending(bytes uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || bytes > o.pending {
		panic("invalid pending metadata transfer")
	}
	beforePending := o.pending
	o.pending -= bytes
	_, file, line, ok := runtime.Caller(1)
	println("COW_PENDING_SITE", o, "TransferPending", bytes, beforePending, o.pending, file, line, ok)
}

// EnrollPair admits the two distinct producer owners in fixed order against
// one governing budget. The second admission and every later failure unwind
// both actual handles. The pair uses the existing enrollment cells.
func EnrollPair(first, second *Owner, budget Budget) (*Enrollment, error) {
	if first != nil && first == second {
		return nil, ErrCapacity
	}
	if first == nil {
		if second != nil {
			return nil, ErrCapacity
		}
		return nil, nil
	}
	a, err := first.Enroll(budget)
	if err != nil {
		return nil, err
	}
	if second != nil {
		a.companion, err = second.Enroll(budget)
		if err != nil {
			a.Close()
			return nil, err
		}
	}
	return a, nil
}
func (e *Enrollment) BelongsToPair(first, second *Owner) bool {
	if !e.BelongsTo(first) {
		return false
	}
	if second == nil {
		return e.companion == nil
	}
	return e.companion.BelongsTo(second)
}

// ActivateProducer attaches this already-admitted fixed pair to the actual
// producer lifetime before persistent effects. Capture release cannot remove
// these governing edges; actual Owner.Close after disposal does. This is not
// called by temporary dictionary borrowers.
func (e *Enrollment) ActivateProducer() error {
	if e == nil {
		return nil
	}
	a := e.owner
	a.mu.Lock()
	defer a.mu.Unlock()
	var b *Owner
	if e.companion != nil {
		b = e.companion.owner
		b.mu.Lock()
		defer b.mu.Unlock()
	}
	if e.closed.Load() || a.closed || e.companion != nil && (e.companion.closed.Load() || b.closed) {
		return ErrClosed
	}
	e.binding.producer = true
	if b != nil {
		e.companion.binding.producer = true
	}
	return nil
}

// SourceOverlayDiagnostic is an inquiry-only value snapshot. It adds no authority.
type SourceOverlayDiagnostic struct {
    Bytes, Baseline, Controls, Pending uint64
    Closed, Retired, CleanupFailed bool
    BindingCount, BindingRefs, ProducerBindings uint64
}
func (o *Owner) SourceOverlayDiagnosticSnapshot() SourceOverlayDiagnostic {
    o.mu.Lock()
    defer o.mu.Unlock()
    d := SourceOverlayDiagnostic{Bytes:o.bytes, Baseline:o.baseline, Controls:o.controls, Pending:o.pending, Closed:o.closed, Retired:o.retired, CleanupFailed:o.cleanupFailed}
    for b := o.head; b != nil; b = b.next {
        d.BindingCount++
        d.BindingRefs += b.refs
        if b.producer { d.ProducerBindings++ }
    }
    return d
}
