package memtable

import "unsafe"

// ShrinkNativeBytes returns an ended phase's credit from the SAME original
// reservation. The caller must first drop every allocation/alias covered by
// bytes. retainedBytes protects the remaining ledger and construction promises;
// neither the remaining receipt nor any split child's ownership changes.
// bytes is the exact credit included in the original aggregate reservation,
// rather than a new independently rounded allocation. Like Resize's shrink,
// this needs no admission and remains valid after budget Close.
func (l *COWExternalLease) ShrinkNativeBytes(bytes, retainedBytes uint64) error {
	if l == nil {
		return ErrCOWClosed
	}
	floor, ok := cowAdd(retainedBytes, cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{}))))
	if !ok {
		return ErrCOWCapacity
	}
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.closed {
		return ErrCOWClosed
	}
	if !cowFits(l.charge, floor, bytes) {
		return ErrCOWCapacity
	}
	l.charge -= bytes
	b.stats.TotalBytes -= bytes
	b.stats.ReservedBytes -= bytes
	b.stats.ExternalBytes -= bytes
	return nil
}

// SplitNativeBytes transfers already admitted bytes from an original native
// build reservation to an independent child receipt. It performs no new byte
// admission, even after budget Close. The parent must remain live and retain
// retainedBytes for its original build ledger, in addition to its own receipt.
// Both amounts use COWAllocationCharge for each concrete allocation first.
// The child then receives the same conservative aggregate rounding as
// COWBudget.AdmitNativeBytes, before adding its independent receipt header.
//
// The child contains no parent, native Work, resource or state backpointer.
// Callers precompute all child construction bounds before physical acceptance;
// an insufficient promise is a terminal original-ledger failure, not permission
// to acquire replacement capacity or replay accepted materialization.
func (l *COWExternalLease) SplitNativeBytes(bytes, retainedBytes uint64) (*COWExternalLease, error) {
	if l == nil {
		return nil, ErrCOWClosed
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	child, ok := cowAdd(COWAllocationCharge(bytes), header)
	if !ok {
		return nil, ErrCOWCapacity
	}
	floor, ok := cowAdd(retainedBytes, header)
	if !ok {
		return nil, ErrCOWCapacity
	}
	b := l.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.closed {
		return nil, ErrCOWClosed
	}
	if b.stats.ExternalLeases == int(^uint(0)>>1) || !cowFits(l.charge, floor, child) {
		return nil, ErrCOWCapacity
	}
	l.charge -= child
	b.stats.ExternalLeases++
	return &COWExternalLease{budget: b, charge: child}, nil
}
