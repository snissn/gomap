package memtable

import "unsafe"

// COWNativeGrowthTransfer is a caller-owned, single-use provisional transfer
// between two existing leases in the exact same budget. The caller must hold
// exclusive ownership of both lease edges until Commit or Rollback; neither
// lease may be resized, split, shrunk, closed, or handed to another caller in
// that interval. A transfer copied after Begin cannot be committed or rolled
// back using the copy. In particular this type is not an owner enrollment, does not
// select a registry binding, and cannot by itself protect a shared owner from
// unrelated growth. Its containing allocation must already be admitted.
//
// Begin transfers previously admitted bytes without acquiring additional
// capacity, even after budget Close while both edges remain live. Rollback
// restores the exact source and target charges, including
// after budget Close. A caller integrating multiple owner bindings must arrange
// exclusive custody and roll back every provisional change before exposing an
// unsuccessful owner resize. No callback or foreign lock is acquired here;
// the only internal lock is the shared budget mutex.
type COWNativeGrowthTransfer struct {
	self                       *COWNativeGrowthTransfer
	source, target             *COWExternalLease
	sourceBefore, targetBefore uint64
	delta                      uint64
	started, active            bool
}

// Begin binds this transfer to an exact target; it cannot spend the source on
// a different target or be reused after completion. retainedBytes is the
// source's remaining ownership floor excluding its own lease header.
func (transfer *COWNativeGrowthTransfer) Begin(source, target *COWExternalLease, delta, retainedBytes uint64) error {
	if transfer == nil || source == nil || target == nil {
		return ErrCOWClosed
	}
	if transfer.started {
		return ErrCOWPending
	}
	if source == target || source.budget == nil || source.budget != target.budget || delta == 0 {
		return ErrCOWCapacity
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	floor, ok := cowAdd(retainedBytes, header)
	if !ok {
		return ErrCOWCapacity
	}
	b := source.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if source.closed || target.closed {
		return ErrCOWClosed
	}
	targetAfter, ok := cowAdd(target.charge, delta)
	if !ok || !cowFits(source.charge, floor, delta) {
		return ErrCOWCapacity
	}
	transfer.self, transfer.source, transfer.target = transfer, source, target
	transfer.sourceBefore, transfer.targetBefore = source.charge, target.charge
	transfer.delta, transfer.started, transfer.active = delta, true, true
	source.charge -= delta
	target.charge = targetAfter
	return nil
}

// Commit consumes this provisional transfer. The target now owns the moved
// charge and must retain it until the corresponding actual storage is gone.
func (transfer *COWNativeGrowthTransfer) Commit() error {
	if transfer == nil || !transfer.active || transfer.self != transfer {
		return ErrCOWPending
	}
	source, target := transfer.source, transfer.target
	b := source.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if source.closed || target.closed {
		return ErrCOWClosed
	}
	if source.charge != transfer.sourceBefore-transfer.delta || target.charge != transfer.targetBefore+transfer.delta {
		return ErrCOWPending
	}
	transfer.active = false
	transfer.source, transfer.target = nil, nil
	return nil
}

// Rollback requires the same exclusive ownership as Begin. An unexpected
// net charge change is refused rather than refunding somebody else's bytes;
// matching charges do not detect an intervening mutation restored to its old
// value, so this check does not replace exclusive ownership.
func (transfer *COWNativeGrowthTransfer) Rollback() error {
	if transfer == nil || !transfer.active || transfer.self != transfer {
		return ErrCOWPending
	}
	source, target := transfer.source, transfer.target
	b := source.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if source.closed || target.closed {
		return ErrCOWClosed
	}
	if source.charge != transfer.sourceBefore-transfer.delta || target.charge != transfer.targetBefore+transfer.delta {
		return ErrCOWPending
	}
	source.charge, target.charge = transfer.sourceBefore, transfer.targetBefore
	transfer.active = false
	transfer.source, transfer.target = nil, nil
	return nil
}
