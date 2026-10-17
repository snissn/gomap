package memtable

import "unsafe"

// COWNativeSourceReservation is the original preadmitted native source cohort.
// It owns no root, generation, source, work record or engine backpointer. The
// generation and its one original captured source share this accounting cell.
// Variable native ACTIVE and captured generations consume the SAME finite
// source counter from their first prewrite admission until the last true edge.
// All live fields are guarded by the original budget; the last edge clears
// budget too, after both original release operations have acquired their lock.
type COWNativeSourceReservation struct {
	budget   *COWBudget
	lease    *COWExternalLease
	capacity uint64
	refs     int
	claimed  bool
}

// NewNativeCOWWriter admits the original source-owner metadata BEFORE any
// native mutation can become accepted. Forks reuse the generation's original
// cell. Physical rollover only claims its source slot and exact immutable root;
// it never asks an exhausted producer for more bytes.
func (b *COWBudget) NewNativeCOWWriter(sourceOwnerBytes uint64) (*COWWriter, error) {
	charge, ok := cowAdd(cowAllocation(uint64(unsafe.Sizeof(COWNativeSourceReservation{}))), sourceOwnerBytes)
	if !ok {
		return nil, ErrCOWCapacity
	}
	lease, err := b.acquireExternal(charge, true)
	if err != nil {
		return nil, err
	}
	cell := &COWNativeSourceReservation{budget: b, lease: lease, capacity: sourceOwnerBytes, refs: 1}
	w, err := NewCOWWriter(b)
	if err != nil {
		lease = cell.detachGeneration()
		cell = nil
		lease.Close()
		return nil, err
	}
	b.mu.Lock()
	// This private generation has no other writer/root yet. Include the entire
	// independently held cohort in its finite generation capacity, too.
	if !cowFits(b.limits.MaxGenerationBytes, w.generation.history, lease.charge) {
		b.mu.Unlock()
		retirement := w.Close()
		w = nil
		retirement.Drain()
		lease = cell.detachGeneration()
		cell = nil
		lease.Close()
		return nil, ErrCOWCapacity
	}
	w.generation.nativeSource = cell
	b.mu.Unlock()
	return w, nil
}

// generationHistoryLocked includes native metadata in the same original
// generation limit without duplicating its independently held byte accounting.
func (g *cowGeneration) generationHistoryLocked() uint64 {
	history := g.history
	if g.nativeSource != nil {
		history += g.nativeSource.lease.charge // constructor proved the finite sum
	}
	return history
}

// claimLocked acquires one original captured-source accounting edge. The caller
// also acquires the exact root edge under that root's lock, without allocation.
func (c *COWNativeSourceReservation) claimLocked(bytes uint64) error {
	b := c.budget
	if b.closed {
		return ErrCOWClosed
	}
	if c.claimed {
		return ErrCOWPending
	}
	if bytes > c.capacity {
		return ErrCOWCapacity
	}
	c.refs++
	c.claimed = true
	return nil
}

// detachGeneration returns only the independently admitted final accounting
// receipt. Its caller clears every generation/cell alias before completing it.
func (c *COWNativeSourceReservation) detachGeneration() *COWExternalLease {
	b := c.budget
	b.mu.Lock()
	c.refs--
	lease := c.takeFinalLeaseLocked()
	b.mu.Unlock()
	return lease
}

func (c *COWNativeSourceReservation) takeFinalLeaseLocked() *COWExternalLease {
	if c.refs < 0 {
		panic("COW native source reservation released twice")
	}
	if c.refs != 0 {
		return nil
	}
	lease := c.lease
	c.budget.stats.Sources--
	c.lease = nil
	c.budget = nil
	c.capacity = 0
	return lease
}

// Close completes the original source claim after its actual root/resource and
// containing source aliases have detached. A canceled rollover can reuse the
// preadmitted cell while its original generation still exists. Cancellation
// adds or refunds no physical source slot; the original active source owns it.
func (c *COWNativeSourceReservation) Close() {
	if c == nil {
		return
	}
	b := c.budget
	b.mu.Lock()
	if !c.claimed {
		b.mu.Unlock()
		panic("COW native source claim released twice")
	}
	c.claimed = false
	c.refs--
	lease := c.takeFinalLeaseLocked()
	b.mu.Unlock()
	c = nil
	lease.Close()
}
