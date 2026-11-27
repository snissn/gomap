package memtable

import "unsafe"

// Idle batch scratch retains cleared, zero-length caller-owned batch slices
// (never Batch, DB, COWExternalLease, key/value, or cut references). At most
// cowIdleBatchMaxSlots entries retain up to min(cowIdleBatchMaxBytes,
// MaxInFlightBytes/4) of continuously charged backing. Parking either shrinks
// an active lease (cowResetAfterWrite) or consumes a closing lease (cowClose);
// Take allocates a fresh external lease outside budget.mu. Eviction uses scalar
// charges under budget.mu, with no callbacks or I/O.
type cowIdleBatchScratch struct {
	value  any
	charge uint64
}

func (b *COWBudget) idleBatchMaxBytesLocked() uint64 {
	return min(uint64(cowIdleBatchMaxBytes), b.limits.MaxInFlightBytes/4)
}

// ParkIdleBatchScratchFromLease transfers retainedPayload from an active batch
// lease into the bounded idle batch stack while keeping lease open at
// header+baseBytes. Excess charge above header+baseBytes+retainedPayload is
// refunded immediately. On failure lease and budget stats are unchanged.
func (b *COWBudget) ParkIdleBatchScratchFromLease(value any, lease *COWExternalLease, baseBytes, retainedPayload uint64) bool {
	if b == nil {
		return false
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	newLeaseCharge, ok := cowAdd(header, baseBytes)
	if !ok {
		return false
	}
	neededCharge, ok := cowAdd(newLeaseCharge, retainedPayload)
	if !ok {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || value == nil || lease == nil || lease.embedded || lease.budget != b || lease.closed || retainedPayload == 0 {
		return false
	}
	if neededCharge > lease.charge {
		return false
	}
	maxBytes := b.idleBatchMaxBytesLocked()
	if int(b.idleBatchCount) >= len(b.idleBatch) || retainedPayload > maxBytes {
		return false
	}
	var totalParked uint64
	for i := uint8(0); i < b.idleBatchCount; i++ {
		totalParked += b.idleBatch[i].charge
	}
	if totalParked > maxBytes-retainedPayload {
		return false
	}
	excess := lease.charge - neededCharge
	lease.charge = newLeaseCharge
	b.stats.TotalBytes -= excess
	b.stats.ReservedBytes -= excess
	b.stats.ExternalBytes -= excess
	if excess != 0 && b.maintenanceChild {
		b.maintenanceRefunds++
	}
	b.updateFastHeadroomLocked()
	b.idleBatch[b.idleBatchCount] = cowIdleBatchScratch{
		value:  value,
		charge: retainedPayload,
	}
	b.idleBatchCount++
	return true
}

// PutIdleBatchScratch consumes a closing batch lease and parks retainedPayload
// into the bounded idle batch stack, refunding lease.charge-retainedPayload and
// invalidating the old lease capability. On failure lease remains open and valid.
func (b *COWBudget) PutIdleBatchScratch(value any, lease *COWExternalLease, retainedPayload uint64) bool {
	if b == nil {
		return false
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || value == nil || lease == nil || lease.embedded || lease.budget != b || lease.closed || retainedPayload == 0 {
		return false
	}
	if lease.charge <= header || retainedPayload > lease.charge-header {
		return false
	}
	maxBytes := b.idleBatchMaxBytesLocked()
	if int(b.idleBatchCount) >= len(b.idleBatch) || retainedPayload > maxBytes {
		return false
	}
	var totalParked uint64
	for i := uint8(0); i < b.idleBatchCount; i++ {
		totalParked += b.idleBatch[i].charge
	}
	if totalParked > maxBytes-retainedPayload {
		return false
	}
	lease.closed = true
	refund := lease.charge - retainedPayload
	b.stats.TotalBytes -= refund
	b.stats.ReservedBytes -= refund
	b.stats.ExternalBytes -= refund
	b.stats.ExternalLeases--
	if b.maintenanceChild {
		b.maintenanceRefunds++
	}
	b.updateFastHeadroomLocked()
	b.idleBatch[b.idleBatchCount] = cowIdleBatchScratch{
		value:  value,
		charge: retainedPayload,
	}
	b.idleBatchCount++
	return true
}

// TakeIdleBatchScratch pops the most recently parked batch scratch (LIFO),
// admits header+baseBytes on top of its continuously charged payload, and
// returns the scratch alongside a fresh external lease allocated outside b.mu.
// On capacity failure the popped payload is refunded cleanly.
func (b *COWBudget) TakeIdleBatchScratch(baseBytes uint64) (any, *COWExternalLease, uint64) {
	if b == nil {
		return nil, nil, 0
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	extra, ok := cowAdd(header, baseBytes)
	if !ok {
		return nil, nil, 0
	}
	b.mu.Lock()
	if b.closed || b.idleBatchCount == 0 {
		b.mu.Unlock()
		return nil, nil, 0
	}
	b.idleBatchCount--
	e := b.idleBatch[b.idleBatchCount]
	b.idleBatch[b.idleBatchCount] = cowIdleBatchScratch{}
	leaseCharge, ok := cowAdd(e.charge, extra)
	if !ok || b.stats.ExternalLeases == int(^uint(0)>>1) ||
		!b.inFlightFitsLocked(extra) || !b.retirementFitsLocked(extra) || !b.addLocked(extra) {
		b.refundIdleBatchLocked(e)
		b.mu.Unlock()
		return nil, nil, 0
	}
	b.stats.ReservedBytes += extra
	b.stats.ExternalBytes += extra
	b.stats.ExternalLeases++
	b.updateFastHeadroomLocked()
	b.mu.Unlock()
	return e.value, &COWExternalLease{budget: b, charge: leaseCharge}, e.charge
}

// TakeIdleBatchScratchIntoLease pops the most recently parked batch scratch
// (LIFO) and transfers its already-charged payload directly into an existing
// open external lease. Because the payload bytes are continuously charged in
// this budget, no new lease allocation or budget capacity check is required.
func (b *COWBudget) TakeIdleBatchScratchIntoLease(lease *COWExternalLease) (any, uint64) {
	if b == nil || lease == nil {
		return nil, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.idleBatchCount == 0 || lease.embedded || lease.budget != b || lease.closed {
		return nil, 0
	}
	e := b.idleBatch[b.idleBatchCount-1]
	newCharge, ok := cowAdd(lease.charge, e.charge)
	if !ok {
		return nil, 0
	}
	b.idleBatchCount--
	b.idleBatch[b.idleBatchCount] = cowIdleBatchScratch{}
	lease.charge = newCharge
	return e.value, e.charge
}

func (b *COWBudget) refundIdleBatchLocked(e cowIdleBatchScratch) {
	b.stats.TotalBytes -= e.charge
	b.stats.ReservedBytes -= e.charge
	b.stats.ExternalBytes -= e.charge
	if e.charge != 0 && b.maintenanceChild {
		b.maintenanceRefunds++
	}
	b.updateFastHeadroomLocked()
}

func (b *COWBudget) reclaimIdleBatchLocked() bool {
	if b.idleBatchCount == 0 {
		return false
	}
	b.idleBatchCount--
	e := b.idleBatch[b.idleBatchCount]
	b.idleBatch[b.idleBatchCount] = cowIdleBatchScratch{}
	b.refundIdleBatchLocked(e)
	return true
}

// DrainIdleBatchScratch refunds and clears all currently parked idle batch
// scratch buffers without allocating a temporary lease.
func (b *COWBudget) DrainIdleBatchScratch() {
	if b == nil {
		return
	}
	b.mu.Lock()
	for b.reclaimIdleBatchLocked() {
	}
	b.mu.Unlock()
}

func (b *COWBudget) hasIdleBatchScratchLocked() bool { return b.idleBatchCount != 0 }

func (b *COWBudget) reclaimIdleScratchLocked() bool {
	if b.reclaimIdleBatchLocked() {
		return true
	}
	return b.reclaimReadScratchLocked()
}
