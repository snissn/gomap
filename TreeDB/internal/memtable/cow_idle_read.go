package memtable

import "unsafe"

// Idle read scratch contains only caller-owned buffers and numeric traversal
// state, never file/dictionary/snapshot authority. At most 16 entries retain
// 512KiB of charged backing. Put consumes the original lease capabilities; Take
// creates fresh ones. Old handles can never refund a later borrower's storage.
// Eviction uses scalar charges under budget.mu, with no callbacks or I/O.
type cowIdleReadScratch struct {
	value   any
	charges [3]uint32 // payload only (bounded by cowIdleReadMaxBytes); no external lease wrappers remain idle
}

// The caller drops its receipt pointers before transferring storage. On failure
// the original receipts remain valid and the caller must restore or close them.
func (b *COWBudget) PutIdleReadScratch(value any, leases [2]*COWExternalLease) bool {
	return b.PutIdleReadScratch3(value, [3]*COWExternalLease{leases[0], leases[1], nil})
}

// PutIdleReadScratch3 parks caller-owned read scratch alongside up to three
// independent external leases.
func (b *COWBudget) PutIdleReadScratch3(value any, leases [3]*COWExternalLease) bool {
	b.mu.Lock()
	ok := b.putIdleReadScratch3Locked(value, leases)
	b.mu.Unlock()
	return ok
}

func (b *COWBudget) putIdleReadScratch3Locked(value any, leases [3]*COWExternalLease) bool {
	if b.closed || value == nil || leases[0] == nil || leases[0] == leases[1] ||
		(leases[2] != nil && (leases[2] == leases[0] || leases[2] == leases[1])) {
		return false
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	var charges [3]uint64
	receipts := uint64(0)
	for i, l := range leases {
		if l == nil {
			continue
		}
		if l.embedded || l.budget != b || l.closed || l.charge <= header {
			return false
		}
		payload := l.charge - header
		if payload > cowIdleReadMaxBytes {
			return false
		}
		charges[i] = payload
		receipts++
	}
	charge := charges[0] + charges[1] + charges[2]
	for _, e := range b.idleRead[:b.idleReadCount] {
		charge += uint64(e.charges[0]) + uint64(e.charges[1]) + uint64(e.charges[2])
	}
	if b.idleReadCount == len(b.idleRead) || charge > cowIdleReadMaxBytes {
		return false
	}
	// Payload stays charged continuously. Retired capability wrappers are dropped
	// before refund, just as an ordinary Close consumes its original receipt.
	for _, l := range leases {
		if l != nil {
			l.closed = true
		}
	}
	b.stats.TotalBytes -= receipts * header
	b.stats.ReservedBytes -= receipts * header
	b.stats.ExternalBytes -= receipts * header
	b.stats.ExternalLeases -= int(receipts)
	b.updateFastHeadroomLocked()
	b.idleRead[b.idleReadCount] = cowIdleReadScratch{
		value:   value,
		charges: [3]uint32{uint32(charges[0]), uint32(charges[1]), uint32(charges[2])},
	}
	b.idleReadCount++
	return true
}

func (b *COWBudget) TakeIdleReadScratch() (any, [2]*COWExternalLease) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, [2]*COWExternalLease{}
	}
	for i := b.idleReadCount - 1; i >= 0; i-- {
		if b.idleRead[i].charges[2] != 0 {
			continue
		}
		e := b.idleRead[i]
		copy(b.idleRead[i:], b.idleRead[i+1:b.idleReadCount])
		b.idleReadCount--
		b.idleRead[b.idleReadCount] = cowIdleReadScratch{} // payload stays charged during transfer
		header, receipts, ok := b.reserveIdleReadLeasesLocked(e)
		b.mu.Unlock()
		if !ok {
			return nil, [2]*COWExternalLease{}
		}
		leases3 := b.allocateIdleReadLeases(e.charges, header, receipts)
		return e.value, [2]*COWExternalLease{leases3[0], leases3[1]}
	}
	b.mu.Unlock()
	return nil, [2]*COWExternalLease{}
}

// TakeIdleReadScratch3 retrieves a parked read scratch entry and creates up to
// three fresh external leases for its charged backing.
func (b *COWBudget) TakeIdleReadScratch3() (any, [3]*COWExternalLease) {
	b.mu.Lock()
	if b.closed || b.idleReadCount == 0 {
		b.mu.Unlock()
		return nil, [3]*COWExternalLease{}
	}
	b.idleReadCount--
	e := b.idleRead[b.idleReadCount]
	b.idleRead[b.idleReadCount] = cowIdleReadScratch{} // payload stays charged during transfer
	header, receipts, ok := b.reserveIdleReadLeasesLocked(e)
	b.mu.Unlock()
	if !ok {
		return nil, [3]*COWExternalLease{}
	}
	return e.value, b.allocateIdleReadLeases(e.charges, header, receipts)
}

func (b *COWBudget) reserveIdleReadLeasesLocked(e cowIdleReadScratch) (header uint64, receipts int, ok bool) {
	header = cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	for _, c := range e.charges {
		if c != 0 {
			receipts++
		}
	}
	extra := uint64(receipts) * header
	if b.stats.ExternalLeases > int(^uint(0)>>1)-receipts || !b.inFlightFitsLocked(extra) || !b.retirementFitsLocked(extra) || !b.addLocked(extra) {
		b.refundIdleReadLocked(e)
		return 0, 0, false
	}
	b.stats.ReservedBytes += extra
	b.stats.ExternalBytes += extra
	b.stats.ExternalLeases += receipts
	b.updateFastHeadroomLocked()
	return header, receipts, true
}

// allocateIdleReadLeases allocates the admitted fresh lease handles outside b.mu
// using a single heap allocation sized to the exact receipt count.
func (b *COWBudget) allocateIdleReadLeases(charges [3]uint32, header uint64, receipts int) [3]*COWExternalLease {
	var leases [3]*COWExternalLease
	var backing []COWExternalLease
	switch receipts {
	case 1:
		backing = new([1]COWExternalLease)[:]
	case 2:
		backing = new([2]COWExternalLease)[:]
	case 3:
		backing = new([3]COWExternalLease)[:]
	default:
		return leases
	}
	slot := 0
	for i, c := range charges {
		if c != 0 {
			backing[slot] = COWExternalLease{budget: b, charge: uint64(c) + header}
			leases[i] = &backing[slot]
			slot++
		}
	}
	return leases
}

func (l *COWExternalLease) closeLocked() {
	if l.closed {
		return
	}
	l.closed = true
	b := l.budget
	b.stats.TotalBytes -= l.charge
	b.stats.ReservedBytes -= l.charge
	b.stats.ExternalBytes -= l.charge
	b.stats.ExternalLeases--
	if b.maintenanceChild {
		b.maintenanceRefunds++
	}
	b.updateFastHeadroomLocked()
}
func (b *COWBudget) refundIdleReadLocked(e cowIdleReadScratch) {
	charge := uint64(e.charges[0]) + uint64(e.charges[1]) + uint64(e.charges[2])
	b.stats.TotalBytes -= charge
	b.stats.ReservedBytes -= charge
	b.stats.ExternalBytes -= charge
	b.updateFastHeadroomLocked()
}
func (b *COWBudget) reclaimReadScratchLocked() bool {
	if b.idleReadCount == 0 {
		return false
	}
	b.idleReadCount--
	e := b.idleRead[b.idleReadCount]
	b.idleRead[b.idleReadCount] = cowIdleReadScratch{}
	b.refundIdleReadLocked(e)
	return true
}
func (b *COWBudget) hasIdleReadScratchLocked() bool { return b.idleReadCount != 0 }
func (b *COWBudget) inFlightFitsLocked(n uint64) bool {
	for !cowFits(b.limits.MaxInFlightBytes, b.stats.ReservedBytes, n) {
		if !b.reclaimIdleScratchLocked() {
			return false
		}
	}
	return true
}
