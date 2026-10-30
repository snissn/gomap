package memtable

import "unsafe"

// Idle read scratch contains only caller-owned buffers and numeric traversal
// state, never file/dictionary/snapshot authority. At most four entries retain
// 128KiB of charged backing. Put consumes the original lease capabilities; Take
// creates fresh ones. Old handles can never refund a later borrower's storage.
// Eviction uses scalar charges under budget.mu, with no callbacks or I/O.
type cowIdleReadScratch struct {
	value   any
	charges [2]uint64 // payload only; no external lease wrappers remain idle
}

// The caller drops its receipt pointers before transferring storage. On failure
// the original receipts remain valid and the caller must restore or close them.
func (b *COWBudget) PutIdleReadScratch(value any, leases [2]*COWExternalLease) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || value == nil || leases[0] == nil || leases[0] == leases[1] {
		return false
	}
	header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
	var charges [2]uint64
	receipts := uint64(0)
	for i, l := range leases {
		if l == nil {
			continue
		}
		if l.budget != b || l.closed || l.charge <= header {
			return false
		}
		charges[i] = l.charge - header
		receipts++
	}
	charge := charges[0] + charges[1]
	for _, e := range b.idleRead[:b.idleReadCount] {
		charge += e.charges[0] + e.charges[1]
	}
	if b.idleReadCount == len(b.idleRead) || charge > 128<<10 {
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
	b.idleRead[b.idleReadCount] = cowIdleReadScratch{value: value, charges: charges}
	b.idleReadCount++
	return true
}

func (b *COWBudget) TakeIdleReadScratch() (any, [2]*COWExternalLease) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, [2]*COWExternalLease{}
	}
	if b.idleReadCount > 0 {
		b.idleReadCount--
		e := b.idleRead[b.idleReadCount]
		b.idleRead[b.idleReadCount] = cowIdleReadScratch{} // payload stays charged during transfer
		header := cowAllocation(uint64(unsafe.Sizeof(COWExternalLease{})))
		receipts := 1
		if e.charges[1] != 0 {
			receipts++
		}
		extra := uint64(receipts) * header
		if b.stats.ExternalLeases > int(^uint(0)>>1)-receipts || !b.inFlightFitsLocked(extra) || !b.retirementFitsLocked(extra) || !b.addLocked(extra) {
			b.refundIdleReadLocked(e)
			return nil, [2]*COWExternalLease{}
		}
		b.stats.ReservedBytes += extra
		b.stats.ExternalBytes += extra
		b.stats.ExternalLeases += receipts
		leases := [2]*COWExternalLease{{budget: b, charge: e.charges[0] + header}, nil}
		if receipts == 2 {
			leases[1] = &COWExternalLease{budget: b, charge: e.charges[1] + header}
		}
		return e.value, leases
	}
	return nil, [2]*COWExternalLease{}
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
}
func (b *COWBudget) refundIdleReadLocked(e cowIdleReadScratch) {
	charge := e.charges[0] + e.charges[1]
	b.stats.TotalBytes -= charge
	b.stats.ReservedBytes -= charge
	b.stats.ExternalBytes -= charge
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
		if !b.reclaimReadScratchLocked() {
			return false
		}
	}
	return true
}
