package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"unsafe"
)

// cowKnownControlCharge bounds concrete storage still ahead of prediction.
// Existing batch/write scratch is already owned and is deliberately excluded.
// Resource identities discovered by placement retain their canonical admission.
func (b *Batch) cowKnownControlCharge(groups [][]memtable.COWMutation) (uint64, error) {
	c := b.db.cow
	n, pointers := 0, 0
	needsSuccessors := false
	for i, group := range groups {
		n += len(group)
		if len(group) > 0 && c.successors[i].writer == nil {
			needsSuccessors = true
		}
		for _, m := range group {
			if m.Flags&node.FlagPointer != 0 {
				pointers++
			}
		}
	}
	total := uint64(0)
	add := func(charge uint64) bool {
		if charge > ^uint64(0)-total {
			return false
		}
		total += charge
		return true
	}
	lease := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	addLease := func(charge uint64) bool { return add(charge) && add(lease) }
	reuse := len(b.shardCnts) == len(groups) && len(b.cowState.predictionGroups) == len(groups) && len(b.cowState.predictionStorage) == n
	if !addLease(cowPreparationScratchCharge(len(groups), n, pointers, reuse)) || !addLease(cowPreparedCutCharge(len(groups), len(c.cut.frozen), needsSuccessors)) {
		return 0, memtable.ErrCOWCapacity
	}
	if pointers > 0 && !addLease(cowFrameCaptureCharge(pointers)) {
		return 0, memtable.ErrCOWCapacity
	}
	if c.txnOwner != nil {
		capacity := c.txnJournalCapacity()
		if c.journal == nil && !add(txnJournalControlCharge(capacity)) {
			return 0, memtable.ErrCOWCapacity
		}
		var effects []txnhistory.Effect
		if b.cowState.native != nil {
			effects = b.cowState.native.plan.Effects
		}
		sizes := txnhistory.PreparationAllocationSizes(capacity, c.budget.Limits().MaxInFlightBytes/8, b.entries, nil, false, effects)
		var arena uint64
		for _, size := range sizes {
			charge := memtable.COWAllocationCharge(size)
			if charge > ^uint64(0)-arena {
				return 0, memtable.ErrCOWCapacity
			}
			arena += charge
		}
		if arena != 0 && !addLease(arena) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	return total, nil
}
