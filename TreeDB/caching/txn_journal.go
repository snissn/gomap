package caching

import (
	"errors"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

var (
	ErrTxnTooOld       = txnhistory.ErrTooOld
	ErrTxnIteratorOpen = errors.New("treedb: mutation with open transaction iterator")
)

type txnJournal = txnhistory.History
type txnMutationOwner = txnhistory.Prepared

func (c *cowCache) ensureTxnJournal() error {
	if c.journal != nil || c.txnOwner == nil {
		return nil
	}
	n := 4096
	for n > 8 && uint64(n)*200 > c.budget.Limits().MaxInFlightBytes/16 {
		n /= 2
	}
	var err error
	c.journal, err = txnhistory.New(n, c.budget.Limits().MaxInFlightBytes/8, func(a, b, d, e, f uint64) (txnhistory.Lease, error) {
		var charge uint64
		for _, size := range [5]uint64{a, b, d, e, f} {
			part := memtable.COWAllocationCharge(size)
			if part > ^uint64(0)-charge {
				return nil, memtable.ErrCOWCapacity
			}
			charge += part
		}
		return c.budget.AcquireExternal(charge)
	})
	return err
}
func (tx *ConditionalTxn) validateCOWJournal() error {
	j := tx.db.cow.journal
	if err := j.Check(tx.startSeq); err != nil {
		return err
	}
	if j.BarrierConflict(tx.startSeq) {
		return backenddb.ErrConcurrentModification
	}
	for _, r := range tx.reads {
		if j.PointChanged(tx.startSeq, r.key) {
			return backenddb.ErrConcurrentModification
		}
	}
	for _, w := range tx.batch.entries {
		if j.PointChanged(tx.startSeq, w.Key) {
			return backenddb.ErrConcurrentModification
		}
	}
	for _, r := range tx.cow.ranges {
		if j.RangeChanged(tx.startSeq, r.start, r.end) {
			return backenddb.ErrConcurrentModification
		}
	}
	return nil
}

func (c *cowCache) updateTxnStats() {
	if c.journal == nil {
		return
	}
	n, bytes, floor := c.journal.State()
	points, ranges, _ := c.journal.Counts()
	c.journalCount.Store(uint64(n))
	c.journalBytes.Store(bytes)
	c.journalFloor.Store(floor)
	c.journalPoints.Store(uint64(points))
	c.journalRanges.Store(uint64(ranges))
	s := c.journal.BarrierStats()
	c.journalBarrierActive.Store(s.Active)
	c.journalBarrierEffects.Store(s.EffectCapacity)
	c.journalBarrierBytes.Store(s.ByteCapacity)
	c.journalBarrierReplacement.Store(s.LogicalReplacement)
	c.journalBarrierConflicts.Store(s.Conflicts)
}
