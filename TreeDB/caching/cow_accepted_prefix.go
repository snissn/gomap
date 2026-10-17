package caching

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// bindAcceptedPrefix runs before the privately owned cache is exposed. Queue
// storage is one fixed producer charge, separate from maintenance utilization.
func (c *cowCache) bindAcceptedPrefix(provider backendSnapshotProvider) error {
	if c.txnOwner == nil {
		return nil
	}
	backend, ok := provider.(interface {
		PrivateCOWAcceptedPrefix(*transactionowner.Owner) (*rootpublication.Coordinator, rootpublication.LogicalFrontiers, error)
	})
	if !ok {
		return ErrCOWUnsupported
	}
	owner, initial, err := backend.PrivateCOWAcceptedPrefix(c.txnOwner)
	if err != nil {
		return err
	}
	limits := c.budget.Limits()
	capacity := limits.MaxSources * limits.MaxGenerations
	if capacity/limits.MaxSources != limits.MaxGenerations {
		return memtable.ErrCOWCapacity
	}
	// The source/generation product is an upper bound, not a promise that its
	// entire queue fits every valid byte limit. Leave producer headroom beside
	// the already reserved maintenance tranche and the finite history arrays.
	queueLimit := limits.MaxInFlightBytes / 16
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	low, high := 1, capacity
	capacity = 0
	for low <= high {
		middle := low + (high-low)/2
		bytes, e := rootpublication.AcceptedPrefixAllocationBytes(middle)
		if e == nil && bytes <= queueLimit && leaseBytes <= queueLimit-bytes {
			capacity, low = middle, middle+1
		} else {
			high = middle - 1
		}
	}
	bytes, err := rootpublication.AcceptedPrefixAllocationBytes(capacity)
	if err != nil {
		return memtable.ErrCOWCapacity
	}
	lease, err := c.budget.AcquireExternal(bytes)
	if err != nil {
		return err
	}
	if err = owner.ConfigureAcceptedPrefix(capacity, initial); err != nil {
		lease.Close()
		return err
	}
	c.prefix, c.prefixLease = owner, lease
	// Recovery has already installed this exact disk/WAL prefix. The opening
	// complete cut must start at the same frontier rather than recapturing a
	// fictitious zero-LSN version during the first empty checkpoint or Close.
	c.cut.logicalSeq, c.cut.visibleCommandLSN = initial.V, initial.C
	c.cut.frozenLogicalSeq, c.cut.frozenCommandLSN = initial.S.LogicalSequence, initial.S.CanonicalLSN
	return nil
}

func acceptedAdmissionError(err error) error {
	if err == rootpublication.ErrAcceptedCapacity {
		return memtable.ErrCOWCapacity
	}
	return err
}
