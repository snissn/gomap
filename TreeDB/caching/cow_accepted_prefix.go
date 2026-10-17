package caching

import (
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
	return nil
}

func acceptedAdmissionError(err error) error {
	if err == rootpublication.ErrAcceptedCapacity {
		return memtable.ErrCOWCapacity
	}
	return err
}
