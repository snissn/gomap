package db

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

type metadataDisposalCompletionV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
}

func (c *metadataDisposalCompletionV6) complete() {
	owner, charge := c.owner, c.charge
	c.owner, c.charge = nil, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
}
