package rootpublication

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

type stableResourceTokenReleaseStateV6 struct {
	graph    resourceReleaseFrameV6
	metadata resourceTokenMetadataReleaseV6
}

// StableResourceTokenReleaseCompletionV6 contains accounting only. Its original
// parent clears the token alias after the callback returns, then consumes this
// value. No physical operation, observation or error ownership is transferred.
type StableResourceTokenReleaseCompletionV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
}

func (c *StableResourceTokenReleaseCompletionV6) Complete() {
	if c == nil {
		return
	}
	owner, charge := c.owner, c.charge
	c.owner, c.charge = nil, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
}
