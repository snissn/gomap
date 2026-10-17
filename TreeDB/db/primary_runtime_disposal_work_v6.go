package db

import (
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// primaryRuntimeDisposalCompletionV6 contains accounting only. The disposing
// call must return and its owner must detach the runtime alias before consuming
// this value. It grants no physical resource or publication authority.
type primaryRuntimeDisposalCompletionV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
}

func (c *primaryRuntimeDisposalCompletionV6) complete() {
	owner, charge := c.owner, c.charge
	c.owner, c.charge = nil, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
}

// releasePrimaryRuntimeOriginalV6 disposes only this original descriptor's
// finite operands. It stops at the first failure and retains that same receipt
// and all remaining custody; a later call cannot replay a consumed edge.
func releasePrimaryRuntimeOriginalV6(a *primaryarena.Arena, r *primaryDurableRuntimeV5) (primaryRuntimeDisposalCompletionV6, bool, error) {
	var empty primaryRuntimeDisposalCompletionV6
	if r == nil {
		return empty, true, nil
	}
	r.disposalMu.Lock()
	defer r.disposalMu.Unlock()
	if r.disposalAttempted {
		return empty, r.disposalErr == nil, r.disposalErr
	}
	if a == nil {
		a = r.arena
	}
	if a == nil || (r.arena != nil && a != r.arena) {
		return empty, false, rootpublication.ErrResourceOwnership
	}
	fail := func(err error) (primaryRuntimeDisposalCompletionV6, bool, error) {
		r.disposalAttempted = true
		r.disposalFailure.retainCause(0, err)
		r.disposalErr = &r.disposalFailure
		if r.metadata != nil {
			r.metadata.CleanupFailed()
		}
		return empty, false, r.disposalErr
	}
	closed := r.metadata != nil && r.metadata.PhysicalClosed()
	drop := func(ref *primaryarena.Ref) error {
		if *ref == (primaryarena.Ref{}) {
			return nil
		}
		if closed {
			*ref = primaryarena.Ref{}
			return nil
		}
		consumed, err := a.Drop(*ref, nil)
		if consumed {
			*ref = primaryarena.Ref{}
		}
		if err == nil && !consumed {
			err = rootpublication.ErrResourceOwnership
		}
		return err
	}
	for i := range r.records {
		for _, ref := range [4]*primaryarena.Ref{&r.records[i], &r.proofs[i], &r.closures[i], &r.proofClosures[i]} {
			err := drop(ref)
			if r.records[i] == (primaryarena.Ref{}) && r.proofs[i] == (primaryarena.Ref{}) && r.closures[i] == (primaryarena.Ref{}) && r.proofClosures[i] == (primaryarena.Ref{}) {
				r.capsuleImages[i], r.capsuleViews[i] = nil, rootpublication.PrimaryCapsuleViewV6{}
			}
			if err != nil {
				return fail(err)
			}
		}
	}
	if r.current != nil {
		if r.current.arena != a {
			return fail(rootpublication.ErrResourceOwnership)
		}
		ref := r.current.ref
		if err := drop(&ref); err != nil {
			return fail(err)
		}
		r.current = nil
	}
	for i := range r.constructionRoots {
		if err := drop(&r.constructionRoots[i]); err != nil {
			return fail(err)
		}
	}
	if scratch := r.rebuildScratch; scratch != nil {
		for i := range scratch.built {
			for _, ref := range [2]*primaryarena.Ref{&scratch.built[i].bundle.Record, &scratch.built[i].bundle.Directory} {
				if err := drop(ref); err != nil {
					return fail(err)
				}
			}
		}
	}
	releaseSet := func(slot **rootpublication.StableResourceSet) error {
		if *slot == nil {
			return nil
		}
		if err := (*slot).ReleaseWithError(); err != nil {
			return err
		}
		if (*slot).Owner() != rootpublication.ResourceOwnerReleased {
			return rootpublication.ErrResourceOwnership
		}
		*slot = nil
		return nil
	}
	for i := range r.proofResources {
		if err := releaseSet(&r.proofResources[i]); err != nil {
			return fail(err)
		}
	}
	for i := range r.slotResources {
		if err := releaseSet(&r.slotResources[i]); err != nil {
			return fail(err)
		}
	}
	if scratch := r.rebuildScratch; scratch != nil {
		for i := range scratch.privateResources {
			if err := releaseSet(&scratch.privateResources[i]); err != nil {
				return fail(err)
			}
		}
	}
	clear(r.constructionRoots[:cap(r.constructionRoots)])
	r.projection = rootpublication.PrimaryProjectionV5{}
	clear(r.slots[:])
	clear(r.proofRecords[:])
	clear(r.dataFloor[:])
	if r.rebuildScratch != nil {
		*r.rebuildScratch = primaryRebuildScratchV5{}
	}
	r.rebuildScratch, r.rebuildScratchCharge = nil, 0
	r.constructionRoots, r.constructionRootsCharge = nil, 0
	r.arena = nil
	completion := primaryRuntimeDisposalCompletionV6{r.metadata, r.metadataCharge}
	r.metadata, r.metadataCharge = nil, 0
	r.disposalAttempted = true
	return completion, true, nil
}
