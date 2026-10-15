package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
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

// primaryResourceReleaseWithWorkV6 returns accounting only after physical Set
// completion. The original containing runtime must detach its Set alias before
// consuming that value; a callback must never refund the still-referenced Set.
type primaryResourceReleaseWithWorkV6 func(*rootpublication.StableResourceSet, *iterator.OrdinalScanWork) (rootpublication.StableResourceTokenReleaseCompletionV6, bool, error)

func releasePrimaryResourceSetWithWorkV6(set *rootpublication.StableResourceSet, w *iterator.OrdinalScanWork) (rootpublication.StableResourceTokenReleaseCompletionV6, bool, error) {
	return set.ReleaseFromWithWorkV6(rootpublication.ResourceOwnerBuilder, w)
}

// continuePrimaryRuntimeDisposalV6 shares the original descriptor's exact
// operands with ordinary disposal. Native disposal uses only its own arena
// queue and an actual work-bearing resource driver; it never falls back to an
// ordinary callback. Refusal retains the original phase and containing charge.
// A real error freezes this descriptor's original first-error custody.
// The ordinary driver also stops at that first failed operation. Remaining
// operands stay on this original descriptor as debt; it does not attempt later
// drops merely to dispose more storage after physical failure.
func continuePrimaryRuntimeDisposalV6(a *primaryarena.Arena, r *primaryDurableRuntimeV5, w *iterator.OrdinalScanWork,
	release primaryResourceReleaseWithWorkV6,
) (primaryRuntimeDisposalCompletionV6, bool, error) {
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
	if a == nil || (r.arena != nil && a != r.arena) || (w != nil && release == nil) {
		return empty, false, rootpublication.ErrResourceOwnership
	}
	admit := func(records, bytes uint64) bool { return w == nil || w.Reserve(records, bytes) }
	fail := func(err error) (primaryRuntimeDisposalCompletionV6, bool, error) {
		r.disposalAttempted = true
		r.disposalFailure.retainCause(0, err)
		r.disposalErr = &r.disposalFailure
		if r.metadata != nil {
			r.metadata.CleanupFailed()
		}
		return empty, false, r.disposalErr
	}
	if r.disposalPhase == 0 {
		if !admit(1, 2*uint64(unsafe.Sizeof(r.disposalQueue))+64) {
			return empty, false, nil
		}
		r.disposalNative = w != nil
		if r.disposalNative {
			a.InitializeReleaseQueue(&r.disposalQueue)
		}
		r.disposalPhase = 1
	} else if r.disposalNative != (w != nil) {
		// An ordinary owner may not silently take over a cursor-owned physical queue.
		return empty, false, rootpublication.ErrResourceOwnership
	}
	closed := r.metadata != nil && r.metadata.PhysicalClosed()
	drop := func(ref *primaryarena.Ref) (bool, error) {
		if !admit(1, 2*uint64(unsafe.Sizeof(*ref))) {
			return false, nil
		}
		if *ref == (primaryarena.Ref{}) {
			return true, nil
		}
		if closed {
			*ref = primaryarena.Ref{}
			return true, nil
		}
		var consumed bool
		var err error
		if r.disposalNative {
			consumed, err = a.DropToReleaseQueue(*ref, &r.disposalQueue, w)
		} else {
			consumed, err = a.Drop(*ref, nil)
		}
		if consumed {
			*ref = primaryarena.Ref{}
		}
		return consumed, err
	}
	// The finite slot arrays, current root, construction roots and recovery
	// scratch each retain their own index. No refused edge is visited again after
	// it was actually consumed.
	for r.disposalPhase == 1 && r.disposalIndex < len(r.records)*4 {
		i := r.disposalIndex / 4
		refs := [4]*primaryarena.Ref{&r.records[i], &r.proofs[i], &r.closures[i], &r.proofClosures[i]}
		ref := refs[r.disposalIndex%4]
		// The image aliases are scrubbed in the same admitted edge operation, even
		// if a later original root fails. No immutable reader wrapper is modified.
		if !admit(1, 2*uint64(unsafe.Sizeof(r.capsuleImages[i]))+2*uint64(unsafe.Sizeof(r.capsuleViews[i]))) {
			return empty, false, nil
		}
		ready, err := drop(ref)
		if r.records[i] == (primaryarena.Ref{}) && r.proofs[i] == (primaryarena.Ref{}) && r.closures[i] == (primaryarena.Ref{}) && r.proofClosures[i] == (primaryarena.Ref{}) {
			r.capsuleImages[i] = nil
			r.capsuleViews[i] = rootpublication.PrimaryCapsuleViewV6{}
		}
		if err != nil {
			return fail(err)
		}
		if !ready {
			return empty, false, nil
		}
		r.disposalIndex++
	}
	if r.disposalPhase == 1 {
		if !admit(1, 2*uint64(unsafe.Sizeof(r.capsuleImages))+2*uint64(unsafe.Sizeof(r.capsuleViews))) {
			return empty, false, nil
		}
		clear(r.capsuleImages[:])
		clear(r.capsuleViews[:])
		r.disposalPhase, r.disposalIndex = 2, 0
	}
	if r.disposalPhase == 2 {
		if r.current != nil {
			if r.current.arena != a {
				return fail(rootpublication.ErrResourceOwnership)
			}
			ref := r.current.ref
			ready, err := drop(&ref)
			if ready {
				r.current = nil
			}
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
		}
		r.disposalPhase = 3
	}
	for r.disposalPhase == 3 && r.disposalIndex < len(r.constructionRoots) {
		ready, err := drop(&r.constructionRoots[r.disposalIndex])
		if err != nil {
			return fail(err)
		}
		if !ready {
			return empty, false, nil
		}
		r.disposalIndex++
	}
	if r.disposalPhase == 3 {
		r.disposalPhase, r.disposalIndex = 4, 0
	}
	if r.disposalPhase == 4 {
		if scratch := r.rebuildScratch; scratch != nil {
			for r.disposalIndex < len(scratch.built)*2 {
				i := r.disposalIndex / 2
				ref := &scratch.built[i].bundle.Record
				if r.disposalIndex%2 != 0 {
					ref = &scratch.built[i].bundle.Directory
				}
				ready, err := drop(ref)
				if err != nil {
					return fail(err)
				}
				if !ready {
					return empty, false, nil
				}
				r.disposalIndex++
			}
		}
		r.disposalPhase, r.disposalIndex = 5, 0
	}
	if r.disposalPhase == 5 {
		if r.disposalNative && !closed {
			for {
				ready, progress, err := a.ReleaseQueueStep(&r.disposalQueue, w)
				if err != nil {
					return fail(err)
				}
				if !ready {
					return empty, false, nil
				}
				if !progress {
					break
				}
			}
		}
		r.disposalPhase = 6
	}
	releaseSet := func(slot **rootpublication.StableResourceSet) (bool, error) {
		if !admit(1, 2*uint64(unsafe.Sizeof(*slot))) {
			return false, nil
		}
		if *slot == nil {
			return true, nil
		}
		var ready bool
		var err error
		var completion rootpublication.StableResourceTokenReleaseCompletionV6
		if r.disposalNative {
			completion, ready, err = release(*slot, w)
		} else {
			err = (*slot).ReleaseWithError()
			ready = err == nil
		}
		if err != nil || !ready {
			return false, err
		}
		if (*slot).Owner() != rootpublication.ResourceOwnerReleased {
			return false, rootpublication.ErrResourceOwnership
		}
		*slot = nil
		// The physical callback has returned and this original runtime's Set
		// alias has ended. Only now may its accounting-only receipt refund.
		completion.Complete()
		return true, nil
	}
	for r.disposalPhase == 6 && r.disposalIndex < len(r.proofResources)+len(r.slotResources) {
		i := r.disposalIndex
		var slot **rootpublication.StableResourceSet
		if i < len(r.proofResources) {
			slot = &r.proofResources[i]
		} else {
			slot = &r.slotResources[i-len(r.proofResources)]
		}
		ready, err := releaseSet(slot)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return empty, false, nil
		}
		r.disposalIndex++
	}
	if r.disposalPhase == 6 {
		r.disposalPhase, r.disposalIndex = 7, 0
	}
	if r.disposalPhase == 7 {
		if scratch := r.rebuildScratch; scratch != nil {
			for r.disposalIndex < len(scratch.privateResources) {
				ready, err := releaseSet(&scratch.privateResources[r.disposalIndex])
				if err != nil {
					return fail(err)
				}
				if !ready {
					return empty, false, nil
				}
				r.disposalIndex++
			}
		}
		r.disposalPhase, r.disposalIndex = 8, 0
	}
	// Clear any unused admitted capacity a cell at a time before detaching it.
	for r.disposalPhase == 8 && r.disposalIndex < cap(r.constructionRoots) {
		if !admit(1, 2*uint64(unsafe.Sizeof(primaryarena.Ref{}))) {
			return empty, false, nil
		}
		r.constructionRoots[:cap(r.constructionRoots)][r.disposalIndex] = primaryarena.Ref{}
		r.disposalIndex++
	}
	if r.disposalPhase == 8 {
		bytes := 2 * uint64(unsafe.Sizeof(*r))
		if r.rebuildScratch != nil {
			bytes += 2 * uint64(unsafe.Sizeof(*r.rebuildScratch))
		}
		if !admit(1, bytes) {
			return empty, false, nil
		}
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
		r.disposalAttempted, r.disposalPhase = true, 9
		return completion, true, nil
	}
	return empty, false, rootpublication.ErrResourceOwnership
}
