package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func primaryRuntimeDescriptorChargeV5() uint64 {
	return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{})))
}
func primaryStateDescriptorChargeV5() uint64 {
	return retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryStateRootV5{})))
}

// The producer has already admitted this exact containing allocation before
// constructing r. Its pending contribution survives any root bank retirement.
func bindReservedPrimaryRuntimeV5(r *primaryDurableRuntimeV5, a *primaryarena.Arena) {
	r.arena = a
	r.metadata, r.metadataCharge = a.MetadataOwner(), primaryRuntimeDescriptorChargeV5()
	r.disposalFailure.payload = r
}

// This consumes only edges still owned by this SAME descriptor. Callers that
// published current transfer the actual DBState root here at terminal disposal;
// a stale runtime.current alias is never a second lifetime edge.
func releasePrimaryRuntimeV5(a *primaryarena.Arena, r *primaryDurableRuntimeV5) error {
	completion, ready, err := releasePrimaryRuntimeOriginalV6(a, r)
	// The shared driver has returned through the actual disposing scope. End this
	// adapter's runtime alias before consuming only its by-value accounting.
	r = nil
	if err != nil {
		return err
	}
	if !ready {
		return rootpublication.ErrResourceOwnership
	}
	completion.complete()
	return nil
}

func (db *DB) releaseOriginalPrimaryRuntimeV5(a *primaryarena.Arena, r *primaryDurableRuntimeV5) error {
	err := releasePrimaryRuntimeV5(a, r)
	if err != nil {
		db.retainOriginalCleanupError(err)
	}
	return err
}

// Finish the original recovered-root scratch only after the copied capsule
// owners are ready. A refusal freezes this SAME descriptor's complete disposal
// receipt so terminal cleanup cannot replay any already consumed physical edge.
func finishPrimaryRuntimeConstructionRootsV5(r *primaryDurableRuntimeV5) error {
	r.disposalMu.Lock()
	defer r.disposalMu.Unlock()
	if r.disposalAttempted {
		return r.disposalErr
	}
	var first error
	for i, ref := range r.constructionRoots {
		if ref == (primaryarena.Ref{}) {
			continue
		}
		consumed, err := r.arena.Drop(ref, nil)
		if consumed {
			r.constructionRoots[i] = primaryarena.Ref{}
		}
		if err == nil && !consumed {
			err = rootpublication.ErrResourceOwnership
		}
		if err != nil && first == nil {
			first = err
		}
	}
	if first != nil {
		r.disposalAttempted = true
		r.disposalFailure.retainCause(0, first)
		r.disposalErr = &r.disposalFailure
		r.metadata.CleanupFailed()
		return r.disposalErr
	}
	clear(r.constructionRoots[:cap(r.constructionRoots)])
	r.constructionRoots = nil
	charge := r.constructionRootsCharge
	r.constructionRootsCharge = 0
	r.metadataCharge -= charge
	r.metadata.RemovePending(charge)
	return nil
}

func finishPrimaryRuntimeRebuildScratchV5(r *primaryDurableRuntimeV5) error {
	r.disposalMu.Lock()
	defer r.disposalMu.Unlock()
	if r.disposalAttempted {
		return r.disposalErr
	}
	scratch := r.rebuildScratch
	if scratch == nil {
		return nil
	}
	var first error
	for i := range scratch.built {
		for _, ref := range []*primaryarena.Ref{&scratch.built[i].bundle.Record, &scratch.built[i].bundle.Directory} {
			if *ref == (primaryarena.Ref{}) {
				continue
			}
			consumed, err := r.arena.Drop(*ref, nil)
			if consumed {
				*ref = primaryarena.Ref{}
			}
			if err == nil && !consumed {
				err = rootpublication.ErrResourceOwnership
			}
			if err != nil && first == nil {
				first = err
			}
		}
	}
	if first == nil {
		for i, set := range scratch.privateResources {
			if err := set.ReleaseWithError(); err != nil {
				if first == nil {
					first = err
				}
			} else {
				scratch.privateResources[i] = nil
			}
		}
	}
	if first != nil {
		r.disposalAttempted = true
		r.disposalFailure.retainCause(0, first)
		r.disposalErr = &r.disposalFailure
		r.metadata.CleanupFailed()
		return r.disposalErr
	}
	*scratch = primaryRebuildScratchV5{}
	r.rebuildScratch = nil
	charge := r.rebuildScratchCharge
	r.rebuildScratchCharge = 0
	r.metadataCharge -= charge
	r.metadata.RemovePending(charge)
	return nil
}
