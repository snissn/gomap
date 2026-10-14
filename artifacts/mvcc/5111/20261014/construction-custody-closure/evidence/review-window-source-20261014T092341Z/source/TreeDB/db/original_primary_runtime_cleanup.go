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
	if r == nil {
		return nil
	}
	r.disposalMu.Lock()
	defer r.disposalMu.Unlock()
	if r.disposalAttempted {
		return r.disposalErr
	}
	r.disposalAttempted = true
	if a == nil {
		a = r.arena
	}
	var first error
	// Only successful physical teardown of this SAME admitted owner can
	// consume bank storage wholesale. A failed Pager/Arena Close is not proof.
	if r.metadata != nil && r.metadata.PhysicalClosed() {
		clear(r.records[:])
		clear(r.proofs[:])
		r.current = nil
		clear(r.constructionRoots)
		if scratch := r.rebuildScratch; scratch != nil {
			for i := range scratch.built {
				scratch.built[i].bundle.Record = primaryarena.Ref{}
				scratch.built[i].bundle.Directory = primaryarena.Ref{}
			}
		}
	}
	for i := range r.records {
		if r.records[i] != (primaryarena.Ref{}) {
			consumed, err := a.Drop(r.records[i], nil)
			if err == nil && !consumed {
				err = rootpublication.ErrResourceOwnership
			}
			if consumed {
				r.records[i] = primaryarena.Ref{}
			}
			if err != nil && first == nil {
				first = err
			}
		}
		if r.proofs[i] != (primaryarena.Ref{}) {
			consumed, err := a.Drop(r.proofs[i], nil)
			if err == nil && !consumed {
				err = rootpublication.ErrResourceOwnership
			}
			if consumed {
				r.proofs[i] = primaryarena.Ref{}
			}
			if err != nil && first == nil {
				first = err
			}
		}
	}
	// These views alias the old record's admitted image. Once this original
	// descriptor consumed both record/proof edges, remove its image aliases
	// before a later failed resource callback can retain the descriptor.
	for i := range r.records {
		if r.records[i] == (primaryarena.Ref{}) && r.proofs[i] == (primaryarena.Ref{}) {
			r.capsuleImages[i] = nil
			r.capsuleViews[i] = rootpublication.PrimaryCapsuleViewV6{}
		}
	}
	if r.current != nil {
		consumed, err := r.current.arena.Drop(r.current.ref, nil)
		if err == nil && !consumed {
			err = rootpublication.ErrResourceOwnership
		}
		if consumed {
			r.current = nil
		}
		if err != nil && first == nil {
			first = err
		}
	}
	for i, ref := range r.constructionRoots {
		if ref == (primaryarena.Ref{}) {
			continue
		}
		consumed, err := a.Drop(ref, nil)
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
	if scratch := r.rebuildScratch; scratch != nil {
		for i := range scratch.built {
			for _, ref := range []*primaryarena.Ref{&scratch.built[i].bundle.Record, &scratch.built[i].bundle.Directory} {
				if *ref == (primaryarena.Ref{}) {
					continue
				}
				consumed, err := a.Drop(*ref, nil)
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
	}
	// Refused root preparation cannot withdraw any original resource edge.
	if first == nil {
		for i, set := range r.proofResources {
			if set == nil {
				continue
			}
			err := set.ReleaseWithError()
			if err == nil && set.Owner() != rootpublication.ResourceOwnerReleased {
				err = rootpublication.ErrResourceOwnership
			}
			if err != nil {
				if first == nil {
					first = err
				}
			} else {
				r.proofResources[i] = nil
			}
		}
		for i, set := range r.slotResources {
			if set == nil {
				continue
			}
			err := set.ReleaseWithError()
			if err == nil && set.Owner() != rootpublication.ResourceOwnerReleased {
				err = rootpublication.ErrResourceOwnership
			}
			if err != nil {
				if first == nil {
					first = err
				}
			} else {
				r.slotResources[i] = nil
			}
		}
		if scratch := r.rebuildScratch; scratch != nil {
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
	}
	if first != nil {
		r.disposalFailure.retainCause(0, first)
		r.disposalErr = &r.disposalFailure
		if r.metadata != nil {
			r.metadata.CleanupFailed()
		}
		return r.disposalErr
	}
	r.projection = rootpublication.PrimaryProjectionV5{}
	clear(r.slots[:])
	clear(r.proofRecords[:])
	clear(r.dataFloor[:])
	clear(r.capsuleImages[:])
	clear(r.capsuleViews[:])
	owner, charge := r.metadata, r.metadataCharge
	if r.rebuildScratch != nil {
		*r.rebuildScratch = primaryRebuildScratchV5{}
	}
	r.rebuildScratch, r.rebuildScratchCharge = nil, 0
	clear(r.constructionRoots[:cap(r.constructionRoots)])
	r.constructionRoots, r.constructionRootsCharge = nil, 0
	r.arena = nil
	r.metadata, r.metadataCharge = nil, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
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
