package db

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// continuePrimaryBankConstructionReleaseV6 drains the SAME original manifest
// construction. Its refs, queue, owner edge and first error remain in claims;
// no publication owner or cleanup registry is introduced. Ordinary release
// drains these phases with nil work. The enclosing seal clears its claims alias
// after this function returns, before consuming the accounting-only completion.
func continuePrimaryBankConstructionReleaseV6(claims *primaryBankConstructionV5, w *iterator.OrdinalScanWork) (primaryRuntimeDisposalCompletionV6, bool, error) {
	var empty primaryRuntimeDisposalCompletionV6
	if claims == nil {
		return empty, true, nil
	}
	if w != nil && !w.Reserve(4, 2*uint64(unsafe.Sizeof(*claims))+2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))+2*uint64(unsafe.Sizeof(retainedalloc.Owner{}))) {
		return empty, false, nil
	}
	claims.releaseMu.Lock()
	defer claims.releaseMu.Unlock()
	if claims.closed {
		return empty, claims.cleanupErr == nil, claims.cleanupErr
	}
	native := w != nil
	reserve := func(records, bytes uint64) bool {
		return w == nil || w.Reserve(records, bytes)
	}
	fail := func(err error) (primaryRuntimeDisposalCompletionV6, bool, error) {
		claims.closed = true
		return empty, false, claims.retainFailure(err)
	}
	if claims.releasePhase == 0 {
		if claims.owner == nil || claims.idx == nil || claims.idx.primary != claims.owner.arena {
			return fail(rootpublication.ErrResourceOwnership)
		}
		if native {
			// Capability is checked before any bank edge is consumed. Sharing at this
			// instant is not used as a promise that the later owner drop stays nonlast.
			owner := claims.owner
			owner.mu.Lock()
			if owner.arena == nil || !owner.arena.Pager().SupportsBoundedPhysicalClose() {
				owner.mu.Unlock()
				return empty, false, ErrNativeMVCCSnapshotWork
			}
			owner.namespaceMu.Lock()
			ready, err := owner.namespaceProof.SupportsNativeReleaseV6(w)
			owner.namespaceMu.Unlock()
			owner.mu.Unlock()
			if !ready || err != nil {
				return empty, false, err
			}
		}
		if !reserve(1, 2*uint64(unsafe.Sizeof(*claims))) {
			return empty, false, nil
		}
		if native {
			claims.idx.primary.InitializeReleaseQueue(&claims.releaseQueue)
		}
		claims.releaseNative, claims.releasePhase = native, 1
	}
	if claims.releaseNative != native {
		return empty, false, rootpublication.ErrResourceOwnership
	}
	if claims.releasePhase == 1 {
		for claims.releaseIndex < len(claims.refs) {
			i := claims.releaseIndex
			if !reserve(1, 2*uint64(unsafe.Sizeof(primaryarena.Ref{}))+16) {
				return empty, false, nil
			}
			if claims.refs[i] != (primaryarena.Ref{}) {
				var consumed bool
				var err error
				if native {
					consumed, err = claims.idx.primary.DropToReleaseQueue(claims.refs[i], &claims.releaseQueue, w)
				} else {
					consumed, err = claims.idx.primary.Drop(claims.refs[i], nil)
				}
				if err != nil {
					return fail(err)
				}
				if !consumed {
					return empty, false, nil
				}
				claims.refs[i] = primaryarena.Ref{}
			}
			claims.releaseIndex++
		}
		claims.releasePhase = 2
	}
	if claims.releasePhase == 2 {
		if native {
			for {
				ready, progress, err := claims.idx.primary.ReleaseNativeQueueStepV6(&claims.releaseQueue, w)
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
		claims.releasePhase = 3
	}
	if claims.releasePhase == 3 {
		if native {
			ready, err := claims.releaseOwner.Bind(claims.owner, w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
		}
		claims.releasePhase = 4
	}
	if claims.releasePhase == 4 {
		if native {
			ready, debt, err := claims.releaseOwner.ContinueWork(w)
			if err != nil {
				if debt {
					return fail(err)
				}
				// A completed nonlast edge can report a prior ordinary sibling failure.
				// Its original authority owns that debt; no callback is replayed here.
				// The original physical owner retains that already-reported error;
				// this completed native edge must not adopt an unrelated sibling debt.
			}
			if !ready {
				return empty, false, nil
			}
		} else {
			consumed, debt, err := claims.owner.releaseWithOutcome(nil)
			if !consumed || debt {
				return fail(err)
			}
			claims.cleanupErr = err
		}
		claims.releaseIndex, claims.releasePhase = len(claims.refs), 5
	}
	if claims.releasePhase == 5 {
		refs := claims.refs[:cap(claims.refs)]
		for claims.releaseIndex < len(refs) {
			if !reserve(1, 2*uint64(unsafe.Sizeof(primaryarena.Ref{}))+16) {
				return empty, false, nil
			}
			refs[claims.releaseIndex] = primaryarena.Ref{}
			claims.releaseIndex++
		}
		if !reserve(1, 2*uint64(unsafe.Sizeof(*claims))) {
			return empty, false, nil
		}
		completion := primaryRuntimeDisposalCompletionV6{owner: claims.metadata, charge: claims.charge}
		err := claims.cleanupErr
		claims.refs, claims.idx, claims.owner, claims.metadata = nil, nil, nil, nil
		claims.charge, claims.releasePhase, claims.closed = 0, 6, true
		return completion, true, err
	}
	return empty, false, rootpublication.ErrResourceOwnership
}
