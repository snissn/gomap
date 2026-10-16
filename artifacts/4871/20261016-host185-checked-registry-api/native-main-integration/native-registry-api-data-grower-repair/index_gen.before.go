package db

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/lifecycle"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// indexGen owns all index.db-scoped state that must remain valid for the
// lifetime of any snapshot/iterator pinned to it.
//
// A vacuum file swap creates a new generation and publishes it as current while
// keeping the previous generation alive until all pinned readers drain.
type indexGen struct {
	// Exact private synchronous rebuild allocator. Never substitutes the final
	// generation allocator or shares another ledger's retirement authority.
	primaryConstructionAllocator *freelist.Allocator

	id             uint64
	metadata       *retainedalloc.Owner
	metadataCharge uint64

	primary               *primaryarena.Arena
	primaryRecoveryLeases *primaryDependencyLeaseV5
	primaryOwner          *primaryArenaOwnerV5
	pager                 *pager.Pager
	allocator             *freelist.Allocator
	zipper                *zipper.Zipper

	registry  *lifecycle.ReaderRegistry
	graveyard *lifecycle.Graveyard

	refs atomic.Int32

	// Original containing-wrapper borrowers are separate from physical
	// generation/retirement refs and survive terminal physical close.
	wrapperMu         sync.Mutex
	wrapperBorrows    atomic.Uint64
	physicalCompleted bool
	closeCompleted    bool
	failedSnapshots   *Snapshot
	// Only wrapperMu accesses these fixed terminal/registry handoff flags.
	registryFailureLatched     bool
	primaryEdgeReleased        bool
	failedPhysicalEdgeRetained bool

	closeOnce sync.Once
	closeErr  error

	// Failed joint COMMIT construction retains exact custody until owner teardown.
	unpublishedSelection *durableRootSelectionV1
	unpublishedPrimary   *primaryDurableRuntimeV5

	offlineNamespaceScope       bool
	offlineScopeRetained        bool
	offlineScopeControlCharge   uint64
	offlineScopePhysicalOwner   *primaryArenaOwnerV5
	stableNamespaceMu           sync.Mutex
	stableNamespaceParent       *os.File
	stableNamespaceProof        *rootpublication.StableNamespaceCreationProof
	stableNamespaceMetadata     *retainedalloc.Owner
	stableNamespaceParentCharge uint64
	stableNamespaceFailure      primaryCleanupFailureV5
	closeFailure                originalDBCleanupFailure
	failedNext                  *indexGen

	// Startup borrows the SAME containing DB, Manager and namespace LOCK until
	// original construction and its actual physical cleanup have completed.
	// These embedded controls are included in this generation's birth charge.
	startupDB               *DB
	startupManager          *valuelog.Manager
	startupLock             *lockfile.Lock
	startupCustody          *rootpublication.OriginalCleanupCustody
	startupConstructed      bool
	startupManagerAttempted bool
	startupManagerErr       error
	startupReceipt          OpenRecoveryCleanupError
}

func newIndexGen(id uint64, p *pager.Pager, alloc *freelist.Allocator, z *zipper.Zipper) *indexGen {
	return newIndexGenWithRegistry(id, p, alloc, z, lifecycle.NewReaderRegistry())
}
func newIndexGenWithRegistry(id uint64, p *pager.Pager, alloc *freelist.Allocator, z *zipper.Zipper, registry *lifecycle.ReaderRegistry) *indexGen {
	g := &indexGen{
		id:        id,
		pager:     p,
		allocator: alloc,
		zipper:    z,
		registry:  registry,
		graveyard: lifecycle.NewGraveyard(),
	}
	g.closeFailure.payload = g
	g.refs.Store(1) // DB holds one ref while generation is live.
	return g
}

// The original DB registry governs the complete generation wrapper before
// its creation, independently from the physical PRIMARY owner.
func newIndexGenWithMetadata(id uint64, p *pager.Pager, alloc *freelist.Allocator, z *zipper.Zipper, metadata *retainedalloc.Owner) (*indexGen, error) {
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(indexGen{}))) + lifecycle.ReaderRegistryHeaderCharge()
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	g := newIndexGenWithRegistry(id, p, alloc, z, lifecycle.NewReaderRegistryWithAdmittedHeader(metadata))
	g.metadata, g.metadataCharge = metadata, charge
	return g, nil
}

func (g *indexGen) originalCloseFailure(err error) error {
	if err == nil {
		return nil
	}
	g.closeFailure.retainCause(0, err)
	return &g.closeFailure
}

func (g *indexGen) acquire() {
	g.refs.Add(1)
}

func (g *indexGen) release() int32 {
	return g.refs.Add(-1)
}

func (g *indexGen) close() error {
	g.closeOnce.Do(func() {
		var closeErr error
		defer func() {
			g.wrapperMu.Lock()
			// The physical body owns closeErr locally. A concurrent failed
			// unregister only latches the independent original registry cause.
			if registryErr := g.registryCleanupFailureLockedV6(); registryErr != nil {
				g.closeFailure.retainCause(0, closeErr)
				closeErr = registryErr
			}
			g.closeErr = closeErr
			if closeErr != nil {
				g.closeErr = g.originalCloseFailure(closeErr)
				if g.metadata != nil {
					g.metadata.CleanupFailed()
				}
				g.closeCompleted = true
				g.wrapperBorrows.Or(indexWrapperTerminalV6)
				g.wrapperMu.Unlock()
				return
			}
			// A successful pager/generation action is not yet successful
			// startup Manager/LOCK cleanup. Keep the original containing
			// payload until the SAME startup completion edge consumes it.
			g.physicalCompleted = true
			g.closeCompleted = true
			g.wrapperBorrows.Or(indexWrapperTerminalV6)
			g.wrapperMu.Unlock()
		}()
		// Failed registration need not have produced any Snapshot borrow.
		// Join the original header before dropping even the physical edge.
		g.wrapperMu.Lock()
		registryErr := g.registryCleanupFailureLockedV6()
		g.wrapperMu.Unlock()
		if registryErr != nil {
			return
		}
		if g.primaryConstructionAllocator != nil {
			if closeErr = g.primaryConstructionAllocator.DrainConstructionLedgerV6(); closeErr != nil {
				if g.primaryOwner != nil {
					g.retainFailedPhysicalEdgeV6(closeErr)
				}
				if g.primary != nil {
					g.primary.MetadataOwner().CleanupFailed()
				}
				return
			}
			g.primaryConstructionAllocator = nil
		}

		g.clearPrimaryRecoveryLeasesV5()
		if g.unpublishedPrimary != nil {
			if closeErr = releasePrimaryRuntimeV5(g.primary, g.unpublishedPrimary); closeErr != nil {
				// A late constructor failure can leave its exact runtime here even
				// after ledger drain. Preserve the containing generation and its
				// physical-owner edge just as for failed ledger cleanup above.
				if g.primaryOwner != nil {
					g.retainFailedPhysicalEdgeV6(closeErr)
				}
				if g.primary != nil {
					g.primary.MetadataOwner().CleanupFailed()
				}
				return
			}
			g.unpublishedPrimary = nil
		}
		if g.unpublishedSelection != nil {
			for _, r := range g.unpublishedSelection.SlotResources {
				r.Release()
			}
			g.unpublishedSelection = nil
		}

		if g.allocator != nil {
			if closeErr = g.allocator.CloseReservationLedgerV6(); closeErr != nil {
				// A failed ledger join retains this generation's existing physical owner
				// edge. Do not close the pager, shared arena, or refund another ledger.
				if g.primaryOwner != nil {
					g.retainFailedPhysicalEdgeV6(closeErr)
				}
				if g.primary != nil {
					g.primary.MetadataOwner().CleanupFailed()
				}
				return
			}
		}
		if g.pager != nil {
			closeErr = g.pager.Close()
		}
		if !g.offlineNamespaceScope {
			g.stableNamespaceMu.Lock()
			if g.stableNamespaceProof != nil {
				if g.stableNamespaceMetadata == nil {
					g.stableNamespaceProof.Release()
					g.stableNamespaceProof = nil
				} else {
					if err := g.stableNamespaceProof.ReleaseWithError(); err == nil {
						g.stableNamespaceProof = nil
					} else {
						g.stableNamespaceFailure.causes[0] = err
					}
				}
			}
			if g.stableNamespaceParent != nil {
				err := g.stableNamespaceParent.Close()
				if g.stableNamespaceMetadata == nil {
					closeErr = errors.Join(closeErr, err)
					g.stableNamespaceParent = nil
				} else if err == nil {
					g.stableNamespaceParent = nil
					g.stableNamespaceMetadata.RemovePending(g.stableNamespaceParentCharge)
					g.stableNamespaceParentCharge = 0
				} else {
					g.stableNamespaceFailure.causes[1] = err
				}
			}
			if g.stableNamespaceFailure.causes[0] != nil || g.stableNamespaceFailure.causes[1] != nil {
				g.stableNamespaceMetadata.CleanupFailed()
				g.closeFailure.retainCause(0, closeErr)
				g.closeFailure.retainCause(1, &g.stableNamespaceFailure)
				closeErr = &g.closeFailure
			}
			g.stableNamespaceMu.Unlock()
		}
		if g.primary != nil {
			if g.primaryOwner == nil {
				closeErr = errors.Join(closeErr, g.primary.Close())
				return
			}
			// Linearize the original physical-owner handoff against late
			// registry failure. No header callback can latch between this
			// check and the existing release; later Snapshot failure owns its
			// separate, already retained physical edge.
			g.wrapperMu.Lock()
			if registryErr := g.registryCleanupFailureLockedV6(); registryErr != nil {
				g.closeFailure.retainCause(0, closeErr)
				closeErr = registryErr
			}
			if closeErr != nil {
				// Preserve this generation's EXISTING owner edge. The same
				// physical owner retains exact failed DATA/proof/parent custody;
				// closing DB discovery lists cannot orphan it or refund its debt.
				g.retainFailedPhysicalEdgeLockedV6()
				g.primary.MetadataOwner().CleanupFailed()
			} else {
				consumed, debt, err := g.primaryOwner.releaseWithOutcome(nil)
				g.primaryEdgeReleased = consumed && !debt
				closeErr = err
				if debt || !consumed {
					g.closeFailure.retainCause(0, err)
					g.retainFailedPhysicalEdgeLockedV6()
				}
			}
			g.wrapperMu.Unlock()
		}
	})
	g.wrapperMu.Lock()
	defer g.wrapperMu.Unlock()
	if registryErr := g.registryCleanupFailureLockedV6(); registryErr != nil {
		return registryErr
	}
	return g.closeErr
}

// The terminal bit and borrow count share one atomic handoff. A release
// either leaves the original physical close caller to observe its decrement,
// or observes the fixed close outcome and joins the locked terminal/error path.
const indexWrapperTerminalV6 = uint64(1) << 63
const indexWrapperCountMaskV6 = indexWrapperTerminalV6 - 1

func (g *indexGen) wrapperBorrowCountV6() uint64 {
	return g.wrapperBorrows.Load() & indexWrapperCountMaskV6
}

func (g *indexGen) retainWrapperBorrow() {
	// Capture already owns the generation's original physical/registry edge.
	// The addressable Snapshot domain is smaller than the 63-bit count.
	if g.wrapperBorrows.Add(1)&indexWrapperCountMaskV6 == 0 {
		panic("generation scope borrow overflow")
	}
}

func (g *indexGen) dropWrapperBorrowV6() uint64 {
	state := g.wrapperBorrows.Add(^uint64(0))
	if state&indexWrapperCountMaskV6 == indexWrapperCountMaskV6 {
		panic("generation completed scope borrow underflow")
	}
	return state
}

// The caller consumes this by-value original completion only after clearing
// generation aliases. No containing refund runs while wrapperMu is held.
func (g *indexGen) releaseWrapperBorrowCompletionV6(snapshot *Snapshot, cleanupErr error) (metadata *retainedalloc.Owner, charge uint64, err error) {
	consumed := false
	if cleanupErr == nil {
		state := g.dropWrapperBorrowV6()
		if state&indexWrapperTerminalV6 == 0 {
			// Last access through this scope's alias. Concurrent close owns
			// its own borrow and observes this atomic decrement.
			return nil, 0, nil
		}
		consumed = true
	}
	g.wrapperMu.Lock()
	err = cleanupErr
	if registryErr := g.registryCleanupFailureLockedV6(); err == nil {
		err = registryErr
	}
	if err == nil && g.closeCompleted {
		err = g.closeErr
	}
	if err != nil {
		if snapshot != nil {
			// A failed Snapshot keeps its exact borrow and payload. Failed
			// physical close cannot detach/refund terminal metadata.
			if consumed {
				g.wrapperBorrows.Add(1)
			}
			snapshot.generationFailedNext = g.failedSnapshots
			g.failedSnapshots = snapshot
		}
		if g.metadata != nil {
			g.metadata.CleanupFailed()
		}
		g.wrapperMu.Unlock()
		if snapshot != nil {
			g.closeFailure.retainCause(2, err)
		}
		return nil, 0, err
	}
	if !consumed {
		g.dropWrapperBorrowV6()
	}
	metadata, charge, err = g.detachGenerationMetadataLockedV6()
	g.wrapperMu.Unlock()
	return metadata, charge, err
}

func (g *indexGen) releaseWrapperBorrow(snapshot *Snapshot, cleanupErr error) {
	metadata, charge, _ := g.releaseWrapperBorrowCompletionV6(snapshot, cleanupErr)
	g = nil
	if charge != 0 {
		metadata.RemovePending(charge)
	}
}

// Close completion remains in the original wrapper borrow until the physical
// close call has returned. The caller consumes only by-value accounting after
// clearing its generation/discovery aliases.
func (g *indexGen) closeWithCompletionV6() (metadata *retainedalloc.Owner, charge uint64, err error) {
	g.retainWrapperBorrow()
	err = g.close()
	g.wrapperMu.Lock()
	// This edge covers the physical call scope only. A failed close retains
	// the SAME closeFailure payload and metadata debt, while each failed
	// Snapshot retains its separate original wrapper borrow.
	g.dropWrapperBorrowV6()
	if err == nil {
		metadata, charge, err = g.detachGenerationMetadataLockedV6()
	}
	g.wrapperMu.Unlock()
	return metadata, charge, err
}

func (g *indexGen) detachGenerationMetadataCompletionV6() (metadata *retainedalloc.Owner, charge uint64, err error) {
	g.wrapperMu.Lock()
	if g.closeCompleted {
		err = g.closeErr
	}
	if err == nil {
		metadata, charge, err = g.detachGenerationMetadataLockedV6()
	}
	g.wrapperMu.Unlock()
	return metadata, charge, err
}

// Caller holds wrapperMu. The same closeFailure retains the original payload;
// a registry error alone is never permission to refund its containing header.
func (g *indexGen) registryCleanupFailureLockedV6() error {
	if g.registry == nil {
		return nil
	}
	cause := g.registry.OriginalCleanupError()
	if cause == nil {
		return nil
	}
	// closeErr belongs to the closeOnce body until its terminal locked merge.
	// Never assign it from a concurrently completing Snapshot.
	g.closeFailure.retainCause(4, cause)
	if !g.registryFailureLatched {
		g.registryFailureLatched = true
		if !g.physicalCompleted && g.primaryOwner != nil {
			g.retainFailedPhysicalEdgeLockedV6()
		} else if g.offlineScopePhysicalOwner != nil && !g.offlineScopeRetained {
			g.offlineScopePhysicalOwner.retainFailedDataGeneration(g)
			g.offlineScopeRetained = true
		}
	}
	if g.metadata != nil {
		g.metadata.CleanupFailed()
	}
	if g.stableNamespaceMetadata != nil {
		g.stableNamespaceMetadata.CleanupFailed()
	}
	return &g.closeFailure
}

func (g *indexGen) retainFailedPhysicalEdgeV6(cause error) {
	g.closeFailure.retainCause(0, cause)
	g.wrapperMu.Lock()
	g.retainFailedPhysicalEdgeLockedV6()
	g.wrapperMu.Unlock()
}

// wrapperMu protects the SAME preexisting physical edge, including deduplication
// with a concurrent registry failure. No edge is recreated after its release.
func (g *indexGen) retainFailedPhysicalEdgeLockedV6() {
	if g.primaryOwner != nil && !g.primaryEdgeReleased && !g.failedPhysicalEdgeRetained {
		g.failedPhysicalEdgeRetained = true
		g.primaryOwner.retainFailedDataGeneration(g)
	}
}

func (g *indexGen) detachGenerationMetadataLockedV6() (*retainedalloc.Owner, uint64, error) {
	if err := g.registryCleanupFailureLockedV6(); err != nil {
		return nil, 0, err
	}
	if !g.physicalCompleted || g.wrapperBorrowCountV6() != 0 || g.startupDB != nil || g.closeErr != nil || g.metadata == nil {
		return nil, 0, nil
	}
	metadata, charge := g.metadata, g.metadataCharge
	g.pager, g.allocator, g.zipper = nil, nil, nil
	g.registry, g.graveyard = nil, nil
	g.primary, g.primaryOwner = nil, nil
	g.metadata, g.metadataCharge = nil, 0
	return metadata, charge, nil
}
