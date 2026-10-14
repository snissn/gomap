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
	wrapperBorrows    uint64
	physicalCompleted bool
	closeCompleted    bool
	failedSnapshots   *Snapshot

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
	g := &indexGen{
		id:        id,
		pager:     p,
		allocator: alloc,
		zipper:    z,
		registry:  lifecycle.NewReaderRegistry(),
		graveyard: lifecycle.NewGraveyard(),
	}
	g.closeFailure.payload = g
	g.refs.Store(1) // DB holds one ref while generation is live.
	return g
}

// The original DB registry governs the complete generation wrapper before
// its creation, independently from the physical PRIMARY owner.
func newIndexGenWithMetadata(id uint64, p *pager.Pager, alloc *freelist.Allocator, z *zipper.Zipper, metadata *retainedalloc.Owner) (*indexGen, error) {
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(indexGen{})))
	if err := metadata.AddPending(charge); err != nil {
		return nil, err
	}
	g := newIndexGen(id, p, alloc, z)
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
		defer func() {
			g.wrapperMu.Lock()
			g.closeCompleted = true
			if g.closeErr != nil {
				g.closeErr = g.originalCloseFailure(g.closeErr)
				if g.metadata != nil {
					g.metadata.CleanupFailed()
				}
				g.wrapperMu.Unlock()
				return
			}
			// A successful pager/generation action is not yet successful
			// startup Manager/LOCK cleanup. Keep the original containing
			// payload until the SAME startup completion edge consumes it.
			g.physicalCompleted = true
			g.scrubAndRefundGenerationLocked()
			g.wrapperMu.Unlock()
		}()
		g.clearPrimaryRecoveryLeasesV5()
		if g.unpublishedPrimary != nil {
			if g.closeErr = releasePrimaryRuntimeV5(g.primary, g.unpublishedPrimary); g.closeErr != nil {
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

		if g.pager != nil {
			g.closeErr = g.pager.Close()
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
					g.closeErr = errors.Join(g.closeErr, err)
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
				g.closeFailure.retainCause(0, g.closeErr)
				g.closeFailure.retainCause(1, &g.stableNamespaceFailure)
				g.closeErr = &g.closeFailure
			}
			g.stableNamespaceMu.Unlock()
		}
		if g.primary != nil {
			if g.primaryOwner == nil {
				g.closeErr = errors.Join(g.closeErr, g.primary.Close())
				return
			}
			if g.closeErr != nil {
				// Preserve this generation's EXISTING owner edge. The same
				// physical owner retains exact failed DATA/proof/parent custody;
				// closing DB discovery lists cannot orphan it or refund its debt.
				g.primaryOwner.retainFailedDataGeneration(g)
				g.primary.MetadataOwner().CleanupFailed()
			} else {
				_, debt, err := g.primaryOwner.releaseWithOutcome(nil)
				g.closeErr = err
				if debt {
					g.primaryOwner.retainFailedDataGeneration(g)
				}
			}
		}
	})
	return g.closeErr
}

func (g *indexGen) retainWrapperBorrow() {
	g.wrapperMu.Lock()
	// Each increment has one live original containing Snapshot/Prune object;
	// uint64 represents the complete physically addressable object domain.
	g.wrapperBorrows++
	g.wrapperMu.Unlock()
}

// The caller consumes this by-value original completion only after clearing
// generation aliases. No containing refund runs while wrapperMu is held.
func (g *indexGen) releaseWrapperBorrowCompletionV6(snapshot *Snapshot, cleanupErr error) (metadata *retainedalloc.Owner, charge uint64, err error) {
	g.wrapperMu.Lock()
	err = cleanupErr
	if err == nil && g.closeCompleted {
		err = g.closeErr
	}
	if err != nil {
		if snapshot != nil {
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
	g.wrapperBorrows--
	if g.physicalCompleted && g.wrapperBorrows == 0 && g.startupDB == nil && g.metadata != nil {
		metadata, charge = g.metadata, g.metadataCharge
		g.pager, g.allocator, g.zipper = nil, nil, nil
		g.registry, g.graveyard = nil, nil
		g.primary, g.primaryOwner = nil, nil
		g.metadata, g.metadataCharge = nil, 0
	}
	g.wrapperMu.Unlock()
	return metadata, charge, nil
}

func (g *indexGen) releaseWrapperBorrow(snapshot *Snapshot, cleanupErr error) {
	metadata, charge, _ := g.releaseWrapperBorrowCompletionV6(snapshot, cleanupErr)
	g = nil
	if charge != 0 {
		metadata.RemovePending(charge)
	}
}
func (g *indexGen) scrubAndRefundGeneration() {
	g.wrapperMu.Lock()
	defer g.wrapperMu.Unlock()
	g.scrubAndRefundGenerationLocked()
}
func (g *indexGen) scrubAndRefundGenerationLocked() {
	if !g.physicalCompleted || g.wrapperBorrows != 0 || g.startupDB != nil || g.closeErr != nil {
		return
	}
	if g.metadata == nil {
		return
	}
	metadata, charge := g.metadata, g.metadataCharge
	g.pager, g.allocator, g.zipper = nil, nil, nil
	g.registry, g.graveyard = nil, nil
	g.primary, g.primaryOwner = nil, nil
	g.metadata, g.metadataCharge = nil, 0
	metadata.RemovePending(charge)
}
