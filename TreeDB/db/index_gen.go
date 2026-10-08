package db

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
	id uint64

	primary               *primaryarena.Arena
	primaryRecoveryLeases *primaryDependencyLeaseV5
	primaryOwner          *primaryArenaOwnerV5
	pager                 *pager.Pager
	allocator             *freelist.Allocator
	zipper                *zipper.Zipper

	registry  *lifecycle.ReaderRegistry
	graveyard *lifecycle.Graveyard

	refs atomic.Int32

	closeOnce sync.Once
	closeErr  error

	// Failed joint COMMIT construction retains exact custody until owner teardown.
	unpublishedSelection *durableRootSelectionV1
	unpublishedPrimary   *primaryDurableRuntimeV5

	stableNamespaceMu           sync.Mutex
	stableNamespaceParent       *os.File
	stableNamespaceProof        *rootpublication.StableNamespaceCreationProof
	stableNamespaceMetadata     *retainedalloc.Owner
	stableNamespaceParentCharge uint64
	stableNamespaceFailure      primaryCleanupFailureV5
	closeFailure                primaryCleanupFailureV5
	failedNext                  *indexGen
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
	g.refs.Store(1) // DB holds one ref while generation is live.
	return g
}

func (g *indexGen) acquire() {
	g.refs.Add(1)
}

func (g *indexGen) release() int32 {
	return g.refs.Add(-1)
}

func (g *indexGen) close() error {
	g.closeOnce.Do(func() {
		g.clearPrimaryRecoveryLeasesV5()
		if g.unpublishedPrimary != nil {
			releasePrimaryRuntimeV5(g.primary, g.unpublishedPrimary)
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
			} else if err == nil || errors.Is(err, os.ErrClosed) {
				g.stableNamespaceParent = nil
				g.stableNamespaceMetadata.RemovePending(g.stableNamespaceParentCharge)
				g.stableNamespaceParentCharge = 0
			} else {
				g.stableNamespaceFailure.causes[1] = err
			}
		}
		if g.stableNamespaceFailure.causes[0] != nil || g.stableNamespaceFailure.causes[1] != nil {
			g.stableNamespaceMetadata.CleanupFailed()
			g.closeFailure.causes[0] = g.closeErr
			g.closeFailure.causes[1] = &g.stableNamespaceFailure
			g.closeErr = &g.closeFailure
		}
		g.stableNamespaceMu.Unlock()
		if g.primary != nil {
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
