package db

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
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

	pager           *pager.Pager
	allocator       *freelist.Allocator
	zipper          *zipper.Zipper
	writerMu        sync.RWMutex // exported writer capture and terminal field detachment
	writerAuthority *allocatorownership.ManagedWriter

	registry  *lifecycle.ReaderRegistry
	graveyard *lifecycle.Graveyard

	refs atomic.Int32

	closeOnce sync.Once
	closeErr  error

	stableNamespaceMu     sync.Mutex
	stableNamespaceParent *os.File
	stableNamespaceProof  *rootpublication.StableNamespaceCreationProof
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
	if alloc != nil {
		authority := allocatorownership.NewManagedWriter()
		if err := alloc.BindManagedIndexWriterV1(authority); err != nil {
			panic("treedb: duplicate managed allocator writer")
		}
		g.writerAuthority = authority
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

func (g *indexGen) close() error { return g.closeHandlesV1(true) }

func (g *indexGen) closeForShutdownV1() error { return g.closeHandlesV1(false) }

func (g *indexGen) closeHandlesV1(healthyRetirement bool) error {
	g.closeOnce.Do(func() {
		// Pager closure alone is not terminal for ambiguous/recovery work.
		if healthyRetirement {
			g.detachAllocatorWriterV1(false)
		}
		if g.pager != nil {
			g.closeErr = g.pager.Close()
		}
		g.stableNamespaceMu.Lock()
		if g.stableNamespaceProof != nil {
			g.stableNamespaceProof.Release()
			g.stableNamespaceProof = nil
		}
		if g.stableNamespaceParent != nil {
			g.closeErr = errors.Join(g.closeErr, g.stableNamespaceParent.Close())
			g.stableNamespaceParent = nil
		}
		g.stableNamespaceMu.Unlock()
	})
	return g.closeErr
}

// detachAllocatorWriterV1 runs after actual writer/publication terminal.
// Reader-only index fields remain valid for held logical snapshots/root sets.
// Lock order is index writerMu -> allocator.mu; allocator never reenters index.
func (g *indexGen) detachAllocatorWriterV1(finalShutdown bool) {
	if g == nil {
		return
	}
	g.writerMu.Lock()
	defer g.writerMu.Unlock()
	alloc := g.allocator
	if alloc == nil {
		return
	}
	if finalShutdown {
		alloc.CloseCOWOwnersAfterShutdownV1()
	} else if !alloc.TryCloseCOWOwnersV1() {
		return
	}
	// Ordinary retaining writer exports preserve their indefinite backing.
	if !alloc.CanDetachManagedIndexWriterV1(g.writerAuthority) {
		return
	}
	authority := g.writerAuthority
	g.allocator, g.zipper, g.writerAuthority = nil, nil, nil
	alloc.DetachManagedIndexWriterV1(authority)
}
