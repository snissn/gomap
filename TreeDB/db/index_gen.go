package db

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/freelist"
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

	pager     *pager.Pager
	allocator *freelist.Allocator
	zipper    *zipper.Zipper

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
