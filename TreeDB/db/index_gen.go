package db

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
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

	closeMu       sync.Mutex
	creator       *residentcredit.Scope
	closeErr      error
	handlesClosed atomic.Bool // publishes successful exact handle/namespace terminal

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

// testIndexClosePager is a serial test-only fault before actual Pager Close.
var testIndexClosePager func(*pager.Pager) error

func (g *indexGen) closeHandlesV1(healthyRetirement bool) error {
	g.closeMu.Lock()
	defer g.closeMu.Unlock()
	if g.handlesClosed.Load() {
		if healthyRetirement {
			g.detachAllocatorWriterV1(false)
			g.releaseConstructorCreatorLockedV1()
		}
		return nil
	}
	// No terminal once caches failure. Exact remaining holders survive retry.
	if healthyRetirement {
		g.detachAllocatorWriterV1(false)
	}
	if g.pager != nil {
		if testIndexClosePager != nil {
			if err := testIndexClosePager(g.pager); err != nil {
				g.closeErr = err
				return err
			}
		}
		if err := g.pager.Close(); err != nil {
			g.closeErr = err
			return err
		}
	}
	g.stableNamespaceMu.Lock()
	defer g.stableNamespaceMu.Unlock()
	if g.stableNamespaceParent != nil {
		if err := g.stableNamespaceParent.Close(); err != nil {
			if _, statErr := g.stableNamespaceParent.Stat(); errors.Is(statErr, os.ErrClosed) {
				g.stableNamespaceParent = nil
			}
			g.closeErr = err
			return err
		}
		g.stableNamespaceParent = nil
	}
	if g.stableNamespaceProof != nil {
		g.stableNamespaceProof.Release()
		g.stableNamespaceProof = nil
	}
	g.closeErr = nil
	g.handlesClosed.Store(true)
	g.releaseConstructorCreatorLockedV1()
	return nil
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

// Constructor control stays on the real generation until both physical handles
// and its actual allocator writer terminal are complete. Shutdown keeps the
// generation on idxAll across failed runtime cleanup, even after mmap Close.
func (g *indexGen) releaseConstructorCreatorLockedV1() {
	g.writerMu.RLock()
	terminal := g.writerAuthority == nil
	g.writerMu.RUnlock()
	if terminal && g.creator != nil {
		g.creator.ReleaseStableMetadata()
		g.creator = nil
	}
}
func (db *DB) finishClosedIndexConstructorV1(g *indexGen) {
	if g == nil {
		return
	}
	g.closeMu.Lock()
	defer g.closeMu.Unlock()
	if !g.handlesClosed.Load() {
		return
	}
	g.releaseConstructorCreatorLockedV1()
	if g.creator != nil {
		return
	}
	db.idxMu.Lock()
	if db.idxAll[g.id] == g {
		delete(db.idxAll, g.id)
	}
	db.idxMu.Unlock()
}

// constructorTerminalCompleteV1 inspects actual completion under the same gates
// used by physical cleanup and writer detachment. A closed Pager alone cannot
// remove an ordinary ghost whose allocator publication owner is still live.
// This is allocation-free and never joins operations or performs cleanup IO.
func (g *indexGen) constructorTerminalCompleteV1() bool {
	if g == nil {
		return true
	}
	g.closeMu.Lock()
	defer g.closeMu.Unlock()
	g.writerMu.RLock()
	defer g.writerMu.RUnlock()
	return g.handlesClosed.Load() && g.creator == nil && g.writerAuthority == nil
}
