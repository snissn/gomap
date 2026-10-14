package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"sync"
)

type primaryDependencyLeaseV5 struct {
	mu     sync.Mutex
	owner  *primaryArenaOwnerV5
	root   uint64
	ref    primaryarena.Ref
	closed bool
}

func (lease *primaryDependencyLeaseV5) activate() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || lease.ref.PageID != 0 {
		return nil
	}
	ref, ready, e := lease.owner.arena.BorrowDependency(lease.root, nil)
	if !ready || e != nil {
		return errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	ready, e = lease.owner.arena.Acquire(ref, nil)
	if !ready || e != nil {
		return errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	lease.ref = ref
	return nil
}
func (lease *primaryDependencyLeaseV5) release() {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return
	}
	lease.closed = true
	if lease.ref.PageID != 0 {
		_, _ = lease.owner.arena.Drop(lease.ref, nil)
		lease.ref = primaryarena.Ref{}
	}
	_, _ = lease.owner.release(nil)
}
func (db *DB) pinPrimaryDependencyV5(idx *indexGen, ref rootpublication.DependencyDirectoryRefV2, extent uint64) (*rootpublication.DependencyDirectoryV2, error) {
	if idx == nil || idx.primaryOwner == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	ready, e := idx.primaryOwner.retain(nil)
	if !ready || e != nil {
		return nil, errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	lease := &primaryDependencyLeaseV5{owner: idx.primaryOwner, root: ref.RootPageID}
	if idx.primary.Recovered() {
		if e := lease.activate(); e != nil {
			lease.release()
			return nil, e
		}
	} else {
		idx.primaryRecoveryLeases = append(idx.primaryRecoveryLeases, lease)
	}
	directory, e := rootpublication.NewLocalPrimaryDependencyDirectoryV5(lease.owner.arena.Pager(), ref, extent, lease.release)
	if e != nil {
		lease.release()
	}
	return directory, e
}

// Ordinary changed-path metadata allocation owns genuine bank claims. All new
// edges are sealed child-first, then private construction refs are released.
// These are ordinary producer operations, not bounded native preparation APIs.
type primaryDependencyAllocatorV5 struct {
	idx  *indexGen
	refs []primaryarena.Ref
}

func (alloc *primaryDependencyAllocatorV5) Alloc(_ uint64) (uint64, error) {
	claim, ready, e := alloc.idx.primary.PrepareClaim(primaryarena.Dependency, nil)
	if !ready || e != nil {
		return 0, errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	for {
		ref, ready, e := claim.Step(nil)
		if e != nil {
			return 0, e
		}
		if ready {
			alloc.refs = append(alloc.refs, ref)
			return ref.PageID, nil
		}
	}
}
func (alloc *primaryDependencyAllocatorV5) release() {
	for _, ref := range alloc.refs {
		_, _ = alloc.idx.primary.Drop(ref, nil)
	}
	alloc.refs = nil
}
func (alloc *primaryDependencyAllocatorV5) seal(root uint64) error {
	claimed := make(map[uint64]primaryarena.Ref, len(alloc.refs))
	for _, ref := range alloc.refs {
		claimed[ref.PageID] = ref
	}
	visiting := make(map[uint64]bool)
	var visit func(uint64) error
	visit = func(id uint64) error {
		ref, owned := claimed[id]
		if !owned {
			return nil
		}
		if visiting[id] {
			return rootpublication.ErrDependencyManifestFormat
		}
		visiting[id] = true
		image, e := alloc.idx.pager.ReadPage(id)
		if e != nil {
			return e
		}
		n := node.NewNode(image)
		if n.Type() == page.PageTypeInternal {
			for i := uint16(0); i < n.Count(); i++ {
				_, child, e := n.GetInternalEntryRefView(i)
				if e != nil || child.Kind != page.ChildRefPage || !primaryarena.IsPage(child.Page) {
					return rootpublication.ErrDependencyManifestFormat
				}
				if e = visit(child.Page); e != nil {
					return e
				}
			}
		}
		ready, e := alloc.idx.primary.SealMetadata(ref, image, nil)
		if e != nil || !ready {
			return errors.Join(e, rootpublication.ErrResourceOwnership)
		}
		delete(claimed, id)
		delete(visiting, id)
		return nil
	}
	if e := visit(root); e != nil {
		return e
	}
	// Unused new pages remain private and are dropped by release; they are never
	// admitted as dependency authority merely because they were allocated.
	return nil
}
