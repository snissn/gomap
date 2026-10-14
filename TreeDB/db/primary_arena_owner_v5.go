package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"sync"
	"unsafe"
)

// primaryArenaOwnerV5 is independent of every DATA generation. References
// retain the actual bank mapping, allocator and namespace proof together.
// A replacement DATA pager never owns, recovers or swaps the companion file.
type primaryArenaOwnerV5 struct {
	mu              sync.Mutex
	refs            uint64
	arena           *primaryarena.Arena
	namespaceMu     sync.Mutex
	namespaceParent *os.File
	namespaceProof  *rootpublication.StableNamespaceCreationProof
}

func newPrimaryArenaOwnerV5(a *primaryarena.Arena) *primaryArenaOwnerV5 {
	return &primaryArenaOwnerV5{refs: 1, arena: a}
}
func (owner *primaryArenaOwnerV5) retain(work *iterator.OrdinalScanWork) (bool, error) {
	if work != nil && !work.Reserve(1, 2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))) {
		return false, nil
	}
	if owner == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.refs == 0 || owner.refs == ^uint64(0) {
		return false, rootpublication.ErrResourceOwnership
	}
	owner.refs++
	return true, nil
}
func (owner *primaryArenaOwnerV5) release(work *iterator.OrdinalScanWork) (bool, error) {
	if work != nil && !work.Reserve(1, 2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))) {
		return false, nil
	}
	if owner == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	owner.mu.Lock()
	if owner.refs == 0 {
		owner.mu.Unlock()
		return false, rootpublication.ErrResourceOwnership
	}
	owner.refs--
	last := owner.refs == 0
	owner.mu.Unlock()
	if !last {
		return true, nil
	}
	// Final close requires every exact bank/dependency/cut owner to have released
	// its reference. Native callers retain their own owner until charged Finish.
	err := owner.arena.Close()
	owner.namespaceMu.Lock()
	if owner.namespaceProof != nil {
		owner.namespaceProof.Release()
		owner.namespaceProof = nil
	}
	if owner.namespaceParent != nil {
		err = errors.Join(err, owner.namespaceParent.Close())
		owner.namespaceParent = nil
	}
	owner.namespaceMu.Unlock()
	return true, err
}

func (generation *indexGen) sharePrimaryArenaV5(source *indexGen) error {
	if generation == nil || source == nil || generation.primaryOwner != nil || source.primaryOwner == nil {
		return rootpublication.ErrResourceOwnership
	}
	owner := source.primaryOwner
	ready, err := owner.retain(nil)
	if !ready || err != nil {
		return errors.Join(err, rootpublication.ErrResourceOwnership)
	}
	if err = generation.pager.AttachPrimaryBankPager(owner.arena.Pager()); err != nil {
		_, closeErr := owner.release(nil)
		return errors.Join(err, closeErr)
	}
	generation.primaryOwner = owner
	generation.primary = owner.arena
	generation.zipper.SetPrimaryArena(owner.arena)
	generation.zipper.SetPrimaryConstructorV6(rootpublication.NewPrimaryConstructionTransactionV6)
	return nil
}
