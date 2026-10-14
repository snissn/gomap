package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"slices"
	"sync"
	"unsafe"
)

type primaryDependencyLeaseV5 struct {
	mu                   sync.Mutex
	owner                *primaryArenaOwnerV5
	root                 uint64
	ref                  primaryarena.Ref
	closed               bool
	metadataCharge       uint64
	cleanupErr           error
	failure              primaryCleanupFailureV5
	constructorFailure   primaryCleanupFailureV5
	ownerReleaseConsumed bool
	failed               bool
	failedNext           *primaryDependencyLeaseV5
	directory            *rootpublication.DependencyDirectoryV2
	recoveryNext         *primaryDependencyLeaseV5
	releaseWork          primaryArenaReleaseWork
	releasePhase         uint8
	releaseMetadata      *retainedalloc.Owner
	releaseNative        bool
}

func (lease *primaryDependencyLeaseV5) activate() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || lease.ref.PageID != 0 {
		return nil
	}
	ref, ready, e := lease.owner.arena.BorrowDependency(lease.root, nil)
	if !ready || e != nil {
		if e != nil {
			return e
		}
		return rootpublication.ErrResourceOwnership
	}
	ready, e = lease.owner.arena.Acquire(ref, nil)
	if !ready || e != nil {
		if e != nil {
			return e
		}
		return rootpublication.ErrResourceOwnership
	}
	lease.ref = ref
	return nil
}
func (lease *primaryDependencyLeaseV5) release() error {
	_, err := lease.releaseOutcome()
	return err
}

// disposed reports this lease's OWN cleanup, independently of prior sibling errors.
func (lease *primaryDependencyLeaseV5) releaseOutcome() (disposed bool, err error) {
	for {
		consumed, ready, completion, err := lease.ReleaseDependencyDirectoryWorkV6(nil)
		if !ready && err == nil {
			continue
		}
		lease = nil
		if consumed && completion.Charge != 0 {
			completion.Metadata.RemovePending(completion.Charge)
		}
		return consumed, err
	}
}

// Constructor failures retain both causes in their preadmitted lease storage.
func (lease *primaryDependencyLeaseV5) constructorError(cause error) (error, rootpublication.DependencyDirectoryReleaseCompletionV6) {
	for {
		disposed, ready, completion, cleanup := lease.ReleaseDependencyDirectoryWorkV6(nil)
		if !ready && cleanup == nil {
			continue
		}
		if disposed || cleanup == nil {
			return cause, completion
		}
		lease.constructorFailure.causes = [3]error{cause, cleanup}
		return &lease.constructorFailure, completion
	}
}

// Consume the producer method's completion only after the original caller's
// lease alias is detached. Failed originals remain in existing owner custody.
func disposePrimaryDependencyConstructorV6(alias **primaryDependencyLeaseV5, cause error) error {
	original := *alias
	result, completion := original.constructorError(cause)
	*alias = nil
	original = nil
	if completion.Charge != 0 {
		completion.Metadata.RemovePending(completion.Charge)
	}
	return result
}
func (db *DB) pinPrimaryDependencyV5(idx *indexGen, ref rootpublication.DependencyDirectoryRefV2, extent uint64) (*rootpublication.DependencyDirectoryV2, error) {
	if idx == nil || idx.primaryOwner == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	// The exact containing lease and its embedded physical release controls are
	// admitted before acquiring the original physical owner edge.
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDependencyLeaseV5{})))
	if err := idx.primary.MetadataOwner().AddPending(charge); err != nil {
		return nil, err
	}
	ready, e := idx.primaryOwner.retain(nil)
	if !ready || e != nil {
		idx.primary.MetadataOwner().RemovePending(charge)
		return nil, errors.Join(e, rootpublication.ErrResourceOwnership)
	}
	lease := &primaryDependencyLeaseV5{owner: idx.primaryOwner, root: ref.RootPageID, metadataCharge: charge}
	if idx.primary.Recovered() {
		if e := lease.activate(); e != nil {
			return nil, disposePrimaryDependencyConstructorV6(&lease, e)
		}
	}
	directory, e := rootpublication.NewOwnedWorkLocalPrimaryDependencyDirectoryV6(lease.owner.arena.Pager(), ref, extent, idx.primary.MetadataOwner(), lease)
	if e != nil {
		return nil, disposePrimaryDependencyConstructorV6(&lease, e)
	}
	lease.directory = directory
	if !idx.primary.Recovered() {
		lease.recoveryNext = idx.primaryRecoveryLeases
		idx.primaryRecoveryLeases = lease
	}
	return directory, nil
}

// Ordinary changed-path metadata allocation owns genuine bank claims. All new
// edges are sealed child-first, then private construction refs are released.
// These are ordinary producer operations, not bounded native preparation APIs.
type primaryDependencyAllocatorV5 primaryBankConstructionV5

func newPrimaryDependencyAllocatorV5(idx *indexGen) (*primaryDependencyAllocatorV5, error) {
	claims, err := newPrimaryBankConstructionV5(idx)
	return (*primaryDependencyAllocatorV5)(claims), err
}
func (alloc *primaryDependencyAllocatorV5) Alloc(_ uint64) (uint64, error) {
	claims := (*primaryBankConstructionV5)(alloc)
	if err := claims.claim(primaryarena.Dependency, 1); err != nil {
		return 0, err
	}
	return claims.refs[len(claims.refs)-1].PageID, nil
}
func (alloc *primaryDependencyAllocatorV5) release() error {
	return (*primaryBankConstructionV5)(alloc).release()
}

// The sorted admitted refs are exact lookup authority. Traversal state lives
// in one admitted frame per new bank, so cycles and child-first sealing need
// neither recursive stack growth nor two opaque maps.
type primaryDependencySealFrameV5 struct {
	image  []byte
	parent int
	next   uint16
	state  uint8
}

func (alloc *primaryDependencyAllocatorV5) seal(root uint64) error {
	claims := (*primaryBankConstructionV5)(alloc)
	if claims.closed {
		return rootpublication.ErrResourceOwnership
	}
	slices.SortFunc(claims.refs, func(a, b primaryarena.Ref) int {
		if a.PageID < b.PageID {
			return -1
		}
		if a.PageID > b.PageID {
			return 1
		}
		return 0
	})
	find := func(id uint64) int {
		lo, hi := 0, len(claims.refs)
		for lo < hi {
			mid := lo + (hi-lo)/2
			if claims.refs[mid].PageID < id {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < len(claims.refs) && claims.refs[lo].PageID == id {
			return lo
		}
		return -1
	}
	current := find(root)
	if current < 0 {
		return nil
	}
	charge := retainedalloc.AllocationCharge(uint64(len(claims.refs)) * uint64(unsafe.Sizeof(primaryDependencySealFrameV5{})))
	if err := claims.metadata.AddPending(charge); err != nil {
		return err
	}
	frames := make([]primaryDependencySealFrameV5, len(claims.refs))
	defer func() { clear(frames); claims.metadata.RemovePending(charge) }()
	frames[current].parent = -1
traversal:
	for current >= 0 {
		frame := &frames[current]
		if frame.state == 0 {
			image, err := claims.idx.pager.ReadPage(claims.refs[current].PageID)
			if err != nil {
				return err
			}
			frame.image, frame.state = image, 1
		}
		n := node.NewNode(frame.image)
		if n.Type() == page.PageTypeInternal {
			for frame.next < n.Count() {
				_, child, err := n.GetInternalEntryRefView(frame.next)
				frame.next++
				if err != nil || child.Kind != page.ChildRefPage || !primaryarena.IsPage(child.Page) {
					return rootpublication.ErrDependencyManifestFormat
				}
				target := find(child.Page)
				if target < 0 || frames[target].state == 2 {
					continue
				}
				if frames[target].state == 1 {
					return rootpublication.ErrDependencyManifestFormat
				}
				frames[target].parent = current
				current = target
				continue traversal
			}
		}
		ready, err := claims.idx.primary.SealMetadata(claims.refs[current], frame.image, nil)
		if err != nil || !ready {
			return errors.Join(err, rootpublication.ErrResourceOwnership)
		}
		frame.image, frame.state = nil, 2
		current = frame.parent
	}
	return nil
}

// This is the pre-Open lease-discovery list, not Accepted receipt ownership.
// Directory resources own every lease; clearing preparatory links transfers no
// physical edge and allocates nothing.
func (idx *indexGen) activatePrimaryRecoveryLeasesV5() error {
	defer idx.clearPrimaryRecoveryLeasesV5()
	for lease := idx.primaryRecoveryLeases; lease != nil; lease = lease.recoveryNext {
		if err := lease.activate(); err != nil {
			return err
		}
	}
	return nil
}
func (idx *indexGen) clearPrimaryRecoveryLeasesV5() {
	for idx.primaryRecoveryLeases != nil {
		lease := idx.primaryRecoveryLeases
		idx.primaryRecoveryLeases = lease.recoveryNext
		lease.recoveryNext = nil
	}
}
