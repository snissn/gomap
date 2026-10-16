package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"sync"
	"unsafe"
)

// StableResourceSetReleaseWorkV6 is storage in the SAME disposing parent's
// admitted receipt. It owns no independent physical authority. Its only delayed
// responsibility is the original wrapper charge after protected edge consumption.
type StableResourceSetReleaseWorkV6 struct {
	mu     sync.Mutex
	phase  uint8
	owner  *retainedalloc.Owner
	charge uint64
}

// ContinueWork checks only the original Set-wrapper completion. Every physical
// edge was already consumed under the exact visible clone's protected ownership;
// this receipt never retries a later ordinary last-owner callback.
func (s *StableResourceSetReleaseWorkV6) ContinueWork(w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	if s.phase != 3 {
		return false, ErrResourceOwnership
	}
	return true, nil
}

// takeCompletion is called AFTER the disposing Set callback has returned and
// its parent cleared the Set alias. The SAME original terminal receipt carries
// these accounting bytes; no physical callback may be replayed for a refund.
func (s *StableResourceSetReleaseWorkV6) takeCompletion() (*retainedalloc.Owner, uint64, error) {
	return s.takeCompletionExpected(nil)
}
func (s *StableResourceSetReleaseWorkV6) takeCompletionExpected(expected *retainedalloc.Owner) (*retainedalloc.Owner, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != 3 || (expected != nil && s.owner != expected) {
		return nil, 0, ErrResourceOwnership
	}
	owner, charge := s.owner, s.charge
	s.owner, s.charge = nil, 0
	return owner, charge, nil
}

// ConsumeSharedWithVisibleV6 consumes the original constructor Set against the
// EXACT original visible clone, while the caller still owns its final publication
// turn. Both Set locks protect that clone's existing edges until all original
// edges are consumed. No new retained sibling or physical owner is created.
//
// counts belongs to the coordinator and MUST remain under its original mutex.
// Every validation/admission precedes the first owner/refcount/map mutation.
// This operation runs no last-owner callback; later Finish owns only the original
// Set wrapper's checked accounting. It is not a general last-Set release.
func (s *StableResourceSetReleaseWorkV6) ConsumeSharedWithVisibleV6(set, visible *StableResourceSet, counts map[ResourceKind]uint64, w *iterator.OrdinalScanWork) (bool, error) {
	return s.consumeSharedWithVisibleV6(set, visible, counts, nil, w)
}

func (s *StableResourceSetReleaseWorkV6) consumeSharedWithVisibleV6(set, visible *StableResourceSet, counts map[ResourceKind]uint64, expectedOwner *retainedalloc.Owner, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || set == nil || visible == nil || set == visible || w == nil {
		return false, ErrResourceOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != 0 {
		return false, ErrResourceOwnership
	}
	first, second := set, visible
	if uintptr(unsafe.Pointer(first)) > uintptr(unsafe.Pointer(second)) {
		first, second = second, first
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*set))+2*uint64(unsafe.Sizeof(*visible))+2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	vo := visible.Owner()
	if set.Owner() != ResourceOwnerCoordinator ||
		vo == ResourceOwnerReleased || vo == ResourceOwnerTransferred ||
		!set.selected || set.metadata == nil || set.metadata.refs.Load() != 1 ||
		len(set.entries) != 0 || len(visible.entries) != 0 {
		return false, ErrResourceOwnership
	}
	if err := set.metadata.cleanupResult(); err != nil {
		return false, err
	}
	if expectedOwner != nil && set.metadata.owner != expectedOwner {
		return false, retainedalloc.ErrWorkRequired
	}
	views := set.kindViews
	if views != visible.kindViews || (views != nil && (views.allocation == nil || views.allocation.refs.Load() < 2)) {
		return false, retainedalloc.ErrWorkRequired
	}
	if views != nil {
		if err := views.allocation.cleanupResult(); err != nil {
			return false, err
		}
		for _, cell := range views.cells {
			if !w.Reserve(1, uint64(unsafe.Sizeof(cell))+uint64(len(cell.kind))) {
				return false, nil
			}
			// The shared immutable backing still owns every token/foreign
			// sourceRoot and directory edge. No recursive release is reachable.
			if counts != nil {
				if kind, builtin := canonicalTelemetryKind(cell.kind); builtin && counts[kind] < uint64(cell.view.count) {
					return false, ErrResourceOwnership
				}
			}
		}
	}
	dirs := set.physicalDependencyDirectoriesLocked()
	empty := set.emptyDependencyDirectoryLocked()
	// Extras own independent edges. A matching visible extra, or the directory
	// edge in the protected immutable backing, must keep each one nonlast.
	for i := 0; i <= len(dirs); i++ {
		if !w.Reserve(1, uint64(unsafe.Sizeof((*DependencyDirectoryV2)(nil)))) {
			return false, nil
		}
		var d *DependencyDirectoryV2
		if i < len(dirs) {
			d = dirs[i]
		} else {
			d = empty
		}
		if d == nil {
			continue
		}
		protected := d == visible.emptyDependencyDirectoryLocked()
		if !protected {
			for _, other := range visible.physicalDependencyDirectoriesLocked() {
				if !w.Reserve(1, 2*uint64(unsafe.Sizeof(d))) {
					return false, nil
				}
				if other == d {
					protected = true
					break
				}
			}
		}
		if !protected && views != nil {
			for _, cell := range views.cells {
				if !w.Reserve(1, uint64(unsafe.Sizeof(cell))) {
					return false, nil
				}
				if cell.directoryOwned && cell.view.directory == d {
					protected = true
					break
				}
			}
		}
		// Multiple original slots may own edges to the same directory. Prove
		// all those drops together leave the protected sibling's edge alive.
		owned := int64(0)
		for _, other := range dirs {
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(d))) {
				return false, nil
			}
			if other == d {
				owned++
			}
		}
		if empty == d {
			owned++
		}
		if !protected || d.refs.Load() <= owned || d.cleanupDone.Load() {
			return false, retainedalloc.ErrWorkRequired
		}
	}
	// Admit each concrete original edge/summary write before effects. The
	// protected clone makes Add(-1) nonlast without a retrying CAS loop.
	records := uint64(2 + len(dirs))
	if empty != nil {
		records++
	}
	if views != nil {
		records += 1 + uint64(len(views.cells))
	}
	bytes := 2*uint64(unsafe.Sizeof(*set)) + 2*uint64(unsafe.Sizeof(*s)) +
		2*uint64(len(dirs))*uint64(unsafe.Sizeof((*DependencyDirectoryV2)(nil)))
	if views != nil {
		bytes += 2*uint64(unsafe.Sizeof(*views)) + 2*uint64(unsafe.Sizeof(*views.allocation)) + 2*uint64(len(views.cells))*uint64(unsafe.Sizeof(resourceKindCell{}))
	}
	if !w.Reserve(records, bytes) {
		return false, nil
	}
	set.owner.Store(uint32(ResourceOwnerReleased))
	for i, d := range dirs {
		if d != nil {
			d.refs.Add(-1)
			dirs[i] = nil
		}
	}
	if empty != nil {
		empty.refs.Add(-1)
		set.extras.empty = nil
	}
	if views != nil {
		if counts != nil {
			for _, cell := range views.cells {
				if kind, builtin := canonicalTelemetryKind(cell.kind); builtin {
					if counts[kind] == uint64(cell.view.count) {
						delete(counts, kind)
					} else {
						counts[kind] -= uint64(cell.view.count)
					}
				}
			}
		}
		views.allocation.refs.Add(-1)
	}
	a := set.metadata
	a.refs.Add(-1)
	s.owner, s.charge = a.owner, a.charge
	a.owner, a.charge = nil, 0
	set.metadata = nil
	set.entries, set.kindViews, set.extras, set.pinHighWater = nil, nil, nil, nil
	set.releasedDirectoryLease = len(dirs) > 0 || empty != nil
	set.cleanupDone.Store(true)
	s.phase = 3
	return true, nil
}

// releaseCoordinatorSetWithWorkV6 is the real Coordinator-owner Finish callback.
// Preadmit its by-value accounting handoff BEFORE the graph may consume its last
// edge. The graph's root completion names exactly its original metadata owner;
// deferred child/scratch charges are disjoint portions of that same admitted
// owner total, so merging with the constructor cannot overflow that total.
func (c *primaryTransactionConstructionV6) releaseCoordinatorSetWithWorkV6(set *StableResourceSet, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || set == nil || !c.setFinishHandoff {
		return false, ErrResourceOwnership
	}
	if !w.Reserve(2, uint64(unsafe.Sizeof(*set))+2*uint64(unsafe.Sizeof(c.setRelease))+2*uint64(unsafe.Sizeof(StableResourceTokenReleaseCompletionV6{}))) {
		return false, nil
	}
	set.mu.Lock()
	a := set.metadata
	valid := a != nil && c.terminalCompletion != nil && a.owner == c.terminalCompletion.owner
	set.mu.Unlock()
	if !valid {
		return false, ErrResourceOwnership
	}
	completion, ready, err := set.ReleaseFromWithWorkV6(ResourceOwnerCoordinator, w)
	if !ready || err != nil {
		return false, err
	}
	// The physical callback returned. No fallible work remains in this handoff.
	c.setRelease.mu.Lock()
	c.setRelease.owner, c.setRelease.charge, c.setRelease.phase = completion.owner, completion.charge, 3
	c.setRelease.mu.Unlock()
	return true, nil
}

// This is only logical transfer of the SAME constructor edge to its Finish.
// The supported immutable empty Set added no coordinator pin contribution; later
// ordinary pins belong to their own Sets and must never be scanned/subtracted.
func (t *DurableRootTransaction) handoffPrimaryResourcesToFinishV6(w *iterator.OrdinalScanWork) (bool, error) {
	if t == nil || w == nil {
		return false, ErrDurableRootOwnership
	}
	if !w.Reserve(2, uint64(unsafe.Sizeof(*t))+2*uint64(unsafe.Sizeof(primaryTransactionConstructionV6{}))) {
		return false, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || !c.nativeWorkOwned || t.phase != durableRootActivated || t.owner != ResourceOwnerCoordinator || c.resources == nil || c.finishReleaseStarted || c.setReleaseConsumed || c.setFinishHandoff {
		return false, ErrDurableRootOwnership
	}
	// Empty provenance was checked under Set exclusion before activation and
	// the Coordinator-owned immutable Set has not been mutated since then.
	c.setFinishHandoff = true
	return true, nil
}
