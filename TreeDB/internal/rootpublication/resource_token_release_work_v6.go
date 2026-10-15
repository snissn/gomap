package rootpublication

import (
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// StableResourceOwnedOperationsWithWorkV6 is implemented by the actual producer,
// never by wrapping its synchronous Release. consumed is storage in the disposing
// original token: a resumed last-release must not decrement its producer twice.
type StableResourceOwnedOperationsWithWorkV6 interface {
	SupportsReleaseWithWorkV6(*iterator.OrdinalScanWork) (bool, error)
	ReleaseWithWorkV6(consumed *bool, w *iterator.OrdinalScanWork) (*retainedalloc.Owner, uint64, bool, error)
}

type stableResourceTokenReleaseStateV6 struct {
	graph              resourceReleaseFrameV6
	metadata           resourceTokenMetadataReleaseV6
	mu                 sync.Mutex
	phase              uint8
	operationsConsumed bool
	namespacePhase     uint8
	obligationIndex    int
	refundOwner        *retainedalloc.Owner
	refundCharge       uint64
}

// StableResourceTokenReleaseCompletionV6 contains accounting only. Its original
// parent clears the token alias after the callback returns, then consumes this
// value. No physical operation, observation or error ownership is transferred.
type StableResourceTokenReleaseCompletionV6 struct {
	owner  *retainedalloc.Owner
	charge uint64
}

func (c *StableResourceTokenReleaseCompletionV6) Complete() {
	if c == nil {
		return
	}
	owner, charge := c.owner, c.charge
	c.owner, c.charge = nil, 0
	if owner != nil {
		owner.RemovePending(charge)
	}
}

// ReleaseWithWorkV6 drives the exact scoped token. Imported cohorts, opaque
// callbacks and directory leases must use their own producer continuations;
// unsupported provenance is rejected before this token edge is consumed.
func (t *StableResourceToken) ReleaseWithWorkV6(w *iterator.OrdinalScanWork) (StableResourceTokenReleaseCompletionV6, bool, error) {
	var empty StableResourceTokenReleaseCompletionV6
	if t == nil {
		return empty, true, nil
	}
	if w == nil {
		return empty, false, retainedalloc.ErrWorkRequired
	}
	s := &t.releaseWork
	s.mu.Lock()
	defer s.mu.Unlock()
	fail := func(err error) (StableResourceTokenReleaseCompletionV6, bool, error) {
		t.cleanupFailure.original, t.cleanupFailure.causes[0] = t, err
		if t.metadata != nil {
			t.metadata.owner.CleanupFailed()
		}
		t.cleanupDone.Store(true)
		s.phase = 255
		return empty, false, &t.cleanupFailure
	}
	if s.phase == 255 {
		return empty, false, t.releaseResult()
	}
	if s.phase == 8 {
		return empty, true, nil
	}
	reserve := func(r, bytes uint64) bool { return w.Reserve(r, bytes) }
	if s.phase == 0 {
		if !reserve(1, 2*uint64(unsafe.Sizeof(*t))) {
			return empty, false, nil
		}
		if ResourceOwnerState(t.owner.Load()) != ResourceOwnerToken || t.released.Load() || t.metadata == nil || t.metadata.refs.Load() != 1 ||
			t.importedSource != nil || t.importedFields != nil || t.directory != nil || t.onRelease != nil ||
			t.observationCleanup != nil || t.releaseObservation != nil || t.physicalBacking == nil ||
			t.physicalBacking.sourceRoot != nil || t.physicalBacking.owner != t.metadata.owner {
			return empty, false, retainedalloc.ErrWorkRequired
		}
		if err := t.metadata.cleanupResult(); err != nil {
			return empty, false, err
		}
		if t.ownedOperations != nil {
			ops, ok := t.ownedOperations.(StableResourceOwnedOperationsWithWorkV6)
			if !ok {
				return empty, false, retainedalloc.ErrWorkRequired
			}
			ready, err := ops.SupportsReleaseWithWorkV6(w)
			if !ready || err != nil {
				return empty, false, err
			}
		}
		if ns := t.namespace; ns != nil {
			ns.mu.Lock()
			supported := ns.metadata != nil && ns.metadata == t.metadata.owner && ns.batch == nil && ns.originalCleanup == nil && !ns.released.Load()
			ns.mu.Unlock()
			if !supported {
				return empty, false, retainedalloc.ErrWorkRequired
			}
		}
		if !t.owner.CompareAndSwap(uint32(ResourceOwnerToken), uint32(ResourceOwnerReleased)) {
			return empty, false, ErrResourceOwnership
		}
		t.released.Store(true)
		s.refundOwner = t.metadata.owner
		s.phase = 1
	}
	if s.phase == 1 {
		if !reserve(1, 2*uint64(unsafe.Sizeof(resourcePinnedBacking{}))+64) {
			return empty, false, nil
		}
		backing := t.physicalBacking
		if backing.failure != nil {
			return fail(backing.failure)
		}
		n := backing.refs.Add(-1)
		if n < 0 {
			panic("unbalanced native token backing")
		}
		if n == 0 {
			if backing.file != nil {
				if err := backing.file.Close(); err != nil {
					backing.failure = err
					backing.owner.CleanupFailed()
					if backing.registry != nil {
						backing.registry.retainFailedPinnedBacking(backing)
					}
					return fail(err)
				}
			}
			s.refundCharge += backing.charge
			backing.file, backing.owner, backing.registry, backing.charge = nil, nil, nil, 0
		}
		t.pinned, t.pinnedRefs, t.physicalBacking = nil, nil, nil
		s.phase = 2
	}
	if s.phase == 2 {
		if ns := t.namespace; ns != nil {
			completion, ready, err := ns.ReleaseWithWorkV6(&s.namespacePhase, w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
			if completion.owner != nil && completion.owner != s.refundOwner {
				return fail(ErrResourceOwnership)
			}
			t.namespace = nil
			s.refundCharge += completion.charge
		}
		s.phase = 3
	}
	if s.phase == 3 {
		ready, err := t.identityPin.ReleaseWithWorkV6(w)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return empty, false, nil
		}
		t.identityPin = nil
		s.phase = 4
	}
	if s.phase == 4 {
		if t.ownedOperations != nil {
			ops := t.ownedOperations.(StableResourceOwnedOperationsWithWorkV6)
			owner, charge, ready, err := ops.ReleaseWithWorkV6(&s.operationsConsumed, w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
			if owner != nil && owner != s.refundOwner {
				return fail(ErrResourceOwnership)
			}
			t.ownedOperations = nil
			s.refundCharge += charge
		}
		s.phase = 5
	}
	if s.phase == 5 {
		if backing := t.obligationBacking; backing != nil {
			// The descriptor's independent backing edge is consumed once. Last backing
			// storage is scrubbed in admitted slots while the original token retains it.
			if s.obligationIndex == 0 {
				if !reserve(1, 2*uint64(unsafe.Sizeof(*backing.allocation))) {
					return empty, false, nil
				}
				if !backing.allocation.drop() {
					t.obligationBacking = nil
					s.phase = 6
				} else {
					s.obligationIndex = 1
				}
			}
			if s.phase == 5 {
				values := backing.values[:cap(backing.values)]
				for s.obligationIndex-1 < len(values) {
					if !reserve(1, 2*uint64(unsafe.Sizeof(StableLogicalObligation{}))) {
						return empty, false, nil
					}
					values[s.obligationIndex-1] = StableLogicalObligation{}
					s.obligationIndex++
				}
				if !reserve(1, 2*uint64(unsafe.Sizeof(*backing))) {
					return empty, false, nil
				}
				s.refundCharge += backing.allocation.charge
				backing.values = nil
				backing.allocation.owner = nil
				backing.allocation.charge = 0
				t.obligationBacking = nil
			}
		}
		s.phase = 6
	}
	if s.phase == 6 {
		if !reserve(1, 2*uint64(unsafe.Sizeof(*t))) {
			return empty, false, nil
		}
		a := t.metadata
		if !a.drop() {
			return fail(ErrResourceOwnership)
		}
		s.refundCharge += a.charge
		a.owner, a.charge = nil, 0
		t.metadata = nil
		t.kind, t.logicalLane, t.resourceID, t.diagnosticPath, t.reachability = "", "", "", "", ""
		t.identity = StableIdentity{}
		t.frontier, t.syncedFrontier = DurableFrontier{}, DurableFrontier{}
		t.logicalObligations = nil
		t.flush, t.sync = nil, nil
		t.cleanupDone.Store(true)
		completion := StableResourceTokenReleaseCompletionV6{owner: s.refundOwner, charge: s.refundCharge}
		s.refundOwner, s.refundCharge = nil, 0
		s.phase = 8
		return completion, true, nil
	}
	return empty, false, ErrResourceOwnership
}
