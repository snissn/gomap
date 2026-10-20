package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"math"
	"unsafe"
)

// Reserve the actual new backing while the old backing is still reachable.
// The original coordinator owns this capacity; no cleanup ledger is created.
func growPublicationSlice[T any](c *Coordinator, values *[]T, required int) error {
	if required < 0 {
		return retainedalloc.ErrCapacity
	}
	if required <= cap(*values) {
		return nil
	}
	capacity := cap(*values) * 2
	if capacity < required {
		capacity = required
	}
	width := uint64(unsafe.Sizeof(*new(T)))
	if uint64(capacity) > math.MaxUint64/width {
		return retainedalloc.ErrCapacity
	}
	added := retainedalloc.AllocationCharge(uint64(capacity) * width)
	if c.metadataOwner != nil {
		if err := c.metadataOwner.AddPending(added); err != nil {
			return err
		}
	}
	next := make([]T, len(*values), capacity)
	copy(next, *values)
	old := retainedalloc.AllocationCharge(uint64(cap(*values)) * width)
	clear(*values)
	*values = next
	if c.metadataOwner != nil {
		c.metadataPhysicalCharge += added - old
		c.metadataOwner.RemovePending(old)
	}
	return nil
}

// A growth plan is scalar bookkeeping on the original producer stack. Reserve
// every new backing together before replacing any existing coordinator array.
func publicationSliceGrowth[T any](values []T, required int) (int, uint64, uint64, error) {
	if required < 0 {
		return 0, 0, 0, retainedalloc.ErrCapacity
	}
	if required <= cap(values) {
		return cap(values), 0, 0, nil
	}
	capacity := cap(values) * 2
	if capacity < required {
		capacity = required
	}
	width := uint64(unsafe.Sizeof(*new(T)))
	if capacity < 0 || uint64(capacity) > math.MaxUint64/width {
		return 0, 0, 0, retainedalloc.ErrCapacity
	}
	return capacity, retainedalloc.AllocationCharge(uint64(capacity) * width), retainedalloc.AllocationCharge(uint64(cap(values)) * width), nil
}
func replacePublicationSlice[T any](values *[]T, capacity int, charge uint64) {
	if charge == 0 {
		return
	}
	next := make([]T, len(*values), capacity)
	copy(next, *values)
	clear(*values)
	*values = next
}

// Enqueue reserves the recovery representation before activation. A later
// Stop can retain the exact accepted members even if the budget is exhausted.
func (c *Coordinator) admitPendingCapacity(candidate *PreparedRootCandidate) error {
	if c.metadataOwner == nil {
		return nil
	}
	if set := candidate.resourceSet(); set != nil && set.MetadataOwner() != c.metadataOwner {
		return ErrResourceOwnership
	}
	if t := candidate.DurableRoot(); t != nil {
		owner := t.metadataOwnerV1()
		if owner != c.metadataOwner {
			return ErrResourceOwnership
		}
	}
	n := len(c.pending) + 1
	roots := len(c.recoveryDurableRoots) + len(candidate.durableRootGroup().members)
	for _, entry := range c.pending {
		roots += len(entry.candidate.durableRootGroup().members)
	}
	pendingCap, pendingNew, pendingOld, err := publicationSliceGrowth(c.pending, n)
	if err != nil {
		return err
	}
	setsCap, setsNew, setsOld, err := publicationSliceGrowth(c.recoverySets, len(c.recoverySets)+n)
	if err != nil {
		return err
	}
	rootsCap, rootsNew, rootsOld, err := publicationSliceGrowth(c.recoveryDurableRoots, roots)
	if err != nil {
		return err
	}
	if pendingNew > math.MaxUint64-setsNew || pendingNew+setsNew > math.MaxUint64-rootsNew {
		return retainedalloc.ErrCapacity
	}
	added, old := pendingNew+setsNew+rootsNew, pendingOld+setsOld+rootsOld
	if err := c.metadataOwner.AddPending(added); err != nil {
		return err
	}
	replacePublicationSlice(&c.pending, pendingCap, pendingNew)
	replacePublicationSlice(&c.recoverySets, setsCap, setsNew)
	replacePublicationSlice(&c.recoveryDurableRoots, rootsCap, rootsNew)
	c.metadataPhysicalCharge += added - old
	c.metadataOwner.RemovePending(old)
	return nil
}

func (t *DurableRootTransaction) metadataOwnerV1() *retainedalloc.Owner {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.constructionV6 != nil {
		return t.constructionV6.arena.MetadataOwner()
	}
	if t.terminalMetadataV6 != nil {
		return t.terminalMetadataV6.owner
	}
	return nil
}

// Attempt backing ends with the original callback/report call. Retry state
// retains only its group-size observation after captured requests are failed.
func (c *Coordinator) admitAttemptLocked() (StableResourceTokenReleaseCompletionV6, error) {
	if c.metadataOwner == nil {
		return StableResourceTokenReleaseCompletionV6{}, nil
	}
	charge := retainedalloc.AllocationCharge(uint64(len(c.pending)) * uint64(unsafe.Sizeof((*PreparedRootCandidate)(nil))))
	charge += retainedalloc.AllocationCharge(uint64(len(c.drains)) * uint64(unsafe.Sizeof((*drainRequest)(nil))))
	charge += retainedalloc.AllocationCharge(uint64(len(c.waiters)) * uint64(unsafe.Sizeof((*durabilityWaiter)(nil))))
	if len(c.pending) > 1 {
		members, obligations := 0, 0
		for _, entry := range c.pending {
			members += len(entry.candidate.durableRootGroup().members)
			obligations += len(entry.candidate.obligations)
		}
		charge += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(PreparedRootCandidate{})))
		charge += retainedalloc.AllocationCharge(uint64(members) * uint64(unsafe.Sizeof((*DurableRootTransaction)(nil))))
		charge += retainedalloc.AllocationCharge(uint64(obligations) * uint64(unsafe.Sizeof(ObligationID{})))
		charge += retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(durableRootGroupExtension{})))
	}
	if err := c.metadataOwner.AddPending(charge); err != nil {
		return StableResourceTokenReleaseCompletionV6{}, err
	}
	return StableResourceTokenReleaseCompletionV6{owner: c.metadataOwner, charge: charge}, nil
}

// Physical cleanup and accepted-prefix cleanup are the two original lifetime
// facts. Return accounting only, after detaching the containing storage; the
// caller drops its original coordinator alias before consuming this receipt.
func (c *Coordinator) metadataHeaderCompletionLocked() StableResourceTokenReleaseCompletionV6 {
	if !c.physicalMetadataDone || c.metadataHeaderCharge == 0 {
		return StableResourceTokenReleaseCompletionV6{}
	}
	c.accepted.mu.Lock()
	complete := c.accepted.changed == nil || c.accepted.closeDone
	for i := range c.accepted.entries {
		if c.accepted.entries[i].releaseErr != nil {
			complete = false
		}
	}
	c.accepted.mu.Unlock()
	if !complete {
		return StableResourceTokenReleaseCompletionV6{}
	}
	result := StableResourceTokenReleaseCompletionV6{owner: c.metadataOwner, charge: c.metadataHeaderCharge}
	c.metadataHeaderCharge = 0
	return result
}

func (c *Coordinator) ReleasePhysicalMetadata() (StableResourceTokenReleaseCompletionV6, error) {
	if c == nil {
		return StableResourceTokenReleaseCompletionV6{}, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped || c.activeDispatch != 0 || len(c.pending) != 0 || len(c.recoverySets) != 0 || len(c.recoveryDurableRoots) != 0 || c.recoveryHandoffErr != nil {
		return StableResourceTokenReleaseCompletionV6{}, ErrRecoveryHandoffUnavailable
	}
	if c.physicalMetadataDone {
		return StableResourceTokenReleaseCompletionV6{}, nil
	}
	c.physicalMetadataDone = true
	clear(c.pending)
	c.pending = nil
	c.recoverySets, c.recoveryDurableRoots = nil, nil
	clear(c.waiters)
	c.waiters = nil
	clear(c.drains)
	c.drains = nil
	c.activeAttempt, c.retryAttempt = PublishAttempt{}, PublishAttempt{}
	c.publisher, c.ctx, c.cancel, c.clock = nil, nil, nil, nil
	// acceptedOwner is immutable logical identity, not physical backing.
	// Cleanup-only users can still inspect original failed custody after this
	// physical runtime is disposed; never redirect them to the empty successor.
	c.resourceActivePins, c.resourcePinHighWater = nil, nil
	completion := c.metadataHeaderCompletionLocked()
	completion.owner = c.metadataOwner
	completion.charge += c.metadataPhysicalCharge
	c.metadataPhysicalCharge = 0
	return completion, nil
}
