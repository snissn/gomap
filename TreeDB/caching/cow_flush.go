package caching

import (
	"errors"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
)

// A successful final physical batch proves coverage of exactly this captured
// prefix. If successor capture refuses, this owner keeps the entire prefix on
// its old basis. The next coordinator pass retries only the handoff.
type cowFlushHandoff struct {
	cut      *cowReadCut
	basis    *cowBackendBasis
	next     *cowReadCut
	guard    *backenddb.StableResourceCaptureLease
	lease    *memtable.COWExternalLease
	accepted bool
	prefix   rootpublication.AcceptedCut
	seal     rootpublication.PhysicalSealReceipt
}

func (c *cowCache) captureBackendBasis(provider backendSnapshotProvider) (*cowBackendBasis, error) {
	bounded, ok := provider.(interface {
		AcquireSnapshotWithAllocationAdmission(func(backenddb.SnapshotAllocationSizes) error) (*backenddb.Snapshot, error)
	})
	if !ok {
		return nil, ErrCOWUnsupported
	}
	// Admit the callback and both captured reservation/count cells before creation.
	scratch, err := c.maintenance.AcquireExternal(memtable.COWAllocationCharge(5*uint64(unsafe.Sizeof(uintptr(0)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof((*memtable.COWExternalLease)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof((*retainedalloc.Enrollment)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(int(0)))))
	if err != nil {
		return nil, err
	}
	defer scratch.Close()
	var admitted *cowBackendBasis
	snapshot, err := bounded.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
		if s.ValueLog.MapHint > c.budget.Limits().MaxResources || s.ValueLog.FileCount > c.budget.Limits().MaxResources {
			return memtable.ErrCOWCapacity
		}
		var e error
		admitted, e = admitCOWBackendBasis(c.maintenance, s)
		return e
	})
	if err == nil {
		err = admitted.adoptSnapshot(snapshot)
	}
	if err != nil {
		cleanupErr := admitted.release()
		if err == backenddb.ErrSnapshotCapacity {
			err = memtable.ErrCOWCapacity
		}
		return nil, joinCOWCleanupError(err, cleanupErr)
	}
	return admitted, nil
}

// installCOWBasis preserves every newer mutable and frozen source. For a flush,
// the exact accepted captured prefix alone is removed. A maintenance refresh
// supplies nil coverage and preserves every source on the equivalent new tree.
// Allocation is admitted before construction; cutMu only performs the swap.
func (c *cowCache) prepareCOWBasisInstall() (*cowReadCut, error) {
	shards, frozen := len(c.writers), c.budget.Limits().MaxSources
	lease, err := c.maintenance.AcquireExternal(cowCutCharge(shards, frozen))
	if err != nil {
		return nil, err
	}
	return &cowReadCut{refs: 1, cache: c, lease: lease,
		shards: make([]cowTable, shards), frozen: make([]cowTable, 0, frozen),
		sources: make([]memtable.Table, 0, shards+frozen),
		domains: make([]rootDomainSnapshot, shards)}, nil
}

func (c *cowCache) installCOWBasis(basis *cowBackendBasis, covered *cowReadCut) (*cowReadCut, error) {
	next, err := c.prepareCOWBasisInstall()
	if err != nil {
		return nil, err
	}
	if err = basis.snapshot.ActivatePrimaryMetadataProducer(); err != nil {
		refund := c.releasePrivateCOWBasisInstall(next)
		next = nil
		refund.Close()
		return nil, err
	}
	old, err := c.installPreparedCOWBasis(basis, covered, next)
	if err != nil {
		refund := c.releasePrivateCOWBasisInstall(next)
		next = nil
		refund.Close()
	}
	return old, err
}

// A failed private successor owns no published raw-root edges. End its semantic
// edges while the retained opening cut still keeps the cleanup lane alive;
// return only independent accounting after all containing aliases are scrubbed.
func (c *cowCache) releasePrivateCOWBasisInstall(next *cowReadCut) *memtable.COWExternalLease {
	if next == nil {
		return nil
	}
	c.releasePrivateSemanticState(next)
	refund := next.lease
	next.lease, next.cache, next.basis = nil, nil, nil
	next.shards, next.frozen, next.sources, next.domains = nil, nil, nil, nil
	return refund
}

func (c *cowCache) installPreparedCOWBasis(basis *cowBackendBasis, covered, next *cowReadCut) (*cowReadCut, error) {
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	old := c.cut
	if old == nil {
		return nil, backenddb.ErrClosed
	}
	n := 0
	if covered != nil {
		n = len(covered.frozen)
		if old.basis != covered.basis || len(old.frozen) < n {
			return nil, backenddb.ErrRootPublicationBasisMismatch
		}
		for i := 0; i < n; i++ {
			if old.frozen[i].root != covered.frozen[i].root {
				return nil, backenddb.ErrRootPublicationBasisMismatch
			}
		}
	}
	if next == nil || basis == nil || basis.snapshot == nil || len(old.frozen)-n > cap(next.frozen) || len(old.shards) != len(next.shards) {
		return nil, backenddb.ErrRootPublicationBasisMismatch
	}
	if covered != nil {
		basis.logicalSeq = covered.frozenLogicalSeq
	} else {
		basis.logicalSeq = old.logicalSeq
	}
	next.visibleCommandLSN, next.logicalSeq, next.basis = old.visibleCommandLSN, old.logicalSeq, basis
	next.frozenLogicalSeq = old.frozenLogicalSeq
	next.frozenCommandLSN = old.frozenCommandLSN
	// Rebase only captured-T native domains onto latest logical state using the
	// exact accepted Snapshot. Preparation is outside cutMu and must acquire
	// no collection mutex. Failure leaves the original accepted handoff intact.
	captured := old.semanticRootSet
	if covered != nil {
		captured = covered.frozenSemanticRootSet
	}
	if captured != nil {
		state, err := captured.PrepareHandoff(old.semanticRootSet, basis.snapshot)
		if err != nil {
			return nil, err
		}
		next.semanticRootSet = state
		if old.frozenSemanticRootSet == old.semanticRootSet {
			next.frozenSemanticRootSet = state
			if state != nil {
				state.Retain()
			}
		} else if old.frozenSemanticRootSet != nil {
			frozen, err := captured.PrepareHandoff(old.frozenSemanticRootSet, basis.snapshot)
			if err != nil {
				c.releasePrivateSemanticState(next)
				return nil, err
			}
			next.frozenSemanticRootSet = frozen
		}
	} else {
		next.frozenSemanticRootSet = old.frozenSemanticRootSet
		next.semanticRootSet = old.semanticRootSet
		retainCOWSemanticState(next)
	}
	next.frozen = next.frozen[:len(old.frozen)-n]
	basis.retain()
	copy(next.shards, old.shards)
	copy(next.frozen, old.frozen[n:])
	for i := range next.shards {
		if !next.shards[i].root.Retain() {
			panic("lost COW handoff mutable")
		}
	}
	for i := range next.frozen {
		if !next.frozen[i].root.Retain() {
			panic("lost COW handoff frozen")
		}
	}
	next.buildDomains()
	c.cutMu.Lock()
	c.cut = next
	c.activeCuts.Add(1)
	old.refs--
	last := old.refs == 0
	c.cutMu.Unlock()
	if last {
		return old, nil
	}
	return nil, nil
}

func (db *DB) completeCOWHandoff() error {
	c := db.cow
	h := c.handoff
	if h == nil {
		return nil
	}
	// The preaccept receipt owns this exact generation even when backend state
	// has advanced or capture admission is now exhausted. Retry only this swap.
	old, err := c.installPreparedCOWBasis(h.basis, h.cut, h.next)
	if err != nil {
		return err
	}
	if h.prefix.Valid() {
		err = h.prefix.CompleteHandoff(h.seal)
		if err != nil {
			c.prefix.PoisonAccepted(err)
		}
	}
	c.handoff = nil
	h.next = nil // the installed cut now owns the admitted descriptor
	c.handoffs.Add(1)
	if old != nil {
		err = joinCOWCleanupError(err, old.drain())
	}
	refund, cleanupErr := h.release(c)
	h = nil
	err = joinCOWCleanupError(err, cleanupErr)
	refund.Close()
	return err
}

// flushCOWFrozen is called with flushMu held. It streams immutable metadata
// through the existing physical batch/build-group authority; values and pointer
// decoders are never materialized by the flush planner.
func (db *DB) flushCOWFrozen(syncFlush bool, publish *checkpointCommandWALPublish) error {
	c := db.cow
	if c.handoff != nil {
		if !c.handoff.accepted {
			return errors.New("COW unfinished publication")
		}
		if err := db.completeCOWHandoff(); err != nil {
			return err
		}
	}
	return db.flushCOWFrozenCut(syncFlush, publish, c.retainCut())
}

// flushCOWFrozenCut consumes the one original retained source cut. Explicit
// boundaries captured it under P before post-frontier admissions; background
// passes use its immutable frozen logical/semantic coverage, excluding mutable
// suffixes. Every preaccept exit unwinds through the same original owners.
func (db *DB) flushCOWFrozenCut(syncFlush bool, publish *checkpointCommandWALPublish, cut *cowReadCut) (retErr error) {
	c := db.cow
	if cut == nil {
		return backenddb.ErrClosed
	}
	target := rootpublication.SealedPrefix{LogicalSequence: cut.frozenLogicalSeq, CanonicalLSN: cut.frozenCommandLSN}
	if publish != nil && publish.appliedLSN != 0 {
		target.CanonicalLSN = publish.appliedLSN
	}
	if len(cut.frozen) == 0 && (c.prefix == nil || target == c.prefix.LogicalFrontiers().S) {
		return c.releaseCut(cut)
	}
	n := len(cut.frozen)
	chunkCap := db.flushBackendMaxEntries
	if chunkCap <= 0 || chunkCap > 1024 {
		chunkCap = 1024
	}
	entries := 0
	for _, source := range cut.frozen {
		entries += min(source.Len(), chunkCap-entries)
		if entries == chunkCap {
			break
		}
	}
	if entries > 0 {
		chunkCap = entries
	} else if n == 0 {
		chunkCap = 1
	}
	fixedCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFlushHandoff{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(backenddb.StableResourceCaptureLease{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(checkpointCommandWALPublish{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof([1]backenddb.CommandWALLSNRange{})))
	charge := fixedCharge + memtable.COWAllocationCharge(uint64(chunkCap)*uint64(unsafe.Sizeof(batch.Entry{})))
	lease, err := c.maintenance.AcquireExternal(fixedCharge)
	if err != nil {
		return joinCOWCleanupError(err, c.releaseCut(cut))
	}
	h := &cowFlushHandoff{cut: cut, lease: lease}
	handoffOwned := true
	defer func() {
		if handoffOwned {
			prefix := h.prefix
			h.prefix = rootpublication.AcceptedCut{}
			if prefix.Valid() {
				retErr = joinCOWCleanupError(retErr, prefix.Abandon())
			}
			refund, cleanupErr := h.release(c)
			h = nil
			retErr = joinCOWCleanupError(retErr, cleanupErr)
			refund.Close()
		}
	}()
	var localPublish checkpointCommandWALPublish
	var localRange [1]backenddb.CommandWALLSNRange
	if c.prefix != nil {
		h.prefix, err = c.prefix.CaptureAcceptedCut(target, rootpublication.LogicalRootSet{Raw: cut, Semantic: cut.frozenSemanticRootSet})
		if err != nil {
			return err
		}
		// Canonical coverage comes from this exact accepted prefix, never a domain
		// maximum. The queue proved every intervening assignment installed.
		basis, ok := cut.basis.snapshot.StateToken()
		if !ok {
			return backenddb.ErrClosed
		}
		if target.CanonicalLSN > basis.AppliedCommandLSN && (publish == nil || publish.appliedLSN == 0) {
			localRange[0] = backenddb.CommandWALLSNRange{First: basis.AppliedCommandLSN + 1, Last: target.CanonicalLSN}
			localPublish.appliedLSN, localPublish.ranges = target.CanonicalLSN, localRange[:]
			publish = &localPublish
		}
	}
	backend, ok := db.backend.(interface {
		AcquireStableResourceCaptureLease() (*backenddb.StableResourceCaptureLease, error)
		DrainRetiredIndexGenerations(*backenddb.StableResourceCaptureLease) error
		BeginRootPublicationBuildGroupBorrowingLease(*backenddb.Snapshot, *backenddb.StableResourceCaptureLease) (*backenddb.RootPublicationBuildGroup, error)
	})
	if !ok {
		return ErrCOWUnsupported
	}
	h.guard, err = backend.AcquireStableResourceCaptureLease()
	if err != nil {
		return err
	}
	if err = backend.DrainRetiredIndexGenerations(h.guard); err != nil {
		return err
	}
	if err = lease.Resize(charge); err != nil {
		return err
	}
	group, err := backend.BeginRootPublicationBuildGroupBorrowingLease(cut.basis.snapshot, h.guard)
	if err != nil {
		return err
	}
	if h.prefix.Valid() {
		if err = group.BindAcceptedPrefixCut(h.prefix); err != nil {
			return errors.Join(err, group.Close())
		}
	}
	h.next, err = c.prepareCOWBasisInstall()
	if err == nil {
		err = group.PrepareAcceptedSnapshot(func(s backenddb.SnapshotAllocationSizes) error {
			if s.ValueLog.MapHint > c.budget.Limits().MaxResources || s.ValueLog.FileCount > c.budget.Limits().MaxResources {
				return memtable.ErrCOWCapacity
			}
			var e error
			h.basis, e = admitCOWBackendBasis(c.maintenance, s)
			return e
		}, func(snapshot *backenddb.Snapshot) error {
			if e := h.basis.adoptSnapshot(snapshot); e != nil {
				return e
			}
			return snapshot.ActivatePrimaryMetadataProducer()
		}, func() error {
			basis := h.basis
			h.basis = nil
			return basis.release()
		})
	}
	if err != nil {
		return errors.Join(err, group.Close())
	}
	if captured := cut.frozenSemanticRootSet; captured != nil {
		if err = captured.PrepareBuild(group, cut.basis.snapshot, h.guard); err != nil {
			var refusal backenddb.NativeSemanticBuildRetryableRefusal
			if !errors.As(err, &refusal) {
				// Logical acceptance predates this physical build. A partial native
				// failure cannot be skipped or rebuilt by a later checkpoint, even
				// when its underlying cause happens to be a capacity error.
				c.prefix.PoisonAccepted(err)
				group.MarkNativeSemanticBuildRecoveryRequired()
			}
			return errors.Join(err, group.Close())
		}
	}
	ops := make([]batch.Entry, 0, chunkCap)
	// Sources apply oldest to newest within the same private build group. This
	// preserves tombstones and precedence without a second materialized merge.
	for i := range cut.frozen {
		it := cut.frozen[i].NewIterator(nil, nil)
		metadata, ok := it.(iterator.RevisionUnsafeIterator)
		if !ok {
			_ = it.Close()
			err = ErrCOWUnsupported
			break
		}
		for it.Valid() {
			value, ptr, flags, revision := metadata.UnsafeEntryWithRevision()
			op := batch.Entry{Type: batch.OpPut, Key: it.(iterator.UnsafeIterator).UnsafeKey(), Value: value, ValuePtr: ptr, IsPtr: flags&node.FlagPointer != 0, Revision: revision}
			if flags&node.FlagTombstone != 0 {
				op.Type = batch.OpDelete
				op.Value = nil
			}
			ops = append(ops, op)
			it.Next()
			if len(ops) == chunkCap || !it.Valid() {
				if err = it.Error(); err != nil {
					break
				}
				last := i == n-1 && !it.Valid()
				if err = db.writeCanonicalFlushRunOpsChunk(ops, syncFlush, publish, group, last, flushCollectionBackground); err != nil {
					break
				}
				ops = ops[:0]
			}
		}
		if err == nil {
			err = it.Error()
		}
		_ = it.Close()
		if err != nil {
			break
		}
	}

	if n == 0 && err == nil {
		err = db.writeCanonicalFlushRunOpsChunk(nil, syncFlush, publish, group, true, flushCollectionBackground)
	}
	err = errors.Join(err, group.Close())
	if group.Accepted() {
		h.accepted, h.seal = true, group.AcceptedSealReceipt()
		c.handoff = h
		handoffOwned, h = false, nil
		if err != nil {
			return err
		}
		return db.completeCOWHandoff()
	}
	return err
}

// release detaches the original handoff aliases before invoking any finalizer.
// A failed basis retains itself on its preborn OriginalCleanupCustody, and a
// failed group retains its own physical owner; neither is flattened into h.
// The caller ends its h alias before completing the independent final refund.
func (h *cowFlushHandoff) release(c *cowCache) (*memtable.COWExternalLease, error) {
	basis, next, cut, guard, refund := h.basis, h.next, h.cut, h.guard, h.lease
	h.basis, h.next, h.cut, h.guard, h.lease = nil, nil, nil, nil, nil
	h.prefix, h.seal = rootpublication.AcceptedCut{}, rootpublication.PhysicalSealReceipt{}
	nextRefund := c.releasePrivateCOWBasisInstall(next)
	next = nil
	nextRefund.Close()
	err := basis.release()
	basis = nil
	err = joinCOWCleanupError(err, c.releaseCut(cut))
	cut = nil
	guard.Release()
	return refund, err
}
