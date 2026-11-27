package caching

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
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
	failure  error
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
			// Releasing a maintenance lease or reader cannot make this shape fit.
			return fmt.Errorf("snapshot value-log shape exceeds COW resource limit: %w", memtable.ErrCOWCapacity)
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
func cowBasisInstallCharge(shards, frozen int) uint64 {
	return cowCutCharge(shards, frozen) + memtable.COWAllocationCharge(6*uint64(unsafe.Sizeof(uintptr(0))))
}

func (c *cowCache) prepareCOWBasisInstall() (*cowReadCut, error) {
	return c.prepareCOWBasisInstallWithFrozen(c.budget.Limits().MaxSources)
}

func (c *cowCache) prepareCOWBasisInstallWithFrozen(frozen int) (*cowReadCut, error) {
	return c.prepareCOWBasisInstallWithOptions(frozen, frozen > 0)
}

func (c *cowCache) prepareCOWBasisInstallWithOptions(frozen int, allocateFrozen bool) (*cowReadCut, error) {
	shards := len(c.writers)
	lease, err := c.maintenance.AcquireExternal(cowBasisInstallCharge(shards, frozen))
	if err != nil {
		return nil, err
	}
	var frozenSet *cowFrozenSet
	if allocateFrozen && frozen > 0 {
		frozenSet, err = admitCOWFrozenSet(c.maintenance, 0, frozen)
		if err != nil {
			lease.Close()
			return nil, err
		}
	}
	cut := &cowReadCut{cache: c, lease: lease, frozenSet: frozenSet}
	cut.refs.Store(1)
	cut.initShards(shards)
	if frozenSet != nil {
		cut.frozen = frozenSet.tables
	}
	cut.sources = make([]memtable.Table, 0, shards+frozen)
	cut.domains = make([]rootDomainSnapshot, shards)
	return cut, nil
}

func (db *DB) installCOWBasis(basis *cowBackendBasis, generationPressure bool) (*cowReadCut, error) {
	c := db.cow
	// The equivalent refresh caller fences both physical cutovers and
	// producer admission, including draining already-admitted writers.
	// The current frozen cardinality suffices until the next producer-owned
	// cut, which always has its own independently admitted source vectors.
	c.cutMu.Lock()
	if c.cut == nil {
		c.cutMu.Unlock()
		return nil, backenddb.ErrClosed
	}
	frozen := len(c.cut.frozen)
	allocateFrozen := frozen > 0 && (c.cut.frozenSet == nil || cap(c.cut.frozen) != frozen)
	c.cutMu.Unlock()
	return db.installCOWBasisWithLeafPackOptions(basis, backenddb.LeafGenerationPackTransition{}, frozen, allocateFrozen, generationPressure)
}

func (db *DB) installCOWBasisWithLeafPack(basis *cowBackendBasis, transition backenddb.LeafGenerationPackTransition, frozen int, generationPressure bool) (*cowReadCut, error) {
	return db.installCOWBasisWithLeafPackOptions(basis, transition, frozen, frozen > 0, generationPressure)
}

func (db *DB) installCOWBasisWithLeafPackOptions(basis *cowBackendBasis, transition backenddb.LeafGenerationPackTransition, frozen int, allocateFrozen, generationPressure bool) (*cowReadCut, error) {
	c := db.cow
	next, err := c.prepareCOWBasisInstallWithOptions(frozen, allocateFrozen)
	if err != nil {
		if generationPressure && err == memtable.ErrCOWCapacity && c.canRetryCOWBasisSuccessor(basis, frozen, allocateFrozen) {
			// The caller can mark this pre-swap refusal retryable after
			// it has successfully released the captured private basis.
			err = &cowGenerationSnapshotAllocationRefusal{error: err, basisRefresh: true}
		}
		return nil, err
	}
	finished := false
	defer func() {
		if !finished {
			refund := c.releasePrivateCOWBasisInstall(next)
			next = nil
			refund.Close()
		}
	}()
	if err = basis.snapshot.ActivatePrimaryMetadataProducer(); err != nil {
		refund := c.releasePrivateCOWBasisInstall(next)
		next = nil
		refund.Close()
		finished = true
		return nil, err
	}
	old, _, err := db.installPreparedCOWBasisWithLeafPack(basis, nil, next, transition)
	if err != nil {
		refund := c.releasePrivateCOWBasisInstall(next)
		next = nil
		refund.Close()
	}
	finished = true
	return old, err
}

func (c *cowCache) canRetryCOWBasisSuccessor(basis *cowBackendBasis, frozen int, allocateFrozen bool) bool {
	// The shared flush fence keeps the live backend basis installed until this
	// refresh swaps it. Neither it nor the private candidate can be refunded
	// to make room for their successor vector.
	c.cutMu.Lock()
	defer c.cutMu.Unlock()
	old := c.cut
	if old == nil {
		return false
	}
	// Each cut holds its original basis lease while installed. A current cut
	// itself can also occupy this same partition after an earlier handoff.
	bytes := cowBasisInstallCharge(len(c.writers), frozen)
	if allocateFrozen && frozen > 0 {
		bytes += cowFrozenSetCharge(frozen)
	}
	var oldFrozenLease *memtable.COWExternalLease
	if old.frozenSet != nil {
		oldFrozenLease = &old.frozenSet.leaseStorage
	}
	return c.maintenance.ExternalAllocationFitsWith(bytes, old.basis.lease, basis.lease, old.lease, oldFrozenLease)
}

// A failed private successor owns no published raw-root edges. End its semantic
// edges while the retained opening cut still keeps the cleanup lane alive;
// return only independent accounting after all containing aliases are scrubbed.
func (c *cowCache) releasePrivateCOWBasisInstall(next *cowReadCut) *memtable.COWExternalLease {
	if next == nil {
		return nil
	}
	c.releasePrivateSemanticState(next)
	frozenSet := next.frozenSet
	refund := next.lease
	next.lease, next.cache, next.basis, next.frozenSet = nil, nil, nil, nil
	next.shards, next.frozen, next.sources, next.domains = nil, nil, nil, nil
	next.domainsCache.Store(nil)
	clear(next.inlineShards[:])
	frozenSet.release()
	return refund
}

func (db *DB) installPreparedCOWBasis(basis *cowBackendBasis, covered, next *cowReadCut) (*cowReadCut, bool, error) {
	return db.installPreparedCOWBasisWithLeafPack(basis, covered, next, backenddb.LeafGenerationPackTransition{})
}

func (db *DB) installPreparedCOWBasisWithLeafPack(basis *cowBackendBasis, covered, next *cowReadCut, transition backenddb.LeafGenerationPackTransition) (*cowReadCut, bool, error) {
	c := db.cow
	var leaves *cowMaintenanceLeafScope
	var unusedFrozen *cowFrozenSet
	c.writerMu.Lock()
	if covered == nil && c.cut != nil && (c.cut.semanticRootSet != nil || c.cut.frozenSemanticRootSet != nil) {
		// Only semantic refresh can read dictionary leaves. Inspect eligibility
		// under the writer gate, then release it before fallible admission. The
		// install below rereads the current cut after reacquiring the gate.
		c.writerMu.Unlock()
		var err error
		leaves, err = db.prepareCOWMaintenanceLeaves()
		if err != nil {
			return nil, false, err
		}
		// Registered before Unlock: queue transfer follows gate release even
		// on panic/Goexit. The existing worker owns checked physical cleanup.
		defer c.retireCOWMaintenanceLeaves(&leaves)
		c.writerMu.Lock()
	}
	defer func() {
		if unusedFrozen != nil {
			unusedFrozen.release()
		}
	}()
	defer c.writerMu.Unlock()
	old := c.cut
	if old == nil {
		return nil, false, backenddb.ErrClosed
	}
	n := 0
	if covered != nil {
		n = len(covered.frozen)
		if old.basis != covered.basis || len(old.frozen) < n {
			return nil, false, backenddb.ErrRootPublicationBasisMismatch
		}
		for i := 0; i < n; i++ {
			if old.frozen[i].root != covered.frozen[i].root {
				return nil, false, backenddb.ErrRootPublicationBasisMismatch
			}
		}
	}
	remaining := len(old.frozen) - n
	shareFrozen := covered == nil && remaining > 0 && old.frozenSet != nil && cap(old.frozen) == remaining
	if next == nil || basis == nil || basis.snapshot == nil || (!shareFrozen && remaining > cap(next.frozen)) || len(old.shards) != len(next.shards) {
		return nil, false, backenddb.ErrRootPublicationBasisMismatch
	}
	if !transition.IsZero() {
		if covered != nil {
			return nil, false, backenddb.ErrRootPublicationBasisMismatch
		}
		if err := transition.Validate(old.basis.snapshot, basis.snapshot); err != nil {
			return nil, false, err
		}
	}
	if covered != nil {
		basis.logicalSeq = covered.frozenLogicalSeq
	} else {
		// Nil coverage changes only the physical Snapshot. A visible mutable or
		// frozen suffix still belongs to the cut, not to this applied basis.
		basis.logicalSeq = old.basis.logicalSeq
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
	if covered == nil {
		// Keep the absent scope a nil interface, rather than a nonnil interface
		// containing a typed nil *cowMaintenanceLeafScope.
		var leafReader backenddb.NativeMaintenanceLeafReader
		if leaves != nil {
			leaves.before, leaves.after = old.basis.snapshot, basis.snapshot
			leafReader = leaves
		}
		// Maintenance changed only the physical basis. It has no captured build
		// ledger and cannot consume a logical source or manufacture coverage.
		// Adopt each private partial before checking err; installCOWBasis releases
		// it through original cleanup only after this writer gate is dropped.
		var err error
		next.semanticRootSet, err = refreshCOWSemanticPhysicalBasis(old.semanticRootSet, old.basis.snapshot, basis.snapshot, transition, leafReader)
		if err != nil {
			db.poisonNativeHandoff(err)
			return nil, true, err
		}
		if old.frozenSemanticRootSet == old.semanticRootSet {
			next.frozenSemanticRootSet = next.semanticRootSet
			if next.frozenSemanticRootSet != nil {
				next.frozenSemanticRootSet.Retain()
			}
		} else {
			next.frozenSemanticRootSet, err = refreshCOWSemanticPhysicalBasis(old.frozenSemanticRootSet, old.basis.snapshot, basis.snapshot, transition, leafReader)
			if err != nil {
				db.poisonNativeHandoff(err)
				return nil, true, err
			}
		}
	} else if captured != nil {
		state, err := captured.PrepareHandoff(old.semanticRootSet, basis.snapshot)
		if err != nil {
			_, retryable := err.(backenddb.NativeSemanticHandoffRetryableRefusal)
			if !retryable {
				db.poisonNativeHandoff(err)
			}
			return nil, !retryable, err
		}
		if state == nil {
			db.poisonNativeHandoff(rootpublication.ErrPublisherProtocol)
			return nil, true, rootpublication.ErrPublisherProtocol
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
				db.poisonNativeHandoff(err)
				c.releasePrivateSemanticState(next)
				return nil, true, err
			}
			if frozen == nil {
				db.poisonNativeHandoff(rootpublication.ErrPublisherProtocol)
				c.releasePrivateSemanticState(next)
				return nil, true, rootpublication.ErrPublisherProtocol
			}
			next.frozenSemanticRootSet = frozen
		}
	} else {
		next.frozenSemanticRootSet = old.frozenSemanticRootSet
		next.semanticRootSet = old.semanticRootSet
		retainCOWSemanticState(next)
	}
	basis.retain()
	copy(next.shards, old.shards)
	for i := range next.shards {
		if !next.shards[i].root.Retain() {
			panic("lost COW handoff mutable")
		}
	}
	if shareFrozen {
		unusedFrozen = next.frozenSet
		next.frozenSet = old.frozenSet
		next.frozenSet.retain()
		next.frozen = old.frozen
	} else if remaining == 0 {
		unusedFrozen = next.frozenSet
		next.frozenSet = nil
		next.frozen = nil
	} else {
		next.frozen = next.frozen[:remaining]
		copy(next.frozen, old.frozen[n:])
		for i := range next.frozen {
			if !next.frozen[i].root.Retain() {
				panic("lost COW handoff frozen")
			}
		}
		if next.frozenSet != nil {
			next.frozenSet.tables = next.frozen
		}
	}
	next.buildDomains()
	c.cutMu.Lock()
	c.cut = next
	c.activeCuts.Add(1)
	c.liveCut.Store(next)
	last := old.refs.Add(-1) == 0
	c.cutMu.Unlock()
	if last {
		return old, false, nil
	}
	return nil, false, nil
}

// Nil is the original raw-only case. A semantic owner must explicitly prove
// equivalent physical rebinding; retaining an unknown descriptor can leave its
// scalar roots referring to the wrong Snapshot after rewrite/vacuum.
func refreshCOWSemanticBasis(state backenddb.NativeSemanticState, before, after *backenddb.Snapshot) (backenddb.NativeSemanticState, error) {
	return refreshCOWSemanticBasisWithMaintenanceLeaves(state, before, after, nil)
}

func refreshCOWSemanticBasisWithMaintenanceLeaves(state backenddb.NativeSemanticState, before, after *backenddb.Snapshot, leaves backenddb.NativeMaintenanceLeafReader) (backenddb.NativeSemanticState, error) {
	return refreshCOWSemanticPhysicalBasis(state, before, after, backenddb.LeafGenerationPackTransition{}, leaves)
}

func refreshCOWSemanticPhysicalBasis(state backenddb.NativeSemanticState, before, after *backenddb.Snapshot, transition backenddb.LeafGenerationPackTransition, leaves backenddb.NativeMaintenanceLeafReader) (backenddb.NativeSemanticState, error) {
	if state == nil {
		return nil, nil
	}
	var next backenddb.NativeSemanticState
	var err error
	if !transition.IsZero() {
		refresher, ok := state.(backenddb.NativeSemanticLeafPackBasisRefresher)
		if !ok {
			return nil, backenddb.ErrNativeSemanticUnsupported // never drop either authority
		}
		next, err = refresher.PrepareLeafPackBasisRefresh(before, after, transition, leaves)
	} else {
		refresher, ok := state.(backenddb.NativeSemanticBasisRefresher)
		if !ok {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if scoped, ok := state.(backenddb.NativeSemanticMaintenanceBasisRefresher); leaves != nil && ok {
			next, err = scoped.PrepareBasisRefreshWithMaintenanceLeaves(before, after, leaves)
		} else {
			next, err = refresher.PrepareBasisRefresh(before, after)
		}
	}
	if err == nil && next == nil {
		err = rootpublication.ErrPublisherProtocol
	}
	return next, err
}

func (db *DB) poisonNativeHandoff(err error) {
	db.cow.prefix.PoisonAccepted(err)
	if marker, ok := db.backend.(interface{ MarkNativeSemanticRecoveryRequired() }); ok {
		marker.MarkNativeSemanticRecoveryRequired()
	}
}

func (db *DB) completeCOWHandoff() error {
	return db.completeCOWHandoffForGeneration(false)
}

func (db *DB) completeCOWHandoffForGeneration(generationPressure bool) error {
	c := db.cow
	h := c.handoff
	if h == nil {
		return nil
	}
	if h.failure != nil {
		return h.failure
	}
	// The preaccept receipt owns this exact generation even when backend state
	// has advanced or capture admission is now exhausted. Retry only this swap.
	old, terminal, err := db.installPreparedCOWBasis(h.basis, h.cut, h.next)
	if err != nil {
		if terminal {
			h.failure = err // same admitted handoff; never replay either preparation
		} else if _, refused := err.(backenddb.NativeSemanticHandoffRetryableRefusal); generationPressure && refused {
			return cowGenerationRetryableRefusal{err}
		}
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

// cowFlushDefaultMaxChunkEntries is the fallback chunk cap when
// db.flushBackendMaxEntries <= 0. Open() supplies normalized profile limits
// (16,384 relaxed / 32,768 durable) and explicit overrides remain
// authoritative; all are bounded by 25% of maintenance MaxInFlightBytes.
const cowFlushDefaultMaxChunkEntries = 65536

func (db *DB) cowFrozenFlushChunkLimit(maintenance *memtable.COWBudget) int {
	limit := cowFlushDefaultMaxChunkEntries
	if db.flushBackendMaxEntries > 0 {
		limit = db.flushBackendMaxEntries
	}
	maxByBudget := (maintenance.Limits().MaxInFlightBytes / 4) / uint64(unsafe.Sizeof(batch.Entry{}))
	return int(min(uint64(limit), max(1, maxByBudget)))
}

// flushCOWFrozenCut consumes the one original retained source cut. Explicit
// boundaries captured it under P before post-frontier admissions; background
// passes use its immutable frozen logical/semantic coverage, excluding mutable
// suffixes. Every preaccept exit unwinds through the same original owners.
func (db *DB) flushCOWFrozenCut(syncFlush bool, publish *checkpointCommandWALPublish, cut *cowReadCut) (retErr error) {
	return db.flushCOWFrozenCutForFallback(syncFlush, publish, cut, false)
}

// forceIdle is private to the exact-basis, fully sealed fallback predicate path.
func (db *DB) flushCOWFrozenCutForFallback(syncFlush bool, publish *checkpointCommandWALPublish, cut *cowReadCut, forceIdle bool) error {
	return db.flushCOWFrozenCutWithColumnCompaction(syncFlush, publish, cut, forceIdle, nil, false, true)
}

func (db *DB) flushCOWFrozenCutWithColumnCompaction(syncFlush bool, publish *checkpointCommandWALPublish, cut *cowReadCut, forceIdle bool, operation backenddb.NativeColumnStoreCompaction, generationPressure bool, syncCommandWAL bool) (retErr error) {
	c := db.cow
	// The caller receives completion only. Coverage is always reconstructed
	// from this immutable cut and its exact captured disk basis below.
	completed := publish
	var localPublish checkpointCommandWALPublish
	publish = &localPublish
	if completed != nil {
		*completed = checkpointCommandWALPublish{}
	}
	if cut == nil {
		return backenddb.ErrClosed
	}
	target := rootpublication.SealedPrefix{LogicalSequence: cut.frozenLogicalSeq, CanonicalLSN: cut.frozenCommandLSN}
	if !forceIdle && len(cut.frozen) == 0 && (c.prefix == nil || target == c.prefix.LogicalFrontiers().S) {
		return c.releaseCut(cut)
	}
	maxCap := db.cowFrozenFlushChunkLimit(c.maintenance)
	totalEntries := 0
	for _, source := range cut.frozen {
		if sourceLen := source.Len(); sourceLen > 0 {
			totalEntries += min(sourceLen, maxCap-totalEntries)
			if totalEntries >= maxCap {
				break
			}
		}
	}
	chunkCap := max(1, totalEntries)
	fixedCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowFlushHandoff{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(backenddb.StableResourceCaptureLease{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(checkpointCommandWALPublish{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof([1]backenddb.CommandWALLSNRange{})))
	lease, err := c.maintenance.AcquireExternal(fixedCharge)
	if err != nil {
		return joinCOWCleanupError(cowGenerationAdmissionError(generationPressure, err), c.releaseCut(cut))
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
	var localRange [1]backenddb.CommandWALLSNRange
	var sealedFallback bool
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
		// Fallback convergence republishes an already sealed basis into the
		// other recovery slot. Any later commands belong to a different cut.
		// The fallback still performs its full DATA publication.
		sealedFallback = forceIdle && operation == nil && len(cut.frozen) == 0 &&
			target.CanonicalLSN == basis.AppliedCommandLSN && target.LogicalSequence == cut.basis.logicalSeq &&
			target == c.prefix.LogicalFrontiers().S
		if target.CanonicalLSN > basis.AppliedCommandLSN {
			localRange[0] = backenddb.CommandWALLSNRange{First: basis.AppliedCommandLSN + 1, Last: target.CanonicalLSN}
			localPublish.appliedLSN, localPublish.ranges = target.CanonicalLSN, localRange[:]
		}
	}
	// The value-log lanes are flushed before checkpoint enters here. Keep the
	// WAL flush/readiness boundary even though its old per-producer coverage
	// provider is no longer used. A later suffix may also become durable, but
	// it cannot enlarge this cut's applied range or semantic build.
	if db.externalCommandWAL {
		wal, ok := db.backend.(interface {
			FlushCommandWAL(bool) error
			CheckCommandWALPublishReady() error
		})
		if !ok {
			return ErrCOWUnsupported
		}
		captured, canCapture := db.backend.(interface {
			SyncPrivateCOWGenerationWAL(*transactionowner.Owner) error
		})
		if generationPressure && db.relaxedSync && canCapture && c.txnOwner != nil {
			err = captured.SyncPrivateCOWGenerationWAL(c.txnOwner)
		} else if syncFlush {
			// Always drain the writer and surface journal-local failures, even
			// when the command prefix already has durable root coverage or
			// explicit relaxed Checkpoint defers active WAL fsync until
			// WaitThrough proves durableAppliedLSN >= frontier.
			err = wal.FlushCommandWAL(!sealedFallback && syncCommandWAL)
		} else {
			err = wal.CheckCommandWALPublishReady()
		}
		if err != nil {
			return err
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
	merge, err := openCOWFrozenMerge(c.budget, cut.frozen)
	if err != nil {
		return err
	}
	defer func() { retErr = joinCOWCleanupError(retErr, merge.close()) }()
	group, err := backend.BeginRootPublicationBuildGroupBorrowingLease(cut.basis.snapshot, h.guard)
	if err != nil {
		return cowGenerationAdmissionError(generationPressure, err)
	}
	if h.prefix.Valid() {
		if err = group.BindAcceptedPrefixCut(h.prefix); err != nil {
			return errors.Join(err, group.Close())
		}
	}
	h.next, err = c.prepareCOWBasisInstall()
	if err != nil {
		return errors.Join(cowGenerationAdmissionError(generationPressure, err), group.Close())
	}
	prepareSnapshot := group.PrepareAcceptedSnapshot
	if generationPressure {
		prepareSnapshot = group.PrepareGenerationAcceptedSnapshot
	}
	err = prepareSnapshot(func(s backenddb.SnapshotAllocationSizes) error {
		if s.ValueLog.MapHint > c.budget.Limits().MaxResources || s.ValueLog.FileCount > c.budget.Limits().MaxResources {
			return fmt.Errorf("candidate snapshot value-log shape exceeds COW resource limit: %w", memtable.ErrCOWCapacity)
		}
		var e error
		h.basis, e = admitCOWBackendBasis(c.maintenance, s)
		if generationPressure && e == memtable.ErrCOWCapacity {
			budget := c.maintenance
			leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			emptySpace := budget.Limits().MaxTotalBytes - budget.Stats().ControlBytes
			if leaseBytes > emptySpace || cowBasisCharge(s) > emptySpace-leaseBytes {
				return fmt.Errorf("candidate snapshot basis exceeds COW maintenance limit: %w", e)
			}
		}
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
	if err != nil {
		return errors.Join(err, group.Close())
	}
	if captured := cut.frozenSemanticRootSet; captured != nil {
		if operation != nil {
			if state, ok := captured.(backenddb.NativeColumnStoreCompactionState); ok {
				err = state.PrepareColumnStoreCompaction(operation, group, cut.basis.snapshot, h.guard)
			} else {
				err = backenddb.NativeSemanticBuildRetryableRefusal{Cause: backenddb.ErrNativeSemanticUnsupported}
			}
		} else {
			err = captured.PrepareBuild(group, cut.basis.snapshot, h.guard)
		}
		if err != nil {
			var refusal backenddb.NativeSemanticBuildRetryableRefusal
			if !errors.As(err, &refusal) {
				// Logical acceptance predates this physical build. A partial native
				// failure cannot be skipped or rebuilt by a later checkpoint, even
				// when its underlying cause happens to be a capacity error.
				c.prefix.PoisonAccepted(err)
				group.MarkNativeSemanticBuildRecoveryRequired()
			} else if _, refused := err.(backenddb.NativeSemanticBuildRetryableRefusal); generationPressure && refused {
				err = cowGenerationRetryableRefusal{err}
			}
			return errors.Join(err, group.Close())
		}
	}
	if cut.frozenSemanticRootSet == nil && operation != nil {
		return errors.Join(backenddb.ErrNativeSemanticUnsupported, group.Close())
	}
	for {
		charge := fixedCharge + memtable.COWAllocationCharge(uint64(chunkCap)*uint64(unsafe.Sizeof(batch.Entry{})))
		err = lease.Resize(charge)
		if err == nil || !errors.Is(err, memtable.ErrCOWCapacity) || chunkCap <= 1 {
			break
		}
		chunkCap /= 2
	}
	if err != nil {
		return errors.Join(cowGenerationAdmissionError(generationPressure, err), group.Close())
	}
	tail, err := writeCOWFrozenRawChunks(cut.frozen, &merge, chunkCap, func(ops []batch.Entry) error {
		return db.writeCanonicalFlushRunOpsChunk(ops, syncFlush, publish, group, false, flushCollectionBackground)
	})
	var finalBatch batch.Interface
	var commandPublishAttached bool
	if err == nil {
		finalBatch, commandPublishAttached, err = db.stageCanonicalFlushRunOpsChunk(tail, publish, group, true, flushCollectionBackground)
	}
	// The staged batch owns its entry metadata and borrows key/value bytes from
	// the captured cut. Refund the raw backing before Write admits the successor
	// snapshot. Only the captured prefix and handoff controls outlive the build.
	tail = nil
	err = joinCOWCleanupError(err, lease.Resize(fixedCharge))
	if finalBatch != nil {
		if err == nil {
			err = db.writeStagedCanonicalFlushRunChunk(finalBatch, syncFlush, publish, commandPublishAttached)
		} else {
			err = joinCOWCleanupError(err, finalBatch.Close())
		}
	}
	err = errors.Join(err, group.Close())
	// End local coverage accesses before completeCOWHandoff can refund this
	// original control tranche. Only scalar completion escapes, never ranges.
	if completed != nil {
		completed.appliedLSN = localPublish.appliedLSN
		completed.consumed = localPublish.consumed
	}
	if group.Accepted() {
		if operation != nil {
			// PrepareAcceptedSnapshot already adopted this exact accepted generation
			// into h.basis. Copy only its scalar receipt before handoff may release it;
			// accepted-plus-error follows the SAME original custody path below.
			token, _ := group.AcceptedSnapshot().StateToken()
			operation.CompleteColumnStoreCompaction(token.SystemRootPageID)
		}
		h.accepted, h.seal = true, group.AcceptedSealReceipt()
		if completed != nil {
			// Only the original value receipt escapes this admitted control tranche.
			completed.seal = h.seal
		}
		c.handoff = h
		handoffOwned, h = false, nil
		if err != nil {
			return err
		}
		return db.completeCOWHandoffForGeneration(generationPressure)
	}
	if generationPressure && backenddb.IsOnlyRootPublicationSnapshotAdmissionRefusal(err) {
		if errors.Is(err, memtable.ErrCOWCapacity) {
			err = &cowGenerationSnapshotAllocationRefusal{error: err, through: target}
		}
		return cowGenerationRetryableRefusal{err}
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
	h.prefix, h.seal, h.failure = rootpublication.AcceptedCut{}, rootpublication.PhysicalSealReceipt{}, nil
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
