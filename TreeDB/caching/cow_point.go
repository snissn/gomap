package caching

import (
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// cowPointStaging is a single writer-owned reusable staging owner for unsynced
// WAL-off private inline point writes. It is charged once under cowCache's
// startup lease, accessed exclusively while holding cowCache.writerMu, cleared
// of all borrowed caller/cut aliases before releasing writerMu, and discarded
// before cowCache refunds its lease on Close.
type cowPointStaging struct {
	owner             cowBatchOwner
	entries           [1]batch.Entry
	shardIdxs         [1]int
	predictionStorage [1]memtable.COWMutation
	shardCnts         []int
	predictionGroups  [][]memtable.COWMutation
	cut               cowPreparedCut
	cutLeaseStorage   memtable.COWExternalLease
	preparedStorage   memtable.COWPrepared
	mutation          txnMutationOwner
	db                *DB
	shard             int
	prepared          bool
	prepareFn         func() error
	finalizeFn        func(commitlog.RawKVOperation) error
}

func cowPointStagingCharge(shards int) uint64 {
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointStaging{}))) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadCut{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(int(0)))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof([]memtable.COWMutation{}))) +
		memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(cowRawSuccessor{}))) +
		2*memtable.COWAllocationCharge(2*uint64(unsafe.Sizeof(uintptr(0))))
	if shards > cowReadCutInlineShards {
		bytes += memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof((*memtable.COWPrepared)(nil)))) +
			memtable.COWAllocationCharge(uint64(shards)*uint64(unsafe.Sizeof(memtable.COWRetirement{})))
	}
	return bytes
}

func newCOWPointStaging(shards int) *cowPointStaging {
	st := &cowPointStaging{
		shardCnts:        make([]int, shards),
		predictionGroups: make([][]memtable.COWMutation, shards),
	}
	st.owner.batch.cowState = &st.owner.state
	st.owner.batch.streamBypassOff = true
	st.owner.batch.entries = st.entries[:]
	st.owner.batch.shardCnts = st.shardCnts
	st.owner.batch.shardIdxs = st.shardIdxs[:]
	st.owner.state.predictionGroups = st.predictionGroups
	st.owner.state.predictionStorage = st.predictionStorage[:]
	st.owner.state.privatePoint = true
	if shards <= cowReadCutInlineShards {
		st.cut.prepared = st.cut.inlinePrepared[:shards:shards]
		st.cut.retired = st.cut.inlineRetired[:shards:shards]
	} else {
		st.cut.prepared = make([]*memtable.COWPrepared, shards)
		st.cut.retired = make([]memtable.COWRetirement, shards)
	}
	st.cut.successors = make([]cowRawSuccessor, shards)
	st.prepareFn = st.prepareInlinePointLocked
	st.finalizeFn = st.finalizeInlinePointLocked
	return st
}

func (st *cowPointStaging) bindLocked(db *DB, shard int, entry batch.Entry) {
	st.db = db
	st.shard = shard
	st.prepared = false
	st.entries[0] = entry
	st.shardIdxs[0] = shard
	st.shardCnts[shard] = 1
	st.owner.batch.db = db
	st.owner.state.shardAssignmentsReady = true
}

func (st *cowPointStaging) clearLocked(shard int) {
	st.db = nil
	st.prepared = false
	st.entries[0] = batch.Entry{}
	st.predictionStorage[0] = memtable.COWMutation{}
	if shard >= 0 && shard < len(st.shardCnts) {
		st.shardCnts[shard] = 0
		st.predictionGroups[shard] = nil
		st.cut.prepared[shard] = nil
		st.cut.retired[shard] = memtable.COWRetirement{}
		st.cut.successors[shard] = cowRawSuccessor{}
	}
	st.cut.cache = nil
	st.cut.next = nil
	st.cut.opening = nil
	st.cut.native = nil
	st.cut.lease = nil
	st.cutLeaseStorage = memtable.COWExternalLease{}
	st.cut.consumed = false
	st.preparedStorage = memtable.COWPrepared{}
	st.mutation = txnMutationOwner{}
	st.owner.batch.db = nil
	st.owner.state.shardAssignmentsReady = false
}

func (st *cowPointStaging) clearAll() {
	for i := range st.shardCnts {
		st.shardCnts[i] = 0
		st.predictionGroups[i] = nil
		st.cut.prepared[i] = nil
		st.cut.retired[i] = memtable.COWRetirement{}
		st.cut.successors[i] = cowRawSuccessor{}
	}
	st.clearLocked(-1)
	st.prepareFn = nil
	st.finalizeFn = nil
}

type cowPointCancelled struct {
	cache        *cowCache
	next         *cowReadCut
	opening      *cowReadCut
	retired      memtable.COWRetirement
	successor    cowRawSuccessor
	leaseStorage memtable.COWExternalLease
}

func (d cowPointCancelled) drain() error {
	var cleanupErr error
	if next := d.next; next != nil {
		d.next = nil
		shards, frozen, frozenSet := next.shards, next.frozen, next.frozenSet
		next.shards, next.frozen, next.frozenSet = nil, nil, nil
		clear(next.sources)
		clear(next.domains)
		next.sources, next.domains = nil, nil
		next.domainsCache.Store(nil)
		drainCOWTables(shards)
		if frozenSet != nil {
			frozenSet.release()
		} else {
			drainCOWTables(frozen)
		}
		clear(next.inlineShards[:])
		d.cache.releasePrivateSemanticState(next)
		basis := next.basis
		if next.lease == &next.leaseStorage {
			d.leaseStorage = next.leaseStorage
			next.leaseStorage = memtable.COWExternalLease{}
		}
		next.basis = nil
		next.cache, next.lease = nil, nil
		cleanupErr = basis.release()
		basis = nil
	}
	d.successor.drain()
	d.retired.Drain()
	if d.opening != nil {
		cleanupErr = joinCOWCleanupError(cleanupErr, d.cache.releaseCut(d.opening))
		d.opening = nil
	}
	d.cache = nil
	d.leaseStorage.Close()
	return cleanupErr
}

func (st *cowPointStaging) cancelAndDetachLocked(shard int) cowPointCancelled {
	if st.mutation.Valid() {
		st.mutation.Cancel()
		st.mutation = txnMutationOwner{}
		if st.db != nil && st.db.cow != nil {
			st.db.cow.updateTxnStats()
		}
	}
	if st.prepared {
		st.cut.cancel()
	}
	detached := cowPointCancelled{
		cache:        st.cut.cache,
		next:         st.cut.next,
		opening:      st.cut.opening,
		retired:      st.cut.retired[shard],
		successor:    st.cut.successors[shard],
		leaseStorage: st.cutLeaseStorage,
	}
	st.clearLocked(shard)
	return detached
}

func (st *cowPointStaging) prepareInlinePointLocked() (err error) {
	db := st.db
	c := db.cow
	if err := durabilitycut.EmitBasic(durabilitycut.BeforeCOWPreparation, durabilitycut.ResourceAuxiliary, db.dir); err != nil {
		return err
	}
	if c.cut.logicalSeq == ^uint64(0) {
		return memtable.ErrCOWCapacity
	}
	if err := c.ensureTxnJournal(); err != nil {
		return err
	}
	if c.journal != nil {
		defer func() {
			if err != nil {
				c.updateTxnStats()
			}
		}()
		mutation, prepErr := c.journal.Prepare(st.entries[:], nil, false)
		if prepErr != nil {
			return prepErr
		}
		st.mutation = mutation
	}
	e := &st.entries[0]
	shard := st.shard
	m := memtable.COWMutation{Key: e.Key, Value: e.Value, Revision: e.Revision, Flags: node.FlagInline}
	if e.Type == batch.OpDelete {
		m.Flags = node.FlagTombstone
		m.Value = nil
	}
	st.predictionStorage[0] = m

	c.prepareCalls.Add(1)
	old := c.cut
	shards := len(c.writers)
	leaseVal, err := c.budget.AcquireExternalValue(cowWriteCutCharge(shards))
	if err != nil {
		return err
	}
	st.cut.cache = c
	st.cutLeaseStorage = leaseVal
	st.cut.consumed = false
	st.prepared = true

	old.refs.Add(1)
	st.cut.opening = old

	if c.successors[shard].writer == nil {
		if err := c.writers[shard].ReserveRawRolloverSource(); err != nil {
			return err
		}
		st.cut.successors[shard].sourceReserved = true
		var succErr error
		st.cut.successors[shard].writer, succErr = c.newWriter(true)
		if succErr == nil {
			st.cut.successors[shard].root, st.cut.successors[shard].resources, succErr = prepareCOWReservedEmptyRoot(st.cut.successors[shard].writer, c.maintenance)
		}
		if succErr != nil {
			return succErr
		}
	}
	staged, err := c.writers[shard].PrepareInto(&st.preparedStorage, st.predictionStorage[:1], memtable.COWPrepareOptions{})
	if err != nil {
		return err
	}
	st.cut.prepared[shard] = staged
	delta, valid := staged.LogicalSizeDelta()
	if !valid {
		return memtable.ErrCOWCapacity
	}
	if _, valid := cowApplySizeDelta(old.shards[shard].size, delta); !valid {
		return memtable.ErrCOWCapacity
	}

	next := c.takeRecycledCut()
	if next == nil {
		next = &cowReadCut{}
	}
	next.visibleCommandLSN = old.visibleCommandLSN
	next.logicalSeq = old.logicalSeq
	next.frozenLogicalSeq = old.frozenLogicalSeq
	next.frozenCommandLSN = old.frozenCommandLSN
	next.frozenSemanticRootSet = old.frozenSemanticRootSet
	next.semanticRootSet = old.semanticRootSet
	next.basis = old.basis
	next.leaseStorage = st.cutLeaseStorage
	next.cache = c
	st.cutLeaseStorage = memtable.COWExternalLease{}
	next.lease = &next.leaseStorage
	next.initShards(shards)
	next.basis.retain()
	retainCOWSemanticState(next)
	if old.frozenSet != nil {
		next.frozenSet = old.frozenSet
		next.frozenSet.retain()
		next.frozen = old.frozen
	} else if len(old.frozen) > 0 {
		next.frozen = make([]cowTable, len(old.frozen))
		copy(next.frozen, old.frozen)
		for i := range next.frozen {
			if !next.frozen[i].root.RetainOwned() {
				panic("lost owned COW frozen root")
			}
		}
	}
	for i := range shards {
		next.shards[i] = old.shards[i]
		if i == shard {
			charge := staged.Charge().History()
			if charge > ^uint64(0)-next.shards[i].history {
				panic("COW accepted history accounting overflow")
			}
			next.shards[i].history += charge
			next.shards[i].root = staged.Root()
			next.shards[i].size += delta
		} else if !next.shards[i].root.RetainOwned() {
			panic("lost owned COW root")
		}
	}
	st.cut.next = next
	return durabilitycut.EmitBasic(durabilitycut.AfterCOWPreparation, durabilitycut.ResourceAuxiliary, db.dir)
}

func (st *cowPointStaging) finalizeInlinePointLocked(op commitlog.RawKVOperation) error {
	if !st.prepared || st.cut.next == nil {
		return ErrCOWUnsupported
	}
	if err := validateCOWCanonicalEntry(0, &st.entries[0], op.Op, op.Key, op.Value, op.Revision, nil); err != nil {
		return err
	}
	return durabilitycut.EmitBasic(durabilitycut.AfterCOWCanonicalPreparation, durabilitycut.ResourceAuxiliary, st.db.dir)
}

func (st *cowPointStaging) canSkipPointRolloverLocked(db *DB, shard int, entry batch.Entry) bool {
	c := db.cow
	if c.ordered || db.mutableBytes.Load() > db.mutableFlushThreshold() {
		return false
	}
	limits := c.budget.Limits()
	maxGen := limits.MaxGenerationBytes
	if c.generationBase > maxGen {
		return false
	}
	m := memtable.COWMutation{Key: entry.Key, Value: entry.Value, Flags: node.FlagInline}
	if entry.Type == batch.OpDelete {
		m.Flags, m.Value = node.FlagTombstone, nil
	}
	bound, valid := c.writers[shard].RawPointPredictionUpperBound(m)
	if !valid || bound.History() > maxGen-c.generationBase {
		return false
	}
	trigger := maxGen - maxGen/4
	used := c.cut.shards[shard].history
	if c.cut.shards[shard].Len() > 0 && (used > trigger || bound.History() > trigger-used) {
		return false
	}
	control := cowWriteCutCharge(len(c.writers))
	if bound.Total() > ^uint64(0)-control {
		return false
	}
	total := control + bound.Total()
	successors := 0
	if c.successors[shard].writer == nil {
		successors = 1
	}
	stats := c.budget.Stats()
	if stats.Sources > limits.MaxSources || successors > limits.MaxSources-stats.Sources ||
		stats.Generations > limits.MaxGenerations || successors > limits.MaxGenerations-stats.Generations {
		return false
	}
	remaining := limits.MaxRetiredBytes
	for _, n := range [...]uint64{stats.HistoryBytes, stats.ReservedBytes, stats.DeferredBytes, total} {
		if n > remaining {
			return false
		}
		remaining -= n
	}
	return stats.TotalBytes <= limits.MaxTotalBytes && total <= limits.MaxTotalBytes-stats.TotalBytes &&
		stats.ReservedBytes <= limits.MaxInFlightBytes && total <= limits.MaxInFlightBytes-stats.ReservedBytes
}

// WritePrivateCOWInlinePoint executes an unsynced private WAL-off inline point
// Set or Delete using cowCache's pre-admitted reusable staging owner, avoiding
// per-op cowBatchOwner, write-storage, and preparation wrapper allocations.
// Large values that require value-log pointer placement return handled=false
// before acquiring writerMu so the caller falls back cleanly to the ordinary
// private point batch path.
func (db *DB) WritePrivateCOWInlinePoint(owner *transactionowner.Owner, key, value []byte, put bool) (handled bool, err error) {
	if db == nil || db.cow == nil || owner == nil || owner != db.cow.txnOwner || !db.disableJournal || db.externalCommandWAL {
		return false, nil
	}
	finalizer, ok := db.backend.(interface {
		PreflightPrivateCOWCommand(*transactionowner.Owner) error
		FinalizePrivateCOWInlinePoint(*transactionowner.Owner, batch.Entry, func() error, func(commitlog.RawKVOperation) error) error
	})
	if !ok {
		return false, nil
	}
	key = normalizeRawKVPointKey(key)
	if put {
		value = normalizeRawKVValue(value)
		if db.valueLogEnabled() && db.allowValueLogPointers() && db.memtableValueLogPointers && db.shouldWriteViaValueLogForKeyValue(key, value) {
			return false, nil
		}
	} else {
		value = nil
	}
	if err := durabilitycut.EmitBasic(durabilitycut.BeforeCOWAdmission, durabilitycut.ResourceAuxiliary, db.dir); err != nil {
		return true, err
	}
	if err := finalizer.PreflightPrivateCOWCommand(owner); err != nil {
		return true, err
	}
	c := db.cow
	if stats, limits := c.budget.Stats(), c.budget.Limits(); stats.ReservedBytes >= limits.MaxInFlightBytes || stats.TotalBytes >= limits.MaxTotalBytes {
		return true, memtable.ErrCOWCapacity
	}
	var accepted rootpublication.AcceptedTicket
	if c.prefix != nil {
		ticket, admitErr := c.prefix.ReserveAccepted(false)
		if admitErr != nil {
			ticket, admitErr = db.reserveCOWAcceptedOrdinaryAfterPressure(c, admitErr)
			if admitErr != nil {
				return true, admitErr
			}
		}
		accepted = ticket
		defer func() {
			if accepted.Valid() {
				if cancelErr := accepted.CancelAccepted(); cancelErr != nil {
					db.reportError(cancelErr)
				}
			}
		}()
	}

	if err := db.beginDirectWriteWithCOWRefreshWait(false); err != nil {
		return true, err
	}
	shard := db.shardIndex(key)
	entry := batch.Entry{
		Type:     batch.OpDelete,
		Key:      key,
		Revision: page.LegacyEntryRevision,
	}
	if put {
		entry.Type = batch.OpPut
		entry.Value = value
	}

	var leaseWaitStart time.Time
	hasWaitStart := false
	pressureJoined := false
	pressureRolloverUsed := false
	st := c.pointStaging
	firstAttempt := true
	for {
		if firstAttempt {
			firstAttempt = false
			if !c.writerMu.TryLock() {
				leaseWaitStart = time.Now()
				hasWaitStart = true
				c.writerMu.Lock()
			}
		} else {
			c.writerMu.Lock()
		}
		st.bindLocked(db, shard, entry)
		if st.canSkipPointRolloverLocked(db, shard, entry) {
			break
		}
		if !hasWaitStart {
			leaseWaitStart = time.Now()
			hasWaitStart = true
		}
		rollover, pressure, pressureRollover, rollErr := st.owner.batch.cowGenerationRollover(!pressureRolloverUsed)
		if pressureRollover {
			pressureRolloverUsed = true
		}
		if pressure {
			st.clearLocked(shard)
			c.writerMu.Unlock()
			db.writeMu.RUnlock()
			if pressureJoined {
				return true, memtable.ErrCOWCapacity
			}
			pressureJoined = true
			if cpErr := db.Checkpoint(); cpErr != nil {
				return true, cpErr
			}
			if waitErr := db.beginDirectWriteWithCOWRefreshWait(false); waitErr != nil {
				return true, waitErr
			}
			continue
		}
		if rollErr != nil || rollover != nil {
			if rollErr == nil {
				rollover.install()
				db.mutableBytes.Store(0)
			}
			st.clearLocked(shard)
			c.writerMu.Unlock()
			db.writeMu.RUnlock()
			if cleanupErr := rollover.drain(); cleanupErr != nil {
				db.reportError(cleanupErr)
				return true, joinCOWCleanupError(rollErr, cleanupErr)
			}
			if rollErr != nil {
				return true, rollErr
			}
			db.flushRequested.Store(true)
			select {
			case db.flushCh <- struct{}{}:
			default:
			}
			if waitErr := db.beginDirectWriteWithCOWRefreshWait(false); waitErr != nil {
				return true, waitErr
			}
			continue
		}
		break
	}

	trackHold := hasWaitStart || c.journal != nil
	var holdBegan time.Time
	if trackHold {
		holdBegan = time.Now()
		if hasWaitStart {
			c.commitWaitNs.Add(uint64(holdBegan.Sub(leaseWaitStart)))
		}
	}
	locksHeld := true
	canCancel := true
	defer func() {
		if !locksHeld {
			return
		}
		var detached cowPointCancelled
		if canCancel {
			detached = st.cancelAndDetachLocked(shard)
			if accepted.Valid() {
				if cancelErr := accepted.CancelAccepted(); cancelErr != nil {
					db.reportError(cancelErr)
				}
				accepted = rootpublication.AcceptedTicket{}
			}
		} else {
			st.clearLocked(shard)
		}
		locksHeld = false
		if trackHold {
			c.commitHoldNs.Add(uint64(time.Since(holdBegan)))
		}
		c.writerMu.Unlock()
		db.writeMu.RUnlock()
		if canCancel {
			if cleanupErr := detached.drain(); cleanupErr != nil {
				db.reportError(cleanupErr)
			}
		}
	}()

	st.entries[0].Revision = db.nextEntryRevision()
	finalizeErr := finalizer.FinalizePrivateCOWInlinePoint(owner, st.entries[0], st.prepareFn, st.finalizeFn)
	if finalizeErr != nil {
		detached := st.cancelAndDetachLocked(shard)
		if accepted.Valid() {
			if cancelErr := accepted.CancelAccepted(); cancelErr != nil {
				db.reportError(cancelErr)
			}
			accepted = rootpublication.AcceptedTicket{}
		}
		locksHeld = false
		if trackHold {
			c.commitHoldNs.Add(uint64(time.Since(holdBegan)))
		}
		c.writerMu.Unlock()
		db.writeMu.RUnlock()
		return true, joinCOWCleanupError(finalizeErr, detached.drain())
	}
	if !st.prepared || st.cut.next == nil {
		panic("COW inline point succeeded without staged publication")
	}

	_ = durabilitycut.EmitBasic(durabilitycut.BeforeCOWCutSwap, durabilitycut.ResourceAuxiliary, db.dir)
	canCancel = false

	next := st.cut.next
	next.logicalSeq = c.cut.logicalSeq + 1
	if st.mutation.Valid() {
		c.journal.Install(st.mutation, next.logicalSeq)
		c.updateTxnStats()
		st.mutation = txnMutationOwner{}
	}
	admission := db.beginMVCCInputCut()
	admission.ObserveSummary(mvccadmission.Summary{})
	old, _, publishErr := st.cut.publishAccepted(accepted)
	if publishErr != nil {
		admission.Abort()
		if poisoner, ok := db.backend.(interface{ MarkCommandWALRecoveryRequired() }); ok {
			poisoner.MarkCommandWALRecoveryRequired()
		}
		detached := st.cancelAndDetachLocked(shard)
		locksHeld = false
		if trackHold {
			c.commitHoldNs.Add(uint64(time.Since(holdBegan)))
		}
		c.writerMu.Unlock()
		db.writeMu.RUnlock()
		return true, joinCOWCleanupError(publishErr, detached.drain())
	}
	admission.Commit()
	accepted = rootpublication.AcceptedTicket{}
	c.inlinePointPublications.Add(1)
	var mutableBytes int64
	for _, s := range next.shards {
		mutableBytes += s.size
	}
	db.mutableBytes.Store(mutableBytes)
	st.clearLocked(shard)
	locksHeld = false
	if trackHold {
		c.commitHoldNs.Add(uint64(time.Since(holdBegan)))
	}
	c.writerMu.Unlock()
	db.writeMu.RUnlock()

	if old != nil {
		if drainErr := old.drain(); drainErr != nil {
			db.reportError(drainErr)
			db.noteWrite()
			return true, drainErr
		}
	}
	db.noteWrite()
	return true, nil
}
