package caching

import (
	"github.com/snissn/gomap/TreeDB/batch"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeConditionalTxn adapts only method names; its sole opening owner is P1.
type nativeConditionalTxn struct {
	*ConditionalTxn
	lease *memtable.COWExternalLease
}

// BeginNativeConditionalTxn uses the original P1 opening Snapshot, sequence,
// read/write validation and batch. It creates no second capture or journal.
func (db *DB) BeginNativeConditionalTxn() (backenddb.NativeConditionalTxn, error) {
	if err := db.PrepareNativeProducer(); err != nil {
		return nil, err
	}
	return db.BeginPreparedNativeConditionalTxn()
}

// BeginPreparedNativeConditionalTxn opens the same original transaction after
// outer producer preparation, without checkpointing under consumer locks.
func (db *DB) BeginPreparedNativeConditionalTxn() (backenddb.NativeConditionalTxn, error) {
	if db == nil || db.cow == nil {
		return nil, ErrCOWUnsupported
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(ConditionalTxn{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeConditionalTxn{})))
	lease, err := db.cow.budget.AcquireExternal(charge)
	if err != nil {
		return nil, err
	}
	tx := new(ConditionalTxn)
	if err := db.InitOwnedCOWTxn(tx, db.cow.txnOwner, nil); err != nil {
		lease.Close()
		return nil, err
	}
	return &nativeConditionalTxn{ConditionalTxn: tx, lease: lease}, nil
}

// A successful drain cannot reclaim unchanged owners retained by a reader.
// Keep current finite accounting, construction and physical/logical identity;
// historical high water is not outstanding debt or new maintenance work.
type cowNativePressureState struct {
	stats      memtable.COWStats
	logicalSeq uint64
	semantic   backenddb.NativeSemanticProducerPressure
	basis      backenddb.StateToken
}

func (s cowNativePressureState) underPressure(limits memtable.COWLimits) bool {
	p := s.semantic
	return s.stats.ReservedBytes >= limits.MaxInFlightBytes-limits.MaxInFlightBytes/4 ||
		s.stats.Sources >= limits.MaxSources || s.stats.Generations >= limits.MaxGenerations ||
		(p.ConstructionBytes != 0 &&
			(p.ConstructionBytes >= p.MaxInFlightBytes-p.MaxInFlightBytes/4 ||
				p.ConstructionBytes >= p.MaxTotalBytes-p.MaxTotalBytes/4 ||
				p.Sources >= p.MaxSources))
}

func (c *cowCache) nativeProducerPressureState() (state cowNativePressureState, err error) {
	// Retain the existing cut, including its exact basis and semantic state, by
	// incrementing its original edge under cutMu. No capture wrapper, receipt,
	// new Snapshot, iterator, or participant callback is created under the latch.
	cut := c.retainCut()
	if cut == nil {
		return state, backenddb.ErrClosed
	}
	state.logicalSeq = cut.logicalSeq
	if observer, ok := cut.semanticRootSet.(backenddb.NativeSemanticProducerPressureState); ok {
		state.semantic, err = observer.NativeProducerPressure(cut.basis.snapshot)
		if err == nil && state.semantic.ConstructionBytes != 0 {
			var valid bool
			state.basis, valid = cut.basis.snapshot.StateToken()
			if !valid {
				err = backenddb.ErrClosed
			}
		}
	}
	// The same cut keeps all borrowed scan inputs alive. End that edge before
	// returning, including on scan failure, so checkpoint/cleanup cannot wait on
	// the producer's observation. Drain and error propagation are off all gates.
	err = joinCOWCleanupError(err, c.releaseCut(cut))
	state.stats = c.budget.Stats()
	state.stats.PeakBytes = 0
	return state, err
}

// PrepareNativeProducer joins at most one existing checkpoint before any
// consumer lock, capture or private preparation. Semantic sources can exhaust
// independently of bytes. This maintenance is not an admission promise: old
// readers or concurrent producers can retain the original finite capacity.
func (db *DB) PrepareNativeProducer() error {
	if db == nil || db.cow == nil {
		return ErrCOWUnsupported
	}
	c := db.cow
	limits := c.budget.Limits()
	observed, err := c.nativeProducerPressureState()
	if err != nil {
		return err
	}
	if !observed.underPressure(limits) {
		return nil
	}
	// A cleanup/Finish callback cannot join its own original worker frontier.
	if err := db.ownedCloseGuard.CheckClose(); err != nil {
		return err
	}
	// Concurrent producers join one drain and recheck after it, before acquiring
	// any opening, allocation receipt, writer gate or append authority. Never
	// retry an application callback or a partially prepared transaction here.
	c.nativePressureMu.Lock()
	defer c.nativePressureMu.Unlock()
	before, err := c.nativeProducerPressureState()
	if err != nil {
		return err
	}
	if !before.underPressure(limits) {
		return nil
	}
	if c.nativePressureRemembered && c.nativePressureState == before {
		return nil
	}
	c.nativePressureRemembered = false
	if err := db.Checkpoint(); err != nil {
		return err
	}
	// Physical handoff queues old semantic edges. Join their actual refunds on
	// the original worker; NoWAL can otherwise return before source slots retire.
	if err := db.joinSemanticCleanupFrontier(); err != nil {
		return err
	}
	after, err := c.nativeProducerPressureState()
	if err != nil {
		return err
	}
	// A concurrently published suffix may not have been covered by this drain.
	// Its new debt must remain eligible for a later producer's checkpoint.
	if after.logicalSeq == before.logicalSeq {
		c.nativePressureState = after
		c.nativePressureRemembered = true
	}
	return nil
}
func (tx *nativeConditionalTxn) State() backenddb.NativeSemanticState {
	if tx == nil || tx.ConditionalTxn == nil || tx.closed || tx.snap == nil {
		return nil
	}
	return tx.snap.NativeSemanticState()
}
func (tx *nativeConditionalTxn) Snapshot() *backenddb.Snapshot {
	if tx == nil || tx.ConditionalTxn == nil || tx.closed || tx.snap == nil || tx.snap.cowCut == nil {
		return nil
	}
	return tx.snap.cowCut.basis.snapshot
}
func (tx *nativeConditionalTxn) LogicalSequence() uint64 {
	if tx == nil || tx.ConditionalTxn == nil || tx.closed {
		return 0
	}
	return tx.startSeq
}
func (tx *nativeConditionalTxn) AdmitNativeBytes(bytes uint64) (backenddb.NativeAllocationReceipt, error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return nil, backenddb.ErrConditionalTxnClosed
	}
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	return tx.db.cow.budget.AdmitNativeBytes(bytes)
}
func (tx *nativeConditionalTxn) NativeAllocator() backenddb.NativeSemanticAllocator {
	if tx == nil || tx.ConditionalTxn == nil || tx.closed || tx.db == nil {
		return nil
	}
	return tx.db.cow.budget
}
func (tx *nativeConditionalTxn) NativeMaintenanceAllocator() backenddb.NativeSemanticAllocator {
	if tx == nil || tx.ConditionalTxn == nil || tx.closed || tx.db == nil {
		return nil
	}
	return tx.db.cow.maintenance
}
func (tx *nativeConditionalTxn) CommitNative(native *backenddb.NativePreparedPublication, intent *backenddb.CommandWALIntent, sync bool) (result backenddb.NativePublicationResult, err error) {
	if tx == nil || tx.ConditionalTxn == nil {
		return result, backenddb.ErrConditionalTxnClosed
	}
	if err = tx.ensureOpen(); err != nil {
		return result, err
	}
	defer func() {
		tx.db.observeConditionalTxnCommit(len(tx.reads), err)
		// Queue private native edges/custody before withdrawing the ORIGINAL
		// transaction opening, even when it differs from the attach-time cut.
		tx.batch.releaseNativePublication()
		err = joinCOWCleanupError(err, tx.Close())
	}()
	// The consumer cannot replace the original opening sequence/cut. Reprepare
	// changes the physical preparation basis only AFTER original dependencies pass.
	if native == nil || native.StartSequence != tx.startSeq || native.Basis != tx.State() {
		return result, ErrCOWUnsupported
	}
	return tx.db.writeNativePublication(tx.batch, native, intent, sync)
}

// Close detaches the original facade/core aliases before the independent
// containing-allocation receipt is refunded. Repeated observation is harmless.
func (tx *nativeConditionalTxn) Close() error {
	if tx == nil || tx.ConditionalTxn == nil {
		return nil
	}
	core, lease := tx.ConditionalTxn, tx.lease
	tx.ConditionalTxn, tx.lease = nil, nil
	// Enqueue native private edges before the original opening cut ends.
	if core.batch != nil {
		core.batch.releaseNativePublication()
	}
	err := core.Close()
	core = nil
	lease.Close()
	return err
}

func (tx *nativeConditionalTxn) NativeRawEntries() []batch.Entry {
	if tx == nil || tx.ConditionalTxn == nil || tx.closed {
		return nil
	}
	return tx.batch.EntriesForTxn()
}
