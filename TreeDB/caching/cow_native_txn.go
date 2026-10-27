package caching

import (
	"context"
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

func (s cowNativePressureState) observation(limits memtable.COWLimits) backenddb.NativeProducerObservation {
	return backenddb.NativeProducerObservation{
		LogicalSequence: s.logicalSeq, Basis: s.basis, Semantic: s.semantic,
		ReservedBytes: s.stats.ReservedBytes, Sources: s.stats.Sources,
		Generations: s.stats.Generations, UnderPressure: s.underPressure(limits),
	}
}

// ObserveNativeProducer releases its borrowed cut before returning this scalar
// identity. In particular it does not wait for nativePressureMu, checkpoint or
// semantic cleanup while its caller holds a consumer mutation gate.
func (db *DB) ObserveNativeProducer() (backenddb.NativeProducerObservation, error) {
	if db == nil || db.cow == nil {
		return backenddb.NativeProducerObservation{}, ErrCOWUnsupported
	}
	state, err := db.cow.nativeProducerPressureState()
	if err != nil {
		return backenddb.NativeProducerObservation{}, err
	}
	return state.observation(db.cow.budget.Limits()), nil
}

// PrepareNativeProducer joins at most one original captured-prefix drain (or
// full checkpoint) before consumer locks, captures or private preparation. It
// reserves no future capacity.
func (db *DB) PrepareNativeProducer() error {
	_, err := db.PrepareNativeProducerObservation()
	return err
}

// PrepareNativeProducerObservation preserves the original preparation algorithm
// and returns the observation at its successful exit, after any original drain.
func (db *DB) PrepareNativeProducerObservation() (backenddb.NativeProducerObservation, error) {
	if db == nil || db.cow == nil {
		return backenddb.NativeProducerObservation{}, ErrCOWUnsupported
	}
	c := db.cow
	limits := c.budget.Limits()
	observed, err := c.nativeProducerPressureState()
	if err != nil {
		return backenddb.NativeProducerObservation{}, err
	}
	if !observed.underPressure(limits) {
		return observed.observation(limits), nil
	}
	// A cleanup/Finish callback cannot join its own original worker frontier.
	if err := db.ownedCloseGuard.CheckClose(); err != nil {
		return backenddb.NativeProducerObservation{}, err
	}
	// No consumer gate or opening is held while waiting for the original drain.
	c.nativePressureMu.Lock()
	defer c.nativePressureMu.Unlock()
	before, err := c.nativeProducerPressureState()
	if err != nil {
		return backenddb.NativeProducerObservation{}, err
	}
	if !before.underPressure(limits) {
		return before.observation(limits), nil
	}
	if c.nativePressureRemembered && c.nativePressureState == before {
		return before.observation(limits), nil
	}
	c.nativePressureRemembered = false
	if db.nativePressureDrainEnabled() {
		err = db.materializeCOWPrefix(context.Background(), false, nil, true)
	} else {
		err = db.Checkpoint()
	}
	if err != nil {
		return backenddb.NativeProducerObservation{}, err
	}
	// Join the actual refunds on the original worker, before consumer locks.
	if err := db.joinSemanticCleanupFrontier(); err != nil {
		return backenddb.NativeProducerObservation{}, err
	}
	after, err := c.nativeProducerPressureState()
	if err != nil {
		return backenddb.NativeProducerObservation{}, err
	}
	// A concurrently published suffix is not remembered as covered by this drain.
	if after.logicalSeq == before.logicalSeq {
		c.nativePressureState = after
		c.nativePressureRemembered = true
	}
	return after.observation(limits), nil
}

// nativePressureDrainEnabled requires a real running size-maintenance worker.
// Its original captured threshold, external total-byte hook and deferred cleanup
// authority must all exist. Merely reconfiguring the notification atomic cannot
// turn an interval-only worker into size maintenance. This is a soft asynchronous
// cleanup capability, not a hard bound on journal bytes.
func (db *DB) nativePressureDrainEnabled() bool {
	return db != nil && db.externalCommandWAL && db.autoCheckpointOn.Load() &&
		db.autoCheckpointWorkerMaxWALBytes.Load() > 0 &&
		db.loadAutoCheckpointBytesHook() != nil &&
		db.loadCommandWALCheckpointCutoverHook() != nil &&
		db.defersAutomaticCommandWALCleanup()
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
