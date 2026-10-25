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
	if db == nil || db.cow == nil {
		return nil, ErrCOWUnsupported
	}
	if err := db.checkpointNativeProducerPressure(); err != nil {
		return nil, err
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

// Semantic-only publications retain allocation reservations without adding raw
// memtable bytes, so the raw generation preflight cannot schedule their drain.
// Leave a quarter of the finite reservation budget for the next producer's
// preparation. This is a maintenance watermark, not an admission promise: the
// allocator still refuses oversized work or reservations held by old readers.
func (db *DB) checkpointNativeProducerPressure() error {
	c := db.cow
	limit := c.budget.Limits().MaxInFlightBytes
	watermark := limit - limit/4
	reserved := c.budget.Stats().ReservedBytes
	// Give the already configured checkpoint worker room to drain before a
	// producer must join IO. Only profiles with post-frontier admission can
	// overlap this work with writes; honor disabled automatic maintenance.
	if reserved >= watermark-limit/8 && db.externalCommandWAL &&
		db.loadCommandWALCheckpointCutoverHook() != nil {
		db.TriggerAutoCheckpoint()
	}
	if reserved < watermark {
		return nil
	}
	// Concurrent producers join one drain and recheck after it, before acquiring
	// any opening, allocation receipt, writer gate or append authority. Never
	// retry an application callback or a partially prepared transaction here.
	c.nativePressureMu.Lock()
	defer c.nativePressureMu.Unlock()
	if c.budget.Stats().ReservedBytes < watermark {
		return nil
	}
	return db.Checkpoint()
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
