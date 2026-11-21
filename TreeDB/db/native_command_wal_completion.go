package db

import (
	"time"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// NativeCommandWALCompletion is transient request ownership through the ordinary
// public group's final ACK. It owns neither a command nor an engine version.
type NativeCommandWALCompletion interface {
	RegisterNativeCommandWAL(func(bool) (uint64, error)) error
	CompleteNativeCommandWAL(error) error
	ReleaseNativeCommandWAL()
}

// NativeCommandWALCompletionBridge is bound once by public Open. Direct backend
// constructors have no bridge and retain their original inline sync behavior.
type NativeCommandWALCompletionBridge interface {
	NativeCommandWALCompletionBytes() uint64
	PrepareNativeCommandWAL(int, bool) (NativeCommandWALCompletion, error)
}

func (db *DB) BindNativeCommandWALCompletion(owner *transactionowner.Owner, bridge NativeCommandWALCompletionBridge) error {
	if db == nil || owner == nil || db.privateCOWOwner != owner || bridge == nil || db.nativeCommandWALCompletion != nil {
		return ErrNativeSemanticUnsupported
	}
	db.nativeCommandWALCompletion = bridge
	return nil
}

// NativeCommandWALPrefixEligible checks the original active native owner and
// the open journal's actual container. Relaxed owners keep their root checkpoint
// boundary, including stores that retain a fixed segment from an earlier policy.
func (db *DB) NativeCommandWALPrefixEligible(owner *transactionowner.Owner) (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if err := db.checkNativeJournalOwner(); err != nil {
		return false, err
	}
	if owner == nil || owner != db.privateCOWOwner {
		return false, ErrNativeSemanticUnsupported
	}
	fixed, err := db.nativeJournalFixedWriter()
	return fixed && db.resolvedProfile == ProfileCommandWALDurable, err
}

// SyncNativeCommandWALPrefix delegates admission and physical completion to the
// original public owner. A healthy capability miss permits a root checkpoint.
func (db *DB) SyncNativeCommandWALPrefix() (selected bool, err error) {
	if db == nil {
		return false, ErrClosed
	}
	syncer, ok := db.nativeCommandWALCompletion.(interface {
		SyncNativeCommandWALPrefix() (bool, error)
	})
	if db.resolvedProfile == ProfileCommandWALDurable && ok {
		// The public callback admits under its own lifecycle before it consults
		// this backend. Taking teardown here would reverse that lock order.
		return syncer.SyncNativeCommandWALPrefix()
	}
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if err := db.checkNativeJournalOwner(); err != nil {
		return false, err
	}
	_, err = db.nativeJournalFixedWriter()
	return false, err
}

// nativeJournalFixedWriter checks the active writer under teardownMu.
func (db *DB) nativeJournalFixedWriter() (bool, error) {
	if !db.commandWAL {
		return false, nil
	}
	if db.commandJournal == nil {
		return false, db.commandWALJournalUnavailableError()
	}
	return db.commandJournal.HasFixedWriter()
}

// checkNativeJournalOwner checks an already active cache owner under teardownMu.
func (db *DB) checkNativeJournalOwner() error {
	if db.closing.Load() {
		return ErrClosed
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	if !db.nativeSemanticActive.Load() {
		return ErrNativeSemanticUnsupported
	}
	return db.ActivateNativeSemanticOwner()
}

// NativeCommandWALCompletionFor authenticates the original unassigned intent.
// Already-combined publications keep their existing synchronous ordinal path.
func (db *DB) NativeCommandWALCompletionFor(owner *transactionowner.Owner, intent *CommandWALIntent, requestedSync bool) (NativeCommandWALCompletionBridge, bool, error) {
	if err := db.ValidateNativeCommandIntent(owner, intent); err != nil {
		return nil, false, err
	}
	durable := commandWALIntentPublishSync(intent, requestedSync)
	if len(intent.inner.durablePrefixGroup) != 0 || intent.inner.fromReplay || !durable {
		return nil, durable, nil
	}
	return db.nativeCommandWALCompletion, durable, nil
}

// IsNewNativeBSONUpdate identifies the original singleton append class. The
// producer separately proves the prepared collection and roots are covered.
func (intent *CommandWALIntent) IsNewNativeBSONUpdate() bool {
	return intent != nil && !intent.inner.fromReplay && intent.AssignedLSN() == 0 && intent.inner.trustedPayload &&
		len(intent.inner.durablePrefixGroup) == 0 && !intent.RequestsExactSeal() &&
		intent.inner.kind == commitlog.CommandKindCollectionUpdateBatchByID &&
		intent.inner.scope == commitlog.CommandScopeCollection &&
		intent.inner.payloadFormat == commitlog.PayloadFormatCollectionUpdateBatchByIDV1
}

// SyncPrivateCOWGenerationWAL gives the relaxed generation worker the same
// physical WAL flush as FlushCommandWAL(true). Capture flushes and pins the
// exact active segment before unlocking the journal. Its sync does not cover
// retired segments, close dependency debt, append a barrier, or advance W.
func (db *DB) SyncPrivateCOWGenerationWAL(owner *transactionowner.Owner) error {
	if db == nil || owner == nil || owner != db.privateCOWOwner || !db.commandWAL || db.durability != DurabilityWALOnRelaxed {
		return ErrNativeSemanticUnsupported
	}
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if db.closing.Load() {
		return ErrClosed
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	if db.commandJournal == nil {
		return db.commandWALJournalUnavailableError()
	}
	started := time.Now()
	target, err := db.commandJournal.CaptureSync(nil)
	if err == nil {
		defer target.Release()
		err = target.Sync()
	}
	if err == nil && db.testFailCommandWALFlush.Load() {
		err = errTestCommandWALFlushFailpoint
	}
	if err != nil {
		db.commandWALFlushPoisoned.Store(true)
	}
	db.observeCommandWALFlush(commandWALAppendStatsBarrier, true, time.Since(started))
	if err != nil {
		return err
	}
	return db.commandWALPoisonedError()
}

// FlushPrivateCOWCommandWALBarrier closes the exact dependency-complete prefix
// after all original installers have released raw. No legacy drain can run.
func (db *DB) FlushPrivateCOWCommandWALBarrier(owner *transactionowner.Owner) (CommandWALBarrierResult, error) {
	if db == nil || owner == nil || owner != db.privateCOWOwner {
		return CommandWALBarrierResult{}, ErrNativeSemanticUnsupported
	}
	db.teardownMu.RLock()
	if db.closing.Load() {
		db.teardownMu.RUnlock()
		return CommandWALBarrierResult{}, ErrClosed
	}
	db.commandWALRawPublishMu.Lock()
	defer db.unlockCommandWALRawPublishWithTeardown()
	if db.closing.Load() {
		return CommandWALBarrierResult{}, ErrClosed
	}
	return db.flushCommandWALBarrierLocked(true, owner)
}

// AppendNativeDurablePrefixParticipant uses the SAME authenticated staging
// ownership but defers durability to the public group's owned prefix barrier.
// capturedDurable selects the singleton frame class, never physical sync.
// Profile-forced durability remains the caller's completion obligation.
func (guard *CommandWALStagingGuardV1) AppendNativeDurablePrefixParticipant(intent *CommandWALIntent, capturedDurable bool) (CommandWALLSNRange, error) {
	if guard == nil || guard.ownership == nil {
		return CommandWALLSNRange{}, ErrClosed
	}
	held := guard.ownership
	held.mu.Lock()
	defer held.mu.Unlock()
	if !held.active || held.db == nil || held.privateOwner == nil || held.privateOwner != held.db.privateCOWOwner || held.intent != nil || intent == nil || intent.AssignedLSN() != 0 || intent.staged() || intent.inner.fromReplay || len(intent.inner.durablePrefixGroup) != 0 {
		return CommandWALLSNRange{}, ErrCommandWALRejected
	}
	if err := held.db.ValidateNativeCommandIntent(held.privateOwner, intent); err != nil {
		return CommandWALLSNRange{}, err
	}
	intent.inner.privateCOWOwner = held.privateOwner
	intent.inner.capturedDurable = capturedDurable
	lsn, err := held.db.appendCommandWALIntent(&intent.inner, false)
	intent.inner.privateCOWOwner = nil
	// Inner append may have assigned before a flush/dependency failure.
	if assigned := intent.AssignedLSN(); assigned != 0 {
		lsn = assigned
	}
	if lsn != 0 {
		held.intent = intent
	}
	if err == nil && lsn != 0 {
		intent.inner.staged = true
	}
	return CommandWALLSNRange{First: lsn, Last: lsn}, err
}
