package db

import "github.com/snissn/gomap/TreeDB/internal/transactionowner"

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
// Profile-forced durability remains the caller's completion obligation.
func (guard *CommandWALStagingGuardV1) AppendNativeDurablePrefixParticipant(intent *CommandWALIntent) (CommandWALLSNRange, error) {
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
