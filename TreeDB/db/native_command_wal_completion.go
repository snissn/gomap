package db

import "github.com/snissn/gomap/TreeDB/internal/transactionowner"

// NativeCommandWALCompletion is transient request ownership through the ordinary
// public group's final ACK. It owns neither a command nor an engine version.
type NativeCommandWALCompletion interface {
	RegisterNativeCommandWAL(func(bool) (uint64, error)) error
	CompleteNativeCommandWAL(error) (NativeCommandWALCompletionResult, error)
	ReleaseNativeCommandWAL()
}

// NativeCommandWALCompletionResult describes the existing registered durable
// wait, rather than a logical-install or WALStable inference. A wait may share
// physical work with other requests; it is not a syscall count.
type NativeCommandWALCompletionResult struct {
	WaitAttempted, PhysicalCompleted bool
	WaitNanos                        uint64
}

// NativeJournalCompletion is an operation-local scalar observation. It retains
// only the original owner's identity, never a database, lease, command or cut.
// Zero means unselected and requires the caller's usual durability boundary.
type NativeJournalCompletion struct {
	owner                         *transactionowner.Owner
	assignedLSN                   uint64
	waitNanos                     uint64
	selected, attempted, physical bool
}

func (r NativeJournalCompletion) Selected() bool      { return r.selected }
func (r NativeJournalCompletion) Attempted() bool     { return r.attempted }
func (r NativeJournalCompletion) Completed() bool     { return r.physical && r.assignedLSN != 0 }
func (r NativeJournalCompletion) AssignedLSN() uint64 { return r.assignedLSN }
func (r NativeJournalCompletion) WaitNanos() uint64   { return r.waitNanos }

// WithoutSuccess preserves selected failure/attempt observations when checked
// cleanup fails after the original physical publication completed.
func (r NativeJournalCompletion) WithoutSuccess() NativeJournalCompletion {
	r.physical = false
	return r
}

// PrepareNativeJournalCompletion authenticates the immutable ORIGINAL binding
// during actual publication preparation. This reuses the original owner query,
// including its nonblocking lifecycle admission, before any raw/publish lock.
// That query alone certifies no completion: assignment/install plus the actual
// registered durable wait below supply the operation-specific physical proof.
// Unknown bridge implementations cannot issue an operation receipt.
func (db *DB) PrepareNativeJournalCompletion(owner *transactionowner.Owner) NativeJournalCompletion {
	if db == nil || owner == nil || db.privateCOWOwner != owner || db.closing.Load() ||
		db.publicationPoisoned.Load() || db.commandWALFlushPoisoned.Load() ||
		!db.nativeSemanticActive.Load() || !db.CommandWALEnabled() || !db.HasNativeSemanticBridge() {
		return NativeJournalCompletion{}
	}
	bridge, ok := db.nativeCommandWALCompletion.(interface {
		NativeCommandWALPrefixOwnerOpen(*DB, *transactionowner.Owner) bool
	})
	if !ok || !bridge.NativeCommandWALPrefixOwnerOpen(db, owner) {
		return NativeJournalCompletion{}
	}
	return NativeJournalCompletion{owner: owner, selected: true}
}

// CompleteNativeJournalCompletion combines the authenticated original binding
// with this publication's assignment/install and its actual durable wait.
func (db *DB) CompleteNativeJournalCompletion(r NativeJournalCompletion, publication NativePublicationResult, completion NativeCommandWALCompletionResult) NativeJournalCompletion {
	if db == nil || !r.selected || r.owner != db.privateCOWOwner {
		return NativeJournalCompletion{}
	}
	r.assignedLSN = publication.AssignedLSN
	r.attempted, r.waitNanos = completion.WaitAttempted, completion.WaitNanos
	r.physical = completion.PhysicalCompleted && publication.Assigned && publication.LogicalInstalled && !publication.Uncertain
	return r
}

func (db *DB) OwnsNativeJournalCompletion(r NativeJournalCompletion) bool {
	return db != nil && r.selected && r.owner != nil && r.owner == db.privateCOWOwner
}

// NativeCommandWALCompletionBridge is bound once by public Open. Direct backend
// constructors have no bridge and retain their original inline sync behavior.
type NativeCommandWALCompletionBridge interface {
	NativeCommandWALCompletionBytes() uint64
	PrepareNativeCommandWAL(int, bool) (NativeCommandWALCompletion, error)
	SyncNativeCommandWALPrefix() error
}

func (db *DB) BindNativeCommandWALCompletion(owner *transactionowner.Owner, bridge NativeCommandWALCompletionBridge) error {
	if db == nil || owner == nil || db.privateCOWOwner != owner || bridge == nil || db.nativeCommandWALCompletion != nil {
		return ErrNativeSemanticUnsupported
	}
	db.nativeCommandWALCompletion = bridge
	return nil
}

// CanShareNativeCommandWALPrefix is a side-effect-free scheduling snapshot of
// the existing original public completion binding. Unknown implementations and
// uncertain lifecycle admission stay exclusive. Actual mutation/fence admission
// remains authoritative after this query returns; no backend lock spans the
// public lifecycle callback, and no activation or sync is used as a probe.
func (db *DB) CanShareNativeCommandWALPrefix() bool {
	if db == nil || db.closing.Load() || db.publicationPoisoned.Load() || db.commandWALFlushPoisoned.Load() ||
		db.privateCOWOwner == nil || !db.nativeSemanticActive.Load() || !db.HasNativeSemanticBridge() || !db.CommandWALEnabled() {
		return false
	}
	bridge, ok := db.nativeCommandWALCompletion.(interface {
		NativeCommandWALPrefixOwnerOpen(*DB, *transactionowner.Owner) bool
	})
	return ok && bridge.NativeCommandWALPrefixOwnerOpen(db, db.privateCOWOwner)
}

// CheckNativeCommandWALPrefixOwner admits only the already-active original
// private COW owner. It neither activates a raw owner nor retains a backend
// lock across public lifecycle acquisition. The public callback supplies the
// retained lifetime; activation's existing open/poison gates also cover NoWAL.
func (db *DB) CheckNativeCommandWALPrefixOwner(owner *transactionowner.Owner) error {
	if db == nil || db.closing.Load() {
		return ErrClosed
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	if owner == nil || owner != db.privateCOWOwner || !db.nativeSemanticActive.Load() || !db.HasNativeSemanticBridge() {
		return ErrNativeSemanticUnsupported
	}
	return db.ActivateNativeSemanticOwner()
}

// SyncNativeCommandWALPrefix dispatches through the once-bound original public
// completion owner. A healthy capability miss permits a physical checkpoint;
// after callback selection every error is final, without retry or fallback.
func (db *DB) SyncNativeCommandWALPrefix() (supported bool, err error) {
	if db == nil {
		return false, ErrClosed
	}
	if err := db.CheckNativeCommandWALPrefixOwner(db.privateCOWOwner); err != nil {
		return false, err
	}
	if !db.CommandWALEnabled() || db.nativeCommandWALCompletion == nil {
		return false, nil
	}
	return true, db.nativeCommandWALCompletion.SyncNativeCommandWALPrefix()
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
