package db

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

// ActivateNativeSemanticOwner explicitly selects the complete-cut owner. Merely
// binding a raw COW cache does not change legacy producer eligibility.
func (db *DB) ActivateNativeSemanticOwner() error {
	if db == nil || db.nativeSemanticBridge == nil {
		return ErrNativeSemanticUnsupported
	}
	if db.closing.Load() {
		return ErrClosed
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	bridge, ok := db.nativeSemanticBridge.(interface {
		ActivateNativeSemanticOwner() error
		CheckNativeSemanticOwnerOpen() error
	})
	if !ok {
		return ErrNativeSemanticUnsupported
	}
	// Cache teardown starts before backend teardown. Check the SAME fixed-once
	// bridge's existing close gate even when activation is already complete.
	if err := bridge.CheckNativeSemanticOwnerOpen(); err != nil {
		return err
	}
	if db.nativeSemanticActive.Load() {
		return nil
	}
	return bridge.ActivateNativeSemanticOwner()
}

// AdmitLegacyCommandProducer atomically admits a live legacy constructor before
// it installs any hook or exposes a handle, including constructors without hooks.
// The once-only release belongs to the manager's entire live lifetime.
func (db *DB) AdmitLegacyCommandProducer() (func(), error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.commandWALRawBarrierMu.Lock()
	defer db.commandWALRawBarrierMu.Unlock()
	if db.closing.Load() {
		return nil, ErrClosed
	}
	if db.nativeSemanticActive.Load() {
		return nil, ErrNativeSemanticUnsupported
	}
	db.legacyCommandProducers++
	var once sync.Once
	return func() {
		once.Do(func() {
			db.commandWALRawBarrierMu.Lock()
			db.legacyCommandProducers--
			db.commandWALRawBarrierMu.Unlock()
		})
	}, nil
}

// ActivatePrivateCOWSemanticOwner is called with the SAME cache writer owner
// held and an otherwise quiescent complete cut. It never drains or waits for
// staging/build/Close progress. Lock order: cache writer -> teardown (try) ->
// raw publisher (try) -> barrier registry -> existing accepted-prefix latch.
func (db *DB) ActivatePrivateCOWSemanticOwner(owner *transactionowner.Owner, basis StateToken) error {
	if db == nil || owner == nil || owner != db.privateCOWOwner {
		return ErrNativeSemanticUnsupported
	}
	if !db.teardownMu.TryLock() {
		return ErrNativeSemanticUnsupported
	}
	defer db.teardownMu.Unlock()
	if !db.commandWALRawPublishMu.TryLock() {
		return ErrNativeSemanticUnsupported
	}
	defer db.commandWALRawPublishMu.Unlock()
	db.commandWALRawBarrierMu.Lock()
	defer db.commandWALRawBarrierMu.Unlock()
	if db.closing.Load() {
		return ErrClosed
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	if db.nativeSemanticActive.Load() {
		return nil
	}
	state, ok := stateTokenFromState(db.state.Load())
	if !ok || state != basis || db.legacyCommandProducers != 0 || len(db.commandWALRawBarriers) != 0 || db.semanticUnownedInFlight != 0 || db.commandWALReplayLSN.Load() != 0 || db.conditionalActiveTxnCount.Load() != 0 {
		return ErrNativeSemanticUnsupported
	}
	prefix := db.acceptedPrefix.Load()
	if prefix == nil {
		return ErrNativeSemanticUnsupported
	}
	f := prefix.LogicalFrontiers()
	if f.V != f.S.LogicalSequence || f.C != f.S.CanonicalLSN || f.C != state.AppliedCommandLSN || db.commandWALLiveAcceptedMax.Load() > state.AppliedCommandLSN {
		return ErrNativeSemanticUnsupported
	}
	db.nativeSemanticActive.Store(true)
	return nil
}

// Existing append/finalize entrypoints use the registry latch for their whole
// operation's admission. This also covers aliases that do not hold teardown.
// The counter prevents activation racing a preexisting unowned operation; it
// adds no publication authority. Accepted-cut authenticity is checked again by
// the existing coordinator before physical acceptance.
func (db *DB) beginSemanticBackendOperation(owner *transactionowner.Owner, acceptedCut bool) (bool, error) {
	if db == nil {
		return false, ErrClosed
	}
	// Original owner staging/cache writer and accepted-cut teardown ownership
	// already exclude activation. They need no extra hot-path bookkeeping.
	if acceptedCut || (owner != nil && owner == db.privateCOWOwner) {
		return false, nil
	}
	db.commandWALRawBarrierMu.Lock()
	defer db.commandWALRawBarrierMu.Unlock()
	if db.nativeSemanticActive.Load() && !acceptedCut && (owner == nil || owner != db.privateCOWOwner) {
		return false, ErrNativeSemanticUnsupported
	}
	db.semanticUnownedInFlight++
	return true, nil
}
func (db *DB) endSemanticBackendOperation(tracked bool) {
	if !tracked {
		return
	}
	db.commandWALRawBarrierMu.Lock()
	db.semanticUnownedInFlight--
	db.commandWALRawBarrierMu.Unlock()
}
