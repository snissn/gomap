package db

import "github.com/snissn/gomap/TreeDB/internal/transactionowner"

// BindPrivateCOWOwner retires the backend compatibility history before a public
// wrapper exposes its privately owned cache. Logical mutations thereafter belong
// exclusively to that cache's complete cut and FIFO; disk work is physical.
func (db *DB) BindPrivateCOWOwner(owner *transactionowner.Owner) error {
	if db == nil || owner == nil {
		return ErrConditionalTxnUnsupported
	}
	db.commandWALRawBarrierMu.Lock()
	defer db.commandWALRawBarrierMu.Unlock()
	if len(db.commandWALRawBarriers) != 0 {
		return ErrConditionalTxnUnsupported
	}
	db.conditionalOracle.mu.Lock()
	defer db.conditionalOracle.mu.Unlock()
	if db.privateCOWOwner != nil || db.conditionalActiveTxnCount.Load() != 0 {
		return ErrConditionalTxnUnsupported
	}
	if db.conditionalOracle.history != nil {
		db.conditionalOracle.history.Close()
		db.conditionalOracle.history = nil
	}
	db.privateCOWOwner = owner
	return nil
}

// PreflightPrivateCOWCommand qualifies the raw-only owner before cache admission.
// A registered higher-level producer invalidates this initial eligibility fence;
// it must migrate to the common transaction engine before it can be enabled.
// Final append repeats this readiness check under its actual raw staging guard.
func (db *DB) PreflightPrivateCOWCommand(owner *transactionowner.Owner) error {
	if db == nil || owner == nil || db.privateCOWOwner != owner {
		return ErrConditionalTxnUnsupported
	}
	if db.closing.Load() {
		return ErrClosed
	}
	if err := db.commandWALPoisonedError(); err != nil {
		return err
	}
	db.commandWALRawBarrierMu.Lock()
	ready := len(db.commandWALRawBarriers) == 0
	db.commandWALRawBarrierMu.Unlock()
	if !ready {
		return ErrConditionalTxnUnsupported
	}
	return nil
}
