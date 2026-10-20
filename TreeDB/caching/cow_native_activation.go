package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

// CheckNativeSemanticOwnerOpen reads the existing cache lifecycle gate. Backend
// activation uses this fixed bridge because cache Close precedes backend Close.
// It grants no activation or publication authority.
func (db *DB) CheckNativeSemanticOwnerOpen() error {
	if db == nil || db.cow == nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	if db.closing.Load() {
		return backenddb.ErrClosed
	}
	return nil
}

// ActivateNativeSemanticOwner selects this SAME existing cache's semantic
// authority, without changing ordinary raw-only Open/Bind behavior. A retained
// opening/private cut or pending logical work requires the caller to retry at a
// quiescent basis; this bounded entrypoint performs no migration or drain.
func (db *DB) ActivateNativeSemanticOwner() error {
	if err := db.CheckNativeSemanticOwnerOpen(); err != nil {
		return err
	}
	c := db.cow
	if !c.writerMu.TryLock() {
		return backenddb.ErrNativeSemanticUnsupported
	}
	defer c.writerMu.Unlock()
	c.cutMu.Lock()
	defer c.cutMu.Unlock()
	cut := c.cut
	if c.closed || cut == nil || cut.refs != 1 || cut.logicalSeq != cut.basis.logicalSeq || len(cut.frozen) != 0 || cut.semanticRootSet != nil {
		return backenddb.ErrNativeSemanticUnsupported
	}
	basis, ok := cut.basis.snapshot.StateToken()
	if !ok {
		return backenddb.ErrClosed
	}
	backend, ok := db.backend.(*backenddb.DB)
	if !ok {
		return backenddb.ErrNativeSemanticUnsupported
	}
	return backend.ActivatePrivateCOWSemanticOwner(c.txnOwner, basis)
}
