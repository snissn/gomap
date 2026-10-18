package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

// ActivateNativeSemanticOwner selects this SAME existing cache's semantic
// authority, without changing ordinary raw-only Open/Bind behavior. A retained
// opening/private cut or pending logical work requires the caller to retry at a
// quiescent basis; this bounded entrypoint performs no migration or drain.
func (db *DB) ActivateNativeSemanticOwner() error {
	if db == nil || db.cow == nil {
		return backenddb.ErrNativeSemanticUnsupported
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
