package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

// JoinNativeSemanticCleanupForTest exposes only the original worker frontier to
// tests observing cancellation refunds after a publication has returned. It does
// not add a worker, force maintenance, or release the test's retained cuts.
func (db *DB) JoinNativeSemanticCleanupForTest() error {
	if db == nil {
		return backenddb.ErrClosed
	}
	if db.cow == nil {
		return ErrCOWUnsupported
	}
	// A Finish/cleanup callback must never wait for its own original worker.
	if err := db.ownedCloseGuard.CheckClose(); err != nil {
		return err
	}
	return db.joinSemanticCleanupFrontier()
}
