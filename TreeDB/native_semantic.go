package treedb

import (
	"github.com/snissn/gomap/TreeDB/caching"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// CaptureNativeSemanticState retains one complete raw/semantic/disk cut for
// internal native collection preparation. Close the returned original owner.
// This seam requires the privately owned COW engine used by public MVCC.
func (db *DB) CaptureNativeSemanticState() (*caching.NativeSemanticCapture, error) {
	cached, _, err := db.captureReadOwners()
	if err != nil {
		return nil, err
	}
	if cached == nil || !cached.COWMode() {
		return nil, caching.ErrCOWUnsupported
	}
	return cached.CaptureNativeSemanticState(db.txnOwner)
}

// SetNativeSemanticPublication attaches admitted native effects to a batch
// created by this same public DB. The caller retains the captured basis until
// Write returns; batch cancellation consumes transferred private ownership.
func (db *DB) SetNativeSemanticPublication(b Batch, native *backenddb.NativePreparedPublication) error {
	cached, _, err := db.captureReadOwners()
	if err != nil {
		return err
	}
	if cached == nil || !cached.COWMode() {
		return caching.ErrCOWUnsupported
	}
	if wrapper, ok := b.(*commandWALPublicBatch); ok {
		if wrapper.db != db {
			return caching.ErrCOWUnsupported
		}
		b = wrapper.inner
	}
	inner, ok := b.(*caching.Batch)
	if !ok {
		return caching.ErrCOWUnsupported
	}
	return inner.SetNativeSemanticPublication(db.txnOwner, native)
}
