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

// NativeSemanticBackend selects this DB's existing complete-cut COW authority
// and returns its backend for restricted native semantic consumers. Activation
// must succeed before any handle escapes; it refuses incompatible or pending
// work and never migrates a backend-only factory. Ordinary raw Open remains
// unactivated until this call. The returned handle borrows this DB's lifetime:
// keep this DB open, and close this DB rather than the backend.
func (db *DB) NativeSemanticBackend() (*backenddb.DB, error) {
	_, backend, err := db.captureReadOwners()
	if err != nil {
		return nil, err
	}
	if backend == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	if err := backend.ActivateNativeSemanticOwner(); err != nil {
		return nil, err
	}
	return backend, nil
}
