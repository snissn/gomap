package collections

import (
	"errors"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// nativeCatalogScope owns one fresh complete cut for synchronous metadata use.
// Its collection/engine aliases must be consumed or copied before Close and are
// scrubbed before their capture/private owner retires. It grants no row-reader
// or mutation authority and cannot outlive or lend its schema to the operation.
type nativeCatalogScope struct {
	capture               backenddb.NativeSemanticCapture
	engine, privateEngine *nativeEngineVersion
	collection            *nativeCollectionState
	lease                 *memtable.COWExternalLease
	failure               error
}

// captureNativeCatalog preserves the shared name, complete-cut and admission
// ordering for both constructors. Each admits its actual containing backing.
func (m *NativeJSONManager) captureNativeCatalog(name string, charge uint64) (backenddb.NativeSemanticCapture, *memtable.COWExternalLease, error) {
	if err := ValidateCollectionName(name); err != nil {
		return nil, nil, err
	}
	if m == nil || m.db == nil {
		return nil, nil, backenddb.ErrClosed
	}
	capture, err := m.db.CaptureNativeSemanticState()
	if err != nil {
		return nil, nil, err
	}
	lease, err := capture.NativeAllocator().AdmitNativeBytes(charge)
	if err != nil {
		return nil, nil, errors.Join(err, capture.Close())
	}
	return capture, lease, nil
}

func (m *NativeJSONManager) captureCatalog(name string) (result *nativeCatalogScope, resultErr error) {
	capture, lease, err := m.captureNativeCatalog(name, memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeCatalogScope{}))))
	if err != nil {
		return nil, err
	}
	scope := &nativeCatalogScope{capture: capture, lease: lease}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, scope.Close())
		}
	}()
	scope.engine, scope.collection, scope.privateEngine, err = resolveNativeCatalog(capture, name)
	if err != nil {
		return nil, err
	}
	if domain, ok := scope.collection.domains.Get("primary"); ok {
		// Preserve the row constructor's refs/snapshot/root validation, including
		// the allowed zero base root, without retaining the large by-value reader.
		if _, err = newNativeDomainRead(domain, capture.Snapshot()); err != nil {
			return nil, err
		}
	}
	return scope, nil
}

func resolveNativeCatalog(capture backenddb.NativeSemanticCapture, name string) (engine *nativeEngineVersion, collection *nativeCollectionState, privateEngine *nativeEngineVersion, err error) {
	base, ok := capture.State().(*nativeEngineVersion)
	if capture.State() != nil && !ok {
		return nil, nil, privateEngine, backenddb.ErrNativeSemanticUnsupported
	}
	if base == nil {
		if projection, ok := capture.(interface {
			NativeCatalogProjection(func(backenddb.NativeSemanticCapture) (backenddb.NativeSemanticState, error)) (backenddb.NativeSemanticState, error)
		}); ok {
			state, projectionErr := projection.NativeCatalogProjection(nativeJSONCatalogProjection)
			base, ok = state.(*nativeEngineVersion)
			if projectionErr != nil {
				// Partial failed construction remains on the original read's
				// cleanup path, outside both projection locks; never shared.
				privateEngine = base
				return nil, nil, privateEngine, projectionErr
			}
			if !ok || base == nil {
				return nil, nil, privateEngine, backenddb.ErrNativeSemanticUnsupported
			}
		} else {
			// Other original capture implementations retain private import
			// ownership. No process/manager cache or alternate capture is used.
			privateEngine, err = importNativeJSONCatalog(capture)
			if err != nil {
				return nil, nil, privateEngine, err
			}
			base = privateEngine
		}
	}

	c, ok := base.names.Get(name)
	if !ok {
		return nil, nil, privateEngine, tree.ErrKeyNotFound
	}

	return base, c, privateEngine, nil
}

// Both callers scrub borrowed aliases before releasing the original owners.
func closeNativeCatalog(capture backenddb.NativeSemanticCapture, privateEngine *nativeEngineVersion) error {
	var err error
	if privateEngine != nil {
		err = finishPrivateNative(privateEngine.Release())
	}
	return errors.Join(err, capture.Close())
}

func (s *nativeCatalogScope) Close() error {
	if s == nil {
		return nil
	}
	if s.capture == nil {
		return s.failure
	}
	capture, privateEngine, lease := s.capture, s.privateEngine, s.lease
	s.capture, s.engine, s.privateEngine, s.collection, s.lease = nil, nil, nil, nil, nil
	s.failure = closeNativeCatalog(capture, privateEngine)
	lease.Close()
	return s.failure
}
