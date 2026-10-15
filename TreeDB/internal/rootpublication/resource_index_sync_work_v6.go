package rootpublication

import (
	"os"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// This is an optional finite operation on the SAME original owned callback
// backing, not a second file/pager owner. Ordinary callbacks need not implement
// it. Native callers refuse a foreign/synchronous-only operation before I/O.
type StableResourceOwnedSyncWithWorkV6 interface {
	SyncThroughWithWorkV6(*os.File, DurableFrontier, *iterator.OrdinalScanWork) (bool, error)
}

// SyncThroughWithWorkV6 preserves the original token's frontier, original
// operations and metrics. Exact-RID traversal and imported/opaque callbacks
// require their own finite engines and are refused before a physical effect.
func (token *StableResourceToken) SyncThroughWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if token == nil || w == nil {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(2, uint64(unsafe.Sizeof(*token))+2*uint64(unsafe.Sizeof(DurableFrontier{}))+2*uint64(unsafe.Sizeof(exactRIDMembership{}))) {
		return false, nil
	}
	if token.released.Load() || token.pinned == nil || token.metadata == nil {
		return false, ErrResourceOwnership
	}
	operations, finite := token.ownedOperations.(StableResourceOwnedSyncWithWorkV6)
	if !finite || token.importedSource != nil || token.frontier.exactRIDs != nil && len(token.frontier.exactRIDs.values) != 0 || token.syncedFrontier.exactRIDs != nil && len(token.syncedFrontier.exactRIDs.values) != 0 {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	// Start/end clock observations and the two original aggregate metric
	// updates are admitted as their actual four operations before the first.
	if !w.Reserve(4, 2*uint64(unsafe.Sizeof(time.Time{}))+2*uint64(unsafe.Sizeof(uint64(0)))) {
		return false, nil
	}
	started := time.Now()
	var err error
	if !token.hasSyncedFrontier || !durableFrontierCovers(token.syncedFrontier, token.frontier) {
		// Physical start/end clocks, two physical metric updates and original
		// typed callback dispatch; the callback admits its own real pager work.
		if !w.Reserve(5, 2*uint64(unsafe.Sizeof(time.Time{}))+2*uint64(unsafe.Sizeof(uint64(0)))+uint64(unsafe.Sizeof(operations))+uint64(unsafe.Sizeof(token.pinned))+uint64(unsafe.Sizeof(token.frontier))) {
			return false, nil
		}
		physicalStarted := time.Now()
		var ready bool
		ready, err = operations.SyncThroughWithWorkV6(token.pinned, token.frontier, w)
		if !ready {
			return false, err
		}
		if err == nil {
			token.metrics.physicalFileSyncs.Add(1)
			token.metrics.physicalFileSyncNanos.Add(uint64(time.Since(physicalStarted)))
		}
	}
	token.metrics.syncs.Add(1)
	token.metrics.syncNanos.Add(uint64(time.Since(started)))
	return true, err
}

// StabilizeWithWorkV6 performs the original already-stable namespace branch
// under its original mutex. It cannot run unpriced identity/sync callbacks and
// is deliberately different from validateStable, which may revalidate a link.
func (token *StableNamespaceToken) StabilizeWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if token == nil || w == nil {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(2, uint64(unsafe.Sizeof(*token))+16) {
		return false, nil
	}
	token.mu.Lock()
	defer token.mu.Unlock()
	if token.released.Load() {
		return false, ErrResourceOwnership
	}
	switch token.state.Load() {
	case namespaceStable:
		return true, nil
	case namespaceFailed:
		return false, token.stabilizeErr
	default:
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
}

func (token *StableResourceToken) StabilizeNamespaceWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if token == nil || w == nil {
		return false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(1, uint64(unsafe.Sizeof(*token))) {
		return false, nil
	}
	if token.released.Load() || token.namespace == nil {
		return false, ErrResourceOwnership
	}
	return token.namespace.StabilizeWithWorkV6(w)
}
