package collections

import (
	"context"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// nativeResourceInventory is the original selected inventory edge. Engines
// share this descriptor, rather than an ancestor engine or backend capture.
// Its enrollment follows the last inventory edge, independently of Snapshot.
// Every partial belongs to this original Work before later fallible work.
type nativeResourceInventory struct {
	refs       atomic.Int64
	resources  *rootpublication.StableResourceSet
	metadata   *retainedalloc.Owner
	enrollment *retainedalloc.Enrollment
	bootstrap  *backenddb.RecoverableRootSet
	failure    error
	cleanup    rootpublication.SemanticCleanup
	refund     *nativeEngineRefund
}

// CloneCapturedNativeResource borrows this inventory's existing live edge. The
// original plan must retain that edge independently before obtaining its
// publication context. Selection uses only this exact immutable inventory;
// neither backend runtime state nor a bound Engine/capture is consulted.
func (i *nativeResourceInventory) CloneCapturedNativeResource(selector rootpublication.StableResourceSelector) (*rootpublication.StableResourceSet, error) {
	if i == nil || i.refs.Load() <= 0 || i.resources == nil || i.failure != nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	return rootpublication.CloneStableResourceForSelector(i.resources, selector)
}

var _ backenddb.NativeResourceSelector = (*nativeResourceInventory)(nil)

// prepareNativeResourceInventory is opening-only, outside all publication and
// cut gates. basis is the exact Snapshot borrowed from the complete opening cut.
// A nonnil partial owns one release edge on error. Missing exact identity
// requires preappend reprepare; there is no fallback to the current inventory.
func prepareNativeResourceInventory(database *backenddb.DB, basis *backenddb.Snapshot, allocator backenddb.NativeSemanticAllocator) (*nativeResourceInventory, error) {
	if database == nil || basis == nil || allocator == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	state, ok := basis.StateToken()
	if !ok {
		return nil, backenddb.ErrClosed
	}
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeResourceInventory{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{})))
	lease, err := allocator.AdmitNativeBytes(bytes)
	if err != nil {
		return nil, err
	}
	i := &nativeResourceInventory{refund: &nativeEngineRefund{lease: lease}}
	i.refs.Store(1)
	i.cleanup.Work = i
	i.bootstrap, err = database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		return i, err
	}
	i.metadata = i.bootstrap.NativeResourceMetadataOwner()
	if i.metadata == nil {
		return i, backenddb.ErrRecoverableRootSetStale
	}
	i.enrollment, err = i.metadata.Enroll(allocator)
	if err != nil {
		return i, err
	}
	root := backenddb.RecoverableRoot{CommitSeq: state.CommitSeq, UserRootPageID: state.RootPageID, SystemRootPageID: state.SystemRootPageID, AppliedCommandLSN: state.AppliedCommandLSN, MaxEntryRevision: uint64(state.MaxEntryRevision)}
	i.resources, err = i.bootstrap.ImportNativeResourcesForRoot(root)
	if err != nil {
		return i, err
	}
	if err := i.bootstrap.ReleaseWithError(); err != nil {
		// The exact original capture already holds its failed partials. Keep
		// this independently selected clone and enrollment on the SAME Work.
		i.failure = err
		return i, err
	}
	i.bootstrap = nil
	return i, nil
}

func (i *nativeResourceInventory) retain() {
	if i == nil || i.refs.Add(1) <= 1 {
		panic("collections: retain released native resource inventory")
	}
}

func (i *nativeResourceInventory) release() *rootpublication.SemanticCleanup {
	if i == nil {
		return nil
	}
	refs := i.refs.Add(-1)
	if refs < 0 {
		panic("collections: native resource inventory released twice")
	}
	if refs != 0 {
		return nil
	}
	return &i.cleanup
}

func (i *nativeResourceInventory) FinishSemanticCleanup() (rootpublication.SemanticCleanupCompletion, error) {
	if i.failure != nil {
		return rootpublication.SemanticCleanupCompletion{}, i.failure
	}
	if i.bootstrap != nil {
		if err := i.bootstrap.ReleaseWithError(); err != nil {
			i.failure = err
			return rootpublication.SemanticCleanupCompletion{}, err
		}
		i.bootstrap = nil
	}
	if i.resources != nil {
		if err := i.resources.ReleaseWithError(); err != nil {
			i.failure = err
			return rootpublication.SemanticCleanupCompletion{}, err
		}
		i.resources = nil
	}
	enrollment := i.enrollment
	i.enrollment, i.metadata = nil, nil
	if enrollment != nil {
		enrollment.Close()
	}
	refund := i.refund
	i.refund = nil
	return rootpublication.SemanticCleanupCompletion{Refund: refund}, nil
}
