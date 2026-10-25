package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeDomainAllocator is the existing COW budget capability. It carries no
// opening cut, snapshot, engine version or collection-manager backpointer.
// The backend bridge exposes this same capability to actual native producers.
type nativeDomainAllocator interface {
	AdmitNativeBytes(uint64) (*memtable.COWExternalLease, error)
	NewNativeCOWWriter(sourceOwnerBytes uint64) (*memtable.COWWriter, error)
}

// prepareNativeDomainUpdate privately prepares an owned root-local successor.
// Input rows are borrowed only during existing COWWriter.Prepare: new payloads
// are copied once and untouched pending rows/nodes remain shared. Internal
// source Publish establishes the original root ownership, not logical database
// visibility; only the complete engine installation may expose the result.
func prepareNativeDomainUpdate(base *nativeDomainState, name string, baseRoot, interpretation uint64, policy backenddb.OrderedRootStoragePolicy, entries []memtable.COWMutation, allocator nativeDomainAllocator) (*nativeDomainState, error) {
	if allocator == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	var writer *memtable.COWWriter
	var err error
	if base != nil {
		if name != base.name || interpretation != base.interpretation || baseRoot != base.baseRoot || policy != base.storagePolicy {
			return nil, backenddb.ErrNativeSemanticUnsupported
		}
		if base.pending != nil {
			writer, err = memtable.ForkCOWWriter(base.pending)
		}
	}
	if err != nil {
		return nil, err
	}
	if writer == nil {
		writer, err = allocator.NewNativeCOWWriter(nativePendingSourceCharge())
		if err != nil {
			return nil, err
		}
	}
	prepared, err := writer.Prepare(entries, memtable.COWPrepareOptions{})
	if err != nil {
		retirement := writer.Close()
		writer = nil
		retirement.Drain()
		return nil, err
	}
	root := prepared.Publish()
	prepared = nil
	retirement := writer.Close()
	writer = nil
	retirement.Drain()
	admit := func(bytes uint64) (cowbtree.OwnedMapLease, error) { return allocator.AdmitNativeBytes(bytes) }
	domain, err := newNativeDomainState(name, baseRoot, interpretation, policy, root, admit)
	if err != nil {
		retirement = root.Release()
		root = nil
		retirement.Drain()
		return nil, err
	}
	if base != nil {
		domain.sourceGeneration = base.sourceGeneration
		base.typed.retain()
		domain.typed = base.typed
	}
	if base != nil && base.captured != nil {
		base.captured.retain()
		domain.captured = base.captured
	}
	root = nil // original source edge transfers only after descriptor success
	return domain, nil
}
