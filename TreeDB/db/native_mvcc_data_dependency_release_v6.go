package db

import (
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This is the original generation's directory borrower, not a physical owner.
// It retains the exact registry/containing wrapper across DB shutdown. The
// directory consumes its by-value completion after clearing the provider alias.
type dataDependencyLeaseV6 struct {
	mu         sync.Mutex
	db         *DB
	generation *indexGen
	registryID int64
	metadata   *retainedalloc.Owner
	charge     uint64
	closed     bool
	err        error
	failure    originalDBCleanupFailure
}

func (db *DB) pinDataDependencyDirectoryV6(idx *indexGen, sequence uint64, ref rootpublication.DependencyDirectoryRefV2, totalPages uint64) (*rootpublication.DependencyDirectoryV2, error) {
	metadata := idx.metadata
	var charge uint64
	if metadata != nil {
		charge = retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(dataDependencyLeaseV6{})))
		if err := metadata.AddPending(charge); err != nil {
			return nil, err
		}
	}
	// Caller holds the original root-reuse exclusion. Admission precedes both
	// containing allocation and the original registry/generation borrow effects.
	lease := &dataDependencyLeaseV6{db: db, generation: idx, metadata: metadata, charge: charge}
	lease.failure.payload = lease
	idx.retainWrapperBorrow()
	// This edge keeps directory pages and their generation reusable only after
	// release; it does not itself expose an old logical asset view to a reader.
	lease.registryID = idx.registry.RegisterPageRetention(sequence)
	directory, err := rootpublication.NewOwnedTypedDependencyDirectoryV6(idx.pager, ref, totalPages, metadata, lease)
	if err != nil {
		consumed, completion, cleanup := lease.ReleaseDependencyDirectoryV6()
		if !consumed {
			lease.failure.retainCause(0, err)
			lease.failure.retainCause(1, cleanup)
			failure := &lease.failure
			db.retainOriginalCleanupError(failure)
			return nil, failure
		}
		lease = nil
		if completion.Charge != 0 {
			completion.Metadata.RemovePending(completion.Charge)
		}
		return nil, err
	}
	return directory, nil
}

func (lease *dataDependencyLeaseV6) ReleaseDependencyDirectoryV6() (consumed bool, completion rootpublication.DependencyDirectoryReleaseCompletionV6, err error) {
	lease.mu.Lock()
	if lease.closed {
		err = lease.err
		lease.mu.Unlock()
		return err == nil, completion, err
	}
	idx, db := lease.generation, lease.db
	idx.registry.Unregister(lease.registryID)
	lease.registryID = 0
	if db.idx.Load() != idx {
		db.maybeReleaseRetiredIndex(idx)
	}
	generationMetadata, generationCharge, releaseErr := idx.releaseWrapperBorrowCompletionV6(nil, nil)
	if releaseErr != nil {
		lease.failure.retainCause(1, releaseErr)
		lease.err = &lease.failure
		if lease.metadata != nil {
			lease.metadata.CleanupFailed()
		}
		err = lease.err
		lease.closed = true
		lease.mu.Unlock()
		return false, completion, err
	}
	if generationCharge != 0 && generationMetadata != lease.metadata {
		panic("DATA directory changed original generation metadata owner")
	}
	idx, db = nil, nil
	completion = rootpublication.DependencyDirectoryReleaseCompletionV6{Metadata: lease.metadata, Charge: lease.charge + generationCharge}
	lease.generation, lease.db, lease.metadata = nil, nil, nil
	lease.charge = 0
	lease.failure.payload = nil
	lease.closed = true
	lease.mu.Unlock()
	return true, completion, nil
}
