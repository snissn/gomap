package db

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// NativeResourceMetadataOwner borrows this original capture's allocation
// authority, never its resource inventory. The caller must retain set and enroll
// this owner in its original budget before constructing an independent import.
func (set *RecoverableRootSet) NativeResourceMetadataOwner() *retainedalloc.Owner {
	if set == nil {
		return nil
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.released.Load() {
		return nil
	}
	return set.metadata
}

// ImportNativeResourcesForRoot is an opening-only operation. Exact identity
// absence is stale, including a mismatch in any of the five scalar fields.
// Import may walk generic/foreign metadata once; subsequent same-owner selected
// imports share the original persistent kind roots. It never samples runtime
// visibleResources, reopens a diagnostic path, or changes physical custody.
// Any nonnil result belongs to the caller, including when err is nonnil.
func (set *RecoverableRootSet) ImportNativeResourcesForRoot(root RecoverableRoot) (*rootpublication.StableResourceSet, error) {
	if set == nil {
		return nil, ErrRecoverableRootSetStale
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if set.released.Load() || set.metadata == nil {
		return nil, ErrRecoverableRootSetStale
	}
	resources, ok := set.lookupRootResources(recoverableRootIdentity(root))
	if !ok {
		return nil, ErrRecoverableRootSetStale
	}
	return rootpublication.ImportStableResourceSetMetadata(set.metadata, resources)
}
