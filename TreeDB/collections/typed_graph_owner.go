package collections

import "sync"

// Reuse the parser's validated records, not a second manifest read. The lease
// includes every typed field: document materialization may start after GC has
// retired this snapshot's roots, long after graph preparation finished.
func (c *Collection) acquireTypedGraphOwnerPin(records []columnManifestRecord, generation uint64, namespace string) (*ColumnAssetLifecyclePinSet, error) {
	requirements, err := stableColumnManifestDurableRequirements(records, generation, namespace)
	if err != nil {
		return nil, err
	}
	refs := make([]ColumnAssetRef, len(requirements.Obligations))
	for i, o := range requirements.Obligations {
		refs[i] = ColumnAssetRef{Kind: ColumnAssetKind(o.Kind), Namespace: o.Namespace, Generation: o.Generation, PartID: o.PartID, FileID: uint32(o.FileID), Offset: o.Offset, Length: o.Length, Checksum: o.Checksum}
	}
	return c.AcquireColumnAssetLifecyclePinSet(ColumnAssetLifecyclePinSetOptions{Source: ColumnAssetLifecyclePinSourcePreparedQuery, Owner: "vector_index_searcher", Refs: refs})
}

var typedGraphOwnerAfterSnapshotHook struct {
	sync.RWMutex
	fn func(*Collection)
}

func runTypedGraphOwnerAfterSnapshotHook(c *Collection) {
	typedGraphOwnerAfterSnapshotHook.RLock()
	fn := typedGraphOwnerAfterSnapshotHook.fn
	typedGraphOwnerAfterSnapshotHook.RUnlock()
	if fn != nil {
		fn(c)
	}
}
