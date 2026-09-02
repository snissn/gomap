package collections

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// StableColumnPhysicalAssetAppend is one producer input for an exact stable
// column-asset append. The physical asset manager, not the caller, derives the
// segment identity, byte frontier, checksum, reachability field, and namespace
// evidence returned by AppendColumnPhysicalAssetsWithStableResources.
type StableColumnPhysicalAssetAppend struct {
	Payload    []byte
	Kind       ColumnAssetKind
	Generation uint64
	PartID     uint64
}

// StableResourceCaptureRecoveryRetainer accepts exact rollback authority while
// a DB capture lease still excludes teardown. Implementations must poison later
// publication and run the callback during DB shutdown.
type StableResourceCaptureRecoveryRetainer interface {
	RetainStableResourceCaptureRecovery(func() error) error
}

// AppendColumnPhysicalAssetsWithStableResources executes the same physical
// append session used by column publication and transfers its already-open
// stable resource set to the caller. Returned refs and authority are validated
// as one producer result before either becomes visible.
func AppendColumnPhysicalAssetsWithStableResources(
	rootDir string,
	cfg ColumnStoreConfig,
	fileID uint32,
	items []StableColumnPhysicalAssetAppend,
	registry *rootpublication.IdentityPinRegistry,
	recoveryRetainer StableResourceCaptureRecoveryRetainer,
) ([]ColumnAssetRef, *rootpublication.StableResourceSet, error) {
	if registry == nil {
		return nil, nil, errors.New("collections: stable column physical append requires identity pin registry")
	}
	if recoveryRetainer == nil {
		return nil, nil, errors.New("collections: stable column physical append requires capture recovery retainer")
	}
	if len(items) == 0 {
		return nil, nil, nil
	}
	internal := make([]columnPhysicalAssetAppendItem, len(items))
	for i := range items {
		internal[i] = columnPhysicalAssetAppendItem{
			payload: items[i].Payload, kind: items[i].Kind,
			generation: items[i].Generation, partID: items[i].PartID,
		}
	}
	session := newColumnPhysicalAssetAppendSessionWithStableResources(rootDir, cfg, registry, recoveryRetainer)
	refs, err := session.appendKinds(fileID, internal)
	if err != nil {
		return nil, nil, errors.Join(err, session.abort())
	}
	prepared := make([]ColumnPreparedAsset, len(refs))
	for i := range refs {
		prepared[i] = ColumnPreparedAsset{Ref: refs[i], Bytes: refs[i].Length}
	}
	_, resources, err := session.closeWithStableResourcesValidated(func(captured *rootpublication.StableResourceSet) error {
		return validateStableColumnResourcesMatchPrepared(prepared, captured)
	})
	if err != nil {
		return nil, nil, err
	}
	return refs, resources, nil
}
