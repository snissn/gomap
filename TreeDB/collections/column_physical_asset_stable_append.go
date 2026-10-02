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
	// FileID selects a segment for this item. Zero uses the call's fileID.
	// Adjacent items with the same effective ID share one append batch.
	FileID uint32
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
	segmented := false
	for _, item := range items {
		segmented = segmented || item.FileID != 0
	}
	refs := make([]ColumnAssetRef, 0, len(items))
	for start := 0; start < len(items); {
		selectedFileID := items[start].FileID
		if selectedFileID == 0 {
			selectedFileID = fileID
		}
		end := start + 1
		for end < len(items) {
			nextFileID := items[end].FileID
			if nextFileID == 0 {
				nextFileID = fileID
			}
			if nextFileID != selectedFileID {
				break
			}
			end++
		}
		batch, err := session.appendKinds(selectedFileID, internal[start:end])
		if err != nil {
			return nil, nil, errors.Join(err, session.abort())
		}
		if segmented {
			preparedBatch := make([]ColumnPreparedAsset, len(batch))
			for i := range batch {
				preparedBatch[i] = ColumnPreparedAsset{Ref: batch[i], Bytes: batch[i].Length}
			}
			if err := session.closeActiveWithStableValidation(func(captured *rootpublication.StableResourceSet) error {
				return validateStableColumnResourcesMatchPrepared(preparedBatch, captured)
			}); err != nil {
				return nil, nil, errors.Join(err, session.abort())
			}
		}
		refs = append(refs, batch...)
		start = end
	}
	prepared := make([]ColumnPreparedAsset, len(refs))
	for i := range refs {
		prepared[i] = ColumnPreparedAsset{Ref: refs[i], Bytes: refs[i].Length}
	}
	var resources *rootpublication.StableResourceSet
	var err error
	if segmented {
		_, resources, err = session.closeWithStableResources()
	} else {
		_, resources, err = session.closeWithStableResourcesValidated(func(captured *rootpublication.StableResourceSet) error {
			return validateStableColumnResourcesMatchPrepared(prepared, captured)
		})
	}
	if err != nil {
		return nil, nil, err
	}
	if segmented {
		if err := validateStableColumnResourcesMatchPrepared(prepared, resources); err != nil {
			resources.Release()
			return nil, nil, errors.Join(err, session.forgetNewStableLinks())
		}
	}
	return refs, resources, nil
}

// appendFreshColumnPhysicalAssetsWithStableResources creates one manager-owned
// O_EXCL output. An interrupted, unpublished output is never reopened, adopted,
// truncated, or reused by a later prepare attempt.
func appendFreshColumnPhysicalAssetsWithStableResources(root string, cfg ColumnStoreConfig, items []StableColumnPhysicalAssetAppend, registry *rootpublication.IdentityPinRegistry, recovery StableResourceCaptureRecoveryRetainer) ([]ColumnAssetRef, *rootpublication.StableResourceSet, error) {
	if registry == nil || recovery == nil || len(items) == 0 {
		return nil, nil, errors.New("collections: incomplete fresh asset append authority")
	}
	session := newColumnPhysicalAssetAppendSessionWithStableResources(root, cfg, registry, recovery)
	appender, err := session.freshAppender()
	if err != nil {
		return nil, nil, errors.Join(err, session.abort())
	}
	internal := make([]columnPhysicalAssetAppendItem, len(items))
	for i, item := range items {
		if item.FileID != 0 {
			return nil, nil, errors.Join(errors.New("collections: fresh prepare cannot select a foreign file"), session.abort())
		}
		internal[i] = columnPhysicalAssetAppendItem{payload: item.Payload, kind: item.Kind, generation: item.Generation, partID: item.PartID}
	}
	refs, err := appender.appendKinds(internal)
	if err != nil {
		return nil, nil, errors.Join(err, session.abort())
	}
	prepared := make([]ColumnPreparedAsset, len(refs))
	for i, ref := range refs {
		prepared[i] = ColumnPreparedAsset{Ref: ref, Bytes: ref.Length}
	}
	_, resources, err := session.closeWithStableResourcesValidated(func(captured *rootpublication.StableResourceSet) error {
		return validateStableColumnResourcesMatchPrepared(prepared, captured)
	})
	return refs, resources, err
}
