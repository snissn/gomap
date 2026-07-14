package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// standaloneColumnStableAssetWriter owns the complete authority assembled by
// a standalone rebuild or rewrite before any of its refs become reachable.
// Each append captures the exact child and parent identities before mutation;
// Merge then coalesces physical segment identity/frontier while retaining every
// immutable logical ref obligation.
type standaloneColumnStableAssetWriter struct {
	rootDir  string
	registry *rootpublication.IdentityPinRegistry
	builder  *rootpublication.StableResourceSetBuilder
	closed   bool
}

func newStandaloneColumnStableAssetWriter(rootDir string, registry *rootpublication.IdentityPinRegistry) *standaloneColumnStableAssetWriter {
	return &standaloneColumnStableAssetWriter{
		rootDir: rootDir, registry: registry,
		builder: rootpublication.NewStableResourceSetBuilder(),
	}
}

func (writer *standaloneColumnStableAssetWriter) appendKinds(cfg ColumnStoreConfig, items []columnPhysicalAssetAppendItem) ([]ColumnAssetRef, error) {
	if writer == nil || writer.closed || writer.builder == nil || writer.registry == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	if len(items) == 0 {
		return nil, nil
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(writer.rootDir, cfg, writer.registry)
	if err != nil {
		return nil, err
	}
	refs, appendErr := appender.appendKinds(items)
	if appendErr != nil {
		return nil, errors.Join(appendErr, appender.abort())
	}
	closeErr := appender.close()
	if closeErr != nil {
		return nil, closeErr
	}
	resources := appender.stableResources
	appender.stableResources = nil
	if resources == nil {
		return nil, errors.New("collections: standalone stable column append returned no authority")
	}
	if err := writer.builder.Merge(resources); err != nil {
		resources.Release()
		return nil, err
	}
	return refs, nil
}

func (writer *standaloneColumnStableAssetWriter) appendKind(cfg ColumnStoreConfig, item columnPhysicalAssetAppendItem) (ColumnAssetRef, error) {
	refs, err := writer.appendKinds(cfg, []columnPhysicalAssetAppendItem{item})
	if err != nil {
		return ColumnAssetRef{}, err
	}
	if len(refs) != 1 {
		return ColumnAssetRef{}, fmt.Errorf("collections: standalone stable column append refs=%d want 1", len(refs))
	}
	return refs[0], nil
}

func (writer *standaloneColumnStableAssetWriter) freeze(expected []ColumnAssetRef) (*rootpublication.StableResourceSet, error) {
	if writer == nil || writer.closed || writer.builder == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	writer.closed = true
	builder := writer.builder
	resources, err := builder.Freeze()
	writer.builder = nil
	if err != nil {
		builder.Abandon()
		return nil, err
	}
	assets := make([]ColumnPreparedAsset, len(expected))
	for i, ref := range expected {
		assets[i] = ColumnPreparedAsset{Ref: ref}
	}
	if err := validateStableColumnResourcesMatchPrepared(assets, resources); err != nil {
		resources.Release()
		return nil, fmt.Errorf("%w: standalone stable column closure: %v", rootpublication.ErrUnresolvedResource, err)
	}
	return resources, nil
}

func (writer *standaloneColumnStableAssetWriter) abandon() {
	if writer == nil || writer.closed {
		return
	}
	writer.closed = true
	if writer.builder != nil {
		writer.builder.Abandon()
		writer.builder = nil
	}
}
