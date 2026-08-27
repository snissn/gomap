package collections

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// columnVectorGraphStableResourceAccumulator owns the exact authority returned
// by every fresh vector/HNSW segment until the complete transitive union can be
// frozen against the state record that will make those refs reachable.
type columnVectorGraphStableResourceAccumulator struct {
	registry       *rootpublication.IdentityPinRegistry
	builder        *rootpublication.StableResourceSetBuilder
	segments       uint64
	contentSyncs   uint64
	namespaceSyncs uint64
	fileSync       time.Duration
	namespaceSync  time.Duration
}

func newColumnVectorGraphStableResourceAccumulator(registry *rootpublication.IdentityPinRegistry) (*columnVectorGraphStableResourceAccumulator, error) {
	if registry == nil {
		return nil, errors.New("collections: column vector rebuild stable authority requires identity pin registry")
	}
	if !rootpublication.StableNamespaceCreationSupported() {
		return nil, fmt.Errorf("%w: column vector rebuild stable authority requires exact child creation persistence", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	return &columnVectorGraphStableResourceAccumulator{
		registry: registry,
		builder:  rootpublication.NewStableResourceSetBuilder(),
	}, nil
}

func (authority *columnVectorGraphStableResourceAccumulator) newAppender(rootDir string, cfg ColumnStoreConfig) (*columnPhysicalAssetSegmentAppender, error) {
	if authority == nil || authority.builder == nil || authority.registry == nil {
		return nil, errors.New("collections: column vector rebuild stable authority is unavailable")
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(rootDir, cfg, authority.registry)
	if err != nil {
		return nil, err
	}
	appender.stableVectorGraphAuthority = true
	return appender, nil
}

func (authority *columnVectorGraphStableResourceAccumulator) closeAppender(appender *columnPhysicalAssetSegmentAppender) error {
	if authority == nil || authority.builder == nil {
		return errors.New("collections: column vector rebuild stable authority is unavailable")
	}
	if appender == nil {
		return errors.New("collections: column vector rebuild stable authority received nil appender")
	}
	if err := appender.close(); err != nil {
		return err
	}
	resources := appender.stableResources
	appender.stableResources = nil
	if resources == nil {
		return errors.New("collections: column vector rebuild segment closed without stable resources")
	}
	var namespaceSyncs uint64
	var namespaceSync time.Duration
	for _, stats := range resources.Stats(time.Now()) {
		namespaceSyncs += stats.NamespaceSyncs
		namespaceSync += stats.NamespaceSyncDuration
	}
	if err := authority.builder.Merge(resources); err != nil {
		resources.Release()
		return err
	}
	authority.segments++
	authority.contentSyncs += uint64(appender.closeStats.FileSyncCount)
	authority.namespaceSyncs += namespaceSyncs
	authority.fileSync += appender.closeStats.FileSync
	authority.namespaceSync += namespaceSync
	return nil
}

func (authority *columnVectorGraphStableResourceAccumulator) freeze(assets []columnVectorIndexStateAssetSnapshot) (*rootpublication.StableResourceSet, error) {
	if authority == nil || authority.builder == nil {
		return nil, errors.New("collections: column vector rebuild stable authority is unavailable")
	}
	resources, err := authority.builder.Freeze()
	if err != nil {
		authority.builder.Abandon()
		authority.builder = nil
		return nil, err
	}
	authority.builder = nil
	prepared := make([]ColumnPreparedAsset, len(assets))
	for i := range assets {
		prepared[i] = ColumnPreparedAsset{Ref: assets[i].Ref, Rows: assets[i].RowCount, Bytes: assets[i].AssetBytes}
	}
	if err := validateStableVectorGraphResourcesMatchPrepared(prepared, resources); err != nil {
		resources.Release()
		return nil, fmt.Errorf("collections: column vector rebuild stable resource union: %w", err)
	}
	if authority.contentSyncs != authority.segments {
		resources.Release()
		return nil, fmt.Errorf("collections: column vector rebuild content syncs=%d want segments=%d", authority.contentSyncs, authority.segments)
	}
	if authority.namespaceSyncs != authority.segments {
		resources.Release()
		return nil, fmt.Errorf("collections: column vector rebuild namespace syncs=%d want segments=%d", authority.namespaceSyncs, authority.segments)
	}
	return resources, nil
}

func (authority *columnVectorGraphStableResourceAccumulator) abandon() {
	if authority == nil || authority.builder == nil {
		return
	}
	authority.builder.Abandon()
	authority.builder = nil
}

func (prepared *columnVectorGraphPreparedPhysicalAsset) releaseStableResources() {
	if prepared == nil || prepared.stableResources == nil {
		return
	}
	prepared.stableResources.Release()
	prepared.stableResources = nil
}

func replaceColumnVectorGraphPreparedPhysicalAsset(current *columnVectorGraphPreparedPhysicalAsset, next columnVectorGraphPreparedPhysicalAsset) {
	current.releaseStableResources()
	*current = next
}

func newColumnVectorGraphAssetAppender(rootDir string, cfg ColumnStoreConfig, authority *columnVectorGraphStableResourceAccumulator) (*columnPhysicalAssetSegmentAppender, error) {
	if authority == nil {
		return newNextColumnPhysicalAssetSegmentAppender(rootDir, cfg)
	}
	return authority.newAppender(rootDir, cfg)
}

func closeColumnVectorGraphAssetAppender(appender *columnPhysicalAssetSegmentAppender, authority *columnVectorGraphStableResourceAccumulator) error {
	if authority == nil {
		return appender.close()
	}
	return authority.closeAppender(appender)
}

var (
	columnVectorGraphStableAuthorityTestHookMu sync.RWMutex
	columnVectorGraphStableAuthorityTestHook   func(*rootpublication.StableResourceSet, []columnVectorIndexStateAssetSnapshot) error
	columnVectorGraphStablePublishTestHook     func(*columnVectorGraphPreparedPhysicalAsset) error
)

func setColumnVectorGraphStableAuthorityTestHook(hook func(*rootpublication.StableResourceSet, []columnVectorIndexStateAssetSnapshot) error) func() {
	columnVectorGraphStableAuthorityTestHookMu.Lock()
	previous := columnVectorGraphStableAuthorityTestHook
	columnVectorGraphStableAuthorityTestHook = hook
	columnVectorGraphStableAuthorityTestHookMu.Unlock()
	return func() {
		columnVectorGraphStableAuthorityTestHookMu.Lock()
		columnVectorGraphStableAuthorityTestHook = previous
		columnVectorGraphStableAuthorityTestHookMu.Unlock()
	}
}

func runColumnVectorGraphStableAuthorityTestHook(resources *rootpublication.StableResourceSet, assets []columnVectorIndexStateAssetSnapshot) error {
	columnVectorGraphStableAuthorityTestHookMu.RLock()
	hook := columnVectorGraphStableAuthorityTestHook
	columnVectorGraphStableAuthorityTestHookMu.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(resources, append([]columnVectorIndexStateAssetSnapshot(nil), assets...))
}

func setColumnVectorGraphStablePublishTestHook(hook func(*columnVectorGraphPreparedPhysicalAsset) error) func() {
	columnVectorGraphStableAuthorityTestHookMu.Lock()
	previous := columnVectorGraphStablePublishTestHook
	columnVectorGraphStablePublishTestHook = hook
	columnVectorGraphStableAuthorityTestHookMu.Unlock()
	return func() {
		columnVectorGraphStableAuthorityTestHookMu.Lock()
		columnVectorGraphStablePublishTestHook = previous
		columnVectorGraphStableAuthorityTestHookMu.Unlock()
	}
}

func runColumnVectorGraphStablePublishTestHook(prepared *columnVectorGraphPreparedPhysicalAsset) error {
	columnVectorGraphStableAuthorityTestHookMu.RLock()
	hook := columnVectorGraphStablePublishTestHook
	columnVectorGraphStableAuthorityTestHookMu.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(prepared)
}
