package main

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
)

// newM8HistoricalOverlayRetainedAssetsV1 makes the explicit V2 overlay source
// used only by #4105 characterization fixtures. Production fixture creation
// deliberately remains on the V3 auxiliary-navigation default.
func newM8HistoricalOverlayRetainedAssetsV1(vectors [][]float64, groups []string, partitions int) (*m8ProductionMultiGroupAssetsV1, error) {
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, groups, partitions)
	if err != nil {
		return nil, err
	}
	assets, resources, err := materializeHistoricalLocalHNSWVariantAssetsV1(source, collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 9980)
	if err != nil {
		return nil, errors.Join(err, source.Close())
	}
	resources.Release()
	source.manifest.Assets = assets
	source.manifest.Canonicalize()
	source.status.Manifest = source.manifest
	return source, nil
}

func materializeHistoricalLocalHNSWVariantV1(source *m8ProductionMultiGroupAssetsV1, variant collections.VectorPartitionLocalGraphVariantV1, fileID uint32) (*localHNSWVariantHarnessV1, error) {
	assets, resources, err := materializeHistoricalLocalHNSWVariantAssetsV1(source, variant, fileID)
	if err != nil {
		return nil, err
	}
	harness := &localHNSWVariantHarnessV1{assets: &m8ProductionMultiGroupAssetsV1{manifest: source.manifest}, resources: resources, packAssets: assets, searchers: make([]*collections.VectorPartitionLocalSearcherV1, len(assets))}
	for partition, asset := range assets {
		if asset.PartitionID != uint32(partition) {
			return nil, errors.Join(errors.New("historical variant partition ordering"), harness.Close())
		}
		searcher, openErr := source.collection.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(context.Background(), source.manifest.IndexName, source.manifest, asset)
		if openErr != nil {
			return nil, errors.Join(openErr, harness.Close())
		}
		harness.searchers[partition] = searcher
	}
	return harness, nil
}

func materializeHistoricalLocalHNSWVariantAssetsV1(source *m8ProductionMultiGroupAssetsV1, variant collections.VectorPartitionLocalGraphVariantV1, fileID uint32) ([]collections.VectorPartitionAssetV1, interface{ Release() }, error) {
	if source == nil || source.collection == nil || source.manifest.PartitionCount == 0 || fileID == 0 {
		return nil, nil, errors.New("historical variant source")
	}
	var dimensions int
	for _, definition := range source.collection.Meta().VectorIndexes {
		if definition.Name == source.manifest.IndexName {
			dimensions = definition.Dimensions
			break
		}
	}
	if dimensions == 0 {
		return nil, nil, errors.New("historical variant dimensions")
	}
	sourceID := collections.VectorPartitionSourceIdentityV1{Generation: source.manifest.SourceGeneration, Checksum: source.manifest.SourceChecksum, SchemaHash: source.manifest.SourceSchemaHash, RowCount: source.manifest.SourceRowCount}
	inputs := make([]collections.VectorPartitionSearchAssetV1, source.manifest.PartitionCount)
	for partition := range inputs {
		inputs[partition] = collections.VectorPartitionSearchAssetV1{Source: sourceID, Generation: source.manifest.Generation, PartitionID: uint32(partition), Dimensions: dimensions}
	}
	return source.collection.MaterializeVectorPartitionLocalSearchAssetsVariantV1(source.manifest.IndexName, source.manifest, fileID, inputs, variant)
}
