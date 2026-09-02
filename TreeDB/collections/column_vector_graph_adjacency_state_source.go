package collections

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

const columnVectorGraphAdjacencyStateSourceScopeID = "column-vector-graph-adjacency-state"

func (c *Collection) openColumnVectorGraphAdjacencyStateSourcesForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnVectorGraphAdjacencyDirectSources, typeddecode.Reason, error) {
	if c == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionNil
	}
	if c.db == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionDBNil
	}
	return openColumnVectorGraphAdjacencyStateSourcesFromRoot(c.db.ColumnAssetRootDir(), collection, cfg, def, graph, state)
}

func openColumnVectorGraphAdjacencyStateSourcesFromRoot(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnVectorGraphAdjacencyDirectSources, typeddecode.Reason, error) {
	if !columnVectorIndexStateDefinitionParametersMatch(&state, &def) || !columnVectorIndexStateMatchesGraph(state, graph) {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state identity mismatch", def.Name)
	}
	assets, reason, err := columnVectorGraphAdjacencyStateAssetsByLayer(state, graph)
	if err != nil {
		return nil, reason, err
	}
	group := &columnVectorGraphAdjacencyDirectSources{sources: make([]*columnVectorGraphLayer0AdjacencyDirectSource, 0, len(assets)), allLayers: true}
	for layer, asset := range assets {
		source, sourceReason, sourceErr := newColumnVectorGraphAdjacencyStateDirectSourceFromRoot(rootDir, collection, cfg, def, graph, state, asset, layer)
		if sourceErr != nil {
			_ = group.Close()
			if sourceReason == "" {
				sourceReason = typeddecode.ReasonValidationFailed
			}
			return nil, sourceReason, sourceErr
		}
		group.sources = append(group.sources, source)
	}
	return group, "", nil
}

func columnVectorGraphAdjacencyStateAssetsByLayer(state columnVectorIndexStateSnapshot, graph columnVectorGraphManifestSnapshot) ([]columnVectorIndexStateAssetSnapshot, typeddecode.Reason, error) {
	seen := make(map[int]struct{})
	maxLayer := -1
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleAdjacency {
			continue
		}
		layer, err := columnVectorIndexStateAdjacencyLayerFromAssetID(asset.AssetID)
		if err != nil {
			return nil, typeddecode.ReasonValidationFailed, err
		}
		if _, ok := seen[layer]; ok {
			return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: vector-index state duplicate adjacency layer=%d", layer)
		}
		seen[layer] = struct{}{}
		if layer > maxLayer {
			maxLayer = layer
		}
	}
	if len(seen) == 0 {
		return nil, typeddecode.ReasonValidationFailed, errors.New("collections: vector-index state missing adjacency uint32_list assets")
	}
	expectedLayers := state.AdjacencyLayerCount
	if expectedLayers <= 0 {
		expectedLayers = graph.AdjacencyLayerCount
	}
	if expectedLayers <= 0 {
		expectedLayers = maxLayer + 1
	}
	if len(seen) != expectedLayers {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: vector-index state adjacency layers=%d want %d", len(seen), expectedLayers)
	}
	assets := make([]columnVectorIndexStateAssetSnapshot, expectedLayers)
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleAdjacency {
			continue
		}
		layer, _ := columnVectorIndexStateAdjacencyLayerFromAssetID(asset.AssetID)
		if layer < 0 || layer >= expectedLayers {
			return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: vector-index state adjacency layer=%d outside expected layers=%d", layer, expectedLayers)
		}
		assets[layer] = asset
	}
	for layer := 0; layer < expectedLayers; layer++ {
		if assets[layer].Role == "" {
			return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: vector-index state missing adjacency layer %d", layer)
		}
	}
	return assets, "", nil
}

func newColumnVectorGraphAdjacencyStateDirectSourceFromRoot(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, asset columnVectorIndexStateAssetSnapshot, layer int) (*columnVectorGraphLayer0AdjacencyDirectSource, typeddecode.Reason, error) {
	if asset.Role != columnVectorIndexStateAssetRoleAdjacency || asset.AssetID != columnVectorIndexStateAdjacencyAssetID(layer) || asset.LogicalType != columnVectorIndexStateLogicalTypeUint32List || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawUint32List {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d asset contract mismatch role=%q id=%q logical=%q physical=%q", def.Name, layer, asset.Role, asset.AssetID, asset.LogicalType, asset.PhysicalEncoding)
	}
	if asset.RowCount != graph.RowCount || asset.RowCount != state.RowCount || asset.Ref.Generation != graph.BaseManifestGeneration || asset.Ref.Namespace != cfg.AssetManager.Namespace || asset.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart || asset.AssetBytes != asset.Ref.Length {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d asset identity mismatch", def.Name, layer)
	}
	sourceCfg, adapterColumn, err := columnVectorIndexStateAdjacencyColumnStoreConfig(collection, cfg, def, layer)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d schema_hash=%d want %d", def.Name, layer, asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if err := validateColumnVectorIndexStateAssetRefAvailable(rootDir, asset); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, asset.Ref)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if int64(len(raw)) != asset.AssetBytes || int64(len(raw)) != asset.Ref.Length {
		return nil, typeddecode.ReasonPayloadLengthMismatch, fmt.Errorf("collections: column_graph %q adjacency state layer %d bytes=%d manifest=%d ref=%d", def.Name, layer, len(raw), asset.AssetBytes, asset.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if image.PartID != asset.Ref.PartID || image.Rows != asset.RowCount || image.Rows != state.RowCount {
		return nil, typeddecode.ReasonRowCountMismatch, fmt.Errorf("collections: column_graph %q adjacency state layer %d image part/rows=(%d,%d) want (%d,%d)", def.Name, layer, image.PartID, image.Rows, asset.Ref.PartID, state.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if adapterPart.Part.Descriptor.SchemaVersion != uint32(sourceCfg.SchemaHash) {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d schema_version=%d want %d", def.Name, layer, adapterPart.Part.Descriptor.SchemaVersion, uint32(sourceCfg.SchemaHash))
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d missing offsets-list sections for column %q", def.Name, layer, adapterColumn.Definition.Name)
	}
	wantOffsetsBytes, err := columnVectorIndexStateAdjacencyOffsetsBytes(state.RowCount)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if offsetsSection.Length != wantOffsetsBytes {
		return nil, typeddecode.ReasonOffsetsCountMismatch, fmt.Errorf("collections: column_graph %q adjacency state layer %d offsets bytes=%d want %d", def.Name, layer, offsetsSection.Length, wantOffsetsBytes)
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if err := validateColumnVectorIndexStateAdjacencySections(layer, offsetsSection, valuesSection, offsetsRaw, valuesRaw, state.RowCount); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d layout certification: %w", def.Name, layer, err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d missing layout certification for column %q", def.Name, layer, adapterColumn.Definition.Name)
	}
	if certColumn.LogicalType != columnVectorIndexStateLogicalTypeUint32List || certColumn.Type != typedcolumn.ColumnTypeUint32List || certColumn.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q adjacency state layer %d logical/type/encoding=(%q,%s,%s) want (%q,%s,%s)", def.Name, layer, certColumn.LogicalType, certColumn.Type, certColumn.Encoding, columnVectorIndexStateLogicalTypeUint32List, typedcolumn.ColumnTypeUint32List, typedcolumn.EncodingRawUint32OffsetsList)
	}
	plan := typeddecode.Uint32ListPlan(certColumn)
	directReq := typeddecode.Uint32OffsetsListDirectViewRequest{
		Plan:           plan,
		Certification:  certColumn,
		Rows:           state.RowCount,
		OffsetsBytes:   offsetsSection.Length,
		ValuesBytes:    valuesSection.Length,
		AssetOffset:    asset.Ref.Offset,
		HasAssetOffset: true,
	}
	manager := mappedresource.NewManager()
	offsetsChecksum := page.Checksum(offsetsRaw)
	valuesChecksum := page.Checksum(valuesRaw)
	expectedOffsetsKey, err := columnVectorGraphAdjacencyStateSectionKey(asset.Ref, image.Version, offsetsSection, offsetsChecksum)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	expectedValuesKey, err := columnVectorGraphAdjacencyStateSectionKey(asset.Ref, image.Version, valuesSection, valuesChecksum)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	offsetsHandle, err := acquireColumnVectorGraphAdjacencyStateSection(rootDir, collection, asset.Ref, image.Version, offsetsSection, offsetsChecksum, manager, fmt.Sprintf("layer %d offsets", layer))
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	valuesHandle, err := acquireColumnVectorGraphAdjacencyStateSection(rootDir, collection, asset.Ref, image.Version, valuesSection, valuesChecksum, manager, fmt.Sprintf("layer %d values", layer))
	if err != nil {
		releaseErr := offsetsHandle.Release()
		return nil, typeddecode.ReasonValidationFailed, errors.Join(err, releaseErr)
	}
	graphReq := typeddecode.GraphUint32ListDirectViewRequest{
		Expectation: typeddecode.GraphDirectViewExpectation{
			ExpectedOwner:  def.Name,
			ActualOwner:    state.IndexName,
			ExpectedRole:   columnVectorIndexStateAssetRoleAdjacency,
			ActualRole:     asset.Role,
			Column:         adapterColumn.Definition.Name,
			Rows:           state.RowCount,
			AssetOffset:    asset.Ref.Offset,
			HasAssetOffset: true,
		},
		Certification:      certColumn,
		OffsetsSection:     offsetsSection,
		ValuesSection:      valuesSection,
		ExpectedOffsetsKey: expectedOffsetsKey,
		ExpectedValuesKey:  expectedValuesKey,
		OffsetsHandle:      offsetsHandle,
		ValuesHandle:       valuesHandle,
		Manager:            manager,
	}
	source, reason, err := columnVectorGraphPreparedCSRAdjacencyDirectSourceFromHandles(manager, layer, state.RowCount, valuesSection.Length/4, graphReq, directReq, offsetsHandle, valuesHandle)
	if err != nil {
		releaseErr := errors.Join(offsetsHandle.Release(), valuesHandle.Release())
		return nil, reason, errors.Join(err, releaseErr)
	}
	source.captureResourceStats()
	return source, "", nil
}

func columnVectorGraphAdjacencyStateSectionKey(ref ColumnAssetRef, imageVersion uint16, section typedcolumn.ColumnPartImageSection, checksum uint32) (mappedresource.Key, error) {
	sectionOffset, err := columnVectorGraphTypedColumnSectionOffset(ref, section)
	if err != nil {
		return mappedresource.Key{}, err
	}
	return mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  ref.Namespace,
		Kind:       string(ref.Kind),
		Generation: ref.Generation,
		PartID:     ref.PartID,
		FileID:     ref.FileID,
		Offset:     sectionOffset,
		Length:     int64(section.Length),
		Checksum:   uint64(checksum),
		Version:    imageVersion,
		Encoding:   section.Encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(section.Kind),
			Category: string(section.Category),
			Name:     section.Name,
			Column:   section.Column,
		},
	}, nil
}

func acquireColumnVectorGraphAdjacencyStateSection(rootDir, collection string, ref ColumnAssetRef, imageVersion uint16, section typedcolumn.ColumnPartImageSection, checksum uint32, manager *mappedresource.Manager, label string) (*mappedresource.Handle, error) {
	if manager == nil {
		return nil, errors.New("collections: column_graph adjacency state requires mappedresource manager")
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return nil, err
	}
	key, err := columnVectorGraphAdjacencyStateSectionKey(ref, imageVersion, section, checksum)
	if err != nil {
		return nil, err
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphAdjacencyStateSourceScopeID, Collection: collection, Namespace: ref.Namespace, Generation: ref.Generation, Reason: "column_graph adjacency state"}
	handle, err := manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{
		Reason:         "column_graph adjacency state " + label,
		ValidationMode: mappedresource.ValidationVerify,
		PreferMapped:   true,
		AllowHeapCopy:  true,
		ResourceRoot:   rootDir,
		ResourcePath:   path,
	})
	if err != nil {
		return nil, err
	}
	if got := page.Checksum(handle.Bytes()); got != checksum {
		releaseErr := handle.Release()
		return nil, errors.Join(fmt.Errorf("collections: column_graph adjacency state %s checksum=%d want %d", label, got, checksum), releaseErr)
	}
	return handle, nil
}
