package collections

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

type columnHNSWSearchPackPreparedAsset struct {
	Present    bool
	Ref        ColumnAssetRef
	Bytes      int64
	Rows       int
	SchemaHash uint64
}

func prepareColumnHNSWSearchPackAsset(assetRootDir string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, generation, partID uint64, rows []columnVectorGraphAssetRow) (columnHNSWSearchPackPreparedAsset, error) {
	return prepareColumnHNSWSearchPackAssetWithStableAuthority(assetRootDir, cfg, def, graph, generation, partID, rows, nil)
}

func prepareColumnHNSWSearchPackAssetWithStableAuthority(assetRootDir string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, generation, partID uint64, rows []columnVectorGraphAssetRow, authority *columnVectorGraphStableResourceAccumulator) (columnHNSWSearchPackPreparedAsset, error) {
	if assetRootDir == "" {
		return columnHNSWSearchPackPreparedAsset{}, errors.New("collections: hnsw search pack requires asset root dir")
	}
	if cfg.AssetManager == nil {
		return columnHNSWSearchPackPreparedAsset{}, errors.New("collections: hnsw search pack requires column asset manager")
	}
	if generation == 0 || partID == 0 {
		return columnHNSWSearchPackPreparedAsset{}, errors.New("collections: hnsw search pack requires generation and part_id")
	}
	if graph.RowCount != len(rows) {
		return columnHNSWSearchPackPreparedAsset{}, fmt.Errorf("collections: hnsw search pack rows=%d want graph row_count=%d", len(rows), graph.RowCount)
	}
	input, err := buildColumnHNSWSearchPackInputWithoutVectors(def, graph, rows)
	if err != nil {
		return columnHNSWSearchPackPreparedAsset{}, err
	}
	plan, err := planColumnHNSWSearchPackStream(input)
	if err != nil {
		return columnHNSWSearchPackPreparedAsset{}, err
	}
	length := int64(plan.totalLength)
	appender, err := newColumnVectorGraphAssetAppender(assetRootDir, cfg, authority)
	if err != nil {
		return columnHNSWSearchPackPreparedAsset{}, err
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1HNSWSearchPack, cfg)
	ref, appendErr := appender.appendKindWithReservedPayload(length, ColumnAssetKindTCS1HNSWSearchPack, generation, partID, alignment, func(payload *columnAssetReservedPayload) error {
		written, err := writeColumnHNSWSearchPackRowsWithBackpatch(payload, func(b []byte) error { return payload.Backpatch(0, b) }, input, rows)
		if err == nil && written != length {
			return io.ErrShortWrite
		}
		return err
	})
	closeErr := closeColumnVectorGraphAssetAppender(appender, authority)
	if appendErr != nil {
		return columnHNSWSearchPackPreparedAsset{}, errors.Join(appendErr, closeErr)
	}
	if closeErr != nil {
		return columnHNSWSearchPackPreparedAsset{}, closeErr
	}
	prepared := columnHNSWSearchPackPreparedAsset{
		Present:    true,
		Ref:        ref,
		Bytes:      ref.Length,
		Rows:       len(rows),
		SchemaHash: cfg.SchemaHash,
	}
	if err := validateColumnHNSWSearchPackPreparedAsset(assetRootDir, prepared, graph, def); err != nil {
		return columnHNSWSearchPackPreparedAsset{}, err
	}
	return prepared, nil
}

func buildColumnHNSWSearchPackInputWithoutVectors(def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, rows []columnVectorGraphAssetRow) (columnHNSWSearchPackBuildInput, error) {
	if def.Metric != VectorMetricCosine {
		return columnHNSWSearchPackBuildInput{}, fmt.Errorf("collections: hnsw search pack supports only metric %q, got %q", VectorMetricCosine, def.Metric)
	}
	if def.Encoding != VectorIndexEncodingFloat32 {
		return columnHNSWSearchPackBuildInput{}, fmt.Errorf("collections: hnsw search pack supports only encoding %q, got %q", VectorIndexEncodingFloat32, def.Encoding)
	}
	if graph.RowCount != len(rows) {
		return columnHNSWSearchPackBuildInput{}, fmt.Errorf("collections: hnsw search pack rows=%d want graph row_count=%d", len(rows), graph.RowCount)
	}
	if !columnVectorGraphDefinitionParametersMatch(&graph, &def) {
		return columnHNSWSearchPackBuildInput{}, fmt.Errorf("collections: hnsw search pack graph/definition mismatch for index %q", def.Name)
	}
	stride, err := columnHNSWSearchPackVectorStrideForDimensions(def.Dimensions)
	if err != nil {
		return columnHNSWSearchPackBuildInput{}, err
	}
	levels, layers, maxLayer, err := buildColumnHNSWSearchPackLevelsAndAdjacency(rows)
	if err != nil {
		return columnHNSWSearchPackBuildInput{}, err
	}
	rowRefGenerations, rowRefPartIDs, rowRefRowIndexes, rowRefAppliedCommandLSNs, err := buildColumnHNSWSearchPackRowRefs(rows, graph.BaseManifestGeneration)
	if err != nil {
		return columnHNSWSearchPackBuildInput{}, err
	}
	docIDs, err := buildColumnVectorGraphDocumentIDStateBytes(rows)
	if err != nil {
		return columnHNSWSearchPackBuildInput{}, err
	}
	entryOrdinal := -1
	if len(rows) > 0 {
		// columnVectorGraphNativeLocalityOrder places the HNSW entry node first;
		// rebuild keeps that order when serializing rows and state assets.
		entryOrdinal = 0
	}
	return columnHNSWSearchPackBuildInput{
		Rows:           len(rows),
		Dimensions:     def.Dimensions,
		VectorStride:   stride,
		M:              def.M,
		EfConstruction: def.EfConstruction,
		EfSearch:       def.EfSearch,
		EntryOrdinal:   entryOrdinal,
		MaxLayer:       maxLayer,
		BaseIdentity: columnHNSWSearchPackBaseIdentity{
			ManifestGeneration: graph.BaseManifestGeneration,
			ManifestChecksum:   graph.BaseManifestChecksum,
			SchemaHash:         graph.BaseSchemaHash,
		},
		Levels:                  levels,
		AdjacencyLayers:         layers,
		RowRefGenerations:       rowRefGenerations,
		RowRefPartIDs:           rowRefPartIDs,
		RowRefRowIndexes:        rowRefRowIndexes,
		RowRefAppliedCommandLSN: rowRefAppliedCommandLSNs,
		DocumentIDOffsets:       docIDs.Offsets,
		DocumentIDBytes:         docIDs.Values,
	}, nil
}

func columnHNSWSearchPackVectorStrideForDimensions(dimensions int) (int, error) {
	if dimensions <= 0 {
		return 0, fmt.Errorf("collections: hnsw search pack dimensions=%d must be positive", dimensions)
	}
	alignmentFloats := int(columnHNSWSearchPackVectorSectionAlignment) / 4
	if alignmentFloats <= 0 {
		return 0, errors.New("collections: hnsw search pack vector alignment is invalid")
	}
	stride := dimensions
	rem := stride % alignmentFloats
	if rem != 0 {
		if stride > math.MaxInt-(alignmentFloats-rem) {
			return 0, errors.New("collections: hnsw search pack vector stride overflows int")
		}
		stride += alignmentFloats - rem
	}
	return stride, nil
}

func buildColumnHNSWSearchPackNormalizedVectors(rows []columnVectorGraphAssetRow, dimensions, stride int) ([]float32, error) {
	if dimensions <= 0 || stride < dimensions {
		return nil, fmt.Errorf("collections: hnsw search pack invalid dimensions/stride=(%d,%d)", dimensions, stride)
	}
	if len(rows) != 0 && stride > math.MaxInt/len(rows) {
		return nil, errors.New("collections: hnsw search pack normalized vector count overflows int")
	}
	values := make([]float32, len(rows)*stride)
	for ordinal, row := range rows {
		if len(row.Vector) != dimensions {
			return nil, fmt.Errorf("collections: hnsw search pack row[%d] vector dims=%d want %d", ordinal, len(row.Vector), dimensions)
		}
		invNorm := row.InvNorm
		if invNorm <= 0 || math.IsNaN(float64(invNorm)) || math.IsInf(float64(invNorm), 0) {
			computed, err := columnVectorGraphInvNorm(row.Vector)
			if err != nil {
				return nil, fmt.Errorf("collections: hnsw search pack row[%d] inverse norm: %w", ordinal, err)
			}
			invNorm = computed
		}
		base := ordinal * stride
		for dim, value := range row.Vector {
			normalized := value * invNorm
			if math.IsNaN(float64(normalized)) || math.IsInf(float64(normalized), 0) {
				return nil, fmt.Errorf("collections: hnsw search pack row[%d] normalized vector[%d] is not finite", ordinal, dim)
			}
			values[base+dim] = normalized
		}
	}
	return values, nil
}

func buildColumnHNSWSearchPackLevelsAndAdjacency(rows []columnVectorGraphAssetRow) ([]uint16, []columnHNSWSearchPackLayerInput, int, error) {
	if len(rows) == 0 {
		return nil, nil, -1, nil
	}
	lists, err := buildColumnVectorIndexStateAdjacencyLists(rows)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(lists) == 0 {
		return nil, nil, 0, errors.New("collections: hnsw search pack non-empty graph produced no adjacency layers")
	}
	maxLayer := len(lists) - 1
	levels := make([]uint16, len(rows))
	for ordinal, row := range rows {
		level, err := columnVectorGraphAdjacencyMaxLayer(row.Adjacency)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("collections: hnsw search pack row[%d] adjacency max layer: %w", ordinal, err)
		}
		if level < 0 || level > maxLayer || level > math.MaxUint16 {
			return nil, nil, 0, fmt.Errorf("collections: hnsw search pack row[%d] level=%d outside max_layer=%d", ordinal, level, maxLayer)
		}
		levels[ordinal] = uint16(level)
	}
	layers := make([]columnHNSWSearchPackLayerInput, len(lists))
	for layer, list := range lists {
		layers[layer] = columnHNSWSearchPackLayerInput{
			Offsets:   append([]uint64(nil), list.Offsets...),
			Neighbors: append([]uint32(nil), list.Values...),
		}
	}
	return levels, layers, maxLayer, nil
}

func buildColumnHNSWSearchPackRowRefs(rows []columnVectorGraphAssetRow, baseGeneration uint64) ([]int64, []int64, []int64, []int64, error) {
	if len(rows) == 0 {
		return nil, nil, nil, nil, nil
	}
	generations, err := columnVectorGraphRowRefStateValues(columnVectorGraphRowRefStateFieldGeneration, rows, baseGeneration)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	partIDs, err := columnVectorGraphRowRefStateValues(columnVectorGraphRowRefStateFieldPartID, rows, baseGeneration)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	rowIndexes, err := columnVectorGraphRowRefStateValues(columnVectorGraphRowRefStateFieldRowIndex, rows, baseGeneration)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	appliedCommandLSNs, err := columnVectorGraphRowRefStateValues(columnVectorGraphRowRefStateFieldAppliedCommandLSN, rows, baseGeneration)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return generations, partIDs, rowIndexes, appliedCommandLSNs, nil
}

func validateColumnHNSWSearchPackPreparedAsset(rootDir string, prepared columnHNSWSearchPackPreparedAsset, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition) error {
	if !prepared.Present {
		return errors.New("collections: hnsw search pack asset is not present")
	}
	if prepared.Ref.Kind != ColumnAssetKindTCS1HNSWSearchPack || prepared.Ref.Generation != graph.BaseManifestGeneration || prepared.Ref.Length != prepared.Bytes || prepared.Rows != graph.RowCount {
		return fmt.Errorf("collections: invalid hnsw search pack asset %+v graph rows/generation=(%d,%d)", prepared, graph.RowCount, graph.BaseManifestGeneration)
	}
	return validateColumnHNSWSearchPackAssetPayload(rootDir, prepared.Ref, prepared.Bytes, graph, def)
}

func columnHNSWSearchPackStateAssetSnapshot(prepared columnHNSWSearchPackPreparedAsset) (columnVectorIndexStateAssetSnapshot, bool) {
	if !prepared.Present {
		return columnVectorIndexStateAssetSnapshot{}, false
	}
	return columnVectorIndexStateAssetSnapshot{
		Role:             columnVectorIndexStateAssetRoleHNSWSearchPack,
		AssetID:          columnVectorIndexStateHNSWSearchPackAssetID,
		LogicalType:      columnVectorIndexStateLogicalTypeSearchPack,
		PhysicalEncoding: columnVectorIndexStateEncodingHNSWSearchPackV1,
		RowCount:         prepared.Rows,
		SourceSchemaHash: prepared.SchemaHash,
		Ref:              prepared.Ref,
		AssetBytes:       prepared.Bytes,
	}, true
}

func findColumnHNSWSearchPackStateAsset(state columnVectorIndexStateSnapshot) (columnVectorIndexStateAssetSnapshot, bool, error) {
	var found columnVectorIndexStateAssetSnapshot
	seen := false
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleHNSWSearchPack {
			continue
		}
		if asset.AssetID != columnVectorIndexStateHNSWSearchPackAssetID {
			return columnVectorIndexStateAssetSnapshot{}, true, fmt.Errorf("collections: vector-index hnsw search pack asset id %q is not %q", asset.AssetID, columnVectorIndexStateHNSWSearchPackAssetID)
		}
		if seen {
			return columnVectorIndexStateAssetSnapshot{}, true, errors.New("collections: duplicate vector-index hnsw search pack asset")
		}
		found = asset
		seen = true
	}
	return found, seen, nil
}

func validateColumnHNSWSearchPackStateAssetIfPresent(rootDir string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) error {
	return validateColumnHNSWSearchPackStateAssetIfPresentWithMode(rootDir, cfg, def, graph, state, true)
}

func validateColumnHNSWSearchPackStateAssetIfPresentWithMode(rootDir string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, validatePayload bool) error {
	asset, found, err := findColumnHNSWSearchPackStateAsset(state)
	if err != nil || !found {
		return err
	}
	if cfg.AssetManager == nil {
		return errors.New("collections: vector-index hnsw search pack requires column asset manager")
	}
	if asset.SourceSchemaHash != cfg.SchemaHash {
		return fmt.Errorf("collections: vector-index hnsw search pack schema_hash=%d want %d", asset.SourceSchemaHash, cfg.SchemaHash)
	}
	if asset.RowCount != graph.RowCount || asset.Ref.Generation != graph.BaseManifestGeneration || asset.Ref.Namespace != cfg.AssetManager.Namespace || asset.Ref.Kind != ColumnAssetKindTCS1HNSWSearchPack || asset.AssetBytes != asset.Ref.Length {
		return fmt.Errorf("collections: vector-index hnsw search pack asset=%+v does not match graph/state", asset)
	}
	if !validatePayload {
		return nil
	}
	return validateColumnHNSWSearchPackAssetPayload(rootDir, asset.Ref, asset.AssetBytes, graph, def)
}

func validateColumnHNSWSearchPackAssetPayload(rootDir string, ref ColumnAssetRef, bytes int64, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition) error {
	return validateColumnHNSWSearchPackAssetPayloadWithManager(rootDir, ref, bytes, graph, def, mappedresource.NewManager())
}

func validateColumnHNSWSearchPackAssetPayloadWithManager(rootDir string, ref ColumnAssetRef, bytes int64, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition, manager *mappedresource.Manager) error {
	if manager == nil {
		return errors.New("collections: hnsw search pack validation requires mapped resource manager")
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return err
	}
	if bytes != ref.Length {
		return fmt.Errorf("collections: hnsw search pack bytes=%d ref=%d", bytes, ref.Length)
	}
	key := columnHNSWSearchPackMappedResourceKey(columnVectorIndexStateAssetSnapshot{
		Role:    columnVectorIndexStateAssetRoleHNSWSearchPack,
		AssetID: columnVectorIndexStateHNSWSearchPackAssetID,
		Ref:     ref,
	})
	handle, err := manager.AcquireFileRange(key, mappedresource.Scope{
		Kind:       mappedresource.ScopePreparedSearch,
		ID:         "hnsw_search_pack_v1/post_publish_validate",
		Namespace:  ref.Namespace,
		Generation: graph.BaseManifestGeneration,
		Reason:     "hnsw_search_pack_v1 post-publication validation",
	}, path, mappedresource.AcquireOptions{
		Reason:         "hnsw_search_pack_v1 post-publication validation",
		ValidationMode: mappedresource.ValidationVerify,
		PreferMapped:   true,
		AllowHeapCopy:  false,
		ResourceRoot:   rootDir,
		ResourcePath:   path,
	})
	if err != nil {
		return err
	}
	view, err := newColumnHNSWSearchPackPreparedViewFromHandle(manager, handle, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{
		ManifestGeneration: graph.BaseManifestGeneration,
		ManifestChecksum:   graph.BaseManifestChecksum,
		SchemaHash:         graph.BaseSchemaHash,
	}})
	if err != nil {
		return errors.Join(err, handle.Release())
	}
	validateErr := validateColumnHNSWSearchPackPreparedView(view, graph, def)
	return errors.Join(validateErr, view.Close())
}

func validateColumnHNSWSearchPackPreparedView(pack *columnHNSWSearchPackPreparedView, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition) error {
	if pack == nil {
		return errors.New("collections: hnsw search pack prepared view is nil")
	}
	if pack.Header.Rows != graph.RowCount || pack.Header.Dimensions != def.Dimensions || pack.Header.M != def.M || pack.Header.EfConstruction != def.EfConstruction || pack.Header.EfSearch != def.EfSearch {
		return fmt.Errorf("collections: hnsw_search_pack_v1 header rows/dims/M/ef=(%d,%d,%d,%d,%d) want (%d,%d,%d,%d,%d)", pack.Header.Rows, pack.Header.Dimensions, pack.Header.M, pack.Header.EfConstruction, pack.Header.EfSearch, graph.RowCount, def.Dimensions, def.M, def.EfConstruction, def.EfSearch)
	}
	if graph.RowCount == 0 {
		if pack.Header.EntryOrdinal != -1 || pack.Header.MaxLayer != -1 || pack.Header.AdjacencyLayerCount != 0 {
			return fmt.Errorf("collections: hnsw_search_pack_v1 empty header entry/max/layers=(%d,%d,%d)", pack.Header.EntryOrdinal, pack.Header.MaxLayer, pack.Header.AdjacencyLayerCount)
		}
		return nil
	}
	if pack.Header.EntryOrdinal < 0 || pack.Header.EntryOrdinal >= graph.RowCount || pack.Header.MaxLayer < 0 || pack.Header.AdjacencyLayerCount != pack.Header.MaxLayer+1 {
		return fmt.Errorf("collections: hnsw_search_pack_v1 invalid entry/max/layers=(%d,%d,%d) rows=%d", pack.Header.EntryOrdinal, pack.Header.MaxLayer, pack.Header.AdjacencyLayerCount, graph.RowCount)
	}
	return nil
}
