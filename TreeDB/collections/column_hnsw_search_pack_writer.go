package collections

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	internalcrc "github.com/snissn/gomap/TreeDB/internal/crc"
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
		if errors.Is(err, mappedresource.ErrMmapUnsupported) {
			return validateColumnHNSWSearchPackAssetPayloadDirectFile(path, ref, graph, def)
		}
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

// validateColumnHNSWSearchPackAssetPayloadDirectFile is the bounded fallback
// for platforms which explicitly cannot mmap. It reads only the fixed header
// and directory, then streams checksums over the referenced file range.
func validateColumnHNSWSearchPackAssetPayloadDirectFile(path string, ref ColumnAssetRef, graph columnVectorGraphManifestSnapshot, def VectorIndexDefinition) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if ref.Offset < 0 || ref.Length < columnHNSWSearchPackHeaderSize || ref.Offset > info.Size() || ref.Length > info.Size()-ref.Offset {
		return fmt.Errorf("collections: hnsw search pack file range offset=%d length=%d outside size=%d", ref.Offset, ref.Length, info.Size())
	}
	prefix := make([]byte, columnHNSWSearchPackHeaderSizeV2)
	if err := readColumnHNSWSearchPackFileAt(file, ref.Offset, prefix[:columnHNSWSearchPackHeaderSize]); err != nil {
		return err
	}
	headerSize := columnHNSWSearchPackHeaderSize
	switch hnswPackU16(prefix, columnHNSWSearchPackHeaderVersionOffset) {
	case columnHNSWSearchPackVersionV1:
	case columnHNSWSearchPackVersionV2, columnHNSWSearchPackVersionV3:
		headerSize = columnHNSWSearchPackHeaderSizeV2
		if err := readColumnHNSWSearchPackFileAt(file, ref.Offset+columnHNSWSearchPackHeaderSize, prefix[columnHNSWSearchPackHeaderSize:headerSize]); err != nil {
			return err
		}
	}
	directoryLength := hnswPackU64(prefix, columnHNSWSearchPackHeaderDirectoryLengthOffset)
	if directoryLength > uint64(ref.Length-int64(headerSize)) || directoryLength > uint64(math.MaxInt-headerSize) {
		return fmt.Errorf("collections: hnsw search pack directory length=%d outside asset", directoryLength)
	}
	metadata := make([]byte, headerSize+int(directoryLength))
	if err := readColumnHNSWSearchPackFileAt(file, ref.Offset, metadata); err != nil {
		return err
	}
	pack, _, err := decodeColumnHNSWSearchPackEnvelopeMetadataWithContext(nil, metadata, uint64(ref.Length), columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{
		ManifestGeneration: graph.BaseManifestGeneration,
		ManifestChecksum:   graph.BaseManifestChecksum,
		SchemaHash:         graph.BaseSchemaHash,
	}}, false)
	if err != nil {
		return err
	}
	if checksum, err := columnHNSWSearchPackFileRangeChecksum(file, ref.Offset, ref.Length); err != nil {
		return err
	} else if checksum != uint32(ref.Checksum) {
		return fmt.Errorf("collections: hnsw_search_pack_v1 checksum=%08x want %08x", checksum, uint32(ref.Checksum))
	}
	for _, section := range pack.Sections {
		if checksum, err := columnHNSWSearchPackFileRangeChecksum(file, ref.Offset+int64(section.Offset), int64(section.Length)); err != nil {
			return err
		} else if checksum != section.Checksum {
			return fmt.Errorf("collections: hnsw_search_pack_v1 section %s[%d] checksum=%08x want %08x", section.Kind, section.Index, checksum, section.Checksum)
		}
	}
	if err := validateColumnHNSWSearchPackDirectFileSections(file, ref.Offset, pack, graph); err != nil {
		return err
	}
	return validateColumnHNSWSearchPackPreparedView(&columnHNSWSearchPackPreparedView{Header: pack.Header, Sections: pack.Sections}, graph, def)
}

// validateColumnHNSWSearchPackDirectFileSections mirrors the content checks in
// prepareSectionViews without retaining corpus-sized sections in memory.
func validateColumnHNSWSearchPackDirectFileSections(file *os.File, baseOffset int64, pack columnHNSWSearchPack, graph columnVectorGraphManifestSnapshot) error {
	rows := uint64(pack.Header.Rows)
	vectorCount, ok := checkedHNSWPackMulOK(rows, uint64(pack.Header.VectorStride))
	if !ok {
		return errors.New("collections: hnsw_search_pack_v1 normalized vector count overflows uint64")
	}
	vectors, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionNormalizedVectors, 0, vectorCount, 4)
	if err != nil {
		return err
	}
	if err := validateColumnHNSWSearchPackDirectFiniteFloat32(file, baseOffset+int64(vectors.Offset), vectors.Length); err != nil {
		return err
	}
	levels, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionLevels, 0, rows, 2)
	if err != nil {
		return err
	}
	if err := validateColumnHNSWSearchPackDirectLevels(file, baseOffset+int64(levels.Offset), levels.Length, pack.Header.MaxLayer); err != nil {
		return err
	}
	for layer := 0; layer < pack.Header.AdjacencyLayerCount; layer++ {
		offsets, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionAdjacencyOffsets, uint16(layer), rows+1, 8)
		if err != nil {
			return err
		}
		neighbors, err := columnHNSWSearchPackFindSection(pack.Sections, columnHNSWSearchPackSectionAdjacencyNeighbors, uint16(layer))
		if err != nil {
			return err
		}
		if neighbors.Length%4 != 0 || neighbors.Count != neighbors.Length/4 || neighbors.Count > (columnHNSWSearchPackDecodeOptions{}).withDefaults().MaxNeighbors {
			return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbors length/count mismatch", layer)
		}
		if err := validateColumnHNSWSearchPackDirectAdjacency(file, baseOffset, layer, rows, offsets, neighbors); err != nil {
			return err
		}
	}
	if pack.Header.HasAuxiliaryNavigation {
		offsets, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionAuxiliaryOffsets, 0, rows+1, 8)
		if err != nil {
			return err
		}
		neighbors, err := columnHNSWSearchPackFindSection(pack.Sections, columnHNSWSearchPackSectionAuxiliaryNeighbors, 0)
		if err != nil {
			return err
		}
		maxNeighbors := uint64(0)
		if rows > 1 {
			maxNeighbors = (rows - 1) * 2
		}
		if neighbors.Length%4 != 0 || neighbors.Count != neighbors.Length/4 || neighbors.Count > maxNeighbors {
			return errors.New("collections: hnsw_search_pack_v1 auxiliary neighbors shape")
		}
		if err := validateColumnHNSWSearchPackDirectAdjacency(file, baseOffset, -1, rows, offsets, neighbors); err != nil {
			return err
		}
	}
	for _, kind := range [...]columnHNSWSearchPackSectionKind{columnHNSWSearchPackSectionRowRefGeneration, columnHNSWSearchPackSectionRowRefPartID, columnHNSWSearchPackSectionRowRefRowIndex, columnHNSWSearchPackSectionRowRefAppliedLSN} {
		section, err := columnHNSWSearchPackRequireSection(pack.Sections, kind, 0, rows, 8)
		if err != nil {
			return err
		}
		if err := validateColumnHNSWSearchPackDirectRowRef(file, baseOffset+int64(section.Offset), section.Length, kind, pack.Header.BaseManifestGeneration); err != nil {
			return err
		}
	}
	docOffsets, err := columnHNSWSearchPackRequireSection(pack.Sections, columnHNSWSearchPackSectionDocumentIDOffsets, 0, rows+1, 8)
	if err != nil {
		return err
	}
	docBytes, err := columnHNSWSearchPackFindSection(pack.Sections, columnHNSWSearchPackSectionDocumentIDBytes, 0)
	if err != nil {
		return err
	}
	if docBytes.Count != docBytes.Length || docBytes.Length > (columnHNSWSearchPackDecodeOptions{}).withDefaults().MaxDocumentIDBytes {
		return errors.New("collections: hnsw_search_pack_v1 document_id_bytes length/count mismatch")
	}
	return validateColumnHNSWSearchPackDirectDocumentOffsets(file, baseOffset+int64(docOffsets.Offset), docOffsets.Length, rows, docBytes.Length)
}

func validateColumnHNSWSearchPackDirectFiniteFloat32(file *os.File, offset int64, length uint64) error {
	buffer := make([]byte, 64<<10)
	for index := uint64(0); length > 0; {
		chunk := min(uint64(len(buffer)), length)
		if err := readColumnHNSWSearchPackFileAt(file, offset, buffer[:chunk]); err != nil {
			return err
		}
		for i := uint64(0); i < chunk; i += 4 {
			if value := math.Float32frombits(binary.LittleEndian.Uint32(buffer[i:])); math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return fmt.Errorf("collections: hnsw_search_pack_v1 normalized vector[%d] is not finite", index+i/4)
			}
		}
		offset += int64(chunk)
		length -= chunk
		index += chunk / 4
	}
	return nil
}

func validateColumnHNSWSearchPackDirectLevels(file *os.File, offset int64, length uint64, maxLayer int) error {
	buffer := make([]byte, 64<<10)
	for index := uint64(0); length > 0; {
		chunk := min(uint64(len(buffer)), length)
		if err := readColumnHNSWSearchPackFileAt(file, offset, buffer[:chunk]); err != nil {
			return err
		}
		for i := uint64(0); i < chunk; i += 2 {
			if level := int(binary.LittleEndian.Uint16(buffer[i:])); level > maxLayer {
				return fmt.Errorf("collections: hnsw_search_pack_v1 level[%d]=%d exceeds max_layer=%d", index+i/2, level, maxLayer)
			}
		}
		offset += int64(chunk)
		length -= chunk
		index += chunk / 2
	}
	return nil
}

func validateColumnHNSWSearchPackDirectAdjacency(file *os.File, baseOffset int64, layer int, rows uint64, offsets, neighbors columnHNSWSearchPackSection) error {
	buffer := make([]byte, 64<<10)
	var previous uint64
	for index := uint64(0); index < offsets.Count; {
		chunk := min(uint64(len(buffer)), (offsets.Count-index)*8)
		if err := readColumnHNSWSearchPackFileAt(file, baseOffset+int64(offsets.Offset+index*8), buffer[:chunk]); err != nil {
			return err
		}
		for i := uint64(0); i < chunk/8; i++ {
			value := binary.LittleEndian.Uint64(buffer[i*8:])
			if index+i == 0 && value != 0 {
				return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d first offset=%d want 0", layer, value)
			}
			if index+i > 0 && value < previous {
				return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d offsets are not monotonic at row=%d", layer, index+i-1)
			}
			previous = value
		}
		index += chunk / 8
	}
	if previous != neighbors.Count {
		return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d last offset=%d want neighbors=%d", layer, previous, neighbors.Count)
	}
	for index := uint64(0); index < neighbors.Count; {
		chunk := min(uint64(len(buffer)), (neighbors.Count-index)*4)
		if err := readColumnHNSWSearchPackFileAt(file, baseOffset+int64(neighbors.Offset+index*4), buffer[:chunk]); err != nil {
			return err
		}
		for i := uint64(0); i < chunk/4; i++ {
			if neighbor := binary.LittleEndian.Uint32(buffer[i*4:]); uint64(neighbor) >= rows {
				return fmt.Errorf("collections: hnsw_search_pack_v1 adjacency layer=%d neighbor ordinal[%d]=%d outside rows=%d", layer, index+i, neighbor, rows)
			}
		}
		index += chunk / 4
	}
	return nil
}

func validateColumnHNSWSearchPackDirectRowRef(file *os.File, offset int64, length uint64, kind columnHNSWSearchPackSectionKind, baseGeneration uint64) error {
	buffer := make([]byte, 64<<10)
	for index := uint64(0); length > 0; {
		chunk := min(uint64(len(buffer)), length)
		if err := readColumnHNSWSearchPackFileAt(file, offset, buffer[:chunk]); err != nil {
			return err
		}
		for i := uint64(0); i < chunk/8; i++ {
			value := int64(binary.LittleEndian.Uint64(buffer[i*8:]))
			if (kind == columnHNSWSearchPackSectionRowRefRowIndex && value < 0) || (kind != columnHNSWSearchPackSectionRowRefRowIndex && value <= 0) {
				return fmt.Errorf("collections: hnsw_search_pack_v1 invalid row-ref ordinal=%d", index+i)
			}
			if kind == columnHNSWSearchPackSectionRowRefGeneration && uint64(value) > baseGeneration {
				return fmt.Errorf("collections: hnsw_search_pack_v1 row-ref generation=%d exceeds base generation=%d", value, baseGeneration)
			}
		}
		offset += int64(chunk)
		length -= chunk
		index += chunk / 8
	}
	return nil
}

func validateColumnHNSWSearchPackDirectDocumentOffsets(file *os.File, offset int64, length, rows, values uint64) error {
	buffer := make([]byte, 64<<10)
	var previous uint64
	for index := uint64(0); index <= rows; {
		chunk := min(uint64(len(buffer)), (rows+1-index)*8)
		if err := readColumnHNSWSearchPackFileAt(file, offset+int64(index*8), buffer[:chunk]); err != nil {
			return err
		}
		for i := uint64(0); i < chunk/8; i++ {
			value := binary.LittleEndian.Uint64(buffer[i*8:])
			if index+i == 0 && value != 0 {
				return fmt.Errorf("collections: hnsw_search_pack_v1 document id first offset=%d want 0", value)
			}
			if index+i > 0 && value < previous {
				return fmt.Errorf("collections: hnsw_search_pack_v1 document id offsets are not monotonic at row=%d", index+i-1)
			}
			if index+i > 0 && value == previous {
				return fmt.Errorf("collections: hnsw_search_pack_v1 document id row=%d is empty", index+i-1)
			}
			previous = value
		}
		index += chunk / 8
	}
	if previous != values {
		return fmt.Errorf("collections: hnsw_search_pack_v1 document id last offset=%d want bytes=%d", previous, values)
	}
	return nil
}

func readColumnHNSWSearchPackFileAt(file *os.File, offset int64, dst []byte) error {
	for len(dst) > 0 {
		n, err := file.ReadAt(dst, offset)
		offset += int64(n)
		dst = dst[n:]
		if err != nil {
			if err == io.EOF && len(dst) == 0 {
				return nil
			}
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func columnHNSWSearchPackFileRangeChecksum(file *os.File, offset, length int64) (uint32, error) {
	if offset < 0 || length < 0 {
		return 0, errors.New("collections: negative hnsw search pack checksum range")
	}
	var checksum uint32
	buffer := make([]byte, 64<<10)
	for length > 0 {
		chunk := int64(len(buffer))
		if length < chunk {
			chunk = length
		}
		if err := readColumnHNSWSearchPackFileAt(file, offset, buffer[:chunk]); err != nil {
			return 0, err
		}
		checksum = internalcrc.Update(checksum, buffer[:chunk])
		offset += chunk
		length -= chunk
	}
	return checksum, nil
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
