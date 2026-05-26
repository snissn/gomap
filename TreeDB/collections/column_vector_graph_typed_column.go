package collections

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

const columnVectorGraphTypedColumnVectorScopeID = "column-vector-graph-typed-column-vector"

type columnVectorGraphTypedColumnVectorSource struct {
	field  TypedStorageField
	column typedColumnAdapterColumn
	dims   int

	locations []columnVectorGraphTypedColumnVectorLocation
	parts     []*columnVectorGraphTypedColumnVectorPart
	manager   *mappedresource.Manager

	decodedDerivedBytes uint64
	mappedBytes         uint64
	heapCopyBytes       uint64
	activeHandles       int64
	deniedResources     uint64
}

type columnVectorGraphTypedColumnVectorLocation struct {
	part     *columnVectorGraphTypedColumnVectorPart
	rowIndex int
}

type columnVectorGraphTypedColumnVectorPart struct {
	generation uint64
	partID     uint64
	rows       int
	values     []float32
	direct     bool
	handle     *mappedresource.Handle
}

type columnVectorGraphTypedColumnPhysicalLocation struct {
	generation uint64
	partID     uint64
	rowIndex   int
}

func (c *Collection) openColumnVectorGraphTypedColumnVectorSourceForReader(catalog *collectionCatalog, cfg ColumnStoreConfig, manifest columnManifestSnapshot, records []columnManifestRecord, graph columnVectorGraphManifestSnapshot, reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphTypedColumnVectorSource, string) {
	source, err := c.newColumnVectorGraphTypedColumnVectorSource(catalog, cfg, manifest, records, graph, reader)
	if err != nil {
		return nil, err.Error()
	}
	return source, ""
}

func (c *Collection) newColumnVectorGraphTypedColumnVectorSource(catalog *collectionCatalog, cfg ColumnStoreConfig, manifest columnManifestSnapshot, records []columnManifestRecord, graph columnVectorGraphManifestSnapshot, reader *columnVectorGraphPhysicalRowReader) (*columnVectorGraphTypedColumnVectorSource, error) {
	if c == nil {
		return nil, errCollectionNil
	}
	if c.db == nil {
		return nil, errCollectionDBNil
	}
	if catalog == nil {
		return nil, errCollectionNotFound
	}
	field, adapterColumn, ok, err := columnVectorGraphTypedColumnVectorField(cfg, graph.Field, graph.Dimensions)
	if err != nil || !ok {
		return nil, err
	}
	if graph.RowCount == 0 {
		return &columnVectorGraphTypedColumnVectorSource{field: field, column: adapterColumn, dims: graph.Dimensions, manager: mappedresource.NewManager()}, nil
	}
	if reader == nil || reader.reader == nil {
		return nil, errNilColumnVectorGraphPhysicalRowReader
	}
	if graph.BaseManifestGeneration != manifest.Generation || graph.BaseManifestGeneration != cfg.ActiveManifest.Generation || graph.BaseSchemaHash != cfg.SchemaHash {
		return nil, fmt.Errorf("collections: column_graph %q typed-column vector source stale graph/base identity", graph.IndexName)
	}
	physicalRefs, mutationParts, err := columnManifestAssetRefsFromRecordsForScan(records, manifest.Generation, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	if mutationParts != 0 {
		return nil, errors.New("collections: column_graph typed-column vector source requires insert-only base physical refs")
	}
	typedRefs, err := typedColumnPartRefsByGenerationFromManifestRecords(records, cfg.AssetManager.Namespace)
	if err != nil {
		return nil, err
	}
	if len(typedRefs) == 0 {
		return nil, errors.New("collections: column_graph typed-column vector source missing typed_column_part refs")
	}

	graphIDs, err := columnVectorGraphDocumentIDsFromReader(reader)
	if err != nil {
		return nil, err
	}
	if len(graphIDs) != graph.RowCount {
		return nil, fmt.Errorf("collections: column_graph %q typed-column vector source graph ids=%d want row_count=%d", graph.IndexName, len(graphIDs), graph.RowCount)
	}
	physicalLocations, physicalRowsByGeneration, err := c.columnVectorGraphTypedColumnPhysicalLocations(catalog.meta.Name, cfg, physicalRefs)
	if err != nil {
		return nil, err
	}
	locations := make([]columnVectorGraphTypedColumnVectorLocation, len(graphIDs))
	// The #1782 vector source is intentionally limited to the current insert-only
	// publication shape: one physical row asset and one typed_column_part per
	// manifest generation. Multipart lifecycle/compaction is deferred to #1787;
	// columnVectorGraphTypedColumnPhysicalLocations and
	// typedColumnPartRefsByGenerationFromManifestRecords fail closed if the
	// manifest violates that shape.
	usedGenerations := make(map[uint64]struct{})
	for ordinal, id := range graphIDs {
		loc, ok := physicalLocations[string(id)]
		if !ok {
			return nil, fmt.Errorf("collections: column_graph %q typed-column vector source missing physical row for graph ordinal=%d id=%q", graph.IndexName, ordinal, string(id))
		}
		if _, ok := typedRefs[loc.generation]; !ok {
			return nil, fmt.Errorf("collections: column_graph %q typed-column vector source missing typed_column_part for generation=%d", graph.IndexName, loc.generation)
		}
		usedGenerations[loc.generation] = struct{}{}
		locations[ordinal].rowIndex = loc.rowIndex
	}

	source := &columnVectorGraphTypedColumnVectorSource{
		field:     field,
		column:    adapterColumn,
		dims:      graph.Dimensions,
		locations: locations,
		manager:   mappedresource.NewManager(),
	}
	success := false
	defer func() {
		if !success {
			_ = source.Close()
		}
	}()
	partsByGeneration := make(map[uint64]*columnVectorGraphTypedColumnVectorPart, len(usedGenerations))
	for generation := range usedGenerations {
		typedRef := typedRefs[generation]
		physicalRows, ok := physicalRowsByGeneration[generation]
		if !ok {
			return nil, fmt.Errorf("collections: column_graph %q typed-column vector source missing physical rows for generation=%d", graph.IndexName, generation)
		}
		part, decodedBytes, loadErr := c.loadColumnVectorGraphTypedColumnVectorPart(catalog.meta.Name, cfg, typedRef, physicalRows, field, adapterColumn, source.manager)
		if loadErr != nil {
			return nil, fmt.Errorf("collections: column_graph %q typed-column vector source load generation=%d part_id=%d: %w", graph.IndexName, typedRef.Ref.Generation, typedRef.Ref.PartID, loadErr)
		}
		source.parts = append(source.parts, part)
		partsByGeneration[generation] = part
		source.decodedDerivedBytes += decodedBytes
	}
	for ordinal, id := range graphIDs {
		loc := physicalLocations[string(id)]
		part := partsByGeneration[loc.generation]
		if part == nil {
			return nil, fmt.Errorf("collections: column_graph %q typed-column vector source missing loaded part for graph ordinal=%d generation=%d", graph.IndexName, ordinal, loc.generation)
		}
		if loc.rowIndex < 0 || loc.rowIndex >= part.rows {
			return nil, fmt.Errorf("collections: column_graph %q typed-column vector source row_index=%d outside typed_column_part rows=%d", graph.IndexName, loc.rowIndex, part.rows)
		}
		locations[ordinal].part = part
	}
	source.captureResourceStats()
	success = true
	return source, nil
}

func columnVectorGraphTypedColumnVectorField(cfg ColumnStoreConfig, fieldPath string, dims int) (TypedStorageField, typedColumnAdapterColumn, bool, error) {
	if cfg.AssetManager == nil {
		return TypedStorageField{}, typedColumnAdapterColumn{}, false, errors.New("collections: column_graph typed-column vector source requires column asset manager metadata")
	}
	fields := columnStoreTypedColumnPartFields(cfg)
	if len(fields) == 0 {
		return TypedStorageField{}, typedColumnAdapterColumn{}, false, nil
	}
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return TypedStorageField{}, typedColumnAdapterColumn{}, false, err
	}
	for i, field := range fields {
		if field.Path != fieldPath && field.Name != fieldPath {
			continue
		}
		column := columns[i]
		if field.Owner != TypedStorageOwnerColumnPart {
			return TypedStorageField{}, typedColumnAdapterColumn{}, false, nil
		}
		if field.ValueType != ColumnStoreValueFloat32Vector || column.Definition.Type != typedcolumn.ColumnTypeFloat32Vector || column.Definition.Encoding != typedcolumn.EncodingRawFloat32Vector {
			return TypedStorageField{}, typedColumnAdapterColumn{}, false, fmt.Errorf("collections: column_graph typed-column vector field %q is not raw float32_vector", fieldPath)
		}
		if field.Nullable {
			return TypedStorageField{}, typedColumnAdapterColumn{}, false, fmt.Errorf("collections: column_graph typed-column vector field %q is nullable", fieldPath)
		}
		if field.VectorDims != dims || column.Definition.FixedWidthElements != dims {
			return TypedStorageField{}, typedColumnAdapterColumn{}, false, fmt.Errorf("collections: column_graph typed-column vector field %q dims=%d fixed_width=%d want %d", fieldPath, field.VectorDims, column.Definition.FixedWidthElements, dims)
		}
		return field, column, true, nil
	}
	return TypedStorageField{}, typedColumnAdapterColumn{}, false, nil
}

func columnVectorGraphDocumentIDsFromReader(reader *columnVectorGraphPhysicalRowReader) ([][]byte, error) {
	physical, err := reader.rowReader()
	if err != nil {
		return nil, err
	}
	ids := make([][]byte, physical.totalRows)
	for _, rowRange := range physical.ranges {
		block, err := physical.loadBlock(rowRange)
		if err != nil {
			return nil, err
		}
		for rowIndex := range block.rowOffsets {
			ordinal := rowRange.startOrdinal + rowIndex
			cur := manifestCursor{raw: block.raw, pos: block.rowOffsets[rowIndex]}
			id := cur.bytesView()
			deleted := false
			if block.version >= columnPhysicalAssetVersionV2 {
				deleted = cur.bool()
			}
			if cur.err != nil {
				return nil, cur.err
			}
			if len(id) == 0 {
				return nil, fmt.Errorf("collections: column_graph %q ordinal=%d missing document id", reader.def.Name, ordinal)
			}
			if deleted {
				return nil, fmt.Errorf("collections: column_graph %q ordinal=%d row is deleted", reader.def.Name, ordinal)
			}
			ids[ordinal] = bytes.Clone(id)
		}
	}
	return ids, nil
}

func (c *Collection) columnVectorGraphTypedColumnPhysicalLocations(collection string, cfg ColumnStoreConfig, refs []columnManifestAssetRefForScan) (map[string]columnVectorGraphTypedColumnPhysicalLocation, map[uint64]int, error) {
	if len(refs) == 0 {
		return nil, nil, errors.New("collections: column_graph typed-column vector source missing physical row refs")
	}
	rowCfg := columnStoreRowAssetConfig(cfg)
	projection := columnPhysicalScanProjection{outputByColumn: make([]int, len(rowCfg.Columns))}
	for i := range projection.outputByColumn {
		projection.outputByColumn[i] = -1
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = readCache.close() }()
	locations := make(map[string]columnVectorGraphTypedColumnPhysicalLocation)
	rowsByGeneration := make(map[uint64]int)
	var scratch []byte
	for _, asset := range refs {
		if asset.Ref.Kind != ColumnAssetKindTCS1PartImage {
			return nil, nil, fmt.Errorf("collections: column_graph typed-column vector source physical ref kind=%q", asset.Ref.Kind)
		}
		if asset.Reason != ColumnPublishOperationInsert {
			return nil, nil, fmt.Errorf("collections: column_graph typed-column vector source requires insert-only physical refs, got %s", asset.Reason)
		}
		if _, exists := rowsByGeneration[asset.Ref.Generation]; exists {
			return nil, nil, fmt.Errorf("collections: column_graph typed-column vector source generation=%d has multiple physical row parts; multipart typed-column vector graph reads are deferred", asset.Ref.Generation)
		}
		raw, err := readCache.read(asset.Ref, scratch)
		if err != nil {
			return nil, nil, err
		}
		scratch = raw
		rowsByGeneration[asset.Ref.Generation] = asset.Rows
		_, err = scanColumnPhysicalAssetRowsWithManifestOperation(raw, asset.Ref, collection, &rowCfg, projection, ColumnPublishOperationInsert, func(row columnPhysicalScanRowView) error {
			if row.Deleted {
				return fmt.Errorf("collections: column_graph typed-column vector source physical row[%d] is deleted", row.RowIndex)
			}
			key := string(row.ID)
			if _, exists := locations[key]; exists {
				return fmt.Errorf("collections: column_graph typed-column vector source duplicate document id %q", key)
			}
			locations[key] = columnVectorGraphTypedColumnPhysicalLocation{generation: row.Generation, partID: row.PartID, rowIndex: row.RowIndex}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}
	return locations, rowsByGeneration, nil
}

func (c *Collection) loadColumnVectorGraphTypedColumnVectorPart(collection string, cfg ColumnStoreConfig, typedRef columnManifestAssetRefForScan, physicalRows int, field TypedStorageField, adapterColumn typedColumnAdapterColumn, manager *mappedresource.Manager) (*columnVectorGraphTypedColumnVectorPart, uint64, error) {
	if typedRef.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
		return nil, 0, fmt.Errorf("typed ref kind=%q want %q", typedRef.Ref.Kind, ColumnAssetKindTCS1TypedColumnPart)
	}
	if typedRef.Reason != ColumnPublishOperationInsert {
		return nil, 0, fmt.Errorf("typed_column_part reason=%s want insert", typedRef.Reason)
	}
	if typedRef.Rows != physicalRows {
		return nil, 0, fmt.Errorf("typed_column_part rows=%d physical_rows=%d", typedRef.Rows, physicalRows)
	}
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(c.db.ColumnAssetRootDir(), cfg.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = readCache.close() }()
	raw, err := readCache.read(typedRef.Ref, nil)
	if err != nil {
		return nil, 0, err
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, 0, err
	}
	if image.PartID != typedRef.Ref.PartID || image.Rows != typedRef.Rows {
		return nil, 0, fmt.Errorf("typed_column_part image/ref mismatch image_part=%d ref_part=%d image_rows=%d ref_rows=%d", image.PartID, typedRef.Ref.PartID, image.Rows, typedRef.Rows)
	}
	adapterPart, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: []TypedStorageField{field}}, image)
	if err != nil {
		return nil, 0, err
	}
	if adapterPart.Part.Descriptor.SchemaVersion != uint32(cfg.SchemaHash) {
		return nil, 0, fmt.Errorf("typed_column_part schema_version=%d want %d", adapterPart.Part.Descriptor.SchemaVersion, uint32(cfg.SchemaHash))
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return nil, 0, fmt.Errorf("typed_column_part dense vector layout certification: %w", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return nil, 0, fmt.Errorf("typed_column_part missing dense vector layout certification for column %q", adapterColumn.Definition.Name)
	}
	section, err := columnVectorGraphTypedColumnVectorSection(image, adapterColumn.Definition.Name)
	if err != nil {
		return nil, 0, err
	}
	if err := validateColumnVectorGraphTypedColumnDenseVectorSection(adapterPart.Part, section, certColumn, adapterColumn.Definition.Name, typedRef.Rows, adapterColumn.Definition.FixedWidthElements); err != nil {
		return nil, 0, err
	}
	sectionBytes, err := image.SectionBytes(section)
	if err != nil {
		return nil, 0, err
	}
	sectionChecksum := page.Checksum(sectionBytes)
	values, handle, direct, scratchDecode, err := c.acquireColumnVectorGraphTypedColumnDenseVectorValues(collection, typedRef.Ref, image.Version, section, certColumn, sectionChecksum, typedRef.Rows, adapterColumn.Definition.FixedWidthElements, manager)
	if err != nil {
		return nil, 0, err
	}
	decodedBytes := uint64(image.ManifestBytes)
	if scratchDecode {
		decodedBytes += uint64(len(values)) * 4
	}
	return &columnVectorGraphTypedColumnVectorPart{
		generation: typedRef.Ref.Generation,
		partID:     typedRef.Ref.PartID,
		rows:       typedRef.Rows,
		values:     values,
		direct:     direct,
		handle:     handle,
	}, decodedBytes, nil
}

func columnVectorGraphTypedColumnVectorSection(image typedcolumn.ColumnPartImage, column string) (typedcolumn.ColumnPartImageSection, error) {
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == column {
			return section, nil
		}
	}
	return typedcolumn.ColumnPartImageSection{}, fmt.Errorf("typed_column_part missing column data section %q", column)
}

func validateColumnVectorGraphTypedColumnDenseVectorSection(part *typedcolumn.ColumnPart, section typedcolumn.ColumnPartImageSection, certColumn typedcolumn.ColumnPartLayoutContractColumn, columnName string, rows, dims int) error {
	if part == nil {
		return errors.New("nil typed_column_part")
	}
	if rows <= 0 {
		return fmt.Errorf("typed_column_part rows=%d", rows)
	}
	if dims <= 0 {
		return fmt.Errorf("typed_column_part vector dims=%d", dims)
	}
	if part.Descriptor.RowCount != rows {
		return fmt.Errorf("typed_column_part descriptor rows=%d want %d", part.Descriptor.RowCount, rows)
	}
	column, ok := part.Columns[columnName]
	if !ok {
		return fmt.Errorf("typed_column_part missing vector column %q", columnName)
	}
	if column.Definition.Type != typedcolumn.ColumnTypeFloat32Vector || column.Definition.Encoding != typedcolumn.EncodingRawFloat32Vector || column.Definition.Compression != typedcolumn.CompressionNone || column.Definition.FixedWidthElements != dims {
		return fmt.Errorf("typed_column_part vector column %q schema mismatch type=%s encoding=%s compression=%s fixed_width_elements=%d", columnName, column.Definition.Type, column.Definition.Encoding, column.Definition.Compression, column.Definition.FixedWidthElements)
	}
	wantBytes, err := columnVectorGraphTypedColumnDenseByteLen(rows, dims)
	if err != nil {
		return err
	}
	if section.Kind != typedcolumn.ColumnPartImageSectionColumnData || section.Category != typedcolumn.ColumnPartImageCategoryDeclaredColumns || section.Column != columnName {
		return fmt.Errorf("typed_column_part vector section identity mismatch kind=%s category=%s column=%q", section.Kind, section.Category, section.Column)
	}
	if section.Encoding != typedcolumn.EncodingRawFloat32Vector || section.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("typed_column_part vector section encoding=%s compression=%s", section.Encoding, section.Compression)
	}
	if section.Rows != rows || section.Length != wantBytes {
		return fmt.Errorf("typed_column_part vector section rows=%d length=%d want rows=%d length=%d", section.Rows, section.Length, rows, wantBytes)
	}
	plan := typeddecode.DenseFloat32VectorPlan(certColumn, dims)
	status := typeddecode.ValidateDirectViewColumn(typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: rows, PayloadBytes: section.Length, AssetOffset: int64(0), HasAssetOffset: true})
	if !status.Direct() {
		return fmt.Errorf("typed_column_part vector direct-view validation failed: %s", status.String())
	}
	nextRow := 0
	totalBytes := 0
	for i, block := range column.Blocks {
		if block.Descriptor.FirstRow != nextRow {
			return fmt.Errorf("typed_column_part vector block %d first_row=%d want %d", i, block.Descriptor.FirstRow, nextRow)
		}
		blockBytes, err := columnVectorGraphTypedColumnDenseByteLen(block.Descriptor.RowCount, dims)
		if err != nil {
			return err
		}
		if block.Descriptor.Encoding != typedcolumn.EncodingRawFloat32Vector || block.Descriptor.Compression != typedcolumn.CompressionNone || block.Descriptor.RawBytes != blockBytes || block.Descriptor.StoredBytes != blockBytes {
			return fmt.Errorf("typed_column_part vector block %d descriptor mismatch encoding=%s compression=%s raw=%d stored=%d want_bytes=%d", i, block.Descriptor.Encoding, block.Descriptor.Compression, block.Descriptor.RawBytes, block.Descriptor.StoredBytes, blockBytes)
		}
		g := block.Granule
		if g.Encoding != typedcolumn.EncodingRawFloat32Vector || g.Compression != typedcolumn.CompressionNone || g.Rows != block.Descriptor.RowCount || g.RawBytes != blockBytes || g.StoredBytes != blockBytes || len(g.Payload) != blockBytes || g.NullCount != 0 || g.DefaultCount != 0 || g.HasMinMax {
			return fmt.Errorf("typed_column_part vector block %d granule mismatch", i)
		}
		nextRow += block.Descriptor.RowCount
		totalBytes += blockBytes
	}
	if nextRow != rows || totalBytes != section.Length {
		return fmt.Errorf("typed_column_part vector blocks rows=%d bytes=%d want rows=%d bytes=%d", nextRow, totalBytes, rows, section.Length)
	}
	return nil
}

func columnVectorGraphTypedColumnDenseByteLen(rows, dims int) (int, error) {
	if rows < 0 || dims <= 0 {
		return 0, fmt.Errorf("typed_column_part dense vector invalid shape rows=%d dims=%d", rows, dims)
	}
	if rows != 0 && dims > math.MaxInt/rows {
		return 0, errors.New("typed_column_part dense vector elements overflow")
	}
	elements := rows * dims
	if elements != 0 && elements > math.MaxInt/4 {
		return 0, errors.New("typed_column_part dense vector bytes overflow")
	}
	return elements * 4, nil
}

func (c *Collection) acquireColumnVectorGraphTypedColumnDenseVectorValues(collection string, ref ColumnAssetRef, imageVersion uint16, section typedcolumn.ColumnPartImageSection, certColumn typedcolumn.ColumnPartLayoutContractColumn, sectionChecksum uint32, rows, dims int, manager *mappedresource.Manager) ([]float32, *mappedresource.Handle, bool, bool, error) {
	if manager == nil {
		return nil, nil, false, false, errors.New("nil mappedresource manager")
	}
	path, err := columnAssetSegmentPath(c.db.ColumnAssetRootDir(), ref)
	if err != nil {
		return nil, nil, false, false, err
	}
	sectionOffset, err := columnVectorGraphTypedColumnSectionOffset(ref, section)
	if err != nil {
		return nil, nil, false, false, err
	}
	key := mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  ref.Namespace,
		Kind:       string(ref.Kind),
		Generation: ref.Generation,
		PartID:     ref.PartID,
		FileID:     ref.FileID,
		Offset:     sectionOffset,
		Length:     int64(section.Length),
		Checksum:   uint64(sectionChecksum),
		Version:    imageVersion,
		Encoding:   section.Encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(section.Kind),
			Category: string(section.Category),
			Column:   section.Column,
		},
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphTypedColumnVectorScopeID, Collection: collection, Namespace: ref.Namespace, Generation: ref.Generation, Reason: "column_graph typed-column vector source"}
	handle, err := manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{Reason: "column_graph typed-column dense vector section", ValidationMode: mappedresource.ValidationVerify, PreferMapped: true, AllowHeapCopy: true})
	if err != nil {
		return nil, nil, false, false, err
	}
	if err := validateColumnVectorGraphTypedColumnHandleChecksum(handle, sectionChecksum); err != nil {
		_ = handle.Release()
		return nil, nil, false, false, err
	}
	expectedElements, err := typedColumnAdapterDenseElements(rows, dims)
	if err != nil {
		_ = handle.Release()
		return nil, nil, false, false, err
	}
	plan := typeddecode.DenseFloat32VectorPlan(certColumn, dims)
	directReq := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: rows, PayloadBytes: section.Length, AssetOffset: int64(ref.Offset), HasAssetOffset: true}
	values, viewStatus := typeddecode.DenseFloat32VectorView(manager, handle, directReq, typeddecode.ResourceViewOptions{ExpectedElements: expectedElements, RequireMapped: true})
	if viewStatus.Direct() {
		return values, handle, true, false, nil
	}
	firstErr := fmt.Errorf("float32 dense vector direct-view validation: %s", viewStatus.String())
	if handle.Source() == mappedresource.SourceMapped && typedColumnDenseDecodeFallbackAllowed(viewStatus) {
		_ = handle.Release()
		handle, err = manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{Reason: "column_graph typed-column dense vector section heap fallback", ValidationMode: mappedresource.ValidationVerify, AllowHeapCopy: true})
		if err != nil {
			return nil, nil, false, false, errors.Join(firstErr, err)
		}
		if checksumErr := validateColumnVectorGraphTypedColumnHandleChecksum(handle, sectionChecksum); checksumErr != nil {
			_ = handle.Release()
			return nil, nil, false, false, errors.Join(firstErr, checksumErr)
		}
		values, viewStatus = typeddecode.DenseFloat32VectorView(manager, handle, directReq, typeddecode.ResourceViewOptions{ExpectedElements: expectedElements, RequireMapped: true})
		if viewStatus.Direct() {
			return values, handle, true, false, nil
		}
	}
	if !typedColumnDenseDecodeFallbackAllowed(viewStatus) {
		_ = handle.Release()
		return nil, nil, false, false, errors.Join(firstErr, fmt.Errorf("float32 dense vector direct-view validation: %s", viewStatus.String()))
	}
	bytes := handle.Bytes()
	decoded, decodeErr := typedcolumn.DecodeRawFloat32VectorPayload(nil, bytes, rows, dims)
	_ = handle.Release()
	if decodeErr != nil {
		return nil, nil, false, false, errors.Join(firstErr, fmt.Errorf("float32 dense vector direct-view validation: %s", viewStatus.String()), decodeErr)
	}
	return decoded, nil, false, true, nil
}

func validateColumnVectorGraphTypedColumnHandleChecksum(handle *mappedresource.Handle, want uint32) error {
	if handle == nil {
		return errors.New("typed_column_part nil mappedresource handle")
	}
	bytes := handle.Bytes()
	if len(bytes) == 0 {
		return errors.New("typed_column_part empty dense vector section")
	}
	if checksum := page.Checksum(bytes); checksum != want {
		return fmt.Errorf("typed_column_part dense vector section checksum=%d want %d", checksum, want)
	}
	return nil
}

func columnVectorGraphTypedColumnSectionOffset(ref ColumnAssetRef, section typedcolumn.ColumnPartImageSection) (int64, error) {
	if section.Offset < 0 || section.Length < 0 {
		return 0, fmt.Errorf("typed_column_part section offset=%d length=%d", section.Offset, section.Length)
	}
	if ref.Offset > math.MaxInt64-int64(section.Offset) {
		return 0, errors.New("typed_column_part section offset overflow")
	}
	return ref.Offset + int64(section.Offset), nil
}

func (r *columnVectorGraphPhysicalRowReader) typedVectorForOrdinal(ordinal int) ([]float32, bool, bool) {
	if r == nil || r.typedVectorSource == nil {
		return nil, false, false
	}
	return r.typedVectorSource.vectorForOrdinal(ordinal)
}

func (r *columnVectorGraphPhysicalRowReader) hasTypedColumnVectorFallback() bool {
	return r != nil && r.typedVectorSource == nil && r.typedVectorFallbackReason != ""
}

func (r *columnVectorGraphPhysicalRowReader) populateTypedColumnVectorSearchStats(stats *columnVectorGraphNativeSearchStats) {
	if r == nil || stats == nil {
		return
	}
	if r.hasTypedColumnVectorFallback() {
		stats.TypedColumnFallbacks = 1
		return
	}
	source := r.typedVectorSource
	if source == nil {
		return
	}
	stats.TypedColumnMappedBytes = source.mappedBytes
	stats.TypedColumnHeapCopyBytes = source.heapCopyBytes
	stats.TypedColumnDecodedBytes = source.decodedDerivedBytes
	stats.TypedColumnActiveHandles = source.activeHandles
	stats.TypedColumnDeniedResources = source.deniedResources
}

func (s *columnVectorGraphTypedColumnVectorSource) vectorForOrdinal(ordinal int) ([]float32, bool, bool) {
	if s == nil || ordinal < 0 || ordinal >= len(s.locations) || s.dims <= 0 {
		return nil, false, false
	}
	loc := s.locations[ordinal]
	if loc.part == nil || loc.rowIndex < 0 || loc.rowIndex >= loc.part.rows {
		return nil, false, false
	}
	start := loc.rowIndex * s.dims
	end := start + s.dims
	if start < 0 || end < start || end > len(loc.part.values) {
		return nil, false, false
	}
	return loc.part.values[start:end], loc.part.direct, true
}

func (s *columnVectorGraphTypedColumnVectorSource) captureResourceStats() {
	if s == nil || s.manager == nil {
		return
	}
	stats := s.manager.Stats()
	if stats.ActiveMappedBytes > 0 {
		s.mappedBytes = uint64(stats.ActiveMappedBytes)
	}
	if stats.ActiveHeapCopyBytes > 0 {
		s.heapCopyBytes = uint64(stats.ActiveHeapCopyBytes)
	}
	s.activeHandles = stats.ActiveHandles
	for _, count := range stats.DeniedByReason {
		s.deniedResources += count
	}
}

func (s *columnVectorGraphTypedColumnVectorSource) Close() error {
	if s == nil {
		return nil
	}
	var closeErr error
	for _, part := range s.parts {
		if part == nil || part.handle == nil {
			continue
		}
		if err := part.handle.Release(); err != nil && closeErr == nil {
			closeErr = err
		}
		part.handle = nil
	}
	return closeErr
}
