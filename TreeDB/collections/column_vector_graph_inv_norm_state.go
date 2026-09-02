package collections

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	columnVectorGraphInvNormStateAssetID    = "inv_norm_by_ordinal"
	columnVectorGraphInvNormStateColumnName = "inv_norm_by_ordinal"
	columnVectorGraphInvNormStatePathSuffix = "_inv_norm_by_ordinal"
	columnVectorGraphInvNormStateScopeID    = "column-vector-graph-inv-norm-state"
)

type columnVectorGraphInvNormStateOutcome uint8

const (
	columnVectorGraphInvNormStateOutcomeUnknown columnVectorGraphInvNormStateOutcome = iota
	columnVectorGraphInvNormStateOutcomeMmapDirect
	columnVectorGraphInvNormStateOutcomeHeapCopyTypedView
	columnVectorGraphInvNormStateOutcomeScratchDecode
)

func (o columnVectorGraphInvNormStateOutcome) String() string {
	switch o {
	case columnVectorGraphInvNormStateOutcomeMmapDirect:
		return "mmap_direct"
	case columnVectorGraphInvNormStateOutcomeHeapCopyTypedView:
		return "heap_copy_typed_view"
	case columnVectorGraphInvNormStateOutcomeScratchDecode:
		return "scratch_decode"
	default:
		return "unknown"
	}
}

type columnVectorGraphPreparedInvNormStateAsset struct {
	Present      bool
	Config       ColumnStoreConfig
	Ref          ColumnAssetRef
	Bytes        int64
	Rows         int
	PaddingBytes int64
	SchemaHash   uint64
}

type columnVectorGraphPreparedInvNormStatePayload struct {
	Config       ColumnStoreConfig
	PartID       uint64
	Payload      []byte
	Bytes        int64
	Rows         int
	PaddingBytes int64
	SchemaHash   uint64
}

type columnVectorGraphInvNormStateSource struct {
	rows           int
	values         []float32
	outcome        columnVectorGraphInvNormStateOutcome
	fallbackReason typeddecode.Reason
	prepared       columnVectorGraphPreparedNormView

	manager         *mappedresource.Manager
	handle          *mappedresource.Handle
	mappedBytes     uint64
	heapCopyBytes   uint64
	decodedBytes    uint64
	activeHandles   int64
	deniedResources uint64
	closed          bool
}

func prepareColumnVectorGraphInvNormStateAsset(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedInvNormStateAsset, error) {
	return prepareColumnVectorGraphInvNormStateAssetWithStableAuthority(assetRootDir, collection, base, def, generation, partID, rows, nil)
}

func prepareColumnVectorGraphInvNormStateAssetWithStableAuthority(assetRootDir, collection string, base ColumnStoreConfig, def VectorIndexDefinition, generation, partID uint64, rows []columnVectorGraphAssetRow, authority *columnVectorGraphStableResourceAccumulator) (columnVectorGraphPreparedInvNormStateAsset, error) {
	payload, err := prepareColumnVectorGraphInvNormStatePayload(collection, base, def, partID, rows)
	if err != nil {
		return columnVectorGraphPreparedInvNormStateAsset{}, err
	}
	if payload.Rows == 0 {
		return columnVectorGraphPreparedInvNormStateAsset{Config: payload.Config, Rows: 0, SchemaHash: payload.SchemaHash}, nil
	}
	if assetRootDir == "" {
		return columnVectorGraphPreparedInvNormStateAsset{}, errors.New("collections: column_graph inv_norm state requires asset root dir")
	}
	if generation == 0 || partID == 0 {
		return columnVectorGraphPreparedInvNormStateAsset{}, errors.New("collections: column_graph inv_norm state requires generation and part_id")
	}
	appender, err := newColumnVectorGraphAssetAppender(assetRootDir, payload.Config, authority)
	if err != nil {
		return columnVectorGraphPreparedInvNormStateAsset{}, err
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, payload.Config)
	ref, appendErr := appender.appendKindWithAlignment(payload.Payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID, alignment)
	closeErr := closeColumnVectorGraphAssetAppender(appender, authority)
	if appendErr != nil {
		return columnVectorGraphPreparedInvNormStateAsset{}, errors.Join(appendErr, closeErr)
	}
	if closeErr != nil {
		return columnVectorGraphPreparedInvNormStateAsset{}, closeErr
	}
	if err := validateColumnVectorGraphPreparedInvNormStateRef(payload, ref, generation); err != nil {
		return columnVectorGraphPreparedInvNormStateAsset{}, err
	}
	return columnVectorGraphPreparedInvNormStateAsset{
		Present:      true,
		Config:       payload.Config,
		Ref:          ref,
		Bytes:        ref.Length,
		Rows:         payload.Rows,
		PaddingBytes: payload.PaddingBytes,
		SchemaHash:   payload.SchemaHash,
	}, nil
}

func prepareColumnVectorGraphInvNormStatePayload(collection string, base ColumnStoreConfig, def VectorIndexDefinition, partID uint64, rows []columnVectorGraphAssetRow) (columnVectorGraphPreparedInvNormStatePayload, error) {
	cfg, adapterColumn, err := columnVectorGraphInvNormStateColumnStoreConfig(collection, base, def)
	if err != nil {
		return columnVectorGraphPreparedInvNormStatePayload{}, err
	}
	if len(rows) == 0 {
		return columnVectorGraphPreparedInvNormStatePayload{Config: cfg, Rows: 0, SchemaHash: cfg.SchemaHash}, nil
	}
	if partID == 0 {
		return columnVectorGraphPreparedInvNormStatePayload{}, errors.New("collections: column_graph inv_norm state requires non-zero part_id")
	}
	primaryIDs := make([]int64, len(rows))
	invNorms := make([]float32, len(rows))
	for ordinal, row := range rows {
		primaryIDs[ordinal] = int64(ordinal)
		invNorm, err := columnVectorGraphInvNorm(row.Vector)
		if err != nil {
			return columnVectorGraphPreparedInvNormStatePayload{}, fmt.Errorf("collections: column_graph inv_norm state ordinal=%d: %w", ordinal, err)
		}
		if err := validateColumnVectorGraphInvNormValue(def.Name, ordinal, invNorm); err != nil {
			return columnVectorGraphPreparedInvNormStatePayload{}, err
		}
		invNorms[ordinal] = invNorm
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(cfg.SchemaHash),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			{
				Name:           typedColumnAdapterPrimaryIDColumn,
				Type:           typedcolumn.ColumnTypeInt64,
				Encoding:       typedcolumn.EncodingRawInt64,
				Compression:    typedcolumn.CompressionNone,
				CompressionSet: true,
				StatsDisabled:  true,
			},
			adapterColumn.Definition,
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{
		Rows: len(rows),
		Columns: map[string][]int64{
			typedColumnAdapterPrimaryIDColumn: primaryIDs,
		},
		Float32Columns: map[string][]float32{
			adapterColumn.Definition.Name: invNorms,
		},
	})
	if err != nil {
		return columnVectorGraphPreparedInvNormStatePayload{}, err
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{
		Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{adapterColumn}),
		LayoutLogicalTypes: map[string]string{
			typedColumnAdapterPrimaryIDColumn: string(typedcolumn.ColumnTypeInt64),
			adapterColumn.Definition.Name:     string(ColumnStoreValueFloat32),
		},
	})
	if err != nil {
		return columnVectorGraphPreparedInvNormStatePayload{}, err
	}
	if image.Rows != len(rows) || image.PartID != partID {
		return columnVectorGraphPreparedInvNormStatePayload{}, fmt.Errorf("collections: column_graph inv_norm state image rows/part=(%d,%d) want (%d,%d)", image.Rows, image.PartID, len(rows), partID)
	}
	accounting := part.ByteAccountingFromImage(image)
	return columnVectorGraphPreparedInvNormStatePayload{
		Config:       cfg,
		PartID:       partID,
		Payload:      image.Bytes,
		Bytes:        int64(len(image.Bytes)),
		Rows:         image.Rows,
		PaddingBytes: int64(accounting.SerializedPaddingBytes),
		SchemaHash:   cfg.SchemaHash,
	}, nil
}

func columnVectorGraphInvNormStateColumnStoreConfig(collection string, base ColumnStoreConfig, def VectorIndexDefinition) (ColumnStoreConfig, typedColumnAdapterColumn, error) {
	normalizedDef, err := normalizeVectorIndexDefinition(def)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if normalizedDef.Strategy != VectorIndexStrategyColumnGraph {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: vector index %q strategy=%q is not column_graph", normalizedDef.Name, normalizedDef.Strategy)
	}
	if !base.Enabled {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph inv_norm state requires enabled base column_store")
	}
	if base.AssetManager == nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, errors.New("collections: column_graph inv_norm state requires base asset manager")
	}
	cfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:               columnVectorGraphInvNormStateColumnName,
			Path:               normalizedDef.Field + columnVectorGraphInvNormStatePathSuffix,
			Owner:              TypedStorageOwnerColumnPart,
			ValueType:          ColumnStoreValueFloat32,
			FixedWidthEncoding: ColumnFixedWidthEncodingLittleEndian,
		}},
		RetainedPayload: ColumnRetainedPayloadNone,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
		AssetManager: &ColumnAssetManagerConfig{
			Kind:      base.AssetManager.Kind,
			Namespace: base.AssetManager.Namespace,
		},
	})
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	fields := columnStoreTypedColumnPartFields(*cfg)
	if len(fields) != 1 {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph inv_norm state fields=%d want 1", len(fields))
	}
	columns, err := typedColumnAdapterColumnsForFields(fields)
	if err != nil {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, err
	}
	if len(columns) != 1 || columns[0].Field.ValueType != ColumnStoreValueFloat32 || columns[0].Definition.Type != typedcolumn.ColumnTypeFloat32 || columns[0].Definition.Encoding != typedcolumn.EncodingRawFloat32 || columns[0].Definition.Compression != typedcolumn.CompressionNone || columns[0].FixedWidthEncoding != ColumnFixedWidthEncodingLittleEndian {
		return ColumnStoreConfig{}, typedColumnAdapterColumn{}, fmt.Errorf("collections: column_graph inv_norm state is not native raw_float32")
	}
	return *cfg, columns[0], nil
}

func validateColumnVectorGraphPreparedInvNormStateRef(payload columnVectorGraphPreparedInvNormStatePayload, ref ColumnAssetRef, generation uint64) error {
	if ref.Namespace != payload.Config.AssetManager.Namespace || ref.Kind != ColumnAssetKindTCS1TypedColumnPart || ref.Generation != generation || ref.PartID != payload.PartID || ref.Length != payload.Bytes {
		return fmt.Errorf("collections: invalid column_graph inv_norm state asset ref %+v", ref)
	}
	if ref.Offset%4 != 0 {
		return fmt.Errorf("collections: column_graph inv_norm state absolute asset offset=%d is not 4-byte aligned", ref.Offset)
	}
	return nil
}

func columnVectorGraphInvNormStateAssetSnapshot(prepared columnVectorGraphPreparedInvNormStateAsset) (columnVectorIndexStateAssetSnapshot, bool) {
	if !prepared.Present {
		return columnVectorIndexStateAssetSnapshot{}, false
	}
	return columnVectorIndexStateAssetSnapshot{
		Role:             columnVectorIndexStateAssetRoleInverseNorm,
		AssetID:          columnVectorGraphInvNormStateAssetID,
		LogicalType:      columnVectorIndexStateLogicalTypeFloat32,
		PhysicalEncoding: columnVectorIndexStateEncodingRawFloat32,
		RowCount:         prepared.Rows,
		SourceSchemaHash: prepared.SchemaHash,
		Ref:              prepared.Ref,
		AssetBytes:       prepared.Bytes,
	}, true
}

func columnVectorGraphNextPartIDAfterPreparedAssets(prepared columnVectorGraphPreparedPhysicalAsset) uint64 {
	return nextColumnVectorGraphPartIDAfter(prepared.Ref.PartID, prepared.Ref.PartID)
}

func findColumnVectorGraphInvNormStateAsset(state columnVectorIndexStateSnapshot) (columnVectorIndexStateAssetSnapshot, bool) {
	for _, asset := range state.Assets {
		if asset.Role == columnVectorIndexStateAssetRoleInverseNorm && asset.AssetID == columnVectorGraphInvNormStateAssetID {
			return asset, true
		}
	}
	return columnVectorIndexStateAssetSnapshot{}, false
}

func validateColumnVectorGraphInvNormStateAssetIfPresent(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) error {
	if _, ok := findColumnVectorGraphInvNormStateAsset(state); !ok {
		return nil
	}
	source, _, err := newColumnVectorGraphInvNormStateSourceFromRoot(rootDir, collection, cfg, def, graph, state)
	if source != nil {
		_ = source.Close()
	}
	return err
}

func (c *Collection) openColumnVectorGraphInvNormStateSourceForReader(collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnVectorGraphInvNormStateSource, typeddecode.Reason, error) {
	if c == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionNil
	}
	if c.db == nil {
		return nil, typeddecode.ReasonValidationFailed, errCollectionDBNil
	}
	return newColumnVectorGraphInvNormStateSourceFromRoot(c.db.ColumnAssetRootDir(), collection, cfg, def, graph, state)
}

func newColumnVectorGraphInvNormStateSourceFromRoot(rootDir, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot) (*columnVectorGraphInvNormStateSource, typeddecode.Reason, error) {
	asset, ok := findColumnVectorGraphInvNormStateAsset(state)
	if !ok {
		return nil, "", nil
	}
	if !columnVectorIndexStateDefinitionParametersMatch(&state, &def) || !columnVectorIndexStateMatchesGraph(state, graph) {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q inv_norm state identity mismatch", def.Name)
	}
	if asset.Role != columnVectorIndexStateAssetRoleInverseNorm || asset.AssetID != columnVectorGraphInvNormStateAssetID || asset.LogicalType != columnVectorIndexStateLogicalTypeFloat32 || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawFloat32 {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q inv_norm state asset contract mismatch role=%q id=%q logical=%q physical=%q", def.Name, asset.Role, asset.AssetID, asset.LogicalType, asset.PhysicalEncoding)
	}
	if asset.RowCount != graph.RowCount || asset.Ref.Generation != graph.BaseManifestGeneration || asset.Ref.Namespace != cfg.AssetManager.Namespace || asset.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart || asset.AssetBytes != asset.Ref.Length {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q inv_norm state asset identity mismatch", def.Name)
	}
	sourceCfg, adapterColumn, err := columnVectorGraphInvNormStateColumnStoreConfig(collection, cfg, def)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if asset.SourceSchemaHash != sourceCfg.SchemaHash {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q inv_norm state schema_hash=%d want %d", def.Name, asset.SourceSchemaHash, sourceCfg.SchemaHash)
	}
	if err := validateColumnVectorGraphAssetRefAvailable(rootDir, asset.Ref); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	raw, err := readColumnPhysicalAssetFromManager(rootDir, asset.Ref)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if int64(len(raw)) != asset.AssetBytes || int64(len(raw)) != asset.Ref.Length {
		return nil, typeddecode.ReasonPayloadLengthMismatch, fmt.Errorf("collections: column_graph %q inv_norm state bytes=%d manifest=%d ref=%d", def.Name, len(raw), asset.AssetBytes, asset.Ref.Length)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if image.PartID != asset.Ref.PartID || image.Rows != asset.RowCount {
		return nil, typeddecode.ReasonRowCountMismatch, fmt.Errorf("collections: column_graph %q inv_norm state image part/rows=(%d,%d) want (%d,%d)", def.Name, image.PartID, image.Rows, asset.Ref.PartID, asset.RowCount)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if adapterPart.Part.Descriptor.SchemaVersion != uint32(sourceCfg.SchemaHash) {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q inv_norm state schema_version=%d want %d", def.Name, adapterPart.Part.Descriptor.SchemaVersion, uint32(sourceCfg.SchemaHash))
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q inv_norm state layout certification: %w", def.Name, err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		return nil, typeddecode.ReasonValidationFailed, fmt.Errorf("collections: column_graph %q inv_norm state missing layout certification for column %q", def.Name, adapterColumn.Definition.Name)
	}
	section, err := columnVectorGraphInvNormStateSection(image, adapterColumn.Definition.Name)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if err := validateColumnVectorGraphInvNormStateSection(adapterPart.Part, section, certColumn, adapterColumn.Definition.Name, asset.RowCount); err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	sectionBytes, err := image.SectionBytes(section)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	manager := mappedresource.NewManager()
	handle, err := acquireColumnVectorGraphInvNormStateSection(rootDir, collection, asset.Ref, image.Version, section, page.Checksum(sectionBytes), manager)
	if err != nil {
		return nil, typeddecode.ReasonValidationFailed, err
	}
	plan := typeddecode.Float32ScalarPlan(certColumn)
	directReq := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: asset.RowCount, PayloadBytes: section.Length, AssetOffset: asset.Ref.Offset, HasAssetOffset: true}
	graphReq := typeddecode.GraphFloat32DirectViewRequest{
		Expectation: typeddecode.GraphDirectViewExpectation{
			ExpectedOwner:  columnVectorGraphPreparedOwnerVectorIndexState,
			ActualOwner:    columnVectorGraphPreparedOwnerVectorIndexState,
			ExpectedRole:   columnVectorIndexStateAssetRoleInverseNorm,
			ActualRole:     asset.Role,
			Column:         certColumn.Name,
			Rows:           asset.RowCount,
			AssetOffset:    asset.Ref.Offset,
			HasAssetOffset: true,
		},
		Certification: certColumn,
		Section:       section,
		ExpectedKey:   handle.Key(),
		Handle:        handle,
		Manager:       manager,
	}
	values, retained, outcome, fallbackReason, err := columnVectorGraphInvNormStateValuesFromHandle(manager, handle, directReq, asset.RowCount, graphReq)
	if err != nil {
		return nil, fallbackReason, err
	}
	source := &columnVectorGraphInvNormStateSource{rows: asset.RowCount, values: values, outcome: outcome, fallbackReason: fallbackReason, manager: manager, handle: retained}
	if err := validateColumnVectorGraphInvNormValues(def.Name, values, asset.RowCount); err != nil {
		_ = source.Close()
		return nil, typeddecode.ReasonValidationFailed, err
	}
	if prepared, _, ok := prepareColumnVectorGraphPreparedNormView(source, asset.RowCount); ok {
		source.prepared = prepared
	}
	if outcome == columnVectorGraphInvNormStateOutcomeScratchDecode {
		source.decodedBytes = uint64(len(values)) * 4
	}
	source.captureResourceStats()
	return source, "", nil
}

func columnVectorGraphInvNormStateSection(image typedcolumn.ColumnPartImage, column string) (typedcolumn.ColumnPartImageSection, error) {
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column == column {
			return section, nil
		}
	}
	return typedcolumn.ColumnPartImageSection{}, fmt.Errorf("collections: column_graph inv_norm state missing column data section %q", column)
}

func validateColumnVectorGraphInvNormStateSection(part *typedcolumn.ColumnPart, section typedcolumn.ColumnPartImageSection, certColumn typedcolumn.ColumnPartLayoutContractColumn, columnName string, rows int) error {
	if part == nil {
		return errors.New("collections: column_graph inv_norm state nil typed_column_part")
	}
	if rows <= 0 {
		return fmt.Errorf("collections: column_graph inv_norm state rows=%d", rows)
	}
	if part.Descriptor.RowCount != rows {
		return fmt.Errorf("collections: column_graph inv_norm state descriptor rows=%d want %d", part.Descriptor.RowCount, rows)
	}
	column, ok := part.Columns[columnName]
	if !ok {
		return fmt.Errorf("collections: column_graph inv_norm state missing column %q", columnName)
	}
	if column.Definition.Type != typedcolumn.ColumnTypeFloat32 || column.Definition.Encoding != typedcolumn.EncodingRawFloat32 || column.Definition.Compression != typedcolumn.CompressionNone || column.Definition.FixedWidthElements != 0 {
		return fmt.Errorf("collections: column_graph inv_norm state column %q schema mismatch type=%s encoding=%s compression=%s fixed_width_elements=%d", columnName, column.Definition.Type, column.Definition.Encoding, column.Definition.Compression, column.Definition.FixedWidthElements)
	}
	wantBytes, err := columnVectorGraphInvNormStateByteLen(rows)
	if err != nil {
		return err
	}
	if section.Kind != typedcolumn.ColumnPartImageSectionColumnData || section.Category != typedcolumn.ColumnPartImageCategoryDeclaredColumns || section.Column != columnName {
		return fmt.Errorf("collections: column_graph inv_norm state section identity mismatch kind=%s category=%s column=%q", section.Kind, section.Category, section.Column)
	}
	if section.Encoding != typedcolumn.EncodingRawFloat32 || section.Compression != typedcolumn.CompressionNone {
		return fmt.Errorf("collections: column_graph inv_norm state section encoding=%s compression=%s", section.Encoding, section.Compression)
	}
	if section.Rows != rows || section.Length != wantBytes {
		return fmt.Errorf("collections: column_graph inv_norm state section rows=%d length=%d want rows=%d length=%d", section.Rows, section.Length, rows, wantBytes)
	}
	if status := typeddecode.ValidateDirectViewColumn(typeddecode.DirectViewColumnRequest{Plan: typeddecode.Float32ScalarPlan(certColumn), Certification: certColumn, Rows: rows, PayloadBytes: section.Length, AssetOffset: 0, HasAssetOffset: true}); !status.Direct() {
		return fmt.Errorf("collections: column_graph inv_norm state image-local direct-view contract validation failed: %s", status.String())
	}
	nextRow := 0
	totalBytes := 0
	for i, block := range column.Blocks {
		if block.Descriptor.FirstRow != nextRow {
			return fmt.Errorf("collections: column_graph inv_norm state block %d first_row=%d want %d", i, block.Descriptor.FirstRow, nextRow)
		}
		blockBytes, err := columnVectorGraphInvNormStateByteLen(block.Descriptor.RowCount)
		if err != nil {
			return err
		}
		if block.Descriptor.Encoding != typedcolumn.EncodingRawFloat32 || block.Descriptor.Compression != typedcolumn.CompressionNone || block.Descriptor.RawBytes != blockBytes || block.Descriptor.StoredBytes != blockBytes {
			return fmt.Errorf("collections: column_graph inv_norm state block %d descriptor mismatch encoding=%s compression=%s raw=%d stored=%d want_bytes=%d", i, block.Descriptor.Encoding, block.Descriptor.Compression, block.Descriptor.RawBytes, block.Descriptor.StoredBytes, blockBytes)
		}
		g := block.Granule
		if g.Encoding != typedcolumn.EncodingRawFloat32 || g.Compression != typedcolumn.CompressionNone || g.Rows != block.Descriptor.RowCount || g.RawBytes != blockBytes || g.StoredBytes != blockBytes || len(g.Payload) != blockBytes || g.NullCount != 0 || g.DefaultCount != 0 || g.HasMinMax {
			return fmt.Errorf("collections: column_graph inv_norm state block %d granule mismatch", i)
		}
		nextRow += block.Descriptor.RowCount
		totalBytes += blockBytes
	}
	if nextRow != rows || totalBytes != section.Length {
		return fmt.Errorf("collections: column_graph inv_norm state blocks rows=%d bytes=%d want rows=%d bytes=%d", nextRow, totalBytes, rows, section.Length)
	}
	return nil
}

func columnVectorGraphInvNormStateByteLen(rows int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("collections: column_graph inv_norm state invalid rows=%d", rows)
	}
	if rows > math.MaxInt/4 {
		return 0, errors.New("collections: column_graph inv_norm state bytes overflow")
	}
	return rows * 4, nil
}

func acquireColumnVectorGraphInvNormStateSection(rootDir, collection string, ref ColumnAssetRef, imageVersion uint16, section typedcolumn.ColumnPartImageSection, checksum uint32, manager *mappedresource.Manager) (*mappedresource.Handle, error) {
	if manager == nil {
		return nil, errors.New("collections: column_graph inv_norm state requires mappedresource manager")
	}
	path, err := columnAssetSegmentPath(rootDir, ref)
	if err != nil {
		return nil, err
	}
	sectionOffset, err := columnVectorGraphTypedColumnSectionOffset(ref, section)
	if err != nil {
		return nil, err
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
		Checksum:   uint64(checksum),
		Version:    imageVersion,
		Encoding:   section.Encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(section.Kind),
			Category: string(section.Category),
			Name:     section.Name,
			Column:   section.Column,
		},
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphInvNormStateScopeID, Collection: collection, Namespace: ref.Namespace, Generation: ref.Generation, Reason: "column_graph inv_norm state"}
	handle, err := manager.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{
		Reason:         "column_graph inv_norm state section",
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
		return nil, errors.Join(fmt.Errorf("collections: column_graph inv_norm state checksum=%d want %d", got, checksum), releaseErr)
	}
	return handle, nil
}

func columnVectorGraphInvNormStateValuesFromHandle(manager *mappedresource.Manager, handle *mappedresource.Handle, directReq typeddecode.DirectViewColumnRequest, rows int, graphReqs ...typeddecode.GraphFloat32DirectViewRequest) ([]float32, *mappedresource.Handle, columnVectorGraphInvNormStateOutcome, typeddecode.Reason, error) {
	if handle == nil {
		status := typeddecode.StreamingStatus(typeddecode.ReasonNilHandle, "nil inv_norm state handle")
		return nil, nil, columnVectorGraphInvNormStateOutcomeUnknown, status.Reason, fmt.Errorf("collections: column_graph inv_norm state handle validation: %s", status.String())
	}
	var status typeddecode.Status
	if len(graphReqs) > 0 {
		view, graphStatus := typeddecode.CertifyGraphFloat32DirectView(graphReqs[0])
		status = graphStatus
		if status.Direct() {
			return view.Values, view.Handle, columnVectorGraphInvNormStateOutcomeMmapDirect, "", nil
		}
	} else {
		values, directStatus := typeddecode.Float32ScalarView(manager, handle, directReq, typeddecode.ResourceViewOptions{ExpectedElements: rows, RequireMapped: true})
		status = directStatus
		if status.Direct() {
			return values, handle, columnVectorGraphInvNormStateOutcomeMmapDirect, "", nil
		}
	}
	firstStatus := status
	fallbackReason := status.Reason
	if status.Reason == typeddecode.ReasonHandleSourceUnsupported || status.Reason == typeddecode.ReasonActualPointerUnaligned {
		var values []float32
		values, status = typeddecode.Float32ScalarView(manager, handle, directReq, typeddecode.ResourceViewOptions{ExpectedElements: rows, RequireMapped: false})
		if status.Direct() {
			return values, handle, columnVectorGraphInvNormStateOutcomeHeapCopyTypedView, "", nil
		}
		fallbackReason = status.Reason
	}
	if !typedColumnDenseDecodeFallbackAllowed(status) {
		releaseErr := handle.Release()
		return nil, nil, columnVectorGraphInvNormStateOutcomeUnknown, fallbackReason, errors.Join(fmt.Errorf("collections: column_graph inv_norm state direct-view validation: %s", firstStatus.String()), fmt.Errorf("collections: column_graph inv_norm state fallback validation: %s", status.String()), releaseErr)
	}
	decoded, decodeErr := typedcolumn.DecodeRawFloat32Payload(nil, handle.Bytes(), rows)
	releaseErr := handle.Release()
	if decodeErr != nil {
		return nil, nil, columnVectorGraphInvNormStateOutcomeUnknown, fallbackReason, errors.Join(fmt.Errorf("collections: column_graph inv_norm state direct-view validation: %s", firstStatus.String()), decodeErr, releaseErr)
	}
	if releaseErr != nil {
		return nil, nil, columnVectorGraphInvNormStateOutcomeUnknown, fallbackReason, errors.Join(fmt.Errorf("collections: column_graph inv_norm state direct-view validation: %s", firstStatus.String()), releaseErr)
	}
	return decoded, nil, columnVectorGraphInvNormStateOutcomeScratchDecode, fallbackReason, nil
}

func validateColumnVectorGraphInvNormValues(indexName string, values []float32, rows int) error {
	if len(values) != rows {
		return fmt.Errorf("collections: column_graph %q inv_norm state values=%d want rows=%d", indexName, len(values), rows)
	}
	for ordinal, value := range values {
		if err := validateColumnVectorGraphInvNormValue(indexName, ordinal, value); err != nil {
			return err
		}
	}
	return nil
}

func validateColumnVectorGraphInvNormValue(indexName string, ordinal int, value float32) error {
	if value <= 0 || math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
		return fmt.Errorf("collections: column_graph %q ordinal=%d invalid inv_norm state value=%v", indexName, ordinal, value)
	}
	return nil
}

func (r *columnVectorGraphPhysicalRowReader) usesInvNormStateSource() bool {
	return r != nil && r.invNormSource != nil
}

func (r *columnVectorGraphPhysicalRowReader) invNormForOrdinal(ordinal int) (float32, columnVectorGraphInvNormStateOutcome, typeddecode.Reason, bool) {
	if r == nil || r.invNormSource == nil {
		return 0, columnVectorGraphInvNormStateOutcomeUnknown, "", false
	}
	return r.invNormSource.invNormForOrdinal(ordinal)
}

func (r *columnVectorGraphPhysicalRowReader) hasInvNormStateFallback() bool {
	return r != nil && r.invNormSource == nil && (r.invNormStateUnavailable || r.invNormStateFallbackReason != "")
}

func (r *columnVectorGraphPhysicalRowReader) populateInvNormStateSearchStats(stats *columnVectorGraphNativeSearchStats) {
	if r == nil || stats == nil {
		return
	}
	if r.hasInvNormStateFallback() {
		stats.NormSourceUnavailable = 1
		stats.NormSourceFallbacks = 1
		recordColumnVectorGraphInvNormFallbackReasonStats(stats, r.invNormStateFallbackReason)
		return
	}
	source := r.invNormSource
	if source == nil {
		return
	}
	stats.NormMappedBytes = source.mappedBytes
	stats.NormHeapCopyBytes = source.heapCopyBytes
	stats.NormDecodedBytes = source.decodedBytes
	stats.NormActiveHandles = source.activeHandles
	stats.NormDeniedResources = source.deniedResources
}

func (s *columnVectorGraphInvNormStateSource) invNormForOrdinal(ordinal int) (float32, columnVectorGraphInvNormStateOutcome, typeddecode.Reason, bool) {
	if s == nil {
		return 0, columnVectorGraphInvNormStateOutcomeUnknown, "", false
	}
	if s.closed || (s.handle != nil && s.handle.Released()) {
		return 0, columnVectorGraphInvNormStateOutcomeUnknown, typeddecode.ReasonStaleHandle, false
	}
	if ordinal < 0 || ordinal >= s.rows || ordinal >= len(s.values) {
		return 0, columnVectorGraphInvNormStateOutcomeUnknown, typeddecode.ReasonRowCountMismatch, false
	}
	return s.values[ordinal], s.outcome, s.fallbackReason, true
}

func (s *columnVectorGraphInvNormStateSource) captureResourceStats() {
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

func (s *columnVectorGraphInvNormStateSource) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	closeErr := releaseMappedResourceHandle(s.handle)
	s.handle = nil
	s.values = nil
	s.rows = 0
	s.prepared = columnVectorGraphPreparedNormView{}
	s.outcome = columnVectorGraphInvNormStateOutcomeUnknown
	s.fallbackReason = ""
	s.activeHandles = 0
	return closeErr
}
