package collections

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

// ColumnStorePhysicalAccountingOptions controls production column-store byte
// accounting. Section bytes are decoded from persisted typed-column part images
// and are a breakdown of typed-column part payload bytes, not an extra total.
type ColumnStorePhysicalAccountingOptions struct {
	DetailedSections bool                     `json:"detailed_sections,omitempty"`
	ReadIntegrity    ColumnAssetReadIntegrity `json:"read_integrity,omitempty"`
}

// ColumnStorePhysicalAccounting reports the active column manifest's referenced
// physical assets and typed-column part section bytes.
type ColumnStorePhysicalAccounting struct {
	Complete                   bool                                     `json:"complete"`
	Collection                 string                                   `json:"collection"`
	Namespace                  string                                   `json:"namespace,omitempty"`
	ManifestRootName           string                                   `json:"manifest_root_name,omitempty"`
	ManifestRootID             uint64                                   `json:"manifest_root_id,omitempty"`
	ManifestGeneration         uint64                                   `json:"manifest_generation,omitempty"`
	ManifestChecksum           uint64                                   `json:"manifest_checksum,omitempty"`
	RecoveryManifestGeneration uint64                                   `json:"recovery_manifest_generation,omitempty"`
	RecoveryManifestChecksum   uint64                                   `json:"recovery_manifest_checksum,omitempty"`
	AppliedCommandLSN          uint64                                   `json:"applied_command_lsn,omitempty"`
	ManifestRecords            int                                      `json:"manifest_records,omitempty"`
	RowAssetRefs               int                                      `json:"row_asset_refs,omitempty"`
	TypedColumnPartRefs        int                                      `json:"typed_column_part_refs,omitempty"`
	AggregateMetadataRefs      int                                      `json:"aggregate_metadata_refs,omitempty"`
	DictionaryCodeRefs         int                                      `json:"dictionary_code_refs,omitempty"`
	Int64ValueRefs             int                                      `json:"int64_value_refs,omitempty"`
	GraphAssetRefs             int                                      `json:"graph_asset_refs,omitempty"`
	Totals                     ColumnStorePhysicalAccountingTotals      `json:"totals"`
	AssetKinds                 []ColumnStorePhysicalAssetKindAccounting `json:"asset_kinds,omitempty"`
	RowAssets                  []ColumnStoreRowAssetAccounting          `json:"row_assets,omitempty"`
	TypedColumnParts           []ColumnStoreTypedColumnPartAccounting   `json:"typed_column_parts,omitempty"`
	SidecarAssets              []ColumnStorePhysicalAssetAccounting     `json:"sidecar_assets,omitempty"`
	GraphAssets                []ColumnStorePhysicalAssetRefAccounting  `json:"graph_assets,omitempty"`
	Warnings                   []string                                 `json:"warnings,omitempty"`
}

// ColumnStorePhysicalAccountingTotals contains manifest-referenced payload
// bytes. TypedColumnSections is a category breakdown of TypedColumnPartBytes.
type ColumnStorePhysicalAccountingTotals struct {
	ReferencedAssetBytes   int64                                    `json:"referenced_asset_bytes,omitempty"`
	RowAssetBytes          int64                                    `json:"row_asset_bytes,omitempty"`
	TypedColumnPartBytes   int64                                    `json:"typed_column_part_bytes,omitempty"`
	AggregateMetadataBytes int64                                    `json:"aggregate_metadata_bytes,omitempty"`
	DictionaryCodeBytes    int64                                    `json:"dictionary_code_bytes,omitempty"`
	Int64ValueBytes        int64                                    `json:"int64_value_bytes,omitempty"`
	GraphAssetBytes        int64                                    `json:"graph_asset_bytes,omitempty"`
	RowAssetSections       ColumnStoreRowAssetByteAccounting        `json:"row_asset_sections"`
	TypedColumnSections    ColumnStoreTypedColumnPartByteAccounting `json:"typed_column_sections"`
}

// ColumnStorePhysicalAssetKindAccounting summarizes active manifest refs by
// concrete column asset kind.
type ColumnStorePhysicalAssetKindAccounting struct {
	Kind  ColumnAssetKind `json:"kind"`
	Count int             `json:"count"`
	Bytes int64           `json:"bytes"`
	Rows  int             `json:"rows,omitempty"`
}

// ColumnStorePhysicalAssetRefAccounting is a JSON-friendly public copy of
// ColumnAssetRef fields.
type ColumnStorePhysicalAssetRefAccounting struct {
	Kind       ColumnAssetKind `json:"kind"`
	Namespace  string          `json:"namespace,omitempty"`
	Generation uint64          `json:"generation"`
	PartID     uint64          `json:"part_id"`
	FileID     uint32          `json:"file_id"`
	Offset     int64           `json:"offset"`
	Length     int64           `json:"length"`
	Checksum   uint32          `json:"checksum"`
}

// ColumnStorePhysicalAssetAccounting describes one active manifest asset.
type ColumnStorePhysicalAssetAccounting struct {
	Ref    ColumnStorePhysicalAssetRefAccounting `json:"ref"`
	Rows   int                                   `json:"rows,omitempty"`
	Bytes  int64                                 `json:"bytes"`
	Role   ColumnManifestPartRole                `json:"role,omitempty"`
	Reason ColumnPublishOperation                `json:"reason,omitempty"`
	Column string                                `json:"column,omitempty"`
	Name   string                                `json:"name,omitempty"`
}

// ColumnStoreRowAssetAccounting reports one physical row asset and its
// serialized payload byte breakdown.
type ColumnStoreRowAssetAccounting struct {
	Asset   ColumnStorePhysicalAssetAccounting `json:"asset"`
	Payload ColumnStoreRowAssetByteAccounting  `json:"payload"`
}

// ColumnStoreRowAssetByteAccounting breaks down the legacy TCPA row asset
// envelope. RowIDStoredBytes includes the per-row length prefix; RowIDValueBytes
// is only the document ID payload. RowValueHeaderBytes covers repeated per-value
// type/null/present metadata, while RowValuePayloadBytes covers the encoded
// value bodies.
type ColumnStoreRowAssetByteAccounting struct {
	Rows                   int                                   `json:"rows,omitempty"`
	DeletedRows            int                                   `json:"deleted_rows,omitempty"`
	Columns                int                                   `json:"columns,omitempty"`
	Operation              ColumnPublishOperation                `json:"operation,omitempty"`
	SerializedAssetBytes   int64                                 `json:"serialized_asset_bytes,omitempty"`
	FormatHeaderBytes      int64                                 `json:"format_header_bytes,omitempty"`
	ColumnMetadataBytes    int64                                 `json:"column_metadata_bytes,omitempty"`
	RowEncodingHeaderBytes int64                                 `json:"row_encoding_header_bytes,omitempty"`
	RowIDStoredBytes       int64                                 `json:"row_id_stored_bytes,omitempty"`
	RowIDValueBytes        int64                                 `json:"row_id_value_bytes,omitempty"`
	RowDeletedFlagBytes    int64                                 `json:"row_deleted_flag_bytes,omitempty"`
	RowValueHeaderBytes    int64                                 `json:"row_value_header_bytes,omitempty"`
	RowValuePayloadBytes   int64                                 `json:"row_value_payload_bytes,omitempty"`
	TotalStoredBytes       int64                                 `json:"total_stored_bytes,omitempty"`
	BytesPerRow            float64                               `json:"bytes_per_row,omitempty"`
	ColumnsDetail          []ColumnStoreRowAssetColumnAccounting `json:"columns_detail,omitempty"`
	columnNames            []string                              `json:"-"`
}

// ColumnStoreRowAssetColumnAccounting reports row-asset bytes for one row-owned
// column.
type ColumnStoreRowAssetColumnAccounting struct {
	Column            string `json:"column"`
	Type              string `json:"type"`
	Rows              int    `json:"rows"`
	PresentRows       int    `json:"present_rows"`
	NullRows          int    `json:"null_rows"`
	ValueHeaderBytes  int64  `json:"value_header_bytes"`
	ValuePayloadBytes int64  `json:"value_payload_bytes"`
	StoredBytes       int64  `json:"stored_bytes"`
}

// ColumnStoreTypedColumnPartAccounting reports one typed-column part image and
// its serialized section/category byte breakdown.
type ColumnStoreTypedColumnPartAccounting struct {
	Asset ColumnStorePhysicalAssetAccounting       `json:"asset"`
	Image ColumnStoreTypedColumnPartByteAccounting `json:"image"`
}

// ColumnStoreTypedColumnPartByteAccounting mirrors the persisted image
// categories needed by external benchmark/accounting tools without exposing
// TreeDB/internal/typedcolumn types.
type ColumnStoreTypedColumnPartByteAccounting struct {
	Rows                       int                                           `json:"rows,omitempty"`
	Columns                    int                                           `json:"columns,omitempty"`
	SerializedImageBytes       int64                                         `json:"serialized_image_bytes,omitempty"`
	SerializedManifestBytes    int64                                         `json:"serialized_manifest_bytes,omitempty"`
	SerializedPaddingBytes     int64                                         `json:"serialized_padding_bytes,omitempty"`
	DeclaredColumnBytes        int64                                         `json:"declared_column_bytes,omitempty"`
	DeclaredColumnOffsetsBytes int64                                         `json:"declared_column_offsets_bytes,omitempty"`
	DeclaredColumnValuesBytes  int64                                         `json:"declared_column_values_bytes,omitempty"`
	DictionaryBytes            int64                                         `json:"dictionary_bytes,omitempty"`
	MarkBytes                  int64                                         `json:"mark_bytes,omitempty"`
	SortKeyMetadataBytes       int64                                         `json:"sort_key_metadata_bytes,omitempty"`
	AggregateMetadataBytes     int64                                         `json:"aggregate_metadata_bytes,omitempty"`
	ColumnStatsBytes           int64                                         `json:"column_stats_bytes,omitempty"`
	PruningMetadataBytes       int64                                         `json:"pruning_metadata_bytes,omitempty"`
	DescriptorBytes            int64                                         `json:"descriptor_bytes,omitempty"`
	LayoutContractBytes        int64                                         `json:"layout_contract_bytes,omitempty"`
	LocatorBytes               int64                                         `json:"locator_bytes,omitempty"`
	TotalStoredBytes           int64                                         `json:"total_stored_bytes,omitempty"`
	BytesPerRow                float64                                       `json:"bytes_per_row,omitempty"`
	LogicalValueBytes          int64                                         `json:"logical_value_bytes,omitempty"`
	EncodedRawBytes            int64                                         `json:"encoded_raw_bytes,omitempty"`
	CodecBlocks                int                                           `json:"codec_blocks,omitempty"`
	CompressionNanos           int64                                         `json:"compression_nanos,omitempty"`
	ColumnsDetail              []ColumnStoreTypedColumnColumnAccounting      `json:"columns_detail,omitempty"`
	CompressionDetail          []ColumnStoreTypedColumnCompressionAccounting `json:"compression_detail,omitempty"`
	SerializedSections         []ColumnStoreTypedColumnPartSectionAccounting `json:"serialized_sections,omitempty"`
	columnNames                []string                                      `json:"-"`
}

// ColumnStoreTypedColumnColumnAccounting reports one declared typed-column
// payload's codec/storage contribution for benchmark/storage reports.
type ColumnStoreTypedColumnColumnAccounting struct {
	Column               string         `json:"column"`
	Type                 string         `json:"type"`
	Rows                 int            `json:"rows"`
	Blocks               int            `json:"blocks"`
	LogicalValueBytes    int64          `json:"logical_value_bytes"`
	EncodedRawBytes      int64          `json:"encoded_raw_bytes"`
	StoredBytes          int64          `json:"stored_bytes"`
	Encoding             string         `json:"encoding"`
	RequestedCompression string         `json:"requested_compression"`
	ActualCompressionMix map[string]int `json:"actual_compression_mix,omitempty"`
	CompressionAttempted int            `json:"compression_attempted"`
	CompressionKept      int            `json:"compression_kept"`
	CompressionRejected  int            `json:"compression_rejected"`
	FallbackReasons      map[string]int `json:"fallback_reasons,omitempty"`
	CompressionNanos     int64          `json:"compression_nanos"`
}

// ColumnStoreTypedColumnCompressionAccounting reports requested-vs-actual
// compression admission and bytes for one column/substream/codec bucket.
type ColumnStoreTypedColumnCompressionAccounting struct {
	Column                 string  `json:"column"`
	Substream              string  `json:"substream"`
	Encoding               string  `json:"encoding"`
	RequestedCompression   string  `json:"requested_compression"`
	ActualCompression      string  `json:"actual_compression"`
	Blocks                 int     `json:"blocks"`
	CompressionAttempted   int     `json:"compression_attempted"`
	CompressionKept        int     `json:"compression_kept"`
	CompressionRejected    int     `json:"compression_rejected"`
	FallbackReason         string  `json:"fallback_reason,omitempty"`
	EncodedRawBytes        int64   `json:"encoded_raw_bytes"`
	StoredBytes            int64   `json:"stored_bytes"`
	CompressionNanos       int64   `json:"compression_nanos"`
	StoredToEncodedRawRate float64 `json:"stored_to_encoded_raw_rate"`
}

// ColumnStoreTypedColumnPartSectionAccounting reports one serialized section in
// a typed-column part image.
type ColumnStoreTypedColumnPartSectionAccounting struct {
	Kind             string  `json:"kind"`
	Category         string  `json:"category"`
	Name             string  `json:"name,omitempty"`
	Column           string  `json:"column,omitempty"`
	Bytes            int64   `json:"bytes"`
	Compression      string  `json:"compression,omitempty"`
	RawBytes         int64   `json:"raw_bytes,omitempty"`
	StoredBytes      int64   `json:"stored_bytes,omitempty"`
	CompressionRatio float64 `json:"compression_ratio,omitempty"`
}

// ColumnStorePhysicalAccounting decodes active production column-store storage
// accounting for the collection.
func (c *Collection) ColumnStorePhysicalAccounting(ctx context.Context, opts ColumnStorePhysicalAccountingOptions) (ColumnStorePhysicalAccounting, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ColumnStorePhysicalAccounting{}, err
	}
	view, closeView, err := c.prepareColumnPhysicalScanSnapshotViewWithContextAndSidecars(ctx, columnManifestScanAllSidecars())
	if err != nil {
		return ColumnStorePhysicalAccounting{}, err
	}
	defer closeView()
	if err := ctx.Err(); err != nil {
		return ColumnStorePhysicalAccounting{}, err
	}
	return physicalAccountingFromScanView(ctx, view, opts)
}

func physicalAccountingFromScanView(ctx context.Context, view columnPhysicalScanSnapshotView, opts ColumnStorePhysicalAccountingOptions) (ColumnStorePhysicalAccounting, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ColumnStorePhysicalAccounting{}, err
	}
	out := ColumnStorePhysicalAccounting{
		Collection:                 view.CollectionName,
		Namespace:                  view.AssetNamespace,
		ManifestRootName:           view.Diagnostics.ManifestRootName,
		ManifestRootID:             view.Diagnostics.ManifestRoot,
		ManifestGeneration:         view.Diagnostics.ManifestGeneration,
		ManifestChecksum:           view.Diagnostics.ActiveManifestChecksum,
		RecoveryManifestGeneration: view.Diagnostics.RecoveryManifestGeneration,
		RecoveryManifestChecksum:   view.Diagnostics.RecoveryManifestChecksum,
		AppliedCommandLSN:          view.Diagnostics.AppliedCommandLSN,
		ManifestRecords:            view.Diagnostics.ManifestRecords,
		RowAssetRefs:               len(view.AssetRefs),
		TypedColumnPartRefs:        len(view.TypedColumnPartRefs),
		AggregateMetadataRefs:      len(view.AggregateMetadata),
		DictionaryCodeRefs:         len(view.DictionaryCodes),
		Int64ValueRefs:             len(view.Int64Values),
		GraphAssetRefs:             len(view.GraphAssetRefs),
		RowAssets:                  make([]ColumnStoreRowAssetAccounting, 0, len(view.AssetRefs)),
		TypedColumnParts:           make([]ColumnStoreTypedColumnPartAccounting, 0, len(view.TypedColumnPartRefs)),
	}
	seen := make(map[ColumnAssetRef]struct{})
	graphSeen := make(map[ColumnAssetRef]struct{})
	kinds := make(map[ColumnAssetKind]*ColumnStorePhysicalAssetKindAccounting)
	addKind := func(ref ColumnAssetRef, rows int, bucket *int64) {
		bytes := positiveColumnStorePhysicalAccountingBytes(ref.Length)
		if _, ok := seen[ref]; !ok {
			seen[ref] = struct{}{}
			out.Totals.ReferencedAssetBytes = addColumnStorePhysicalAccountingBytes(out.Totals.ReferencedAssetBytes, bytes)
			if bucket != nil {
				*bucket = addColumnStorePhysicalAccountingBytes(*bucket, bytes)
			}
			total := kinds[ref.Kind]
			if total == nil {
				total = &ColumnStorePhysicalAssetKindAccounting{Kind: ref.Kind}
				kinds[ref.Kind] = total
			}
			total.Count++
			total.Bytes = addColumnStorePhysicalAccountingBytes(total.Bytes, bytes)
			total.Rows = addColumnStorePhysicalAccountingRows(total.Rows, rows)
		}
	}
	addGraphKind := func(ref ColumnAssetRef) {
		addKind(ref, 0, nil)
		if _, ok := graphSeen[ref]; ok {
			return
		}
		graphSeen[ref] = struct{}{}
		out.Totals.GraphAssetBytes = addColumnStorePhysicalAccountingBytes(out.Totals.GraphAssetBytes, positiveColumnStorePhysicalAccountingBytes(ref.Length))
	}

	for _, asset := range view.AssetRefs {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		addKind(asset.Ref, asset.Rows, &out.Totals.RowAssetBytes)
		rowAsset, err := columnStoreRowAssetAccountingFromRef(view.ColumnAssetRootDir, view.CollectionName, view.Config, asset, opts)
		if err != nil {
			return out, err
		}
		out.RowAssets = append(out.RowAssets, rowAsset)
		addColumnStoreRowAssetByteAccounting(&out.Totals.RowAssetSections, rowAsset.Payload)
	}
	for _, asset := range view.TypedColumnPartRefs {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		addKind(asset.Ref, asset.Rows, &out.Totals.TypedColumnPartBytes)
		part, err := columnStoreTypedColumnPartAccountingFromRef(view.ColumnAssetRootDir, asset, opts)
		if err != nil {
			return out, err
		}
		out.TypedColumnParts = append(out.TypedColumnParts, part)
		addColumnStoreTypedColumnPartByteAccounting(&out.Totals.TypedColumnSections, part.Image)
	}
	for _, asset := range view.AggregateMetadata {
		addKind(asset.AssetRef, 0, &out.Totals.AggregateMetadataBytes)
		out.SidecarAssets = append(out.SidecarAssets, ColumnStorePhysicalAssetAccounting{
			Ref:   columnStorePhysicalAssetRefAccounting(asset.AssetRef),
			Bytes: positiveColumnStorePhysicalAccountingBytes(asset.AssetRef.Length),
			Name:  asset.Name,
		})
	}
	for _, asset := range view.DictionaryCodes {
		addKind(asset.AssetRef, 0, &out.Totals.DictionaryCodeBytes)
		out.SidecarAssets = append(out.SidecarAssets, ColumnStorePhysicalAssetAccounting{
			Ref:    columnStorePhysicalAssetRefAccounting(asset.AssetRef),
			Bytes:  positiveColumnStorePhysicalAccountingBytes(asset.AssetRef.Length),
			Column: asset.ColumnName,
		})
	}
	for _, asset := range view.Int64Values {
		addKind(asset.AssetRef, asset.Rows, &out.Totals.Int64ValueBytes)
		out.SidecarAssets = append(out.SidecarAssets, ColumnStorePhysicalAssetAccounting{
			Ref:    columnStorePhysicalAssetRefAccounting(asset.AssetRef),
			Rows:   asset.Rows,
			Bytes:  positiveColumnStorePhysicalAccountingBytes(asset.AssetRef.Length),
			Column: asset.ColumnName,
		})
	}
	for _, ref := range view.GraphAssetRefs {
		addGraphKind(ref)
		out.GraphAssets = append(out.GraphAssets, columnStorePhysicalAssetRefAccounting(ref))
	}
	out.AssetKinds = columnStorePhysicalAccountingKindTotals(kinds)
	out.Complete = true
	return out, nil
}

func columnStoreRowAssetAccountingFromRef(rootDir string, expectedCollection string, cfg ColumnStoreConfig, asset columnManifestAssetRefForScan, opts ColumnStorePhysicalAccountingOptions) (ColumnStoreRowAssetAccounting, error) {
	raw, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(rootDir, asset.Ref, nil, opts.ReadIntegrity)
	if err != nil {
		return ColumnStoreRowAssetAccounting{}, err
	}
	payload, err := columnStoreRowAssetPayloadAccounting(raw, asset.Ref, expectedCollection, cfg, asset.Reason, opts.DetailedSections)
	if err != nil {
		return ColumnStoreRowAssetAccounting{}, fmt.Errorf("collections: parse row asset generation=%d part=%d: %w", asset.Ref.Generation, asset.Ref.PartID, err)
	}
	if payload.Rows != asset.Rows {
		return ColumnStoreRowAssetAccounting{}, fmt.Errorf("collections: row asset rows=%d does not match asset rows=%d", payload.Rows, asset.Rows)
	}
	return ColumnStoreRowAssetAccounting{
		Asset: ColumnStorePhysicalAssetAccounting{
			Ref:    columnStorePhysicalAssetRefAccounting(asset.Ref),
			Rows:   asset.Rows,
			Bytes:  positiveColumnStorePhysicalAccountingBytes(asset.Ref.Length),
			Role:   asset.Role,
			Reason: asset.Reason,
		},
		Payload: payload,
	}, nil
}

func columnStoreRowAssetPayloadAccounting(raw []byte, ref ColumnAssetRef, expectedCollection string, cfg ColumnStoreConfig, expectedOperation ColumnPublishOperation, detailed bool) (ColumnStoreRowAssetByteAccounting, error) {
	if ref.Kind != ColumnAssetKindTCS1PartImage {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("row asset kind=%q want %q", ref.Kind, ColumnAssetKindTCS1PartImage)
	}
	if int64(len(raw)) != ref.Length {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("row asset bytes=%d does not match ref length=%d", len(raw), ref.Length)
	}
	cur := manifestCursor{raw: raw}
	if magic := cur.u32(); magic != columnPhysicalAssetMagic {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("bad column physical asset magic=0x%08x", magic)
	}
	version := cur.u16()
	if !isSupportedColumnPhysicalAssetVersion(version) {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("unsupported column physical asset version=%d", version)
	}
	collection := cur.stringBytes()
	namespace := cur.stringBytes()
	generation := cur.u64()
	partID := cur.u64()
	appliedCommandLSN := cur.u64()
	operationBytes := cur.stringBytes()
	operation, operationOK := columnPhysicalScanOperationFromBytes(operationBytes)
	schemaHash := cur.u64()
	columnCount := cur.u64()
	rowCount := cur.u64()
	if err := cur.err; err != nil {
		return ColumnStoreRowAssetByteAccounting{}, err
	}
	if columnCount > uint64(maxCollectionInt) {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset column_count=%d overflows int max=%d", columnCount, maxCollectionInt)
	}
	if rowCount > uint64(maxCollectionInt) {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset row_count=%d overflows int max=%d", rowCount, maxCollectionInt)
	}
	header := columnPhysicalAssetScanHeader{
		Collection:        collection,
		Namespace:         namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: appliedCommandLSN,
		Operation:         operation,
		SchemaHash:        schemaHash,
		ColumnCount:       int(columnCount),
		RowCount:          int(rowCount),
	}
	if !operationOK {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("unsupported column physical asset operation %q", operationBytes)
	}
	if version == columnPhysicalAssetVersionV1 && header.Operation == ColumnPublishOperationDelete {
		return ColumnStoreRowAssetByteAccounting{}, errors.New("legacy v1 column physical asset delete operation unsupported")
	}
	if err := validateColumnPhysicalAssetScanHeader(header, ref, expectedCollection, &cfg); err != nil {
		return ColumnStoreRowAssetByteAccounting{}, err
	}
	if expectedOperation != "" && header.Operation != expectedOperation {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("%w: manifest reason=%q asset operation=%q", errColumnPhysicalAssetManifestOperationMismatch, expectedOperation, header.Operation)
	}
	if header.ColumnCount != len(cfg.Columns) {
		return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset columns=%d want %d", header.ColumnCount, len(cfg.Columns))
	}

	out := ColumnStoreRowAssetByteAccounting{
		Rows:                 header.RowCount,
		Columns:              header.ColumnCount,
		Operation:            header.Operation,
		SerializedAssetBytes: int64(len(raw)),
		FormatHeaderBytes:    int64(cur.pos),
		TotalStoredBytes:     int64(len(raw)),
		columnNames:          make([]string, 0, header.ColumnCount),
	}
	columns := make([]ColumnStoreColumn, header.ColumnCount)
	columnStart := cur.pos
	for colIdx := 0; colIdx < header.ColumnCount; colIdx++ {
		name := cur.stringBytes()
		path := cur.stringBytes()
		valueTypeBytes := cur.stringBytes()
		nullable := cur.bool()
		dictionary := cur.bool()
		vectorDims := 0
		if version >= columnPhysicalAssetVersionV4 {
			rawVectorDims := cur.u64()
			if rawVectorDims > uint64(maxCollectionInt) {
				return ColumnStoreRowAssetByteAccounting{}, errors.New("column physical asset vector_dims overflows int")
			}
			vectorDims = int(rawVectorDims)
		}
		elementsPerRow := 0
		if version >= columnPhysicalAssetVersionV6 {
			rawElementsPerRow := cur.u64()
			if rawElementsPerRow > uint64(maxCollectionInt) {
				return ColumnStoreRowAssetByteAccounting{}, errors.New("column physical asset elements_per_row overflows int")
			}
			elementsPerRow = int(rawElementsPerRow)
		}
		fixedWidthEncoding := ColumnFixedWidthEncodingDefault
		if version >= columnPhysicalAssetVersionV5 {
			fixedWidthEncoding = ColumnFixedWidthEncoding(string(cur.stringBytes()))
			if _, err := normalizeColumnFixedWidthEncoding(fixedWidthEncoding); err != nil {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset column[%d] fixed_width_encoding: %w", colIdx, err)
			}
			if fixedWidthEncoding != ColumnFixedWidthEncodingDefault && !columnStoreValueTypeSupportsFixedWidthEncoding(ColumnStoreValueType(string(valueTypeBytes))) {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset column[%d] fixed_width_encoding unsupported for value_type %q", colIdx, string(valueTypeBytes))
			}
			if fixedWidthEncoding != ColumnFixedWidthEncodingDefault && columnStoreValueTypeHasScalarFixedWidthPayload(ColumnStoreValueType(string(valueTypeBytes))) {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset column[%d] scalar fixed_width_encoding unsupported for value_type %q", colIdx, string(valueTypeBytes))
			}
		}
		if cur.err != nil {
			return ColumnStoreRowAssetByteAccounting{}, cur.err
		}
		got := ColumnStoreColumn{
			Name:               string(name),
			Path:               string(path),
			ValueType:          ColumnStoreValueType(string(valueTypeBytes)),
			Nullable:           nullable,
			Dictionary:         dictionary,
			VectorDims:         vectorDims,
			ElementsPerRow:     elementsPerRow,
			FixedWidthEncoding: fixedWidthEncoding,
		}
		want := cfg.Columns[colIdx]
		geometryMatches := got.VectorDims == want.VectorDims && got.ElementsPerRow == want.ElementsPerRow
		if got.ValueType == ColumnStoreValueFloat32Vector && want.ValueType == ColumnStoreValueFloat32Vector {
			gotWidth := columnStoreFloat32VectorElementsPerRow(got)
			geometryMatches = gotWidth == columnStoreFloat32VectorElementsPerRow(want)
		}
		if got.Name != want.Name ||
			got.Path != want.Path ||
			got.ValueType != want.ValueType ||
			got.Nullable != want.Nullable ||
			got.Dictionary != want.Dictionary ||
			!geometryMatches ||
			got.FixedWidthEncoding != want.FixedWidthEncoding {
			return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset column[%d]=%+v want %+v", colIdx, got, want)
		}
		columns[colIdx] = got
		out.columnNames = append(out.columnNames, got.Name)
	}
	out.ColumnMetadataBytes = int64(cur.pos - columnStart)

	columnDetails := make([]ColumnStoreRowAssetColumnAccounting, header.ColumnCount)
	for i, col := range columns {
		columnDetails[i] = ColumnStoreRowAssetColumnAccounting{
			Column: col.Name,
			Type:   string(col.ValueType),
		}
	}
	if version >= columnPhysicalAssetVersionV7 {
		rowEncodingHeaderStart := cur.pos
		rowEncoding := cur.string()
		if header.ColumnCount != 0 {
			return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset row encoding %q requires zero columns", rowEncoding)
		}
		deleted := header.Operation == ColumnPublishOperationDelete
		if header.Operation != ColumnPublishOperationInsert && header.Operation != ColumnPublishOperationUpdate && header.Operation != ColumnPublishOperationDelete {
			return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("unsupported column physical asset operation %q", header.Operation)
		}
		switch rowEncoding {
		case columnPhysicalAssetRowEncodingFixedID:
			idWidth := cur.u64()
			if cur.err != nil {
				return ColumnStoreRowAssetByteAccounting{}, cur.err
			}
			if idWidth == 0 || idWidth > uint64(maxCollectionInt) {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset fixed id width=%d invalid", idWidth)
			}
			out.RowEncodingHeaderBytes = int64(cur.pos - rowEncodingHeaderStart)
			for rowIdx := 0; rowIdx < header.RowCount; rowIdx++ {
				if uint64(len(raw)-cur.pos) < idWidth {
					return ColumnStoreRowAssetByteAccounting{}, errors.New("short column physical asset fixed row id block")
				}
				cur.pos += int(idWidth)
				out.RowIDStoredBytes = addColumnStorePhysicalAccountingBytes(out.RowIDStoredBytes, int64(idWidth))
				out.RowIDValueBytes = addColumnStorePhysicalAccountingBytes(out.RowIDValueBytes, int64(idWidth))
				if deleted {
					out.DeletedRows++
				}
			}
		case columnPhysicalAssetRowEncodingDenseIDRange:
			if version < columnPhysicalAssetVersionV8 {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset row encoding %q requires version >= %d", rowEncoding, columnPhysicalAssetVersionV8)
			}
			baseID := cur.u64()
			if cur.err != nil {
				return ColumnStoreRowAssetByteAccounting{}, cur.err
			}
			if header.RowCount > 0 && baseID > ^uint64(0)-uint64(header.RowCount-1) {
				return ColumnStoreRowAssetByteAccounting{}, errors.New("column physical asset dense id range overflows uint64")
			}
			out.RowEncodingHeaderBytes = int64(cur.pos - rowEncodingHeaderStart)
			out.RowIDValueBytes = addColumnStorePhysicalAccountingBytes(out.RowIDValueBytes, int64(header.RowCount)*8)
			if deleted {
				out.DeletedRows = header.RowCount
			}
		default:
			return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("unsupported column physical asset row encoding %q", rowEncoding)
		}
		if cur.err != nil {
			return ColumnStoreRowAssetByteAccounting{}, cur.err
		}
		if cur.pos != len(raw) {
			return ColumnStoreRowAssetByteAccounting{}, errors.New("trailing bytes in column physical asset")
		}
		if out.Rows > 0 {
			out.BytesPerRow = float64(out.TotalStoredBytes) / float64(out.Rows)
		}
		return out, nil
	}
	for rowIdx := 0; rowIdx < header.RowCount; rowIdx++ {
		idStart := cur.pos
		id := cur.bytesView()
		if cur.err != nil {
			return ColumnStoreRowAssetByteAccounting{}, cur.err
		}
		out.RowIDStoredBytes = addColumnStorePhysicalAccountingBytes(out.RowIDStoredBytes, int64(cur.pos-idStart))
		out.RowIDValueBytes = addColumnStorePhysicalAccountingBytes(out.RowIDValueBytes, int64(len(id)))
		deleted := false
		if version >= columnPhysicalAssetVersionV2 {
			deletedStart := cur.pos
			deleted = cur.bool()
			if cur.err != nil {
				return ColumnStoreRowAssetByteAccounting{}, cur.err
			}
			out.RowDeletedFlagBytes = addColumnStorePhysicalAccountingBytes(out.RowDeletedFlagBytes, int64(cur.pos-deletedStart))
		}
		if deleted {
			if header.Operation != ColumnPublishOperationDelete {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset %s row[%d] is marked deleted", header.Operation, rowIdx)
			}
			out.DeletedRows++
			continue
		}
		if header.Operation == ColumnPublishOperationDelete {
			return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("column physical asset delete row[%d] is not marked deleted", rowIdx)
		}
		for colIdx, col := range columns {
			valueHeaderStart := cur.pos
			typeBytes := cur.stringBytes()
			if cur.err != nil {
				return ColumnStoreRowAssetByteAccounting{}, cur.err
			}
			if !columnPhysicalBytesEqualString(typeBytes, string(col.ValueType)) {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("row[%d] column[%d] type=%q want %q", rowIdx, colIdx, string(typeBytes), col.ValueType)
			}
			null := cur.bool()
			if cur.err != nil {
				return ColumnStoreRowAssetByteAccounting{}, cur.err
			}
			present := true
			if version >= columnPhysicalAssetVersionV3 {
				present = cur.bool()
				if cur.err != nil {
					return ColumnStoreRowAssetByteAccounting{}, cur.err
				}
			}
			valueHeaderBytes := int64(cur.pos - valueHeaderStart)
			out.RowValueHeaderBytes = addColumnStorePhysicalAccountingBytes(out.RowValueHeaderBytes, valueHeaderBytes)
			columnDetails[colIdx].Rows++
			columnDetails[colIdx].ValueHeaderBytes = addColumnStorePhysicalAccountingBytes(columnDetails[colIdx].ValueHeaderBytes, valueHeaderBytes)
			if !present {
				if !null {
					return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("row[%d] column[%d] absent value is not null", rowIdx, colIdx)
				}
				if !col.Nullable {
					return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("row[%d] column[%d] is absent but column is not nullable", rowIdx, colIdx)
				}
				columnDetails[colIdx].NullRows++
				continue
			}
			columnDetails[colIdx].PresentRows++
			if null {
				if !col.Nullable {
					return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("row[%d] column[%d] is null but column is not nullable", rowIdx, colIdx)
				}
				columnDetails[colIdx].NullRows++
				continue
			}
			valuePayloadStart := cur.pos
			if err := skipColumnStoreRowAssetValuePayload(&cur, col); err != nil {
				return ColumnStoreRowAssetByteAccounting{}, fmt.Errorf("row[%d] column[%d]: %w", rowIdx, colIdx, err)
			}
			valuePayloadBytes := int64(cur.pos - valuePayloadStart)
			out.RowValuePayloadBytes = addColumnStorePhysicalAccountingBytes(out.RowValuePayloadBytes, valuePayloadBytes)
			columnDetails[colIdx].ValuePayloadBytes = addColumnStorePhysicalAccountingBytes(columnDetails[colIdx].ValuePayloadBytes, valuePayloadBytes)
		}
	}
	if cur.err != nil {
		return ColumnStoreRowAssetByteAccounting{}, cur.err
	}
	if cur.pos != len(raw) {
		return ColumnStoreRowAssetByteAccounting{}, errors.New("trailing bytes in column physical asset")
	}
	if out.Rows > 0 {
		out.BytesPerRow = float64(out.TotalStoredBytes) / float64(out.Rows)
	}
	if detailed && len(columnDetails) != 0 {
		for i := range columnDetails {
			columnDetails[i].StoredBytes = addColumnStorePhysicalAccountingBytes(columnDetails[i].ValueHeaderBytes, columnDetails[i].ValuePayloadBytes)
		}
		out.ColumnsDetail = columnDetails
	}
	return out, nil
}

func skipColumnStoreRowAssetValuePayload(cur *manifestCursor, col ColumnStoreColumn) error {
	switch col.ValueType {
	case ColumnStoreValueBool:
		_ = cur.bool()
	case ColumnStoreValueInt64:
		_ = cur.u64()
	case ColumnStoreValueFloat32:
		_ = cur.u32()
	case ColumnStoreValueDouble:
		_ = cur.u64()
	case ColumnStoreValueString:
		_ = cur.stringBytes()
	case ColumnStoreValueInt8:
		_ = cur.u8()
	case ColumnStoreValueUint8:
		_ = cur.u8()
	case ColumnStoreValueInt16:
		_ = cur.u16()
	case ColumnStoreValueUint16:
		_ = cur.u16()
	case ColumnStoreValueInt32:
		_ = cur.u32()
	case ColumnStoreValueUint32:
		_ = cur.u32()
	case ColumnStoreValueUint64:
		_ = cur.u64()
	case ColumnStoreValueFloat16:
		_ = cur.u16()
	case ColumnStoreValueBFloat16:
		_ = cur.u16()
	case ColumnStoreValueFloat32Vector:
		cur.skipFloat32SliceWithExpectedLength(columnStoreFloat32VectorElementsPerRow(col))
	case ColumnStoreValueUint8Vector, ColumnStoreValueInt8Vector, ColumnStoreValueUint16Vector, ColumnStoreValueInt16Vector, ColumnStoreValueUint32Vector, ColumnStoreValueInt32Vector, ColumnStoreValueUint64Vector, ColumnStoreValueInt64Vector, ColumnStoreValueFloat16Vector, ColumnStoreValueBFloat16Vector, ColumnStoreValueFloat64Vector:
		cur.skipDenseNumericVectorBytesWithExpectedLength(col)
	case ColumnStoreValueUint32List, ColumnStoreValueAdjacencyList:
		cur.skipUint32Slice()
	default:
		return fmt.Errorf("unsupported column physical value type %q", col.ValueType)
	}
	return cur.err
}

func columnStoreTypedColumnPartAccountingFromRef(rootDir string, asset columnManifestAssetRefForScan, opts ColumnStorePhysicalAccountingOptions) (ColumnStoreTypedColumnPartAccounting, error) {
	raw, err := readColumnPhysicalAssetFromManagerIntoWithIntegrity(rootDir, asset.Ref, nil, opts.ReadIntegrity)
	if err != nil {
		return ColumnStoreTypedColumnPartAccounting{}, err
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		return ColumnStoreTypedColumnPartAccounting{}, fmt.Errorf("collections: parse typed-column part image generation=%d part=%d: %w", asset.Ref.Generation, asset.Ref.PartID, err)
	}
	if int64(image.TotalBytes()) != positiveColumnStorePhysicalAccountingBytes(asset.Ref.Length) {
		return ColumnStoreTypedColumnPartAccounting{}, fmt.Errorf("collections: typed-column part image bytes=%d does not match asset ref length=%d", image.TotalBytes(), asset.Ref.Length)
	}
	if image.PartID != asset.Ref.PartID {
		return ColumnStoreTypedColumnPartAccounting{}, fmt.Errorf("collections: typed-column part image part_id=%d does not match asset ref part_id=%d", image.PartID, asset.Ref.PartID)
	}
	if image.Rows != asset.Rows {
		return ColumnStoreTypedColumnPartAccounting{}, fmt.Errorf("collections: typed-column part image rows=%d does not match asset rows=%d", image.Rows, asset.Rows)
	}
	imageAccounting, err := columnStoreTypedColumnPartImageAccounting(image, opts.DetailedSections)
	if err != nil {
		return ColumnStoreTypedColumnPartAccounting{}, err
	}
	return ColumnStoreTypedColumnPartAccounting{
		Asset: ColumnStorePhysicalAssetAccounting{
			Ref:    columnStorePhysicalAssetRefAccounting(asset.Ref),
			Rows:   asset.Rows,
			Bytes:  positiveColumnStorePhysicalAccountingBytes(asset.Ref.Length),
			Role:   asset.Role,
			Reason: asset.Reason,
		},
		Image: imageAccounting,
	}, nil
}

func columnStoreTypedColumnPartImageAccounting(image typedcolumn.ColumnPartImage, detailed bool) (ColumnStoreTypedColumnPartByteAccounting, error) {
	columnNames := columnStoreTypedColumnPartImageColumnNames(image)
	out := ColumnStoreTypedColumnPartByteAccounting{
		Rows:                       image.Rows,
		Columns:                    len(columnNames),
		SerializedImageBytes:       int64(image.TotalBytes()),
		SerializedManifestBytes:    int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryManifest)),
		SerializedPaddingBytes:     int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryPadding)),
		DeclaredColumnOffsetsBytes: int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryDeclaredColumnOffsets)),
		DeclaredColumnValuesBytes:  int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryDeclaredColumnValues)),
		DictionaryBytes:            int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryDictionaries)),
		MarkBytes:                  int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryMarks)),
		SortKeyMetadataBytes:       int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategorySortKeyMetadata)),
		AggregateMetadataBytes:     int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryAggregateMetadata)),
		ColumnStatsBytes:           int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryColumnStats)),
		PruningMetadataBytes:       int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryPruningMetadata)),
		DescriptorBytes:            int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryDescriptor)),
		LayoutContractBytes:        int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryLayoutContract)),
		LocatorBytes:               int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryLocators)),
		columnNames:                columnNames,
	}
	out.DeclaredColumnBytes = int64(image.CategoryBytes(typedcolumn.ColumnPartImageCategoryDeclaredColumns))
	out.TotalStoredBytes = out.SerializedImageBytes
	if out.Rows > 0 {
		out.BytesPerRow = float64(out.TotalStoredBytes) / float64(out.Rows)
	}
	part, err := typedcolumn.ColumnPartFromImageWithOptions(image, typedcolumn.ColumnPartImageReadOptions{
		IncludeRowLocators:       false,
		ValidateRowLocators:      false,
		IncludeAggregateMetadata: false,
		IncludeColumnStats:       false,
		IncludePruningMetadata:   false,
	})
	if err != nil {
		return ColumnStoreTypedColumnPartByteAccounting{}, err
	}
	accounting := part.ByteAccountingFromImage(image)
	out.LogicalValueBytes = int64(accounting.LogicalValueBytes)
	out.EncodedRawBytes = int64(accounting.EncodedRawBytes)
	out.CodecBlocks = accounting.CodecBlocks
	for _, detail := range accounting.ColumnsDetail {
		out.ColumnsDetail = append(out.ColumnsDetail, ColumnStoreTypedColumnColumnAccounting{
			Column:               detail.Column,
			Type:                 string(detail.Type),
			Rows:                 detail.Rows,
			Blocks:               detail.Blocks,
			LogicalValueBytes:    int64(detail.LogicalValueBytes),
			EncodedRawBytes:      int64(detail.EncodedRawBytes),
			StoredBytes:          int64(detail.StoredBytes),
			Encoding:             detail.Encoding.String(),
			RequestedCompression: detail.RequestedCompression.String(),
			ActualCompressionMix: detail.ActualCompressionMix,
			CompressionAttempted: detail.CompressionAttempted,
			CompressionKept:      detail.CompressionKept,
			CompressionRejected:  detail.CompressionRejected,
			FallbackReasons:      detail.FallbackReasons,
			CompressionNanos:     detail.CompressionNanos,
		})
		out.CompressionNanos = addColumnStorePhysicalAccountingBytes(out.CompressionNanos, detail.CompressionNanos)
	}
	for _, detail := range accounting.CompressionDetail {
		out.CompressionDetail = append(out.CompressionDetail, ColumnStoreTypedColumnCompressionAccounting{
			Column:                 detail.Column,
			Substream:              detail.Substream,
			Encoding:               detail.Encoding.String(),
			RequestedCompression:   detail.RequestedCompression.String(),
			ActualCompression:      detail.ActualCompression.String(),
			Blocks:                 detail.Blocks,
			CompressionAttempted:   detail.CompressionAttempted,
			CompressionKept:        detail.CompressionKept,
			CompressionRejected:    detail.CompressionRejected,
			FallbackReason:         detail.FallbackReason,
			EncodedRawBytes:        int64(detail.EncodedRawBytes),
			StoredBytes:            int64(detail.StoredBytes),
			CompressionNanos:       detail.CompressionNanos,
			StoredToEncodedRawRate: detail.StoredToEncodedRawRate,
		})
	}
	if detailed {
		columnSectionBytes := columnStoreTypedColumnPartColumnSectionBytes(accounting.ColumnsDetail)
		sections := image.SectionByteAccounting()
		out.SerializedSections = make([]ColumnStoreTypedColumnPartSectionAccounting, 0, len(sections))
		for _, section := range sections {
			rawBytes := int64(section.RawBytes)
			storedBytes := int64(section.StoredBytes)
			compression := section.Compression.String()
			if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Column != "" {
				if bytes, ok := columnSectionBytes[section.Column]; ok {
					rawBytes = bytes.raw
					storedBytes = bytes.stored
					compression = bytes.compression
				}
			}
			if rawBytes == 0 && section.Kind != typedcolumn.ColumnPartImageSectionRowLocators {
				rawBytes = int64(section.Bytes)
			}
			if storedBytes == 0 {
				storedBytes = int64(section.Bytes)
			}
			out.SerializedSections = append(out.SerializedSections, ColumnStoreTypedColumnPartSectionAccounting{
				Kind:             string(section.Kind),
				Category:         string(section.Category),
				Name:             section.Name,
				Column:           section.Column,
				Bytes:            int64(section.Bytes),
				Compression:      compression,
				RawBytes:         rawBytes,
				StoredBytes:      storedBytes,
				CompressionRatio: columnStoreCompressionRatioInt64(storedBytes, rawBytes),
			})
		}
	}
	return out, nil
}

type columnStoreTypedColumnPartColumnSectionByteTotals struct {
	raw         int64
	stored      int64
	compression string
}

func columnStoreTypedColumnPartColumnSectionBytes(details []typedcolumn.ColumnPartColumnByteAccounting) map[string]columnStoreTypedColumnPartColumnSectionByteTotals {
	out := make(map[string]columnStoreTypedColumnPartColumnSectionByteTotals, len(details))
	for _, detail := range details {
		if detail.Column == "" {
			continue
		}
		out[detail.Column] = columnStoreTypedColumnPartColumnSectionByteTotals{
			raw:         int64(detail.EncodedRawBytes),
			stored:      int64(detail.StoredBytes),
			compression: columnStoreTypedColumnPartColumnSectionCompression(detail),
		}
	}
	return out
}

func columnStoreTypedColumnPartColumnSectionCompression(detail typedcolumn.ColumnPartColumnByteAccounting) string {
	switch len(detail.ActualCompressionMix) {
	case 0:
		return detail.RequestedCompression.String()
	case 1:
		for actual := range detail.ActualCompressionMix {
			return actual
		}
	default:
		return "mixed"
	}
	return detail.RequestedCompression.String()
}

func columnStoreTypedColumnPartImageColumnNames(image typedcolumn.ColumnPartImage) []string {
	if len(image.Sections) == 0 {
		return nil
	}
	columns := make(map[string]struct{})
	for _, section := range image.Sections {
		if section.Column == "" {
			continue
		}
		columns[section.Column] = struct{}{}
	}
	if len(columns) == 0 {
		return nil
	}
	out := make([]string, 0, len(columns))
	for column := range columns {
		out = append(out, column)
	}
	sort.Strings(out)
	return out
}

func addColumnStoreRowAssetByteAccounting(dst *ColumnStoreRowAssetByteAccounting, src ColumnStoreRowAssetByteAccounting) {
	if dst == nil {
		return
	}
	dst.Rows = addColumnStorePhysicalAccountingRows(dst.Rows, src.Rows)
	dst.DeletedRows = addColumnStorePhysicalAccountingRows(dst.DeletedRows, src.DeletedRows)
	addColumnStoreRowAssetByteAccountingColumns(dst, src)
	if dst.Operation == "" {
		dst.Operation = src.Operation
	} else if src.Operation != "" && dst.Operation != src.Operation {
		dst.Operation = ""
	}
	dst.SerializedAssetBytes = addColumnStorePhysicalAccountingBytes(dst.SerializedAssetBytes, src.SerializedAssetBytes)
	dst.FormatHeaderBytes = addColumnStorePhysicalAccountingBytes(dst.FormatHeaderBytes, src.FormatHeaderBytes)
	dst.ColumnMetadataBytes = addColumnStorePhysicalAccountingBytes(dst.ColumnMetadataBytes, src.ColumnMetadataBytes)
	dst.RowEncodingHeaderBytes = addColumnStorePhysicalAccountingBytes(dst.RowEncodingHeaderBytes, src.RowEncodingHeaderBytes)
	dst.RowIDStoredBytes = addColumnStorePhysicalAccountingBytes(dst.RowIDStoredBytes, src.RowIDStoredBytes)
	dst.RowIDValueBytes = addColumnStorePhysicalAccountingBytes(dst.RowIDValueBytes, src.RowIDValueBytes)
	dst.RowDeletedFlagBytes = addColumnStorePhysicalAccountingBytes(dst.RowDeletedFlagBytes, src.RowDeletedFlagBytes)
	dst.RowValueHeaderBytes = addColumnStorePhysicalAccountingBytes(dst.RowValueHeaderBytes, src.RowValueHeaderBytes)
	dst.RowValuePayloadBytes = addColumnStorePhysicalAccountingBytes(dst.RowValuePayloadBytes, src.RowValuePayloadBytes)
	dst.TotalStoredBytes = addColumnStorePhysicalAccountingBytes(dst.TotalStoredBytes, src.TotalStoredBytes)
	if dst.Rows > 0 {
		dst.BytesPerRow = float64(dst.TotalStoredBytes) / float64(dst.Rows)
	}
}

func addColumnStoreRowAssetByteAccountingColumns(dst *ColumnStoreRowAssetByteAccounting, src ColumnStoreRowAssetByteAccounting) {
	if dst == nil {
		return
	}
	if len(src.columnNames) == 0 {
		if src.Columns > dst.Columns {
			dst.Columns = src.Columns
		}
		return
	}
	if len(dst.columnNames) == 0 && dst.Columns == 0 {
		dst.columnNames = append(dst.columnNames[:0], src.columnNames...)
		dst.Columns = len(dst.columnNames)
		return
	}
	if len(dst.columnNames) == 0 && dst.Columns > 0 {
		if src.Columns > dst.Columns {
			dst.Columns = src.Columns
		}
		return
	}
	seen := make(map[string]struct{}, len(dst.columnNames)+len(src.columnNames))
	for _, column := range dst.columnNames {
		seen[column] = struct{}{}
	}
	for _, column := range src.columnNames {
		seen[column] = struct{}{}
	}
	dst.columnNames = dst.columnNames[:0]
	for column := range seen {
		dst.columnNames = append(dst.columnNames, column)
	}
	sort.Strings(dst.columnNames)
	dst.Columns = len(dst.columnNames)
}

func addColumnStoreTypedColumnPartByteAccounting(dst *ColumnStoreTypedColumnPartByteAccounting, src ColumnStoreTypedColumnPartByteAccounting) {
	if dst == nil {
		return
	}
	dst.Rows = addColumnStorePhysicalAccountingRows(dst.Rows, src.Rows)
	addColumnStoreTypedColumnPartByteAccountingColumns(dst, src)
	dst.SerializedImageBytes = addColumnStorePhysicalAccountingBytes(dst.SerializedImageBytes, src.SerializedImageBytes)
	dst.LogicalValueBytes = addColumnStorePhysicalAccountingBytes(dst.LogicalValueBytes, src.LogicalValueBytes)
	dst.EncodedRawBytes = addColumnStorePhysicalAccountingBytes(dst.EncodedRawBytes, src.EncodedRawBytes)
	dst.CodecBlocks += src.CodecBlocks
	dst.CompressionNanos = addColumnStorePhysicalAccountingBytes(dst.CompressionNanos, src.CompressionNanos)
	dst.SerializedManifestBytes = addColumnStorePhysicalAccountingBytes(dst.SerializedManifestBytes, src.SerializedManifestBytes)
	dst.SerializedPaddingBytes = addColumnStorePhysicalAccountingBytes(dst.SerializedPaddingBytes, src.SerializedPaddingBytes)
	dst.DeclaredColumnBytes = addColumnStorePhysicalAccountingBytes(dst.DeclaredColumnBytes, src.DeclaredColumnBytes)
	dst.DeclaredColumnOffsetsBytes = addColumnStorePhysicalAccountingBytes(dst.DeclaredColumnOffsetsBytes, src.DeclaredColumnOffsetsBytes)
	dst.DeclaredColumnValuesBytes = addColumnStorePhysicalAccountingBytes(dst.DeclaredColumnValuesBytes, src.DeclaredColumnValuesBytes)
	dst.DictionaryBytes = addColumnStorePhysicalAccountingBytes(dst.DictionaryBytes, src.DictionaryBytes)
	dst.MarkBytes = addColumnStorePhysicalAccountingBytes(dst.MarkBytes, src.MarkBytes)
	dst.SortKeyMetadataBytes = addColumnStorePhysicalAccountingBytes(dst.SortKeyMetadataBytes, src.SortKeyMetadataBytes)
	dst.AggregateMetadataBytes = addColumnStorePhysicalAccountingBytes(dst.AggregateMetadataBytes, src.AggregateMetadataBytes)
	dst.ColumnStatsBytes = addColumnStorePhysicalAccountingBytes(dst.ColumnStatsBytes, src.ColumnStatsBytes)
	dst.PruningMetadataBytes = addColumnStorePhysicalAccountingBytes(dst.PruningMetadataBytes, src.PruningMetadataBytes)
	dst.DescriptorBytes = addColumnStorePhysicalAccountingBytes(dst.DescriptorBytes, src.DescriptorBytes)
	dst.LayoutContractBytes = addColumnStorePhysicalAccountingBytes(dst.LayoutContractBytes, src.LayoutContractBytes)
	dst.LocatorBytes = addColumnStorePhysicalAccountingBytes(dst.LocatorBytes, src.LocatorBytes)
	dst.TotalStoredBytes = addColumnStorePhysicalAccountingBytes(dst.TotalStoredBytes, src.TotalStoredBytes)
	if dst.Rows > 0 {
		dst.BytesPerRow = float64(dst.TotalStoredBytes) / float64(dst.Rows)
	}
}

func addColumnStoreTypedColumnPartByteAccountingColumns(dst *ColumnStoreTypedColumnPartByteAccounting, src ColumnStoreTypedColumnPartByteAccounting) {
	if dst == nil {
		return
	}
	if len(src.columnNames) == 0 {
		if src.Columns > dst.Columns {
			dst.Columns = src.Columns
		}
		return
	}
	if len(dst.columnNames) == 0 && dst.Columns == 0 {
		dst.columnNames = append(dst.columnNames[:0], src.columnNames...)
		dst.Columns = len(dst.columnNames)
		return
	}
	if len(dst.columnNames) == 0 && dst.Columns > 0 {
		if src.Columns > dst.Columns {
			dst.Columns = src.Columns
		}
		return
	}
	seen := make(map[string]struct{}, len(dst.columnNames)+len(src.columnNames))
	for _, column := range dst.columnNames {
		seen[column] = struct{}{}
	}
	for _, column := range src.columnNames {
		seen[column] = struct{}{}
	}
	dst.columnNames = dst.columnNames[:0]
	for column := range seen {
		dst.columnNames = append(dst.columnNames, column)
	}
	sort.Strings(dst.columnNames)
	dst.Columns = len(dst.columnNames)
}

func columnStorePhysicalAccountingKindTotals(kinds map[ColumnAssetKind]*ColumnStorePhysicalAssetKindAccounting) []ColumnStorePhysicalAssetKindAccounting {
	if len(kinds) == 0 {
		return nil
	}
	out := make([]ColumnStorePhysicalAssetKindAccounting, 0, len(kinds))
	for _, total := range kinds {
		out = append(out, *total)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Kind < out[j].Kind
	})
	return out
}

func columnStorePhysicalAssetRefAccounting(ref ColumnAssetRef) ColumnStorePhysicalAssetRefAccounting {
	return ColumnStorePhysicalAssetRefAccounting{
		Kind:       ref.Kind,
		Namespace:  ref.Namespace,
		Generation: ref.Generation,
		PartID:     ref.PartID,
		FileID:     ref.FileID,
		Offset:     ref.Offset,
		Length:     ref.Length,
		Checksum:   ref.Checksum,
	}
}

func positiveColumnStorePhysicalAccountingBytes(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return value
}

func addColumnStorePhysicalAccountingBytes(left, right int64) int64 {
	if right <= 0 {
		return left
	}
	if left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func addColumnStorePhysicalAccountingRows(left, right int) int {
	if right <= 0 {
		return left
	}
	if left > math.MaxInt-right {
		return math.MaxInt
	}
	return left + right
}

func columnStoreCompressionRatioInt64(storedBytes, rawBytes int64) float64 {
	if rawBytes <= 0 {
		return 0
	}
	return float64(storedBytes) / float64(rawBytes)
}
