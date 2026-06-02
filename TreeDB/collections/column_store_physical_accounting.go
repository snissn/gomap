package collections

import (
	"context"
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
		addKind(asset.Ref, asset.Rows, &out.Totals.RowAssetBytes)
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
		sections := image.SectionByteAccounting()
		out.SerializedSections = make([]ColumnStoreTypedColumnPartSectionAccounting, 0, len(sections))
		for _, section := range sections {
			rawBytes := int64(section.RawBytes)
			storedBytes := int64(section.StoredBytes)
			if rawBytes == 0 {
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
				Compression:      section.Compression.String(),
				RawBytes:         rawBytes,
				StoredBytes:      storedBytes,
				CompressionRatio: columnStoreCompressionRatioInt64(storedBytes, rawBytes),
			})
		}
	}
	return out, nil
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
