package colgranule

import "sort"

const (
	columnPartDescriptorBaseBytes = 96
	granuleDescriptorBytes        = 64
	columnDescriptorBytes         = 32
	columnBlockDescriptorBytes    = 64
	sortKeyColumnDescriptorBytes  = 32
	sortKeyMarkBaseBytes          = 16
	sortKeyPrefixBaseBytes        = 16
	sortKeyBoundInt64Bytes        = 8
	rowLocatorBytes               = 32
	dictionaryHeaderBytes         = 32
	dictionaryEntryOverheadBytes  = 12
)

type ColumnPartByteAccounting struct {
	Rows                      int                                    `json:"rows"`
	Columns                   int                                    `json:"columns"`
	Granules                  int                                    `json:"granules"`
	CodecBlocks               int                                    `json:"codec_blocks"`
	PhysicalFiles             int                                    `json:"physical_files"`
	SerializedImageBytes      int                                    `json:"serialized_image_bytes,omitempty"`
	SerializedManifestBytes   int                                    `json:"serialized_manifest_bytes,omitempty"`
	LogicalValueBytes         int                                    `json:"logical_value_bytes"`
	EncodedRawBytes           int                                    `json:"encoded_raw_bytes"`
	DeclaredColumnStoredBytes int                                    `json:"declared_column_stored_bytes"`
	DictionaryBytes           int                                    `json:"dictionary_bytes"`
	MarkBytes                 int                                    `json:"mark_bytes"`
	SortKeyMetadataBytes      int                                    `json:"sort_key_metadata_bytes"`
	AggregateMetadataBytes    int                                    `json:"aggregate_metadata_bytes"`
	DescriptorBytes           int                                    `json:"descriptor_bytes"`
	LocatorBytes              int                                    `json:"locator_bytes"`
	TotalStoredBytes          int                                    `json:"total_stored_bytes"`
	BytesPerRow               float64                                `json:"bytes_per_row"`
	RetainedJSONPayload       string                                 `json:"retained_json_payload"`
	ColumnsDetail             []ColumnPartColumnByteAccounting       `json:"columns_detail"`
	CompressionDetail         []ColumnPartCompressionByteAccounting  `json:"compression_detail"`
	SerializedSections        []ColumnPartImageSectionByteAccounting `json:"serialized_sections,omitempty"`
}

type ColumnPartColumnByteAccounting struct {
	Column               string         `json:"column"`
	Type                 ColumnType     `json:"type"`
	Rows                 int            `json:"rows"`
	Blocks               int            `json:"blocks"`
	LogicalValueBytes    int            `json:"logical_value_bytes"`
	EncodedRawBytes      int            `json:"encoded_raw_bytes"`
	StoredBytes          int            `json:"stored_bytes"`
	Encoding             Encoding       `json:"encoding"`
	RequestedCompression Compression    `json:"requested_compression"`
	ActualCompressionMix map[string]int `json:"actual_compression_mix"`
	CompressionAttempted int            `json:"compression_attempted"`
	CompressionKept      int            `json:"compression_kept"`
	CompressionRejected  int            `json:"compression_rejected"`
	FallbackReasons      map[string]int `json:"fallback_reasons,omitempty"`
	CompressionNanos     int64          `json:"compression_nanos"`
}

type ColumnPartCompressionByteAccounting struct {
	Column                 string      `json:"column"`
	Substream              string      `json:"substream"`
	Encoding               Encoding    `json:"encoding"`
	RequestedCompression   Compression `json:"requested_compression"`
	ActualCompression      Compression `json:"actual_compression"`
	Blocks                 int         `json:"blocks"`
	CompressionAttempted   int         `json:"compression_attempted"`
	CompressionKept        int         `json:"compression_kept"`
	CompressionRejected    int         `json:"compression_rejected"`
	FallbackReason         string      `json:"fallback_reason,omitempty"`
	EncodedRawBytes        int         `json:"encoded_raw_bytes"`
	StoredBytes            int         `json:"stored_bytes"`
	CompressionNanos       int64       `json:"compression_nanos"`
	StoredToEncodedRawRate float64     `json:"stored_to_encoded_raw_rate"`
}

func (p *ColumnPart) ByteAccounting() ColumnPartByteAccounting {
	if p == nil {
		return ColumnPartByteAccounting{}
	}
	out := ColumnPartByteAccounting{
		Rows:                p.Descriptor.RowCount,
		Granules:            len(p.Descriptor.Granules),
		PhysicalFiles:       0,
		RetainedJSONPayload: "absent_declared_columns_only",
	}
	compressionByKey := make(map[columnCompressionKey]*ColumnPartCompressionByteAccounting)
	for _, columnDescriptor := range p.Descriptor.Columns {
		column, ok := p.Columns[columnDescriptor.Name]
		if !ok {
			continue
		}
		detail := ColumnPartColumnByteAccounting{
			Column:               columnDescriptor.Name,
			Type:                 column.Definition.Type,
			Encoding:             column.Definition.Encoding,
			RequestedCompression: column.Definition.Compression,
			ActualCompressionMix: make(map[string]int),
			FallbackReasons:      make(map[string]int),
		}
		for _, block := range column.Blocks {
			report := block.Granule.CodecReport
			out.CodecBlocks++
			valueBytes := logicalColumnValueBytes(column.Definition.Type, block.Descriptor.RowCount)
			detail.Rows += block.Descriptor.RowCount
			detail.Blocks++
			detail.LogicalValueBytes += valueBytes
			detail.EncodedRawBytes += block.Descriptor.RawBytes
			detail.StoredBytes += block.Descriptor.StoredBytes
			detail.ActualCompressionMix[block.Descriptor.Compression.String()]++
			if report.CompressionAttempted {
				detail.CompressionAttempted++
			}
			if report.CompressionKept {
				detail.CompressionKept++
			}
			if report.CompressionAttempted && !report.CompressionKept {
				detail.CompressionRejected++
			}
			if report.CompressionFallbackReason != "" {
				detail.FallbackReasons[report.CompressionFallbackReason]++
			}
			detail.CompressionNanos += report.CompressionNanos

			key := columnCompressionKey{
				column:               columnDescriptor.Name,
				substream:            "values",
				encoding:             block.Descriptor.Encoding,
				requestedCompression: column.Definition.Compression,
				actualCompression:    block.Descriptor.Compression,
				fallbackReason:       report.CompressionFallbackReason,
			}
			compression := compressionByKey[key]
			if compression == nil {
				compression = &ColumnPartCompressionByteAccounting{
					Column:               key.column,
					Substream:            key.substream,
					Encoding:             key.encoding,
					RequestedCompression: key.requestedCompression,
					ActualCompression:    key.actualCompression,
					FallbackReason:       key.fallbackReason,
				}
				compressionByKey[key] = compression
			}
			compression.Blocks++
			if report.CompressionAttempted {
				compression.CompressionAttempted++
			}
			if report.CompressionKept {
				compression.CompressionKept++
			}
			if report.CompressionAttempted && !report.CompressionKept {
				compression.CompressionRejected++
			}
			compression.EncodedRawBytes += block.Descriptor.RawBytes
			compression.StoredBytes += block.Descriptor.StoredBytes
			compression.CompressionNanos += report.CompressionNanos
		}
		if len(detail.FallbackReasons) == 0 {
			detail.FallbackReasons = nil
		}
		out.LogicalValueBytes += detail.LogicalValueBytes
		out.EncodedRawBytes += detail.EncodedRawBytes
		out.DeclaredColumnStoredBytes += detail.StoredBytes
		out.ColumnsDetail = append(out.ColumnsDetail, detail)
	}
	out.Columns = len(out.ColumnsDetail)
	out.MarkBytes = estimateSortKeyMarkBytes(p.Marks)
	out.SortKeyMetadataBytes = estimateSortKeyMetadataBytes(p.Descriptor.SortKey)
	out.AggregateMetadataBytes = aggregateMetadataStoredBytes(p.AggregateMetadata)
	out.DescriptorBytes = estimateColumnPartDescriptorBytes(p.Descriptor)
	out.LocatorBytes = len(p.Locators) * rowLocatorBytes
	for _, compression := range compressionByKey {
		if compression.EncodedRawBytes > 0 {
			compression.StoredToEncodedRawRate = float64(compression.StoredBytes) / float64(compression.EncodedRawBytes)
		}
		out.CompressionDetail = append(out.CompressionDetail, *compression)
	}
	sort.Slice(out.CompressionDetail, func(i, j int) bool {
		left := out.CompressionDetail[i]
		right := out.CompressionDetail[j]
		if left.Column != right.Column {
			return left.Column < right.Column
		}
		if left.Substream != right.Substream {
			return left.Substream < right.Substream
		}
		if left.Encoding != right.Encoding {
			return left.Encoding < right.Encoding
		}
		if left.RequestedCompression != right.RequestedCompression {
			return left.RequestedCompression < right.RequestedCompression
		}
		if left.ActualCompression != right.ActualCompression {
			return left.ActualCompression < right.ActualCompression
		}
		return left.FallbackReason < right.FallbackReason
	})
	out.RecomputeTotals()
	return out
}

func (p *ColumnPart) ByteAccountingFromImage(image ColumnPartImage) ColumnPartByteAccounting {
	out := p.ByteAccounting()
	if image.TotalBytes() == 0 {
		return out
	}
	out.PhysicalFiles = 0
	out.SerializedImageBytes = image.TotalBytes()
	out.SerializedManifestBytes = image.ManifestBytes
	out.DeclaredColumnStoredBytes = image.CategoryBytes(ColumnPartImageCategoryDeclaredColumns)
	out.DictionaryBytes = image.CategoryBytes(ColumnPartImageCategoryDictionaries)
	out.MarkBytes = image.CategoryBytes(ColumnPartImageCategoryMarks)
	out.SortKeyMetadataBytes = image.CategoryBytes(ColumnPartImageCategorySortKeyMetadata)
	out.AggregateMetadataBytes = image.CategoryBytes(ColumnPartImageCategoryAggregateMetadata)
	out.DescriptorBytes = image.CategoryBytes(ColumnPartImageCategoryDescriptor)
	out.LocatorBytes = image.CategoryBytes(ColumnPartImageCategoryLocators)
	out.SerializedSections = image.SectionByteAccounting()
	out.RecomputeTotals()
	return out
}

func (a *ColumnPartByteAccounting) RecomputeTotals() {
	a.TotalStoredBytes = a.CategoryBytes()
	if a.Rows > 0 {
		a.BytesPerRow = float64(a.TotalStoredBytes) / float64(a.Rows)
	} else {
		a.BytesPerRow = 0
	}
}

func (a ColumnPartByteAccounting) CategoryBytes() int {
	return a.SerializedManifestBytes +
		a.DeclaredColumnStoredBytes +
		a.DictionaryBytes +
		a.MarkBytes +
		a.SortKeyMetadataBytes +
		a.AggregateMetadataBytes +
		a.DescriptorBytes +
		a.LocatorBytes
}

func EstimateJSONBenchDictionaryBytes(ds JSONBenchDataset) int {
	total := 0
	for _, dict := range ds.Dictionaries {
		if len(dict) == 0 {
			continue
		}
		total += dictionaryHeaderBytes
		for value := range dict {
			total += len(value) + dictionaryEntryOverheadBytes
		}
	}
	return total
}

func logicalColumnValueBytes(columnType ColumnType, rows int) int {
	switch columnType {
	case ColumnTypeBool:
		return rows
	case ColumnTypeLowCardinalityCode:
		return rows * 4
	default:
		return rows * 8
	}
}

func estimateColumnPartDescriptorBytes(desc ColumnPartDescriptor) int {
	total := columnPartDescriptorBaseBytes
	total += len(desc.Granules) * granuleDescriptorBytes
	for _, column := range desc.Columns {
		total += columnDescriptorBytes
		total += len(column.Blocks) * columnBlockDescriptorBytes
	}
	return total
}

func estimateSortKeyMetadataBytes(columns []SortKeyColumn) int {
	return len(columns) * sortKeyColumnDescriptorBytes
}

func estimateSortKeyMarkBytes(marks []SortKeyMark) int {
	total := 0
	for _, mark := range marks {
		total += sortKeyMarkBaseBytes
		for _, prefix := range mark.Prefixes {
			total += sortKeyPrefixBaseBytes
			total += len(prefix.Lower.Values) * sortKeyBoundInt64Bytes
			total += len(prefix.UpperExclusive.Values) * sortKeyBoundInt64Bytes
		}
	}
	return total
}

func aggregateMetadataStoredBytes(metadata map[string]AggregateMetadata) int {
	total := 0
	for _, m := range metadata {
		if m.Stats.Admitted {
			total += m.Stats.TotalBytes
		}
	}
	return total
}

type columnCompressionKey struct {
	column               string
	substream            string
	encoding             Encoding
	requestedCompression Compression
	actualCompression    Compression
	fallbackReason       string
}
