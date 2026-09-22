package typedcolumn

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

const maxColumnPartImageStringBytes = 1 << 20

const maxCompressedImageSectionRawBytes = 256 << 20

const maxCompressedRowLocatorSectionRawBytes = maxCompressedImageSectionRawBytes

const maxCompressedDictionarySectionRawBytes = maxCompressedImageSectionRawBytes

const maxCompressedPruningMetadataSectionRawBytes = maxCompressedImageSectionRawBytes

const ColumnPartImageManifestHeaderBytes = 32

func ParseColumnPartImage(data []byte) (ColumnPartImage, error) {
	return parseColumnPartImage(data, len(data))
}

func ParseColumnPartImageManifest(data []byte, totalBytes int) (ColumnPartImage, error) {
	if totalBytes < 0 {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: negative image total bytes %d", totalBytes)
	}
	return parseColumnPartImage(data, totalBytes)
}

func ColumnPartImageManifestLength(header []byte) (int, error) {
	if len(header) < ColumnPartImageManifestHeaderBytes {
		return 0, fmt.Errorf("typedcolumn: manifest header bytes=%d want at least %d", len(header), ColumnPartImageManifestHeaderBytes)
	}
	dec := columnPartImageDecoder{data: header[:ColumnPartImageManifestHeaderBytes]}
	magic, err := dec.u32()
	if err != nil {
		return 0, err
	}
	if magic != columnPartImageMagic {
		return 0, fmt.Errorf("typedcolumn: invalid part image magic 0x%x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return 0, err
	}
	if version != columnPartImageVersion {
		return 0, fmt.Errorf("typedcolumn: unsupported part image version %d", version)
	}
	if _, err := dec.u16(); err != nil {
		return 0, err
	}
	if _, err := dec.u64(); err != nil {
		return 0, err
	}
	if _, err := dec.i64(); err != nil {
		return 0, err
	}
	manifestBytes, err := dec.u32()
	if err != nil {
		return 0, err
	}
	if uint64(int(manifestBytes)) != uint64(manifestBytes) {
		return 0, fmt.Errorf("typedcolumn: manifest bytes=%d exceed host int", manifestBytes)
	}
	manifestLength := int(manifestBytes)
	if manifestLength < ColumnPartImageManifestHeaderBytes {
		return 0, fmt.Errorf("typedcolumn: manifest bytes=%d shorter than header=%d", manifestBytes, ColumnPartImageManifestHeaderBytes)
	}
	return manifestLength, nil
}

func parseColumnPartImage(data []byte, totalBytes int) (ColumnPartImage, error) {
	dec := columnPartImageDecoder{data: data}
	magic, err := dec.u32()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if magic != columnPartImageMagic {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: invalid part image magic 0x%x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if version != columnPartImageVersion {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: unsupported part image version %d", version)
	}
	if _, err := dec.u16(); err != nil {
		return ColumnPartImage{}, err
	}
	partID, err := dec.u64()
	if err != nil {
		return ColumnPartImage{}, err
	}
	rows, err := dec.i64()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if rows < 0 {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: negative image row count %d", rows)
	}
	imageRows, err := nonNegativeInt64ToInt(rows, "image rows")
	if err != nil {
		return ColumnPartImage{}, err
	}
	manifestBytes, err := dec.u32()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if uint64(int(manifestBytes)) != uint64(manifestBytes) {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: manifest bytes=%d exceed host int", manifestBytes)
	}
	manifestLength := int(manifestBytes)
	if manifestLength > len(data) {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: manifest bytes=%d exceed provided bytes=%d", manifestBytes, len(data))
	}
	if manifestLength > totalBytes {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: manifest bytes=%d exceed image bytes=%d", manifestBytes, totalBytes)
	}
	sectionCount, err := dec.u32()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if uint64(int(sectionCount)) != uint64(sectionCount) {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: section count=%d exceed host int", sectionCount)
	}
	if err := validateImageSectionCount(sectionCount, manifestLength, dec.offset); err != nil {
		return ColumnPartImage{}, err
	}
	sections := make([]ColumnPartImageSection, 0, int(sectionCount))
	for i := 0; i < int(sectionCount); i++ {
		kindCode, err := dec.u16()
		if err != nil {
			return ColumnPartImage{}, err
		}
		categoryCode, err := dec.u16()
		if err != nil {
			return ColumnPartImage{}, err
		}
		offset, err := dec.u64()
		if err != nil {
			return ColumnPartImage{}, err
		}
		length, err := dec.u64()
		if err != nil {
			return ColumnPartImage{}, err
		}
		sectionRows, err := dec.i64()
		if err != nil {
			return ColumnPartImage{}, err
		}
		granules, err := dec.i64()
		if err != nil {
			return ColumnPartImage{}, err
		}
		blocks, err := dec.i64()
		if err != nil {
			return ColumnPartImage{}, err
		}
		encoding, err := dec.u16()
		if err != nil {
			return ColumnPartImage{}, err
		}
		compression, err := dec.u16()
		if err != nil {
			return ColumnPartImage{}, err
		}
		rawBytes, err := dec.u64()
		if err != nil {
			return ColumnPartImage{}, err
		}
		name, err := dec.str()
		if err != nil {
			return ColumnPartImage{}, err
		}
		column, err := dec.str()
		if err != nil {
			return ColumnPartImage{}, err
		}
		kind, err := columnPartImageSectionKindFromCode(kindCode)
		if err != nil {
			return ColumnPartImage{}, err
		}
		category, err := columnPartImageSectionCategoryFromCode(categoryCode)
		if err != nil {
			return ColumnPartImage{}, err
		}
		if kind == ColumnPartImageSectionManifest {
			return ColumnPartImage{}, fmt.Errorf("typedcolumn: manifest is not a directory section")
		}
		if err := validateImageSectionCategory(kind, category); err != nil {
			return ColumnPartImage{}, err
		}
		if sectionRows < 0 || granules < 0 || blocks < 0 {
			return ColumnPartImage{}, fmt.Errorf("typedcolumn: section %s has negative rows/granules/blocks (%d,%d,%d)", kind, sectionRows, granules, blocks)
		}
		if int64(int(sectionRows)) != sectionRows || int64(int(granules)) != granules || int64(int(blocks)) != blocks {
			return ColumnPartImage{}, fmt.Errorf("typedcolumn: section %s rows/granules/blocks exceed host int (%d,%d,%d)", kind, sectionRows, granules, blocks)
		}
		if uint64(int(offset)) != offset || uint64(int(length)) != length || uint64(int(rawBytes)) != rawBytes {
			return ColumnPartImage{}, fmt.Errorf("typedcolumn: section %s offset=%d length=%d raw_bytes=%d exceed host int", kind, offset, length, rawBytes)
		}
		section := ColumnPartImageSection{
			Kind:        kind,
			Category:    category,
			Name:        name,
			Column:      column,
			Offset:      int(offset),
			Length:      int(length),
			Rows:        int(sectionRows),
			Granules:    int(granules),
			Blocks:      int(blocks),
			Encoding:    Encoding(encoding),
			Compression: Compression(compression),
			RawBytes:    int(rawBytes),
		}
		if err := validateImageSectionCompression(section); err != nil {
			return ColumnPartImage{}, err
		}
		if err := validateImageSectionBounds(section, int(manifestBytes), totalBytes); err != nil {
			return ColumnPartImage{}, err
		}
		sections = append(sections, section)
	}
	if dec.offset != manifestLength {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: manifest bytes=%d decoded=%d", manifestBytes, dec.offset)
	}
	if err := validateImageSectionLayout(sections, manifestLength, totalBytes); err != nil {
		return ColumnPartImage{}, err
	}
	if err := validateImageSectionMultiplicity(sections); err != nil {
		return ColumnPartImage{}, err
	}
	return ColumnPartImage{
		Version:       version,
		PartID:        partID,
		Rows:          imageRows,
		ManifestBytes: int(manifestBytes),
		Sections:      sections,
		Bytes:         data,
	}, nil
}

type ColumnPartImageReadOptions struct {
	IncludeRowLocators       bool
	ValidateRowLocators      bool
	IncludeAggregateMetadata bool
	IncludeColumnStats       bool
	IncludePruningMetadata   bool
}

func ColumnPartFromImage(image ColumnPartImage) (*ColumnPart, error) {
	return ColumnPartFromImageWithOptions(image, ColumnPartImageReadOptions{
		IncludeRowLocators:       true,
		ValidateRowLocators:      true,
		IncludeAggregateMetadata: true,
		IncludeColumnStats:       true,
		IncludePruningMetadata:   true,
	})
}

func ColumnPartFromImageWithOptions(image ColumnPartImage, opts ColumnPartImageReadOptions) (*ColumnPart, error) {
	if image.TotalBytes() == 0 {
		return nil, fmt.Errorf("typedcolumn: empty part image")
	}
	if opts.ValidateRowLocators {
		opts.IncludeRowLocators = true
	}
	if err := image.validateForRead(); err != nil {
		return nil, err
	}
	descriptorSection, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		return nil, err
	}
	desc, columns, err := decodeColumnPartDescriptorSection(image.sectionBytes(descriptorSection))
	if err != nil {
		return nil, err
	}
	if desc.PartID != image.PartID {
		return nil, fmt.Errorf("typedcolumn: descriptor part id=%d manifest part id=%d", desc.PartID, image.PartID)
	}
	if desc.RowCount != image.Rows {
		return nil, fmt.Errorf("typedcolumn: descriptor rows=%d manifest rows=%d", desc.RowCount, image.Rows)
	}
	sortKey, err := decodeSortKeyMetadataSection(image)
	if err != nil {
		return nil, err
	}
	desc.SortKey = sortKey
	marks, err := decodeSortKeyMarksSection(image)
	if err != nil {
		return nil, err
	}
	if err := validateDecodedSortKeyMarks(desc, marks); err != nil {
		return nil, err
	}
	var locators map[int64]RowLocator
	if opts.IncludeRowLocators {
		locators, err = decodeRowLocatorsSection(image, desc)
		if err != nil {
			return nil, err
		}
		if opts.ValidateRowLocators {
			if err := validateDecodedRowLocators(desc, image.PartID, locators); err != nil {
				return nil, err
			}
		}
	}
	if err := attachColumnPayloadsFromImage(image, columns); err != nil {
		return nil, err
	}
	if err := restoreColumnDefinitionCompressionFromImageSections(image, columns); err != nil {
		return nil, err
	}
	if opts.ValidateRowLocators {
		if err := validateDecodedRowLocatorsPrimaryKey(desc, columns, locators); err != nil {
			return nil, err
		}
	}
	optionsColumns := make([]ColumnDefinition, 0, len(desc.Columns))
	for _, columnDescriptor := range desc.Columns {
		column, ok := columns[columnDescriptor.Name]
		if !ok {
			return nil, fmt.Errorf("typedcolumn: descriptor column %s missing decoded column", columnDescriptor.Name)
		}
		def := column.Definition
		if def.Type == ColumnTypeLowCardinalityCode {
			if def.Cardinality > maxCodeCardinality || (def.Cardinality == 0 && def.Encoding != EncodingNullableInt64) {
				return nil, fmt.Errorf("typedcolumn: invalid low-cardinality cardinality %d for %s", def.Cardinality, def.Name)
			}
			column.Definition = def
			columns[columnDescriptor.Name] = column
		}
		optionsColumns = append(optionsColumns, def)
	}
	var aggregateMetadata map[string]AggregateMetadata
	if opts.IncludeAggregateMetadata {
		aggregateMetadata, err = decodeAggregateMetadataSections(image)
		if err != nil {
			return nil, err
		}
	}
	var columnStats ColumnPartStats
	if opts.IncludeColumnStats {
		columnStats, err = decodeColumnStatsSectionFromImage(image, desc, columns)
		if err != nil {
			return nil, err
		}
	}
	var pruningMetadata ColumnPartPruning
	if opts.IncludePruningMetadata {
		pruningMetadata, err = decodeColumnPruningSectionFromImage(image, desc, columns)
		if err != nil {
			return nil, err
		}
	}
	aggregateDefinitions := make([]AggregateMetadataDefinition, 0, len(aggregateMetadata))
	for _, metadata := range aggregateMetadata {
		aggregateDefinitions = append(aggregateDefinitions, metadata.Definition)
	}
	sort.Slice(aggregateDefinitions, func(i, j int) bool {
		return aggregateDefinitions[i].Name < aggregateDefinitions[j].Name
	})
	part := &ColumnPart{
		Options: Options{
			SchemaVersion: desc.SchemaVersion,
			SchemaMode:    ColumnSchemaFixed,
			Columns:       optionsColumns,
			LogicalPrimaryKey: LogicalPrimaryKey{
				Columns: append([]string(nil), desc.LogicalPrimaryKey...),
			},
			SortKey: SortKey{
				Columns: append([]SortKeyColumn(nil), sortKey...),
			},
			PartPolicy: ColumnPartPolicy{
				RowsPerGranule:        inferredRowsPerGranule(desc.Granules),
				DefaultCodecBlockRows: inferredDefaultCodecBlockRows(desc.Columns),
			},
			AggregateMetadata: aggregateDefinitions,
		},
		Descriptor:        desc,
		Columns:           columns,
		Marks:             marks,
		Locators:          locators,
		AggregateMetadata: aggregateMetadata,
		ColumnStats:       columnStats,
		PruningMetadata:   pruningMetadata,
	}
	return part, nil
}

func DecodeColumnPartDescriptorSection(data []byte) (ColumnPartDescriptor, map[string]ColumnPartColumn, error) {
	return decodeColumnPartDescriptorSection(data)
}

func decodeColumnPartDescriptorSection(data []byte) (ColumnPartDescriptor, map[string]ColumnPartColumn, error) {
	dec := columnPartImageDecoder{data: data}
	version, err := dec.u16()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	if version != columnPartDescriptorVersion {
		return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: unsupported descriptor version %d", version)
	}
	partID, err := dec.u64()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	schemaVersion, err := dec.u32()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	rowCount, err := dec.i64()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	rowCountInt, err := nonNegativeInt64ToInt(rowCount, "descriptor row count")
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	visibleRows, err := dec.i64()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	visibleRowsInt, err := nonNegativeInt64ToInt(visibleRows, "descriptor visible rows")
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	logicalPrimaryKey, err := dec.stringSlice()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	granuleCount, err := dec.u32()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	granules, err := dec.boundedCount(granuleCount, 64, "descriptor granules")
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	desc := ColumnPartDescriptor{
		Version:           uint8(version),
		PartID:            partID,
		SchemaVersion:     schemaVersion,
		RowCount:          rowCountInt,
		VisibleRowCount:   visibleRowsInt,
		LogicalPrimaryKey: logicalPrimaryKey,
		Granules:          make([]GranuleDescriptor, 0, granules),
	}
	for i := 0; i < granules; i++ {
		granule, err := decodeGranuleDescriptor(&dec)
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		desc.Granules = append(desc.Granules, granule)
	}
	if err := validateDecodedGranuleDescriptors(desc); err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	columnCount, err := dec.u32()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	columnTotal, err := dec.boundedCount(columnCount, 18, "descriptor columns")
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	columns := make(map[string]ColumnPartColumn, columnTotal)
	for i := 0; i < columnTotal; i++ {
		name, err := dec.str()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		if _, exists := columns[name]; exists {
			return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: duplicate descriptor column %s", name)
		}
		columnTypeCode, err := dec.u16()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		columnType, err := columnTypeFromCode(columnTypeCode)
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		cardinality, err := dec.u32()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		if columnType == ColumnTypeLowCardinalityCode {
			if cardinality > maxCodeCardinality {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: invalid low-cardinality cardinality %d for %s", cardinality, name)
			}
		} else if cardinality != 0 {
			return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s has cardinality %d", name, columnType, cardinality)
		}
		fixedWidthElements32, err := dec.u32()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		fixedWidthElements, err := uint32ToInt(fixedWidthElements32, "descriptor fixed-width elements")
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		bitsPerElement32, err := dec.u32()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		bitsPerElement, err := uint32ToInt(bitsPerElement32, "descriptor bits per element")
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		switch columnType {
		case ColumnTypeFloat32Vector, ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
			if fixedWidthElements <= 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires positive fixed-width elements", name, columnType)
			}
			if bitsPerElement != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
			}
		case ColumnTypeFixedBytes:
			if fixedWidthElements <= 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires positive fixed-width elements/bytes_per_row", name, columnType)
			}
			if bitsPerElement != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
			}
		case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
			wantBits, ok := PackedUintVectorBits(columnType)
			if !ok || fixedWidthElements <= 0 || bitsPerElement != wantBits {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires positive fixed-width elements and bits_per_element=%d", name, columnType, wantBits)
			}
		case ColumnTypeUint32List:
			if bitsPerElement != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
			}
			if fixedWidthElements != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires fixed-width elements=0", name, columnType)
			}
		case ColumnTypeBytes:
			if bitsPerElement != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
			}
			if fixedWidthElements != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires fixed-width elements=0", name, columnType)
			}
		case ColumnTypeAdjacencyList:
			if bitsPerElement != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
			}
			if fixedWidthElements < 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s has negative fixed-width elements %d", name, columnType, fixedWidthElements)
			}
		default:
			if bitsPerElement != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s has bits_per_element %d", name, columnType, bitsPerElement)
			}
			if fixedWidthElements != 0 {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: column %s type=%s has fixed-width elements %d", name, columnType, fixedWidthElements)
			}
		}
		blockCount, err := dec.u32()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		blocks, err := dec.boundedCount(blockCount, 94, "descriptor column blocks")
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		columnDesc := ColumnPartColumnDescriptor{Name: name, Type: columnType, FixedWidthElements: fixedWidthElements, BitsPerElement: bitsPerElement, Blocks: make([]ColumnBlockDescriptor, 0, blocks)}
		column := ColumnPartColumn{
			Definition: ColumnDefinition{
				Name:               name,
				Type:               columnType,
				Cardinality:        cardinality,
				FixedWidthElements: fixedWidthElements,
				BitsPerElement:     bitsPerElement,
			},
			Blocks: make([]ColumnBlock, 0, blocks),
		}
		expectedFirstRow := 0
		if blocks == 0 && desc.RowCount == 0 {
			switch columnType {
			case ColumnTypeInt64:
				column.Definition.Encoding = EncodingRawInt64
				column.Definition.Compression = CompressionNone
			case ColumnTypeInt8, ColumnTypeUint8, ColumnTypeInt16, ColumnTypeUint16, ColumnTypeInt32, ColumnTypeUint32, ColumnTypeUint64, ColumnTypeFloat16, ColumnTypeBFloat16:
				column.Definition.Encoding = rawScalarEncodingForColumnType(columnType)
				column.Definition.Compression = CompressionNone
			case ColumnTypeFloat32Vector:
				column.Definition.Encoding = EncodingRawFloat32Vector
				column.Definition.Compression = CompressionNone
			case ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
				encoding, ok := DenseFixedWidthVectorEncoding(columnType)
				if !ok {
					return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: empty dense vector column %s type=%s has no raw encoding mapping", name, columnType)
				}
				column.Definition.Encoding = encoding
				column.Definition.Compression = CompressionNone
			case ColumnTypeFixedBytes:
				column.Definition.Encoding = EncodingRawFixedBytes
				column.Definition.Compression = CompressionNone
			case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
				encoding, ok := PackedUintVectorEncoding(columnType)
				if !ok {
					return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: empty packed_uint column %s type=%s has no raw encoding mapping", name, columnType)
				}
				column.Definition.Encoding = encoding
				column.Definition.Compression = CompressionNone
			case ColumnTypeUint32List:
				column.Definition.Encoding = EncodingRawUint32OffsetsList
				column.Definition.Compression = CompressionNone
			case ColumnTypeBytes:
				column.Definition.Encoding = EncodingRawBytesOffsets
				column.Definition.Compression = CompressionNone
			case ColumnTypeAdjacencyList:
				if fixedWidthElements == 0 {
					column.Definition.Encoding = EncodingRawUint32OffsetsList
					column.Definition.Compression = CompressionNone
				}
			}
		}
		for j := 0; j < blocks; j++ {
			blockDesc, granule, err := decodeColumnBlockDescriptorAndGranule(&dec)
			if err != nil {
				return ColumnPartDescriptor{}, nil, err
			}
			if err := validateDecodedColumnBlockGranuleMetadata(name, j, granule); err != nil {
				return ColumnPartDescriptor{}, nil, err
			}
			if columnType == ColumnTypeBytes && blockDesc.Encoding == EncodingRawBytesOffsets && (granule.NullCount != 0 || granule.DefaultCount != 0) {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: descriptor column %s block %d bytes null/default count=%d/%d want 0/0", name, j, granule.NullCount, granule.DefaultCount)
			}
			if err := validateDecodedColumnBlockDescriptor(desc, name, columnType, cardinality, fixedWidthElements, j, blockDesc); err != nil {
				return ColumnPartDescriptor{}, nil, err
			}
			if blockDesc.FirstRow != expectedFirstRow {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: descriptor column %s block %d first row=%d want contiguous first row=%d", name, j, blockDesc.FirstRow, expectedFirstRow)
			}
			expectedFirstRow += blockDesc.RowCount
			if j == 0 {
				column.Definition.Encoding = blockDesc.Encoding
				column.Definition.Compression = blockDesc.Compression
			}
			columnDesc.Blocks = append(columnDesc.Blocks, blockDesc)
			column.Blocks = append(column.Blocks, ColumnBlock{Descriptor: blockDesc, Granule: granule})
		}
		if err := validateDecodedColumnFixedWidthElements(name, columnType, fixedWidthElements, bitsPerElement, columnDesc.Blocks, desc.RowCount); err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		if expectedFirstRow != desc.RowCount {
			return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: descriptor column %s covers %d rows, want %d", name, expectedFirstRow, desc.RowCount)
		}
		if columnType == ColumnTypeLowCardinalityCode && cardinality == 0 && (len(column.Blocks) == 0 || column.Definition.Encoding != EncodingNullableInt64) {
			return ColumnPartDescriptor{}, nil, fmt.Errorf("typedcolumn: invalid low-cardinality cardinality %d for %s", cardinality, name)
		}
		desc.Columns = append(desc.Columns, columnDesc)
		columns[name] = column
	}
	if err := dec.finish(); err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	return desc, columns, nil
}

func validateDecodedColumnFixedWidthElements(name string, columnType ColumnType, fixedWidthElements int, bitsPerElement int, blocks []ColumnBlockDescriptor, rows int) error {
	switch columnType {
	case ColumnTypeFloat32Vector, ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
		if fixedWidthElements <= 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires positive fixed-width elements", name, columnType)
		}
		if bitsPerElement != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
		}
	case ColumnTypeFixedBytes:
		if fixedWidthElements <= 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires positive fixed-width elements/bytes_per_row", name, columnType)
		}
		if bitsPerElement != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
		}
		for _, block := range blocks {
			if block.Encoding != EncodingRawFixedBytes {
				return fmt.Errorf("typedcolumn: column %s type=%s requires encoding=%s", name, columnType, EncodingRawFixedBytes)
			}
		}
	case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
		wantBits, ok := PackedUintVectorBits(columnType)
		if !ok || fixedWidthElements <= 0 || bitsPerElement != wantBits {
			return fmt.Errorf("typedcolumn: column %s type=%s requires positive fixed-width elements and bits_per_element=%d", name, columnType, wantBits)
		}
		wantEncoding, _ := PackedUintVectorEncoding(columnType)
		for _, block := range blocks {
			if block.Encoding != wantEncoding {
				return fmt.Errorf("typedcolumn: column %s type=%s requires encoding=%s", name, columnType, wantEncoding)
			}
		}
	case ColumnTypeUint32List:
		if bitsPerElement != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
		}
		if fixedWidthElements != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires fixed-width elements=0", name, columnType)
		}
		if len(blocks) == 0 {
			if rows == 0 {
				return nil
			}
			return fmt.Errorf("typedcolumn: column %s type=%s requires blocks for %s", name, columnType, EncodingRawUint32OffsetsList)
		}
		for _, block := range blocks {
			if block.Encoding != EncodingRawUint32OffsetsList {
				return fmt.Errorf("typedcolumn: column %s type=%s requires encoding=%s", name, columnType, EncodingRawUint32OffsetsList)
			}
		}
	case ColumnTypeBytes:
		if bitsPerElement != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
		}
		if fixedWidthElements != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires fixed-width elements=0", name, columnType)
		}
		if len(blocks) == 0 {
			if rows == 0 {
				return nil
			}
			return fmt.Errorf("typedcolumn: column %s type=%s requires blocks for %s", name, columnType, EncodingRawBytesOffsets)
		}
		for _, block := range blocks {
			if block.Encoding != EncodingRawBytesOffsets {
				return fmt.Errorf("typedcolumn: column %s type=%s requires encoding=%s", name, columnType, EncodingRawBytesOffsets)
			}
		}
	case ColumnTypeAdjacencyList:
		if bitsPerElement != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s requires bits_per_element=0", name, columnType)
		}
		if fixedWidthElements < 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s has negative fixed-width elements %d", name, columnType, fixedWidthElements)
		}
		if fixedWidthElements == 0 {
			if len(blocks) == 0 {
				if rows == 0 {
					return nil
				}
				return fmt.Errorf("typedcolumn: column %s type=%s requires blocks for %s", name, columnType, EncodingRawUint32OffsetsList)
			}
			for _, block := range blocks {
				if block.Encoding != EncodingRawUint32OffsetsList {
					return fmt.Errorf("typedcolumn: column %s type=%s requires positive fixed-width elements unless encoding=%s", name, columnType, EncodingRawUint32OffsetsList)
				}
			}
		}
	default:
		if bitsPerElement != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s has bits_per_element %d", name, columnType, bitsPerElement)
		}
		if fixedWidthElements != 0 {
			return fmt.Errorf("typedcolumn: column %s type=%s has fixed-width elements %d", name, columnType, fixedWidthElements)
		}
	}
	return nil
}

func validateDecodedGranuleDescriptors(desc ColumnPartDescriptor) error {
	if desc.RowCount > 0 && len(desc.Granules) == 0 {
		return fmt.Errorf("typedcolumn: descriptor rows=%d has no granules", desc.RowCount)
	}
	expectedFirstRow := 0
	for i, granule := range desc.Granules {
		if granule.Ordinal != i {
			return fmt.Errorf("typedcolumn: granule %d has ordinal=%d", i, granule.Ordinal)
		}
		if granule.RowCount <= 0 {
			return fmt.Errorf("typedcolumn: granule %d has invalid row count %d", i, granule.RowCount)
		}
		if granule.FirstRow != expectedFirstRow {
			return fmt.Errorf("typedcolumn: granule %d first row=%d want %d", i, granule.FirstRow, expectedFirstRow)
		}
		if granule.VisibleRows > granule.RowCount {
			return fmt.Errorf("typedcolumn: granule %d visible rows=%d exceed row count=%d", i, granule.VisibleRows, granule.RowCount)
		}
		if granule.DeletedRows > granule.RowCount {
			return fmt.Errorf("typedcolumn: granule %d deleted rows=%d exceed row count=%d", i, granule.DeletedRows, granule.RowCount)
		}
		if granule.VisibleRows+granule.DeletedRows > granule.RowCount {
			return fmt.Errorf("typedcolumn: granule %d visible+deleted rows=%d exceed row count=%d", i, granule.VisibleRows+granule.DeletedRows, granule.RowCount)
		}
		expectedFirstRow += granule.RowCount
	}
	if expectedFirstRow != desc.RowCount {
		return fmt.Errorf("typedcolumn: descriptor granules cover %d rows, want %d", expectedFirstRow, desc.RowCount)
	}
	return nil
}

func validateDecodedColumnBlockDescriptor(desc ColumnPartDescriptor, column string, columnType ColumnType, cardinality uint32, fixedWidthElements int, blockIndex int, block ColumnBlockDescriptor) error {
	bitsPerElement := 0
	if bits, ok := PackedUintVectorBits(columnType); ok {
		bitsPerElement = bits
	}
	if block.RowCount <= 0 {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d has invalid row count %d", column, blockIndex, block.RowCount)
	}
	if block.FirstRow > desc.RowCount || block.RowCount > desc.RowCount-block.FirstRow {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d first row=%d row count=%d outside part rows=%d", column, blockIndex, block.FirstRow, block.RowCount, desc.RowCount)
	}
	if block.FirstGranule > block.LastGranule {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d granule range [%d,%d] is inverted", column, blockIndex, block.FirstGranule, block.LastGranule)
	}
	if len(desc.Granules) > 0 && block.LastGranule >= len(desc.Granules) {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d last granule=%d outside granules=%d", column, blockIndex, block.LastGranule, len(desc.Granules))
	}
	firstGranule, lastGranule, err := decodedBlockGranuleRange(desc.Granules, block.FirstRow, block.RowCount)
	if err != nil {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d: %w", column, blockIndex, err)
	}
	if block.FirstGranule != firstGranule || block.LastGranule != lastGranule {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d granule range [%d,%d] want [%d,%d] for rows [%d,%d)", column, blockIndex, block.FirstGranule, block.LastGranule, firstGranule, lastGranule, block.FirstRow, block.FirstRow+block.RowCount)
	}
	if (columnType == ColumnTypeUint32List || columnType == ColumnTypeAdjacencyList) && block.Encoding == EncodingRawUint32OffsetsList {
		return validateDecodedOffsetsListColumnBlockDescriptor(column, blockIndex, fixedWidthElements, block)
	}
	if columnType == ColumnTypeBytes && block.Encoding == EncodingRawBytesOffsets {
		return validateDecodedBytesColumnBlockDescriptor(column, blockIndex, fixedWidthElements, block)
	}
	maxRawBytes, err := maxDecodedBlockRawBytes(columnType, cardinality, fixedWidthElements, bitsPerElement, block.Encoding, block.RowCount)
	if err != nil {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d: %w", column, blockIndex, err)
	}
	if block.RawBytes > maxRawBytes {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d raw bytes=%d exceed max=%d for %d rows", column, blockIndex, block.RawBytes, maxRawBytes, block.RowCount)
	}
	if IsDenseFixedWidthVectorColumnType(columnType) || IsPackedUintVectorColumnType(columnType) || columnType == ColumnTypeFixedBytes || columnType == ColumnTypeAdjacencyList {
		if block.Compression != CompressionNone {
			return fmt.Errorf("typedcolumn: descriptor column %s block %d dense compression=%s want %s", column, blockIndex, block.Compression, CompressionNone)
		}
		if block.RawBytes != maxRawBytes {
			return fmt.Errorf("typedcolumn: descriptor column %s block %d dense raw bytes=%d want %d for %d rows", column, blockIndex, block.RawBytes, maxRawBytes, block.RowCount)
		}
	}
	if columnType == ColumnTypeFloat32 || columnType == ColumnTypeFloat64 || rawScalarWidthForColumnType(columnType) != 0 {
		if block.RawBytes != maxRawBytes {
			return fmt.Errorf("typedcolumn: descriptor column %s block %d fixed-width raw bytes=%d want %d for %d rows", column, blockIndex, block.RawBytes, maxRawBytes, block.RowCount)
		}
	}
	if err := validateCompression(block.Compression); err != nil {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d compression=%s is unsupported", column, blockIndex, block.Compression)
	}
	if block.Compression == CompressionNone && block.StoredBytes != block.RawBytes {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d uncompressed stored bytes=%d raw bytes=%d", column, blockIndex, block.StoredBytes, block.RawBytes)
	}
	if block.Compression != CompressionNone && (block.StoredBytes <= 0 || block.StoredBytes >= block.RawBytes) {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d compressed stored bytes=%d raw bytes=%d want 0 < stored < raw", column, blockIndex, block.StoredBytes, block.RawBytes)
	}
	return nil
}

func validateDecodedColumnBlockGranuleMetadata(column string, blockIndex int, granule EncodedGranule) error {
	if granule.Rows <= 0 {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d granule has invalid row count %d", column, blockIndex, granule.Rows)
	}
	if granule.NullCount < 0 {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d granule has negative null count %d", column, blockIndex, granule.NullCount)
	}
	if granule.DefaultCount < 0 {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d granule has negative default count %d", column, blockIndex, granule.DefaultCount)
	}
	if granule.NullCount > granule.Rows || granule.DefaultCount > granule.Rows-granule.NullCount {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d granule null/default count exceeds rows", column, blockIndex)
	}
	if granule.HasMinMax && granule.Min > granule.Max {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d granule min=%d exceeds max=%d", column, blockIndex, granule.Min, granule.Max)
	}
	return nil
}

func validateDecodedOffsetsListColumnBlockDescriptor(column string, blockIndex int, fixedWidthElements int, block ColumnBlockDescriptor) error {
	if fixedWidthElements != 0 {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d raw_uint32_offsets_list fixed_width_elements=%d want 0", column, blockIndex, fixedWidthElements)
	}
	if block.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d offsets-list compression=%s want %s", column, blockIndex, block.Compression, CompressionNone)
	}
	if _, _, err := RawUint32OffsetsListBlockPayloadBytes(block.RowCount, block.RawBytes); err != nil {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d offsets-list raw bytes: %w", column, blockIndex, err)
	}
	if block.StoredBytes != block.RawBytes {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d offsets-list stored bytes=%d raw bytes=%d", column, blockIndex, block.StoredBytes, block.RawBytes)
	}
	return nil
}

func validateDecodedBytesColumnBlockDescriptor(column string, blockIndex int, fixedWidthElements int, block ColumnBlockDescriptor) error {
	if fixedWidthElements != 0 {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d raw_bytes_offsets fixed_width_elements=%d want 0", column, blockIndex, fixedWidthElements)
	}
	if block.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d bytes compression=%s want %s", column, blockIndex, block.Compression, CompressionNone)
	}
	if _, _, err := RawBytesOffsetsBlockPayloadBytes(block.RowCount, block.RawBytes); err != nil {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d bytes raw bytes: %w", column, blockIndex, err)
	}
	if block.StoredBytes != block.RawBytes {
		return fmt.Errorf("typedcolumn: descriptor column %s block %d bytes stored bytes=%d raw bytes=%d", column, blockIndex, block.StoredBytes, block.RawBytes)
	}
	return nil
}

func decodedBlockGranuleRange(granules []GranuleDescriptor, firstRow int, rowCount int) (int, int, error) {
	if rowCount <= 0 {
		return 0, 0, fmt.Errorf("invalid block row count %d", rowCount)
	}
	firstGranule, ok := decodedGranuleIndexForRow(granules, firstRow)
	if !ok {
		return 0, 0, fmt.Errorf("first row=%d is not covered by descriptor granules", firstRow)
	}
	lastRow := firstRow + rowCount - 1
	lastGranule, ok := decodedGranuleIndexForRow(granules, lastRow)
	if !ok {
		return 0, 0, fmt.Errorf("last row=%d is not covered by descriptor granules", lastRow)
	}
	return firstGranule, lastGranule, nil
}

func decodedGranuleIndexForRow(granules []GranuleDescriptor, row int) (int, bool) {
	i := sort.Search(len(granules), func(i int) bool {
		granule := granules[i]
		return granule.FirstRow+granule.RowCount > row
	})
	if i < len(granules) {
		granule := granules[i]
		if row >= granule.FirstRow && row < granule.FirstRow+granule.RowCount {
			return i, true
		}
	}
	return 0, false
}

func maxDecodedBlockRawBytes(columnType ColumnType, cardinality uint32, fixedWidthElements int, bitsPerElement int, encoding Encoding, rows int) (int, error) {
	switch columnType {
	case ColumnTypeInt64:
		switch encoding {
		case EncodingRawInt64:
			return checkedMulInt(rows, 8, "raw int64 bytes")
		case EncodingDeltaVarint, EncodingDoubleDeltaVarint:
			return checkedMulInt(rows, binary.MaxVarintLen64, "varint bytes")
		case EncodingNullableInt64:
			return maxNullableInt64RawBytes(rows, binary.MaxVarintLen64)
		default:
			return 0, fmt.Errorf("unsupported int64 encoding %d", encoding)
		}
	case ColumnTypeLowCardinalityCode:
		if encoding == EncodingNullableInt64 {
			return maxNullableInt64RawBytes(rows, 8)
		}
		if encoding != EncodingLowCardinalityUint32 {
			return 0, fmt.Errorf("unsupported low-cardinality encoding %d", encoding)
		}
		if cardinality == 0 || cardinality > maxCodeCardinality {
			return 0, fmt.Errorf("invalid low-cardinality cardinality %d", cardinality)
		}
		codeBytes, err := checkedMulInt(rows, 4, "low-cardinality code bytes")
		if err != nil {
			return 0, err
		}
		return checkedAddInt(1+binary.MaxVarintLen64, codeBytes, "low-cardinality raw bytes")
	case ColumnTypeBool:
		if encoding == EncodingNullableInt64 {
			return maxNullableInt64RawBytes(rows, 8)
		}
		if encoding != EncodingBoolBitpackRLE {
			return 0, fmt.Errorf("unsupported bool encoding %d", encoding)
		}
		rleBytes, err := checkedMulInt(rows, binary.MaxVarintLen64, "bool rle bytes")
		if err != nil {
			return 0, err
		}
		return checkedAddInt(2, rleBytes, "bool raw bytes")
	case ColumnTypeFloat32:
		if encoding != EncodingRawFloat32 {
			return 0, fmt.Errorf("unsupported float32 encoding %d", encoding)
		}
		return checkedMulInt(rows, 4, "float32 raw bytes")
	case ColumnTypeFloat64:
		if encoding != EncodingRawFloat64 {
			return 0, fmt.Errorf("unsupported float64 encoding %d", encoding)
		}
		return checkedMulInt(rows, 8, "float64 raw bytes")
	case ColumnTypeInt8, ColumnTypeUint8, ColumnTypeInt16, ColumnTypeUint16, ColumnTypeInt32, ColumnTypeUint32, ColumnTypeUint64, ColumnTypeFloat16, ColumnTypeBFloat16:
		if fixedWidthElements != 0 {
			return 0, fmt.Errorf("unsupported %s fixed_width_elements=%d", columnType, fixedWidthElements)
		}
		want := rawScalarEncodingForColumnType(columnType)
		if want == 0 || encoding != want {
			return 0, fmt.Errorf("unsupported %s encoding %d", columnType, encoding)
		}
		width := rawScalarWidthForColumnType(columnType)
		if width == 0 {
			return 0, fmt.Errorf("unsupported %s fixed-width bytes", columnType)
		}
		return checkedMulInt(rows, width, string(columnType)+" raw bytes")
	case ColumnTypeFloat32Vector:
		if encoding != EncodingRawFloat32Vector {
			return 0, fmt.Errorf("unsupported float32_vector encoding %d", encoding)
		}
		elements, err := checkedMulInt(rows, fixedWidthElements, "float32_vector elements")
		if err != nil {
			return 0, err
		}
		return checkedMulInt(elements, 4, "float32_vector raw bytes")
	case ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
		wantEncoding, ok := DenseFixedWidthVectorEncoding(columnType)
		if !ok || encoding != wantEncoding {
			return 0, fmt.Errorf("unsupported %s encoding %d", columnType, encoding)
		}
		width, ok := DenseFixedWidthVectorElementWidth(columnType)
		if !ok || width == 0 {
			return 0, fmt.Errorf("unsupported %s fixed-width bytes", columnType)
		}
		elements, err := checkedMulInt(rows, fixedWidthElements, string(columnType)+" elements")
		if err != nil {
			return 0, err
		}
		return checkedMulInt(elements, width, string(columnType)+" raw bytes")
	case ColumnTypeFixedBytes:
		if encoding != EncodingRawFixedBytes {
			return 0, fmt.Errorf("unsupported fixed_bytes encoding %d", encoding)
		}
		if bitsPerElement != 0 {
			return 0, fmt.Errorf("unsupported fixed_bytes bits_per_element=%d", bitsPerElement)
		}
		return FixedBytesPayloadBytes(rows, fixedWidthElements)
	case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
		wantBits, ok := PackedUintVectorBits(columnType)
		wantEncoding, encOK := PackedUintVectorEncoding(columnType)
		if !ok || !encOK || bitsPerElement != wantBits || encoding != wantEncoding {
			return 0, fmt.Errorf("unsupported %s encoding/bits (%d,%d)", columnType, encoding, bitsPerElement)
		}
		return PackedUintPayloadBytes(rows, fixedWidthElements, bitsPerElement)
	case ColumnTypeUint32List:
		return 0, fmt.Errorf("unsupported uint32_list encoding %d", encoding)
	case ColumnTypeBytes:
		return 0, fmt.Errorf("unsupported bytes encoding %d", encoding)
	case ColumnTypeAdjacencyList:
		if encoding != EncodingRawUint32Dense {
			return 0, fmt.Errorf("unsupported adjacency_list encoding %d", encoding)
		}
		elements, err := checkedMulInt(rows, fixedWidthElements, "adjacency_list elements")
		if err != nil {
			return 0, err
		}
		return checkedMulInt(elements, 4, "adjacency_list raw bytes")
	default:
		return 0, fmt.Errorf("unsupported column type %s", columnType)
	}
}

func maxNullableInt64RawBytes(rows int, valueBytesPerRow int) (int, error) {
	valueBytes, err := checkedMulInt(rows, valueBytesPerRow, "nullable int64 value bytes")
	if err != nil {
		return 0, err
	}
	maskBytes := (rows + 7) / 8
	masks, err := checkedMulInt(maskBytes, 2, "nullable int64 mask bytes")
	if err != nil {
		return 0, err
	}
	withMasks, err := checkedAddInt(nullableInt64HeaderBytes, masks, "nullable int64 raw bytes")
	if err != nil {
		return 0, err
	}
	return checkedAddInt(withMasks, valueBytes, "nullable int64 raw bytes")
}

func checkedMulInt(a int, b int, field string) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("%s has negative operand %d*%d", field, a, b)
	}
	if b != 0 && a > int(^uint(0)>>1)/b {
		return 0, fmt.Errorf("%s overflow %d*%d", field, a, b)
	}
	return a * b, nil
}

func checkedAddInt(a int, b int, field string) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("%s has negative operand %d+%d", field, a, b)
	}
	if a > int(^uint(0)>>1)-b {
		return 0, fmt.Errorf("%s overflow %d+%d", field, a, b)
	}
	return a + b, nil
}

func uint32ToInt(v uint32, field string) (int, error) {
	if uint64(int(v)) != uint64(v) {
		return 0, fmt.Errorf("typedcolumn: %s=%d exceeds host int", field, v)
	}
	return int(v), nil
}

func decodeGranuleDescriptor(dec *columnPartImageDecoder) (GranuleDescriptor, error) {
	ordinal, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	firstRow, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	rowCount, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	visibleRows, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	deletedRows, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	idLower, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	idUpper, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	markOrdinal, err := dec.i64()
	if err != nil {
		return GranuleDescriptor{}, err
	}
	ordinalInt, err := nonNegativeInt64ToInt(ordinal, "granule ordinal")
	if err != nil {
		return GranuleDescriptor{}, err
	}
	firstRowInt, err := nonNegativeInt64ToInt(firstRow, "granule first row")
	if err != nil {
		return GranuleDescriptor{}, err
	}
	rowCountInt, err := nonNegativeInt64ToInt(rowCount, "granule row count")
	if err != nil {
		return GranuleDescriptor{}, err
	}
	visibleRowsInt, err := nonNegativeInt64ToInt(visibleRows, "granule visible rows")
	if err != nil {
		return GranuleDescriptor{}, err
	}
	deletedRowsInt, err := nonNegativeInt64ToInt(deletedRows, "granule deleted rows")
	if err != nil {
		return GranuleDescriptor{}, err
	}
	markOrdinalInt, err := nonNegativeInt64ToInt(markOrdinal, "granule mark ordinal")
	if err != nil {
		return GranuleDescriptor{}, err
	}
	return GranuleDescriptor{
		Ordinal:          ordinalInt,
		FirstRow:         firstRowInt,
		RowCount:         rowCountInt,
		VisibleRows:      visibleRowsInt,
		DeletedRows:      deletedRowsInt,
		IDLower:          idLower,
		IDUpperExclusive: idUpper,
		MarkOrdinal:      markOrdinalInt,
	}, nil
}

func decodeColumnBlockDescriptorAndGranule(dec *columnPartImageDecoder) (ColumnBlockDescriptor, EncodedGranule, error) {
	firstRow, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	rowCount, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	firstGranule, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	lastGranule, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	encodingCode, err := dec.u16()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	compressionCode, err := dec.u16()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	rawBytes, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	storedBytes, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	ordinal, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	nullCount, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	defaultCount, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	hasMinMax, err := dec.boolean()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	minValue, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	maxValue, err := dec.i64()
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	firstRowInt, err := nonNegativeInt64ToInt(firstRow, "column block first row")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	rowCountInt, err := nonNegativeInt64ToInt(rowCount, "column block row count")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	firstGranuleInt, err := nonNegativeInt64ToInt(firstGranule, "column block first granule")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	lastGranuleInt, err := nonNegativeInt64ToInt(lastGranule, "column block last granule")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	rawBytesInt, err := nonNegativeInt64ToInt(rawBytes, "column block raw bytes")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	storedBytesInt, err := nonNegativeInt64ToInt(storedBytes, "column block stored bytes")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	ordinalInt, err := nonNegativeInt64ToInt(ordinal, "column block ordinal")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	nullCountInt, err := nonNegativeInt64ToInt(nullCount, "granule null count")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	defaultCountInt, err := nonNegativeInt64ToInt(defaultCount, "granule default count")
	if err != nil {
		return ColumnBlockDescriptor{}, EncodedGranule{}, err
	}
	encoding := Encoding(encodingCode)
	compression := Compression(compressionCode)
	desc := ColumnBlockDescriptor{
		FirstRow:          firstRowInt,
		RowCount:          rowCountInt,
		FirstGranule:      firstGranuleInt,
		LastGranule:       lastGranuleInt,
		Encoding:          encoding,
		Compression:       compression,
		RawBytes:          rawBytesInt,
		StoredBytes:       storedBytesInt,
		CodecBlockOrdinal: ordinalInt,
	}
	granule := EncodedGranule{
		Rows:         rowCountInt,
		NullCount:    nullCountInt,
		DefaultCount: defaultCountInt,
		HasMinMax:    hasMinMax,
		Min:          minValue,
		Max:          maxValue,
		Encoding:     encoding,
		Compression:  compression,
		RawBytes:     rawBytesInt,
		StoredBytes:  storedBytesInt,
		PayloadRef: PayloadRef{
			Kind:   PayloadRefInline,
			Length: storedBytesInt,
		},
		CodecReport: CodecReport{
			Encoding:             encoding,
			ActualCompression:    compression,
			RequestedCompression: compression,
			RawBytes:             rawBytesInt,
			StoredBytes:          storedBytesInt,
		},
	}
	return desc, granule, nil
}

func applyImageSectionCompressionToColumns(image ColumnPartImage, columns map[string]ColumnPartColumn) error {
	for _, section := range image.Sections {
		if section.Kind != ColumnPartImageSectionColumnData {
			continue
		}
		column, ok := columns[section.Column]
		if !ok {
			return fmt.Errorf("typedcolumn: image column data section %s missing decoded column", section.Column)
		}
		if section.Encoding != column.Definition.Encoding {
			return fmt.Errorf("typedcolumn: image column %s section encoding=%s want %s", section.Column, section.Encoding, column.Definition.Encoding)
		}
		column.Definition.Compression = section.Compression
		columns[section.Column] = column
	}
	return nil
}

func decodeSortKeyMetadataSection(image ColumnPartImage) ([]SortKeyColumn, error) {
	section, err := image.singleSection(ColumnPartImageSectionSortKeyMetadata)
	if err != nil {
		return nil, err
	}
	return DecodeColumnPartSortKeyMetadataSectionPayload(image.sectionBytes(section))
}

func DecodeColumnPartSortKeyMetadataSectionPayload(data []byte) ([]SortKeyColumn, error) {
	dec := columnPartImageDecoder{data: data}
	count, err := dec.u32()
	if err != nil {
		return nil, err
	}
	total, err := dec.boundedCount(count, 12, "sort key columns")
	if err != nil {
		return nil, err
	}
	out := make([]SortKeyColumn, 0, total)
	for i := 0; i < total; i++ {
		column, err := dec.str()
		if err != nil {
			return nil, err
		}
		direction, err := dec.str()
		if err != nil {
			return nil, err
		}
		nulls, err := dec.str()
		if err != nil {
			return nil, err
		}
		out = append(out, SortKeyColumn{Column: column, Direction: SortKeyDirection(direction), Nulls: SortKeyNullOrder(nulls)})
	}
	if err := dec.finish(); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeSortKeyMarksSection(image ColumnPartImage) ([]SortKeyMark, error) {
	section, err := image.singleSection(ColumnPartImageSectionSortKeyMarks)
	if err != nil {
		return nil, err
	}
	return DecodeColumnPartSortKeyMarksSectionPayload(image.sectionBytes(section))
}

func DecodeColumnPartSortKeyMarksSectionPayload(data []byte) ([]SortKeyMark, error) {
	dec := columnPartImageDecoder{data: data}
	count, err := dec.u32()
	if err != nil {
		return nil, err
	}
	total, err := dec.boundedCount(count, 16, "sort key marks")
	if err != nil {
		return nil, err
	}
	out := make([]SortKeyMark, 0, total)
	for i := 0; i < total; i++ {
		rows, err := dec.i64()
		if err != nil {
			return nil, err
		}
		rowsInt, err := nonNegativeInt64ToInt(rows, "sort key mark rows")
		if err != nil {
			return nil, err
		}
		columns, err := dec.stringSlice()
		if err != nil {
			return nil, err
		}
		prefixCount, err := dec.u32()
		if err != nil {
			return nil, err
		}
		prefixes, err := dec.boundedCount(prefixCount, 20, "sort key mark prefixes")
		if err != nil {
			return nil, err
		}
		mark := SortKeyMark{Rows: rowsInt, Columns: columns, Prefixes: make([]SortKeyPrefixSummary, 0, prefixes)}
		for j := 0; j < prefixes; j++ {
			prefixColumns, err := dec.stringSlice()
			if err != nil {
				return nil, err
			}
			lower, err := decodeSortKeyBound(&dec)
			if err != nil {
				return nil, err
			}
			upper, err := decodeSortKeyBound(&dec)
			if err != nil {
				return nil, err
			}
			mark.Prefixes = append(mark.Prefixes, SortKeyPrefixSummary{
				Columns:        prefixColumns,
				Lower:          lower,
				UpperExclusive: upper,
			})
		}
		if err := validateDecodedSortKeyMark(mark, i); err != nil {
			return nil, err
		}
		out = append(out, mark)
	}
	if err := dec.finish(); err != nil {
		return nil, err
	}
	return out, nil
}

func validateDecodedSortKeyMark(mark SortKeyMark, index int) error {
	if mark.Rows <= 0 {
		return fmt.Errorf("typedcolumn: sort key mark %d has invalid row count %d", index, mark.Rows)
	}
	if len(mark.Columns) == 0 {
		return fmt.Errorf("typedcolumn: sort key mark %d has no columns", index)
	}
	if len(mark.Columns) > maxSortKeyColumns {
		return fmt.Errorf("typedcolumn: sort key mark %d columns=%d exceeds cap %d", index, len(mark.Columns), maxSortKeyColumns)
	}
	for columnIndex, column := range mark.Columns {
		if column == "" {
			return fmt.Errorf("typedcolumn: sort key mark %d has empty column name at %d", index, columnIndex)
		}
	}
	if len(mark.Prefixes) != len(mark.Columns) {
		return fmt.Errorf("typedcolumn: sort key mark %d prefixes=%d want columns=%d", index, len(mark.Prefixes), len(mark.Columns))
	}
	for prefixIndex, prefix := range mark.Prefixes {
		prefixLen := prefixIndex + 1
		if !sameStringSlice(prefix.Columns, mark.Columns[:prefixLen]) {
			return fmt.Errorf("typedcolumn: sort key mark %d prefix %d columns=%v want %v", index, prefixIndex, prefix.Columns, mark.Columns[:prefixLen])
		}
		if prefix.Lower.Exclusive {
			return fmt.Errorf("typedcolumn: sort key mark %d prefix %d lower bound is exclusive", index, prefixIndex)
		}
		if prefix.Lower.Unbounded {
			return fmt.Errorf("typedcolumn: sort key mark %d prefix %d lower bound is unbounded", index, prefixIndex)
		}
		if len(prefix.Lower.Values) != prefixLen {
			return fmt.Errorf("typedcolumn: sort key mark %d prefix %d lower values=%d want %d", index, prefixIndex, len(prefix.Lower.Values), prefixLen)
		}
		if !prefix.UpperExclusive.Exclusive {
			return fmt.Errorf("typedcolumn: sort key mark %d prefix %d upper bound is not exclusive", index, prefixIndex)
		}
		if prefix.UpperExclusive.Unbounded {
			if len(prefix.UpperExclusive.Values) != 0 {
				return fmt.Errorf("typedcolumn: sort key mark %d prefix %d unbounded upper has %d values", index, prefixIndex, len(prefix.UpperExclusive.Values))
			}
			continue
		}
		if len(prefix.UpperExclusive.Values) != prefixLen {
			return fmt.Errorf("typedcolumn: sort key mark %d prefix %d upper values=%d want %d", index, prefixIndex, len(prefix.UpperExclusive.Values), prefixLen)
		}
		if compareInt64Tuple(prefix.UpperExclusive.Values, prefix.Lower.Values) <= 0 {
			return fmt.Errorf("typedcolumn: sort key mark %d prefix %d upper bound %v is not greater than lower bound %v", index, prefixIndex, prefix.UpperExclusive.Values, prefix.Lower.Values)
		}
	}
	return nil
}

func validateDecodedSortKeyMarks(desc ColumnPartDescriptor, marks []SortKeyMark) error {
	if len(desc.SortKey) == 0 {
		return fmt.Errorf("typedcolumn: descriptor has no sort key")
	}
	if len(marks) != len(desc.Granules) {
		return fmt.Errorf("typedcolumn: sort key marks=%d granules=%d", len(marks), len(desc.Granules))
	}
	expectedColumns := make([]string, len(desc.SortKey))
	for i, column := range desc.SortKey {
		if column.Column == "" {
			return fmt.Errorf("typedcolumn: descriptor sort key has empty column at %d", i)
		}
		expectedColumns[i] = column.Column
	}
	for i, mark := range marks {
		if !sameStringSlice(mark.Columns, expectedColumns) {
			return fmt.Errorf("typedcolumn: sort key mark %d columns=%v want descriptor sort key %v", i, mark.Columns, expectedColumns)
		}
	}
	for i, granule := range desc.Granules {
		if granule.MarkOrdinal != i {
			return fmt.Errorf("typedcolumn: granule %d mark ordinal=%d want %d", i, granule.MarkOrdinal, i)
		}
		if granule.MarkOrdinal >= len(marks) {
			return fmt.Errorf("typedcolumn: granule %d mark ordinal=%d outside marks=%d", i, granule.MarkOrdinal, len(marks))
		}
		if marks[i].Rows != granule.RowCount {
			return fmt.Errorf("typedcolumn: granule %d rows=%d mark rows=%d", i, granule.RowCount, marks[i].Rows)
		}
	}
	return nil
}

func ValidateColumnPartSortKeyMarks(desc ColumnPartDescriptor, marks []SortKeyMark) error {
	return validateDecodedSortKeyMarks(desc, marks)
}

func decodeSortKeyBound(dec *columnPartImageDecoder) (SortKeyBound, error) {
	exclusive, err := dec.boolean()
	if err != nil {
		return SortKeyBound{}, err
	}
	unbounded, err := dec.boolean()
	if err != nil {
		return SortKeyBound{}, err
	}
	values, err := dec.int64Slice()
	if err != nil {
		return SortKeyBound{}, err
	}
	return SortKeyBound{Values: values, Exclusive: exclusive, Unbounded: unbounded}, nil
}

func decodeRowLocatorsSection(image ColumnPartImage, desc ColumnPartDescriptor) (map[int64]RowLocator, error) {
	section, err := image.singleSection(ColumnPartImageSectionRowLocators)
	if err != nil {
		return nil, err
	}
	data, err := image.rowLocatorSectionBytes(section)
	if err != nil {
		return nil, err
	}
	switch section.Encoding {
	case 0:
		return decodeRawRowLocatorsSection(data)
	case EncodingRowLocatorContiguous:
		return decodeContiguousRowLocatorSection(data, desc, section)
	default:
		return nil, fmt.Errorf("typedcolumn: row locators encoding=%s is unsupported", section.Encoding)
	}
}

func decodeRawRowLocatorsSection(data []byte) (map[int64]RowLocator, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("typedcolumn: truncated row locators section bytes=%d", len(data))
	}
	count := binary.LittleEndian.Uint32(data[:4])
	maxLocators := (len(data) - 4) / rowLocatorBytes
	if uint64(count) > uint64(maxLocators) {
		return nil, fmt.Errorf("typedcolumn: row locators count=%d exceeds section capacity=%d", count, maxLocators)
	}
	total, err := uint32ToInt(count, "row locators count")
	if err != nil {
		return nil, err
	}
	recordBytes, err := checkedMulInt(total, rowLocatorBytes, "row locator section bytes")
	if err != nil {
		return nil, err
	}
	wantBytes, err := checkedAddInt(4, recordBytes, "row locator section bytes")
	if err != nil {
		return nil, err
	}
	if len(data) != wantBytes {
		return nil, fmt.Errorf("typedcolumn: row locators section bytes=%d want %d", len(data), wantBytes)
	}
	out := make(map[int64]RowLocator, total)
	offset := 4
	for i := 0; i < total; i++ {
		primaryID := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
		partID := binary.LittleEndian.Uint64(data[offset:])
		offset += 8
		partRow := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		granuleOrdinal := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		rowInGranule := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		reserved := binary.LittleEndian.Uint32(data[offset:])
		offset += 4
		if reserved != 0 {
			return nil, fmt.Errorf("typedcolumn: row locator primary id %d reserved=%d want 0", primaryID, reserved)
		}
		partRowInt, err := uint32ToInt(partRow, "row locator part row")
		if err != nil {
			return nil, err
		}
		granuleOrdinalInt, err := uint32ToInt(granuleOrdinal, "row locator granule ordinal")
		if err != nil {
			return nil, err
		}
		rowInGranuleInt, err := uint32ToInt(rowInGranule, "row locator row in granule")
		if err != nil {
			return nil, err
		}
		if _, exists := out[primaryID]; exists {
			return nil, fmt.Errorf("typedcolumn: duplicate row locator primary id %d", primaryID)
		}
		out[primaryID] = RowLocator{
			PrimaryID:      primaryID,
			PartID:         partID,
			PartRow:        partRowInt,
			GranuleOrdinal: granuleOrdinalInt,
			RowInGranule:   rowInGranuleInt,
		}
	}
	return out, nil
}

func decodeContiguousRowLocatorSection(data []byte, desc ColumnPartDescriptor, section ColumnPartImageSection) (map[int64]RowLocator, error) {
	if len(data) != rowLocatorContiguousPayloadBytes {
		return nil, fmt.Errorf("typedcolumn: contiguous row locator section bytes=%d want %d", len(data), rowLocatorContiguousPayloadBytes)
	}
	dec := columnPartImageDecoder{data: data}
	magic, err := dec.u32()
	if err != nil {
		return nil, err
	}
	if magic != rowLocatorContiguousMagic {
		return nil, fmt.Errorf("typedcolumn: invalid contiguous row locator magic 0x%x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return nil, err
	}
	if version != rowLocatorContiguousVersion {
		return nil, fmt.Errorf("typedcolumn: unsupported contiguous row locator version %d", version)
	}
	reserved, err := dec.u16()
	if err != nil {
		return nil, err
	}
	if reserved != 0 {
		return nil, fmt.Errorf("typedcolumn: contiguous row locator reserved=%d want 0", reserved)
	}
	partID, err := dec.u64()
	if err != nil {
		return nil, err
	}
	if partID != desc.PartID {
		return nil, fmt.Errorf("typedcolumn: contiguous row locator part id=%d want %d", partID, desc.PartID)
	}
	rows, err := dec.u64()
	if err != nil {
		return nil, err
	}
	if uint64(int(rows)) != rows {
		return nil, fmt.Errorf("typedcolumn: contiguous row locator rows=%d exceed host int", rows)
	}
	rowCount := int(rows)
	if rowCount != desc.RowCount || rowCount != section.Rows {
		return nil, fmt.Errorf("typedcolumn: contiguous row locator rows=%d want descriptor=%d section=%d", rowCount, desc.RowCount, section.Rows)
	}
	base, err := dec.i64()
	if err != nil {
		return nil, err
	}
	if rowCount > 0 && base > math.MaxInt64-int64(rowCount-1) {
		return nil, fmt.Errorf("typedcolumn: contiguous row locator base=%d rows=%d exceeds int64 primary id range", base, rowCount)
	}
	out := make(map[int64]RowLocator, rowCount)
	for granuleIndex, granule := range desc.Granules {
		for rowInGranule := 0; rowInGranule < granule.RowCount; rowInGranule++ {
			partRow := granule.FirstRow + rowInGranule
			if partRow < 0 || partRow >= rowCount {
				return nil, fmt.Errorf("typedcolumn: contiguous row locator granule %d part row=%d outside rows=%d", granuleIndex, partRow, rowCount)
			}
			primaryID := base + int64(partRow)
			if _, exists := out[primaryID]; exists {
				return nil, fmt.Errorf("typedcolumn: duplicate contiguous row locator primary id %d", primaryID)
			}
			out[primaryID] = RowLocator{
				PrimaryID:      primaryID,
				PartID:         partID,
				PartRow:        partRow,
				GranuleOrdinal: granuleIndex,
				RowInGranule:   rowInGranule,
			}
		}
	}
	if len(out) != rowCount {
		return nil, fmt.Errorf("typedcolumn: contiguous row locator synthesized rows=%d want %d", len(out), rowCount)
	}
	return out, nil
}

func validateDecodedRowLocators(desc ColumnPartDescriptor, partID uint64, locators map[int64]RowLocator) error {
	if len(locators) != desc.RowCount {
		return fmt.Errorf("typedcolumn: row locator count=%d want part rows=%d", len(locators), desc.RowCount)
	}
	seenRows := make([]bool, desc.RowCount)
	for primaryID, locator := range locators {
		if locator.PrimaryID != primaryID {
			return fmt.Errorf("typedcolumn: row locator key %d has primary id %d", primaryID, locator.PrimaryID)
		}
		if locator.PartID != partID {
			return fmt.Errorf("typedcolumn: row locator primary id %d part id=%d want %d", primaryID, locator.PartID, partID)
		}
		if locator.PartRow < 0 || locator.PartRow >= desc.RowCount {
			return fmt.Errorf("typedcolumn: row locator primary id %d part row=%d outside part rows=%d", primaryID, locator.PartRow, desc.RowCount)
		}
		if locator.GranuleOrdinal < 0 || locator.GranuleOrdinal >= len(desc.Granules) {
			return fmt.Errorf("typedcolumn: row locator primary id %d granule ordinal=%d outside granules=%d", primaryID, locator.GranuleOrdinal, len(desc.Granules))
		}
		granule := desc.Granules[locator.GranuleOrdinal]
		if locator.RowInGranule < 0 || locator.RowInGranule >= granule.RowCount {
			return fmt.Errorf("typedcolumn: row locator primary id %d row in granule=%d outside granule rows=%d", primaryID, locator.RowInGranule, granule.RowCount)
		}
		partRow := granule.FirstRow + locator.RowInGranule
		if locator.PartRow != partRow {
			return fmt.Errorf("typedcolumn: row locator primary id %d part row=%d want %d from granule %d", primaryID, locator.PartRow, partRow, locator.GranuleOrdinal)
		}
		if seenRows[locator.PartRow] {
			return fmt.Errorf("typedcolumn: duplicate row locator part row %d", locator.PartRow)
		}
		seenRows[locator.PartRow] = true
	}
	return nil
}

func validateDecodedRowLocatorsPrimaryKey(desc ColumnPartDescriptor, columns map[string]ColumnPartColumn, locators map[int64]RowLocator) error {
	if len(desc.LogicalPrimaryKey) != 1 {
		return fmt.Errorf("typedcolumn: row locator validation requires one logical primary key column, got %d", len(desc.LogicalPrimaryKey))
	}
	primaryColumnName := desc.LogicalPrimaryKey[0]
	column, ok := columns[primaryColumnName]
	if !ok {
		return fmt.Errorf("typedcolumn: row locator validation missing primary key column %s", primaryColumnName)
	}
	if column.Definition.Type != ColumnTypeInt64 {
		return fmt.Errorf("typedcolumn: row locator validation primary key column %s type=%s want int64", primaryColumnName, column.Definition.Type)
	}
	var reader GranuleReader
	var scratch []int64
	for blockIndex, block := range column.Blocks {
		values, err := reader.DecodeInt64Into(scratch[:0], block.Granule)
		if err != nil {
			return fmt.Errorf("typedcolumn: decode primary key column %s block %d: %w", primaryColumnName, blockIndex, err)
		}
		scratch = values
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("typedcolumn: primary key column %s block %d decoded rows=%d want %d", primaryColumnName, blockIndex, len(values), block.Descriptor.RowCount)
		}
		for rowOffset, primaryID := range values {
			partRow := block.Descriptor.FirstRow + rowOffset
			locator, ok := locators[primaryID]
			if !ok {
				return fmt.Errorf("typedcolumn: row locator missing primary id %d for part row %d", primaryID, partRow)
			}
			if locator.PartRow != partRow {
				return fmt.Errorf("typedcolumn: row locator primary id %d points to part row %d want %d", primaryID, locator.PartRow, partRow)
			}
		}
	}
	return nil
}

func attachColumnPayloadsFromImage(image ColumnPartImage, columns map[string]ColumnPartColumn) error {
	if err := validateColumnDataSectionsForColumns(image, columns); err != nil {
		return err
	}
	for name, column := range columns {
		if column.Definition.Encoding == EncodingRawUint32OffsetsList {
			if err := attachUint32OffsetsListColumnPayloadsFromImage(image, name, column, columns); err != nil {
				return err
			}
			continue
		}
		if column.Definition.Encoding == EncodingRawBytesOffsets {
			if err := attachBytesColumnPayloadsFromImage(image, name, column, columns); err != nil {
				return err
			}
			continue
		}
		section, ok := image.columnDataSection(name)
		if !ok {
			return fmt.Errorf("typedcolumn: image missing column data section %s", name)
		}
		offset := section.Offset
		sectionEnd := section.Offset + section.Length
		for i := range column.Blocks {
			block := &column.Blocks[i]
			length := block.Descriptor.StoredBytes
			if length < 0 || offset > sectionEnd || length > sectionEnd-offset {
				return fmt.Errorf("typedcolumn: image column %s block %d length=%d outside section", name, i, length)
			}
			block.Granule.Payload = image.Bytes[offset : offset+length]
			// The payload slice is already narrowed to the block bytes; keep inline refs normalized for granule validation.
			block.Granule.PayloadRef = PayloadRef{Kind: PayloadRefInline, Length: length}
			if err := validateAttachedColumnPayloadShape(name, column.Definition, *block); err != nil {
				return err
			}
			offset += length
		}
		if offset != sectionEnd {
			return fmt.Errorf("typedcolumn: image column %s consumed=%d section=%d", name, offset-section.Offset, section.Length)
		}
		columns[name] = column
	}
	return nil
}

func validateAttachedColumnPayloadShape(name string, def ColumnDefinition, block ColumnBlock) error {
	switch def.Type {
	case ColumnTypeFixedBytes:
		if _, err := NewFixedBytesRows(block.Descriptor.RowCount, def.FixedWidthElements, block.Granule.Payload); err != nil {
			return fmt.Errorf("typedcolumn: image column %s block %d fixed_bytes payload: %w", name, block.Descriptor.CodecBlockOrdinal, err)
		}
	case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
		bitsPerElement, ok := PackedUintVectorBits(def.Type)
		if !ok || def.BitsPerElement != bitsPerElement {
			return fmt.Errorf("typedcolumn: image column %s block %d packed_uint bits_per_element=%d", name, block.Descriptor.CodecBlockOrdinal, def.BitsPerElement)
		}
		if _, err := NewPackedUintRows(block.Descriptor.RowCount, def.FixedWidthElements, bitsPerElement, block.Granule.Payload); err != nil {
			return fmt.Errorf("typedcolumn: image column %s block %d packed_uint payload: %w", name, block.Descriptor.CodecBlockOrdinal, err)
		}
	}
	return nil
}

func attachUint32OffsetsListColumnPayloadsFromImage(image ColumnPartImage, name string, column ColumnPartColumn, columns map[string]ColumnPartColumn) error {
	offsetsSection, valuesSection, ok := image.columnOffsetsListSections(name)
	if !ok {
		return fmt.Errorf("typedcolumn: image missing offsets-list sections %s", name)
	}
	offsetsRaw := image.sectionBytes(offsetsSection)
	valuesRaw := image.sectionBytes(valuesSection)
	if err := ValidateRawUint32OffsetsListSections(offsetsSection, valuesSection, offsetsRaw, valuesRaw, image.Rows); err != nil {
		return err
	}
	global, err := DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, image.Rows)
	if err != nil {
		return err
	}
	expectedFirstRow := 0
	for i := range column.Blocks {
		block := &column.Blocks[i]
		if block.Descriptor.FirstRow != expectedFirstRow {
			return fmt.Errorf("typedcolumn: image column %s block %d first_row=%d want %d", name, i, block.Descriptor.FirstRow, expectedFirstRow)
		}
		if block.Descriptor.RowCount <= 0 || block.Descriptor.FirstRow > global.Rows-block.Descriptor.RowCount {
			return fmt.Errorf("typedcolumn: image column %s block %d row span [%d,%d) outside rows=%d", name, i, block.Descriptor.FirstRow, block.Descriptor.FirstRow+block.Descriptor.RowCount, global.Rows)
		}
		first := block.Descriptor.FirstRow
		begin := global.Offsets[first]
		end := global.Offsets[first+block.Descriptor.RowCount]
		if begin > end || end > maxHostIntUint64() {
			return fmt.Errorf("typedcolumn: image column %s block %d offsets range [%d,%d) invalid", name, i, begin, end)
		}
		beginInt := int(begin)
		endInt := int(end)
		if beginInt > len(global.Values) || endInt > len(global.Values) {
			return fmt.Errorf("typedcolumn: image column %s block %d values range [%d,%d) outside values=%d", name, i, begin, end, len(global.Values))
		}
		localOffsets := resizeFixedWidthValues([]uint64(nil), block.Descriptor.RowCount+1)
		for row := 0; row <= block.Descriptor.RowCount; row++ {
			localOffsets[row] = global.Offsets[first+row] - begin
		}
		payload, err := EncodeRawUint32OffsetsListPayload(nil, block.Descriptor.RowCount, localOffsets, global.Values[beginInt:endInt])
		if err != nil {
			return fmt.Errorf("typedcolumn: image column %s block %d offsets-list payload: %w", name, i, err)
		}
		if len(payload) != block.Descriptor.StoredBytes {
			return fmt.Errorf("typedcolumn: image column %s block %d payload bytes=%d descriptor stored bytes=%d", name, i, len(payload), block.Descriptor.StoredBytes)
		}
		block.Granule.Payload = payload
		block.Granule.PayloadRef = PayloadRef{Kind: PayloadRefInline, Length: len(payload)}
		expectedFirstRow += block.Descriptor.RowCount
	}
	if expectedFirstRow != global.Rows {
		return fmt.Errorf("typedcolumn: image column %s covers %d rows, want %d", name, expectedFirstRow, global.Rows)
	}
	columns[name] = column
	return nil
}

func attachBytesColumnPayloadsFromImage(image ColumnPartImage, name string, column ColumnPartColumn, columns map[string]ColumnPartColumn) error {
	offsetsSection, valuesSection, ok := image.columnOffsetsListSections(name)
	if !ok {
		return fmt.Errorf("typedcolumn: image missing bytes sections %s", name)
	}
	offsetsRaw := image.sectionBytes(offsetsSection)
	valuesRaw := image.sectionBytes(valuesSection)
	if err := ValidateRawBytesOffsetsSections(offsetsSection, valuesSection, offsetsRaw, valuesRaw, image.Rows); err != nil {
		return err
	}
	global, err := DecodeRawBytesOffsetsFallback(nil, nil, offsetsRaw, valuesRaw, image.Rows)
	if err != nil {
		return err
	}
	expectedFirstRow := 0
	for i := range column.Blocks {
		block := &column.Blocks[i]
		if block.Descriptor.FirstRow != expectedFirstRow {
			return fmt.Errorf("typedcolumn: image column %s block %d first_row=%d want %d", name, i, block.Descriptor.FirstRow, expectedFirstRow)
		}
		if block.Descriptor.RowCount <= 0 || block.Descriptor.FirstRow > global.Rows-block.Descriptor.RowCount {
			return fmt.Errorf("typedcolumn: image column %s block %d row span [%d,%d) outside rows=%d", name, i, block.Descriptor.FirstRow, block.Descriptor.FirstRow+block.Descriptor.RowCount, global.Rows)
		}
		first := block.Descriptor.FirstRow
		begin := global.Offsets[first]
		end := global.Offsets[first+block.Descriptor.RowCount]
		if begin > end || end > maxHostIntUint64() {
			return fmt.Errorf("typedcolumn: image column %s block %d bytes range [%d,%d) invalid", name, i, begin, end)
		}
		beginInt := int(begin)
		endInt := int(end)
		if beginInt > len(global.Values) || endInt > len(global.Values) {
			return fmt.Errorf("typedcolumn: image column %s block %d bytes range [%d,%d) outside values=%d", name, i, begin, end, len(global.Values))
		}
		localOffsets := resizeFixedWidthValues([]uint64(nil), block.Descriptor.RowCount+1)
		for row := 0; row <= block.Descriptor.RowCount; row++ {
			localOffsets[row] = global.Offsets[first+row] - begin
		}
		payload, err := EncodeRawBytesOffsetsPayload(nil, block.Descriptor.RowCount, localOffsets, global.Values[beginInt:endInt])
		if err != nil {
			return fmt.Errorf("typedcolumn: image column %s block %d bytes payload: %w", name, i, err)
		}
		if len(payload) != block.Descriptor.StoredBytes {
			return fmt.Errorf("typedcolumn: image column %s block %d payload bytes=%d descriptor stored bytes=%d", name, i, len(payload), block.Descriptor.StoredBytes)
		}
		block.Granule.Payload = payload
		block.Granule.PayloadRef = PayloadRef{Kind: PayloadRefInline, Length: len(payload)}
		expectedFirstRow += block.Descriptor.RowCount
	}
	if expectedFirstRow != global.Rows {
		return fmt.Errorf("typedcolumn: image column %s covers %d rows, want %d", name, expectedFirstRow, global.Rows)
	}
	columns[name] = column
	return nil
}

func restoreColumnDefinitionCompressionFromImageSections(image ColumnPartImage, columns map[string]ColumnPartColumn) error {
	for name, column := range columns {
		if column.Definition.Encoding == EncodingRawUint32OffsetsList || column.Definition.Encoding == EncodingRawBytesOffsets {
			continue
		}
		section, ok := image.columnDataSection(name)
		if !ok {
			return fmt.Errorf("typedcolumn: image missing column data section %s", name)
		}
		if section.Encoding != column.Definition.Encoding {
			return fmt.Errorf("typedcolumn: image column %s section encoding=%s definition=%s", name, section.Encoding, column.Definition.Encoding)
		}
		for i, block := range column.Blocks {
			if block.Descriptor.Encoding != section.Encoding {
				return fmt.Errorf("typedcolumn: image column %s block %d encoding=%s section=%s", name, i, block.Descriptor.Encoding, section.Encoding)
			}
			switch section.Compression {
			case CompressionNone:
				if block.Descriptor.Compression != CompressionNone {
					return fmt.Errorf("typedcolumn: image column %s block %d compression=%s section=%s", name, i, block.Descriptor.Compression, section.Compression)
				}
			case CompressionSnappy, CompressionLZ4, CompressionZSTD:
				if block.Descriptor.Compression != section.Compression && block.Descriptor.Compression != CompressionNone {
					return fmt.Errorf("typedcolumn: image column %s block %d compression=%s section requested=%s", name, i, block.Descriptor.Compression, section.Compression)
				}
			default:
				return fmt.Errorf("typedcolumn: image column %s section compression=%s is unsupported", name, section.Compression)
			}
		}
		column.Definition.Compression = section.Compression
		columns[name] = column
	}
	return nil
}

func validateColumnDataSectionsForColumns(image ColumnPartImage, columns map[string]ColumnPartColumn) error {
	required := make(map[string]ColumnPartColumn, len(columns))
	for name, column := range columns {
		required[name] = column
	}
	seenData := make(map[string]ColumnPartImageSection, len(columns))
	seenOffsets := make(map[string]ColumnPartImageSection, len(columns))
	seenValues := make(map[string]ColumnPartImageSection, len(columns))
	for _, section := range image.Sections {
		switch section.Kind {
		case ColumnPartImageSectionColumnData, ColumnPartImageSectionColumnOffsets, ColumnPartImageSectionColumnValues:
		default:
			continue
		}
		if section.Column == "" {
			return fmt.Errorf("typedcolumn: image %s section at offset=%d has empty column", section.Kind, section.Offset)
		}
		if _, ok := required[section.Column]; !ok {
			return fmt.Errorf("typedcolumn: image has %s section for unknown column %s at offset=%d", section.Kind, section.Column, section.Offset)
		}
		var seen map[string]ColumnPartImageSection
		switch section.Kind {
		case ColumnPartImageSectionColumnData:
			seen = seenData
		case ColumnPartImageSectionColumnOffsets:
			seen = seenOffsets
		case ColumnPartImageSectionColumnValues:
			seen = seenValues
		}
		previous, exists := seen[section.Column]
		if exists {
			return fmt.Errorf("typedcolumn: duplicate %s section %s at offset=%d previous_offset=%d", section.Kind, section.Column, section.Offset, previous.Offset)
		}
		seen[section.Column] = section
	}
	for name, column := range required {
		_, hasData := seenData[name]
		_, hasOffsets := seenOffsets[name]
		_, hasValues := seenValues[name]
		if column.Definition.Encoding == EncodingRawUint32OffsetsList || column.Definition.Encoding == EncodingRawBytesOffsets {
			if hasData || !hasOffsets || !hasValues {
				return fmt.Errorf("typedcolumn: image column %s offsets/value sections data=%v offsets=%v values=%v", name, hasData, hasOffsets, hasValues)
			}
			continue
		}
		if !hasData && !hasOffsets && !hasValues {
			return fmt.Errorf("typedcolumn: image missing column data section %s", name)
		}
		if !hasData || hasOffsets || hasValues {
			return fmt.Errorf("typedcolumn: image column %s dense/scalar sections data=%v offsets=%v values=%v", name, hasData, hasOffsets, hasValues)
		}
	}
	return nil
}

func (i ColumnPartImage) Dictionaries() (map[string]map[string]int64, error) {
	if err := i.validateForRead(); err != nil {
		return nil, err
	}
	sections := i.sectionsByKind(ColumnPartImageSectionDictionaries)
	if len(sections) == 0 {
		if err := i.validateDictionariesForDescriptor(nil); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if len(sections) != 1 {
		return nil, fmt.Errorf("typedcolumn: image has %d dictionary sections, want 1", len(sections))
	}
	dictionaryBytes, err := i.dictionarySectionBytes(sections[0])
	if err != nil {
		return nil, err
	}
	var out map[string]map[string]int64
	switch sections[0].Encoding {
	case 0:
		out, err = decodeRawDictionarySection(dictionaryBytes)
	case EncodingDictionaryDense:
		out, err = decodeDenseDictionarySection(dictionaryBytes)
	default:
		return nil, fmt.Errorf("typedcolumn: dictionaries encoding=%s is unsupported", sections[0].Encoding)
	}
	if err != nil {
		return nil, err
	}
	if err := i.validateDictionariesForDescriptor(out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeRawDictionarySection(dictionaryBytes []byte) (map[string]map[string]int64, error) {
	dec := columnPartImageDecoder{data: dictionaryBytes}
	count, err := dec.u32()
	if err != nil {
		return nil, err
	}
	total, err := dec.boundedCount(count, 8, "dictionaries")
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]int64, total)
	for idx := 0; idx < total; idx++ {
		name, err := dec.str()
		if err != nil {
			return nil, err
		}
		if _, exists := out[name]; exists {
			return nil, fmt.Errorf("typedcolumn: duplicate dictionary %s", name)
		}
		entryCount, err := dec.u32()
		if err != nil {
			return nil, err
		}
		entries, err := dec.boundedCount(entryCount, 12, "dictionary entries")
		if err != nil {
			return nil, err
		}
		values := make(map[string]int64, entries)
		codes := make(map[int64]string, entries)
		for j := 0; j < entries; j++ {
			code, err := dec.i64()
			if err != nil {
				return nil, err
			}
			value, err := dec.str()
			if err != nil {
				return nil, err
			}
			if _, exists := values[value]; exists {
				return nil, fmt.Errorf("typedcolumn: duplicate dictionary value %s in %s", value, name)
			}
			if previous, exists := codes[code]; exists {
				return nil, fmt.Errorf("typedcolumn: duplicate dictionary code %d in %s for %q and %q", code, name, previous, value)
			}
			values[value] = code
			codes[code] = value
		}
		out[name] = values
	}
	if err := dec.finish(); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeDenseDictionarySection(dictionaryBytes []byte) (map[string]map[string]int64, error) {
	dec := columnPartImageDecoder{data: dictionaryBytes}
	magic, err := dec.u32()
	if err != nil {
		return nil, err
	}
	if magic != dictionaryDenseMagic {
		return nil, fmt.Errorf("typedcolumn: invalid dense dictionary magic 0x%x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return nil, err
	}
	if version != dictionaryDenseVersion {
		return nil, fmt.Errorf("typedcolumn: unsupported dense dictionary version %d", version)
	}
	if reserved, err := dec.u16(); err != nil {
		return nil, err
	} else if reserved != 0 {
		return nil, fmt.Errorf("typedcolumn: dense dictionary reserved=%d want 0", reserved)
	}
	count, err := dec.u32()
	if err != nil {
		return nil, err
	}
	total, err := dec.boundedCount(count, 8, "dictionaries")
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]int64, total)
	for idx := 0; idx < total; idx++ {
		name, err := dec.str()
		if err != nil {
			return nil, err
		}
		if _, exists := out[name]; exists {
			return nil, fmt.Errorf("typedcolumn: duplicate dictionary %s", name)
		}
		entryCount, err := dec.u32()
		if err != nil {
			return nil, err
		}
		entries, err := dec.boundedCount(entryCount, 4, "dictionary entries")
		if err != nil {
			return nil, err
		}
		values := make(map[string]int64, entries)
		for j := 0; j < entries; j++ {
			value, err := dec.str()
			if err != nil {
				return nil, err
			}
			if _, exists := values[value]; exists {
				return nil, fmt.Errorf("typedcolumn: duplicate dictionary value %s in %s", value, name)
			}
			values[value] = int64(j)
		}
		out[name] = values
	}
	if err := dec.finish(); err != nil {
		return nil, err
	}
	return out, nil
}

func (i ColumnPartImage) validateDictionariesForDescriptor(dictionaries map[string]map[string]int64) error {
	descriptorSection, err := i.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		return err
	}
	_, columns, err := decodeColumnPartDescriptorSection(i.sectionBytes(descriptorSection))
	if err != nil {
		return fmt.Errorf("typedcolumn: decode descriptor for dictionaries: %w", err)
	}
	lowCardinality := make(map[string]uint32)
	for name, column := range columns {
		if column.Definition.Type != ColumnTypeLowCardinalityCode {
			continue
		}
		cardinality, err := imageColumnCardinalityForDescriptor(ColumnPartColumnDescriptor{
			Name: name,
			Type: column.Definition.Type,
		}, column)
		if err != nil {
			return err
		}
		if cardinality == 0 && column.Definition.Encoding == EncodingNullableInt64 {
			continue
		}
		lowCardinality[name] = cardinality
	}
	for name, values := range dictionaries {
		cardinality, ok := lowCardinality[name]
		if !ok {
			continue
		}
		seen := make([]bool, int(cardinality))
		for value, code := range values {
			if code < 0 || uint64(code) >= uint64(cardinality) {
				return fmt.Errorf("typedcolumn: dictionary %s value %q code %d outside cardinality %d", name, value, code, cardinality)
			}
			seen[int(code)] = true
		}
		for code, ok := range seen {
			if !ok {
				return fmt.Errorf("typedcolumn: missing dictionary code %d in %s", code, name)
			}
		}
	}
	for name := range lowCardinality {
		if _, ok := dictionaries[name]; !ok {
			return fmt.Errorf("typedcolumn: missing dictionary for low-cardinality column %s", name)
		}
	}
	return nil
}

func decodeAggregateMetadataSections(image ColumnPartImage) (map[string]AggregateMetadata, error) {
	sections := image.sectionsByKind(ColumnPartImageSectionAggregateMetadata)
	if len(sections) == 0 {
		return nil, nil
	}
	out := make(map[string]AggregateMetadata, len(sections))
	for _, section := range sections {
		dec := columnPartImageDecoder{data: image.sectionBytes(section)}
		def, err := decodeAggregateMetadataDefinition(&dec)
		if err != nil {
			return nil, err
		}
		stats, err := decodeAggregateMetadataStats(&dec)
		if err != nil {
			return nil, err
		}
		granuleCount, err := dec.u32()
		if err != nil {
			return nil, err
		}
		granules, err := dec.boundedCount(granuleCount, 36, "aggregate metadata granules")
		if err != nil {
			return nil, err
		}
		metadata := AggregateMetadata{
			Definition: def,
			Stats:      stats,
			Granules:   make([]AggregateMetadataGranule, 0, granules),
		}
		totalMatchedRows := 0
		for i := 0; i < granules; i++ {
			granuleOrdinal, err := dec.i64()
			if err != nil {
				return nil, err
			}
			firstRow, err := dec.i64()
			if err != nil {
				return nil, err
			}
			rowCount, err := dec.i64()
			if err != nil {
				return nil, err
			}
			matchedRows, err := dec.i64()
			if err != nil {
				return nil, err
			}
			entryCount, err := dec.u32()
			if err != nil {
				return nil, err
			}
			entries, err := dec.boundedCount(entryCount, 24, "aggregate metadata entries")
			if err != nil {
				return nil, err
			}
			granuleOrdinalInt, err := nonNegativeInt64ToInt(granuleOrdinal, "aggregate metadata granule ordinal")
			if err != nil {
				return nil, err
			}
			firstRowInt, err := nonNegativeInt64ToInt(firstRow, "aggregate metadata first row")
			if err != nil {
				return nil, err
			}
			rowCountInt, err := nonNegativeInt64ToInt(rowCount, "aggregate metadata row count")
			if err != nil {
				return nil, err
			}
			matchedRowsInt, err := nonNegativeInt64ToInt(matchedRows, "aggregate metadata matched rows")
			if err != nil {
				return nil, err
			}
			granule := AggregateMetadataGranule{
				GranuleOrdinal: granuleOrdinalInt,
				FirstRow:       firstRowInt,
				RowCount:       rowCountInt,
				MatchedRows:    matchedRowsInt,
				Entries:        make([]AggregateMetadataEntry, 0, entries),
			}
			entryRows := 0
			for j := 0; j < entries; j++ {
				group, err := dec.u32()
				if err != nil {
					return nil, err
				}
				count, err := dec.u32()
				if err != nil {
					return nil, err
				}
				if count == 0 {
					return nil, fmt.Errorf("typedcolumn: aggregate metadata %s granule %d entry %d has zero count", metadata.Definition.Name, granuleOrdinalInt, j)
				}
				minValue, err := dec.i64()
				if err != nil {
					return nil, err
				}
				maxValue, err := dec.i64()
				if err != nil {
					return nil, err
				}
				if minValue > maxValue {
					return nil, fmt.Errorf("typedcolumn: aggregate metadata %s granule %d entry %d min=%d exceeds max=%d", metadata.Definition.Name, granuleOrdinalInt, j, minValue, maxValue)
				}
				granule.Entries = append(granule.Entries, AggregateMetadataEntry{Group: group, Count: count, Min: minValue, Max: maxValue})
				entryRows += int(count)
			}
			if entryRows != granule.MatchedRows {
				return nil, fmt.Errorf("typedcolumn: aggregate metadata %s granule %d entry rows=%d matched rows=%d", metadata.Definition.Name, granule.GranuleOrdinal, entryRows, granule.MatchedRows)
			}
			metadata.Granules = append(metadata.Granules, granule)
			totalMatchedRows += granule.MatchedRows
		}
		if metadata.Stats.Admitted && totalMatchedRows != metadata.Stats.RowsMatched {
			return nil, fmt.Errorf("typedcolumn: aggregate metadata %s granule matched rows=%d stats matched rows=%d", metadata.Definition.Name, totalMatchedRows, metadata.Stats.RowsMatched)
		}
		if !metadata.Stats.Admitted && len(metadata.Granules) != 0 {
			return nil, fmt.Errorf("typedcolumn: aggregate metadata %s is rejected but has %d granules", metadata.Definition.Name, len(metadata.Granules))
		}
		if err := dec.finish(); err != nil {
			return nil, err
		}
		if _, exists := out[metadata.Definition.Name]; exists {
			return nil, fmt.Errorf("typedcolumn: duplicate aggregate metadata %s at section %s offset=%d", metadata.Definition.Name, section.Name, section.Offset)
		}
		out[metadata.Definition.Name] = metadata
	}
	return out, nil
}

func decodeAggregateMetadataDefinition(dec *columnPartImageDecoder) (AggregateMetadataDefinition, error) {
	name, err := dec.str()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	version, err := dec.u16()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	kind, err := dec.str()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	scope, err := dec.str()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	groupKeys, err := dec.stringSlice()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	measureCount, err := dec.u32()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	measureTotal, err := dec.boundedCount(measureCount, 8, "aggregate metadata measures")
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	measures := make([]AggregateMetadataMeasure, 0, measureTotal)
	for i := 0; i < measureTotal; i++ {
		op, err := dec.str()
		if err != nil {
			return AggregateMetadataDefinition{}, err
		}
		column, err := dec.str()
		if err != nil {
			return AggregateMetadataDefinition{}, err
		}
		measures = append(measures, AggregateMetadataMeasure{Op: AggregateMetadataMeasureOp(op), Column: column})
	}
	predicateCount, err := dec.u32()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	predicateTotal, err := dec.boundedCount(predicateCount, 16, "aggregate metadata predicates")
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	predicates := make([]AggregateMetadataPredicate, 0, predicateTotal)
	for i := 0; i < predicateTotal; i++ {
		column, err := dec.str()
		if err != nil {
			return AggregateMetadataDefinition{}, err
		}
		op, err := dec.str()
		if err != nil {
			return AggregateMetadataDefinition{}, err
		}
		value, err := dec.i64()
		if err != nil {
			return AggregateMetadataDefinition{}, err
		}
		predicates = append(predicates, AggregateMetadataPredicate{Column: column, Op: AggregateMetadataPredicateOp(op), Value: value})
	}
	maxBytesPerRow, err := dec.i64()
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	maxBytesPerRowFloat, err := nonNegativeScaledInt64ToFloat64(maxBytesPerRow, "aggregate metadata max bytes per row")
	if err != nil {
		return AggregateMetadataDefinition{}, err
	}
	return AggregateMetadataDefinition{
		Name:           name,
		Version:        version,
		Kind:           AggregateMetadataKind(kind),
		Scope:          AggregateMetadataScope(scope),
		GroupKeys:      groupKeys,
		Measures:       measures,
		Predicates:     predicates,
		MaxBytesPerRow: maxBytesPerRowFloat,
	}, nil
}

func decodeAggregateMetadataStats(dec *columnPartImageDecoder) (AggregateMetadataStats, error) {
	admitted, err := dec.boolean()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	rejectedReason, err := dec.str()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	buildNanos, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	granules, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	granulesWithRows, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	rowsMatched, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	entries, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	valueBytes, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	descriptorBytes, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	totalBytes, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	bytesPerPartRow, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	bytesPerMatchedRow, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	compression, err := dec.str()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	admissionMaxBytes, err := dec.i64()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	admissionMeasuredBy, err := dec.str()
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	if buildNanos < 0 {
		return AggregateMetadataStats{}, fmt.Errorf("typedcolumn: negative aggregate metadata build duration %d", buildNanos)
	}
	granulesInt, err := nonNegativeInt64ToInt(granules, "aggregate metadata granules")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	granulesWithRowsInt, err := nonNegativeInt64ToInt(granulesWithRows, "aggregate metadata granules with rows")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	rowsMatchedInt, err := nonNegativeInt64ToInt(rowsMatched, "aggregate metadata rows matched")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	entriesInt, err := nonNegativeInt64ToInt(entries, "aggregate metadata entries")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	valueBytesInt, err := nonNegativeInt64ToInt(valueBytes, "aggregate metadata value bytes")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	descriptorBytesInt, err := nonNegativeInt64ToInt(descriptorBytes, "aggregate metadata descriptor bytes")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	totalBytesInt, err := nonNegativeInt64ToInt(totalBytes, "aggregate metadata total bytes")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	bytesPerPartRowFloat, err := nonNegativeScaledInt64ToFloat64(bytesPerPartRow, "aggregate metadata bytes per part row")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	bytesPerMatchedRowFloat, err := nonNegativeScaledInt64ToFloat64(bytesPerMatchedRow, "aggregate metadata bytes per matched row")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	admissionMaxBytesFloat, err := nonNegativeScaledInt64ToFloat64(admissionMaxBytes, "aggregate metadata admission max bytes")
	if err != nil {
		return AggregateMetadataStats{}, err
	}
	return AggregateMetadataStats{
		Admitted:            admitted,
		RejectedReason:      rejectedReason,
		BuildDuration:       time.Duration(buildNanos),
		Granules:            granulesInt,
		GranulesWithRows:    granulesWithRowsInt,
		RowsMatched:         rowsMatchedInt,
		Entries:             entriesInt,
		ValueBytes:          valueBytesInt,
		DescriptorBytes:     descriptorBytesInt,
		TotalBytes:          totalBytesInt,
		BytesPerPartRow:     bytesPerPartRowFloat,
		BytesPerMatchedRow:  bytesPerMatchedRowFloat,
		Compression:         compression,
		AdmissionMaxBytes:   admissionMaxBytesFloat,
		AdmissionMeasuredBy: admissionMeasuredBy,
	}, nil
}

func nonNegativeScaledInt64ToFloat64(v int64, field string) (float64, error) {
	if v < 0 {
		return 0, fmt.Errorf("typedcolumn: negative %s %d", field, v)
	}
	return float64(v) / 1_000_000, nil
}

func (i ColumnPartImage) singleSection(kind ColumnPartImageSectionKind) (ColumnPartImageSection, error) {
	sections := i.sectionsByKind(kind)
	if len(sections) != 1 {
		return ColumnPartImageSection{}, fmt.Errorf("typedcolumn: image has %d %s sections, want 1", len(sections), kind)
	}
	return sections[0], nil
}

func (i ColumnPartImage) sectionsByKind(kind ColumnPartImageSectionKind) []ColumnPartImageSection {
	out := make([]ColumnPartImageSection, 0, 1)
	for _, section := range i.Sections {
		if section.Kind == kind {
			out = append(out, section)
		}
	}
	return out
}

func (i ColumnPartImage) sectionBytes(section ColumnPartImageSection) []byte {
	return i.Bytes[section.Offset : section.Offset+section.Length]
}

func (i ColumnPartImage) rowLocatorSectionBytes(section ColumnPartImageSection) ([]byte, error) {
	if section.Rows != i.Rows {
		return nil, fmt.Errorf("typedcolumn: row locator section rows=%d want image rows=%d", section.Rows, i.Rows)
	}
	rawBytes, err := rowLocatorSectionEncodedRawBytes(section)
	if err != nil {
		return nil, err
	}
	return i.sectionBytesWithKnownRawLength(section, rawBytes, maxCompressedRowLocatorSectionRawBytes, "row locators")
}

func rowLocatorSectionEncodedRawBytes(section ColumnPartImageSection) (int, error) {
	switch section.Encoding {
	case 0:
		return rowLocatorSectionRawBytes(section.Rows)
	case EncodingRowLocatorContiguous:
		return rowLocatorContiguousPayloadBytes, nil
	default:
		return 0, fmt.Errorf("typedcolumn: row locators encoding=%s is unsupported", section.Encoding)
	}
}

func rowLocatorSectionRawBytes(rows int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("typedcolumn: row locator section rows=%d is negative", rows)
	}
	recordBytes, err := checkedMulInt(rows, rowLocatorBytes, "row locator section bytes")
	if err != nil {
		return 0, err
	}
	payloadBytes, err := checkedAddInt(4, recordBytes, "row locator section bytes")
	if err != nil {
		return 0, err
	}
	return payloadBytes, nil
}

func (i ColumnPartImage) dictionarySectionBytes(section ColumnPartImageSection) ([]byte, error) {
	switch section.Encoding {
	case 0, EncodingDictionaryDense:
	default:
		return nil, fmt.Errorf("typedcolumn: dictionaries encoding=%s is unsupported", section.Encoding)
	}
	rawBytes := section.RawBytes
	if rawBytes == 0 && section.Compression == CompressionNone {
		rawBytes = section.Length
	}
	return i.sectionBytesWithKnownRawLength(section, rawBytes, maxCompressedDictionarySectionRawBytes, "dictionaries")
}

func (i ColumnPartImage) pruningMetadataSectionBytes(section ColumnPartImageSection) ([]byte, error) {
	rawBytes := section.RawBytes
	if rawBytes == 0 && section.Compression == CompressionNone {
		rawBytes = section.Length
	}
	return i.sectionBytesWithKnownRawLength(section, rawBytes, maxCompressedPruningMetadataSectionRawBytes, "pruning metadata")
}

func (i ColumnPartImage) sectionBytesWithKnownRawLength(section ColumnPartImageSection, rawBytes int, maxRawBytes int, label string) ([]byte, error) {
	payload := i.sectionBytes(section)
	return sectionPayloadBytesWithKnownRawLength(section, payload, rawBytes, maxRawBytes, label)
}

func sectionPayloadBytesWithKnownRawLength(section ColumnPartImageSection, payload []byte, rawBytes int, maxRawBytes int, label string) ([]byte, error) {
	if len(payload) != section.Length {
		return nil, fmt.Errorf("typedcolumn: %s section payload bytes=%d want section length=%d", label, len(payload), section.Length)
	}
	switch section.Compression {
	case CompressionNone:
		if len(payload) != rawBytes {
			return nil, fmt.Errorf("typedcolumn: %s section bytes=%d want raw bytes=%d", label, len(payload), rawBytes)
		}
		return payload, nil
	case CompressionSnappy:
		if rawBytes <= 0 {
			return nil, fmt.Errorf("typedcolumn: %s compressed raw bytes=%d is invalid", label, rawBytes)
		}
		if rawBytes > maxRawBytes {
			return nil, fmt.Errorf("typedcolumn: %s compressed raw bytes=%d exceeds max=%d", label, rawBytes, maxRawBytes)
		}
		decodedLen, err := snappy.DecodedLen(payload)
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: %s snappy decoded length: %w", label, err)
		}
		if decodedLen != rawBytes {
			return nil, fmt.Errorf("typedcolumn: %s snappy decoded length=%d want=%d", label, decodedLen, rawBytes)
		}
		out, err := snappy.Decode(make([]byte, decodedLen), payload)
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: %s snappy decode: %w", label, err)
		}
		if len(out) != rawBytes {
			return nil, fmt.Errorf("typedcolumn: %s snappy decoded length=%d want=%d", label, len(out), rawBytes)
		}
		return out, nil
	case CompressionLZ4:
		if rawBytes <= 0 {
			return nil, fmt.Errorf("typedcolumn: %s compressed raw bytes=%d is invalid", label, rawBytes)
		}
		if rawBytes > maxRawBytes {
			return nil, fmt.Errorf("typedcolumn: %s compressed raw bytes=%d exceeds max=%d", label, rawBytes, maxRawBytes)
		}
		out := make([]byte, rawBytes)
		n, err := lz4.UncompressBlock(payload, out)
		if err != nil {
			return nil, fmt.Errorf("typedcolumn: %s lz4 decode: %w", label, err)
		}
		if n != rawBytes {
			return nil, fmt.Errorf("typedcolumn: %s lz4 decoded length=%d want=%d", label, n, rawBytes)
		}
		return out, nil
	case CompressionZSTD:
		if rawBytes <= 0 {
			return nil, fmt.Errorf("typedcolumn: %s compressed raw bytes=%d is invalid", label, rawBytes)
		}
		if rawBytes > maxRawBytes {
			return nil, fmt.Errorf("typedcolumn: %s compressed raw bytes=%d exceeds max=%d", label, rawBytes, maxRawBytes)
		}
		return decodeZstdPayload(label+" section", payload, rawBytes, make([]byte, 0, rawBytes))
	default:
		return nil, fmt.Errorf("typedcolumn: %s section compression=%s is unsupported", label, section.Compression)
	}
}

func (i ColumnPartImage) validateForRead() error {
	if i.TotalBytes() == 0 {
		return fmt.Errorf("typedcolumn: empty part image")
	}
	if i.Version != columnPartImageVersion {
		return fmt.Errorf("typedcolumn: unsupported part image version %d", i.Version)
	}
	if i.Rows < 0 {
		return fmt.Errorf("typedcolumn: negative image row count %d", i.Rows)
	}
	if i.ManifestBytes <= 0 || i.ManifestBytes > len(i.Bytes) {
		return fmt.Errorf("typedcolumn: manifest bytes=%d exceed image bytes=%d", i.ManifestBytes, len(i.Bytes))
	}
	for _, section := range i.Sections {
		if section.Kind == ColumnPartImageSectionManifest {
			return fmt.Errorf("typedcolumn: manifest is not a directory section")
		}
		if err := validateImageSectionCategory(section.Kind, section.Category); err != nil {
			return err
		}
		if err := validateImageSectionCompression(section); err != nil {
			return err
		}
		if err := validateImageSectionBounds(section, i.ManifestBytes, len(i.Bytes)); err != nil {
			return err
		}
	}
	if err := validateImageSectionLayout(i.Sections, i.ManifestBytes, len(i.Bytes)); err != nil {
		return err
	}
	if err := validateImageSectionMultiplicity(i.Sections); err != nil {
		return err
	}
	return nil
}

func validateImageSectionCompression(section ColumnPartImageSection) error {
	switch section.Kind {
	case ColumnPartImageSectionColumnData:
		switch section.Compression {
		case CompressionNone, CompressionSnappy, CompressionLZ4, CompressionZSTD:
			return nil
		default:
			return fmt.Errorf("typedcolumn: section %s compression=%s is unsupported", section.Kind, section.Compression)
		}
	case ColumnPartImageSectionRowLocators:
		rawBytes, err := rowLocatorSectionEncodedRawBytes(section)
		if err != nil {
			return err
		}
		switch section.Compression {
		case CompressionNone:
			if section.RawBytes != 0 && section.RawBytes != section.Length {
				return fmt.Errorf("typedcolumn: section %s raw bytes=%d length=%d for uncompressed section", section.Kind, section.RawBytes, section.Length)
			}
			return nil
		case CompressionSnappy, CompressionLZ4, CompressionZSTD:
			if section.RawBytes != rawBytes {
				return fmt.Errorf("typedcolumn: section %s raw bytes=%d want %d", section.Kind, section.RawBytes, rawBytes)
			}
			if section.RawBytes > maxCompressedRowLocatorSectionRawBytes {
				return fmt.Errorf("typedcolumn: section %s raw bytes=%d exceeds max=%d", section.Kind, section.RawBytes, maxCompressedRowLocatorSectionRawBytes)
			}
			return nil
		default:
			return fmt.Errorf("typedcolumn: section %s compression=%s is unsupported", section.Kind, section.Compression)
		}
	case ColumnPartImageSectionDictionaries:
		switch section.Encoding {
		case 0, EncodingDictionaryDense:
		default:
			return fmt.Errorf("typedcolumn: dictionaries encoding=%s is unsupported", section.Encoding)
		}
		switch section.Compression {
		case CompressionNone:
			if section.RawBytes != 0 && section.RawBytes != section.Length {
				return fmt.Errorf("typedcolumn: section %s raw bytes=%d length=%d for uncompressed section", section.Kind, section.RawBytes, section.Length)
			}
			return nil
		case CompressionSnappy, CompressionLZ4, CompressionZSTD:
			if section.RawBytes <= 0 {
				return fmt.Errorf("typedcolumn: section %s compressed raw bytes=%d is invalid", section.Kind, section.RawBytes)
			}
			if section.RawBytes > maxCompressedDictionarySectionRawBytes {
				return fmt.Errorf("typedcolumn: section %s raw bytes=%d exceeds max=%d", section.Kind, section.RawBytes, maxCompressedDictionarySectionRawBytes)
			}
			return nil
		default:
			return fmt.Errorf("typedcolumn: section %s compression=%s is unsupported", section.Kind, section.Compression)
		}
	case ColumnPartImageSectionPruningMetadata:
		if section.Encoding != 0 {
			return fmt.Errorf("typedcolumn: pruning metadata encoding=%s is unsupported", section.Encoding)
		}
		switch section.Compression {
		case CompressionNone:
			if section.RawBytes != 0 && section.RawBytes != section.Length {
				return fmt.Errorf("typedcolumn: section %s raw bytes=%d length=%d for uncompressed section", section.Kind, section.RawBytes, section.Length)
			}
			return nil
		case CompressionSnappy, CompressionLZ4, CompressionZSTD:
			if section.RawBytes <= 0 {
				return fmt.Errorf("typedcolumn: section %s compressed raw bytes=%d is invalid", section.Kind, section.RawBytes)
			}
			if section.RawBytes > maxCompressedPruningMetadataSectionRawBytes {
				return fmt.Errorf("typedcolumn: section %s raw bytes=%d exceeds max=%d", section.Kind, section.RawBytes, maxCompressedPruningMetadataSectionRawBytes)
			}
			return nil
		default:
			return fmt.Errorf("typedcolumn: section %s compression=%s is unsupported", section.Kind, section.Compression)
		}
	default:
		if section.Compression != CompressionNone {
			return fmt.Errorf("typedcolumn: section %s compression=%s is unsupported without section raw-length metadata", section.Kind, section.Compression)
		}
		return nil
	}
}

func validateImageSectionBounds(section ColumnPartImageSection, manifestBytes int, totalBytes int) error {
	if section.Offset < 0 {
		return fmt.Errorf("typedcolumn: section %s has negative offset %d", section.Kind, section.Offset)
	}
	if section.Offset < manifestBytes {
		return fmt.Errorf("typedcolumn: section %s offset=%d before manifest=%d", section.Kind, section.Offset, manifestBytes)
	}
	if section.Offset%columnPartImageSectionAlignment != 0 {
		return fmt.Errorf("typedcolumn: section %s offset=%d is not %d-byte aligned", section.Kind, section.Offset, columnPartImageSectionAlignment)
	}
	if section.Length < 0 {
		return fmt.Errorf("typedcolumn: section %s has negative length %d", section.Kind, section.Length)
	}
	if section.Length > totalBytes || section.Offset > totalBytes-section.Length {
		return fmt.Errorf("typedcolumn: section %s offset=%d length=%d exceeds image bytes=%d", section.Kind, section.Offset, section.Length, totalBytes)
	}
	return nil
}

func validateImageSectionCount(sectionCount uint32, manifestBytes int, decodedManifestBytes int) error {
	const minSectionDirectoryEntryBytes = 64
	remainingManifestBytes := manifestBytes - decodedManifestBytes
	if remainingManifestBytes < 0 {
		return fmt.Errorf("typedcolumn: decoded manifest bytes=%d exceed manifest bytes=%d", decodedManifestBytes, manifestBytes)
	}
	maxSections := remainingManifestBytes / minSectionDirectoryEntryBytes
	if int(sectionCount) > maxSections {
		return fmt.Errorf("typedcolumn: section count=%d exceeds manifest capacity=%d", sectionCount, maxSections)
	}
	return nil
}

func validateImageSectionLayout(sections []ColumnPartImageSection, manifestBytes int, totalBytes int) error {
	cursor := manifestBytes
	for _, section := range sections {
		if section.Offset < cursor {
			return fmt.Errorf("typedcolumn: section %s offset=%d overlaps previous end=%d", section.Kind, section.Offset, cursor)
		}
		cursor = section.Offset + section.Length
	}
	if cursor != totalBytes {
		return fmt.Errorf("typedcolumn: image sections cover %d bytes, image has %d", cursor, totalBytes)
	}
	return nil
}

func validateImageSectionMultiplicity(sections []ColumnPartImageSection) error {
	counts := make(map[ColumnPartImageSectionKind]int, len(sections))
	columnCounts := make(map[struct {
		kind   ColumnPartImageSectionKind
		column string
	}]int)
	for _, section := range sections {
		counts[section.Kind]++
		switch section.Kind {
		case ColumnPartImageSectionColumnOffsets, ColumnPartImageSectionColumnValues:
			key := struct {
				kind   ColumnPartImageSectionKind
				column string
			}{kind: section.Kind, column: section.Column}
			columnCounts[key]++
			if columnCounts[key] > 1 {
				return fmt.Errorf("typedcolumn: image has %d %s sections for column %q, want at most 1", columnCounts[key], section.Kind, section.Column)
			}
		}
	}
	for _, kind := range []ColumnPartImageSectionKind{
		ColumnPartImageSectionDescriptor,
		ColumnPartImageSectionSortKeyMetadata,
		ColumnPartImageSectionSortKeyMarks,
		ColumnPartImageSectionRowLocators,
		ColumnPartImageSectionDictionaries,
		ColumnPartImageSectionLayoutContract,
		ColumnPartImageSectionColumnStats,
		ColumnPartImageSectionPruningMetadata,
	} {
		if counts[kind] > 1 {
			return fmt.Errorf("typedcolumn: image has %d %s sections, want at most 1", counts[kind], kind)
		}
	}
	return nil
}

func validateImageSectionCategory(kind ColumnPartImageSectionKind, category ColumnPartImageSectionCategory) error {
	expected, ok := expectedImageSectionCategory(kind)
	if ok && category != expected {
		return fmt.Errorf("typedcolumn: section %s category=%s want %s", kind, category, expected)
	}
	return nil
}

func expectedImageSectionCategory(kind ColumnPartImageSectionKind) (ColumnPartImageSectionCategory, bool) {
	switch kind {
	case ColumnPartImageSectionDescriptor:
		return ColumnPartImageCategoryDescriptor, true
	case ColumnPartImageSectionSortKeyMetadata:
		return ColumnPartImageCategorySortKeyMetadata, true
	case ColumnPartImageSectionSortKeyMarks:
		return ColumnPartImageCategoryMarks, true
	case ColumnPartImageSectionRowLocators:
		return ColumnPartImageCategoryLocators, true
	case ColumnPartImageSectionAggregateMetadata:
		return ColumnPartImageCategoryAggregateMetadata, true
	case ColumnPartImageSectionColumnStats:
		return ColumnPartImageCategoryColumnStats, true
	case ColumnPartImageSectionPruningMetadata:
		return ColumnPartImageCategoryPruningMetadata, true
	case ColumnPartImageSectionDictionaries:
		return ColumnPartImageCategoryDictionaries, true
	case ColumnPartImageSectionLayoutContract:
		return ColumnPartImageCategoryLayoutContract, true
	case ColumnPartImageSectionColumnData:
		return ColumnPartImageCategoryDeclaredColumns, true
	case ColumnPartImageSectionColumnOffsets:
		return ColumnPartImageCategoryDeclaredColumnOffsets, true
	case ColumnPartImageSectionColumnValues:
		return ColumnPartImageCategoryDeclaredColumnValues, true
	case ColumnPartImageSectionManifest:
		return ColumnPartImageCategoryManifest, true
	default:
		return "", false
	}
}

func inferredRowsPerGranule(granules []GranuleDescriptor) int {
	if len(granules) == 0 {
		return 0
	}
	return granules[0].RowCount
}

func inferredDefaultCodecBlockRows(columns []ColumnPartColumnDescriptor) int {
	for _, column := range columns {
		if len(column.Blocks) > 0 {
			return column.Blocks[0].RowCount
		}
	}
	return 0
}

type columnPartImageDecoder struct {
	data   []byte
	offset int
}

func (d *columnPartImageDecoder) u16() (uint16, error) {
	if err := d.require(2); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint16(d.data[d.offset:])
	d.offset += 2
	return v, nil
}

func (d *columnPartImageDecoder) u32() (uint32, error) {
	if err := d.require(4); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(d.data[d.offset:])
	d.offset += 4
	return v, nil
}

func (d *columnPartImageDecoder) u64() (uint64, error) {
	if err := d.require(8); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(d.data[d.offset:])
	d.offset += 8
	return v, nil
}

func (d *columnPartImageDecoder) i64() (int64, error) {
	v, err := d.u64()
	return int64(v), err
}

func (d *columnPartImageDecoder) boolean() (bool, error) {
	v, err := d.u16()
	if err != nil {
		return false, err
	}
	switch v {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("typedcolumn: invalid boolean value %d", v)
	}
}

func (d *columnPartImageDecoder) str() (string, error) {
	length, err := d.u32()
	if err != nil {
		return "", err
	}
	lengthInt, err := d.countToInt(length, "string bytes")
	if err != nil {
		return "", err
	}
	if lengthInt > maxColumnPartImageStringBytes {
		return "", fmt.Errorf("typedcolumn: string bytes=%d exceeds max=%d", lengthInt, maxColumnPartImageStringBytes)
	}
	if err := d.require(lengthInt); err != nil {
		return "", err
	}
	v := string(d.data[d.offset : d.offset+lengthInt])
	d.offset += lengthInt
	return v, nil
}

func (d *columnPartImageDecoder) stringSlice() ([]string, error) {
	count, err := d.u32()
	if err != nil {
		return nil, err
	}
	total, err := d.boundedCount(count, 4, "string slice values")
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, total)
	for i := 0; i < total; i++ {
		value, err := d.str()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func (d *columnPartImageDecoder) int64Slice() ([]int64, error) {
	count, err := d.u32()
	if err != nil {
		return nil, err
	}
	total, err := d.boundedCount(count, 8, "int64 slice values")
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, total)
	for i := 0; i < total; i++ {
		value, err := d.i64()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func (d *columnPartImageDecoder) require(n int) error {
	if n < 0 || n > len(d.data)-d.offset {
		return fmt.Errorf("typedcolumn: truncated part image at offset %d need %d bytes have %d", d.offset, n, len(d.data)-d.offset)
	}
	return nil
}

func (d *columnPartImageDecoder) boundedCount(count uint32, minItemBytes int, label string) (int, error) {
	total, err := d.countToInt(count, label)
	if err != nil {
		return 0, err
	}
	if minItemBytes < 0 {
		return 0, fmt.Errorf("typedcolumn: invalid minimum item bytes %d for %s", minItemBytes, label)
	}
	if minItemBytes > 0 && total > (len(d.data)-d.offset)/minItemBytes {
		return 0, fmt.Errorf("typedcolumn: %s count=%d exceeds section capacity=%d", label, count, (len(d.data)-d.offset)/minItemBytes)
	}
	return total, nil
}

func (d *columnPartImageDecoder) countToInt(count uint32, label string) (int, error) {
	total := int(count)
	if uint64(total) != uint64(count) {
		return 0, fmt.Errorf("typedcolumn: %s count=%d exceeds host int", label, count)
	}
	return total, nil
}

func nonNegativeInt64ToInt(value int64, label string) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("typedcolumn: negative %s %d", label, value)
	}
	out := int(value)
	if int64(out) != value {
		return 0, fmt.Errorf("typedcolumn: %s=%d exceeds host int", label, value)
	}
	return out, nil
}

func (d *columnPartImageDecoder) finish() error {
	if d.offset != len(d.data) {
		return fmt.Errorf("typedcolumn: trailing part image section bytes=%d", len(d.data)-d.offset)
	}
	return nil
}

func columnTypeFromCode(code uint16) (ColumnType, error) {
	switch code {
	case 1:
		return ColumnTypeInt64, nil
	case 2:
		return ColumnTypeLowCardinalityCode, nil
	case 3:
		return ColumnTypeBool, nil
	case 4:
		return ColumnTypeFloat32Vector, nil
	case 5:
		return ColumnTypeAdjacencyList, nil
	case 6:
		return ColumnTypeFloat32, nil
	case 7:
		return ColumnTypeFloat64, nil
	case 8:
		return ColumnTypeUint32List, nil
	case 9:
		return ColumnTypeBytes, nil
	case 10:
		return ColumnTypeInt8, nil
	case 11:
		return ColumnTypeUint8, nil
	case 12:
		return ColumnTypeInt16, nil
	case 13:
		return ColumnTypeUint16, nil
	case 14:
		return ColumnTypeInt32, nil
	case 15:
		return ColumnTypeUint32, nil
	case 16:
		return ColumnTypeUint64, nil
	case 17:
		return ColumnTypeFloat16, nil
	case 18:
		return ColumnTypeBFloat16, nil
	case 19:
		return ColumnTypeUint8Vector, nil
	case 20:
		return ColumnTypeInt8Vector, nil
	case 21:
		return ColumnTypeUint16Vector, nil
	case 22:
		return ColumnTypeInt16Vector, nil
	case 23:
		return ColumnTypeUint32Vector, nil
	case 24:
		return ColumnTypeInt32Vector, nil
	case 25:
		return ColumnTypeUint64Vector, nil
	case 26:
		return ColumnTypeInt64Vector, nil
	case 27:
		return ColumnTypeFloat16Vector, nil
	case 28:
		return ColumnTypeBFloat16Vector, nil
	case 29:
		return ColumnTypeFloat64Vector, nil
	case 30:
		return ColumnTypeFixedBytes, nil
	case 31:
		return ColumnTypePackedBitVector, nil
	case 32:
		return ColumnTypePackedUint2Vector, nil
	case 33:
		return ColumnTypePackedUint4Vector, nil
	default:
		return "", fmt.Errorf("typedcolumn: unknown column type code %d", code)
	}
}
