package colgranule

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"
)

const maxColumnPartImageStringBytes = 1 << 20

func ParseColumnPartImage(data []byte) (ColumnPartImage, error) {
	dec := columnPartImageDecoder{data: data}
	magic, err := dec.u32()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if magic != columnPartImageMagic {
		return ColumnPartImage{}, fmt.Errorf("colgranule: invalid part image magic 0x%x", magic)
	}
	version, err := dec.u16()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if version != columnPartImageVersion {
		return ColumnPartImage{}, fmt.Errorf("colgranule: unsupported part image version %d", version)
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
		return ColumnPartImage{}, fmt.Errorf("colgranule: negative image row count %d", rows)
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
		return ColumnPartImage{}, fmt.Errorf("colgranule: manifest bytes=%d exceed host int", manifestBytes)
	}
	manifestLength := int(manifestBytes)
	if manifestLength > len(data) {
		return ColumnPartImage{}, fmt.Errorf("colgranule: manifest bytes=%d exceed image bytes=%d", manifestBytes, len(data))
	}
	sectionCount, err := dec.u32()
	if err != nil {
		return ColumnPartImage{}, err
	}
	if uint64(int(sectionCount)) != uint64(sectionCount) {
		return ColumnPartImage{}, fmt.Errorf("colgranule: section count=%d exceed host int", sectionCount)
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
			return ColumnPartImage{}, fmt.Errorf("colgranule: manifest is not a directory section")
		}
		if err := validateImageSectionCategory(kind, category); err != nil {
			return ColumnPartImage{}, err
		}
		if sectionRows < 0 || granules < 0 || blocks < 0 {
			return ColumnPartImage{}, fmt.Errorf("colgranule: section %s has negative rows/granules/blocks (%d,%d,%d)", kind, sectionRows, granules, blocks)
		}
		if int64(int(sectionRows)) != sectionRows || int64(int(granules)) != granules || int64(int(blocks)) != blocks {
			return ColumnPartImage{}, fmt.Errorf("colgranule: section %s rows/granules/blocks exceed host int (%d,%d,%d)", kind, sectionRows, granules, blocks)
		}
		if uint64(int(offset)) != offset || uint64(int(length)) != length {
			return ColumnPartImage{}, fmt.Errorf("colgranule: section %s offset=%d length=%d exceed host int", kind, offset, length)
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
		}
		if err := validateImageSectionBounds(section, int(manifestBytes), len(data)); err != nil {
			return ColumnPartImage{}, err
		}
		sections = append(sections, section)
	}
	if dec.offset != manifestLength {
		return ColumnPartImage{}, fmt.Errorf("colgranule: manifest bytes=%d decoded=%d", manifestBytes, dec.offset)
	}
	if err := validateImageSectionLayout(sections, manifestLength, len(data)); err != nil {
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
}

func ColumnPartFromImage(image ColumnPartImage) (*ColumnPart, error) {
	return ColumnPartFromImageWithOptions(image, ColumnPartImageReadOptions{
		IncludeRowLocators:       true,
		ValidateRowLocators:      true,
		IncludeAggregateMetadata: true,
	})
}

func ColumnPartFromImageWithOptions(image ColumnPartImage, opts ColumnPartImageReadOptions) (*ColumnPart, error) {
	if image.TotalBytes() == 0 {
		return nil, fmt.Errorf("colgranule: empty part image")
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
		return nil, fmt.Errorf("colgranule: descriptor part id=%d manifest part id=%d", desc.PartID, image.PartID)
	}
	if desc.RowCount != image.Rows {
		return nil, fmt.Errorf("colgranule: descriptor rows=%d manifest rows=%d", desc.RowCount, image.Rows)
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
		locators, err = decodeRowLocatorsSection(image)
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
	if opts.ValidateRowLocators {
		if err := validateDecodedRowLocatorsPrimaryKey(desc, columns, locators); err != nil {
			return nil, err
		}
	}
	optionsColumns := make([]ColumnDefinition, 0, len(desc.Columns))
	for _, columnDescriptor := range desc.Columns {
		column, ok := columns[columnDescriptor.Name]
		if !ok {
			return nil, fmt.Errorf("colgranule: descriptor column %s missing decoded column", columnDescriptor.Name)
		}
		def := column.Definition
		if def.Type == ColumnTypeLowCardinalityCode {
			if def.Cardinality == 0 || def.Cardinality > maxCodeCardinality {
				return nil, fmt.Errorf("colgranule: invalid low-cardinality cardinality %d for %s", def.Cardinality, def.Name)
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
	aggregateDefinitions := make([]AggregateMetadataDefinition, 0, len(aggregateMetadata))
	for _, metadata := range aggregateMetadata {
		aggregateDefinitions = append(aggregateDefinitions, metadata.Definition)
	}
	sort.Slice(aggregateDefinitions, func(i, j int) bool {
		return aggregateDefinitions[i].Name < aggregateDefinitions[j].Name
	})
	part := &ColumnPart{
		Options: ColumnStoreOptions{
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
	}
	return part, nil
}

func decodeColumnPartDescriptorSection(data []byte) (ColumnPartDescriptor, map[string]ColumnPartColumn, error) {
	dec := columnPartImageDecoder{data: data}
	version, err := dec.u16()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	if version != columnPartDescriptorVersion {
		return ColumnPartDescriptor{}, nil, fmt.Errorf("colgranule: unsupported descriptor version %d", version)
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
	columnTotal, err := dec.boundedCount(columnCount, 14, "descriptor columns")
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
			return ColumnPartDescriptor{}, nil, fmt.Errorf("colgranule: duplicate descriptor column %s", name)
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
			if cardinality == 0 || cardinality > maxCodeCardinality {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("colgranule: invalid low-cardinality cardinality %d for %s", cardinality, name)
			}
		} else if cardinality != 0 {
			return ColumnPartDescriptor{}, nil, fmt.Errorf("colgranule: column %s type=%s has cardinality %d", name, columnType, cardinality)
		}
		blockCount, err := dec.u32()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		blocks, err := dec.boundedCount(blockCount, 94, "descriptor column blocks")
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		columnDesc := ColumnPartColumnDescriptor{Name: name, Type: columnType, Blocks: make([]ColumnBlockDescriptor, 0, blocks)}
		column := ColumnPartColumn{
			Definition: ColumnDefinition{
				Name:        name,
				Type:        columnType,
				Cardinality: cardinality,
			},
			Blocks: make([]ColumnBlock, 0, blocks),
		}
		expectedFirstRow := 0
		for j := 0; j < blocks; j++ {
			blockDesc, granule, err := decodeColumnBlockDescriptorAndGranule(&dec)
			if err != nil {
				return ColumnPartDescriptor{}, nil, err
			}
			if err := validateDecodedColumnBlockDescriptor(desc, name, columnType, cardinality, j, blockDesc); err != nil {
				return ColumnPartDescriptor{}, nil, err
			}
			if err := validateDecodedColumnBlockGranuleMetadata(name, j, granule); err != nil {
				return ColumnPartDescriptor{}, nil, err
			}
			if blockDesc.FirstRow != expectedFirstRow {
				return ColumnPartDescriptor{}, nil, fmt.Errorf("colgranule: descriptor column %s block %d first row=%d want contiguous first row=%d", name, j, blockDesc.FirstRow, expectedFirstRow)
			}
			expectedFirstRow += blockDesc.RowCount
			if j == 0 {
				column.Definition.Encoding = blockDesc.Encoding
				column.Definition.Compression = blockDesc.Compression
			}
			columnDesc.Blocks = append(columnDesc.Blocks, blockDesc)
			column.Blocks = append(column.Blocks, ColumnBlock{Descriptor: blockDesc, Granule: granule})
		}
		if expectedFirstRow != desc.RowCount {
			return ColumnPartDescriptor{}, nil, fmt.Errorf("colgranule: descriptor column %s covers %d rows, want %d", name, expectedFirstRow, desc.RowCount)
		}
		desc.Columns = append(desc.Columns, columnDesc)
		columns[name] = column
	}
	if err := dec.finish(); err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	return desc, columns, nil
}

func validateDecodedGranuleDescriptors(desc ColumnPartDescriptor) error {
	if desc.RowCount > 0 && len(desc.Granules) == 0 {
		return fmt.Errorf("colgranule: descriptor rows=%d has no granules", desc.RowCount)
	}
	expectedFirstRow := 0
	for i, granule := range desc.Granules {
		if granule.Ordinal != i {
			return fmt.Errorf("colgranule: granule %d has ordinal=%d", i, granule.Ordinal)
		}
		if granule.RowCount <= 0 {
			return fmt.Errorf("colgranule: granule %d has invalid row count %d", i, granule.RowCount)
		}
		if granule.FirstRow != expectedFirstRow {
			return fmt.Errorf("colgranule: granule %d first row=%d want %d", i, granule.FirstRow, expectedFirstRow)
		}
		if granule.VisibleRows > granule.RowCount {
			return fmt.Errorf("colgranule: granule %d visible rows=%d exceed row count=%d", i, granule.VisibleRows, granule.RowCount)
		}
		if granule.DeletedRows > granule.RowCount {
			return fmt.Errorf("colgranule: granule %d deleted rows=%d exceed row count=%d", i, granule.DeletedRows, granule.RowCount)
		}
		if granule.VisibleRows+granule.DeletedRows > granule.RowCount {
			return fmt.Errorf("colgranule: granule %d visible+deleted rows=%d exceed row count=%d", i, granule.VisibleRows+granule.DeletedRows, granule.RowCount)
		}
		expectedFirstRow += granule.RowCount
	}
	if expectedFirstRow != desc.RowCount {
		return fmt.Errorf("colgranule: descriptor granules cover %d rows, want %d", expectedFirstRow, desc.RowCount)
	}
	return nil
}

func validateDecodedColumnBlockDescriptor(desc ColumnPartDescriptor, column string, columnType ColumnType, cardinality uint32, blockIndex int, block ColumnBlockDescriptor) error {
	if block.RowCount <= 0 {
		return fmt.Errorf("colgranule: descriptor column %s block %d has invalid row count %d", column, blockIndex, block.RowCount)
	}
	if block.FirstRow > desc.RowCount || block.RowCount > desc.RowCount-block.FirstRow {
		return fmt.Errorf("colgranule: descriptor column %s block %d first row=%d row count=%d outside part rows=%d", column, blockIndex, block.FirstRow, block.RowCount, desc.RowCount)
	}
	if block.FirstGranule > block.LastGranule {
		return fmt.Errorf("colgranule: descriptor column %s block %d granule range [%d,%d] is inverted", column, blockIndex, block.FirstGranule, block.LastGranule)
	}
	if len(desc.Granules) > 0 && block.LastGranule >= len(desc.Granules) {
		return fmt.Errorf("colgranule: descriptor column %s block %d last granule=%d outside granules=%d", column, blockIndex, block.LastGranule, len(desc.Granules))
	}
	firstGranule, lastGranule, err := decodedBlockGranuleRange(desc.Granules, block.FirstRow, block.RowCount)
	if err != nil {
		return fmt.Errorf("colgranule: descriptor column %s block %d: %w", column, blockIndex, err)
	}
	if block.FirstGranule != firstGranule || block.LastGranule != lastGranule {
		return fmt.Errorf("colgranule: descriptor column %s block %d granule range [%d,%d] want [%d,%d] for rows [%d,%d)", column, blockIndex, block.FirstGranule, block.LastGranule, firstGranule, lastGranule, block.FirstRow, block.FirstRow+block.RowCount)
	}
	maxRawBytes, err := maxDecodedBlockRawBytes(columnType, cardinality, block.Encoding, block.RowCount)
	if err != nil {
		return fmt.Errorf("colgranule: descriptor column %s block %d: %w", column, blockIndex, err)
	}
	if block.RawBytes > maxRawBytes {
		return fmt.Errorf("colgranule: descriptor column %s block %d raw bytes=%d exceed max=%d for %d rows", column, blockIndex, block.RawBytes, maxRawBytes, block.RowCount)
	}
	if block.Compression == CompressionNone && block.StoredBytes != block.RawBytes {
		return fmt.Errorf("colgranule: descriptor column %s block %d uncompressed stored bytes=%d raw bytes=%d", column, blockIndex, block.StoredBytes, block.RawBytes)
	}
	if block.Compression != CompressionNone && block.StoredBytes > block.RawBytes {
		return fmt.Errorf("colgranule: descriptor column %s block %d compressed stored bytes=%d exceed raw bytes=%d", column, blockIndex, block.StoredBytes, block.RawBytes)
	}
	return nil
}

func validateDecodedColumnBlockGranuleMetadata(column string, blockIndex int, granule EncodedGranule) error {
	if granule.Rows <= 0 {
		return fmt.Errorf("colgranule: descriptor column %s block %d granule has invalid row count %d", column, blockIndex, granule.Rows)
	}
	if granule.NullCount < 0 {
		return fmt.Errorf("colgranule: descriptor column %s block %d granule has negative null count %d", column, blockIndex, granule.NullCount)
	}
	if granule.DefaultCount < 0 {
		return fmt.Errorf("colgranule: descriptor column %s block %d granule has negative default count %d", column, blockIndex, granule.DefaultCount)
	}
	if granule.NullCount > granule.Rows || granule.DefaultCount > granule.Rows-granule.NullCount {
		return fmt.Errorf("colgranule: descriptor column %s block %d granule null/default count exceeds rows", column, blockIndex)
	}
	if granule.HasMinMax && granule.Min > granule.Max {
		return fmt.Errorf("colgranule: descriptor column %s block %d granule min=%d exceeds max=%d", column, blockIndex, granule.Min, granule.Max)
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

func maxDecodedBlockRawBytes(columnType ColumnType, cardinality uint32, encoding Encoding, rows int) (int, error) {
	switch columnType {
	case ColumnTypeInt64:
		switch encoding {
		case EncodingRawInt64:
			return checkedMulInt(rows, 8, "raw int64 bytes")
		case EncodingDeltaVarint, EncodingDoubleDeltaVarint:
			return checkedMulInt(rows, binary.MaxVarintLen64, "varint bytes")
		default:
			return 0, fmt.Errorf("unsupported int64 encoding %d", encoding)
		}
	case ColumnTypeLowCardinalityCode:
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
		if encoding != EncodingBoolBitpackRLE {
			return 0, fmt.Errorf("unsupported bool encoding %d", encoding)
		}
		rleBytes, err := checkedMulInt(rows, binary.MaxVarintLen64, "bool rle bytes")
		if err != nil {
			return 0, err
		}
		return checkedAddInt(2, rleBytes, "bool raw bytes")
	default:
		return 0, fmt.Errorf("unsupported column type %s", columnType)
	}
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
		return 0, fmt.Errorf("colgranule: %s=%d exceeds host int", field, v)
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

func decodeSortKeyMetadataSection(image ColumnPartImage) ([]SortKeyColumn, error) {
	section, err := image.singleSection(ColumnPartImageSectionSortKeyMetadata)
	if err != nil {
		return nil, err
	}
	dec := columnPartImageDecoder{data: image.sectionBytes(section)}
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
	dec := columnPartImageDecoder{data: image.sectionBytes(section)}
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
		return fmt.Errorf("colgranule: sort key mark %d has invalid row count %d", index, mark.Rows)
	}
	if len(mark.Columns) == 0 {
		return fmt.Errorf("colgranule: sort key mark %d has no columns", index)
	}
	if len(mark.Columns) > maxSortKeyColumns {
		return fmt.Errorf("colgranule: sort key mark %d columns=%d exceeds cap %d", index, len(mark.Columns), maxSortKeyColumns)
	}
	for columnIndex, column := range mark.Columns {
		if column == "" {
			return fmt.Errorf("colgranule: sort key mark %d has empty column name at %d", index, columnIndex)
		}
	}
	if len(mark.Prefixes) != len(mark.Columns) {
		return fmt.Errorf("colgranule: sort key mark %d prefixes=%d want columns=%d", index, len(mark.Prefixes), len(mark.Columns))
	}
	for prefixIndex, prefix := range mark.Prefixes {
		prefixLen := prefixIndex + 1
		if !sameStringSlice(prefix.Columns, mark.Columns[:prefixLen]) {
			return fmt.Errorf("colgranule: sort key mark %d prefix %d columns=%v want %v", index, prefixIndex, prefix.Columns, mark.Columns[:prefixLen])
		}
		if prefix.Lower.Exclusive {
			return fmt.Errorf("colgranule: sort key mark %d prefix %d lower bound is exclusive", index, prefixIndex)
		}
		if prefix.Lower.Unbounded {
			return fmt.Errorf("colgranule: sort key mark %d prefix %d lower bound is unbounded", index, prefixIndex)
		}
		if len(prefix.Lower.Values) != prefixLen {
			return fmt.Errorf("colgranule: sort key mark %d prefix %d lower values=%d want %d", index, prefixIndex, len(prefix.Lower.Values), prefixLen)
		}
		if !prefix.UpperExclusive.Exclusive {
			return fmt.Errorf("colgranule: sort key mark %d prefix %d upper bound is not exclusive", index, prefixIndex)
		}
		if prefix.UpperExclusive.Unbounded {
			if len(prefix.UpperExclusive.Values) != 0 {
				return fmt.Errorf("colgranule: sort key mark %d prefix %d unbounded upper has %d values", index, prefixIndex, len(prefix.UpperExclusive.Values))
			}
			continue
		}
		if len(prefix.UpperExclusive.Values) != prefixLen {
			return fmt.Errorf("colgranule: sort key mark %d prefix %d upper values=%d want %d", index, prefixIndex, len(prefix.UpperExclusive.Values), prefixLen)
		}
		if compareInt64Tuple(prefix.UpperExclusive.Values, prefix.Lower.Values) <= 0 {
			return fmt.Errorf("colgranule: sort key mark %d prefix %d upper bound %v is not greater than lower bound %v", index, prefixIndex, prefix.UpperExclusive.Values, prefix.Lower.Values)
		}
	}
	return nil
}

func validateDecodedSortKeyMarks(desc ColumnPartDescriptor, marks []SortKeyMark) error {
	if len(desc.SortKey) == 0 {
		return fmt.Errorf("colgranule: descriptor has no sort key")
	}
	if len(marks) != len(desc.Granules) {
		return fmt.Errorf("colgranule: sort key marks=%d granules=%d", len(marks), len(desc.Granules))
	}
	expectedColumns := make([]string, len(desc.SortKey))
	for i, column := range desc.SortKey {
		if column.Column == "" {
			return fmt.Errorf("colgranule: descriptor sort key has empty column at %d", i)
		}
		expectedColumns[i] = column.Column
	}
	for i, mark := range marks {
		if !sameStringSlice(mark.Columns, expectedColumns) {
			return fmt.Errorf("colgranule: sort key mark %d columns=%v want descriptor sort key %v", i, mark.Columns, expectedColumns)
		}
	}
	for i, granule := range desc.Granules {
		if granule.MarkOrdinal != i {
			return fmt.Errorf("colgranule: granule %d mark ordinal=%d want %d", i, granule.MarkOrdinal, i)
		}
		if granule.MarkOrdinal >= len(marks) {
			return fmt.Errorf("colgranule: granule %d mark ordinal=%d outside marks=%d", i, granule.MarkOrdinal, len(marks))
		}
		if marks[i].Rows != granule.RowCount {
			return fmt.Errorf("colgranule: granule %d rows=%d mark rows=%d", i, granule.RowCount, marks[i].Rows)
		}
	}
	return nil
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

func decodeRowLocatorsSection(image ColumnPartImage) (map[int64]RowLocator, error) {
	section, err := image.singleSection(ColumnPartImageSectionRowLocators)
	if err != nil {
		return nil, err
	}
	data := image.sectionBytes(section)
	if len(data) < 4 {
		return nil, fmt.Errorf("colgranule: truncated row locators section bytes=%d", len(data))
	}
	count := binary.LittleEndian.Uint32(data[:4])
	maxLocators := (len(data) - 4) / rowLocatorBytes
	if uint64(count) > uint64(maxLocators) {
		return nil, fmt.Errorf("colgranule: row locators count=%d exceeds section capacity=%d", count, maxLocators)
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
		return nil, fmt.Errorf("colgranule: row locators section bytes=%d want %d", len(data), wantBytes)
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
			return nil, fmt.Errorf("colgranule: row locator primary id %d reserved=%d want 0", primaryID, reserved)
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
			return nil, fmt.Errorf("colgranule: duplicate row locator primary id %d", primaryID)
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

func validateDecodedRowLocators(desc ColumnPartDescriptor, partID uint64, locators map[int64]RowLocator) error {
	if len(locators) != desc.RowCount {
		return fmt.Errorf("colgranule: row locator count=%d want part rows=%d", len(locators), desc.RowCount)
	}
	seenRows := make([]bool, desc.RowCount)
	for primaryID, locator := range locators {
		if locator.PrimaryID != primaryID {
			return fmt.Errorf("colgranule: row locator key %d has primary id %d", primaryID, locator.PrimaryID)
		}
		if locator.PartID != partID {
			return fmt.Errorf("colgranule: row locator primary id %d part id=%d want %d", primaryID, locator.PartID, partID)
		}
		if locator.PartRow < 0 || locator.PartRow >= desc.RowCount {
			return fmt.Errorf("colgranule: row locator primary id %d part row=%d outside part rows=%d", primaryID, locator.PartRow, desc.RowCount)
		}
		if locator.GranuleOrdinal < 0 || locator.GranuleOrdinal >= len(desc.Granules) {
			return fmt.Errorf("colgranule: row locator primary id %d granule ordinal=%d outside granules=%d", primaryID, locator.GranuleOrdinal, len(desc.Granules))
		}
		granule := desc.Granules[locator.GranuleOrdinal]
		if locator.RowInGranule < 0 || locator.RowInGranule >= granule.RowCount {
			return fmt.Errorf("colgranule: row locator primary id %d row in granule=%d outside granule rows=%d", primaryID, locator.RowInGranule, granule.RowCount)
		}
		partRow := granule.FirstRow + locator.RowInGranule
		if locator.PartRow != partRow {
			return fmt.Errorf("colgranule: row locator primary id %d part row=%d want %d from granule %d", primaryID, locator.PartRow, partRow, locator.GranuleOrdinal)
		}
		if seenRows[locator.PartRow] {
			return fmt.Errorf("colgranule: duplicate row locator part row %d", locator.PartRow)
		}
		seenRows[locator.PartRow] = true
	}
	return nil
}

func validateDecodedRowLocatorsPrimaryKey(desc ColumnPartDescriptor, columns map[string]ColumnPartColumn, locators map[int64]RowLocator) error {
	if len(desc.LogicalPrimaryKey) != 1 {
		return fmt.Errorf("colgranule: row locator validation requires one logical primary key column, got %d", len(desc.LogicalPrimaryKey))
	}
	primaryColumnName := desc.LogicalPrimaryKey[0]
	column, ok := columns[primaryColumnName]
	if !ok {
		return fmt.Errorf("colgranule: row locator validation missing primary key column %s", primaryColumnName)
	}
	if column.Definition.Type != ColumnTypeInt64 {
		return fmt.Errorf("colgranule: row locator validation primary key column %s type=%s want int64", primaryColumnName, column.Definition.Type)
	}
	var reader GranuleReader
	var scratch []int64
	for blockIndex, block := range column.Blocks {
		values, err := reader.DecodeInt64Into(scratch[:0], block.Granule)
		if err != nil {
			return fmt.Errorf("colgranule: decode primary key column %s block %d: %w", primaryColumnName, blockIndex, err)
		}
		scratch = values
		if len(values) != block.Descriptor.RowCount {
			return fmt.Errorf("colgranule: primary key column %s block %d decoded rows=%d want %d", primaryColumnName, blockIndex, len(values), block.Descriptor.RowCount)
		}
		for rowOffset, primaryID := range values {
			partRow := block.Descriptor.FirstRow + rowOffset
			locator, ok := locators[primaryID]
			if !ok {
				return fmt.Errorf("colgranule: row locator missing primary id %d for part row %d", primaryID, partRow)
			}
			if locator.PartRow != partRow {
				return fmt.Errorf("colgranule: row locator primary id %d points to part row %d want %d", primaryID, locator.PartRow, partRow)
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
		section, ok := image.columnDataSection(name)
		if !ok {
			return fmt.Errorf("colgranule: image missing column data section %s", name)
		}
		offset := section.Offset
		sectionEnd := section.Offset + section.Length
		for i := range column.Blocks {
			block := &column.Blocks[i]
			length := block.Descriptor.StoredBytes
			if length < 0 || offset > sectionEnd || length > sectionEnd-offset {
				return fmt.Errorf("colgranule: image column %s block %d length=%d outside section", name, i, length)
			}
			block.Granule.Payload = image.Bytes[offset : offset+length]
			// The payload slice is already narrowed to the block bytes; keep inline refs normalized for granule validation.
			block.Granule.PayloadRef = PayloadRef{Kind: PayloadRefInline, Length: length}
			offset += length
		}
		if offset != sectionEnd {
			return fmt.Errorf("colgranule: image column %s consumed=%d section=%d", name, offset-section.Offset, section.Length)
		}
		columns[name] = column
	}
	return nil
}

func validateColumnDataSectionsForColumns(image ColumnPartImage, columns map[string]ColumnPartColumn) error {
	required := make(map[string]struct{}, len(columns))
	for name := range columns {
		required[name] = struct{}{}
	}
	seen := make(map[string]ColumnPartImageSection, len(columns))
	for _, section := range image.Sections {
		if section.Kind != ColumnPartImageSectionColumnData {
			continue
		}
		if section.Column == "" {
			return fmt.Errorf("colgranule: image column data section at offset=%d has empty column", section.Offset)
		}
		if _, ok := required[section.Column]; !ok {
			return fmt.Errorf("colgranule: image has column data section for unknown column %s at offset=%d", section.Column, section.Offset)
		}
		previous, exists := seen[section.Column]
		if exists {
			return fmt.Errorf("colgranule: duplicate column data section %s at offset=%d previous_offset=%d", section.Column, section.Offset, previous.Offset)
		}
		seen[section.Column] = section
	}
	for name := range required {
		if _, ok := seen[name]; !ok {
			return fmt.Errorf("colgranule: image missing column data section %s", name)
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
		return nil, nil
	}
	if len(sections) != 1 {
		return nil, fmt.Errorf("colgranule: image has %d dictionary sections, want 1", len(sections))
	}
	dec := columnPartImageDecoder{data: i.sectionBytes(sections[0])}
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
			return nil, fmt.Errorf("colgranule: duplicate dictionary %s", name)
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
				return nil, fmt.Errorf("colgranule: duplicate dictionary value %s in %s", value, name)
			}
			if previous, exists := codes[code]; exists {
				return nil, fmt.Errorf("colgranule: duplicate dictionary code %d in %s for %q and %q", code, name, previous, value)
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
					return nil, fmt.Errorf("colgranule: aggregate metadata %s granule %d entry %d has zero count", metadata.Definition.Name, granuleOrdinalInt, j)
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
					return nil, fmt.Errorf("colgranule: aggregate metadata %s granule %d entry %d min=%d exceeds max=%d", metadata.Definition.Name, granuleOrdinalInt, j, minValue, maxValue)
				}
				granule.Entries = append(granule.Entries, AggregateMetadataEntry{Group: group, Count: count, Min: minValue, Max: maxValue})
				entryRows += int(count)
			}
			if entryRows != granule.MatchedRows {
				return nil, fmt.Errorf("colgranule: aggregate metadata %s granule %d entry rows=%d matched rows=%d", metadata.Definition.Name, granule.GranuleOrdinal, entryRows, granule.MatchedRows)
			}
			metadata.Granules = append(metadata.Granules, granule)
			totalMatchedRows += granule.MatchedRows
		}
		if metadata.Stats.Admitted && totalMatchedRows != metadata.Stats.RowsMatched {
			return nil, fmt.Errorf("colgranule: aggregate metadata %s granule matched rows=%d stats matched rows=%d", metadata.Definition.Name, totalMatchedRows, metadata.Stats.RowsMatched)
		}
		if !metadata.Stats.Admitted && len(metadata.Granules) != 0 {
			return nil, fmt.Errorf("colgranule: aggregate metadata %s is rejected but has %d granules", metadata.Definition.Name, len(metadata.Granules))
		}
		if err := dec.finish(); err != nil {
			return nil, err
		}
		if _, exists := out[metadata.Definition.Name]; exists {
			return nil, fmt.Errorf("colgranule: duplicate aggregate metadata %s at section %s offset=%d", metadata.Definition.Name, section.Name, section.Offset)
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
		return AggregateMetadataStats{}, fmt.Errorf("colgranule: negative aggregate metadata build duration %d", buildNanos)
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
		return 0, fmt.Errorf("colgranule: negative %s %d", field, v)
	}
	return float64(v) / 1_000_000, nil
}

func (i ColumnPartImage) singleSection(kind ColumnPartImageSectionKind) (ColumnPartImageSection, error) {
	sections := i.sectionsByKind(kind)
	if len(sections) != 1 {
		return ColumnPartImageSection{}, fmt.Errorf("colgranule: image has %d %s sections, want 1", len(sections), kind)
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

func (i ColumnPartImage) validateForRead() error {
	if i.TotalBytes() == 0 {
		return fmt.Errorf("colgranule: empty part image")
	}
	if i.Version != columnPartImageVersion {
		return fmt.Errorf("colgranule: unsupported part image version %d", i.Version)
	}
	if i.Rows < 0 {
		return fmt.Errorf("colgranule: negative image row count %d", i.Rows)
	}
	if i.ManifestBytes <= 0 || i.ManifestBytes > len(i.Bytes) {
		return fmt.Errorf("colgranule: manifest bytes=%d exceed image bytes=%d", i.ManifestBytes, len(i.Bytes))
	}
	for _, section := range i.Sections {
		if section.Kind == ColumnPartImageSectionManifest {
			return fmt.Errorf("colgranule: manifest is not a directory section")
		}
		if err := validateImageSectionCategory(section.Kind, section.Category); err != nil {
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

func validateImageSectionBounds(section ColumnPartImageSection, manifestBytes int, totalBytes int) error {
	if section.Offset < 0 {
		return fmt.Errorf("colgranule: section %s has negative offset %d", section.Kind, section.Offset)
	}
	if section.Offset < manifestBytes {
		return fmt.Errorf("colgranule: section %s offset=%d before manifest=%d", section.Kind, section.Offset, manifestBytes)
	}
	if section.Length < 0 {
		return fmt.Errorf("colgranule: section %s has negative length %d", section.Kind, section.Length)
	}
	if section.Length > totalBytes || section.Offset > totalBytes-section.Length {
		return fmt.Errorf("colgranule: section %s offset=%d length=%d exceeds image bytes=%d", section.Kind, section.Offset, section.Length, totalBytes)
	}
	return nil
}

func validateImageSectionCount(sectionCount uint32, manifestBytes int, decodedManifestBytes int) error {
	const minSectionDirectoryEntryBytes = 56
	remainingManifestBytes := manifestBytes - decodedManifestBytes
	if remainingManifestBytes < 0 {
		return fmt.Errorf("colgranule: decoded manifest bytes=%d exceed manifest bytes=%d", decodedManifestBytes, manifestBytes)
	}
	maxSections := remainingManifestBytes / minSectionDirectoryEntryBytes
	if int(sectionCount) > maxSections {
		return fmt.Errorf("colgranule: section count=%d exceeds manifest capacity=%d", sectionCount, maxSections)
	}
	return nil
}

func validateImageSectionLayout(sections []ColumnPartImageSection, manifestBytes int, totalBytes int) error {
	cursor := manifestBytes
	for _, section := range sections {
		if section.Offset != cursor {
			return fmt.Errorf("colgranule: section %s offset=%d want contiguous offset=%d", section.Kind, section.Offset, cursor)
		}
		cursor += section.Length
	}
	if cursor != totalBytes {
		return fmt.Errorf("colgranule: image sections cover %d bytes, image has %d", cursor, totalBytes)
	}
	return nil
}

func validateImageSectionMultiplicity(sections []ColumnPartImageSection) error {
	counts := make(map[ColumnPartImageSectionKind]int, len(sections))
	for _, section := range sections {
		counts[section.Kind]++
	}
	for _, kind := range []ColumnPartImageSectionKind{
		ColumnPartImageSectionDescriptor,
		ColumnPartImageSectionSortKeyMetadata,
		ColumnPartImageSectionSortKeyMarks,
		ColumnPartImageSectionRowLocators,
		ColumnPartImageSectionDictionaries,
	} {
		if counts[kind] > 1 {
			return fmt.Errorf("colgranule: image has %d %s sections, want at most 1", counts[kind], kind)
		}
	}
	return nil
}

func validateImageSectionCategory(kind ColumnPartImageSectionKind, category ColumnPartImageSectionCategory) error {
	expected, ok := expectedImageSectionCategory(kind)
	if ok && category != expected {
		return fmt.Errorf("colgranule: section %s category=%s want %s", kind, category, expected)
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
	case ColumnPartImageSectionDictionaries:
		return ColumnPartImageCategoryDictionaries, true
	case ColumnPartImageSectionColumnData:
		return ColumnPartImageCategoryDeclaredColumns, true
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
		return false, fmt.Errorf("colgranule: invalid boolean value %d", v)
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
		return "", fmt.Errorf("colgranule: string bytes=%d exceeds max=%d", lengthInt, maxColumnPartImageStringBytes)
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
		return fmt.Errorf("colgranule: truncated part image at offset %d need %d bytes have %d", d.offset, n, len(d.data)-d.offset)
	}
	return nil
}

func (d *columnPartImageDecoder) boundedCount(count uint32, minItemBytes int, label string) (int, error) {
	total, err := d.countToInt(count, label)
	if err != nil {
		return 0, err
	}
	if minItemBytes < 0 {
		return 0, fmt.Errorf("colgranule: invalid minimum item bytes %d for %s", minItemBytes, label)
	}
	if minItemBytes > 0 && total > (len(d.data)-d.offset)/minItemBytes {
		return 0, fmt.Errorf("colgranule: %s count=%d exceeds section capacity=%d", label, count, (len(d.data)-d.offset)/minItemBytes)
	}
	return total, nil
}

func (d *columnPartImageDecoder) countToInt(count uint32, label string) (int, error) {
	total := int(count)
	if uint64(total) != uint64(count) {
		return 0, fmt.Errorf("colgranule: %s count=%d exceeds host int", label, count)
	}
	return total, nil
}

func nonNegativeInt64ToInt(value int64, label string) (int, error) {
	if value < 0 {
		return 0, fmt.Errorf("colgranule: negative %s %d", label, value)
	}
	out := int(value)
	if int64(out) != value {
		return 0, fmt.Errorf("colgranule: %s=%d exceeds host int", label, value)
	}
	return out, nil
}

func (d *columnPartImageDecoder) finish() error {
	if d.offset != len(d.data) {
		return fmt.Errorf("colgranule: trailing part image section bytes=%d", len(d.data)-d.offset)
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
	default:
		return "", fmt.Errorf("colgranule: unknown column type code %d", code)
	}
}
