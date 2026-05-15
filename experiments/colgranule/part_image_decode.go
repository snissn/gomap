package colgranule

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"
)

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
	manifestBytes, err := dec.u32()
	if err != nil {
		return ColumnPartImage{}, err
	}
	sectionCount, err := dec.u32()
	if err != nil {
		return ColumnPartImage{}, err
	}
	sections := make([]ColumnPartImageSection, 0, sectionCount)
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
	if dec.offset != int(manifestBytes) {
		return ColumnPartImage{}, fmt.Errorf("colgranule: manifest bytes=%d decoded=%d", manifestBytes, dec.offset)
	}
	return ColumnPartImage{
		Version:       version,
		PartID:        partID,
		Rows:          int(rows),
		ManifestBytes: int(manifestBytes),
		Sections:      sections,
		Bytes:         data,
	}, nil
}

func ColumnPartFromImage(image ColumnPartImage) (*ColumnPart, error) {
	if image.TotalBytes() == 0 {
		return nil, fmt.Errorf("colgranule: empty part image")
	}
	descriptorSection, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		return nil, err
	}
	desc, columns, err := decodeColumnPartDescriptorSection(image.sectionBytes(descriptorSection))
	if err != nil {
		return nil, err
	}
	desc.PartID = image.PartID
	desc.RowCount = image.Rows
	sortKey, err := decodeSortKeyMetadataSection(image)
	if err != nil {
		return nil, err
	}
	desc.SortKey = sortKey
	marks, err := decodeSortKeyMarksSection(image)
	if err != nil {
		return nil, err
	}
	locators, err := decodeRowLocatorsSection(image)
	if err != nil {
		return nil, err
	}
	if err := attachImageColumnPayloads(image, columns); err != nil {
		return nil, err
	}
	optionsColumns := make([]ColumnDefinition, 0, len(desc.Columns))
	for _, columnDescriptor := range desc.Columns {
		column := columns[columnDescriptor.Name]
		def := column.Definition
		if def.Type == ColumnTypeLowCardinalityCode {
			granules := make([]EncodedGranule, len(column.Blocks))
			for i, block := range column.Blocks {
				granules[i] = block.Granule
			}
			cardinality, err := inferCodeCardinality(granules, 0)
			if err != nil {
				return nil, fmt.Errorf("colgranule: infer cardinality for %s: %w", def.Name, err)
			}
			def.Cardinality = cardinality
			column.Definition = def
			columns[columnDescriptor.Name] = column
		}
		optionsColumns = append(optionsColumns, def)
	}
	aggregateMetadata, err := decodeAggregateMetadataSections(image)
	if err != nil {
		return nil, err
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
	visibleRows, err := dec.i64()
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
	desc := ColumnPartDescriptor{
		Version:           uint8(version),
		PartID:            partID,
		SchemaVersion:     schemaVersion,
		RowCount:          int(rowCount),
		VisibleRowCount:   int(visibleRows),
		LogicalPrimaryKey: logicalPrimaryKey,
		Granules:          make([]GranuleDescriptor, 0, granuleCount),
	}
	for i := 0; i < int(granuleCount); i++ {
		granule, err := decodeGranuleDescriptor(&dec)
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		desc.Granules = append(desc.Granules, granule)
	}
	columnCount, err := dec.u32()
	if err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	columns := make(map[string]ColumnPartColumn, columnCount)
	for i := 0; i < int(columnCount); i++ {
		name, err := dec.str()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		columnTypeCode, err := dec.u16()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		columnType, err := columnTypeFromCode(columnTypeCode)
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		blockCount, err := dec.u32()
		if err != nil {
			return ColumnPartDescriptor{}, nil, err
		}
		columnDesc := ColumnPartColumnDescriptor{Name: name, Type: columnType, Blocks: make([]ColumnBlockDescriptor, 0, blockCount)}
		column := ColumnPartColumn{
			Definition: ColumnDefinition{
				Name: name,
				Type: columnType,
			},
			Blocks: make([]ColumnBlock, 0, blockCount),
		}
		for j := 0; j < int(blockCount); j++ {
			blockDesc, granule, err := decodeColumnBlockDescriptorAndGranule(&dec)
			if err != nil {
				return ColumnPartDescriptor{}, nil, err
			}
			if j == 0 {
				column.Definition.Encoding = blockDesc.Encoding
				column.Definition.Compression = blockDesc.Compression
			}
			columnDesc.Blocks = append(columnDesc.Blocks, blockDesc)
			column.Blocks = append(column.Blocks, ColumnBlock{Descriptor: blockDesc, Granule: granule})
		}
		desc.Columns = append(desc.Columns, columnDesc)
		columns[name] = column
	}
	if err := dec.finish(); err != nil {
		return ColumnPartDescriptor{}, nil, err
	}
	return desc, columns, nil
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
	return GranuleDescriptor{
		Ordinal:          int(ordinal),
		FirstRow:         int(firstRow),
		RowCount:         int(rowCount),
		VisibleRows:      int(visibleRows),
		DeletedRows:      int(deletedRows),
		IDLower:          idLower,
		IDUpperExclusive: idUpper,
		MarkOrdinal:      int(markOrdinal),
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
	encoding := Encoding(encodingCode)
	compression := Compression(compressionCode)
	desc := ColumnBlockDescriptor{
		FirstRow:          int(firstRow),
		RowCount:          int(rowCount),
		FirstGranule:      int(firstGranule),
		LastGranule:       int(lastGranule),
		Encoding:          encoding,
		Compression:       compression,
		RawBytes:          int(rawBytes),
		StoredBytes:       int(storedBytes),
		CodecBlockOrdinal: int(ordinal),
	}
	granule := EncodedGranule{
		Rows:         int(rowCount),
		NullCount:    int(nullCount),
		DefaultCount: int(defaultCount),
		HasMinMax:    hasMinMax,
		Min:          minValue,
		Max:          maxValue,
		Encoding:     encoding,
		Compression:  compression,
		RawBytes:     int(rawBytes),
		StoredBytes:  int(storedBytes),
		PayloadRef: PayloadRef{
			Kind:   PayloadRefInline,
			Length: int(storedBytes),
		},
		CodecReport: CodecReport{
			Encoding:             encoding,
			ActualCompression:    compression,
			RequestedCompression: compression,
			RawBytes:             int(rawBytes),
			StoredBytes:          int(storedBytes),
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
	out := make([]SortKeyColumn, 0, count)
	for i := 0; i < int(count); i++ {
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
	out := make([]SortKeyMark, 0, count)
	for i := 0; i < int(count); i++ {
		rows, err := dec.i64()
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
		mark := SortKeyMark{Rows: int(rows), Columns: columns, Prefixes: make([]SortKeyPrefixSummary, 0, prefixCount)}
		for j := 0; j < int(prefixCount); j++ {
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
		out = append(out, mark)
	}
	if err := dec.finish(); err != nil {
		return nil, err
	}
	return out, nil
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
	dec := columnPartImageDecoder{data: image.sectionBytes(section)}
	count, err := dec.u32()
	if err != nil {
		return nil, err
	}
	out := make(map[int64]RowLocator, count)
	for i := 0; i < int(count); i++ {
		primaryID, err := dec.i64()
		if err != nil {
			return nil, err
		}
		partID, err := dec.u64()
		if err != nil {
			return nil, err
		}
		partRow, err := dec.u32()
		if err != nil {
			return nil, err
		}
		granuleOrdinal, err := dec.u32()
		if err != nil {
			return nil, err
		}
		rowInGranule, err := dec.u32()
		if err != nil {
			return nil, err
		}
		if _, err := dec.u32(); err != nil {
			return nil, err
		}
		out[primaryID] = RowLocator{
			PrimaryID:      primaryID,
			PartID:         partID,
			PartRow:        int(partRow),
			GranuleOrdinal: int(granuleOrdinal),
			RowInGranule:   int(rowInGranule),
		}
	}
	if err := dec.finish(); err != nil {
		return nil, err
	}
	return out, nil
}

func attachImageColumnPayloads(image ColumnPartImage, columns map[string]ColumnPartColumn) error {
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
			if length < 0 || offset+length > sectionEnd {
				return fmt.Errorf("colgranule: image column %s block %d length=%d outside section", name, i, length)
			}
			block.Granule.Payload = image.Bytes[offset : offset+length]
			block.Granule.PayloadRef = PayloadRef{Kind: PayloadRefInline, Offset: int64(offset), Length: length}
			offset += length
		}
		if offset != sectionEnd {
			return fmt.Errorf("colgranule: image column %s consumed=%d section=%d", name, offset-section.Offset, section.Length)
		}
		columns[name] = column
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
		metadata := AggregateMetadata{
			Definition: def,
			Stats:      stats,
			Granules:   make([]AggregateMetadataGranule, 0, granuleCount),
		}
		for i := 0; i < int(granuleCount); i++ {
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
			granule := AggregateMetadataGranule{
				GranuleOrdinal: int(granuleOrdinal),
				FirstRow:       int(firstRow),
				RowCount:       int(rowCount),
				MatchedRows:    int(matchedRows),
				Entries:        make([]AggregateMetadataEntry, 0, entryCount),
			}
			for j := 0; j < int(entryCount); j++ {
				group, err := dec.u32()
				if err != nil {
					return nil, err
				}
				count, err := dec.u32()
				if err != nil {
					return nil, err
				}
				minValue, err := dec.i64()
				if err != nil {
					return nil, err
				}
				maxValue, err := dec.i64()
				if err != nil {
					return nil, err
				}
				granule.Entries = append(granule.Entries, AggregateMetadataEntry{Group: group, Count: count, Min: minValue, Max: maxValue})
			}
			metadata.Granules = append(metadata.Granules, granule)
		}
		if err := dec.finish(); err != nil {
			return nil, err
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
	measures := make([]AggregateMetadataMeasure, 0, measureCount)
	for i := 0; i < int(measureCount); i++ {
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
	predicates := make([]AggregateMetadataPredicate, 0, predicateCount)
	for i := 0; i < int(predicateCount); i++ {
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
	return AggregateMetadataDefinition{
		Name:           name,
		Version:        version,
		Kind:           AggregateMetadataKind(kind),
		Scope:          AggregateMetadataScope(scope),
		GroupKeys:      groupKeys,
		Measures:       measures,
		Predicates:     predicates,
		MaxBytesPerRow: float64(maxBytesPerRow) / 1_000_000,
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
	return AggregateMetadataStats{
		Admitted:            admitted,
		RejectedReason:      rejectedReason,
		BuildDuration:       time.Duration(buildNanos),
		Granules:            int(granules),
		GranulesWithRows:    int(granulesWithRows),
		RowsMatched:         int(rowsMatched),
		Entries:             int(entries),
		ValueBytes:          int(valueBytes),
		DescriptorBytes:     int(descriptorBytes),
		TotalBytes:          int(totalBytes),
		BytesPerPartRow:     float64(bytesPerPartRow) / 1_000_000,
		BytesPerMatchedRow:  float64(bytesPerMatchedRow) / 1_000_000,
		Compression:         compression,
		AdmissionMaxBytes:   float64(admissionMaxBytes) / 1_000_000,
		AdmissionMeasuredBy: admissionMeasuredBy,
	}, nil
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

func validateImageSectionBounds(section ColumnPartImageSection, manifestBytes int, totalBytes int) error {
	if section.Offset < manifestBytes {
		return fmt.Errorf("colgranule: section %s offset=%d before manifest=%d", section.Kind, section.Offset, manifestBytes)
	}
	if section.Length < 0 {
		return fmt.Errorf("colgranule: section %s has negative length %d", section.Kind, section.Length)
	}
	if section.Offset+section.Length > totalBytes {
		return fmt.Errorf("colgranule: section %s offset=%d length=%d exceeds image bytes=%d", section.Kind, section.Offset, section.Length, totalBytes)
	}
	return nil
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
	if err := d.require(int(length)); err != nil {
		return "", err
	}
	v := string(d.data[d.offset : d.offset+int(length)])
	d.offset += int(length)
	return v, nil
}

func (d *columnPartImageDecoder) stringSlice() ([]string, error) {
	count, err := d.u32()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, count)
	for i := 0; i < int(count); i++ {
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
	out := make([]int64, 0, count)
	for i := 0; i < int(count); i++ {
		value, err := d.i64()
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func (d *columnPartImageDecoder) require(n int) error {
	if n < 0 || d.offset+n > len(d.data) {
		return fmt.Errorf("colgranule: truncated part image at offset %d need %d bytes have %d", d.offset, n, len(d.data)-d.offset)
	}
	return nil
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

func columnPartImageSectionKindFromCode(code uint16) (ColumnPartImageSectionKind, error) {
	switch code {
	case 8:
		return ColumnPartImageSectionManifest, nil
	case 1:
		return ColumnPartImageSectionDescriptor, nil
	case 2:
		return ColumnPartImageSectionSortKeyMetadata, nil
	case 3:
		return ColumnPartImageSectionSortKeyMarks, nil
	case 4:
		return ColumnPartImageSectionRowLocators, nil
	case 5:
		return ColumnPartImageSectionAggregateMetadata, nil
	case 6:
		return ColumnPartImageSectionDictionaries, nil
	case 7:
		return ColumnPartImageSectionColumnData, nil
	default:
		return "", fmt.Errorf("colgranule: unknown image section kind code %d", code)
	}
}

func columnPartImageSectionCategoryFromCode(code uint16) (ColumnPartImageSectionCategory, error) {
	switch code {
	case 8:
		return ColumnPartImageCategoryManifest, nil
	case 1:
		return ColumnPartImageCategoryDescriptor, nil
	case 2:
		return ColumnPartImageCategorySortKeyMetadata, nil
	case 3:
		return ColumnPartImageCategoryMarks, nil
	case 4:
		return ColumnPartImageCategoryLocators, nil
	case 5:
		return ColumnPartImageCategoryAggregateMetadata, nil
	case 6:
		return ColumnPartImageCategoryDictionaries, nil
	case 7:
		return ColumnPartImageCategoryDeclaredColumns, nil
	default:
		return "", fmt.Errorf("colgranule: unknown image section category code %d", code)
	}
}
