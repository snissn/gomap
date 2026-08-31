package typedcolumn

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"
)

const columnPartImageVersion uint16 = 4

const columnPartImageMagic uint32 = 0x4d494354 // "TCIM", little-endian on disk.

const columnPartImageSectionAlignment = 8

const (
	rowLocatorContiguousMagic        uint32 = 0x54434c52 // "RLCT", little-endian on disk.
	rowLocatorContiguousVersion      uint16 = 1
	rowLocatorContiguousPayloadBytes        = 32
	dictionaryDenseMagic             uint32 = 0x54434944 // "DICT", little-endian on disk.
	dictionaryDenseVersion           uint16 = 1
)

type ColumnPartImageSectionKind string

const (
	ColumnPartImageSectionManifest          ColumnPartImageSectionKind = "manifest"
	ColumnPartImageSectionDescriptor        ColumnPartImageSectionKind = "descriptor"
	ColumnPartImageSectionSortKeyMetadata   ColumnPartImageSectionKind = "sort_key_metadata"
	ColumnPartImageSectionSortKeyMarks      ColumnPartImageSectionKind = "sort_key_marks"
	ColumnPartImageSectionRowLocators       ColumnPartImageSectionKind = "row_locators"
	ColumnPartImageSectionAggregateMetadata ColumnPartImageSectionKind = "aggregate_metadata"
	ColumnPartImageSectionColumnStats       ColumnPartImageSectionKind = "column_stats"
	ColumnPartImageSectionPruningMetadata   ColumnPartImageSectionKind = "pruning_metadata"
	ColumnPartImageSectionDictionaries      ColumnPartImageSectionKind = "dictionaries"
	ColumnPartImageSectionLayoutContract    ColumnPartImageSectionKind = "layout_contract"
	ColumnPartImageSectionColumnData        ColumnPartImageSectionKind = "column_data"
	ColumnPartImageSectionColumnOffsets     ColumnPartImageSectionKind = "column_offsets"
	ColumnPartImageSectionColumnValues      ColumnPartImageSectionKind = "column_values"
	ColumnPartImageSectionPadding           ColumnPartImageSectionKind = "padding"
)

type ColumnPartImageSectionCategory string

const (
	ColumnPartImageCategoryManifest              ColumnPartImageSectionCategory = "manifest"
	ColumnPartImageCategoryDescriptor            ColumnPartImageSectionCategory = "descriptor"
	ColumnPartImageCategorySortKeyMetadata       ColumnPartImageSectionCategory = "sort_key_metadata"
	ColumnPartImageCategoryMarks                 ColumnPartImageSectionCategory = "marks"
	ColumnPartImageCategoryLocators              ColumnPartImageSectionCategory = "locators"
	ColumnPartImageCategoryAggregateMetadata     ColumnPartImageSectionCategory = "aggregate_metadata"
	ColumnPartImageCategoryColumnStats           ColumnPartImageSectionCategory = "column_stats"
	ColumnPartImageCategoryPruningMetadata       ColumnPartImageSectionCategory = "pruning_metadata"
	ColumnPartImageCategoryDictionaries          ColumnPartImageSectionCategory = "dictionaries"
	ColumnPartImageCategoryLayoutContract        ColumnPartImageSectionCategory = "layout_contract"
	ColumnPartImageCategoryDeclaredColumns       ColumnPartImageSectionCategory = "declared_columns"
	ColumnPartImageCategoryDeclaredColumnOffsets ColumnPartImageSectionCategory = "declared_column_offsets"
	ColumnPartImageCategoryDeclaredColumnValues  ColumnPartImageSectionCategory = "declared_column_values"
	ColumnPartImageCategoryPadding               ColumnPartImageSectionCategory = "padding"
)

type ColumnPartImageOptions struct {
	Dictionaries        map[string]map[string]int64
	LayoutLogicalTypes  map[string]string
	DictionaryOrder     map[string]bool
	DictionaryCollation map[string]string
	// SectionAlignment defaults to 8. Writers may request 64-byte section
	// placement for direct SIMD views; readers continue accepting 8-byte images.
	SectionAlignment int
	// SectionCompression compresses eligible whole-image sections. It is
	// intentionally limited to sections whose raw length can be recovered from
	// existing manifest fields without a TCIM/TCS1 format change.
	SectionCompression                   Compression
	RowLocatorSectionCompression         Compression
	RowLocatorSectionCompressionSet      bool
	DictionarySectionCompression         Compression
	DictionarySectionCompressionSet      bool
	PruningMetadataSectionCompression    Compression
	PruningMetadataSectionCompressionSet bool
}

type ColumnPartImage struct {
	Version       uint16                   `json:"version"`
	PartID        uint64                   `json:"part_id"`
	Rows          int                      `json:"rows"`
	ManifestBytes int                      `json:"manifest_bytes"`
	Sections      []ColumnPartImageSection `json:"sections"`
	Bytes         []byte                   `json:"-"`
}

type ColumnPartImageSection struct {
	Kind        ColumnPartImageSectionKind     `json:"kind"`
	Category    ColumnPartImageSectionCategory `json:"category"`
	Name        string                         `json:"name,omitempty"`
	Column      string                         `json:"column,omitempty"`
	Offset      int                            `json:"offset"`
	Length      int                            `json:"length"`
	Rows        int                            `json:"rows,omitempty"`
	Granules    int                            `json:"granules,omitempty"`
	Blocks      int                            `json:"blocks,omitempty"`
	Encoding    Encoding                       `json:"encoding,omitempty"`
	Compression Compression                    `json:"compression,omitempty"`
	RawBytes    int                            `json:"raw_bytes,omitempty"`
}

type ColumnPartImageSectionByteAccounting struct {
	Kind        ColumnPartImageSectionKind     `json:"kind"`
	Category    ColumnPartImageSectionCategory `json:"category"`
	Name        string                         `json:"name,omitempty"`
	Column      string                         `json:"column,omitempty"`
	Bytes       int                            `json:"bytes"`
	Compression Compression                    `json:"compression,omitempty"`
	RawBytes    int                            `json:"raw_bytes,omitempty"`
	StoredBytes int                            `json:"stored_bytes,omitempty"`
}

func BuildColumnPartImage(part *ColumnPart, opts ColumnPartImageOptions) (ColumnPartImage, error) {
	if part == nil {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: nil part")
	}
	sectionAlignment, err := columnPartImageAlignment(opts.SectionAlignment)
	if err != nil {
		return ColumnPartImage{}, err
	}
	builder := columnPartImageBuilder{part: part, opts: opts, sectionAlignment: sectionAlignment}
	return builder.build()
}

func (p *ColumnPart) WithImagePayloads(image ColumnPartImage) (*ColumnPart, error) {
	if p == nil {
		return nil, fmt.Errorf("typedcolumn: nil part")
	}
	if image.TotalBytes() == 0 {
		return nil, fmt.Errorf("typedcolumn: empty part image")
	}
	if err := image.validateForRead(); err != nil {
		return nil, err
	}
	if image.PartID != p.Descriptor.PartID {
		return nil, fmt.Errorf("typedcolumn: image part id=%d does not match part id=%d", image.PartID, p.Descriptor.PartID)
	}
	if image.Rows != p.Descriptor.RowCount {
		return nil, fmt.Errorf("typedcolumn: image rows=%d does not match part rows=%d", image.Rows, p.Descriptor.RowCount)
	}
	if err := validateImageDescriptorMatchesPart(image, p); err != nil {
		return nil, err
	}
	out := *p
	out.ColumnStats = cloneColumnPartStats(p.ColumnStats)
	out.PruningMetadata = cloneColumnPartPruning(p.PruningMetadata)
	out.Columns = make(map[string]ColumnPartColumn, len(p.Columns))
	for name, column := range p.Columns {
		outColumn := column
		outColumn.Blocks = append([]ColumnBlock(nil), column.Blocks...)
		out.Columns[name] = outColumn
	}
	if err := attachColumnPayloadsFromImage(image, out.Columns); err != nil {
		return nil, err
	}
	return &out, nil
}

func (i ColumnPartImage) TotalBytes() int {
	return len(i.Bytes)
}

func (i ColumnPartImage) CategoryBytes(category ColumnPartImageSectionCategory) int {
	switch category {
	case ColumnPartImageCategoryManifest:
		return i.ManifestBytes
	case ColumnPartImageCategoryPadding:
		return i.PaddingBytes()
	}
	total := 0
	for _, section := range i.Sections {
		if section.Category == category {
			total += section.Length
		}
	}
	return total
}

func (i ColumnPartImage) PaddingBytes() int {
	cursor := i.ManifestBytes
	padding := 0
	for _, section := range i.Sections {
		if section.Offset > cursor {
			padding += section.Offset - cursor
		}
		if end := section.Offset + section.Length; end > cursor {
			cursor = end
		}
	}
	if len(i.Bytes) > cursor {
		padding += len(i.Bytes) - cursor
	}
	return padding
}

func (i ColumnPartImage) SectionByteAccounting() []ColumnPartImageSectionByteAccounting {
	out := make([]ColumnPartImageSectionByteAccounting, 0, len(i.Sections)+1)
	if i.ManifestBytes > 0 {
		out = append(out, ColumnPartImageSectionByteAccounting{
			Kind:     ColumnPartImageSectionManifest,
			Category: ColumnPartImageCategoryManifest,
			Bytes:    i.ManifestBytes,
		})
	}
	if padding := i.PaddingBytes(); padding > 0 {
		out = append(out, ColumnPartImageSectionByteAccounting{
			Kind:     ColumnPartImageSectionPadding,
			Category: ColumnPartImageCategoryPadding,
			Bytes:    padding,
		})
	}
	for _, section := range i.Sections {
		rawBytes := section.Length
		if section.Kind == ColumnPartImageSectionRowLocators {
			if raw, err := rowLocatorSectionRawBytes(section.Rows); err == nil {
				rawBytes = raw
			} else {
				rawBytes = 0
			}
		} else if section.RawBytes > 0 {
			rawBytes = section.RawBytes
		}
		out = append(out, ColumnPartImageSectionByteAccounting{
			Kind:        section.Kind,
			Category:    section.Category,
			Name:        section.Name,
			Column:      section.Column,
			Bytes:       section.Length,
			Compression: section.Compression,
			RawBytes:    rawBytes,
			StoredBytes: section.Length,
		})
	}
	return out
}

func (i ColumnPartImage) columnDataSection(column string) (ColumnPartImageSection, bool) {
	for _, section := range i.Sections {
		if section.Kind == ColumnPartImageSectionColumnData && section.Column == column {
			return section, true
		}
	}
	return ColumnPartImageSection{}, false
}

func (i ColumnPartImage) columnOffsetsListSections(column string) (ColumnPartImageSection, ColumnPartImageSection, bool) {
	var offsets ColumnPartImageSection
	var values ColumnPartImageSection
	foundOffsets := false
	foundValues := false
	for _, section := range i.Sections {
		if section.Column != column {
			continue
		}
		switch section.Kind {
		case ColumnPartImageSectionColumnOffsets:
			if foundOffsets {
				return ColumnPartImageSection{}, ColumnPartImageSection{}, false
			}
			offsets = section
			foundOffsets = true
		case ColumnPartImageSectionColumnValues:
			if foundValues {
				return ColumnPartImageSection{}, ColumnPartImageSection{}, false
			}
			values = section
			foundValues = true
		}
	}
	return offsets, values, foundOffsets && foundValues
}

// ColumnOffsetsListSections returns the unique offsets and values sections for
// the named offsets-list column. The boolean is false when either section is
// missing or duplicated, so callers fail closed instead of selecting a last
// matching section from a malformed image.
func (i ColumnPartImage) ColumnOffsetsListSections(column string) (ColumnPartImageSection, ColumnPartImageSection, bool) {
	return i.columnOffsetsListSections(column)
}

func validateImageDescriptorMatchesPart(image ColumnPartImage, part *ColumnPart) error {
	descriptorSection, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		return err
	}
	imageDesc, imageColumns, err := decodeColumnPartDescriptorSection(image.sectionBytes(descriptorSection))
	if err != nil {
		return err
	}
	partDesc := part.Descriptor
	imageDesc.SortKey = nil
	partDesc.SortKey = nil
	if !reflect.DeepEqual(imageDesc, partDesc) {
		return fmt.Errorf("typedcolumn: image descriptor does not match part descriptor")
	}
	sortKey, err := decodeSortKeyMetadataSection(image)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(sortKey, part.Descriptor.SortKey) {
		return fmt.Errorf("typedcolumn: image sort key does not match part sort key")
	}
	if len(imageColumns) != len(part.Columns) {
		return fmt.Errorf("typedcolumn: image has %d columns, part has %d", len(imageColumns), len(part.Columns))
	}
	for name, imageColumn := range imageColumns {
		partColumn, ok := part.Columns[name]
		if !ok {
			return fmt.Errorf("typedcolumn: image descriptor has unknown column %s", name)
		}
		partDefinition, err := comparablePartColumnDefinitionForImage(imageColumn.Definition, partColumn)
		if err != nil {
			return err
		}
		if comparableColumnDefinition(imageColumn.Definition) != partDefinition {
			return fmt.Errorf("typedcolumn: image descriptor column %s definition does not match part", name)
		}
		if len(imageColumn.Blocks) != len(partColumn.Blocks) {
			return fmt.Errorf("typedcolumn: image descriptor column %s blocks=%d part blocks=%d", name, len(imageColumn.Blocks), len(partColumn.Blocks))
		}
		for i := range imageColumn.Blocks {
			if imageColumn.Blocks[i].Descriptor != partColumn.Blocks[i].Descriptor {
				return fmt.Errorf("typedcolumn: image descriptor column %s block %d descriptor does not match part", name, i)
			}
			if comparableGranuleMetadata(imageColumn.Blocks[i].Granule) != comparableGranuleMetadata(partColumn.Blocks[i].Granule) {
				return fmt.Errorf("typedcolumn: image descriptor column %s block %d granule metadata does not match part", name, i)
			}
		}
	}
	return nil
}

func comparablePartColumnDefinitionForImage(imageDefinition ColumnDefinition, partColumn ColumnPartColumn) (ColumnDefinition, error) {
	definition := comparableColumnDefinition(partColumn.Definition)
	if imageDefinition.Type != ColumnTypeLowCardinalityCode || definition.Cardinality != 0 {
		return definition, nil
	}
	cardinality, err := imageColumnCardinalityForDescriptor(ColumnPartColumnDescriptor{
		Name: imageDefinition.Name,
		Type: imageDefinition.Type,
	}, partColumn)
	if err != nil {
		return ColumnDefinition{}, err
	}
	definition.Cardinality = cardinality
	return definition, nil
}

func comparableColumnDefinition(def ColumnDefinition) ColumnDefinition {
	def.CodecBlockRows = 0
	def.Compression = 0
	def.StatsDisabled = false
	return def
}

type comparableEncodedGranuleMetadata struct {
	Rows         int
	NullCount    int
	DefaultCount int
	HasMinMax    bool
	Min          int64
	Max          int64
	Encoding     Encoding
	Compression  Compression
	RawBytes     int
	StoredBytes  int
}

func comparableGranuleMetadata(granule EncodedGranule) comparableEncodedGranuleMetadata {
	return comparableEncodedGranuleMetadata{
		Rows:         granule.Rows,
		NullCount:    granule.NullCount,
		DefaultCount: granule.DefaultCount,
		HasMinMax:    granule.HasMinMax,
		Min:          granule.Min,
		Max:          granule.Max,
		Encoding:     granule.Encoding,
		Compression:  granule.Compression,
		RawBytes:     granule.RawBytes,
		StoredBytes:  granule.StoredBytes,
	}
}

type columnPartImageBuilder struct {
	part             *ColumnPart
	opts             ColumnPartImageOptions
	sectionAlignment int
	sections         []columnPartImageSectionData
}

type columnPartImageSectionData struct {
	section ColumnPartImageSection
	data    []byte
	chunks  [][]byte
}

func (b *columnPartImageBuilder) build() (ColumnPartImage, error) {
	if err := b.addDescriptorSection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addSortKeyMetadataSection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addSortKeyMarksSection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addRowLocatorsSection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addAggregateMetadataSections(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addColumnStatsSection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addPruningMetadataSection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addDictionarySection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addColumnDataSections(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addLayoutContractSection(); err != nil {
		return ColumnPartImage{}, err
	}

	sections, manifest, err := b.layoutManifestAndSections()
	if err != nil {
		return ColumnPartImage{}, err
	}
	imageBytes := len(manifest)
	if len(sections) > 0 {
		last := sections[len(sections)-1]
		imageBytes = last.Offset + last.Length
	}
	out := make([]byte, 0, imageBytes)
	out = append(out, manifest...)
	for _, section := range b.sections {
		if section.section.Offset < len(out) {
			return ColumnPartImage{}, fmt.Errorf("typedcolumn: section %s offset=%d overlaps image bytes=%d", section.section.Kind, section.section.Offset, len(out))
		}
		out = append(out, make([]byte, section.section.Offset-len(out))...)
		out = section.appendPayloadTo(out)
	}
	image := ColumnPartImage{
		Version:       columnPartImageVersion,
		PartID:        b.part.Descriptor.PartID,
		Rows:          b.part.Descriptor.RowCount,
		ManifestBytes: len(manifest),
		Sections:      sections,
		Bytes:         out,
	}
	if _, err := CertifyColumnPartLayoutContractFromImage(image); err != nil {
		return ColumnPartImage{}, fmt.Errorf("typedcolumn: writer-certified layout contract validation: %w", err)
	}
	return image, nil
}

func (b *columnPartImageBuilder) layoutManifestAndSections() ([]ColumnPartImageSection, []byte, error) {
	sections := make([]ColumnPartImageSection, len(b.sections))
	for i := range b.sections {
		sections[i] = b.sections[i].section
	}
	manifestBytes := 0
	for attempt := 0; attempt < 8; attempt++ {
		manifest, err := encodeColumnPartImageManifest(b.part, sections, manifestBytes)
		if err != nil {
			return nil, nil, err
		}
		offset := alignColumnPartImageOffsetTo(len(manifest), b.sectionAlignment)
		for i := range b.sections {
			offset = alignColumnPartImageOffsetTo(offset, b.sectionAlignment)
			b.sections[i].section.Offset = offset
			b.sections[i].section.Length = b.sections[i].payloadLen()
			sections[i] = b.sections[i].section
			offset += b.sections[i].payloadLen()
		}
		if err := b.refreshLayoutContractSection(sections, len(manifest)); err != nil {
			return nil, nil, err
		}
		finalManifest, err := encodeColumnPartImageManifest(b.part, sections, len(manifest))
		if err != nil {
			return nil, nil, err
		}
		if len(finalManifest) == len(manifest) {
			return sections, finalManifest, nil
		}
		manifestBytes = len(finalManifest)
	}
	return nil, nil, fmt.Errorf("typedcolumn: part image manifest length did not stabilize")
}

func (b *columnPartImageBuilder) addDescriptorSection() error {
	var enc columnPartImageEncoder
	desc := b.part.Descriptor
	enc.u16(uint16(desc.Version))
	enc.u64(desc.PartID)
	enc.u32(desc.SchemaVersion)
	enc.i64(int64(desc.RowCount))
	enc.i64(int64(desc.VisibleRowCount))
	enc.stringSlice(desc.LogicalPrimaryKey)
	enc.u32(uint32(len(desc.Granules)))
	for _, granule := range desc.Granules {
		enc.i64(int64(granule.Ordinal))
		enc.i64(int64(granule.FirstRow))
		enc.i64(int64(granule.RowCount))
		enc.i64(int64(granule.VisibleRows))
		enc.i64(int64(granule.DeletedRows))
		enc.i64(granule.IDLower)
		enc.i64(granule.IDUpperExclusive)
		enc.i64(int64(granule.MarkOrdinal))
	}
	enc.u32(uint32(len(desc.Columns)))
	for _, column := range desc.Columns {
		partColumn, ok := b.part.Columns[column.Name]
		if !ok {
			return fmt.Errorf("typedcolumn: missing column %s", column.Name)
		}
		enc.str(column.Name)
		columnType, err := columnTypeCode(column.Type)
		if err != nil {
			return err
		}
		enc.u16(columnType)
		cardinality, err := imageColumnCardinalityForDescriptor(column, partColumn)
		if err != nil {
			return err
		}
		enc.u32(cardinality)
		if column.FixedWidthElements < 0 || uint64(column.FixedWidthElements) > uint64(^uint32(0)) {
			return fmt.Errorf("typedcolumn: descriptor column %s fixed-width elements=%d", column.Name, column.FixedWidthElements)
		}
		switch column.Type {
		case ColumnTypeFloat32Vector, ColumnTypeUint8Vector, ColumnTypeInt8Vector, ColumnTypeUint16Vector, ColumnTypeInt16Vector, ColumnTypeUint32Vector, ColumnTypeInt32Vector, ColumnTypeUint64Vector, ColumnTypeInt64Vector, ColumnTypeFloat16Vector, ColumnTypeBFloat16Vector, ColumnTypeFloat64Vector:
			if column.FixedWidthElements <= 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires positive fixed-width elements", column.Name, column.Type)
			}
			if column.BitsPerElement != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires bits_per_element=0", column.Name, column.Type)
			}
		case ColumnTypeFixedBytes:
			if column.FixedWidthElements <= 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires positive fixed-width elements/bytes_per_row", column.Name, column.Type)
			}
			if column.BitsPerElement != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires bits_per_element=0", column.Name, column.Type)
			}
			if !columnDescriptorAllBlocksEncoding(column, EncodingRawFixedBytes) {
				zeroRowFixedBytes := desc.RowCount == 0 && len(column.Blocks) == 0 && partColumn.Definition.Encoding == EncodingRawFixedBytes
				if !zeroRowFixedBytes {
					return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires encoding=%s", column.Name, column.Type, EncodingRawFixedBytes)
				}
			}
		case ColumnTypePackedBitVector, ColumnTypePackedUint2Vector, ColumnTypePackedUint4Vector:
			bitsPerElement, ok := PackedUintVectorBits(column.Type)
			if !ok || column.FixedWidthElements <= 0 || column.BitsPerElement != bitsPerElement {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires positive fixed-width elements and bits_per_element=%d", column.Name, column.Type, bitsPerElement)
			}
			wantEncoding, _ := PackedUintVectorEncoding(column.Type)
			if !columnDescriptorAllBlocksEncoding(column, wantEncoding) {
				zeroRowPacked := desc.RowCount == 0 && len(column.Blocks) == 0 && partColumn.Definition.Encoding == wantEncoding
				if !zeroRowPacked {
					return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires encoding=%s", column.Name, column.Type, wantEncoding)
				}
			}
		case ColumnTypeUint32List:
			if column.BitsPerElement != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires bits_per_element=0", column.Name, column.Type)
			}
			if column.FixedWidthElements != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires fixed-width elements=0", column.Name, column.Type)
			}
			if !columnDescriptorAllBlocksEncoding(column, EncodingRawUint32OffsetsList) {
				zeroRowOffsetsList := desc.RowCount == 0 && len(column.Blocks) == 0 && partColumn.Definition.Encoding == EncodingRawUint32OffsetsList
				if !zeroRowOffsetsList {
					return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires encoding=%s", column.Name, column.Type, EncodingRawUint32OffsetsList)
				}
			}
		case ColumnTypeBytes:
			if column.BitsPerElement != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires bits_per_element=0", column.Name, column.Type)
			}
			if column.FixedWidthElements != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires fixed-width elements=0", column.Name, column.Type)
			}
			if !columnDescriptorAllBlocksEncoding(column, EncodingRawBytesOffsets) {
				zeroRowBytes := desc.RowCount == 0 && len(column.Blocks) == 0 && partColumn.Definition.Encoding == EncodingRawBytesOffsets
				if !zeroRowBytes {
					return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires encoding=%s", column.Name, column.Type, EncodingRawBytesOffsets)
				}
			}
		case ColumnTypeAdjacencyList:
			if column.BitsPerElement != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires bits_per_element=0", column.Name, column.Type)
			}
			if column.FixedWidthElements < 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s has negative fixed-width elements=%d", column.Name, column.Type, column.FixedWidthElements)
			}
			if column.FixedWidthElements == 0 && !columnDescriptorAllBlocksEncoding(column, EncodingRawUint32OffsetsList) {
				zeroRowOffsetsList := desc.RowCount == 0 && len(column.Blocks) == 0 && partColumn.Definition.Encoding == EncodingRawUint32OffsetsList
				if !zeroRowOffsetsList {
					return fmt.Errorf("typedcolumn: descriptor column %s type=%s requires positive fixed-width elements unless encoding=%s", column.Name, column.Type, EncodingRawUint32OffsetsList)
				}
			}
		default:
			if column.BitsPerElement != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s has bits_per_element=%d", column.Name, column.Type, column.BitsPerElement)
			}
			if column.FixedWidthElements != 0 {
				return fmt.Errorf("typedcolumn: descriptor column %s type=%s has fixed-width elements=%d", column.Name, column.Type, column.FixedWidthElements)
			}
		}
		enc.u32(uint32(column.FixedWidthElements))
		if column.BitsPerElement < 0 || uint64(column.BitsPerElement) > uint64(^uint32(0)) {
			return fmt.Errorf("typedcolumn: descriptor column %s bits_per_element=%d", column.Name, column.BitsPerElement)
		}
		enc.u32(uint32(column.BitsPerElement))
		enc.u32(uint32(len(column.Blocks)))
		for i, block := range column.Blocks {
			if i >= len(partColumn.Blocks) {
				return fmt.Errorf("typedcolumn: descriptor column %s block %d missing payload block", column.Name, i)
			}
			granule := partColumn.Blocks[i].Granule
			enc.i64(int64(block.FirstRow))
			enc.i64(int64(block.RowCount))
			enc.i64(int64(block.FirstGranule))
			enc.i64(int64(block.LastGranule))
			enc.u16(uint16(block.Encoding))
			enc.u16(uint16(block.Compression))
			enc.i64(int64(block.RawBytes))
			enc.i64(int64(block.StoredBytes))
			enc.i64(int64(block.CodecBlockOrdinal))
			enc.i64(int64(granule.NullCount))
			enc.i64(int64(granule.DefaultCount))
			enc.boolean(granule.HasMinMax)
			enc.i64(granule.Min)
			enc.i64(granule.Max)
		}
	}
	b.appendSection(ColumnPartImageSection{
		Kind:     ColumnPartImageSectionDescriptor,
		Category: ColumnPartImageCategoryDescriptor,
		Name:     "part_descriptor",
		Rows:     desc.RowCount,
		Granules: len(desc.Granules),
		Blocks:   countColumnBlocks(desc),
	}, enc.bytes())
	return nil
}

func columnDescriptorAllBlocksEncoding(column ColumnPartColumnDescriptor, encoding Encoding) bool {
	if len(column.Blocks) == 0 {
		return false
	}
	for _, block := range column.Blocks {
		if block.Encoding != encoding {
			return false
		}
	}
	return true
}

func imageColumnCardinalityForDescriptor(column ColumnPartColumnDescriptor, partColumn ColumnPartColumn) (uint32, error) {
	cardinality := partColumn.Definition.Cardinality
	if column.Type != ColumnTypeLowCardinalityCode {
		return cardinality, nil
	}
	for i, block := range partColumn.Blocks {
		if !block.Granule.HasMinMax {
			continue
		}
		if block.Granule.Min < 0 || block.Granule.Max < 0 {
			return 0, fmt.Errorf("typedcolumn: descriptor column %s block %d has negative low-cardinality min/max", column.Name, i)
		}
		needed := uint64(block.Granule.Max) + 1
		if needed > maxCodeCardinality {
			return 0, fmt.Errorf("typedcolumn: descriptor column %s block %d inferred cardinality %d exceeds cap %d", column.Name, i, needed, maxCodeCardinality)
		}
		if uint32(needed) > cardinality {
			cardinality = uint32(needed)
		}
	}
	if cardinality == 0 && (len(partColumn.Blocks) == 0 || partColumn.Definition.Encoding != EncodingNullableInt64) {
		return 0, fmt.Errorf("typedcolumn: descriptor column %s has zero low-cardinality cardinality", column.Name)
	}
	return cardinality, nil
}

func (b *columnPartImageBuilder) addSortKeyMetadataSection() error {
	var enc columnPartImageEncoder
	enc.u32(uint32(len(b.part.Descriptor.SortKey)))
	for _, column := range b.part.Descriptor.SortKey {
		enc.str(column.Column)
		enc.str(string(column.Direction))
		enc.str(string(column.Nulls))
	}
	b.appendSection(ColumnPartImageSection{
		Kind:     ColumnPartImageSectionSortKeyMetadata,
		Category: ColumnPartImageCategorySortKeyMetadata,
		Name:     "sort_key",
	}, enc.bytes())
	return nil
}

func (b *columnPartImageBuilder) addSortKeyMarksSection() error {
	var enc columnPartImageEncoder
	enc.u32(uint32(len(b.part.Marks)))
	for _, mark := range b.part.Marks {
		enc.i64(int64(mark.Rows))
		enc.stringSlice(mark.Columns)
		enc.u32(uint32(len(mark.Prefixes)))
		for _, prefix := range mark.Prefixes {
			enc.stringSlice(prefix.Columns)
			encodeSortKeyBound(&enc, prefix.Lower)
			encodeSortKeyBound(&enc, prefix.UpperExclusive)
		}
	}
	b.appendSection(ColumnPartImageSection{
		Kind:     ColumnPartImageSectionSortKeyMarks,
		Category: ColumnPartImageCategoryMarks,
		Name:     "sort_key_marks",
		Rows:     b.part.Descriptor.RowCount,
		Granules: len(b.part.Marks),
	}, enc.bytes())
	return nil
}

func (b *columnPartImageBuilder) addRowLocatorsSection() error {
	if err := validateDecodedRowLocators(b.part.Descriptor, b.part.Descriptor.PartID, b.part.Locators); err != nil {
		return err
	}
	section := ColumnPartImageSection{
		Kind:     ColumnPartImageSectionRowLocators,
		Category: ColumnPartImageCategoryLocators,
		Name:     "primary_id_locators",
		Rows:     len(b.part.Locators),
	}
	if uint64(len(b.part.Locators)) > uint64(^uint32(0)) {
		return fmt.Errorf("typedcolumn: row locator count=%d exceeds uint32", len(b.part.Locators))
	}
	payloadBytes, err := rowLocatorSectionRawBytes(len(b.part.Locators))
	if err != nil {
		return err
	}
	if compact, ok, err := encodeContiguousRowLocatorSection(b.part.Descriptor, b.part.Locators, payloadBytes); err != nil {
		return err
	} else if ok {
		section.Encoding = EncodingRowLocatorContiguous
		return b.appendSectionWithOptionalCompression(section, compact, b.sectionCompression(section.Kind))
	}
	primaryIDs := make([]int64, 0, len(b.part.Locators))
	for primaryID := range b.part.Locators {
		primaryIDs = append(primaryIDs, primaryID)
	}
	sort.Slice(primaryIDs, func(i, j int) bool { return primaryIDs[i] < primaryIDs[j] })
	enc := columnPartImageEncoder{buf: make([]byte, 0, payloadBytes)}
	enc.u32(uint32(len(primaryIDs)))
	for _, primaryID := range primaryIDs {
		locator := b.part.Locators[primaryID]
		enc.i64(primaryID)
		enc.u64(locator.PartID)
		enc.u32(uint32(locator.PartRow))
		enc.u32(uint32(locator.GranuleOrdinal))
		enc.u32(uint32(locator.RowInGranule))
		enc.u32(0)
	}
	return b.appendSectionWithOptionalCompression(section, enc.bytes(), b.sectionCompression(section.Kind))
}

func encodeContiguousRowLocatorSection(desc ColumnPartDescriptor, locators map[int64]RowLocator, rawPayloadBytes int) ([]byte, bool, error) {
	if len(locators) != desc.RowCount || rawPayloadBytes <= rowLocatorContiguousPayloadBytes {
		return nil, false, nil
	}
	base, ok := contiguousRowLocatorBase(locators)
	if !ok {
		return nil, false, nil
	}
	if desc.RowCount > 0 && base > math.MaxInt64-int64(desc.RowCount-1) {
		return nil, false, fmt.Errorf("typedcolumn: contiguous row locator base=%d rows=%d exceeds int64 primary id range", base, desc.RowCount)
	}
	for primaryID, locator := range locators {
		expectedPrimaryID := base + int64(locator.PartRow)
		if primaryID != expectedPrimaryID || locator.PrimaryID != expectedPrimaryID {
			return nil, false, nil
		}
	}
	var enc columnPartImageEncoder
	enc.u32(rowLocatorContiguousMagic)
	enc.u16(rowLocatorContiguousVersion)
	enc.u16(0)
	enc.u64(desc.PartID)
	enc.u64(uint64(desc.RowCount))
	enc.i64(base)
	return enc.bytes(), true, nil
}

func contiguousRowLocatorBase(locators map[int64]RowLocator) (int64, bool) {
	if len(locators) == 0 {
		return 0, true
	}
	for primaryID, locator := range locators {
		if locator.PartRow == 0 {
			if locator.PrimaryID != primaryID {
				return 0, false
			}
			return primaryID, true
		}
	}
	return 0, false
}

func (b *columnPartImageBuilder) addAggregateMetadataSections() error {
	if len(b.part.AggregateMetadata) == 0 {
		return nil
	}
	names := make([]string, 0, len(b.part.AggregateMetadata))
	for name := range b.part.AggregateMetadata {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		metadata := b.part.AggregateMetadata[name]
		var enc columnPartImageEncoder
		if err := encodeAggregateMetadataDefinition(&enc, metadata.Definition); err != nil {
			return err
		}
		if err := encodeAggregateMetadataStats(&enc, metadata.Stats); err != nil {
			return err
		}
		enc.u32(uint32(len(metadata.Granules)))
		for _, granule := range metadata.Granules {
			enc.i64(int64(granule.GranuleOrdinal))
			enc.i64(int64(granule.FirstRow))
			enc.i64(int64(granule.RowCount))
			enc.i64(int64(granule.MatchedRows))
			enc.u32(uint32(len(granule.Entries)))
			for _, entry := range granule.Entries {
				enc.u32(entry.Group)
				enc.u32(entry.Count)
				enc.i64(entry.Min)
				enc.i64(entry.Max)
			}
		}
		b.appendSection(ColumnPartImageSection{
			Kind:     ColumnPartImageSectionAggregateMetadata,
			Category: ColumnPartImageCategoryAggregateMetadata,
			Name:     name,
			Rows:     metadata.Stats.RowsMatched,
			Granules: metadata.Stats.Granules,
			Blocks:   metadata.Stats.Entries,
		}, enc.bytes())
	}
	return nil
}

func (b *columnPartImageBuilder) addColumnStatsSection() error {
	data, err := encodeColumnPartStatsSection(b.part.ColumnStats)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	b.appendSection(ColumnPartImageSection{
		Kind:     ColumnPartImageSectionColumnStats,
		Category: ColumnPartImageCategoryColumnStats,
		Name:     "column_stats",
		Rows:     b.part.Descriptor.RowCount,
		Granules: len(b.part.Descriptor.Granules),
		Blocks:   countColumnBlocks(b.part.Descriptor),
	}, data)
	return nil
}

func (b *columnPartImageBuilder) addPruningMetadataSection() error {
	data, err := encodeColumnPartPruningSection(b.part.PruningMetadata)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	section := ColumnPartImageSection{
		Kind:     ColumnPartImageSectionPruningMetadata,
		Category: ColumnPartImageCategoryPruningMetadata,
		Name:     "column_pruning",
		Rows:     b.part.Descriptor.RowCount,
		Granules: len(b.part.Descriptor.Granules),
		Blocks:   countColumnBlocks(b.part.Descriptor),
	}
	return b.appendSectionWithOptionalCompression(section, data, b.sectionCompression(section.Kind))
}

func (b *columnPartImageBuilder) addDictionarySection() error {
	if len(b.opts.Dictionaries) == 0 {
		return nil
	}
	rawData, compactData, compactOK := encodeDictionarySectionPayloads(b.opts.Dictionaries)
	section := ColumnPartImageSection{
		Kind:     ColumnPartImageSectionDictionaries,
		Category: ColumnPartImageCategoryDictionaries,
		Name:     "part_dictionaries",
	}
	compression := b.sectionCompression(section.Kind)
	rawSection, err := selectImageSectionPayload(section, rawData, compression)
	if err != nil {
		return err
	}
	selected := rawSection
	if compactOK {
		compactSection := section
		compactSection.Encoding = EncodingDictionaryDense
		compactSelection, err := selectImageSectionPayload(compactSection, compactData, compression)
		if err != nil {
			return err
		}
		if len(compactSelection.data) < len(selected.data) {
			selected = compactSelection
		}
	}
	b.sections = append(b.sections, selected)
	return nil
}

func encodeDictionarySectionPayloads(dictionaries map[string]map[string]int64) ([]byte, []byte, bool) {
	var rawEnc columnPartImageEncoder
	var compactEnc columnPartImageEncoder
	names := make([]string, 0, len(dictionaries))
	for name := range dictionaries {
		names = append(names, name)
	}
	sort.Strings(names)
	rawEnc.u32(uint32(len(names)))
	compactEnc.u32(dictionaryDenseMagic)
	compactEnc.u16(dictionaryDenseVersion)
	compactEnc.u16(0)
	compactEnc.u32(uint32(len(names)))
	compactOK := true
	for _, name := range names {
		values := dictionaries[name]
		entries := make([]dictionaryImageEntry, 0, len(values))
		for value, code := range values {
			entries = append(entries, dictionaryImageEntry{Value: value, Code: code})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Code != entries[j].Code {
				return entries[i].Code < entries[j].Code
			}
			return entries[i].Value < entries[j].Value
		})
		rawEnc.str(name)
		rawEnc.u32(uint32(len(entries)))
		compactEnc.str(name)
		compactEnc.u32(uint32(len(entries)))
		for idx, entry := range entries {
			rawEnc.i64(entry.Code)
			rawEnc.str(entry.Value)
			if entry.Code != int64(idx) {
				compactOK = false
			}
			compactEnc.str(entry.Value)
		}
	}
	return rawEnc.bytes(), compactEnc.bytes(), compactOK && len(compactEnc.bytes()) < len(rawEnc.bytes())
}

func (b *columnPartImageBuilder) addColumnDataSections() error {
	for _, columnDescriptor := range b.part.Descriptor.Columns {
		column, ok := b.part.Columns[columnDescriptor.Name]
		if !ok {
			return fmt.Errorf("typedcolumn: missing column %s", columnDescriptor.Name)
		}
		if column.Definition.Encoding == EncodingRawUint32OffsetsList {
			if err := b.addUint32OffsetsListColumnSections(columnDescriptor, column); err != nil {
				return err
			}
			continue
		}
		if column.Definition.Encoding == EncodingRawBytesOffsets {
			if err := b.addBytesColumnSections(columnDescriptor, column); err != nil {
				return err
			}
			continue
		}
		totalPayloadBytes := 0
		payloadChunks := make([][]byte, 0, len(column.Blocks))
		for i, block := range column.Blocks {
			if len(block.Granule.Payload) != block.Descriptor.StoredBytes {
				return fmt.Errorf("typedcolumn: column %s block %d payload bytes=%d descriptor stored bytes=%d", columnDescriptor.Name, i, len(block.Granule.Payload), block.Descriptor.StoredBytes)
			}
			totalPayloadBytes += block.Descriptor.StoredBytes
			payloadChunks = append(payloadChunks, block.Granule.Payload)
		}
		b.appendChunkedSection(ColumnPartImageSection{
			Kind:        ColumnPartImageSectionColumnData,
			Category:    ColumnPartImageCategoryDeclaredColumns,
			Column:      columnDescriptor.Name,
			Rows:        b.part.Descriptor.RowCount,
			Granules:    len(b.part.Descriptor.Granules),
			Blocks:      len(column.Blocks),
			Encoding:    column.Definition.Encoding,
			Compression: column.Definition.Compression,
		}, payloadChunks, totalPayloadBytes)
	}
	return nil
}

func (b *columnPartImageBuilder) addUint32OffsetsListColumnSections(columnDescriptor ColumnPartColumnDescriptor, column ColumnPartColumn) error {
	globalOffsets := resizeFixedWidthValues([]uint64(nil), b.part.Descriptor.RowCount+1)
	globalValues := make([]uint32, 0)
	expectedFirstRow := 0
	var reader GranuleReader
	var offsetsScratch []uint64
	var valuesScratch []uint32
	for i, block := range column.Blocks {
		if len(block.Granule.Payload) != block.Descriptor.StoredBytes {
			return fmt.Errorf("typedcolumn: column %s block %d payload bytes=%d descriptor stored bytes=%d", columnDescriptor.Name, i, len(block.Granule.Payload), block.Descriptor.StoredBytes)
		}
		if block.Descriptor.FirstRow != expectedFirstRow {
			return fmt.Errorf("typedcolumn: column %s block %d first_row=%d want %d", columnDescriptor.Name, i, block.Descriptor.FirstRow, expectedFirstRow)
		}
		decoded, err := reader.DecodeUint32OffsetsListInto(offsetsScratch[:0], valuesScratch[:0], block.Granule)
		if err != nil {
			return fmt.Errorf("typedcolumn: column %s block %d offsets-list payload: %w", columnDescriptor.Name, i, err)
		}
		if decoded.Rows != block.Descriptor.RowCount || len(decoded.Offsets) != block.Descriptor.RowCount+1 {
			return fmt.Errorf("typedcolumn: column %s block %d offsets-list rows=%d offsets=%d want rows=%d", columnDescriptor.Name, i, decoded.Rows, len(decoded.Offsets), block.Descriptor.RowCount)
		}
		base := uint64(len(globalValues))
		if base > maxHostIntUint64() || uint64(len(decoded.Values)) > maxHostIntUint64()-base {
			return fmt.Errorf("typedcolumn: column %s offsets-list values exceed host int", columnDescriptor.Name)
		}
		first := block.Descriptor.FirstRow
		for row := 0; row <= decoded.Rows; row++ {
			globalOffsets[first+row] = base + decoded.Offsets[row]
		}
		globalValues = append(globalValues, decoded.Values...)
		offsetsScratch = decoded.Offsets
		valuesScratch = decoded.Values
		expectedFirstRow += block.Descriptor.RowCount
	}
	if expectedFirstRow != b.part.Descriptor.RowCount {
		return fmt.Errorf("typedcolumn: column %s offsets-list covers %d rows, want %d", columnDescriptor.Name, expectedFirstRow, b.part.Descriptor.RowCount)
	}
	if err := ValidateRawUint32OffsetsListShape(b.part.Descriptor.RowCount, globalOffsets, uint64(len(globalValues))); err != nil {
		return fmt.Errorf("typedcolumn: column %s offsets-list global shape: %w", columnDescriptor.Name, err)
	}
	offsetsData, err := EncodeRawUint32OffsetsListOffsets(nil, globalOffsets)
	if err != nil {
		return err
	}
	valuesData, err := EncodeRawUint32OffsetsListValues(nil, globalValues)
	if err != nil {
		return err
	}
	offsetsSection, valuesSection, err := NewRawUint32OffsetsListImageSections(columnDescriptor.Name, b.part.Descriptor.RowCount, len(offsetsData), len(valuesData))
	if err != nil {
		return err
	}
	offsetsSection.Granules = len(b.part.Descriptor.Granules)
	offsetsSection.Blocks = len(column.Blocks)
	valuesSection.Granules = len(b.part.Descriptor.Granules)
	valuesSection.Blocks = len(column.Blocks)
	b.appendSection(offsetsSection, offsetsData)
	b.appendSection(valuesSection, valuesData)
	return nil
}

func (b *columnPartImageBuilder) addBytesColumnSections(columnDescriptor ColumnPartColumnDescriptor, column ColumnPartColumn) error {
	globalOffsets := resizeFixedWidthValues([]uint64(nil), b.part.Descriptor.RowCount+1)
	globalValues := make([]byte, 0)
	expectedFirstRow := 0
	var reader GranuleReader
	var offsetsScratch []uint64
	var valuesScratch []byte
	for i, block := range column.Blocks {
		if len(block.Granule.Payload) != block.Descriptor.StoredBytes {
			return fmt.Errorf("typedcolumn: column %s block %d payload bytes=%d descriptor stored bytes=%d", columnDescriptor.Name, i, len(block.Granule.Payload), block.Descriptor.StoredBytes)
		}
		if block.Descriptor.FirstRow != expectedFirstRow {
			return fmt.Errorf("typedcolumn: column %s block %d first_row=%d want %d", columnDescriptor.Name, i, block.Descriptor.FirstRow, expectedFirstRow)
		}
		decoded, err := reader.DecodeBytesInto(offsetsScratch[:0], valuesScratch[:0], block.Granule)
		if err != nil {
			return fmt.Errorf("typedcolumn: column %s block %d bytes payload: %w", columnDescriptor.Name, i, err)
		}
		if decoded.Rows != block.Descriptor.RowCount || len(decoded.Offsets) != block.Descriptor.RowCount+1 {
			return fmt.Errorf("typedcolumn: column %s block %d bytes rows=%d offsets=%d want rows=%d", columnDescriptor.Name, i, decoded.Rows, len(decoded.Offsets), block.Descriptor.RowCount)
		}
		base := uint64(len(globalValues))
		if base > maxHostIntUint64() || uint64(len(decoded.Values)) > maxHostIntUint64()-base {
			return fmt.Errorf("typedcolumn: column %s bytes values exceed host int", columnDescriptor.Name)
		}
		first := block.Descriptor.FirstRow
		for row := 0; row <= decoded.Rows; row++ {
			globalOffsets[first+row] = base + decoded.Offsets[row]
		}
		globalValues = append(globalValues, decoded.Values...)
		offsetsScratch = decoded.Offsets
		valuesScratch = decoded.Values
		expectedFirstRow += block.Descriptor.RowCount
	}
	if expectedFirstRow != b.part.Descriptor.RowCount {
		return fmt.Errorf("typedcolumn: column %s bytes covers %d rows, want %d", columnDescriptor.Name, expectedFirstRow, b.part.Descriptor.RowCount)
	}
	if err := ValidateRawBytesOffsetsShape(b.part.Descriptor.RowCount, globalOffsets, uint64(len(globalValues))); err != nil {
		return fmt.Errorf("typedcolumn: column %s bytes global shape: %w", columnDescriptor.Name, err)
	}
	offsetsData, err := EncodeRawBytesOffsetsOffsets(nil, globalOffsets)
	if err != nil {
		return err
	}
	valuesData, err := EncodeRawBytesOffsetsValues(nil, globalValues)
	if err != nil {
		return err
	}
	offsetsSection, valuesSection, err := NewRawBytesOffsetsImageSections(columnDescriptor.Name, b.part.Descriptor.RowCount, len(offsetsData), len(valuesData))
	if err != nil {
		return err
	}
	offsetsSection.Granules = len(b.part.Descriptor.Granules)
	offsetsSection.Blocks = len(column.Blocks)
	valuesSection.Granules = len(b.part.Descriptor.Granules)
	valuesSection.Blocks = len(column.Blocks)
	b.appendSection(offsetsSection, offsetsData)
	b.appendSection(valuesSection, valuesData)
	return nil
}

func (b *columnPartImageBuilder) addLayoutContractSection() error {
	data, err := encodeColumnPartLayoutContract(b.part, b.opts, b.sections, 0)
	if err != nil {
		return err
	}
	section := ColumnPartImageSection{
		Kind:     ColumnPartImageSectionLayoutContract,
		Category: ColumnPartImageCategoryLayoutContract,
		Name:     "writer_layout_contract",
		Rows:     b.part.Descriptor.RowCount,
		Granules: len(b.part.Descriptor.Granules),
		Blocks:   countColumnBlocks(b.part.Descriptor),
	}
	insert := len(b.sections)
	for i, existing := range b.sections {
		if existing.section.Kind == ColumnPartImageSectionColumnData {
			insert = i
			break
		}
	}
	entry := columnPartImageSectionData{section: section, data: data}
	b.sections = append(b.sections, columnPartImageSectionData{})
	copy(b.sections[insert+1:], b.sections[insert:])
	b.sections[insert] = entry
	return nil
}

func (b *columnPartImageBuilder) refreshLayoutContractSection(sections []ColumnPartImageSection, manifestBytes int) error {
	idx := -1
	for i := range b.sections {
		if b.sections[i].section.Kind == ColumnPartImageSectionLayoutContract {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("typedcolumn: missing layout contract section")
	}
	data, err := encodeColumnPartLayoutContract(b.part, b.opts, b.sections, manifestBytes)
	if err != nil {
		return err
	}
	if len(data) != len(b.sections[idx].data) {
		return fmt.Errorf("typedcolumn: layout contract bytes changed from %d to %d during layout", len(b.sections[idx].data), len(data))
	}
	b.sections[idx].data = data
	b.sections[idx].section.Length = len(data)
	sections[idx] = b.sections[idx].section
	return nil
}

func (b *columnPartImageBuilder) appendSection(section ColumnPartImageSection, data []byte) {
	section.Length = len(data)
	if section.RawBytes == 0 && (section.Compression == CompressionNone || section.Kind != ColumnPartImageSectionColumnData) {
		section.RawBytes = len(data)
	}
	b.sections = append(b.sections, columnPartImageSectionData{
		section: section,
		data:    data,
	})
}

func (b *columnPartImageBuilder) appendChunkedSection(section ColumnPartImageSection, chunks [][]byte, payloadBytes int) {
	section.Length = payloadBytes
	if section.RawBytes == 0 && (section.Compression == CompressionNone || section.Kind != ColumnPartImageSectionColumnData) {
		section.RawBytes = payloadBytes
	}
	b.sections = append(b.sections, columnPartImageSectionData{
		section: section,
		chunks:  chunks,
	})
}

func (b *columnPartImageBuilder) appendSectionWithOptionalCompression(section ColumnPartImageSection, data []byte, compression Compression) error {
	selected, err := selectImageSectionPayload(section, data, compression)
	if err != nil {
		return err
	}
	b.sections = append(b.sections, selected)
	return nil
}

func (b *columnPartImageBuilder) sectionCompression(kind ColumnPartImageSectionKind) Compression {
	switch kind {
	case ColumnPartImageSectionRowLocators:
		if b.opts.RowLocatorSectionCompressionSet {
			return b.opts.RowLocatorSectionCompression
		}
	case ColumnPartImageSectionDictionaries:
		if b.opts.DictionarySectionCompressionSet {
			return b.opts.DictionarySectionCompression
		}
	case ColumnPartImageSectionPruningMetadata:
		if b.opts.PruningMetadataSectionCompressionSet {
			return b.opts.PruningMetadataSectionCompression
		}
	}
	return b.opts.SectionCompression
}

func selectImageSectionPayload(section ColumnPartImageSection, data []byte, compression Compression) (columnPartImageSectionData, error) {
	if !canCompressImageSection(section, len(data), compression) {
		section.Length = len(data)
		if section.RawBytes == 0 && (section.Compression == CompressionNone || section.Kind != ColumnPartImageSectionColumnData) {
			section.RawBytes = len(data)
		}
		return columnPartImageSectionData{section: section, data: data}, nil
	}
	selection, err := admitCompressionInto(nil, data, 0, compression)
	if err != nil {
		return columnPartImageSectionData{}, fmt.Errorf("typedcolumn: compress section %s/%s: %w", section.Kind, section.Name, err)
	}
	section.Compression = selection.Actual
	section.RawBytes = len(data)
	section.Length = len(selection.Payload)
	return columnPartImageSectionData{section: section, data: selection.Payload}, nil
}

func canCompressImageSection(section ColumnPartImageSection, rawBytes int, compression Compression) bool {
	switch compression {
	case CompressionSnappy, CompressionLZ4, CompressionZSTD:
		if rawBytes > maxCompressedImageSectionRawBytes {
			return false
		}
		switch section.Kind {
		case ColumnPartImageSectionRowLocators, ColumnPartImageSectionDictionaries, ColumnPartImageSectionPruningMetadata:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

type dictionaryImageEntry struct {
	Value string
	Code  int64
}

type columnPartImageEncoder struct {
	buf []byte
}

func (e *columnPartImageEncoder) bytes() []byte {
	return e.buf
}

func (e *columnPartImageEncoder) u16(v uint16) {
	e.buf = binary.LittleEndian.AppendUint16(e.buf, v)
}

func (e *columnPartImageEncoder) u32(v uint32) {
	e.buf = binary.LittleEndian.AppendUint32(e.buf, v)
}

func (e *columnPartImageEncoder) u64(v uint64) {
	e.buf = binary.LittleEndian.AppendUint64(e.buf, v)
}

func (e *columnPartImageEncoder) i64(v int64) {
	e.u64(uint64(v))
}

func (e *columnPartImageEncoder) boolean(v bool) {
	if v {
		e.u16(1)
		return
	}
	e.u16(0)
}

func (e *columnPartImageEncoder) str(v string) {
	e.u32(uint32(len(v)))
	e.buf = append(e.buf, v...)
}

func (e *columnPartImageEncoder) stringSlice(values []string) {
	e.u32(uint32(len(values)))
	for _, value := range values {
		e.str(value)
	}
}

func encodeColumnPartImageManifest(part *ColumnPart, sections []ColumnPartImageSection, manifestBytes int) ([]byte, error) {
	var enc columnPartImageEncoder
	enc.u32(columnPartImageMagic)
	enc.u16(columnPartImageVersion)
	enc.u16(0)
	enc.u64(part.Descriptor.PartID)
	enc.i64(int64(part.Descriptor.RowCount))
	enc.u32(uint32(manifestBytes))
	enc.u32(uint32(len(sections)))
	for _, section := range sections {
		kindCode, err := columnPartImageSectionKindCode(section.Kind)
		if err != nil {
			return nil, err
		}
		categoryCode, err := columnPartImageSectionCategoryCode(section.Category)
		if err != nil {
			return nil, err
		}
		enc.u16(kindCode)
		enc.u16(categoryCode)
		enc.u64(uint64(section.Offset))
		enc.u64(uint64(section.Length))
		enc.i64(int64(section.Rows))
		enc.i64(int64(section.Granules))
		enc.i64(int64(section.Blocks))
		enc.u16(uint16(section.Encoding))
		enc.u16(uint16(section.Compression))
		enc.u64(uint64(section.RawBytes))
		enc.str(section.Name)
		enc.str(section.Column)
	}
	return enc.bytes(), nil
}

func encodeSortKeyBound(enc *columnPartImageEncoder, bound SortKeyBound) {
	enc.boolean(bound.Exclusive)
	enc.boolean(bound.Unbounded)
	enc.u32(uint32(len(bound.Values)))
	for _, value := range bound.Values {
		enc.i64(value)
	}
}

func encodeAggregateMetadataDefinition(enc *columnPartImageEncoder, def AggregateMetadataDefinition) error {
	enc.str(def.Name)
	enc.u16(def.Version)
	enc.str(string(def.Kind))
	enc.str(string(def.Scope))
	enc.stringSlice(def.GroupKeys)
	enc.u32(uint32(len(def.Measures)))
	for _, measure := range def.Measures {
		enc.str(string(measure.Op))
		enc.str(measure.Column)
	}
	enc.u32(uint32(len(def.Predicates)))
	for _, predicate := range def.Predicates {
		enc.str(predicate.Column)
		enc.str(string(predicate.Op))
		enc.i64(predicate.Value)
	}
	if err := encodeNonNegativeScaledFloat(enc, fmt.Sprintf("aggregate metadata %s max bytes per row", def.Name), def.MaxBytesPerRow); err != nil {
		return err
	}
	return nil
}

func encodeAggregateMetadataStats(enc *columnPartImageEncoder, stats AggregateMetadataStats) error {
	enc.boolean(stats.Admitted)
	enc.str(stats.RejectedReason)
	enc.i64(durationNanos(stats.BuildDuration))
	enc.i64(int64(stats.Granules))
	enc.i64(int64(stats.GranulesWithRows))
	enc.i64(int64(stats.RowsMatched))
	enc.i64(int64(stats.Entries))
	enc.i64(int64(stats.ValueBytes))
	enc.i64(int64(stats.DescriptorBytes))
	enc.i64(int64(stats.TotalBytes))
	if err := encodeNonNegativeScaledFloat(enc, "aggregate metadata bytes per part row", stats.BytesPerPartRow); err != nil {
		return err
	}
	if err := encodeNonNegativeScaledFloat(enc, "aggregate metadata bytes per matched row", stats.BytesPerMatchedRow); err != nil {
		return err
	}
	enc.str(stats.Compression)
	if err := encodeNonNegativeScaledFloat(enc, "aggregate metadata admission max bytes", stats.AdmissionMaxBytes); err != nil {
		return err
	}
	enc.str(stats.AdmissionMeasuredBy)
	return nil
}

func encodeNonNegativeScaledFloat(enc *columnPartImageEncoder, field string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Errorf("typedcolumn: %s must be finite, got %v", field, value)
	}
	if value < 0 {
		return fmt.Errorf("typedcolumn: %s %.6f is negative", field, value)
	}
	scaled := math.Round(value * 1_000_000)
	if scaled > math.MaxInt64 {
		return fmt.Errorf("typedcolumn: %s %.6f exceeds scaled int64", field, value)
	}
	enc.i64(int64(scaled))
	return nil
}

func countColumnBlocks(desc ColumnPartDescriptor) int {
	total := 0
	for _, column := range desc.Columns {
		total += len(column.Blocks)
	}
	return total
}

func (s columnPartImageSectionData) payloadLen() int {
	if len(s.chunks) == 0 {
		return len(s.data)
	}
	total := 0
	for _, chunk := range s.chunks {
		total += len(chunk)
	}
	return total
}

func (s columnPartImageSectionData) appendPayloadTo(dst []byte) []byte {
	if len(s.chunks) == 0 {
		return append(dst, s.data...)
	}
	for _, chunk := range s.chunks {
		dst = append(dst, chunk...)
	}
	return dst
}

func alignColumnPartImageOffset(offset int) int {
	return alignColumnPartImageOffsetTo(offset, columnPartImageSectionAlignment)
}

func alignColumnPartImageOffsetTo(offset, alignment int) int {
	mask := alignment - 1
	return (offset + mask) &^ mask
}

func columnPartImageAlignment(requested int) (int, error) {
	switch requested {
	case 0, columnPartImageSectionAlignment:
		return columnPartImageSectionAlignment, nil
	case 64:
		return 64, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported section alignment %d", requested)
	}
}

func durationNanos(d time.Duration) int64 {
	return int64(d)
}

func columnTypeCode(t ColumnType) (uint16, error) {
	switch t {
	case ColumnTypeInt64:
		return 1, nil
	case ColumnTypeLowCardinalityCode:
		return 2, nil
	case ColumnTypeBool:
		return 3, nil
	case ColumnTypeFloat32Vector:
		return 4, nil
	case ColumnTypeAdjacencyList:
		return 5, nil
	case ColumnTypeFloat32:
		return 6, nil
	case ColumnTypeFloat64:
		return 7, nil
	case ColumnTypeUint32List:
		return 8, nil
	case ColumnTypeBytes:
		return 9, nil
	case ColumnTypeInt8:
		return 10, nil
	case ColumnTypeUint8:
		return 11, nil
	case ColumnTypeInt16:
		return 12, nil
	case ColumnTypeUint16:
		return 13, nil
	case ColumnTypeInt32:
		return 14, nil
	case ColumnTypeUint32:
		return 15, nil
	case ColumnTypeUint64:
		return 16, nil
	case ColumnTypeFloat16:
		return 17, nil
	case ColumnTypeBFloat16:
		return 18, nil
	case ColumnTypeUint8Vector:
		return 19, nil
	case ColumnTypeInt8Vector:
		return 20, nil
	case ColumnTypeUint16Vector:
		return 21, nil
	case ColumnTypeInt16Vector:
		return 22, nil
	case ColumnTypeUint32Vector:
		return 23, nil
	case ColumnTypeInt32Vector:
		return 24, nil
	case ColumnTypeUint64Vector:
		return 25, nil
	case ColumnTypeInt64Vector:
		return 26, nil
	case ColumnTypeFloat16Vector:
		return 27, nil
	case ColumnTypeBFloat16Vector:
		return 28, nil
	case ColumnTypeFloat64Vector:
		return 29, nil
	case ColumnTypeFixedBytes:
		return 30, nil
	case ColumnTypePackedBitVector:
		return 31, nil
	case ColumnTypePackedUint2Vector:
		return 32, nil
	case ColumnTypePackedUint4Vector:
		return 33, nil
	default:
		return 0, fmt.Errorf("typedcolumn: unsupported column type %s", t)
	}
}

type columnPartImageSectionCode struct {
	kind         ColumnPartImageSectionKind
	kindCode     uint16
	category     ColumnPartImageSectionCategory
	categoryCode uint16
}

var columnPartImageSectionCodes = []columnPartImageSectionCode{
	{kind: ColumnPartImageSectionDescriptor, kindCode: 1, category: ColumnPartImageCategoryDescriptor, categoryCode: 1},
	{kind: ColumnPartImageSectionSortKeyMetadata, kindCode: 2, category: ColumnPartImageCategorySortKeyMetadata, categoryCode: 2},
	{kind: ColumnPartImageSectionSortKeyMarks, kindCode: 3, category: ColumnPartImageCategoryMarks, categoryCode: 3},
	{kind: ColumnPartImageSectionRowLocators, kindCode: 4, category: ColumnPartImageCategoryLocators, categoryCode: 4},
	{kind: ColumnPartImageSectionAggregateMetadata, kindCode: 5, category: ColumnPartImageCategoryAggregateMetadata, categoryCode: 5},
	{kind: ColumnPartImageSectionDictionaries, kindCode: 6, category: ColumnPartImageCategoryDictionaries, categoryCode: 6},
	{kind: ColumnPartImageSectionColumnData, kindCode: 7, category: ColumnPartImageCategoryDeclaredColumns, categoryCode: 7},
	{kind: ColumnPartImageSectionManifest, kindCode: 8, category: ColumnPartImageCategoryManifest, categoryCode: 8},
	{kind: ColumnPartImageSectionLayoutContract, kindCode: 9, category: ColumnPartImageCategoryLayoutContract, categoryCode: 9},
	{kind: ColumnPartImageSectionColumnStats, kindCode: 10, category: ColumnPartImageCategoryColumnStats, categoryCode: 10},
	{kind: ColumnPartImageSectionPruningMetadata, kindCode: 11, category: ColumnPartImageCategoryPruningMetadata, categoryCode: 11},
	{kind: ColumnPartImageSectionColumnOffsets, kindCode: 12, category: ColumnPartImageCategoryDeclaredColumnOffsets, categoryCode: 12},
	{kind: ColumnPartImageSectionColumnValues, kindCode: 13, category: ColumnPartImageCategoryDeclaredColumnValues, categoryCode: 13},
}

func columnPartImageSectionKindCode(kind ColumnPartImageSectionKind) (uint16, error) {
	for _, code := range columnPartImageSectionCodes {
		if code.kind == kind {
			return code.kindCode, nil
		}
	}
	return 0, fmt.Errorf("typedcolumn: unknown image section kind %s", kind)
}

func columnPartImageSectionKindFromCode(code uint16) (ColumnPartImageSectionKind, error) {
	for _, entry := range columnPartImageSectionCodes {
		if entry.kindCode == code {
			return entry.kind, nil
		}
	}
	return "", fmt.Errorf("typedcolumn: unknown image section kind code %d", code)
}

func columnPartImageSectionCategoryCode(category ColumnPartImageSectionCategory) (uint16, error) {
	for _, code := range columnPartImageSectionCodes {
		if code.category == category {
			return code.categoryCode, nil
		}
	}
	return 0, fmt.Errorf("typedcolumn: unknown image section category %s", category)
}

func columnPartImageSectionCategoryFromCode(code uint16) (ColumnPartImageSectionCategory, error) {
	for _, entry := range columnPartImageSectionCodes {
		if entry.categoryCode == code {
			return entry.category, nil
		}
	}
	return "", fmt.Errorf("typedcolumn: unknown image section category code %d", code)
}
