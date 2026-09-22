package colgranule

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"
)

const columnPartImageVersion uint16 = 1

const columnPartImageMagic uint32 = 0x4d494354 // "TCIM", little-endian on disk.

type ColumnPartImageSectionKind string

const (
	ColumnPartImageSectionManifest          ColumnPartImageSectionKind = "manifest"
	ColumnPartImageSectionDescriptor        ColumnPartImageSectionKind = "descriptor"
	ColumnPartImageSectionSortKeyMetadata   ColumnPartImageSectionKind = "sort_key_metadata"
	ColumnPartImageSectionSortKeyMarks      ColumnPartImageSectionKind = "sort_key_marks"
	ColumnPartImageSectionRowLocators       ColumnPartImageSectionKind = "row_locators"
	ColumnPartImageSectionAggregateMetadata ColumnPartImageSectionKind = "aggregate_metadata"
	ColumnPartImageSectionDictionaries      ColumnPartImageSectionKind = "dictionaries"
	ColumnPartImageSectionColumnData        ColumnPartImageSectionKind = "column_data"
)

type ColumnPartImageSectionCategory string

const (
	ColumnPartImageCategoryManifest          ColumnPartImageSectionCategory = "manifest"
	ColumnPartImageCategoryDescriptor        ColumnPartImageSectionCategory = "descriptor"
	ColumnPartImageCategorySortKeyMetadata   ColumnPartImageSectionCategory = "sort_key_metadata"
	ColumnPartImageCategoryMarks             ColumnPartImageSectionCategory = "marks"
	ColumnPartImageCategoryLocators          ColumnPartImageSectionCategory = "locators"
	ColumnPartImageCategoryAggregateMetadata ColumnPartImageSectionCategory = "aggregate_metadata"
	ColumnPartImageCategoryDictionaries      ColumnPartImageSectionCategory = "dictionaries"
	ColumnPartImageCategoryDeclaredColumns   ColumnPartImageSectionCategory = "declared_columns"
)

type ColumnPartImageOptions struct {
	Dictionaries map[string]map[string]int64
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
}

type ColumnPartImageSectionByteAccounting struct {
	Kind     ColumnPartImageSectionKind     `json:"kind"`
	Category ColumnPartImageSectionCategory `json:"category"`
	Name     string                         `json:"name,omitempty"`
	Column   string                         `json:"column,omitempty"`
	Bytes    int                            `json:"bytes"`
}

func BuildColumnPartImage(part *ColumnPart, opts ColumnPartImageOptions) (ColumnPartImage, error) {
	if part == nil {
		return ColumnPartImage{}, fmt.Errorf("colgranule: nil part")
	}
	builder := columnPartImageBuilder{part: part, opts: opts}
	return builder.build()
}

func (p *ColumnPart) WithImagePayloads(image ColumnPartImage) (*ColumnPart, error) {
	if p == nil {
		return nil, fmt.Errorf("colgranule: nil part")
	}
	if image.TotalBytes() == 0 {
		return nil, fmt.Errorf("colgranule: empty part image")
	}
	if err := image.validateForRead(); err != nil {
		return nil, err
	}
	if image.PartID != p.Descriptor.PartID {
		return nil, fmt.Errorf("colgranule: image part id=%d does not match part id=%d", image.PartID, p.Descriptor.PartID)
	}
	if image.Rows != p.Descriptor.RowCount {
		return nil, fmt.Errorf("colgranule: image rows=%d does not match part rows=%d", image.Rows, p.Descriptor.RowCount)
	}
	if err := validateImageDescriptorMatchesPart(image, p); err != nil {
		return nil, err
	}
	out := *p
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
	if category == ColumnPartImageCategoryManifest {
		return i.ManifestBytes
	}
	total := 0
	for _, section := range i.Sections {
		if section.Category == category {
			total += section.Length
		}
	}
	return total
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
	for _, section := range i.Sections {
		out = append(out, ColumnPartImageSectionByteAccounting{
			Kind:     section.Kind,
			Category: section.Category,
			Name:     section.Name,
			Column:   section.Column,
			Bytes:    section.Length,
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
		return fmt.Errorf("colgranule: image descriptor does not match part descriptor")
	}
	sortKey, err := decodeSortKeyMetadataSection(image)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(sortKey, part.Descriptor.SortKey) {
		return fmt.Errorf("colgranule: image sort key does not match part sort key")
	}
	if len(imageColumns) != len(part.Columns) {
		return fmt.Errorf("colgranule: image has %d columns, part has %d", len(imageColumns), len(part.Columns))
	}
	for name, imageColumn := range imageColumns {
		partColumn, ok := part.Columns[name]
		if !ok {
			return fmt.Errorf("colgranule: image descriptor has unknown column %s", name)
		}
		partDefinition, err := comparablePartColumnDefinitionForImage(imageColumn.Definition, partColumn)
		if err != nil {
			return err
		}
		if comparableColumnDefinition(imageColumn.Definition) != partDefinition {
			return fmt.Errorf("colgranule: image descriptor column %s definition does not match part", name)
		}
		if len(imageColumn.Blocks) != len(partColumn.Blocks) {
			return fmt.Errorf("colgranule: image descriptor column %s blocks=%d part blocks=%d", name, len(imageColumn.Blocks), len(partColumn.Blocks))
		}
		for i := range imageColumn.Blocks {
			if imageColumn.Blocks[i].Descriptor != partColumn.Blocks[i].Descriptor {
				return fmt.Errorf("colgranule: image descriptor column %s block %d descriptor does not match part", name, i)
			}
			if comparableGranuleMetadata(imageColumn.Blocks[i].Granule) != comparableGranuleMetadata(partColumn.Blocks[i].Granule) {
				return fmt.Errorf("colgranule: image descriptor column %s block %d granule metadata does not match part", name, i)
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
	part     *ColumnPart
	opts     ColumnPartImageOptions
	sections []columnPartImageSectionData
}

type columnPartImageSectionData struct {
	section ColumnPartImageSection
	data    []byte
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
	if err := b.addDictionarySection(); err != nil {
		return ColumnPartImage{}, err
	}
	if err := b.addColumnDataSections(); err != nil {
		return ColumnPartImage{}, err
	}

	sections, manifest, err := b.layoutManifestAndSections()
	if err != nil {
		return ColumnPartImage{}, err
	}
	out := make([]byte, 0, len(manifest)+sumImageSectionDataBytes(b.sections))
	out = append(out, manifest...)
	for _, section := range b.sections {
		out = append(out, section.data...)
	}
	return ColumnPartImage{
		Version:       columnPartImageVersion,
		PartID:        b.part.Descriptor.PartID,
		Rows:          b.part.Descriptor.RowCount,
		ManifestBytes: len(manifest),
		Sections:      sections,
		Bytes:         out,
	}, nil
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
		offset := len(manifest)
		for i := range b.sections {
			b.sections[i].section.Offset = offset
			b.sections[i].section.Length = len(b.sections[i].data)
			sections[i] = b.sections[i].section
			offset += len(b.sections[i].data)
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
	return nil, nil, fmt.Errorf("colgranule: part image manifest length did not stabilize")
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
			return fmt.Errorf("colgranule: missing column %s", column.Name)
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
		enc.u32(uint32(len(column.Blocks)))
		for i, block := range column.Blocks {
			if i >= len(partColumn.Blocks) {
				return fmt.Errorf("colgranule: descriptor column %s block %d missing payload block", column.Name, i)
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
			return 0, fmt.Errorf("colgranule: descriptor column %s block %d has negative low-cardinality min/max", column.Name, i)
		}
		needed := uint64(block.Granule.Max) + 1
		if needed > maxCodeCardinality {
			return 0, fmt.Errorf("colgranule: descriptor column %s block %d inferred cardinality %d exceeds cap %d", column.Name, i, needed, maxCodeCardinality)
		}
		if uint32(needed) > cardinality {
			cardinality = uint32(needed)
		}
	}
	if cardinality == 0 {
		return 0, fmt.Errorf("colgranule: descriptor column %s has zero low-cardinality cardinality", column.Name)
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
	primaryIDs := make([]int64, 0, len(b.part.Locators))
	for primaryID := range b.part.Locators {
		primaryIDs = append(primaryIDs, primaryID)
	}
	sort.Slice(primaryIDs, func(i, j int) bool { return primaryIDs[i] < primaryIDs[j] })
	if uint64(len(primaryIDs)) > uint64(^uint32(0)) {
		return fmt.Errorf("colgranule: row locator count=%d exceeds uint32", len(primaryIDs))
	}
	recordBytes, err := checkedMulInt(len(primaryIDs), rowLocatorBytes, "row locator section bytes")
	if err != nil {
		return err
	}
	payloadBytes, err := checkedAddInt(4, recordBytes, "row locator section bytes")
	if err != nil {
		return err
	}
	enc := columnPartImageEncoder{buf: make([]byte, 0, payloadBytes)}
	enc.u32(uint32(len(primaryIDs)))
	for _, primaryID := range primaryIDs {
		locator := b.part.Locators[primaryID]
		enc.i64(locator.PrimaryID)
		enc.u64(locator.PartID)
		enc.u32(uint32(locator.PartRow))
		enc.u32(uint32(locator.GranuleOrdinal))
		enc.u32(uint32(locator.RowInGranule))
		enc.u32(0)
	}
	b.appendSection(ColumnPartImageSection{
		Kind:     ColumnPartImageSectionRowLocators,
		Category: ColumnPartImageCategoryLocators,
		Name:     "primary_id_locators",
		Rows:     len(primaryIDs),
	}, enc.bytes())
	return nil
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

func (b *columnPartImageBuilder) addDictionarySection() error {
	if len(b.opts.Dictionaries) == 0 {
		return nil
	}
	var enc columnPartImageEncoder
	names := make([]string, 0, len(b.opts.Dictionaries))
	for name := range b.opts.Dictionaries {
		names = append(names, name)
	}
	sort.Strings(names)
	enc.u32(uint32(len(names)))
	for _, name := range names {
		values := b.opts.Dictionaries[name]
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
		enc.str(name)
		enc.u32(uint32(len(entries)))
		for _, entry := range entries {
			enc.i64(entry.Code)
			enc.str(entry.Value)
		}
	}
	b.appendSection(ColumnPartImageSection{
		Kind:     ColumnPartImageSectionDictionaries,
		Category: ColumnPartImageCategoryDictionaries,
		Name:     "part_dictionaries",
	}, enc.bytes())
	return nil
}

func (b *columnPartImageBuilder) addColumnDataSections() error {
	for _, columnDescriptor := range b.part.Descriptor.Columns {
		column, ok := b.part.Columns[columnDescriptor.Name]
		if !ok {
			return fmt.Errorf("colgranule: missing column %s", columnDescriptor.Name)
		}
		totalPayloadBytes := 0
		for i, block := range column.Blocks {
			if len(block.Granule.Payload) != block.Descriptor.StoredBytes {
				return fmt.Errorf("colgranule: column %s block %d payload bytes=%d descriptor stored bytes=%d", columnDescriptor.Name, i, len(block.Granule.Payload), block.Descriptor.StoredBytes)
			}
			totalPayloadBytes += block.Descriptor.StoredBytes
		}
		data := make([]byte, 0, totalPayloadBytes)
		for _, block := range column.Blocks {
			data = append(data, block.Granule.Payload...)
		}
		b.appendSection(ColumnPartImageSection{
			Kind:        ColumnPartImageSectionColumnData,
			Category:    ColumnPartImageCategoryDeclaredColumns,
			Column:      columnDescriptor.Name,
			Rows:        b.part.Descriptor.RowCount,
			Granules:    len(b.part.Descriptor.Granules),
			Blocks:      len(column.Blocks),
			Encoding:    column.Definition.Encoding,
			Compression: column.Definition.Compression,
		}, data)
	}
	return nil
}

func (b *columnPartImageBuilder) appendSection(section ColumnPartImageSection, data []byte) {
	section.Length = len(data)
	b.sections = append(b.sections, columnPartImageSectionData{
		section: section,
		data:    data,
	})
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
		return fmt.Errorf("colgranule: %s must be finite, got %v", field, value)
	}
	if value < 0 {
		return fmt.Errorf("colgranule: %s %.6f is negative", field, value)
	}
	scaled := math.Round(value * 1_000_000)
	if scaled > math.MaxInt64 {
		return fmt.Errorf("colgranule: %s %.6f exceeds scaled int64", field, value)
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

func sumImageSectionDataBytes(sections []columnPartImageSectionData) int {
	total := 0
	for _, section := range sections {
		total += len(section.data)
	}
	return total
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
	default:
		return 0, fmt.Errorf("colgranule: unsupported column type %s", t)
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
}

func columnPartImageSectionKindCode(kind ColumnPartImageSectionKind) (uint16, error) {
	for _, code := range columnPartImageSectionCodes {
		if code.kind == kind {
			return code.kindCode, nil
		}
	}
	return 0, fmt.Errorf("colgranule: unknown image section kind %s", kind)
}

func columnPartImageSectionKindFromCode(code uint16) (ColumnPartImageSectionKind, error) {
	for _, entry := range columnPartImageSectionCodes {
		if entry.kindCode == code {
			return entry.kind, nil
		}
	}
	return "", fmt.Errorf("colgranule: unknown image section kind code %d", code)
}

func columnPartImageSectionCategoryCode(category ColumnPartImageSectionCategory) (uint16, error) {
	for _, code := range columnPartImageSectionCodes {
		if code.category == category {
			return code.categoryCode, nil
		}
	}
	return 0, fmt.Errorf("colgranule: unknown image section category %s", category)
}

func columnPartImageSectionCategoryFromCode(code uint16) (ColumnPartImageSectionCategory, error) {
	for _, entry := range columnPartImageSectionCodes {
		if entry.categoryCode == code {
			return entry.category, nil
		}
	}
	return "", fmt.Errorf("colgranule: unknown image section category code %d", code)
}
