package typedcolumn

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

const ColumnPartLayoutContractVersion uint16 = 1

// ColumnPartLayoutEndian records the byte order guaranteed by the writer for a
// certified physical section. Codec-defined variable-width encodings do not
// expose fixed-width endian-sensitive values to hot direct-view paths.
type ColumnPartLayoutEndian uint16

const (
	ColumnPartLayoutEndianNone ColumnPartLayoutEndian = iota
	ColumnPartLayoutEndianLittle
	ColumnPartLayoutEndianCodecDefined
)

func (e ColumnPartLayoutEndian) String() string {
	switch e {
	case ColumnPartLayoutEndianNone:
		return "none"
	case ColumnPartLayoutEndianLittle:
		return "little"
	case ColumnPartLayoutEndianCodecDefined:
		return "codec_defined"
	default:
		return fmt.Sprintf("endian_%d", e)
	}
}

type ColumnPartLayoutContractSection struct {
	Offset   int
	Length   int
	Checksum uint32
}

type ColumnPartLayoutContractBlock struct {
	FirstRow      int
	RowCount      int
	FirstGranule  int
	LastGranule   int
	Encoding      Encoding
	Compression   Compression
	RawBytes      int
	StoredBytes   int
	PayloadOffset int
	PayloadLength int
	NullCount     int
	DefaultCount  int
}

type ColumnPartLayoutContractColumn struct {
	Name                string
	LogicalType         string
	Type                ColumnType
	Encoding            Encoding
	Compression         Compression
	Rows                int
	Section             ColumnPartLayoutContractSection
	FixedWidthElements  int
	ElementSize         int
	Alignment           int
	Endian              ColumnPartLayoutEndian
	LengthMultiple      int
	DirectViewCertified bool
	StreamingCertified  bool
	StatsCertified      bool
	PruningCertified    bool
	Dictionary          bool
	DictionaryOrdered   bool
	DictionaryCollation string
	DictionarySection   ColumnPartLayoutContractSection
	NullMaskPresent     bool
	DefaultMaskPresent  bool
	NullCount           int
	DefaultCount        int
	Blocks              []ColumnPartLayoutContractBlock
}

type ColumnPartLayoutContract struct {
	Version       uint16
	PartID        uint64
	Rows          int
	ImageVersion  uint16
	ManifestBytes int
	Descriptor    ColumnPartLayoutContractSection
	Columns       []ColumnPartLayoutContractColumn
}

type ColumnPartLayoutCertification struct {
	Contract              ColumnPartLayoutContract
	DirectViewCertified   int
	StreamingCertified    int
	StatsCertified        int
	PruningCertified      int
	CertificationFallback []string
}

func (c ColumnPartLayoutContract) Column(name string) (ColumnPartLayoutContractColumn, bool) {
	for _, column := range c.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return ColumnPartLayoutContractColumn{}, false
}

func (c ColumnPartLayoutCertification) Column(name string) (ColumnPartLayoutContractColumn, bool) {
	return c.Contract.Column(name)
}

func (i ColumnPartImage) LayoutContractSection() (ColumnPartImageSection, error) {
	section, err := i.singleSection(ColumnPartImageSectionLayoutContract)
	if err != nil {
		return ColumnPartImageSection{}, fmt.Errorf("typedcolumn: missing writer-certified layout contract (pre-alpha typed-column assets must be rebuilt): %w", err)
	}
	return section, nil
}

func CertifyColumnPartLayoutContractFromImage(image ColumnPartImage) (ColumnPartLayoutCertification, error) {
	if err := image.validateForRead(); err != nil {
		return ColumnPartLayoutCertification{}, err
	}
	descriptorSection, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		return ColumnPartLayoutCertification{}, err
	}
	descriptorRaw := image.sectionBytes(descriptorSection)
	desc, columns, err := decodeColumnPartDescriptorSection(descriptorRaw)
	if err != nil {
		return ColumnPartLayoutCertification{}, err
	}
	contractSection, err := image.LayoutContractSection()
	if err != nil {
		return ColumnPartLayoutCertification{}, err
	}
	return CertifyColumnPartLayoutContract(image, desc, columns, descriptorRaw, image.sectionBytes(contractSection))
}

func DecodeColumnPartLayoutContract(data []byte) (ColumnPartLayoutContract, error) {
	dec := columnPartImageDecoder{data: data}
	version, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	if version != ColumnPartLayoutContractVersion {
		return ColumnPartLayoutContract{}, fmt.Errorf("typedcolumn: unsupported layout contract version %d", version)
	}
	if reserved, err := dec.u16(); err != nil {
		return ColumnPartLayoutContract{}, err
	} else if reserved != 0 {
		return ColumnPartLayoutContract{}, fmt.Errorf("typedcolumn: layout contract reserved=%d want 0", reserved)
	}
	partID, err := dec.u64()
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	rows64, err := dec.i64()
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	rows, err := nonNegativeInt64ToInt(rows64, "layout contract rows")
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	imageVersion, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	if reserved, err := dec.u16(); err != nil {
		return ColumnPartLayoutContract{}, err
	} else if reserved != 0 {
		return ColumnPartLayoutContract{}, fmt.Errorf("typedcolumn: layout contract image reserved=%d want 0", reserved)
	}
	manifestBytes64, err := dec.i64()
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	manifestBytes, err := nonNegativeInt64ToInt(manifestBytes64, "layout contract manifest bytes")
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	descriptor, err := decodeColumnPartLayoutContractSection(&dec)
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	columnCount, err := dec.u32()
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	columnsTotal, err := dec.boundedCount(columnCount, 96, "layout contract columns")
	if err != nil {
		return ColumnPartLayoutContract{}, err
	}
	contract := ColumnPartLayoutContract{
		Version:       version,
		PartID:        partID,
		Rows:          rows,
		ImageVersion:  imageVersion,
		ManifestBytes: manifestBytes,
		Descriptor:    descriptor,
		Columns:       make([]ColumnPartLayoutContractColumn, 0, columnsTotal),
	}
	seen := make(map[string]struct{}, columnsTotal)
	for i := 0; i < columnsTotal; i++ {
		column, err := decodeColumnPartLayoutContractColumn(&dec)
		if err != nil {
			return ColumnPartLayoutContract{}, err
		}
		if column.Name == "" {
			return ColumnPartLayoutContract{}, fmt.Errorf("typedcolumn: layout contract column %d has empty name", i)
		}
		if _, exists := seen[column.Name]; exists {
			return ColumnPartLayoutContract{}, fmt.Errorf("typedcolumn: duplicate layout contract column %s", column.Name)
		}
		seen[column.Name] = struct{}{}
		contract.Columns = append(contract.Columns, column)
	}
	if err := dec.finish(); err != nil {
		return ColumnPartLayoutContract{}, err
	}
	return contract, nil
}

func CertifyColumnPartLayoutContract(image ColumnPartImage, desc ColumnPartDescriptor, columns map[string]ColumnPartColumn, descriptorRaw []byte, contractRaw []byte) (ColumnPartLayoutCertification, error) {
	if len(contractRaw) == 0 {
		return ColumnPartLayoutCertification{}, fmt.Errorf("typedcolumn: missing writer-certified layout contract (pre-alpha typed-column assets must be rebuilt)")
	}
	contractSection, err := image.LayoutContractSection()
	if err != nil {
		return ColumnPartLayoutCertification{}, err
	}
	if len(contractRaw) != contractSection.Length {
		return ColumnPartLayoutCertification{}, fmt.Errorf("typedcolumn: layout contract bytes=%d want section length=%d", len(contractRaw), contractSection.Length)
	}
	contract, err := DecodeColumnPartLayoutContract(contractRaw)
	if err != nil {
		return ColumnPartLayoutCertification{}, err
	}
	cert := ColumnPartLayoutCertification{Contract: contract}
	if contract.PartID != image.PartID || contract.PartID != desc.PartID {
		return cert, fmt.Errorf("typedcolumn: layout contract part_id=%d image_part=%d descriptor_part=%d", contract.PartID, image.PartID, desc.PartID)
	}
	if contract.Rows != image.Rows || contract.Rows != desc.RowCount {
		return cert, fmt.Errorf("typedcolumn: layout contract rows=%d image_rows=%d descriptor_rows=%d", contract.Rows, image.Rows, desc.RowCount)
	}
	if contract.ImageVersion != image.Version {
		return cert, fmt.Errorf("typedcolumn: layout contract image version=%d want %d", contract.ImageVersion, image.Version)
	}
	if contract.ManifestBytes != image.ManifestBytes {
		return cert, fmt.Errorf("typedcolumn: layout contract manifest bytes=%d want %d", contract.ManifestBytes, image.ManifestBytes)
	}
	descriptorSection, err := image.singleSection(ColumnPartImageSectionDescriptor)
	if err != nil {
		return cert, err
	}
	if err := certifyLayoutContractSection("descriptor", contract.Descriptor, descriptorSection, descriptorRaw); err != nil {
		return cert, err
	}
	if len(contract.Columns) != len(desc.Columns) {
		return cert, fmt.Errorf("typedcolumn: layout contract columns=%d descriptor columns=%d", len(contract.Columns), len(desc.Columns))
	}
	contractColumns := make(map[string]ColumnPartLayoutContractColumn, len(contract.Columns))
	for _, column := range contract.Columns {
		contractColumns[column.Name] = column
	}
	for _, columnDesc := range desc.Columns {
		contractColumn, ok := contractColumns[columnDesc.Name]
		if !ok {
			return cert, fmt.Errorf("typedcolumn: layout contract missing column %s", columnDesc.Name)
		}
		column, ok := columns[columnDesc.Name]
		if !ok {
			return cert, fmt.Errorf("typedcolumn: layout contract descriptor column %s missing decoded column", columnDesc.Name)
		}
		if err := certifyLayoutContractColumn(image, desc, columnDesc, column, contractColumn); err != nil {
			return cert, err
		}
		if contractColumn.DirectViewCertified {
			cert.DirectViewCertified++
		}
		if contractColumn.StreamingCertified {
			cert.StreamingCertified++
		}
		if contractColumn.StatsCertified {
			cert.StatsCertified++
		}
		if contractColumn.PruningCertified {
			cert.PruningCertified++
		}
		if !contractColumn.DirectViewCertified && !contractColumn.StreamingCertified {
			cert.CertificationFallback = append(cert.CertificationFallback, fmt.Sprintf("%s:no_fast_layout", contractColumn.Name))
		}
	}
	return cert, nil
}

func encodeColumnPartLayoutContract(part *ColumnPart, opts ColumnPartImageOptions, sectionData []columnPartImageSectionData, manifestBytes int) ([]byte, error) {
	if part == nil {
		return nil, fmt.Errorf("typedcolumn: nil part")
	}
	sectionsByColumn := make(map[string]columnPartImageSectionData, len(part.Descriptor.Columns))
	var descriptorData columnPartImageSectionData
	var dictionaryData columnPartImageSectionData
	for _, section := range sectionData {
		switch section.section.Kind {
		case ColumnPartImageSectionDescriptor:
			descriptorData = section
		case ColumnPartImageSectionDictionaries:
			dictionaryData = section
		case ColumnPartImageSectionColumnData:
			sectionsByColumn[section.section.Column] = section
		}
	}
	if descriptorData.section.Kind != ColumnPartImageSectionDescriptor {
		return nil, fmt.Errorf("typedcolumn: layout contract requires descriptor section")
	}
	var enc columnPartImageEncoder
	enc.u16(ColumnPartLayoutContractVersion)
	enc.u16(0)
	enc.u64(part.Descriptor.PartID)
	enc.i64(int64(part.Descriptor.RowCount))
	enc.u16(columnPartImageVersion)
	enc.u16(0)
	enc.i64(int64(manifestBytes))
	encodeColumnPartLayoutContractSection(&enc, ColumnPartLayoutContractSection{Offset: descriptorData.section.Offset, Length: len(descriptorData.data), Checksum: crc.Checksum(descriptorData.data)})
	enc.u32(uint32(len(part.Descriptor.Columns)))
	for _, columnDesc := range part.Descriptor.Columns {
		column, ok := part.Columns[columnDesc.Name]
		if !ok {
			return nil, fmt.Errorf("typedcolumn: layout contract missing part column %s", columnDesc.Name)
		}
		data, ok := sectionsByColumn[columnDesc.Name]
		if !ok {
			return nil, fmt.Errorf("typedcolumn: layout contract missing data section %s", columnDesc.Name)
		}
		contractColumn, err := buildColumnPartLayoutContractColumn(opts, columnDesc, column, data, dictionaryData)
		if err != nil {
			return nil, err
		}
		encodeColumnPartLayoutContractColumn(&enc, contractColumn)
	}
	return enc.bytes(), nil
}

func buildColumnPartLayoutContractColumn(opts ColumnPartImageOptions, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn, data columnPartImageSectionData, dictionaryData columnPartImageSectionData) (ColumnPartLayoutContractColumn, error) {
	logicalType := ""
	if opts.LayoutLogicalTypes != nil {
		logicalType = opts.LayoutLogicalTypes[columnDesc.Name]
	}
	dictOrdered := false
	if opts.DictionaryOrder != nil {
		dictOrdered = opts.DictionaryOrder[columnDesc.Name]
	}
	dictCollation := ""
	if opts.DictionaryCollation != nil {
		dictCollation = opts.DictionaryCollation[columnDesc.Name]
	}
	contractDef := column.Definition
	contractDef.Encoding = data.section.Encoding
	contractDef.Compression = data.section.Compression
	layout := physicalColumnLayoutForContract(logicalType, contractDef)
	contract := ColumnPartLayoutContractColumn{
		Name:                columnDesc.Name,
		LogicalType:         logicalType,
		Type:                columnDesc.Type,
		Encoding:            data.section.Encoding,
		Compression:         data.section.Compression,
		Rows:                data.section.Rows,
		Section:             ColumnPartLayoutContractSection{Offset: data.section.Offset, Length: len(data.data), Checksum: crc.Checksum(data.data)},
		FixedWidthElements:  column.Definition.FixedWidthElements,
		ElementSize:         layout.elementSize,
		Alignment:           layout.alignment,
		Endian:              layout.endian,
		LengthMultiple:      layout.lengthMultiple,
		DirectViewCertified: layout.direct,
		StreamingCertified:  layout.streaming,
		StatsCertified:      layout.stats,
		PruningCertified:    layout.pruning,
		Dictionary:          column.Definition.Type == ColumnTypeLowCardinalityCode,
		DictionaryOrdered:   dictOrdered,
		DictionaryCollation: dictCollation,
		Blocks:              make([]ColumnPartLayoutContractBlock, 0, len(column.Blocks)),
	}
	if contract.Dictionary && dictionaryData.section.Kind == ColumnPartImageSectionDictionaries {
		contract.DictionarySection = ColumnPartLayoutContractSection{Offset: dictionaryData.section.Offset, Length: len(dictionaryData.data), Checksum: crc.Checksum(dictionaryData.data)}
	} else if contract.Dictionary {
		contract.StreamingCertified = false
		contract.StatsCertified = false
		contract.PruningCertified = false
	}
	payloadOffset := data.section.Offset
	for i, block := range column.Blocks {
		if i >= len(columnDesc.Blocks) {
			return ColumnPartLayoutContractColumn{}, fmt.Errorf("typedcolumn: layout contract column %s block %d missing descriptor", columnDesc.Name, i)
		}
		if block.Granule.NullCount > 0 {
			contract.NullMaskPresent = true
		}
		if block.Granule.DefaultCount > 0 {
			contract.DefaultMaskPresent = true
		}
		contract.NullCount += block.Granule.NullCount
		contract.DefaultCount += block.Granule.DefaultCount
		contract.Blocks = append(contract.Blocks, ColumnPartLayoutContractBlock{
			FirstRow:      block.Descriptor.FirstRow,
			RowCount:      block.Descriptor.RowCount,
			FirstGranule:  block.Descriptor.FirstGranule,
			LastGranule:   block.Descriptor.LastGranule,
			Encoding:      block.Descriptor.Encoding,
			Compression:   block.Descriptor.Compression,
			RawBytes:      block.Descriptor.RawBytes,
			StoredBytes:   block.Descriptor.StoredBytes,
			PayloadOffset: payloadOffset,
			PayloadLength: block.Descriptor.StoredBytes,
			NullCount:     block.Granule.NullCount,
			DefaultCount:  block.Granule.DefaultCount,
		})
		payloadOffset += block.Descriptor.StoredBytes
	}
	return contract, nil
}

type columnPartContractLayout struct {
	elementSize    int
	alignment      int
	endian         ColumnPartLayoutEndian
	lengthMultiple int
	direct         bool
	streaming      bool
	stats          bool
	pruning        bool
}

func physicalColumnLayoutForContract(logicalType string, def ColumnDefinition) columnPartContractLayout {
	switch def.Encoding {
	case EncodingRawInt64:
		direct := def.Compression == CompressionNone && (logicalType == "" || logicalType == "int64")
		return columnPartContractLayout{elementSize: 8, alignment: 8, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 8, direct: direct, stats: direct, pruning: direct}
	case EncodingRawFloat32Vector:
		direct := def.Compression == CompressionNone && (logicalType == "" || logicalType == "float32_vector")
		return columnPartContractLayout{elementSize: 4, alignment: 4, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 4, direct: direct, stats: direct, pruning: direct}
	case EncodingRawUint32Dense:
		direct := def.Compression == CompressionNone && (logicalType == "" || logicalType == "adjacency_list")
		return columnPartContractLayout{elementSize: 4, alignment: 4, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 4, direct: direct, pruning: direct}
	case EncodingDeltaVarint, EncodingDoubleDeltaVarint:
		streaming := def.Compression == CompressionNone && (logicalType == "" || logicalType == "int64")
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined, streaming: streaming, stats: streaming, pruning: streaming}
	case EncodingBoolBitpackRLE:
		streaming := def.Compression == CompressionNone && (logicalType == "" || logicalType == "bool")
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined, streaming: streaming, stats: streaming}
	case EncodingLowCardinalityUint32:
		streaming := def.Compression == CompressionNone && (logicalType == "" || logicalType == "string")
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined, streaming: streaming, stats: streaming, pruning: streaming}
	case EncodingNullableInt64:
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined}
	default:
		return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
	}
}

func certifyLayoutContractColumn(image ColumnPartImage, desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn, contract ColumnPartLayoutContractColumn) error {
	section, ok := image.columnDataSection(columnDesc.Name)
	if !ok {
		return fmt.Errorf("typedcolumn: layout contract column %s missing image section", columnDesc.Name)
	}
	var sectionRaw []byte
	if section.Offset >= 0 && section.Length >= 0 && section.Offset <= len(image.Bytes)-section.Length {
		sectionRaw = image.sectionBytes(section)
	}
	if err := certifyLayoutContractSection("column "+columnDesc.Name, contract.Section, section, sectionRaw); err != nil {
		return err
	}
	if contract.Type != columnDesc.Type || contract.Type != column.Definition.Type {
		return fmt.Errorf("typedcolumn: layout contract column %s type=%s descriptor=%s definition=%s", columnDesc.Name, contract.Type, columnDesc.Type, column.Definition.Type)
	}
	if contract.Encoding != section.Encoding || contract.Compression != section.Compression {
		return fmt.Errorf("typedcolumn: layout contract column %s encoding=%s compression=%s want section encoding=%s compression=%s", columnDesc.Name, contract.Encoding, contract.Compression, section.Encoding, section.Compression)
	}
	if contract.FixedWidthElements != column.Definition.FixedWidthElements || contract.FixedWidthElements != columnDesc.FixedWidthElements {
		return fmt.Errorf("typedcolumn: layout contract column %s fixed_width_elements=%d want %d", columnDesc.Name, contract.FixedWidthElements, column.Definition.FixedWidthElements)
	}
	if contract.Rows != desc.RowCount || section.Rows != desc.RowCount {
		return fmt.Errorf("typedcolumn: layout contract column %s rows=%d section_rows=%d descriptor_rows=%d", columnDesc.Name, contract.Rows, section.Rows, desc.RowCount)
	}
	if len(contract.Blocks) != len(column.Blocks) || len(contract.Blocks) != len(columnDesc.Blocks) {
		return fmt.Errorf("typedcolumn: layout contract column %s blocks=%d descriptor=%d", columnDesc.Name, len(contract.Blocks), len(column.Blocks))
	}
	expectedDef := column.Definition
	expectedDef.Encoding = contract.Encoding
	expectedDef.Compression = contract.Compression
	expectedLayout := physicalColumnLayoutForContract(contract.LogicalType, expectedDef)
	if contract.Dictionary && contract.DictionarySection == (ColumnPartLayoutContractSection{}) {
		expectedLayout.streaming = false
		expectedLayout.stats = false
		expectedLayout.pruning = false
	}
	if contract.ElementSize != expectedLayout.elementSize || contract.Alignment != expectedLayout.alignment || contract.Endian != expectedLayout.endian || contract.LengthMultiple != expectedLayout.lengthMultiple {
		return fmt.Errorf("typedcolumn: layout contract column %s element/alignment/endian/length=(%d,%d,%s,%d) want (%d,%d,%s,%d)", columnDesc.Name, contract.ElementSize, contract.Alignment, contract.Endian, contract.LengthMultiple, expectedLayout.elementSize, expectedLayout.alignment, expectedLayout.endian, expectedLayout.lengthMultiple)
	}
	if contract.DirectViewCertified != expectedLayout.direct || contract.StreamingCertified != expectedLayout.streaming || contract.StatsCertified != expectedLayout.stats || contract.PruningCertified != expectedLayout.pruning {
		return fmt.Errorf("typedcolumn: layout contract column %s capability flags direct=%v streaming=%v stats=%v pruning=%v want direct=%v streaming=%v stats=%v pruning=%v", columnDesc.Name, contract.DirectViewCertified, contract.StreamingCertified, contract.StatsCertified, contract.PruningCertified, expectedLayout.direct, expectedLayout.streaming, expectedLayout.stats, expectedLayout.pruning)
	}
	if contract.Dictionary != (column.Definition.Type == ColumnTypeLowCardinalityCode) {
		return fmt.Errorf("typedcolumn: layout contract column %s dictionary=%v type=%s", columnDesc.Name, contract.Dictionary, column.Definition.Type)
	}
	if contract.DictionaryOrdered && contract.DictionaryCollation == "" {
		return fmt.Errorf("typedcolumn: layout contract column %s dictionary order requires collation", columnDesc.Name)
	}
	if contract.Dictionary {
		dictSection, err := image.singleSection(ColumnPartImageSectionDictionaries)
		if err != nil {
			if contract.DictionarySection != (ColumnPartLayoutContractSection{}) || contract.StreamingCertified || contract.PruningCertified || contract.StatsCertified {
				return fmt.Errorf("typedcolumn: layout contract column %s dictionary identity: %w", columnDesc.Name, err)
			}
		} else if contract.DictionarySection.Offset != dictSection.Offset || contract.DictionarySection.Length != dictSection.Length {
			return fmt.Errorf("typedcolumn: layout contract column %s dictionary section offset/length=(%d,%d) want (%d,%d)", columnDesc.Name, contract.DictionarySection.Offset, contract.DictionarySection.Length, dictSection.Offset, dictSection.Length)
		} else if dictSection.Offset >= 0 && dictSection.Length >= 0 && dictSection.Offset <= len(image.Bytes)-dictSection.Length {
			if err := certifyLayoutContractSection("column "+columnDesc.Name+" dictionary", contract.DictionarySection, dictSection, image.sectionBytes(dictSection)); err != nil {
				return err
			}
		}
	}
	payloadOffset := section.Offset
	nullCount := 0
	defaultCount := 0
	for i, block := range column.Blocks {
		if err := certifyLayoutContractBlock(columnDesc.Name, i, contract, block, contract.Blocks[i], payloadOffset); err != nil {
			return err
		}
		payloadOffset += block.Descriptor.StoredBytes
		nullCount += block.Granule.NullCount
		defaultCount += block.Granule.DefaultCount
	}
	if payloadOffset != section.Offset+section.Length {
		return fmt.Errorf("typedcolumn: layout contract column %s consumed=%d section=%d", columnDesc.Name, payloadOffset-section.Offset, section.Length)
	}
	if contract.NullCount != nullCount || contract.DefaultCount != defaultCount {
		return fmt.Errorf("typedcolumn: layout contract column %s null/default counts=(%d,%d) want (%d,%d)", columnDesc.Name, contract.NullCount, contract.DefaultCount, nullCount, defaultCount)
	}
	if contract.NullMaskPresent != (nullCount > 0) || contract.DefaultMaskPresent != (defaultCount > 0) {
		return fmt.Errorf("typedcolumn: layout contract column %s null/default mask flags=(%v,%v) want (%v,%v)", columnDesc.Name, contract.NullMaskPresent, contract.DefaultMaskPresent, nullCount > 0, defaultCount > 0)
	}
	return nil
}

func certifyLayoutContractBlock(column string, index int, contract ColumnPartLayoutContractColumn, block ColumnBlock, certified ColumnPartLayoutContractBlock, expectedPayloadOffset int) error {
	want := block.Descriptor
	if certified.FirstRow != want.FirstRow || certified.RowCount != want.RowCount || certified.FirstGranule != want.FirstGranule || certified.LastGranule != want.LastGranule {
		return fmt.Errorf("typedcolumn: layout contract column %s block %d row span=(%d,%d,%d,%d) want (%d,%d,%d,%d)", column, index, certified.FirstRow, certified.RowCount, certified.FirstGranule, certified.LastGranule, want.FirstRow, want.RowCount, want.FirstGranule, want.LastGranule)
	}
	if certified.Encoding != want.Encoding || certified.Compression != want.Compression || certified.RawBytes != want.RawBytes || certified.StoredBytes != want.StoredBytes {
		return fmt.Errorf("typedcolumn: layout contract column %s block %d encoding/compression/raw/stored=(%s,%s,%d,%d) want (%s,%s,%d,%d)", column, index, certified.Encoding, certified.Compression, certified.RawBytes, certified.StoredBytes, want.Encoding, want.Compression, want.RawBytes, want.StoredBytes)
	}
	if certified.NullCount != block.Granule.NullCount || certified.DefaultCount != block.Granule.DefaultCount {
		return fmt.Errorf("typedcolumn: layout contract column %s block %d null/default=(%d,%d) want (%d,%d)", column, index, certified.NullCount, certified.DefaultCount, block.Granule.NullCount, block.Granule.DefaultCount)
	}
	if certified.PayloadOffset != expectedPayloadOffset || certified.PayloadLength != want.StoredBytes {
		return fmt.Errorf("typedcolumn: layout contract column %s block %d payload offset/length=(%d,%d) want (%d,%d)", column, index, certified.PayloadOffset, certified.PayloadLength, expectedPayloadOffset, want.StoredBytes)
	}
	if contract.DirectViewCertified {
		if contract.Alignment <= 0 || certified.PayloadOffset%contract.Alignment != 0 || contract.Section.Offset%contract.Alignment != 0 {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d direct-view alignment offset=%d section=%d alignment=%d", column, index, certified.PayloadOffset, contract.Section.Offset, contract.Alignment)
		}
		if contract.LengthMultiple <= 0 || certified.PayloadLength%contract.LengthMultiple != 0 || certified.RawBytes%contract.LengthMultiple != 0 {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d direct-view length payload=%d raw=%d multiple=%d", column, index, certified.PayloadLength, certified.RawBytes, contract.LengthMultiple)
		}
		if contract.Endian != ColumnPartLayoutEndianLittle || certified.Compression != CompressionNone {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d direct-view endian/compression=(%s,%s)", column, index, contract.Endian, certified.Compression)
		}
		elementsPerRow := contract.FixedWidthElements
		if elementsPerRow == 0 {
			elementsPerRow = 1
		}
		wantBytes, err := checkedMulInt(certified.RowCount, elementsPerRow, "layout contract fixed-width elements")
		if err != nil {
			return err
		}
		wantBytes, err = checkedMulInt(wantBytes, contract.ElementSize, "layout contract fixed-width bytes")
		if err != nil {
			return err
		}
		if certified.PayloadLength != wantBytes || certified.RawBytes != wantBytes || certified.StoredBytes != wantBytes {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d fixed-width bytes payload/raw/stored=(%d,%d,%d) want %d", column, index, certified.PayloadLength, certified.RawBytes, certified.StoredBytes, wantBytes)
		}
	}
	return nil
}

func certifyLayoutContractSection(name string, contract ColumnPartLayoutContractSection, section ColumnPartImageSection, raw []byte) error {
	if contract.Offset != section.Offset || contract.Length != section.Length {
		return fmt.Errorf("typedcolumn: layout contract %s section offset/length=(%d,%d) want (%d,%d)", name, contract.Offset, contract.Length, section.Offset, section.Length)
	}
	if raw != nil {
		if len(raw) != section.Length {
			return fmt.Errorf("typedcolumn: layout contract %s raw bytes=%d want %d", name, len(raw), section.Length)
		}
		if got := crc.Checksum(raw); got != contract.Checksum {
			return fmt.Errorf("typedcolumn: layout contract %s checksum=%08x want %08x", name, got, contract.Checksum)
		}
	}
	return nil
}

func encodeColumnPartLayoutContractSection(enc *columnPartImageEncoder, section ColumnPartLayoutContractSection) {
	enc.i64(int64(section.Offset))
	enc.i64(int64(section.Length))
	enc.u32(section.Checksum)
}

func decodeColumnPartLayoutContractSection(dec *columnPartImageDecoder) (ColumnPartLayoutContractSection, error) {
	offset64, err := dec.i64()
	if err != nil {
		return ColumnPartLayoutContractSection{}, err
	}
	length64, err := dec.i64()
	if err != nil {
		return ColumnPartLayoutContractSection{}, err
	}
	checksum, err := dec.u32()
	if err != nil {
		return ColumnPartLayoutContractSection{}, err
	}
	offset, err := nonNegativeInt64ToInt(offset64, "layout contract section offset")
	if err != nil {
		return ColumnPartLayoutContractSection{}, err
	}
	length, err := nonNegativeInt64ToInt(length64, "layout contract section length")
	if err != nil {
		return ColumnPartLayoutContractSection{}, err
	}
	return ColumnPartLayoutContractSection{Offset: offset, Length: length, Checksum: checksum}, nil
}

const (
	layoutContractFlagDirectView uint32 = 1 << iota
	layoutContractFlagStreaming
	layoutContractFlagStats
	layoutContractFlagPruning
	layoutContractFlagDictionary
	layoutContractFlagDictionaryOrdered
	layoutContractFlagNullMask
	layoutContractFlagDefaultMask
)

func encodeColumnPartLayoutContractColumn(enc *columnPartImageEncoder, column ColumnPartLayoutContractColumn) {
	enc.str(column.Name)
	enc.str(column.LogicalType)
	columnTypeCode, _ := columnTypeCode(column.Type)
	enc.u16(columnTypeCode)
	enc.u16(uint16(column.Encoding))
	enc.u16(uint16(column.Compression))
	enc.u16(0)
	flags := uint32(0)
	if column.DirectViewCertified {
		flags |= layoutContractFlagDirectView
	}
	if column.StreamingCertified {
		flags |= layoutContractFlagStreaming
	}
	if column.StatsCertified {
		flags |= layoutContractFlagStats
	}
	if column.PruningCertified {
		flags |= layoutContractFlagPruning
	}
	if column.Dictionary {
		flags |= layoutContractFlagDictionary
	}
	if column.DictionaryOrdered {
		flags |= layoutContractFlagDictionaryOrdered
	}
	if column.NullMaskPresent {
		flags |= layoutContractFlagNullMask
	}
	if column.DefaultMaskPresent {
		flags |= layoutContractFlagDefaultMask
	}
	enc.u32(flags)
	enc.i64(int64(column.Rows))
	encodeColumnPartLayoutContractSection(enc, column.Section)
	enc.i64(int64(column.FixedWidthElements))
	enc.i64(int64(column.ElementSize))
	enc.i64(int64(column.Alignment))
	enc.u16(uint16(column.Endian))
	enc.u16(0)
	enc.i64(int64(column.LengthMultiple))
	enc.i64(int64(column.NullCount))
	enc.i64(int64(column.DefaultCount))
	encodeColumnPartLayoutContractSection(enc, column.DictionarySection)
	enc.str(column.DictionaryCollation)
	enc.u32(uint32(len(column.Blocks)))
	for _, block := range column.Blocks {
		encodeColumnPartLayoutContractBlock(enc, block)
	}
}

func decodeColumnPartLayoutContractColumn(dec *columnPartImageDecoder) (ColumnPartLayoutContractColumn, error) {
	name, err := dec.str()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	logicalType, err := dec.str()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	columnTypeCode, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	columnType, err := columnTypeFromCode(columnTypeCode)
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	encoding, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	compression, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	if reserved, err := dec.u16(); err != nil {
		return ColumnPartLayoutContractColumn{}, err
	} else if reserved != 0 {
		return ColumnPartLayoutContractColumn{}, fmt.Errorf("typedcolumn: layout contract column %s reserved=%d want 0", name, reserved)
	}
	flags, err := dec.u32()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	if flags&^(layoutContractFlagDirectView|layoutContractFlagStreaming|layoutContractFlagStats|layoutContractFlagPruning|layoutContractFlagDictionary|layoutContractFlagDictionaryOrdered|layoutContractFlagNullMask|layoutContractFlagDefaultMask) != 0 {
		return ColumnPartLayoutContractColumn{}, fmt.Errorf("typedcolumn: layout contract column %s unsupported flags 0x%x", name, flags)
	}
	rows64, err := dec.i64()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	rows, err := nonNegativeInt64ToInt(rows64, "layout contract column rows")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	section, err := decodeColumnPartLayoutContractSection(dec)
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	fixedWidthElements, err := decodeNonNegativeLayoutInt(dec, "layout contract fixed-width elements")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	elementSize, err := decodeNonNegativeLayoutInt(dec, "layout contract element size")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	alignment, err := decodeNonNegativeLayoutInt(dec, "layout contract alignment")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	endianCode, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	if endianCode > uint16(ColumnPartLayoutEndianCodecDefined) {
		return ColumnPartLayoutContractColumn{}, fmt.Errorf("typedcolumn: layout contract column %s unsupported endian=%d", name, endianCode)
	}
	if reserved, err := dec.u16(); err != nil {
		return ColumnPartLayoutContractColumn{}, err
	} else if reserved != 0 {
		return ColumnPartLayoutContractColumn{}, fmt.Errorf("typedcolumn: layout contract column %s endian reserved=%d want 0", name, reserved)
	}
	lengthMultiple, err := decodeNonNegativeLayoutInt(dec, "layout contract length multiple")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	nullCount, err := decodeNonNegativeLayoutInt(dec, "layout contract null count")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	defaultCount, err := decodeNonNegativeLayoutInt(dec, "layout contract default count")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	dictionarySection, err := decodeColumnPartLayoutContractSection(dec)
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	dictionaryCollation, err := dec.str()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	blockCount, err := dec.u32()
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	blocksTotal, err := dec.boundedCount(blockCount, 88, "layout contract column blocks")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	column := ColumnPartLayoutContractColumn{
		Name:                name,
		LogicalType:         logicalType,
		Type:                columnType,
		Encoding:            Encoding(encoding),
		Compression:         Compression(compression),
		Rows:                rows,
		Section:             section,
		FixedWidthElements:  fixedWidthElements,
		ElementSize:         elementSize,
		Alignment:           alignment,
		Endian:              ColumnPartLayoutEndian(endianCode),
		LengthMultiple:      lengthMultiple,
		DirectViewCertified: flags&layoutContractFlagDirectView != 0,
		StreamingCertified:  flags&layoutContractFlagStreaming != 0,
		StatsCertified:      flags&layoutContractFlagStats != 0,
		PruningCertified:    flags&layoutContractFlagPruning != 0,
		Dictionary:          flags&layoutContractFlagDictionary != 0,
		DictionaryOrdered:   flags&layoutContractFlagDictionaryOrdered != 0,
		NullMaskPresent:     flags&layoutContractFlagNullMask != 0,
		DefaultMaskPresent:  flags&layoutContractFlagDefaultMask != 0,
		NullCount:           nullCount,
		DefaultCount:        defaultCount,
		DictionarySection:   dictionarySection,
		DictionaryCollation: dictionaryCollation,
		Blocks:              make([]ColumnPartLayoutContractBlock, 0, blocksTotal),
	}
	for i := 0; i < blocksTotal; i++ {
		block, err := decodeColumnPartLayoutContractBlock(dec)
		if err != nil {
			return ColumnPartLayoutContractColumn{}, err
		}
		column.Blocks = append(column.Blocks, block)
	}
	return column, nil
}

func encodeColumnPartLayoutContractBlock(enc *columnPartImageEncoder, block ColumnPartLayoutContractBlock) {
	enc.i64(int64(block.FirstRow))
	enc.i64(int64(block.RowCount))
	enc.i64(int64(block.FirstGranule))
	enc.i64(int64(block.LastGranule))
	enc.u16(uint16(block.Encoding))
	enc.u16(uint16(block.Compression))
	enc.u32(0)
	enc.i64(int64(block.RawBytes))
	enc.i64(int64(block.StoredBytes))
	enc.i64(int64(block.PayloadOffset))
	enc.i64(int64(block.PayloadLength))
	enc.i64(int64(block.NullCount))
	enc.i64(int64(block.DefaultCount))
}

func decodeColumnPartLayoutContractBlock(dec *columnPartImageDecoder) (ColumnPartLayoutContractBlock, error) {
	firstRow, err := decodeNonNegativeLayoutInt(dec, "layout contract block first row")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	rowCount, err := decodeNonNegativeLayoutInt(dec, "layout contract block row count")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	firstGranule, err := decodeNonNegativeLayoutInt(dec, "layout contract block first granule")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	lastGranule, err := decodeNonNegativeLayoutInt(dec, "layout contract block last granule")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	encoding, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	compression, err := dec.u16()
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	if reserved, err := dec.u32(); err != nil {
		return ColumnPartLayoutContractBlock{}, err
	} else if reserved != 0 {
		return ColumnPartLayoutContractBlock{}, fmt.Errorf("typedcolumn: layout contract block reserved=%d want 0", reserved)
	}
	rawBytes, err := decodeNonNegativeLayoutInt(dec, "layout contract block raw bytes")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	storedBytes, err := decodeNonNegativeLayoutInt(dec, "layout contract block stored bytes")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	payloadOffset, err := decodeNonNegativeLayoutInt(dec, "layout contract block payload offset")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	payloadLength, err := decodeNonNegativeLayoutInt(dec, "layout contract block payload length")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	nullCount, err := decodeNonNegativeLayoutInt(dec, "layout contract block null count")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	defaultCount, err := decodeNonNegativeLayoutInt(dec, "layout contract block default count")
	if err != nil {
		return ColumnPartLayoutContractBlock{}, err
	}
	return ColumnPartLayoutContractBlock{FirstRow: firstRow, RowCount: rowCount, FirstGranule: firstGranule, LastGranule: lastGranule, Encoding: Encoding(encoding), Compression: Compression(compression), RawBytes: rawBytes, StoredBytes: storedBytes, PayloadOffset: payloadOffset, PayloadLength: payloadLength, NullCount: nullCount, DefaultCount: defaultCount}, nil
}

func decodeNonNegativeLayoutInt(dec *columnPartImageDecoder, field string) (int, error) {
	value, err := dec.i64()
	if err != nil {
		return 0, err
	}
	return nonNegativeInt64ToInt(value, field)
}
