package typedcolumn

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

const ColumnPartLayoutContractVersion uint16 = 2

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
	OffsetsSection      ColumnPartLayoutContractSection
	ValuesSection       ColumnPartLayoutContractSection
	OffsetsBytes        int
	ValuesBytes         int
	FixedWidthElements  int
	BitsPerElement      int
	BytesPerRow         int
	LogicalBitsPerRow   int
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
	if descriptorRaw == nil {
		return cert, fmt.Errorf("typedcolumn: descriptor bytes are required for layout contract certification")
	}
	if len(descriptorRaw) != descriptorSection.Length {
		return cert, fmt.Errorf("typedcolumn: descriptor bytes=%d want section length=%d", len(descriptorRaw), descriptorSection.Length)
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

type columnPartImageColumnSections struct {
	data    columnPartImageSectionData
	offsets columnPartImageSectionData
	values  columnPartImageSectionData
}

func encodeColumnPartLayoutContract(part *ColumnPart, opts ColumnPartImageOptions, sectionData []columnPartImageSectionData, manifestBytes int) ([]byte, error) {
	if part == nil {
		return nil, fmt.Errorf("typedcolumn: nil part")
	}
	sectionsByColumn := make(map[string]columnPartImageColumnSections, len(part.Descriptor.Columns))
	var descriptorData columnPartImageSectionData
	var dictionaryData columnPartImageSectionData
	for _, section := range sectionData {
		switch section.section.Kind {
		case ColumnPartImageSectionDescriptor:
			descriptorData = section
		case ColumnPartImageSectionDictionaries:
			dictionaryData = section
		case ColumnPartImageSectionColumnData:
			entry := sectionsByColumn[section.section.Column]
			entry.data = section
			sectionsByColumn[section.section.Column] = entry
		case ColumnPartImageSectionColumnOffsets:
			entry := sectionsByColumn[section.section.Column]
			entry.offsets = section
			sectionsByColumn[section.section.Column] = entry
		case ColumnPartImageSectionColumnValues:
			entry := sectionsByColumn[section.section.Column]
			entry.values = section
			sectionsByColumn[section.section.Column] = entry
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
	encodeColumnPartLayoutContractSection(&enc, ColumnPartLayoutContractSection{Offset: descriptorData.section.Offset, Length: descriptorData.payloadLen(), Checksum: descriptorData.payloadChecksum()})
	enc.u32(uint32(len(part.Descriptor.Columns)))
	for _, columnDesc := range part.Descriptor.Columns {
		column, ok := part.Columns[columnDesc.Name]
		if !ok {
			return nil, fmt.Errorf("typedcolumn: layout contract missing part column %s", columnDesc.Name)
		}
		sections, ok := sectionsByColumn[columnDesc.Name]
		if !ok {
			return nil, fmt.Errorf("typedcolumn: layout contract missing data section %s", columnDesc.Name)
		}
		contractColumn, err := buildColumnPartLayoutContractColumn(opts, columnDesc, column, sections, dictionaryData)
		if err != nil {
			return nil, err
		}
		if err := encodeColumnPartLayoutContractColumn(&enc, contractColumn); err != nil {
			return nil, err
		}
	}
	return enc.bytes(), nil
}

func buildColumnPartLayoutContractColumn(opts ColumnPartImageOptions, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn, sections columnPartImageColumnSections, dictionaryData columnPartImageSectionData) (ColumnPartLayoutContractColumn, error) {
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
	sectionEncoding := column.Definition.Encoding
	sectionCompression := column.Definition.Compression
	sectionRows := 0
	var sectionContract ColumnPartLayoutContractSection
	if column.Definition.Encoding == EncodingRawUint32OffsetsList || column.Definition.Encoding == EncodingRawBytesOffsets {
		if sections.offsets.section.Kind != ColumnPartImageSectionColumnOffsets || sections.values.section.Kind != ColumnPartImageSectionColumnValues {
			return ColumnPartLayoutContractColumn{}, fmt.Errorf("typedcolumn: layout contract column %s missing offsets/value sections", columnDesc.Name)
		}
		sectionRows = sections.offsets.section.Rows
		sectionEncoding = sections.offsets.section.Encoding
		sectionCompression = sections.offsets.section.Compression
	} else {
		if sections.data.section.Kind != ColumnPartImageSectionColumnData {
			return ColumnPartLayoutContractColumn{}, fmt.Errorf("typedcolumn: layout contract missing data section %s", columnDesc.Name)
		}
		sectionRows = sections.data.section.Rows
		sectionEncoding = sections.data.section.Encoding
		sectionCompression = sections.data.section.Compression
		sectionContract = ColumnPartLayoutContractSection{Offset: sections.data.section.Offset, Length: sections.data.payloadLen(), Checksum: sections.data.payloadChecksum()}
	}
	contractDef := column.Definition
	contractDef.Encoding = sectionEncoding
	contractDef.Compression = sectionCompression
	layout := physicalColumnLayoutForContract(logicalType, contractDef)
	contract := ColumnPartLayoutContractColumn{
		Name:                columnDesc.Name,
		LogicalType:         logicalType,
		Type:                columnDesc.Type,
		Encoding:            sectionEncoding,
		Compression:         sectionCompression,
		Rows:                sectionRows,
		Section:             sectionContract,
		FixedWidthElements:  column.Definition.FixedWidthElements,
		BitsPerElement:      column.Definition.BitsPerElement,
		BytesPerRow:         layout.bytesPerRow,
		LogicalBitsPerRow:   layout.logicalBitsPerRow,
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
	if column.Definition.Encoding == EncodingRawUint32OffsetsList || column.Definition.Encoding == EncodingRawBytesOffsets {
		contract.OffsetsSection = ColumnPartLayoutContractSection{Offset: sections.offsets.section.Offset, Length: sections.offsets.payloadLen(), Checksum: sections.offsets.payloadChecksum()}
		contract.ValuesSection = ColumnPartLayoutContractSection{Offset: sections.values.section.Offset, Length: sections.values.payloadLen(), Checksum: sections.values.payloadChecksum()}
		contract.OffsetsBytes = sections.offsets.payloadLen()
		contract.ValuesBytes = sections.values.payloadLen()
	}
	if contract.Dictionary && dictionaryData.section.Kind == ColumnPartImageSectionDictionaries {
		contract.DictionarySection = ColumnPartLayoutContractSection{Offset: dictionaryData.section.Offset, Length: dictionaryData.payloadLen(), Checksum: dictionaryData.payloadChecksum()}
	} else if contract.Dictionary {
		contract.StreamingCertified = false
		contract.StatsCertified = false
		contract.PruningCertified = false
	}
	payloadOffset := sections.data.section.Offset
	offsetsList := column.Definition.Encoding == EncodingRawUint32OffsetsList || column.Definition.Encoding == EncodingRawBytesOffsets
	if offsetsList {
		// Offsets-list images store two discontiguous global sections, so the generic
		// per-block combined payload offset/length fields are intentionally empty.
		// Section-level offsets/checksums plus block row spans carry the durable
		// identity until a future contract version adds per-block split ranges.
		payloadOffset = 0
	}
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
		payloadLength := block.Descriptor.StoredBytes
		if offsetsList {
			payloadLength = 0
		}
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
			PayloadLength: payloadLength,
			NullCount:     block.Granule.NullCount,
			DefaultCount:  block.Granule.DefaultCount,
		})
		if !offsetsList {
			payloadOffset += block.Descriptor.StoredBytes
		}
	}
	return contract, nil
}

func (s columnPartImageSectionData) payloadChecksum() uint32 {
	if len(s.chunks) == 0 {
		return crc.Checksum(s.data)
	}
	return crc.ChecksumParts(s.chunks...)
}

type columnPartContractLayout struct {
	elementSize       int
	alignment         int
	endian            ColumnPartLayoutEndian
	lengthMultiple    int
	bytesPerRow       int
	logicalBitsPerRow int
	direct            bool
	streaming         bool
	stats             bool
	pruning           bool
}

func physicalColumnLayoutForContract(logicalType string, def ColumnDefinition) columnPartContractLayout {
	switch def.Encoding {
	case EncodingRawInt64:
		scalarInt64 := logicalType == "int64" && def.Type == ColumnTypeInt64 && def.FixedWidthElements == 0
		direct := def.Compression == CompressionNone && scalarInt64
		streaming := scalarInt64 && !direct
		return columnPartContractLayout{elementSize: 8, alignment: 8, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 8, direct: direct, streaming: streaming, stats: scalarInt64, pruning: scalarInt64}
	case EncodingRawFloat32:
		direct := def.Compression == CompressionNone && logicalType == "float32" && def.Type == ColumnTypeFloat32 && def.FixedWidthElements == 0
		return columnPartContractLayout{elementSize: 4, alignment: 4, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 4, direct: direct}
	case EncodingRawFloat64:
		direct := def.Compression == CompressionNone && logicalType == "double" && def.Type == ColumnTypeFloat64 && def.FixedWidthElements == 0
		return columnPartContractLayout{elementSize: 8, alignment: 8, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 8, direct: direct}
	case EncodingRawInt8, EncodingRawUint8, EncodingRawInt16, EncodingRawUint16, EncodingRawInt32, EncodingRawUint32, EncodingRawUint64, EncodingRawFloat16, EncodingRawBFloat16:
		width := rawScalarWidthForColumnType(def.Type)
		if width == 0 || rawScalarEncodingForColumnType(def.Type) != def.Encoding {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		direct := def.Compression == CompressionNone && logicalType == string(def.Type) && def.FixedWidthElements == 0
		statsPruning := logicalType == string(def.Type) && def.FixedWidthElements == 0 && integerStatsPayloadColumnType(def.Type)
		return columnPartContractLayout{elementSize: width, alignment: width, endian: ColumnPartLayoutEndianLittle, lengthMultiple: width, direct: direct, stats: statsPruning, pruning: statsPruning}
	case EncodingRawFloat32Vector:
		direct := def.Compression == CompressionNone && logicalType == "float32_vector" && def.Type == ColumnTypeFloat32Vector && def.FixedWidthElements > 0
		return columnPartContractLayout{elementSize: 4, alignment: 4, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 4, direct: direct, stats: direct, pruning: direct}
	case EncodingRawUint8Vector, EncodingRawInt8Vector, EncodingRawUint16Vector, EncodingRawInt16Vector, EncodingRawUint32Vector, EncodingRawInt32Vector, EncodingRawUint64Vector, EncodingRawInt64Vector, EncodingRawFloat16Vector, EncodingRawBFloat16Vector, EncodingRawFloat64Vector:
		width, ok := DenseFixedWidthVectorElementWidth(def.Type)
		wantEncoding, encOK := DenseFixedWidthVectorEncoding(def.Type)
		if !ok || !encOK || def.Encoding != wantEncoding || def.FixedWidthElements <= 0 {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		bytesPerRow, err := checkedMulInt(def.FixedWidthElements, width, "layout contract dense vector row bytes")
		if err != nil {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		logicalBitsPerRow, err := checkedMulInt(bytesPerRow, 8, "layout contract dense vector logical bits")
		if err != nil {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		direct := def.Compression == CompressionNone && logicalType == string(def.Type)
		return columnPartContractLayout{elementSize: width, alignment: width, endian: ColumnPartLayoutEndianLittle, lengthMultiple: width, bytesPerRow: bytesPerRow, logicalBitsPerRow: logicalBitsPerRow, direct: direct}
	case EncodingRawFixedBytes:
		if def.Type != ColumnTypeFixedBytes || def.FixedWidthElements <= 0 || def.BitsPerElement != 0 {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		logicalBitsPerRow, err := checkedMulInt(def.FixedWidthElements, 8, "layout contract fixed-bytes logical bits")
		if err != nil {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		direct := def.Compression == CompressionNone && logicalType == "byte_vector"
		return columnPartContractLayout{elementSize: 1, alignment: 1, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 1, bytesPerRow: def.FixedWidthElements, logicalBitsPerRow: logicalBitsPerRow, direct: direct}
	case EncodingRawPackedBitVector, EncodingRawPackedUint2Vector, EncodingRawPackedUint4Vector:
		bitsPerElement, ok := PackedUintEncodingBits(def.Encoding)
		wantEncoding, encOK := PackedUintVectorEncoding(def.Type)
		if !ok || !encOK || def.Encoding != wantEncoding || def.FixedWidthElements <= 0 || def.BitsPerElement != bitsPerElement {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		rowBytes, err := PackedUintRowBytes(def.FixedWidthElements, bitsPerElement)
		if err != nil {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		logicalBitsPerRow, err := checkedMulInt(def.FixedWidthElements, bitsPerElement, "layout contract packed logical bits")
		if err != nil {
			return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
		}
		direct := def.Compression == CompressionNone && logicalType == string(def.Type)
		return columnPartContractLayout{elementSize: 1, alignment: 1, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 1, bytesPerRow: rowBytes, logicalBitsPerRow: logicalBitsPerRow, direct: direct}
	case EncodingRawUint32Dense:
		// adjacency_list payload bytes are little-endian dense uint32, but certified
		// adjacency direct views are deferred to #1901 for the active #1886 stack.
		return columnPartContractLayout{elementSize: 4, alignment: 4, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 4}
	case EncodingRawUint32OffsetsList:
		// The primitive is sectioned into uint64 offsets and uint32 values. The
		// shared element identity is the value width; direct-view validation checks
		// offsets-section (8-byte) and values-section (4-byte) alignment separately.
		direct := def.Compression == CompressionNone && def.FixedWidthElements == 0 && ((logicalType == "uint32_list" && def.Type == ColumnTypeUint32List) || (logicalType == "adjacency_list" && def.Type == ColumnTypeAdjacencyList))
		return columnPartContractLayout{elementSize: 4, alignment: 4, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 4, direct: direct}
	case EncodingRawBytesOffsets:
		// The bytes primitive is sectioned into uint64 offsets and uninterpreted
		// payload bytes. Direct-view validation checks offsets-section alignment;
		// values are byte-aligned and never reinterpreted as text.
		direct := def.Compression == CompressionNone && def.FixedWidthElements == 0 && logicalType == "bytes" && def.Type == ColumnTypeBytes
		return columnPartContractLayout{elementSize: 1, alignment: 1, endian: ColumnPartLayoutEndianLittle, lengthMultiple: 1, direct: direct}
	case EncodingDeltaVarint, EncodingDoubleDeltaVarint:
		streaming := logicalType == "int64"
		statsPruning := streaming
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined, streaming: streaming, stats: statsPruning, pruning: statsPruning}
	case EncodingBoolBitpackRLE:
		streaming := logicalType == "bool"
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined, streaming: streaming, stats: streaming && def.Compression == CompressionNone}
	case EncodingLowCardinalityUint32:
		streaming := logicalType == "string"
		statsPruning := streaming && def.Compression == CompressionNone
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined, streaming: streaming, stats: statsPruning, pruning: statsPruning}
	case EncodingNullableInt64:
		return columnPartContractLayout{endian: ColumnPartLayoutEndianCodecDefined}
	default:
		return columnPartContractLayout{endian: ColumnPartLayoutEndianNone}
	}
}

func certifyLayoutContractColumn(image ColumnPartImage, desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn, contract ColumnPartLayoutContractColumn) error {
	if contract.Encoding == EncodingRawUint32OffsetsList || column.Definition.Encoding == EncodingRawUint32OffsetsList {
		return certifyLayoutContractOffsetsListColumn(image, desc, columnDesc, column, contract)
	}
	if contract.Encoding == EncodingRawBytesOffsets || column.Definition.Encoding == EncodingRawBytesOffsets {
		return certifyLayoutContractBytesColumn(image, desc, columnDesc, column, contract)
	}
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
	if contract.BitsPerElement != column.Definition.BitsPerElement || contract.BitsPerElement != columnDesc.BitsPerElement {
		return fmt.Errorf("typedcolumn: layout contract column %s bits_per_element=%d want %d", columnDesc.Name, contract.BitsPerElement, column.Definition.BitsPerElement)
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
	if contract.ElementSize != expectedLayout.elementSize || contract.Alignment != expectedLayout.alignment || contract.Endian != expectedLayout.endian || contract.LengthMultiple != expectedLayout.lengthMultiple || contract.BytesPerRow != expectedLayout.bytesPerRow || contract.LogicalBitsPerRow != expectedLayout.logicalBitsPerRow {
		return fmt.Errorf("typedcolumn: layout contract column %s element/alignment/endian/length/row=(%d,%d,%s,%d,%d,%d) want (%d,%d,%s,%d,%d,%d)", columnDesc.Name, contract.ElementSize, contract.Alignment, contract.Endian, contract.LengthMultiple, contract.BytesPerRow, contract.LogicalBitsPerRow, expectedLayout.elementSize, expectedLayout.alignment, expectedLayout.endian, expectedLayout.lengthMultiple, expectedLayout.bytesPerRow, expectedLayout.logicalBitsPerRow)
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
	if contract.DirectViewCertified {
		if nullCount != 0 || defaultCount != 0 || contract.NullMaskPresent || contract.DefaultMaskPresent {
			return fmt.Errorf("typedcolumn: layout contract column %s direct-view null/default counts=(%d,%d) mask_flags=(%v,%v)", columnDesc.Name, nullCount, defaultCount, contract.NullMaskPresent, contract.DefaultMaskPresent)
		}
	}
	return nil
}

func certifyLayoutContractOffsetsListColumn(image ColumnPartImage, desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn, contract ColumnPartLayoutContractColumn) error {
	offsetsSection, valuesSection, ok := image.columnOffsetsListSections(columnDesc.Name)
	if !ok {
		return fmt.Errorf("typedcolumn: layout contract column %s missing offsets-list image sections", columnDesc.Name)
	}
	if contract.Type != columnDesc.Type || contract.Type != column.Definition.Type || (contract.Type != ColumnTypeUint32List && contract.Type != ColumnTypeAdjacencyList) {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list type=%s descriptor=%s definition=%s", columnDesc.Name, contract.Type, columnDesc.Type, column.Definition.Type)
	}
	if contract.Encoding != EncodingRawUint32OffsetsList || column.Definition.Encoding != EncodingRawUint32OffsetsList || offsetsSection.Encoding != EncodingRawUint32OffsetsList || valuesSection.Encoding != EncodingRawUint32OffsetsList {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list encoding contract=%s definition=%s offsets=%s values=%s", columnDesc.Name, contract.Encoding, column.Definition.Encoding, offsetsSection.Encoding, valuesSection.Encoding)
	}
	if contract.Compression != CompressionNone || column.Definition.Compression != CompressionNone || offsetsSection.Compression != CompressionNone || valuesSection.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list compression contract=%s definition=%s offsets=%s values=%s", columnDesc.Name, contract.Compression, column.Definition.Compression, offsetsSection.Compression, valuesSection.Compression)
	}
	if contract.FixedWidthElements != 0 || column.Definition.FixedWidthElements != 0 || columnDesc.FixedWidthElements != 0 {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list fixed_width_elements=(%d,%d,%d) want 0", columnDesc.Name, contract.FixedWidthElements, column.Definition.FixedWidthElements, columnDesc.FixedWidthElements)
	}
	if contract.Rows != desc.RowCount || offsetsSection.Rows != desc.RowCount || valuesSection.Rows != desc.RowCount {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list rows=%d offsets_rows=%d values_rows=%d descriptor_rows=%d", columnDesc.Name, contract.Rows, offsetsSection.Rows, valuesSection.Rows, desc.RowCount)
	}
	expectedLayout := physicalColumnLayoutForContract(contract.LogicalType, column.Definition)
	if contract.ElementSize != expectedLayout.elementSize || contract.Alignment != expectedLayout.alignment || contract.Endian != expectedLayout.endian || contract.LengthMultiple != expectedLayout.lengthMultiple {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list element/alignment/endian/length=(%d,%d,%s,%d) want (%d,%d,%s,%d)", columnDesc.Name, contract.ElementSize, contract.Alignment, contract.Endian, contract.LengthMultiple, expectedLayout.elementSize, expectedLayout.alignment, expectedLayout.endian, expectedLayout.lengthMultiple)
	}
	if contract.DirectViewCertified != expectedLayout.direct || contract.StreamingCertified != expectedLayout.streaming || contract.StatsCertified != expectedLayout.stats || contract.PruningCertified != expectedLayout.pruning {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list capability flags direct=%v streaming=%v stats=%v pruning=%v want direct=%v streaming=%v stats=%v pruning=%v", columnDesc.Name, contract.DirectViewCertified, contract.StreamingCertified, contract.StatsCertified, contract.PruningCertified, expectedLayout.direct, expectedLayout.streaming, expectedLayout.stats, expectedLayout.pruning)
	}
	if contract.Dictionary || contract.DictionarySection != (ColumnPartLayoutContractSection{}) {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list dictionary metadata present", columnDesc.Name)
	}
	if err := certifyLayoutContractSection("column "+columnDesc.Name+" offsets", contract.OffsetsSection, offsetsSection, image.sectionBytes(offsetsSection)); err != nil {
		return err
	}
	if err := certifyLayoutContractSection("column "+columnDesc.Name+" values", contract.ValuesSection, valuesSection, image.sectionBytes(valuesSection)); err != nil {
		return err
	}
	if contract.OffsetsBytes != offsetsSection.Length || contract.ValuesBytes != valuesSection.Length {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list bytes offsets=%d/%d values=%d/%d", columnDesc.Name, contract.OffsetsBytes, offsetsSection.Length, contract.ValuesBytes, valuesSection.Length)
	}
	if len(contract.Blocks) != len(column.Blocks) || len(contract.Blocks) != len(columnDesc.Blocks) {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list blocks=%d descriptor=%d", columnDesc.Name, len(contract.Blocks), len(column.Blocks))
	}
	expectedOffsetCount, err := checkedAddInt(desc.RowCount, 1, "layout contract offsets-list global offset count")
	if err != nil {
		return err
	}
	expectedOffsetsBytes, err := checkedMulInt(expectedOffsetCount, 8, "layout contract offsets-list global offsets bytes")
	if err != nil {
		return err
	}
	valuesBytes := 0
	nullCount := 0
	defaultCount := 0
	for i, block := range column.Blocks {
		certified := contract.Blocks[i]
		want := block.Descriptor
		if certified.FirstRow != want.FirstRow || certified.RowCount != want.RowCount || certified.FirstGranule != want.FirstGranule || certified.LastGranule != want.LastGranule {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d row span=(%d,%d,%d,%d) want (%d,%d,%d,%d)", columnDesc.Name, i, certified.FirstRow, certified.RowCount, certified.FirstGranule, certified.LastGranule, want.FirstRow, want.RowCount, want.FirstGranule, want.LastGranule)
		}
		if certified.Encoding != want.Encoding || certified.Compression != want.Compression || certified.RawBytes != want.RawBytes || certified.StoredBytes != want.StoredBytes {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d encoding/compression/raw/stored=(%s,%s,%d,%d) want (%s,%s,%d,%d)", columnDesc.Name, i, certified.Encoding, certified.Compression, certified.RawBytes, certified.StoredBytes, want.Encoding, want.Compression, want.RawBytes, want.StoredBytes)
		}
		if certified.PayloadOffset != 0 || certified.PayloadLength != 0 {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d offsets-list payload offset/length=(%d,%d) want (0,0)", columnDesc.Name, i, certified.PayloadOffset, certified.PayloadLength)
		}
		_, blockValuesBytes, err := RawUint32OffsetsListBlockPayloadBytes(block.Descriptor.RowCount, block.Descriptor.StoredBytes)
		if err != nil {
			return err
		}
		valuesBytes += blockValuesBytes
		nullCount += block.Granule.NullCount
		defaultCount += block.Granule.DefaultCount
		if certified.NullCount != block.Granule.NullCount || certified.DefaultCount != block.Granule.DefaultCount {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d null/default=(%d,%d) want (%d,%d)", columnDesc.Name, i, certified.NullCount, certified.DefaultCount, block.Granule.NullCount, block.Granule.DefaultCount)
		}
	}
	if expectedOffsetsBytes != offsetsSection.Length || valuesBytes != valuesSection.Length {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list consumed offsets=%d/%d values=%d/%d", columnDesc.Name, expectedOffsetsBytes, offsetsSection.Length, valuesBytes, valuesSection.Length)
	}
	if contract.NullCount != nullCount || contract.DefaultCount != defaultCount || contract.NullMaskPresent || contract.DefaultMaskPresent || nullCount != 0 || defaultCount != 0 {
		return fmt.Errorf("typedcolumn: layout contract column %s offsets-list null/default counts=(%d,%d) flags=(%v,%v)", columnDesc.Name, contract.NullCount, contract.DefaultCount, contract.NullMaskPresent, contract.DefaultMaskPresent)
	}
	if contract.DirectViewCertified {
		if contract.Endian != ColumnPartLayoutEndianLittle || contract.Compression != CompressionNone {
			return fmt.Errorf("typedcolumn: layout contract column %s offsets-list direct-view endian/compression=(%s,%s)", columnDesc.Name, contract.Endian, contract.Compression)
		}
		if offsetsSection.Offset%8 != 0 || valuesSection.Offset%4 != 0 {
			return fmt.Errorf("typedcolumn: layout contract column %s offsets-list direct-view alignment offsets=%d values=%d", columnDesc.Name, offsetsSection.Offset, valuesSection.Offset)
		}
		if offsetsSection.Length != expectedOffsetsBytes || valuesSection.Length%4 != 0 {
			return fmt.Errorf("typedcolumn: layout contract column %s offsets-list direct-view length offsets=%d/%d values=%d", columnDesc.Name, offsetsSection.Length, expectedOffsetsBytes, valuesSection.Length)
		}
	}
	return nil
}

func certifyLayoutContractBytesColumn(image ColumnPartImage, desc ColumnPartDescriptor, columnDesc ColumnPartColumnDescriptor, column ColumnPartColumn, contract ColumnPartLayoutContractColumn) error {
	offsetsSection, valuesSection, ok := image.columnOffsetsListSections(columnDesc.Name)
	if !ok {
		return fmt.Errorf("typedcolumn: layout contract column %s missing bytes image sections", columnDesc.Name)
	}
	if contract.Type != columnDesc.Type || contract.Type != column.Definition.Type || contract.Type != ColumnTypeBytes {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes type=%s descriptor=%s definition=%s", columnDesc.Name, contract.Type, columnDesc.Type, column.Definition.Type)
	}
	if contract.Encoding != EncodingRawBytesOffsets || column.Definition.Encoding != EncodingRawBytesOffsets || offsetsSection.Encoding != EncodingRawBytesOffsets || valuesSection.Encoding != EncodingRawBytesOffsets {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes encoding contract=%s definition=%s offsets=%s values=%s", columnDesc.Name, contract.Encoding, column.Definition.Encoding, offsetsSection.Encoding, valuesSection.Encoding)
	}
	if contract.Compression != CompressionNone || column.Definition.Compression != CompressionNone || offsetsSection.Compression != CompressionNone || valuesSection.Compression != CompressionNone {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes compression contract=%s definition=%s offsets=%s values=%s", columnDesc.Name, contract.Compression, column.Definition.Compression, offsetsSection.Compression, valuesSection.Compression)
	}
	if contract.FixedWidthElements != 0 || column.Definition.FixedWidthElements != 0 || columnDesc.FixedWidthElements != 0 {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes fixed_width_elements=(%d,%d,%d) want 0", columnDesc.Name, contract.FixedWidthElements, column.Definition.FixedWidthElements, columnDesc.FixedWidthElements)
	}
	if contract.Rows != desc.RowCount || offsetsSection.Rows != desc.RowCount || valuesSection.Rows != desc.RowCount {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes rows=%d offsets_rows=%d values_rows=%d descriptor_rows=%d", columnDesc.Name, contract.Rows, offsetsSection.Rows, valuesSection.Rows, desc.RowCount)
	}
	expectedLayout := physicalColumnLayoutForContract(contract.LogicalType, column.Definition)
	if contract.ElementSize != expectedLayout.elementSize || contract.Alignment != expectedLayout.alignment || contract.Endian != expectedLayout.endian || contract.LengthMultiple != expectedLayout.lengthMultiple {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes element/alignment/endian/length=(%d,%d,%s,%d) want (%d,%d,%s,%d)", columnDesc.Name, contract.ElementSize, contract.Alignment, contract.Endian, contract.LengthMultiple, expectedLayout.elementSize, expectedLayout.alignment, expectedLayout.endian, expectedLayout.lengthMultiple)
	}
	if contract.DirectViewCertified != expectedLayout.direct || contract.StreamingCertified != expectedLayout.streaming || contract.StatsCertified != expectedLayout.stats || contract.PruningCertified != expectedLayout.pruning {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes capability flags direct=%v streaming=%v stats=%v pruning=%v want direct=%v streaming=%v stats=%v pruning=%v", columnDesc.Name, contract.DirectViewCertified, contract.StreamingCertified, contract.StatsCertified, contract.PruningCertified, expectedLayout.direct, expectedLayout.streaming, expectedLayout.stats, expectedLayout.pruning)
	}
	if contract.Dictionary || contract.DictionarySection != (ColumnPartLayoutContractSection{}) {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes dictionary metadata present", columnDesc.Name)
	}
	if err := certifyLayoutContractSection("column "+columnDesc.Name+" offsets", contract.OffsetsSection, offsetsSection, image.sectionBytes(offsetsSection)); err != nil {
		return err
	}
	if err := certifyLayoutContractSection("column "+columnDesc.Name+" values", contract.ValuesSection, valuesSection, image.sectionBytes(valuesSection)); err != nil {
		return err
	}
	if contract.OffsetsBytes != offsetsSection.Length || contract.ValuesBytes != valuesSection.Length {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes bytes offsets=%d/%d values=%d/%d", columnDesc.Name, contract.OffsetsBytes, offsetsSection.Length, contract.ValuesBytes, valuesSection.Length)
	}
	if len(contract.Blocks) != len(column.Blocks) || len(contract.Blocks) != len(columnDesc.Blocks) {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes blocks=%d descriptor=%d", columnDesc.Name, len(contract.Blocks), len(column.Blocks))
	}
	expectedOffsetCount, err := checkedAddInt(desc.RowCount, 1, "layout contract bytes global offset count")
	if err != nil {
		return err
	}
	expectedOffsetsBytes, err := checkedMulInt(expectedOffsetCount, 8, "layout contract bytes global offsets bytes")
	if err != nil {
		return err
	}
	valuesBytes := 0
	nullCount := 0
	defaultCount := 0
	for i, block := range column.Blocks {
		certified := contract.Blocks[i]
		want := block.Descriptor
		if certified.FirstRow != want.FirstRow || certified.RowCount != want.RowCount || certified.FirstGranule != want.FirstGranule || certified.LastGranule != want.LastGranule {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d row span=(%d,%d,%d,%d) want (%d,%d,%d,%d)", columnDesc.Name, i, certified.FirstRow, certified.RowCount, certified.FirstGranule, certified.LastGranule, want.FirstRow, want.RowCount, want.FirstGranule, want.LastGranule)
		}
		if certified.Encoding != want.Encoding || certified.Compression != want.Compression || certified.RawBytes != want.RawBytes || certified.StoredBytes != want.StoredBytes {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d encoding/compression/raw/stored=(%s,%s,%d,%d) want (%s,%s,%d,%d)", columnDesc.Name, i, certified.Encoding, certified.Compression, certified.RawBytes, certified.StoredBytes, want.Encoding, want.Compression, want.RawBytes, want.StoredBytes)
		}
		if certified.PayloadOffset != 0 || certified.PayloadLength != 0 {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d bytes payload offset/length=(%d,%d) want (0,0)", columnDesc.Name, i, certified.PayloadOffset, certified.PayloadLength)
		}
		_, blockValuesBytes, err := RawBytesOffsetsBlockPayloadBytes(block.Descriptor.RowCount, block.Descriptor.StoredBytes)
		if err != nil {
			return err
		}
		valuesBytes += blockValuesBytes
		nullCount += block.Granule.NullCount
		defaultCount += block.Granule.DefaultCount
		if certified.NullCount != block.Granule.NullCount || certified.DefaultCount != block.Granule.DefaultCount {
			return fmt.Errorf("typedcolumn: layout contract column %s block %d null/default=(%d,%d) want (%d,%d)", columnDesc.Name, i, certified.NullCount, certified.DefaultCount, block.Granule.NullCount, block.Granule.DefaultCount)
		}
	}
	if expectedOffsetsBytes != offsetsSection.Length || valuesBytes != valuesSection.Length {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes consumed offsets=%d/%d values=%d/%d", columnDesc.Name, expectedOffsetsBytes, offsetsSection.Length, valuesBytes, valuesSection.Length)
	}
	if contract.NullCount != nullCount || contract.DefaultCount != defaultCount || contract.NullMaskPresent || contract.DefaultMaskPresent || nullCount != 0 || defaultCount != 0 {
		return fmt.Errorf("typedcolumn: layout contract column %s bytes null/default counts=(%d,%d) flags=(%v,%v)", columnDesc.Name, contract.NullCount, contract.DefaultCount, contract.NullMaskPresent, contract.DefaultMaskPresent)
	}
	if contract.DirectViewCertified {
		if contract.Endian != ColumnPartLayoutEndianLittle || contract.Compression != CompressionNone {
			return fmt.Errorf("typedcolumn: layout contract column %s bytes direct-view endian/compression=(%s,%s)", columnDesc.Name, contract.Endian, contract.Compression)
		}
		if offsetsSection.Offset%8 != 0 {
			return fmt.Errorf("typedcolumn: layout contract column %s bytes direct-view offsets alignment=%d", columnDesc.Name, offsetsSection.Offset)
		}
		if offsetsSection.Length != expectedOffsetsBytes {
			return fmt.Errorf("typedcolumn: layout contract column %s bytes direct-view length offsets=%d/%d values=%d", columnDesc.Name, offsetsSection.Length, expectedOffsetsBytes, valuesSection.Length)
		}
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
		wantBytes := 0
		var err error
		if contract.BytesPerRow > 0 {
			wantBytes, err = checkedMulInt(certified.RowCount, contract.BytesPerRow, "layout contract fixed-width row bytes")
			if err != nil {
				return err
			}
		} else {
			elementsPerRow := contract.FixedWidthElements
			if elementsPerRow == 0 {
				elementsPerRow = 1
			}
			wantBytes, err = checkedMulInt(certified.RowCount, elementsPerRow, "layout contract fixed-width elements")
			if err != nil {
				return err
			}
			wantBytes, err = checkedMulInt(wantBytes, contract.ElementSize, "layout contract fixed-width bytes")
			if err != nil {
				return err
			}
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

func encodeColumnPartLayoutContractColumn(enc *columnPartImageEncoder, column ColumnPartLayoutContractColumn) error {
	enc.str(column.Name)
	enc.str(column.LogicalType)
	columnTypeCode, err := columnTypeCode(column.Type)
	if err != nil {
		return err
	}
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
	if column.Encoding == EncodingRawUint32OffsetsList || column.Encoding == EncodingRawBytesOffsets {
		encodeColumnPartLayoutContractSection(enc, column.OffsetsSection)
		encodeColumnPartLayoutContractSection(enc, column.ValuesSection)
		enc.i64(int64(column.OffsetsBytes))
		enc.i64(int64(column.ValuesBytes))
	}
	enc.i64(int64(column.FixedWidthElements))
	enc.i64(int64(column.BitsPerElement))
	enc.i64(int64(column.BytesPerRow))
	enc.i64(int64(column.LogicalBitsPerRow))
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
	return nil
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
	var offsetsSection ColumnPartLayoutContractSection
	var valuesSection ColumnPartLayoutContractSection
	var offsetsBytes int
	var valuesBytes int
	if Encoding(encoding) == EncodingRawUint32OffsetsList || Encoding(encoding) == EncodingRawBytesOffsets {
		offsetsSection, err = decodeColumnPartLayoutContractSection(dec)
		if err != nil {
			return ColumnPartLayoutContractColumn{}, err
		}
		valuesSection, err = decodeColumnPartLayoutContractSection(dec)
		if err != nil {
			return ColumnPartLayoutContractColumn{}, err
		}
		offsetsBytes, err = decodeNonNegativeLayoutInt(dec, "layout contract offsets bytes")
		if err != nil {
			return ColumnPartLayoutContractColumn{}, err
		}
		valuesBytes, err = decodeNonNegativeLayoutInt(dec, "layout contract values bytes")
		if err != nil {
			return ColumnPartLayoutContractColumn{}, err
		}
	}
	fixedWidthElements, err := decodeNonNegativeLayoutInt(dec, "layout contract fixed-width elements")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	bitsPerElement, err := decodeNonNegativeLayoutInt(dec, "layout contract bits per element")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	bytesPerRow, err := decodeNonNegativeLayoutInt(dec, "layout contract bytes per row")
	if err != nil {
		return ColumnPartLayoutContractColumn{}, err
	}
	logicalBitsPerRow, err := decodeNonNegativeLayoutInt(dec, "layout contract logical bits per row")
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
		OffsetsSection:      offsetsSection,
		ValuesSection:       valuesSection,
		OffsetsBytes:        offsetsBytes,
		ValuesBytes:         valuesBytes,
		FixedWidthElements:  fixedWidthElements,
		BitsPerElement:      bitsPerElement,
		BytesPerRow:         bytesPerRow,
		LogicalBitsPerRow:   logicalBitsPerRow,
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
