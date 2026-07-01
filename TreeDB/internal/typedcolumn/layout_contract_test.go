package typedcolumn

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

func TestColumnPartImageSectionDataChunkedPayloadHelpers(t *testing.T) {
	chunks := [][]byte{
		[]byte("alpha"),
		[]byte(""),
		[]byte("beta"),
		[]byte("gamma"),
	}
	section := columnPartImageSectionData{chunks: chunks}
	if got, want := section.payloadLen(), len("alphabetagamma"); got != want {
		t.Fatalf("payloadLen=%d want %d", got, want)
	}
	got := section.appendPayloadTo([]byte("prefix:"))
	want := []byte("prefix:alphabetagamma")
	if string(got) != string(want) {
		t.Fatalf("appendPayloadTo=%q want %q", got, want)
	}
	if got, want := section.payloadChecksum(), crc.Checksum([]byte("alphabetagamma")); got != want {
		t.Fatalf("payloadChecksum=%08x want %08x", got, want)
	}
}

func TestColumnPartLayoutContractCertifiesAlignedFixedWidthSections(t *testing.T) {
	image := mustLayoutContractFixedWidthImage(t)
	cert, err := CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	for _, tc := range []struct {
		name               string
		logicalType        string
		columnType         ColumnType
		encoding           Encoding
		elementSize        int
		alignment          int
		lengthMultiple     int
		fixedWidthElements int
		sectionLength      int
		blockLengths       []int
	}{
		{name: "id", logicalType: "int64", columnType: ColumnTypeInt64, encoding: EncodingRawInt64, elementSize: 8, alignment: 8, lengthMultiple: 8, sectionLength: 24, blockLengths: []int{16, 8}},
		{name: "value", logicalType: "int64", columnType: ColumnTypeInt64, encoding: EncodingRawInt64, elementSize: 8, alignment: 8, lengthMultiple: 8, sectionLength: 24, blockLengths: []int{16, 8}},
		{name: "score32", logicalType: "float32", columnType: ColumnTypeFloat32, encoding: EncodingRawFloat32, elementSize: 4, alignment: 4, lengthMultiple: 4, sectionLength: 12, blockLengths: []int{8, 4}},
		{name: "score64", logicalType: "double", columnType: ColumnTypeFloat64, encoding: EncodingRawFloat64, elementSize: 8, alignment: 8, lengthMultiple: 8, sectionLength: 24, blockLengths: []int{16, 8}},
		{name: "embedding", logicalType: "float32_vector", columnType: ColumnTypeFloat32Vector, encoding: EncodingRawFloat32Vector, elementSize: 4, alignment: 4, lengthMultiple: 4, fixedWidthElements: 3, sectionLength: 36, blockLengths: []int{24, 12}},
	} {
		assertLayoutContractDirectColumn(t, image, cert, tc.name, tc.logicalType, tc.columnType, tc.encoding, tc.elementSize, tc.alignment, tc.lengthMultiple, tc.fixedWidthElements, tc.sectionLength, tc.blockLengths)
	}
	neighbors, ok := cert.Column("neighbors")
	if !ok {
		t.Fatalf("missing neighbors column")
	}
	if neighbors.DirectViewCertified || neighbors.Endian != ColumnPartLayoutEndianLittle || neighbors.ElementSize != 4 || neighbors.Alignment != 4 {
		t.Fatalf("neighbors contract=%+v want little-endian payload metadata but no direct-view certification", neighbors)
	}
	if cert.DirectViewCertified != 5 {
		t.Fatalf("direct-view certified=%d want exactly 5 active fixed-width columns", cert.DirectViewCertified)
	}
}

func assertLayoutContractDirectColumn(t *testing.T, image ColumnPartImage, cert ColumnPartLayoutCertification, name string, logicalType string, columnType ColumnType, encoding Encoding, elementSize int, alignment int, lengthMultiple int, fixedWidthElements int, sectionLength int, blockLengths []int) {
	t.Helper()
	column, ok := cert.Column(name)
	if !ok {
		t.Fatalf("missing certified column %q", name)
	}
	if !column.DirectViewCertified {
		t.Fatalf("column %q contract=%+v want direct-view certified", name, column)
	}
	if column.LogicalType != logicalType || column.Type != columnType || column.Encoding != encoding || column.Compression != CompressionNone {
		t.Fatalf("column %q identity=(%q,%s,%s,%s) want (%q,%s,%s,%s)", name, column.LogicalType, column.Type, column.Encoding, column.Compression, logicalType, columnType, encoding, CompressionNone)
	}
	if column.Rows != image.Rows || column.FixedWidthElements != fixedWidthElements || column.ElementSize != elementSize || column.Alignment != alignment || column.Endian != ColumnPartLayoutEndianLittle || column.LengthMultiple != lengthMultiple {
		t.Fatalf("column %q layout rows/fixed/elem/align/endian/multiple=(%d,%d,%d,%d,%s,%d) want (%d,%d,%d,%d,%s,%d)", name, column.Rows, column.FixedWidthElements, column.ElementSize, column.Alignment, column.Endian, column.LengthMultiple, image.Rows, fixedWidthElements, elementSize, alignment, ColumnPartLayoutEndianLittle, lengthMultiple)
	}
	if column.NullMaskPresent || column.DefaultMaskPresent || column.NullCount != 0 || column.DefaultCount != 0 {
		t.Fatalf("column %q null/default contract=%+v want non-null/non-default direct view", name, column)
	}
	section, ok := image.columnDataSection(name)
	if !ok {
		t.Fatalf("missing image section for column %q", name)
	}
	if section.Offset%alignment != 0 || column.Section.Offset != section.Offset || column.Section.Length != section.Length || column.Section.Length != sectionLength || section.Rows != image.Rows || section.Encoding != encoding || section.Compression != CompressionNone {
		t.Fatalf("column %q section contract=(%d,%d) image=(%d,%d rows=%d enc=%s comp=%s) want length=%d align=%d", name, column.Section.Offset, column.Section.Length, section.Offset, section.Length, section.Rows, section.Encoding, section.Compression, sectionLength, alignment)
	}
	if len(column.Blocks) != len(blockLengths) {
		t.Fatalf("column %q blocks=%d want %d", name, len(column.Blocks), len(blockLengths))
	}
	offset := section.Offset
	firstRow := 0
	for i, wantLength := range blockLengths {
		block := column.Blocks[i]
		wantRows := wantLength / elementSize
		if fixedWidthElements > 0 {
			wantRows /= fixedWidthElements
		}
		if block.FirstRow != firstRow || block.RowCount != wantRows || block.Encoding != encoding || block.Compression != CompressionNone || block.RawBytes != wantLength || block.StoredBytes != wantLength || block.PayloadOffset != offset || block.PayloadLength != wantLength || block.NullCount != 0 || block.DefaultCount != 0 {
			t.Fatalf("column %q block %d=%+v want first_row=%d rows=%d encoding=%s payload=(%d,%d) bytes=%d", name, i, block, firstRow, wantRows, encoding, offset, wantLength, wantLength)
		}
		if block.PayloadOffset%alignment != 0 || block.PayloadLength%lengthMultiple != 0 {
			t.Fatalf("column %q block %d payload offset/length=(%d,%d) alignment=%d multiple=%d", name, i, block.PayloadOffset, block.PayloadLength, alignment, lengthMultiple)
		}
		offset += wantLength
		firstRow += wantRows
	}
	if offset != section.Offset+section.Length || firstRow != image.Rows {
		t.Fatalf("column %q consumed offset=%d rows=%d want offset=%d rows=%d", name, offset, firstRow, section.Offset+section.Length, image.Rows)
	}
}

func TestColumnPartLayoutContractMissingLogicalTypeDoesNotCertifyFastPath(t *testing.T) {
	image, err := BuildColumnPartImage(mustLayoutContractFixedWidthPart(t), ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage without logical types: %v", err)
	}
	cert, err := CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	value, ok := cert.Column("value")
	if !ok {
		t.Fatalf("missing value column")
	}
	if value.DirectViewCertified || cert.DirectViewCertified != 0 {
		t.Fatalf("missing logical type certified direct view: column=%+v total=%d", value, cert.DirectViewCertified)
	}
}

func TestColumnPartLayoutContractCorruptionFailsClosed(t *testing.T) {
	image := mustLayoutContractFixedWidthImage(t)
	cases := []struct {
		name string
		edit func(*ColumnPartLayoutContract)
		want string
	}{
		{
			name: "row_count",
			edit: func(c *ColumnPartLayoutContract) { c.Rows++ },
			want: "rows",
		},
		{
			name: "descriptor_checksum",
			edit: func(c *ColumnPartLayoutContract) { c.Descriptor.Checksum ^= 0xfeedface },
			want: "checksum",
		},
		{
			name: "section_length",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "value")
				column.Section.Length--
			},
			want: "section offset/length",
		},
		{
			name: "payload_offset",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "value")
				column.Blocks[0].PayloadOffset++
			},
			want: "payload offset/length",
		},
		{
			name: "direct_flag",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "score32")
				column.DirectViewCertified = false
			},
			want: "capability flags",
		},
		{
			name: "encoding",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "value")
				column.Encoding = EncodingDeltaVarint
			},
			want: "encoding",
		},
		{
			name: "compression",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "value")
				column.Compression = CompressionSnappy
			},
			want: "compression",
		},
		{
			name: "endian",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "value")
				column.Endian = ColumnPartLayoutEndianCodecDefined
			},
			want: "endian",
		},
		{
			name: "element_size",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "score32")
				column.ElementSize = 8
			},
			want: "element/alignment/endian/length",
		},
		{
			name: "length_multiple",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "score32")
				column.LengthMultiple = 8
			},
			want: "element/alignment/endian/length",
		},
		{
			name: "alignment",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "value")
				column.Alignment = 4
			},
			want: "alignment",
		},
		{
			name: "block_row_count",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "score64")
				column.Blocks[0].RowCount++
			},
			want: "row span",
		},
		{
			name: "null_mask_flag",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "score64")
				column.NullMaskPresent = true
			},
			want: "null/default mask flags",
		},
		{
			name: "fixed_width_elements",
			edit: func(c *ColumnPartLayoutContract) {
				column := mustLayoutContractColumnPtr(t, c, "embedding")
				column.FixedWidthElements++
			},
			want: "fixed_width_elements",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := cloneColumnPartImageBytes(image)
			contract := mustLayoutContractFromImage(t, corrupt)
			tc.edit(&contract)
			replaceLayoutContract(t, &corrupt, contract)
			_, err := CertifyColumnPartLayoutContractFromImage(corrupt)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("certify err=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestPhysicalColumnLayoutForContractOverflowFailsClosed1931(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	for _, tc := range []struct {
		name        string
		logicalType string
		def         ColumnDefinition
	}{
		{
			name:        "dense_vector_bytes_per_row",
			logicalType: string(ColumnTypeUint64Vector),
			def: ColumnDefinition{
				Name:               "codes64",
				Type:               ColumnTypeUint64Vector,
				Encoding:           EncodingRawUint64Vector,
				Compression:        CompressionNone,
				FixedWidthElements: maxInt/8 + 1,
			},
		},
		{
			name:        "fixed_bytes_logical_bits",
			logicalType: "byte_vector",
			def: ColumnDefinition{
				Name:               "codes",
				Type:               ColumnTypeFixedBytes,
				Encoding:           EncodingRawFixedBytes,
				Compression:        CompressionNone,
				FixedWidthElements: maxInt/8 + 1,
			},
		},
		{
			name:        "packed_logical_bits",
			logicalType: string(ColumnTypePackedUint4Vector),
			def: ColumnDefinition{
				Name:               "codes4",
				Type:               ColumnTypePackedUint4Vector,
				Encoding:           EncodingRawPackedUint4Vector,
				Compression:        CompressionNone,
				FixedWidthElements: maxInt/4 + 1,
				BitsPerElement:     4,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layout := physicalColumnLayoutForContract(tc.logicalType, tc.def)
			if layout.endian != ColumnPartLayoutEndianNone || layout.direct || layout.bytesPerRow != 0 || layout.logicalBitsPerRow != 0 {
				t.Fatalf("overflow layout=%+v want fail-closed empty layout", layout)
			}
		})
	}
}

func TestColumnPartLayoutContractOldAssetMissingContractFailsClosed(t *testing.T) {
	image := mustLayoutContractFixedWidthImage(t)
	descriptor := mustValidationSection(t, image, ColumnPartImageSectionDescriptor)
	desc, columns, err := DecodeColumnPartDescriptorSection(image.sectionBytes(descriptor))
	if err != nil {
		t.Fatalf("DecodeColumnPartDescriptorSection: %v", err)
	}
	old := image
	old.Sections = append([]ColumnPartImageSection(nil), image.Sections...)
	for i, section := range old.Sections {
		if section.Kind == ColumnPartImageSectionLayoutContract {
			old.Sections = append(old.Sections[:i], old.Sections[i+1:]...)
			break
		}
	}
	_, err = CertifyColumnPartLayoutContract(old, desc, columns, image.sectionBytes(descriptor), nil)
	if err == nil || !strings.Contains(err.Error(), "pre-alpha typed-column assets must be rebuilt") {
		t.Fatalf("missing contract err=%v want clear pre-alpha rebuild error", err)
	}
}

func TestColumnPartLayoutContractRequiresDescriptorBytes(t *testing.T) {
	image := mustLayoutContractFixedWidthImage(t)
	descriptor := mustValidationSection(t, image, ColumnPartImageSectionDescriptor)
	desc, columns, err := DecodeColumnPartDescriptorSection(image.sectionBytes(descriptor))
	if err != nil {
		t.Fatalf("DecodeColumnPartDescriptorSection: %v", err)
	}
	contract := mustValidationSection(t, image, ColumnPartImageSectionLayoutContract)
	_, err = CertifyColumnPartLayoutContract(image, desc, columns, nil, image.sectionBytes(contract))
	if err == nil || !strings.Contains(err.Error(), "descriptor bytes") {
		t.Fatalf("nil descriptor bytes err=%v want descriptor bytes error", err)
	}
}

func TestColumnPartLayoutContractDictionaryAndNullableMismatchesFailClosed(t *testing.T) {
	t.Run("dictionary_identity_order", func(t *testing.T) {
		part := mustTransplantPart(t, 185001, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
		image := mustTransplantImage(t, part)
		cert, err := CertifyColumnPartLayoutContractFromImage(image)
		if err != nil {
			t.Fatalf("certify dictionary image: %v", err)
		}
		kind, ok := cert.Column("kind_code")
		if !ok || !kind.Dictionary || kind.DictionaryOrdered || kind.DictionaryCollation != "" || kind.DictionarySection.Length == 0 {
			t.Fatalf("kind_code contract=%+v want unordered dictionary identity", kind)
		}

		corrupt := cloneColumnPartImageBytes(image)
		contract := mustLayoutContractFromImage(t, corrupt)
		column := mustLayoutContractColumnPtr(t, &contract, "kind_code")
		column.DictionaryOrdered = true
		replaceLayoutContract(t, &corrupt, contract)
		if _, err := CertifyColumnPartLayoutContractFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "dictionary order requires collation") {
			t.Fatalf("ordered dictionary without collation err=%v", err)
		}

		corrupt = cloneColumnPartImageBytes(image)
		contract = mustLayoutContractFromImage(t, corrupt)
		column = mustLayoutContractColumnPtr(t, &contract, "kind_code")
		column.DictionarySection.Length--
		replaceLayoutContract(t, &corrupt, contract)
		if _, err := CertifyColumnPartLayoutContractFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "dictionary section offset/length") {
			t.Fatalf("dictionary section mismatch err=%v", err)
		}
	})

	t.Run("nullable_mask_counts", func(t *testing.T) {
		part := mustLayoutContractNullablePart(t)
		image := mustTransplantImage(t, part)
		cert, err := CertifyColumnPartLayoutContractFromImage(image)
		if err != nil {
			t.Fatalf("certify nullable image: %v", err)
		}
		maybe, ok := cert.Column("maybe")
		if !ok || maybe.DirectViewCertified || maybe.StreamingCertified || !maybe.NullMaskPresent || !maybe.DefaultMaskPresent || maybe.NullCount != 1 || maybe.DefaultCount != 1 {
			t.Fatalf("nullable contract=%+v want masks and no value fast path", maybe)
		}
		corrupt := cloneColumnPartImageBytes(image)
		contract := mustLayoutContractFromImage(t, corrupt)
		column := mustLayoutContractColumnPtr(t, &contract, "maybe")
		column.NullCount++
		replaceLayoutContract(t, &corrupt, contract)
		if _, err := CertifyColumnPartLayoutContractFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "null/default counts") {
			t.Fatalf("nullable count mismatch err=%v", err)
		}
	})
}

func mustLayoutContractFixedWidthPart(t *testing.T) *ColumnPart {
	t.Helper()
	part, err := BuildColumnPart(185000, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "score32", Type: ColumnTypeFloat32, Encoding: EncodingRawFloat32, Compression: CompressionNone, CompressionSet: true},
			{Name: "score64", Type: ColumnTypeFloat64, Encoding: EncodingRawFloat64, Compression: CompressionNone, CompressionSet: true},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 3},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Encoding: EncodingRawUint32Dense, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 2},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:           3,
		Columns:        map[string][]int64{"id": {3, 1, 2}, "value": {30, 10, 20}},
		Float32Columns: map[string][]float32{"score32": {3.25, 1.25, 2.25}},
		Float64Columns: map[string][]float64{"score64": {30.5, 10.5, 20.5}},
		Float32Vectors: map[string][]float32{"embedding": {3, 0, 0, 1, 0, 0, 2, 0, 0}},
		Uint32Vectors:  map[string][]uint32{"neighbors": {30, 31, 10, 11, 20, 21}},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart fixed-width: %v", err)
	}
	return part
}

func mustLayoutContractFixedWidthImage(t *testing.T) ColumnPartImage {
	t.Helper()
	image, err := BuildColumnPartImage(mustLayoutContractFixedWidthPart(t), ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{
		"id":        "int64",
		"value":     "int64",
		"score32":   "float32",
		"score64":   "double",
		"embedding": "float32_vector",
		"neighbors": "adjacency_list",
	}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage fixed-width layout contract: %v", err)
	}
	return image
}

func mustLayoutContractNullablePart(t *testing.T) *ColumnPart {
	t.Helper()
	part, err := BuildColumnPart(185002, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "maybe", Type: ColumnTypeInt64, Encoding: EncodingNullableInt64, Compression: CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 4},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:     4,
		Columns:  map[string][]int64{"id": {1, 2, 3, 4}, "maybe": {10, 0, 30, 0}},
		Nulls:    map[string][]bool{"maybe": {false, true, false, false}},
		Defaults: map[string][]bool{"maybe": {false, false, false, true}},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart nullable: %v", err)
	}
	return part
}

func mustLayoutContractFromImage(t *testing.T, image ColumnPartImage) ColumnPartLayoutContract {
	t.Helper()
	section, err := image.LayoutContractSection()
	if err != nil {
		t.Fatalf("LayoutContractSection: %v", err)
	}
	contract, err := DecodeColumnPartLayoutContract(image.sectionBytes(section))
	if err != nil {
		t.Fatalf("DecodeColumnPartLayoutContract: %v", err)
	}
	return contract
}

func mustLayoutContractColumnPtr(t *testing.T, contract *ColumnPartLayoutContract, name string) *ColumnPartLayoutContractColumn {
	t.Helper()
	for i := range contract.Columns {
		if contract.Columns[i].Name == name {
			return &contract.Columns[i]
		}
	}
	t.Fatalf("missing contract column %q", name)
	return nil
}

func replaceLayoutContract(t *testing.T, image *ColumnPartImage, contract ColumnPartLayoutContract) {
	t.Helper()
	section, err := image.LayoutContractSection()
	if err != nil {
		t.Fatalf("LayoutContractSection: %v", err)
	}
	encoded := encodeLayoutContractForTest(t, contract)
	if len(encoded) != section.Length {
		t.Fatalf("encoded contract bytes=%d want existing section length=%d", len(encoded), section.Length)
	}
	copy(image.Bytes[section.Offset:section.Offset+section.Length], encoded)
}

func mustLayoutContractDescriptorStoredBytesOffset(t *testing.T, image ColumnPartImage, column string) int {
	t.Helper()
	section := mustValidationSection(t, image, ColumnPartImageSectionDescriptor)
	dec := columnPartImageDecoder{data: image.sectionBytes(section)}
	mustValidationSkipU16(t, &dec)
	mustValidationSkipU64(t, &dec)
	mustValidationReadU32(t, &dec)
	mustValidationSkipI64(t, &dec)
	mustValidationSkipI64(t, &dec)
	if _, err := dec.stringSlice(); err != nil {
		t.Fatalf("decode logical primary key: %v", err)
	}
	granuleCount := mustValidationReadU32(t, &dec)
	dec.offset += int(granuleCount) * 64
	columnCount := mustValidationReadU32(t, &dec)
	for i := 0; i < int(columnCount); i++ {
		name, err := dec.str()
		if err != nil {
			t.Fatalf("decode descriptor column name: %v", err)
		}
		mustValidationSkipU16(t, &dec)
		mustValidationReadU32(t, &dec)
		mustValidationReadU32(t, &dec)
		mustValidationReadU32(t, &dec) // bits_per_element
		blockCount := mustValidationReadU32(t, &dec)
		for block := 0; block < int(blockCount); block++ {
			if name == column {
				dec.offset += 32 + 2 + 2 + 8
				return section.Offset + dec.offset
			}
			dec.offset += 94
		}
	}
	t.Fatalf("descriptor column %q not found", column)
	return 0
}

func encodeLayoutContractForTest(t *testing.T, contract ColumnPartLayoutContract) []byte {
	t.Helper()
	var enc columnPartImageEncoder
	enc.u16(contract.Version)
	enc.u16(0)
	enc.u64(contract.PartID)
	enc.i64(int64(contract.Rows))
	enc.u16(contract.ImageVersion)
	enc.u16(0)
	enc.i64(int64(contract.ManifestBytes))
	encodeColumnPartLayoutContractSection(&enc, contract.Descriptor)
	enc.u32(uint32(len(contract.Columns)))
	for _, column := range contract.Columns {
		if err := encodeColumnPartLayoutContractColumn(&enc, column); err != nil {
			t.Fatalf("encode layout contract column: %v", err)
		}
	}
	return enc.bytes()
}

func mustLayoutContractBlockStoredBytesOffset(t *testing.T, contractRaw []byte, columnName string, blockIndex int) int {
	t.Helper()
	dec := columnPartImageDecoder{data: contractRaw}
	mustValidationSkipU16(t, &dec) // version
	mustValidationSkipU16(t, &dec) // reserved
	mustValidationSkipU64(t, &dec) // part_id
	mustValidationSkipI64(t, &dec) // rows
	mustValidationSkipU16(t, &dec) // image version
	mustValidationSkipU16(t, &dec) // reserved
	mustValidationSkipI64(t, &dec) // manifest bytes
	mustValidationSkipI64(t, &dec) // descriptor offset
	mustValidationSkipI64(t, &dec) // descriptor length
	mustValidationReadU32(t, &dec) // descriptor checksum
	columnCount := mustValidationReadU32(t, &dec)
	for columnIndex := 0; columnIndex < int(columnCount); columnIndex++ {
		name, err := dec.str()
		if err != nil {
			t.Fatalf("decode layout contract column name: %v", err)
		}
		if _, err := dec.str(); err != nil { // logical type
			t.Fatalf("decode layout contract logical type: %v", err)
		}
		mustValidationSkipU16(t, &dec)       // column type
		mustValidationSkipU16(t, &dec)       // encoding
		mustValidationSkipU16(t, &dec)       // compression
		mustValidationSkipU16(t, &dec)       // reserved
		mustValidationReadU32(t, &dec)       // flags
		mustValidationSkipI64(t, &dec)       // rows
		mustValidationSkipI64(t, &dec)       // section offset
		mustValidationSkipI64(t, &dec)       // section length
		mustValidationReadU32(t, &dec)       // section checksum
		mustValidationSkipI64(t, &dec)       // fixed-width elements
		mustValidationSkipI64(t, &dec)       // bits per element
		mustValidationSkipI64(t, &dec)       // bytes per row
		mustValidationSkipI64(t, &dec)       // logical bits per row
		mustValidationSkipI64(t, &dec)       // element size
		mustValidationSkipI64(t, &dec)       // alignment
		mustValidationSkipU16(t, &dec)       // endian
		mustValidationSkipU16(t, &dec)       // reserved
		mustValidationSkipI64(t, &dec)       // length multiple
		mustValidationSkipI64(t, &dec)       // null count
		mustValidationSkipI64(t, &dec)       // default count
		mustValidationSkipI64(t, &dec)       // dictionary offset
		mustValidationSkipI64(t, &dec)       // dictionary length
		mustValidationReadU32(t, &dec)       // dictionary checksum
		if _, err := dec.str(); err != nil { // dictionary collation
			t.Fatalf("decode layout contract dictionary collation: %v", err)
		}
		blockCount := mustValidationReadU32(t, &dec)
		for block := 0; block < int(blockCount); block++ {
			mustValidationSkipI64(t, &dec) // first row
			mustValidationSkipI64(t, &dec) // row count
			mustValidationSkipI64(t, &dec) // first granule
			mustValidationSkipI64(t, &dec) // last granule
			mustValidationSkipU16(t, &dec) // encoding
			mustValidationSkipU16(t, &dec) // compression
			mustValidationReadU32(t, &dec) // reserved
			mustValidationSkipI64(t, &dec) // raw bytes
			storedBytesOffset := dec.offset
			mustValidationSkipI64(t, &dec) // stored bytes
			if name == columnName && block == blockIndex {
				return storedBytesOffset
			}
			mustValidationSkipI64(t, &dec) // payload offset
			mustValidationSkipI64(t, &dec) // payload length
			mustValidationSkipI64(t, &dec) // null count
			mustValidationSkipI64(t, &dec) // default count
		}
	}
	t.Fatalf("layout contract column %q block %d not found", columnName, blockIndex)
	return 0
}

func TestColumnPartLayoutContractDescriptorStoredBytesFailsClosed(t *testing.T) {
	image := mustLayoutContractFixedWidthImage(t)
	corrupt := cloneColumnPartImageBytes(image)
	storedBytesOffset := mustLayoutContractDescriptorStoredBytesOffset(t, corrupt, "value")
	storedBytes := int64(binary.LittleEndian.Uint64(corrupt.Bytes[storedBytesOffset : storedBytesOffset+8]))
	binary.LittleEndian.PutUint64(corrupt.Bytes[storedBytesOffset:storedBytesOffset+8], uint64(storedBytes-1))
	_, err := CertifyColumnPartLayoutContractFromImage(corrupt)
	if err == nil || !(strings.Contains(err.Error(), "stored bytes") || strings.Contains(err.Error(), "descriptor")) {
		t.Fatalf("descriptor stored bytes corruption err=%v want fail closed", err)
	}
}

func TestColumnPartLayoutContractStoredBytesFieldFailsClosed(t *testing.T) {
	image := mustLayoutContractFixedWidthImage(t)
	corrupt := append([]byte(nil), image.Bytes...)
	section := mustValidationSection(t, image, ColumnPartImageSectionLayoutContract)
	contract := mustLayoutContractFromImage(t, image)
	value := mustLayoutContractColumnPtr(t, &contract, "value")
	// Corrupt the serialized stored_bytes for the first value block in-place to
	// prove the stored contract bytes, not only decoded helper structs, fail.
	idx := mustLayoutContractBlockStoredBytesOffset(t, image.sectionBytes(section), "value", 0)
	binary.LittleEndian.PutUint64(corrupt[section.Offset+idx:section.Offset+idx+8], uint64(value.Blocks[0].StoredBytes-1))
	corruptImage := image
	corruptImage.Bytes = corrupt
	_, err := CertifyColumnPartLayoutContractFromImage(corruptImage)
	if err == nil || !strings.Contains(err.Error(), "raw/stored") {
		t.Fatalf("stored bytes corruption err=%v want raw/stored mismatch", err)
	}
}
