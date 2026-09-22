package typedcolumn

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestTypedColumnCompressedRowLocatorsRejectHugeDecodedLength1952(t *testing.T) {
	rows := maxCompressedRowLocatorSectionRawBytes/rowLocatorBytes + 1
	image := ColumnPartImage{Rows: rows, Bytes: []byte{0}}
	section := ColumnPartImageSection{Kind: ColumnPartImageSectionRowLocators, Offset: 0, Length: 1, Rows: rows, Compression: CompressionLZ4}
	if _, err := image.rowLocatorSectionBytes(section); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("rowLocatorSectionBytes huge decoded length err=%v want max-length fail-closed", err)
	}
}

func TestTypedColumnUncompressedRowLocatorRawBytesAreNotCompressedCapLimited1952(t *testing.T) {
	rows := maxCompressedRowLocatorSectionRawBytes/rowLocatorBytes + 1
	rawBytes, err := rowLocatorSectionRawBytes(rows)
	if err != nil {
		t.Fatalf("rowLocatorSectionRawBytes(%d): %v", rows, err)
	}
	if rawBytes <= maxCompressedRowLocatorSectionRawBytes {
		t.Fatalf("rawBytes=%d want above compressed decode cap=%d", rawBytes, maxCompressedRowLocatorSectionRawBytes)
	}
	section := ColumnPartImageSection{Kind: ColumnPartImageSectionRowLocators, Rows: rows}
	if canCompressImageSection(section, rawBytes, CompressionSnappy) {
		t.Fatalf("canCompressImageSection allowed compressed locator rawBytes=%d above cap=%d", rawBytes, maxCompressedRowLocatorSectionRawBytes)
	}
	if !canCompressImageSection(ColumnPartImageSection{Kind: ColumnPartImageSectionRowLocators, Rows: 1}, rowLocatorBytes+4, CompressionSnappy) {
		t.Fatalf("canCompressImageSection rejected small compressed locator section")
	}
	if !canCompressImageSection(ColumnPartImageSection{Kind: ColumnPartImageSectionDictionaries}, 128, CompressionSnappy) {
		t.Fatalf("canCompressImageSection rejected small compressed dictionary section")
	}
	if canCompressImageSection(ColumnPartImageSection{Kind: ColumnPartImageSectionDictionaries}, maxCompressedDictionarySectionRawBytes+1, CompressionSnappy) {
		t.Fatalf("canCompressImageSection allowed compressed dictionary above cap")
	}
	if !canCompressImageSection(ColumnPartImageSection{Kind: ColumnPartImageSectionRowLocators, Rows: 1}, rowLocatorBytes+4, CompressionZSTD) {
		t.Fatalf("canCompressImageSection rejected small zstd locator section")
	}
}

func TestTypedColumnSectionAccountingLeavesInvalidLocatorRawBytesUnknown1952(t *testing.T) {
	rows := int(^uint(0)>>1)/rowLocatorBytes + 1
	image := ColumnPartImage{
		Rows:  rows,
		Bytes: []byte{0},
		Sections: []ColumnPartImageSection{{
			Kind:        ColumnPartImageSectionRowLocators,
			Category:    ColumnPartImageCategoryLocators,
			Name:        "primary_id_locators",
			Offset:      0,
			Length:      1,
			Rows:        rows,
			Compression: CompressionLZ4,
		}},
	}
	sections := image.SectionByteAccounting()
	if len(sections) != 1 {
		t.Fatalf("SectionByteAccounting rows=%+v want one locator row", sections)
	}
	if sections[0].Kind != ColumnPartImageSectionRowLocators || sections[0].RawBytes != 0 || sections[0].StoredBytes != 1 || sections[0].Bytes != 1 {
		t.Fatalf("locator accounting=%+v want raw bytes unknown and stored bytes preserved", sections[0])
	}
}

func TestTypedColumnValidationUnsupportedImageVersionFailsClosed(t *testing.T) {
	image := mustTransplantImage(t, mustTransplantPart(t, 178901, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))

	badImage := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint16(badImage[4:6], columnPartImageVersion+1)
	if _, err := ParseColumnPartImage(badImage); err == nil || !strings.Contains(err.Error(), "unsupported part image version") {
		t.Fatalf("ParseColumnPartImage unsupported image version err=%v want unsupported part image version", err)
	}

	tcs1, _, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	badTCS1 := append([]byte(nil), tcs1...)
	binary.LittleEndian.PutUint16(badTCS1[tcs1ImageVersionOffset:tcs1ReservedOffset], columnPartImageVersion+1)
	badTCS1[tcs1PayloadOffset] ^= 0xff // Must fail on the header image version before payload checksum/parse.
	if _, err := DecodeTCS1ColumnPartHeader(badTCS1[:tcs1HeaderBytes], int64(len(badTCS1))); err == nil || !strings.Contains(err.Error(), "unsupported part image version") {
		t.Fatalf("DecodeTCS1ColumnPartHeader unsupported image version err=%v want unsupported part image version", err)
	}
	if _, _, err := DecodeTCS1ColumnPartImage(badTCS1); err == nil || !strings.Contains(err.Error(), "unsupported part image version") || strings.Contains(err.Error(), "checksum") {
		t.Fatalf("DecodeTCS1ColumnPartImage unsupported image version err=%v want header version failure before checksum", err)
	}
}

func TestTypedColumnManifestOnlyParseRejectsOutOfBoundsSection(t *testing.T) {
	part := mustTransplantPart(t, 178905, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	sections := append([]ColumnPartImageSection(nil), image.Sections...)
	if len(sections) == 0 {
		t.Fatal("image has no sections")
	}
	sections[0].Offset = alignColumnPartImageOffset(len(image.Bytes))
	sections[0].Length = columnPartImageSectionAlignment
	manifest, err := encodeColumnPartImageManifest(part, sections, image.ManifestBytes)
	if err != nil {
		t.Fatalf("encodeColumnPartImageManifest: %v", err)
	}
	if len(manifest) != image.ManifestBytes {
		t.Fatalf("manifest bytes=%d want stable original %d", len(manifest), image.ManifestBytes)
	}
	if _, err := ParseColumnPartImageManifest(manifest, len(image.Bytes)); err == nil || !strings.Contains(err.Error(), "exceeds image bytes") {
		t.Fatalf("ParseColumnPartImageManifest out-of-bounds err=%v want fail-closed exceeds-image error", err)
	}
}

func TestTypedColumnValidationUnsupportedDescriptorVersionFailsClosed(t *testing.T) {
	image := mustTransplantImage(t, mustTransplantPart(t, 178902, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	corrupt := cloneColumnPartImageBytes(image)
	descriptor := mustValidationSection(t, corrupt, ColumnPartImageSectionDescriptor)
	binary.LittleEndian.PutUint16(corrupt.Bytes[descriptor.Offset:descriptor.Offset+2], columnPartDescriptorVersion+1)

	if _, err := ColumnPartFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "unsupported descriptor version") {
		t.Fatalf("ColumnPartFromImage unsupported descriptor version err=%v want unsupported descriptor version", err)
	}
}

func TestTypedColumnValidationDescriptorSchemaVersionMismatchFailsClosed(t *testing.T) {
	part := mustTransplantPart(t, 178903, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	corrupt := cloneColumnPartImageBytes(image)
	descriptor := mustValidationSection(t, corrupt, ColumnPartImageSectionDescriptor)
	binary.LittleEndian.PutUint32(corrupt.Bytes[descriptor.Offset+10:descriptor.Offset+14], part.Descriptor.SchemaVersion+1)

	if _, err := part.WithImagePayloads(corrupt); err == nil || !strings.Contains(err.Error(), "image descriptor does not match part descriptor") {
		t.Fatalf("WithImagePayloads schema version mismatch err=%v want descriptor mismatch", err)
	}
}

func TestTypedColumnValidationFixedWidthLayoutMismatchFailsClosed(t *testing.T) {
	part := mustValidationVectorPart(t, 178904)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	corrupt := cloneColumnPartImageBytes(image)
	fixedWidthOffset := mustValidationDescriptorFixedWidthOffset(t, corrupt, "embedding")
	binary.LittleEndian.PutUint32(corrupt.Bytes[fixedWidthOffset:fixedWidthOffset+4], 2)

	if _, err := ColumnPartFromImage(corrupt); err == nil || !(strings.Contains(err.Error(), "raw bytes") || strings.Contains(err.Error(), "fixed-width elements")) {
		t.Fatalf("ColumnPartFromImage fixed-width layout mismatch err=%v want descriptor layout failure", err)
	}
}

func cloneColumnPartImageBytes(image ColumnPartImage) ColumnPartImage {
	clone := image
	clone.Bytes = append([]byte(nil), image.Bytes...)
	clone.Sections = append([]ColumnPartImageSection(nil), image.Sections...)
	return clone
}

func mustValidationSection(t *testing.T, image ColumnPartImage, kind ColumnPartImageSectionKind) ColumnPartImageSection {
	t.Helper()
	section, err := image.singleSection(kind)
	if err != nil {
		t.Fatalf("singleSection(%s): %v", kind, err)
	}
	return section
}

func mustValidationVectorPart(t *testing.T, partID uint64) *ColumnPart {
	t.Helper()
	part, err := BuildColumnPart(partID, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 3},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:           2,
		Columns:        map[string][]int64{"id": {2, 1}},
		Float32Vectors: map[string][]float32{"embedding": {1, 2, 3, 4, 5, 6}},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart(vector): %v", err)
	}
	return part
}

func mustValidationDescriptorFixedWidthOffset(t *testing.T, image ColumnPartImage, column string) int {
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
	granuleBytes := int(granuleCount) * 64
	if err := dec.require(granuleBytes); err != nil {
		t.Fatalf("skip descriptor granules: %v", err)
	}
	dec.offset += granuleBytes
	columnCount := mustValidationReadU32(t, &dec)
	for i := 0; i < int(columnCount); i++ {
		name, err := dec.str()
		if err != nil {
			t.Fatalf("decode descriptor column name: %v", err)
		}
		mustValidationSkipU16(t, &dec)
		mustValidationReadU32(t, &dec)
		fixedWidthOffset := dec.offset
		mustValidationReadU32(t, &dec)
		mustValidationReadU32(t, &dec) // bits_per_element
		blockCount := mustValidationReadU32(t, &dec)
		if name == column {
			return section.Offset + fixedWidthOffset
		}
		blockBytes := int(blockCount) * 94
		if err := dec.require(blockBytes); err != nil {
			t.Fatalf("skip descriptor column %q blocks: %v", name, err)
		}
		dec.offset += blockBytes
	}
	t.Fatalf("descriptor column %q not found", column)
	return 0
}

func mustValidationSkipU16(t *testing.T, dec *columnPartImageDecoder) {
	t.Helper()
	if _, err := dec.u16(); err != nil {
		t.Fatalf("decode descriptor: %v", err)
	}
}

func mustValidationReadU32(t *testing.T, dec *columnPartImageDecoder) uint32 {
	t.Helper()
	v, err := dec.u32()
	if err != nil {
		t.Fatalf("decode descriptor: %v", err)
	}
	return v
}

func mustValidationSkipU64(t *testing.T, dec *columnPartImageDecoder) {
	t.Helper()
	if _, err := dec.u64(); err != nil {
		t.Fatalf("decode descriptor: %v", err)
	}
}

func mustValidationSkipI64(t *testing.T, dec *columnPartImageDecoder) {
	t.Helper()
	if _, err := dec.i64(); err != nil {
		t.Fatalf("decode descriptor: %v", err)
	}
}
