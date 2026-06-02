package typedcolumn

import (
	"encoding/binary"
	"math/bits"
	"strings"
	"testing"
)

var packedCodeByteSink byte
var packedCodeUint64Sink uint64

func TestFixedBytesRowsRoundTripAndNoAlloc(t *testing.T) {
	values := []byte{
		0x00, 0x01, 0x02,
		0x10, 0x11, 0x12,
		0x20, 0x21, 0x22,
	}
	column, err := NewFixedBytesRows(3, 3, values)
	if err != nil {
		t.Fatalf("NewFixedBytesRows: %v", err)
	}
	row, err := column.Row(1)
	if err != nil {
		t.Fatalf("Row: %v", err)
	}
	if got, want := row, []byte{0x10, 0x11, 0x12}; string(got) != string(want) {
		t.Fatalf("row=%x want %x", got, want)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		row, err := column.Row(2)
		if err != nil {
			t.Fatal(err)
		}
		packedCodeByteSink ^= row[0]
	})
	if allocs != 0 {
		t.Fatalf("Row allocs=%v want 0", allocs)
	}
}

func TestPackedUintRowsGoldenBitOrderAndRoundTrip(t *testing.T) {
	tests := []struct {
		name            string
		bitsPerElement  int
		elementsPerRow  int
		values          []uint8
		wantEncodedRows []byte
	}{
		{
			name:            "packed_bit_vector_lsb0_non_byte_multiple",
			bitsPerElement:  1,
			elementsPerRow:  10,
			values:          []uint8{1, 0, 1, 1, 0, 0, 1, 0, 1, 1},
			wantEncodedRows: []byte{0x4d, 0x03},
		},
		{
			name:            "packed_uint2_vector_lsb0_non_byte_multiple",
			bitsPerElement:  2,
			elementsPerRow:  5,
			values:          []uint8{0, 1, 2, 3, 1},
			wantEncodedRows: []byte{0xe4, 0x01},
		},
		{
			name:            "packed_uint4_vector_lsb0_non_byte_multiple",
			bitsPerElement:  4,
			elementsPerRow:  3,
			values:          []uint8{0x0a, 0x0b, 0x0c},
			wantEncodedRows: []byte{0xba, 0x0c},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := EncodePackedUintRows(nil, 1, tc.elementsPerRow, tc.bitsPerElement, tc.values)
			if err != nil {
				t.Fatalf("EncodePackedUintRows: %v", err)
			}
			if got, want := encoded, tc.wantEncodedRows; string(got) != string(want) {
				t.Fatalf("encoded=% x want % x", got, want)
			}
			column, err := NewPackedUintRows(1, tc.elementsPerRow, tc.bitsPerElement, encoded)
			if err != nil {
				t.Fatalf("NewPackedUintRows: %v", err)
			}
			row, err := column.RowBytes(0)
			if err != nil {
				t.Fatalf("RowBytes: %v", err)
			}
			decoded, err := UnpackUintRow(nil, row, tc.elementsPerRow, tc.bitsPerElement)
			if err != nil {
				t.Fatalf("UnpackUintRow: %v", err)
			}
			if got, want := decoded, tc.values; len(got) != len(want) {
				t.Fatalf("decoded len=%d want %d", len(got), len(want))
			} else {
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("decoded[%d]=%d want %d (decoded=%v)", i, got[i], want[i], got)
					}
				}
			}
			for i, want := range tc.values {
				got, err := column.Element(0, i)
				if err != nil {
					t.Fatalf("Element(%d): %v", i, err)
				}
				if got != want {
					t.Fatalf("Element(%d)=%d want %d", i, got, want)
				}
			}
		})
	}
}

func TestPackedUintRowsMultipleRowsAndNonDivisibleDimensions(t *testing.T) {
	rows := 3
	elementsPerRow := 13
	values := make([]uint8, rows*elementsPerRow)
	for row := 0; row < rows; row++ {
		for element := 0; element < elementsPerRow; element++ {
			values[row*elementsPerRow+element] = uint8((row + element) & 1)
		}
	}
	encoded, err := EncodePackedUintRows(nil, rows, elementsPerRow, 1, values)
	if err != nil {
		t.Fatalf("EncodePackedUintRows: %v", err)
	}
	column, err := NewPackedUintRows(rows, elementsPerRow, 1, encoded)
	if err != nil {
		t.Fatalf("NewPackedUintRows: %v", err)
	}
	if got, want := column.BytesPerRow, 2; got != want {
		t.Fatalf("BytesPerRow=%d want %d", got, want)
	}
	for row := 0; row < rows; row++ {
		for element := 0; element < elementsPerRow; element++ {
			got, err := column.Element(row, element)
			if err != nil {
				t.Fatalf("Element(%d,%d): %v", row, element, err)
			}
			want := values[row*elementsPerRow+element]
			if got != want {
				t.Fatalf("Element(%d,%d)=%d want %d", row, element, got, want)
			}
		}
	}
}

func TestPackedUintPaddingValidationFailsClosed(t *testing.T) {
	if _, err := NewPackedUintRows(1, 10, 1, []byte{0x4d, 0x43}); err == nil {
		t.Fatal("NewPackedUintRows with non-zero padding err=nil, want failure")
	}
	if err := ValidatePackedUintRowPadding([]byte{0xe4, 0xc1}, 5, 2); err == nil {
		t.Fatal("ValidatePackedUintRowPadding with u2 non-zero padding err=nil, want failure")
	}
	if err := ValidatePackedUintRowPadding([]byte{0xba, 0xfc}, 3, 4); err == nil {
		t.Fatal("ValidatePackedUintRowPadding with u4 non-zero padding err=nil, want failure")
	}
}

func TestPackedUintLittleEndianWordViews(t *testing.T) {
	rowBytes := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	column, err := NewPackedUintRows(1, 64, 1, rowBytes)
	if err != nil {
		t.Fatalf("NewPackedUintRows: %v", err)
	}
	words, _, err := column.RowWords(0, nil)
	if err != nil {
		t.Fatalf("RowWords: %v", err)
	}
	if got, want := len(words), 1; got != want {
		t.Fatalf("words len=%d want %d", got, want)
	}
	if got, want := words[0], uint64(0x0807060504030201); got != want {
		t.Fatalf("word=%#x want %#x", got, want)
	}

	partial := []byte{0x11, 0x22, 0x33}
	fixed, err := NewFixedBytesRows(1, len(partial), partial)
	if err != nil {
		t.Fatalf("NewFixedBytesRows: %v", err)
	}
	words, direct, err := fixed.RowWords(0, []uint64{99})
	if err != nil {
		t.Fatalf("fixed RowWords: %v", err)
	}
	if direct {
		t.Fatal("partial fixed row returned direct words, want scratch")
	}
	if got, want := words[0], uint64(0x332211); got != want {
		t.Fatalf("partial word=%#x want %#x", got, want)
	}
}

func TestPackedUintRandomOrdinalAccessNoAllocation(t *testing.T) {
	rows := 257
	elementsPerRow := 130 // 17 bytes/row, 3 scratch words/row.
	values := make([]uint8, rows*elementsPerRow)
	for i := range values {
		values[i] = uint8((i*1103515245 + 12345) >> 31 & 1)
	}
	encoded, err := EncodePackedUintRows(nil, rows, elementsPerRow, 1, values)
	if err != nil {
		t.Fatalf("EncodePackedUintRows: %v", err)
	}
	column, err := NewPackedUintRows(rows, elementsPerRow, 1, encoded)
	if err != nil {
		t.Fatalf("NewPackedUintRows: %v", err)
	}
	ordinals := make([]int, 1024)
	for i := range ordinals {
		ordinals[i] = (i*37 + 11) % rows
	}
	scratch := make([]uint64, column.WordCount())
	idx := 0
	allocs := testing.AllocsPerRun(1000, func() {
		row := ordinals[idx&(len(ordinals)-1)]
		idx++
		rowBytes, err := column.RowBytes(row)
		if err != nil {
			t.Fatal(err)
		}
		words, direct, err := column.RowWords(row, scratch)
		if err != nil {
			t.Fatal(err)
		}
		if direct {
			t.Fatal("17-byte packed row unexpectedly returned direct words")
		}
		packedCodeByteSink ^= rowBytes[0]
		packedCodeUint64Sink ^= words[0]
	})
	if allocs != 0 {
		t.Fatalf("random ordinal access allocs=%v want 0", allocs)
	}
}

func TestPackedUintCorruptShapeFailsClosed(t *testing.T) {
	if _, err := PackedUintRowBytes(10, 3); err == nil {
		t.Fatal("PackedUintRowBytes bits=3 err=nil, want failure")
	}
	if _, err := NewPackedUintRows(2, 10, 1, []byte{0x00, 0x00, 0x00}); err == nil {
		t.Fatal("NewPackedUintRows short payload err=nil, want failure")
	}
	short := PackedUintRows{Rows: 2, ElementsPerRow: 10, BitsPerElement: 1, BytesPerRow: 2, Values: []byte{0x00, 0x00, 0x00}}
	if err := short.ValidatePadding(); err == nil {
		t.Fatal("ValidatePadding short payload err=nil, want failure")
	}
	bad := PackedUintRows{Rows: 1, ElementsPerRow: 10, BitsPerElement: 1, BytesPerRow: 3, Values: []byte{0x00, 0x00}}
	if err := bad.Validate(); err == nil {
		t.Fatal("PackedUintRows metadata mismatch err=nil, want failure")
	}
	if _, err := NewFixedBytesRows(2, 4, []byte{0x00, 0x01, 0x02}); err == nil {
		t.Fatal("NewFixedBytesRows short payload err=nil, want failure")
	}
	if _, err := NewFixedBytesRows(1, 0, nil); err == nil {
		t.Fatal("NewFixedBytesRows bytes_per_row=0 err=nil, want failure")
	}
}

func TestFixedBytesAndPackedUintColumnPartImageRoundTrip1931(t *testing.T) {
	fixedValues := []byte{
		0x20, 0x21, 0x22,
		0x00, 0x01, 0x02,
		0x10, 0x11, 0x12,
	}
	fixedRows, err := NewFixedBytesRows(3, 3, fixedValues)
	if err != nil {
		t.Fatalf("NewFixedBytesRows: %v", err)
	}
	unpackedBits := []uint8{
		1, 0, 1, 1, 0, 0, 1, 0, 1, 1,
		0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
		1, 1, 1, 1, 0, 0, 0, 0, 1, 0,
	}
	packedValues, err := EncodePackedUintRows(nil, 3, 10, 1, unpackedBits)
	if err != nil {
		t.Fatalf("EncodePackedUintRows: %v", err)
	}
	packedRows, err := NewPackedUintRows(3, 10, 1, packedValues)
	if err != nil {
		t.Fatalf("NewPackedUintRows: %v", err)
	}
	part, err := BuildColumnPart(1931, Options{
		SchemaMode: ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "blob", Type: ColumnTypeFixedBytes, FixedWidthElements: 3, Encoding: EncodingRawFixedBytes, Compression: CompressionNone, CompressionSet: true},
			{Name: "bits", Type: ColumnTypePackedBitVector, FixedWidthElements: 10, BitsPerElement: 1, Encoding: EncodingRawPackedBitVector, Compression: CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:              3,
		Columns:           map[string][]int64{"id": {2, 0, 1}},
		FixedBytesColumns: map[string]FixedBytesRows{"blob": fixedRows},
		PackedUintColumns: map[string]PackedUintRows{"bits": packedRows},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	for _, tc := range []struct {
		name       string
		columnName string
		encoding   Encoding
		want       string
	}{
		{name: "fixed_bytes", columnName: "blob", encoding: EncodingRawInt64, want: "requires encoding=raw_fixed_bytes"},
		{name: "packed_bit", columnName: "bits", encoding: EncodingRawFixedBytes, want: "requires encoding=raw_packed_bit_vector"},
	} {
		t.Run("writer_rejects_descriptor_block_encoding_mismatch_"+tc.name, func(t *testing.T) {
			mutated := cloneColumnPartDescriptorBlockEncoding1931(part, tc.columnName, tc.encoding)
			if _, err := BuildColumnPartImage(mutated, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"blob": "byte_vector", "bits": string(ColumnTypePackedBitVector)}}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("BuildColumnPartImage err=%v want %q", err, tc.want)
			}
		})
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{"blob": "byte_vector", "bits": string(ColumnTypePackedBitVector)}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	decoded, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	gotFixed, err := decoded.FixedBytesColumn("blob", nil)
	if err != nil {
		t.Fatalf("FixedBytesColumn: %v", err)
	}
	wantFixed := [][]byte{{0x00, 0x01, 0x02}, {0x10, 0x11, 0x12}, {0x20, 0x21, 0x22}}
	for row, want := range wantFixed {
		got, err := gotFixed.Row(row)
		if err != nil {
			t.Fatalf("fixed Row(%d): %v", row, err)
		}
		if string(got) != string(want) {
			t.Fatalf("fixed row %d=%x want %x", row, got, want)
		}
	}
	gotPacked, err := decoded.PackedUintColumn("bits", nil)
	if err != nil {
		t.Fatalf("PackedUintColumn: %v", err)
	}
	wantRows := [][]uint8{
		unpackedBits[10:20],
		unpackedBits[20:30],
		unpackedBits[0:10],
	}
	for row, want := range wantRows {
		decodedRow, err := UnpackUintRow(nil, mustPackedUintRowBytes1931(t, gotPacked, row), gotPacked.ElementsPerRow, gotPacked.BitsPerElement)
		if err != nil {
			t.Fatalf("UnpackUintRow row %d: %v", row, err)
		}
		for i := range want {
			if decodedRow[i] != want[i] {
				t.Fatalf("packed row %d element %d=%d want %d decoded=%v", row, i, decodedRow[i], want[i], decodedRow)
			}
		}
	}
	cert, err := CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		t.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	blob, ok := cert.Column("blob")
	if !ok || !blob.DirectViewCertified || blob.BytesPerRow != 3 || blob.LogicalBitsPerRow != 24 {
		t.Fatalf("blob layout certification=%+v ok=%v want direct bytes_per_row=3 logical_bits_per_row=24", blob, ok)
	}
	bits, ok := cert.Column("bits")
	if !ok || !bits.DirectViewCertified || bits.BitsPerElement != 1 || bits.BytesPerRow != 2 || bits.LogicalBitsPerRow != 10 {
		t.Fatalf("bits layout certification=%+v ok=%v want packed metadata", bits, ok)
	}

	gapFixed := *decoded
	gapFixed.Columns = cloneColumnPartColumns1931(decoded.Columns)
	blobColumn := gapFixed.Columns["blob"]
	blobColumn.Blocks = append([]ColumnBlock(nil), blobColumn.Blocks...)
	blobColumn.Blocks[1].Descriptor.FirstRow++
	gapFixed.Columns["blob"] = blobColumn
	if _, err := gapFixed.FixedBytesColumn("blob", nil); err == nil || !strings.Contains(err.Error(), "fixed_bytes block first_row") {
		t.Fatalf("fixed_bytes gap err=%v want first_row fail-closed", err)
	}

	overlapPacked := *decoded
	overlapPacked.Columns = cloneColumnPartColumns1931(decoded.Columns)
	bitsColumn := overlapPacked.Columns["bits"]
	bitsColumn.Blocks = append([]ColumnBlock(nil), bitsColumn.Blocks...)
	bitsColumn.Blocks[1].Descriptor.FirstRow = 0
	overlapPacked.Columns["bits"] = bitsColumn
	if _, err := overlapPacked.PackedUintColumn("bits", nil); err == nil || !strings.Contains(err.Error(), "packed_uint block first_row") {
		t.Fatalf("packed_uint overlap err=%v want first_row fail-closed", err)
	}

	corrupt := image
	corrupt.Bytes = append([]byte(nil), image.Bytes...)
	for _, section := range corrupt.Sections {
		if section.Kind == ColumnPartImageSectionColumnData && section.Column == "bits" {
			if section.Length == 0 {
				t.Fatalf("bits column data section is empty: %+v", section)
			}
			corrupt.Bytes[section.Offset+section.Length-1] |= 0x80
			break
		}
	}
	corruptPart, err := ColumnPartFromImage(corrupt)
	if err == nil {
		_, err = corruptPart.PackedUintColumn("bits", nil)
	}
	if err == nil || !strings.Contains(err.Error(), "padding") {
		t.Fatalf("corrupt packed padding err=%v want padding failure", err)
	}
}

func cloneColumnPartDescriptorBlockEncoding1931(part *ColumnPart, columnName string, encoding Encoding) *ColumnPart {
	mutated := *part
	mutated.Descriptor = part.Descriptor
	mutated.Descriptor.Columns = append([]ColumnPartColumnDescriptor(nil), part.Descriptor.Columns...)
	for i := range mutated.Descriptor.Columns {
		if mutated.Descriptor.Columns[i].Name != columnName {
			continue
		}
		mutated.Descriptor.Columns[i].Blocks = append([]ColumnBlockDescriptor(nil), mutated.Descriptor.Columns[i].Blocks...)
		if len(mutated.Descriptor.Columns[i].Blocks) == 0 {
			panic("test part has no blocks for " + columnName)
		}
		mutated.Descriptor.Columns[i].Blocks[0].Encoding = encoding
		return &mutated
	}
	panic("test part missing column " + columnName)
}

func cloneColumnPartColumns1931(columns map[string]ColumnPartColumn) map[string]ColumnPartColumn {
	out := make(map[string]ColumnPartColumn, len(columns))
	for name, column := range columns {
		out[name] = column
	}
	return out
}

func mustPackedUintRowBytes1931(t *testing.T, rows PackedUintRows, row int) []byte {
	t.Helper()
	rowBytes, err := rows.RowBytes(row)
	if err != nil {
		t.Fatalf("packed RowBytes(%d): %v", row, err)
	}
	return rowBytes
}

func BenchmarkFixedBytesRowsRandomOrdinalLookup(b *testing.B) {
	const rows = 1 << 15
	const bytesPerRow = 32
	values := make([]byte, rows*bytesPerRow)
	for i := range values {
		values[i] = byte(i)
	}
	column, err := NewFixedBytesRows(rows, bytesPerRow, values)
	if err != nil {
		b.Fatal(err)
	}
	ordinals := benchmarkOrdinals(rows, 1<<14)
	b.ReportAllocs()
	b.SetBytes(bytesPerRow)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row, err := column.Row(ordinals[i&(len(ordinals)-1)])
		if err != nil {
			b.Fatal(err)
		}
		packedCodeByteSink ^= row[0]
	}
}

func BenchmarkPackedBitRowsRandomOrdinalLookupBytes(b *testing.B) {
	const rows = 1 << 15
	const elementsPerRow = 1536
	column := benchmarkPackedBitColumn(b, rows, elementsPerRow)
	ordinals := benchmarkOrdinals(rows, 1<<14)
	b.ReportAllocs()
	b.SetBytes(int64(column.BytesPerRow))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row, err := column.RowBytes(ordinals[i&(len(ordinals)-1)])
		if err != nil {
			b.Fatal(err)
		}
		packedCodeByteSink ^= row[0]
	}
}

func BenchmarkPackedBitRowsRandomOrdinalLookupWords(b *testing.B) {
	const rows = 1 << 15
	const elementsPerRow = 1537 // exercise scratch zero-padding path.
	column := benchmarkPackedBitColumn(b, rows, elementsPerRow)
	ordinals := benchmarkOrdinals(rows, 1<<14)
	scratch := make([]uint64, column.WordCount())
	b.ReportAllocs()
	b.SetBytes(int64(column.BytesPerRow))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		words, _, err := column.RowWords(ordinals[i&(len(ordinals)-1)], scratch)
		if err != nil {
			b.Fatal(err)
		}
		packedCodeUint64Sink ^= words[0]
	}
}

func BenchmarkPackedBitRowsPopcount(b *testing.B) {
	const rows = 1 << 15
	const elementsPerRow = 1537
	column := benchmarkPackedBitColumn(b, rows, elementsPerRow)
	ordinals := benchmarkOrdinals(rows, 1<<14)
	scratch := make([]uint64, column.WordCount())
	query := make([]uint64, column.WordCount())
	for i := range query {
		query[i] = 0xa5a5a5a5a5a5a5a5
	}
	b.ReportAllocs()
	b.SetBytes(int64(column.BytesPerRow))
	b.ResetTimer()
	var score int
	for i := 0; i < b.N; i++ {
		words, _, err := column.RowWords(ordinals[i&(len(ordinals)-1)], scratch)
		if err != nil {
			b.Fatal(err)
		}
		local := 0
		for j, word := range words {
			local += bits.OnesCount64(word ^ query[j])
		}
		score += local
	}
	packedCodeUint64Sink ^= uint64(score)
}

func BenchmarkPackedBitRowsScorerShapedAccessAllocs(b *testing.B) {
	const rows = 1 << 15
	const elementsPerRow = 1025
	column := benchmarkPackedBitColumn(b, rows, elementsPerRow)
	ordinals := benchmarkOrdinals(rows, 1<<14)
	scratch := make([]uint64, column.WordCount())
	query := make([]uint64, column.WordCount())
	for i := range query {
		query[i] = binary.LittleEndian.Uint64([]byte{byte(i), 1, 2, 3, 4, 5, 6, 7})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		words, _, err := column.RowWords(ordinals[i&(len(ordinals)-1)], scratch)
		if err != nil {
			b.Fatal(err)
		}
		var score uint64
		for j, word := range words {
			score += uint64(bits.OnesCount64(word & query[j]))
		}
		packedCodeUint64Sink ^= score
	}
}

func benchmarkPackedBitColumn(b *testing.B, rows int, elementsPerRow int) PackedUintRows {
	b.Helper()
	values := make([]uint8, rows*elementsPerRow)
	for i := range values {
		values[i] = uint8((i ^ (i >> 3) ^ (i >> 11)) & 1)
	}
	encoded, err := EncodePackedUintRows(nil, rows, elementsPerRow, 1, values)
	if err != nil {
		b.Fatal(err)
	}
	column, err := NewPackedUintRows(rows, elementsPerRow, 1, encoded)
	if err != nil {
		b.Fatal(err)
	}
	return column
}

func benchmarkOrdinals(rows int, n int) []int {
	ordinals := make([]int, n)
	var x uint64 = 0x9e3779b97f4a7c15
	for i := range ordinals {
		x ^= x << 7
		x ^= x >> 9
		x ^= x << 8
		ordinals[i] = int(x % uint64(rows))
	}
	return ordinals
}
