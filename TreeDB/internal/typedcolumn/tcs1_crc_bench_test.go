package typedcolumn

import "testing"

var benchmarkTCS1RowsSink int
var benchmarkTCS1CRCSink uint32

func BenchmarkTCS1DecodeChecksum(b *testing.B) {
	const (
		rows = 8192
		dims = 128
	)
	image := benchmarkTCS1ColumnPartImage(b, rows, dims)
	tcs1, record, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		b.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	if record.PayloadBytes == 0 || record.PayloadCRC32 == 0 {
		b.Fatalf("unexpected TCS1 record: %+v", record)
	}

	b.SetBytes(int64(len(tcs1)))
	b.ReportAllocs()
	b.ResetTimer()
	var rowSum int
	var crcSum uint32
	for i := 0; i < b.N; i++ {
		decodedImage, decodedRecord, err := DecodeTCS1ColumnPartImage(tcs1)
		if err != nil {
			b.Fatalf("DecodeTCS1ColumnPartImage: %v", err)
		}
		rowSum += decodedImage.Rows
		crcSum ^= decodedRecord.PayloadCRC32
	}
	b.StopTimer()
	benchmarkTCS1RowsSink = rowSum
	benchmarkTCS1CRCSink = crcSum
	b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "rows/s")
}

func benchmarkTCS1ColumnPartImage(tb testing.TB, rows, dims int) ColumnPartImage {
	tb.Helper()
	ids := make([]int64, rows)
	values := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		ids[row] = int64(row)
		for dim := 0; dim < dims; dim++ {
			values[row*dims+dim] = float32((row+1)*(dim+3)%997) / 997
		}
	}
	part, err := BuildColumnPart(1851, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: dims},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: rows},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{Rows: rows, Columns: map[string][]int64{"id": ids}, Float32Vectors: map[string][]float32{"embedding": values}})
	if err != nil {
		tb.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage: %v", err)
	}
	return parsed
}
