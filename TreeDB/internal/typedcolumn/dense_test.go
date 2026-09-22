package typedcolumn

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestTypedColumnVectorDenseDirectViewAligned(t *testing.T) {
	image := mustDenseVectorAdjacencyImage1756(t)
	section := mustColumnDataSection1756(t, image, "embedding")
	mgr := mappedresource.NewManager()
	h := mustAcquireImageSectionBytes1756(t, mgr, image, section, image.SectionBytesMust1756(t, section))
	defer h.Release()
	view, err := mgr.Float32View(h)
	if err != nil {
		t.Fatalf("Float32View aligned: %v", err)
	}
	if len(view) != 6 || view[0] != 1 || view[5] != 4 {
		t.Fatalf("view=%v want dense vector values", view)
	}
	if stats := mgr.Stats(); stats.DirectViewSuccesses != 1 || stats.DirectViewFailures != 0 {
		t.Fatalf("direct view stats=%+v", stats)
	}
}

func TestTypedColumnVectorDenseMisalignedFallsBackOrFailsClosed(t *testing.T) {
	image := mustDenseVectorAdjacencyImage1756(t)
	section := mustColumnDataSection1756(t, image, "embedding")
	raw := image.SectionBytesMust1756(t, section)
	misalignedBacking := append([]byte{0}, raw...)
	misaligned := misalignedBacking[1:]
	mgr := mappedresource.NewManager()
	h := mustAcquireImageSectionBytes1756(t, mgr, image, section, misaligned)
	defer h.Release()
	if _, err := mgr.Float32View(h); err == nil {
		t.Fatalf("Float32View misaligned err=nil, want validation failure")
	}
	values, err := DecodeRawFloat32VectorPayload(nil, h.Bytes(), 2, 3)
	if err != nil {
		t.Fatalf("fallback DecodeRawFloat32VectorPayload: %v", err)
	}
	if len(values) != 6 || values[1] != 0.5 || values[2] != -0.25 {
		t.Fatalf("fallback values=%v", values)
	}
}

func TestTypedColumnAdjacencyLittleEndianPayloadFixture(t *testing.T) {
	image := mustDenseVectorAdjacencyImage1756(t)
	section := mustColumnDataSection1756(t, image, "neighbors")
	values, err := DecodeRawUint32DensePayload(nil, image.SectionBytesMust1756(t, section), 2, 2)
	if err != nil {
		t.Fatalf("DecodeRawUint32DensePayload: %v", err)
	}
	if !slices.Equal(values, []uint32{1, 2, 0, 2}) {
		t.Fatalf("adjacency payload values=%v", values)
	}
}

func TestTypedColumnAdjacencyMisalignedFallsBackOrFailsClosed(t *testing.T) {
	image := mustDenseVectorAdjacencyImage1756(t)
	section := mustColumnDataSection1756(t, image, "neighbors")
	raw := image.SectionBytesMust1756(t, section)
	misalignedBacking := append([]byte{0}, raw...)
	misaligned := misalignedBacking[1:]
	mgr := mappedresource.NewManager()
	h := mustAcquireImageSectionBytes1756(t, mgr, image, section, misaligned)
	defer h.Release()
	if _, err := mgr.Uint32View(h); err == nil {
		t.Fatalf("Uint32View misaligned err=nil, want validation failure")
	}
	values, err := DecodeRawUint32DensePayload(nil, h.Bytes(), 2, 2)
	if err != nil {
		t.Fatalf("fallback DecodeRawUint32DensePayload: %v", err)
	}
	if !slices.Equal(values, []uint32{1, 2, 0, 2}) {
		t.Fatalf("fallback adjacency=%v", values)
	}
}

func TestTypedColumnDenseDescriptorRejectsTruncatedRawBytes1756(t *testing.T) {
	part := mustDenseVectorAdjacencyPart1756(t)
	for _, tc := range []struct {
		name               string
		columnType         ColumnType
		fixedWidthElements int
	}{
		{name: "embedding", columnType: ColumnTypeFloat32Vector, fixedWidthElements: 3},
		{name: "neighbors", columnType: ColumnTypeAdjacencyList, fixedWidthElements: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := part.Columns[tc.name].Blocks[0].Descriptor
			block.RawBytes -= 4
			block.StoredBytes -= 4
			err := validateDecodedColumnBlockDescriptor(part.Descriptor, tc.name, tc.columnType, 0, tc.fixedWidthElements, 0, block)
			if err == nil || !strings.Contains(err.Error(), "dense raw bytes") {
				t.Fatalf("validate truncated dense descriptor err=%v, want dense raw-bytes failure", err)
			}
		})
	}
}

func TestTypedColumnDenseDescriptorRejectsCompressedBlocks1756(t *testing.T) {
	part := mustDenseVectorAdjacencyPart1756(t)
	for _, tc := range []struct {
		name               string
		columnType         ColumnType
		fixedWidthElements int
	}{
		{name: "embedding", columnType: ColumnTypeFloat32Vector, fixedWidthElements: 3},
		{name: "neighbors", columnType: ColumnTypeAdjacencyList, fixedWidthElements: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := part.Columns[tc.name].Blocks[0].Descriptor
			block.Compression = CompressionSnappy
			err := validateDecodedColumnBlockDescriptor(part.Descriptor, tc.name, tc.columnType, 0, tc.fixedWidthElements, 0, block)
			if err == nil || !strings.Contains(err.Error(), "dense compression") {
				t.Fatalf("validate compressed dense descriptor err=%v, want dense compression failure", err)
			}
		})
	}
}

func TestTypedColumnVectorLogicalPrimaryKeyFailsClosed1756(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BuildColumnPart panicked for vector logical primary key: %v", r)
		}
	}()
	_, err := BuildColumnPart(9, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 3},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"embedding"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:           2,
		Columns:        map[string][]int64{"id": {10, 11}},
		Float32Vectors: map[string][]float32{"embedding": {1, 0.5, -0.25, 2, 3, 4}},
	})
	if err == nil {
		t.Fatalf("BuildColumnPart err=nil for vector logical primary key")
	}
	if !strings.Contains(err.Error(), "logical primary key column embedding type=float32_vector is not scalar") {
		t.Fatalf("BuildColumnPart err=%v, want vector logical primary key fail-closed", err)
	}
}

func TestTypedColumnAggregateMetadataVectorPredicateFailsClosed1756(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BuildColumnPart panicked for vector aggregate predicate: %v", r)
		}
	}()
	_, err := BuildColumnPart(10, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "time_us", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "kind_code", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone, CompressionSet: true, Cardinality: 3},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 3},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
		AggregateMetadata: []AggregateMetadataDefinition{{
			Name:      "kind_time",
			Version:   AggregateMetadataDefinitionVersion,
			Kind:      AggregateMetadataGroupMinMax,
			Scope:     AggregateMetadataScopeGranule,
			GroupKeys: []string{"kind_code"},
			Measures: []AggregateMetadataMeasure{
				{Op: AggregateMetadataMeasureCount},
				{Op: AggregateMetadataMeasureMin, Column: "time_us"},
				{Op: AggregateMetadataMeasureMax, Column: "time_us"},
			},
			Predicates:     []AggregateMetadataPredicate{{Column: "embedding", Op: AggregateMetadataPredicateEq, Value: 1}},
			MaxBytesPerRow: 256,
		}},
	}, Batch{
		Rows: 2,
		Columns: map[string][]int64{
			"id":        {10, 11},
			"time_us":   {100, 200},
			"kind_code": {1, 2},
		},
		Float32Vectors: map[string][]float32{"embedding": {1, 0.5, -0.25, 2, 3, 4}},
	})
	if err == nil {
		t.Fatalf("BuildColumnPart err=nil for vector aggregate predicate")
	}
	if !strings.Contains(err.Error(), "aggregate metadata kind_time predicate column embedding type=float32_vector is not scalar") {
		t.Fatalf("BuildColumnPart err=%v, want vector aggregate predicate fail-closed", err)
	}
}

func TestTypedColumnAggregateMetadataAdjacencyListPredicateFailsClosed1756(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("BuildColumnPart panicked for adjacency aggregate predicate: %v", r)
		}
	}()
	_, err := BuildColumnPart(11, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "time_us", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "kind_code", Type: ColumnTypeLowCardinalityCode, Encoding: EncodingLowCardinalityUint32, Compression: CompressionNone, CompressionSet: true, Cardinality: 3},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Encoding: EncodingRawUint32Dense, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 2},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
		AggregateMetadata: []AggregateMetadataDefinition{{
			Name:      "kind_time",
			Version:   AggregateMetadataDefinitionVersion,
			Kind:      AggregateMetadataGroupMinMax,
			Scope:     AggregateMetadataScopeGranule,
			GroupKeys: []string{"kind_code"},
			Measures: []AggregateMetadataMeasure{
				{Op: AggregateMetadataMeasureCount},
				{Op: AggregateMetadataMeasureMin, Column: "time_us"},
				{Op: AggregateMetadataMeasureMax, Column: "time_us"},
			},
			Predicates:     []AggregateMetadataPredicate{{Column: "neighbors", Op: AggregateMetadataPredicateEq, Value: 1}},
			MaxBytesPerRow: 256,
		}},
	}, Batch{
		Rows: 2,
		Columns: map[string][]int64{
			"id":        {10, 11},
			"time_us":   {100, 200},
			"kind_code": {1, 2},
		},
		Uint32Vectors: map[string][]uint32{"neighbors": {1, 2, 0, 2}},
	})
	if err == nil {
		t.Fatalf("BuildColumnPart err=nil for adjacency aggregate predicate")
	}
	if !strings.Contains(err.Error(), "aggregate metadata kind_time predicate column neighbors type=adjacency_list is not scalar") {
		t.Fatalf("BuildColumnPart err=%v, want adjacency aggregate predicate fail-closed", err)
	}
}

func TestTypedColumnDescriptorAccountingIncludesFixedWidthElements1756(t *testing.T) {
	part := mustDenseVectorAdjacencyPart1756(t)
	blocks := 0
	for _, column := range part.Descriptor.Columns {
		blocks += len(column.Blocks)
	}
	want := columnPartDescriptorBaseBytes + len(part.Descriptor.Granules)*granuleDescriptorBytes + len(part.Descriptor.Columns)*36 + blocks*columnBlockDescriptorBytes
	if got := estimateColumnPartDescriptorBytes(part.Descriptor); got != want {
		t.Fatalf("estimateColumnPartDescriptorBytes=%d want %d including 4-byte fixed-width field per column", got, want)
	}
}

func TestTypedColumnVectorCountersDirectViewsAndScratchDecodes(t *testing.T) {
	image := mustDenseVectorAdjacencyImage1756(t)
	section := mustColumnDataSection1756(t, image, "embedding")
	raw := image.SectionBytesMust1756(t, section)
	mgr := mappedresource.NewManager()
	aligned := mustAcquireImageSectionBytes1756(t, mgr, image, section, raw)
	if _, err := mgr.Float32View(aligned); err != nil {
		t.Fatalf("aligned Float32View: %v", err)
	}
	if err := aligned.Release(); err != nil {
		t.Fatalf("aligned release: %v", err)
	}
	misalignedBacking := append([]byte{0}, raw...)
	misaligned := mustAcquireImageSectionBytes1756(t, mgr, image, section, misalignedBacking[1:])
	if _, err := mgr.Float32View(misaligned); err == nil {
		t.Fatalf("misaligned Float32View err=nil")
	}
	if _, err := DecodeRawFloat32VectorPayload(nil, misaligned.Bytes(), 2, 3); err != nil {
		t.Fatalf("fallback decode: %v", err)
	}
	if err := misaligned.Release(); err != nil {
		t.Fatalf("misaligned release: %v", err)
	}
	stats := mgr.Stats()
	if stats.DirectViewSuccesses != 1 || stats.DirectViewFailures != 1 || stats.TotalHeapCopyBytes != uint64(len(raw)*2) {
		t.Fatalf("stats=%+v want one direct view, one scratch decode, heap bytes", stats)
	}
}

func TestTypedColumnDenseMmapHeapParity1756(t *testing.T) {
	image := mustDenseVectorAdjacencyImage1756(t)
	section := mustColumnDataSection1756(t, image, "embedding")
	path := filepath.Join(t.TempDir(), "part.tcim")
	if err := os.WriteFile(path, image.Bytes, 0o600); err != nil {
		t.Fatalf("write image: %v", err)
	}
	mgr := mappedresource.NewManager()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "dense-parity", Namespace: "typedcolumn-test"}
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: string(section.Kind), PartID: image.PartID, FileID: 1, Offset: int64(section.Offset), Length: int64(section.Length), Version: image.Version}
	mappedHandle, err := mgr.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{Reason: "mapped dense vector", ValidationMode: mappedresource.ValidationVerify, PreferMapped: true, AllowHeapCopy: true})
	if err != nil {
		t.Fatalf("AcquireFileRange mapped: %v", err)
	}
	defer mappedHandle.Release()
	heapHandle, err := mgr.AcquireFileRange(key, scope, path, mappedresource.AcquireOptions{Reason: "heap dense vector", ValidationMode: mappedresource.ValidationVerify, AllowHeapCopy: true})
	if err != nil {
		t.Fatalf("AcquireFileRange heap: %v", err)
	}
	defer heapHandle.Release()
	mappedValues, err := mgr.Float32View(mappedHandle)
	if err != nil {
		t.Fatalf("mapped Float32View: %v", err)
	}
	heapValues, err := mgr.Float32View(heapHandle)
	if err != nil {
		t.Fatalf("heap Float32View: %v", err)
	}
	if !slices.Equal(mappedValues, heapValues) || len(mappedValues) != 6 || math.Float32bits(mappedValues[2]) != math.Float32bits(-0.25) {
		t.Fatalf("mapped=%v heap=%v", mappedValues, heapValues)
	}
}

func TestTypedColumnVectorDenseDirectViewKernelZeroAllocs1781(t *testing.T) {
	const rows = 1024
	const dims = 16
	part := mustDenseVectorPart1756(t, rows, dims)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	section := mustColumnDataSection1756(t, parsed, "embedding")
	mgr := mappedresource.NewManager()
	h := mustAcquireImageSectionBytes1756(t, mgr, parsed, section, parsed.SectionBytesMust1756(t, section))
	defer h.Release()
	view, err := mgr.Float32View(h)
	if err != nil {
		t.Fatalf("Float32View setup: %v", err)
	}
	if len(view) != rows*dims {
		t.Fatalf("view elements=%d want %d", len(view), rows*dims)
	}
	var sum float32
	allocs := testing.AllocsPerRun(1000, func() {
		sum += denseFloat32KernelSum1781(view)
	})
	if allocs != 0 {
		t.Fatalf("direct-view dense float32 kernel allocs/run=%v want 0", allocs)
	}
	if sum == 0 {
		t.Fatalf("sum=0")
	}
}

func BenchmarkTypedColumnVectorDenseDirectViewScan(b *testing.B) {
	const rows = 1024
	const dims = 16
	part := mustDenseVectorPart1756(b, rows, dims)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		b.Fatalf("BuildColumnPartImage: %v", err)
	}
	section := mustColumnDataSection1756(b, image, "embedding")
	mgr := mappedresource.NewManager()
	h := mustAcquireImageSectionBytes1756(b, mgr, image, section, image.SectionBytesMust1756(b, section))
	defer h.Release()
	b.ReportAllocs()
	b.ResetTimer()
	var sum float32
	for i := 0; i < b.N; i++ {
		view, err := mgr.Float32View(h)
		if err != nil {
			b.Fatalf("Float32View: %v", err)
		}
		for _, value := range view {
			sum += value
		}
	}
	b.StopTimer()
	if sum == 0 {
		b.Fatalf("sum=0")
	}
	b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "rows/s")
	b.ReportMetric(float64(b.N*rows*dims)/b.Elapsed().Seconds(), "elements/s")
	stats := mgr.Stats()
	b.ReportMetric(float64(stats.DirectViewSuccesses)/float64(b.N), "direct_views/op")
	b.ReportMetric(float64(stats.DirectViewFailures)/float64(b.N), "scratch_decodes/op")
	b.ReportMetric(float64(stats.TotalMappedBytes), "mapped_B")
	b.ReportMetric(float64(stats.TotalHeapCopyBytes), "heap_copy_B")
}

func BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan(b *testing.B) {
	const rows = 1024
	const dims = 16
	part := mustDenseVectorPart1756(b, rows, dims)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		b.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		b.Fatalf("ParseColumnPartImage: %v", err)
	}
	section := mustColumnDataSection1756(b, parsed, "embedding")
	path := filepath.Join(b.TempDir(), "part.tcim")
	if err := os.WriteFile(path, parsed.Bytes, 0o600); err != nil {
		b.Fatalf("write image: %v", err)
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "dense-mmap-heap-bench", Namespace: "typedcolumn-test"}
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: string(section.Kind), PartID: parsed.PartID, FileID: 1, Offset: int64(section.Offset), Length: int64(section.Length), Version: parsed.Version, Encoding: section.Encoding.String()}
	for _, tc := range []struct {
		name string
		opts mappedresource.AcquireOptions
	}{
		{name: "mapped", opts: mappedresource.AcquireOptions{Reason: "mapped dense vector benchmark", ValidationMode: mappedresource.ValidationVerify, PreferMapped: true, AllowHeapCopy: true}},
		{name: "heap", opts: mappedresource.AcquireOptions{Reason: "heap dense vector benchmark", ValidationMode: mappedresource.ValidationVerify, AllowHeapCopy: true}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mgr := mappedresource.NewManager()
			h, err := mgr.AcquireFileRange(key, scope, path, tc.opts)
			if err != nil {
				b.Fatalf("AcquireFileRange: %v", err)
			}
			defer h.Release()
			if _, err := mgr.Float32View(h); err != nil {
				b.Fatalf("Float32View setup: %v", err)
			}
			baseStats := mgr.Stats()
			b.ReportAllocs()
			b.ResetTimer()
			var sum float32
			for i := 0; i < b.N; i++ {
				view, err := mgr.Float32View(h)
				if err != nil {
					b.Fatalf("Float32View: %v", err)
				}
				sum += denseFloat32KernelSum1781(view)
			}
			b.StopTimer()
			if sum == 0 {
				b.Fatalf("sum=0")
			}
			elapsed := b.Elapsed()
			stats := mgr.Stats()
			b.ReportMetric(float64(b.N*rows)/elapsed.Seconds(), "rows/s")
			b.ReportMetric(float64(b.N*rows*dims)/elapsed.Seconds(), "elements/s")
			b.ReportMetric(float64(stats.DirectViewSuccesses-baseStats.DirectViewSuccesses)/float64(b.N), "direct_views/op")
			b.ReportMetric(float64(stats.DirectViewFailures-baseStats.DirectViewFailures)/float64(b.N), "scratch_decodes/op")
			b.ReportMetric(float64(stats.TotalMappedBytes), "mapped_B")
			b.ReportMetric(float64(stats.TotalHeapCopyBytes), "heap_copy_B")
		})
	}
}

func BenchmarkTypedColumnVectorDenseSectionScan(b *testing.B) {
	const rows = 1024
	const dims = 16
	part := mustDenseVectorPart1756(b, rows, dims)
	b.ReportAllocs()
	b.ResetTimer()
	var sum float32
	for i := 0; i < b.N; i++ {
		matrix, err := part.DenseFloat32VectorColumn("embedding", nil)
		if err != nil {
			b.Fatalf("DenseFloat32VectorColumn: %v", err)
		}
		for _, value := range matrix.Values {
			sum += value
		}
	}
	b.StopTimer()
	if sum == 0 {
		b.Fatalf("sum=0")
	}
	b.ReportMetric(float64(b.N*rows)/b.Elapsed().Seconds(), "rows/s")
	b.ReportMetric(float64(b.N*rows*dims)/b.Elapsed().Seconds(), "elements/s")
}

func denseFloat32KernelSum1781(values []float32) float32 {
	var sum float32
	for _, value := range values {
		sum += value
	}
	return sum
}

func mustDenseVectorAdjacencyImage1756(t testing.TB) ColumnPartImage {
	t.Helper()
	part := mustDenseVectorAdjacencyPart1756(t)
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	return parsed
}

func mustDenseVectorAdjacencyPart1756(t testing.TB) *ColumnPart {
	t.Helper()
	opts := Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, Encoding: EncodingRawFloat32Vector, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 3},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Encoding: EncodingRawUint32Dense, Compression: CompressionNone, CompressionSet: true, FixedWidthElements: 2},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}
	part, err := BuildColumnPart(7, opts, Batch{
		Rows:           2,
		Columns:        map[string][]int64{"id": []int64{10, 11}},
		Float32Vectors: map[string][]float32{"embedding": []float32{1, 0.5, -0.25, 2, 3, 4}},
		Uint32Vectors:  map[string][]uint32{"neighbors": []uint32{1, 2, 0, 2}},
	})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	return part
}

func mustDenseVectorPart1756(t testing.TB, rows int, dims int) *ColumnPart {
	t.Helper()
	ids := make([]int64, rows)
	values := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		ids[row] = int64(row)
		for dim := 0; dim < dims; dim++ {
			values[row*dims+dim] = float32(row+dim) / 10
		}
	}
	part, err := BuildColumnPart(8, Options{
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
		t.Fatalf("BuildColumnPart: %v", err)
	}
	return part
}

func mustColumnDataSection1756(t testing.TB, image ColumnPartImage, column string) ColumnPartImageSection {
	t.Helper()
	for _, section := range image.Sections {
		if section.Kind == ColumnPartImageSectionColumnData && section.Column == column {
			return section
		}
	}
	t.Fatalf("missing column section %q in %+v", column, image.Sections)
	return ColumnPartImageSection{}
}

func mustAcquireImageSectionBytes1756(t testing.TB, mgr *mappedresource.Manager, image ColumnPartImage, section ColumnPartImageSection, data []byte) *mappedresource.Handle {
	t.Helper()
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "dense-test", Namespace: "typedcolumn-test"}
	key := mappedresource.Key{Class: mappedresource.ClassTypedColumnAsset, Namespace: scope.Namespace, Kind: string(section.Kind), PartID: image.PartID, FileID: 1, Offset: int64(section.Offset), Length: int64(len(data)), Version: image.Version, Encoding: section.Encoding.String()}
	h, err := mgr.AcquireBytes(key, scope, mappedresource.SourceHeapCopy, data, mappedresource.AcquireOptions{Reason: "dense section", ValidationMode: mappedresource.ValidationVerify})
	if err != nil {
		t.Fatalf("AcquireBytes: %v", err)
	}
	return h
}

func (i ColumnPartImage) SectionBytesMust1756(t testing.TB, section ColumnPartImageSection) []byte {
	t.Helper()
	data, err := i.SectionBytes(section)
	if err != nil {
		t.Fatalf("SectionBytes(%s): %v", section.Column, err)
	}
	return data
}
