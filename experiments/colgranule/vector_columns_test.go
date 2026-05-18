package colgranule

import (
	"slices"
	"strings"
	"testing"
)

func TestColumnPartBuildsFloat32VectorAndAdjacencyColumns(t *testing.T) {
	part, err := BuildColumnPart(41, vectorPartTestOptions(), vectorPartTestBatch())
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}

	scan, err := part.NewScanner().ScanProjected([]string{"id"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5})

	scanner := part.NewScanner()
	vectors, err := scanner.ScanFloat32VectorsInto("embedding", nil)
	if err != nil {
		t.Fatalf("ScanFloat32VectorsInto: %v", err)
	}
	if vectors.Rows != 5 || vectors.Dims != 3 {
		t.Fatalf("vector scan rows/dims=(%d,%d) want (5,3)", vectors.Rows, vectors.Dims)
	}
	assertFloat32s(t, "embedding", vectors.Values, []float32{
		10, 11, 12,
		20, 21, 22,
		30, 31, 32,
		40, 41, 42,
		50, 51, 52,
	})

	neighbors, err := scanner.ScanAdjacencyListsInto("neighbors", nil, nil)
	if err != nil {
		t.Fatalf("ScanAdjacencyListsInto: %v", err)
	}
	if !slices.Equal(neighbors.Offsets, []uint32{0, 1, 4, 6, 8, 8}) {
		t.Fatalf("neighbors offsets=%v", neighbors.Offsets)
	}
	assertUint32s(t, "neighbors", neighbors.Values, []uint32{11, 21, 22, 23, 31, 32, 41, 42})

	locator, ok := part.LocatePrimaryID(4)
	if !ok {
		t.Fatal("missing locator for primary id 4")
	}
	pointVector, err := scanner.Float32VectorAt(locator, "embedding", nil)
	if err != nil {
		t.Fatalf("Float32VectorAt: %v", err)
	}
	assertFloat32s(t, "point vector", pointVector, []float32{40, 41, 42})
	pointNeighbors, err := scanner.AdjacencyListAt(locator, "neighbors", nil)
	if err != nil {
		t.Fatalf("AdjacencyListAt: %v", err)
	}
	assertUint32s(t, "point neighbors", pointNeighbors, []uint32{41, 42})
}

func TestColumnPartVectorColumnsSurviveImageRoundTrip(t *testing.T) {
	part, err := BuildColumnPart(42, vectorPartTestOptions(), vectorPartTestBatch())
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	imagePart, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	if got := imagePart.Columns["embedding"].Definition.VectorDims; got != 3 {
		t.Fatalf("image vector dims=%d want 3", got)
	}

	scanner := imagePart.NewScanner()
	vectors, err := scanner.ScanFloat32VectorsInto("embedding", nil)
	if err != nil {
		t.Fatalf("ScanFloat32VectorsInto: %v", err)
	}
	assertFloat32s(t, "image embedding", vectors.Values, []float32{
		10, 11, 12,
		20, 21, 22,
		30, 31, 32,
		40, 41, 42,
		50, 51, 52,
	})
	neighbors, err := scanner.ScanAdjacencyListsInto("neighbors", nil, nil)
	if err != nil {
		t.Fatalf("ScanAdjacencyListsInto: %v", err)
	}
	if !slices.Equal(neighbors.Offsets, []uint32{0, 1, 4, 6, 8, 8}) {
		t.Fatalf("image neighbors offsets=%v", neighbors.Offsets)
	}
	assertUint32s(t, "image neighbors", neighbors.Values, []uint32{11, 21, 22, 23, 31, 32, 41, 42})
}

func TestColumnPartVectorColumnsValidateShape(t *testing.T) {
	opts := vectorPartTestOptions()
	batch := vectorPartTestBatch()
	batch.Float32Vectors["embedding"] = Float32VectorColumn{Dims: 2, Values: batch.Float32Vectors["embedding"].Values}
	_, err := BuildColumnPart(1, opts, batch)
	if err == nil || !strings.Contains(err.Error(), "dims=2 want=3") {
		t.Fatalf("vector dims err=%v want dims mismatch", err)
	}

	batch = vectorPartTestBatch()
	batch.AdjacencyLists["neighbors"] = AdjacencyListColumn{
		Offsets: []uint32{0, 1, 3, 3, 4, 6},
		Values:  []uint32{11, 21, 22, 41, 42},
	}
	_, err = BuildColumnPart(1, opts, batch)
	if err == nil || !strings.Contains(err.Error(), "final offset=6 values=5") {
		t.Fatalf("adjacency shape err=%v want final offset mismatch", err)
	}
}

func TestColumnMutationAdapterMergesVectorAndAdjacencyBatches(t *testing.T) {
	opts, err := normalizeColumnStoreOptions(vectorPartTestOptions())
	if err != nil {
		t.Fatalf("normalizeColumnStoreOptions: %v", err)
	}
	adapter := &ColumnMutationAdapter{opts: opts}
	insertBatch := ColumnBatch{
		Rows: 2,
		Columns: map[string][]int64{
			"id": {30, 10},
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding": {
				Dims:   3,
				Values: []float32{30, 31, 32, 10, 11, 12},
			},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {
				Offsets: []uint32{0, 2, 3},
				Values:  []uint32{31, 32, 11},
			},
		},
	}
	updateBatch := ColumnBatch{
		Rows: 1,
		Columns: map[string][]int64{
			"id": {20},
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding": {
				Dims:   3,
				Values: []float32{20, 21, 22},
			},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {
				Offsets: []uint32{0, 2},
				Values:  []uint32{21, 22},
			},
		},
	}

	rows, inserted, updated, err := adapter.mergeMutationRows(insertBatch, updateBatch)
	if err != nil {
		t.Fatalf("mergeMutationRows: %v", err)
	}
	if rows.Rows != 3 || inserted != 2 || updated != 1 {
		t.Fatalf("merge rows=%d inserted=%d updated=%d", rows.Rows, inserted, updated)
	}
	part, err := BuildColumnDeltaPart(55, opts, rows)
	if err != nil {
		t.Fatalf("BuildColumnDeltaPart: %v", err)
	}
	scanner := part.NewScanner()
	vectors, err := scanner.ScanFloat32VectorsInto("embedding", nil)
	if err != nil {
		t.Fatalf("ScanFloat32VectorsInto: %v", err)
	}
	assertFloat32s(t, "merged vectors", vectors.Values, []float32{
		10, 11, 12,
		20, 21, 22,
		30, 31, 32,
	})
	neighbors, err := scanner.ScanAdjacencyListsInto("neighbors", nil, nil)
	if err != nil {
		t.Fatalf("ScanAdjacencyListsInto: %v", err)
	}
	if !slices.Equal(neighbors.Offsets, []uint32{0, 1, 3, 5}) {
		t.Fatalf("merged neighbor offsets=%v", neighbors.Offsets)
	}
	assertUint32s(t, "merged neighbors", neighbors.Values, []uint32{11, 21, 22, 31, 32})
}

func TestColumnPartSetCompactionPreservesVectorAndAdjacencyColumns(t *testing.T) {
	opts := vectorPartTestOptions()
	workspace, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{Collection: "vectors"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()

	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:        "vectors",
		StoreOptions:      opts,
		InitialPartID:     100,
		InitialGeneration: 1,
	})
	if err != nil {
		t.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	if _, err := adapter.PublishBaseBatch(vectorPartTestBatch(), ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: 5}); err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	_, err = adapter.Apply(ColumnMutationBatch{
		Inserts: ColumnBatch{
			Rows: 1,
			Columns: map[string][]int64{
				"id": {6},
			},
			Float32Vectors: map[string]Float32VectorColumn{
				"embedding": {Dims: 3, Values: []float32{60, 61, 62}},
			},
			AdjacencyLists: map[string]AdjacencyListColumn{
				"neighbors": {Offsets: []uint32{0, 3}, Values: []uint32{61, 62, 63}},
			},
		},
		Updates: ColumnBatch{
			Rows: 1,
			Columns: map[string][]int64{
				"id": {2},
			},
			Float32Vectors: map[string]Float32VectorColumn{
				"embedding": {Dims: 3, Values: []float32{200, 201, 202}},
			},
			AdjacencyLists: map[string]AdjacencyListColumn{
				"neighbors": {Offsets: []uint32{0, 2}, Values: []uint32{2001, 2002}},
			},
		},
		Deletes:                 []int64{3},
		SourceRowRootGeneration: 1,
		SourceRowVersionLower:   5,
		SourceRowVersionUpper:   7,
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	compacted, err := CompactColumnPartSet(workspace, reader, opts, nil, 200)
	if err != nil {
		t.Fatalf("CompactColumnPartSet: %v", err)
	}
	if compacted.VisibleRows != 5 || compacted.DroppedRows != 2 {
		t.Fatalf("compaction rows visible=%d dropped=%d want visible=5 dropped=2", compacted.VisibleRows, compacted.DroppedRows)
	}
	load, err := workspace.LoadPartWithOptions(compacted.Part.PartID, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("LoadPartWithOptions(compacted): %v", err)
	}
	scanner := load.Part.NewScanner()
	ids, err := scanner.ScanProjected([]string{"id"})
	if err != nil {
		t.Fatalf("ScanProjected(id): %v", err)
	}
	assertInt64s(t, "compacted ids", ids.Columns["id"], []int64{1, 2, 4, 5, 6})
	vectors, err := scanner.ScanFloat32VectorsInto("embedding", nil)
	if err != nil {
		t.Fatalf("ScanFloat32VectorsInto: %v", err)
	}
	assertFloat32s(t, "compacted vectors", vectors.Values, []float32{
		10, 11, 12,
		200, 201, 202,
		40, 41, 42,
		50, 51, 52,
		60, 61, 62,
	})
	neighbors, err := scanner.ScanAdjacencyListsInto("neighbors", nil, nil)
	if err != nil {
		t.Fatalf("ScanAdjacencyListsInto: %v", err)
	}
	if !slices.Equal(neighbors.Offsets, []uint32{0, 1, 3, 5, 5, 8}) {
		t.Fatalf("compacted adjacency offsets=%v", neighbors.Offsets)
	}
	assertUint32s(t, "compacted neighbors", neighbors.Values, []uint32{11, 2001, 2002, 41, 42, 61, 62, 63})
}

func BenchmarkColumnPartVectorAdjacencyBuild(b *testing.B) {
	opts := vectorPartBenchmarkOptions(8192, 128)
	batch := vectorPartBenchmarkBatch(8192, 128, 16)
	b.ReportAllocs()
	b.SetBytes(int64(8192*128*4 + 8192*16*4))
	for i := 0; i < b.N; i++ {
		part, err := BuildColumnPart(uint64(i+1), opts, batch)
		if err != nil {
			b.Fatalf("BuildColumnPart: %v", err)
		}
		benchSink += int64(part.Descriptor.RowCount + len(part.Columns))
	}
}

func BenchmarkColumnPartVectorScan(b *testing.B) {
	opts := vectorPartBenchmarkOptions(8192, 128)
	batch := vectorPartBenchmarkBatch(8192, 128, 16)
	part, err := BuildColumnPart(51, opts, batch)
	if err != nil {
		b.Fatalf("BuildColumnPart: %v", err)
	}
	scanner := part.NewScanner()
	warm, err := scanner.ScanFloat32VectorsInto("embedding", nil)
	if err != nil {
		b.Fatalf("ScanFloat32VectorsInto: %v", err)
	}
	scratch := warm.Values
	b.ReportAllocs()
	b.SetBytes(int64(len(scratch) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan, err := scanner.ScanFloat32VectorsInto("embedding", scratch)
		if err != nil {
			b.Fatalf("ScanFloat32VectorsInto: %v", err)
		}
		scratch = scan.Values
		benchSink += int64(scan.Values[0])
	}
}

func BenchmarkColumnPartAdjacencyScan(b *testing.B) {
	opts := vectorPartBenchmarkOptions(8192, 128)
	batch := vectorPartBenchmarkBatch(8192, 128, 16)
	part, err := BuildColumnPart(52, opts, batch)
	if err != nil {
		b.Fatalf("BuildColumnPart: %v", err)
	}
	scanner := part.NewScanner()
	warm, err := scanner.ScanAdjacencyListsInto("neighbors", nil, nil)
	if err != nil {
		b.Fatalf("ScanAdjacencyListsInto: %v", err)
	}
	offsets := warm.Offsets
	values := warm.Values
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan, err := scanner.ScanAdjacencyListsInto("neighbors", offsets, values)
		if err != nil {
			b.Fatalf("ScanAdjacencyListsInto: %v", err)
		}
		offsets = scan.Offsets
		values = scan.Values
		benchSink += int64(scan.Offsets[len(scan.Offsets)-1])
	}
}

func BenchmarkColumnPartVectorPointLookup(b *testing.B) {
	opts := vectorPartBenchmarkOptions(8192, 128)
	batch := vectorPartBenchmarkBatch(8192, 128, 16)
	part, err := BuildColumnPart(53, opts, batch)
	if err != nil {
		b.Fatalf("BuildColumnPart: %v", err)
	}
	locator, ok := part.LocatePrimaryID(4096)
	if !ok {
		b.Fatal("missing locator")
	}
	scanner := part.NewScanner()
	warm, err := scanner.Float32VectorAt(locator, "embedding", nil)
	if err != nil {
		b.Fatalf("Float32VectorAt: %v", err)
	}
	scratch := warm
	b.ReportAllocs()
	b.SetBytes(int64(len(scratch) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vector, err := scanner.Float32VectorAt(locator, "embedding", scratch)
		if err != nil {
			b.Fatalf("Float32VectorAt: %v", err)
		}
		scratch = vector
		benchSink += int64(vector[0])
	}
}

func BenchmarkColumnPartAdjacencyPointLookup(b *testing.B) {
	opts := vectorPartBenchmarkOptions(8192, 128)
	batch := vectorPartBenchmarkBatch(8192, 128, 16)
	part, err := BuildColumnPart(54, opts, batch)
	if err != nil {
		b.Fatalf("BuildColumnPart: %v", err)
	}
	locator, ok := part.LocatePrimaryID(4096)
	if !ok {
		b.Fatal("missing locator")
	}
	scanner := part.NewScanner()
	warm, err := scanner.AdjacencyListAt(locator, "neighbors", nil)
	if err != nil {
		b.Fatalf("AdjacencyListAt: %v", err)
	}
	scratch := warm
	b.ReportAllocs()
	b.SetBytes(int64(len(scratch) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		neighbors, err := scanner.AdjacencyListAt(locator, "neighbors", scratch)
		if err != nil {
			b.Fatalf("AdjacencyListAt: %v", err)
		}
		scratch = neighbors
		benchSink += int64(neighbors[0])
	}
}

func vectorPartTestOptions() ColumnStoreOptions {
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, VectorDims: 3, Compression: CompressionNone, CodecBlockRows: 2},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Compression: CompressionNone, CodecBlockRows: 2},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
	}
}

func vectorPartTestBatch() ColumnBatch {
	return ColumnBatch{
		Columns: map[string][]int64{
			"id": {3, 1, 2, 5, 4},
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding": {
				Dims: 3,
				Values: []float32{
					30, 31, 32,
					10, 11, 12,
					20, 21, 22,
					50, 51, 52,
					40, 41, 42,
				},
			},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {
				Offsets: []uint32{0, 2, 3, 6, 6, 8},
				Values:  []uint32{31, 32, 11, 21, 22, 23, 41, 42},
			},
		},
	}
}

func vectorPartBenchmarkOptions(rows int, dims int) ColumnStoreOptions {
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CodecBlockRows: rows},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, VectorDims: dims, Compression: CompressionNone, CodecBlockRows: rows},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Compression: CompressionNone, CodecBlockRows: rows},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy: ColumnPartPolicy{
			RowsPerGranule:        rows,
			DefaultCodecBlockRows: rows,
		},
	}
}

func vectorPartBenchmarkBatch(rows int, dims int, degree int) ColumnBatch {
	ids := make([]int64, rows)
	vectors := make([]float32, rows*dims)
	offsets := make([]uint32, rows+1)
	neighbors := make([]uint32, rows*degree)
	for row := 0; row < rows; row++ {
		ids[row] = int64(row)
		for dim := 0; dim < dims; dim++ {
			vectors[row*dims+dim] = float32((row+dim)%1024) / 1024
		}
		offsets[row] = uint32(row * degree)
		for edge := 0; edge < degree; edge++ {
			neighbors[row*degree+edge] = uint32((row + edge + 1) % rows)
		}
	}
	offsets[rows] = uint32(len(neighbors))
	return ColumnBatch{
		Rows: rows,
		Columns: map[string][]int64{
			"id": ids,
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding": {Dims: dims, Values: vectors},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {Offsets: offsets, Values: neighbors},
		},
	}
}

func assertFloat32s(t *testing.T, label string, got []float32, want []float32) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s=%v want %v", label, got, want)
	}
}
